//! 对照 `orange/order_service.py`。幂等、截止时间和推荐熔断都在这里。

use crate::common::{self, canonical_body, error_body, header_request_id, log_event, now_ms, valid_caller, valid_idempotency_key, validate_order_body, CircuitBreaker, Metrics, RECOMMEND_BUDGET_MS, STORE_CALL_CAP_MS};
use crate::http_util::{self, empty_response, json_response, text_response, Response};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

struct LeaderView {
    leader: String,
    epoch: i64,
    leader_ok: bool,
    checked_at: f64,
}

pub struct OrderService {
    name: String,
    supervisor: String,
    recommender: String,
    breaker: CircuitBreaker,
    metrics: Metrics,
    view: Mutex<LeaderView>,
}

impl OrderService {
    fn new(name: &str, supervisor: &str, recommender: &str) -> Arc<Self> {
        Arc::new(Self {
            name: name.to_string(),
            supervisor: supervisor.to_string(),
            recommender: recommender.to_string(),
            breaker: CircuitBreaker::new(),
            metrics: Metrics::new(),
            view: Mutex::new(LeaderView { leader: String::new(), epoch: 1, leader_ok: false, checked_at: 0.0 }),
        })
    }

    fn refresh_leader(self: &Arc<Self>) {
        while !common::stop_requested() {
            self.refresh_once();
            thread::sleep(Duration::from_millis(200));
        }
    }

    fn refresh_once(&self) {
        let Ok((200, _, raw)) = http_util::http_request("GET", &format!("{}/internal/leader", self.supervisor), None, &[], 0.3) else {
            return;
        };
        let Ok(Some(body)) = http_util::decode_json(&raw) else {
            return;
        };
        let leader = body.get("leader").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let mut ready = false;
        if !leader.is_empty() {
            ready = http_util::http_request("GET", &format!("{leader}/ready"), None, &[], 0.3).map(|(status, _, _)| status == 200).unwrap_or(false);
        }
        let mut view = self.view.lock().expect("leader");
        view.leader = leader;
        view.epoch = body.get("epoch").and_then(|v| v.as_i64()).unwrap_or(view.epoch);
        view.leader_ok = ready;
        view.checked_at = common::monotonic();
    }

    fn current_leader(&self) -> (String, i64, bool) {
        let view = self.view.lock().expect("leader");
        let fresh = common::monotonic() - view.checked_at < 1.0 && !view.leader.is_empty();
        (view.leader.clone(), view.epoch, fresh && view.leader_ok)
    }
}

fn store_call(method: &str, url: &str, request_id: &str, deadline_ms: i64, body: Option<&Value>) -> Result<(u16, Option<Value>), ()> {
    let remaining = deadline_ms - now_ms();
    if remaining <= 0 {
        return Ok((504, None));
    }
    let timeout = (remaining.min(STORE_CALL_CAP_MS) as f64) / 1000.0;
    let headers = [("X-Request-Id", request_id), ("X-Deadline-Ms", &deadline_ms.to_string())];
    let (status, _, raw) = http_util::http_request(method, url, body, &headers, timeout).map_err(|_| ())?;
    let parsed = http_util::decode_json(&raw).map_err(|_| ())?;
    Ok((status, parsed))
}

fn proxy_read(service: &OrderService, route: &str, query: &HashMap<String, String>, headers: &HashMap<String, String>, request_id: &str) -> Response {
    let caller = headers.get("x-caller-id").map(|s| s.as_str()).unwrap_or("");
    if !valid_caller(caller) {
        return json_response(400, &error_body("invalid_argument", "X-Caller-Id 不合法", request_id), &[]);
    }
    let (leader, _, ok) = service.current_leader();
    let deadline = headers.get("x-deadline-ms").and_then(|v| v.parse().ok()).unwrap_or_else(|| now_ms() + 800);
    if !ok {
        return json_response(503, &error_body("unavailable", "主存储不可用", request_id), &[]);
    }
    let url = if route == "/internal/orders" {
        let key = query.get("idempotency_key").map(|s| s.as_str()).unwrap_or("");
        format!("{leader}/internal/orders/by-key?caller_id={}&idempotency_key={}", http_util::encode(caller), http_util::encode(key))
    } else {
        let order_id = route.rsplit('/').next().unwrap_or("");
        format!("{leader}/internal/orders/{order_id}?caller_id={}", http_util::encode(caller))
    };
    let (status, mut body) = match store_call("GET", &url, request_id, deadline, None) {
        Err(()) => {
            service.metrics.inc("orange_errors_total", 1);
            return json_response(503, &error_body("unavailable", "主存储不可用", request_id), &[]);
        }
        Ok(pair) => pair,
    };
    if status == 504 || body.is_none() {
        return json_response(504, &error_body("deadline_exceeded", "超过截止时间", request_id), &[]);
    }
    if let Some(value) = body.as_mut() {
        if let Some(object) = value.as_object_mut() {
            object.remove("request_hash");
        }
    }
    json_response(status, &body.unwrap_or(Value::Null), &[])
}

fn lookup_existing(service: &OrderService, caller: &str, key: &str, fields: &Value, request_id: &str, deadline: i64) -> Option<Response> {
    let (leader, _, ok) = service.current_leader();
    if !ok {
        return Some(json_response(503, &error_body("unavailable", "主存储不可用", request_id), &[]));
    }
    let url = format!("{leader}/internal/orders/by-key?caller_id={}&idempotency_key={}", http_util::encode(caller), http_util::encode(key));
    let (status, body) = store_call("GET", &url, request_id, if deadline == 0 { now_ms() + 800 } else { deadline }, None).ok()?;
    let Some(mut body) = body else {
        return None;
    };
    if status == 200 && body.get("order_id").is_some() {
        let digest = canonical_body(
            fields["sku"].as_str().unwrap_or(""),
            fields["qty"].as_i64().unwrap_or(0),
            fields.get("external_ref").and_then(|v| v.as_str()).unwrap_or(""),
            fields.get("fulfill_url").and_then(|v| v.as_str()).unwrap_or(""),
        );
        if body.get("request_hash").and_then(|v| v.as_str()) != Some(digest.as_str()) {
            return Some(json_response(409, &error_body("idempotency_mismatch", "幂等键与已有订单正文不一致", request_id), &[]));
        }
        body.as_object_mut().map(|object| object.remove("request_hash"));
        log_event("order_replayed", request_id, "info", &[("order_id", body["order_id"].clone())]);
        return Some(json_response(201, &body, &[]));
    }
    None
}

fn call_append(service: &OrderService, caller: &str, key: &str, fields: &Value, request_id: &str, deadline: i64) -> Result<Value, Response> {
    let mut last_error = json_response(503, &error_body("unavailable", "主存储不可用", request_id), &[]);
    for _ in 0..2 {
        let (mut leader, mut epoch, mut ok) = service.current_leader();
        if !ok {
            service.refresh_once();
            (leader, epoch, ok) = service.current_leader();
        }
        if !ok {
            return Err(last_error);
        }
        let payload = json!({"epoch": epoch, "caller_id": caller, "idempotency_key": key, "order": fields});
        let (status, body) = match store_call("POST", &format!("{leader}/internal/append"), request_id, if deadline == 0 { now_ms() + 800 } else { deadline }, Some(&payload)) {
            Err(()) => {
                service.metrics.inc("orange_errors_total", 1);
                log_event("deadline_exceeded", request_id, "info", &[]);
                return Err(json_response(504, &error_body("deadline_exceeded", "超过截止时间", request_id), &[]));
            }
            Ok(pair) => pair,
        };
        if status == 409 && body.as_ref().and_then(|v| v.get("error")).and_then(|v| v.get("code")).and_then(|v| v.as_str()) == Some("epoch_fenced") {
            log_event("write_fenced", request_id, "info", &[("epoch", body.as_ref().unwrap()["error"]["epoch"].clone())]);
            service.refresh_once();
            continue;
        }
        if status == 409 {
            return Err(json_response(409, &body.unwrap_or(Value::Null), &[]));
        }
        if status == 200 {
            if let Some(body) = &body {
                if body.get("order").is_some() {
                    if !body.get("created").and_then(|v| v.as_bool()).unwrap_or(false) {
                        log_event("order_replayed", request_id, "info", &[("order_id", body["order"]["order_id"].clone())]);
                    }
                    return Ok(body["order"].clone());
                }
            }
        }
        last_error = json_response(if status == 0 { 503 } else { status }, &body.unwrap_or_else(|| error_body("unavailable", "主存储不可用", request_id)), &[]);
        return Err(last_error);
    }
    Err(json_response(503, &error_body("unavailable", "主存储正在切换", request_id), &[]))
}

fn write_new(service: &Arc<OrderService>, caller: &str, key: &str, fields: &Value, request_id: &str, deadline: i64) -> Response {
    let started = common::monotonic();
    let sku = fields["sku"].as_str().unwrap_or("").to_string();
    let recommender = service.recommender.clone();
    let request = request_id.to_string();
    let breaker_service = service.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let text = ask_recommend(&breaker_service, &recommender, &sku, &request);
        let _ = tx.send(text);
    });
    let stored = match call_append(service, caller, key, fields, request_id, deadline) {
        Err(response) => return response,
        Ok(order) => order,
    };
    let remaining = (RECOMMEND_BUDGET_MS as f64 / 1000.0) - (common::monotonic() - started);
    let text = if remaining > 0.0 {
        rx.recv_timeout(Duration::from_secs_f64(remaining)).ok().flatten()
    } else {
        rx.try_recv().ok().flatten()
    };
    let Some(text) = text else {
        log_event(
            "order_accepted",
            request_id,
            "info",
            &[("order_id", stored["order_id"].clone()), ("epoch", stored["epoch"].clone()), ("degraded", json!(true))],
        );
        return json_response(201, &stored, &[]);
    };
    let (leader, epoch, ok) = service.current_leader();
    if !ok {
        return json_response(201, &stored, &[]);
    }
    let payload = json!({"order_id": stored["order_id"], "epoch": epoch, "recommendation": text, "degraded": false});
    let saved = store_call(
        "POST",
        &format!("{leader}/internal/snapshot"),
        request_id,
        if deadline == 0 { now_ms() + 200 } else { deadline },
        Some(&payload),
    );
    if let Ok((200, Some(body))) = saved {
        log_event("order_accepted", request_id, "info", &[("order_id", body["order_id"].clone()), ("epoch", body["epoch"].clone()), ("degraded", json!(false))]);
        return json_response(201, &body, &[]);
    }
    json_response(201, &stored, &[])
}

fn ask_recommend(service: &OrderService, recommender: &str, sku: &str, request_id: &str) -> Option<String> {
    if !service.breaker.allow() {
        log_event("breaker_open", request_id, "info", &[]);
        service.metrics.set("orange_breaker_open", 1);
        return None;
    }
    match http_util::http_request(
        "GET",
        &format!("{recommender}/internal/recommend?sku={}", http_util::encode(sku)),
        None,
        &[("X-Request-Id", request_id)],
        RECOMMEND_BUDGET_MS as f64 / 1000.0,
    ) {
        Ok((200, _, raw)) => {
            let body = http_util::decode_json(&raw).ok().flatten().unwrap_or(Value::Null);
            match body.get("recommendation").and_then(|v| v.as_str()) {
                Some(text) if !text.is_empty() => {
                    service.breaker.record(true);
                    service.metrics.set("orange_breaker_open", 0);
                    Some(text.to_string())
                }
                _ => {
                    service.breaker.record(false);
                    log_event("recommend_failed", request_id, "info", &[("status", json!(200))]);
                    None
                }
            }
        }
        Ok((status, _, _)) => {
            service.breaker.record(false);
            log_event("recommend_failed", request_id, "info", &[("status", json!(status))]);
            None
        }
        Err(err) => {
            service.breaker.record(false);
            log_event("recommend_failed", request_id, "info", &[("message", json!(err.to_string()))]);
            None
        }
    }
}

fn create_order(service: &Arc<OrderService>, headers: &HashMap<String, String>, raw: &[u8], request_id: &str) -> Response {
    let caller = headers.get("x-caller-id").map(|s| s.as_str()).unwrap_or("");
    let key = headers.get("idempotency-key").map(|s| s.as_str()).unwrap_or("");
    let deadline = headers.get("x-deadline-ms").and_then(|v| v.parse::<i64>().ok()).unwrap_or(0);
    if !valid_caller(caller) {
        return json_response(400, &error_body("invalid_argument", "X-Caller-Id 不合法", request_id), &[]);
    }
    if !valid_idempotency_key(key) {
        return json_response(400, &error_body("invalid_argument", "Idempotency-Key 不合法", request_id), &[]);
    }
    if deadline != 0 && now_ms() >= deadline {
        log_event("deadline_exceeded", request_id, "info", &[]);
        return json_response(504, &error_body("deadline_exceeded", "超过截止时间", request_id), &[]);
    }
    let parsed = match http_util::decode_json(raw) {
        Err(()) => return json_response(400, &error_body("invalid_argument", "正文不是 JSON", request_id), &[]),
        Ok(value) => value.unwrap_or(Value::Null),
    };
    let fields = match validate_order_body(&parsed) {
        Err(message) => return json_response(400, &error_body("invalid_argument", &message, request_id), &[]),
        Ok(fields) => fields,
    };
    service.metrics.inc("orange_inflight", 1);
    let response = if let Some(replay) = lookup_existing(service, caller, key, &fields, request_id, deadline) {
        replay
    } else {
        write_new(service, caller, key, &fields, request_id, deadline)
    };
    service.metrics.inc("orange_inflight", -1);
    response
}

fn handle(service: &Arc<OrderService>, method: String, path: String, headers: HashMap<String, String>, raw: Vec<u8>) -> Response {
    let request_id = header_request_id(&headers);
    let (route, query) = http_util::split_path(&path);
    if method == "GET" && route == "/health" {
        return empty_response(200);
    }
    if method == "GET" && route == "/ready" {
        return empty_response(if service.current_leader().2 { 200 } else { 503 });
    }
    if method == "GET" && route == "/metrics" {
        service.metrics.set("orange_breaker_open", if service.breaker.is_open() { 1 } else { 0 });
        return text_response(200, &service.metrics.render(&["orange_errors_total", "orange_inflight", "orange_breaker_open"]));
    }
    if method == "GET" && (route.starts_with("/internal/orders/") || route == "/internal/orders") {
        return proxy_read(service, &route, &query, &headers, &request_id);
    }
    if method == "POST" && route == "/internal/orders" {
        return create_order(service, &headers, &raw, &request_id);
    }
    json_response(404, &error_body("not_found", "没有这个路径", &request_id), &[])
}

pub fn main(args: &[String]) {
    let name = common::flag(args, "--process-name").unwrap_or("order");
    let port: u16 = common::flag(args, "--port").and_then(|v| v.parse().ok()).unwrap_or(8101);
    let supervisor = common::flag(args, "--supervisor").unwrap_or("http://127.0.0.1:8090");
    let recommender = common::flag(args, "--recommender").unwrap_or("http://127.0.0.1:8200");
    let root = common::flag_or(args, "--root", ".");
    common::set_context(name, root);
    common::install_signals();
    common::start_fault_reader();
    let service = OrderService::new(name, supervisor, recommender);
    let refresher = service.clone();
    thread::spawn(move || refresher.refresh_leader());
    let handler = service.clone();
    http_util::serve(port, move |method, path, headers, raw| handle(&handler, method, path, headers, raw));
    log_event("process_started", "", "info", &[("port", json!(port))]);
    let _ = service.name;
    http_util::block_until_stopped();
}
