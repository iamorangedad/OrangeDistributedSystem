//! 对照 `orange/gateway.py`。限流、截止时间，并把请求分给就绪的订单副本。

use crate::common::{self, error_body, header_request_id, log_event, now_ms, valid_caller, valid_idempotency_key, validate_order_body, Metrics, TokenBucket, GATEWAY_INFLIGHT_LIMIT, JOBS_PENDING_LIMIT, ORDER_DEADLINE_MS};
use crate::http_util::{self, empty_response, json_response, text_response, Response};
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

struct Gate {
    ready: HashMap<String, bool>,
    paused: bool,
    jobs_pending: i64,
    inflight: i64,
    cursor: usize,
}

pub struct Gateway {
    supervisor: String,
    orders: Vec<String>,
    bucket: TokenBucket,
    metrics: Metrics,
    gate: Mutex<Gate>,
}

impl Gateway {
    fn poll(self: &Arc<Self>) {
        while !common::stop_requested() {
            for url in &self.orders {
                let ready = http_util::http_request("GET", &format!("{url}/ready"), None, &[], 0.3).map(|(status, _, _)| status == 200).unwrap_or(false);
                self.gate.lock().expect("gate").ready.insert(url.clone(), ready);
            }
            if let Ok((200, _, raw)) = http_util::http_request("GET", &format!("{}/internal/gateway-control", self.supervisor), None, &[], 0.3) {
                if let Ok(Some(body)) = http_util::decode_json(&raw) {
                    let mut gate = self.gate.lock().expect("gate");
                    gate.paused = body.get("paused").and_then(|v| v.as_bool()).unwrap_or(false);
                    gate.jobs_pending = body.get("jobs_pending").and_then(|v| v.as_i64()).unwrap_or(0);
                }
            }
            thread::sleep(Duration::from_millis(200));
        }
    }

    fn pick(&self) -> Option<String> {
        let mut gate = self.gate.lock().expect("gate");
        if gate.paused {
            return None;
        }
        let ready: Vec<String> = self.orders.iter().filter(|url| gate.ready.get(*url).copied().unwrap_or(false)).cloned().collect();
        if ready.is_empty() {
            return None;
        }
        let chosen = ready[gate.cursor % ready.len()].clone();
        gate.cursor += 1;
        Some(chosen)
    }

    fn ready(&self) -> bool {
        let gate = self.gate.lock().expect("gate");
        !gate.paused && gate.ready.values().any(|ready| *ready)
    }
}

fn reject(gateway: &Gateway, status: u16, code: &str, message: &str, request_id: &str, retry_after: Option<i64>) -> Response {
    gateway.metrics.inc("orange_errors_total", 1);
    if code == "rate_limited" {
        log_event("rate_limited", request_id, "info", &[("message", json!(message))]);
    }
    let mut extra = Vec::new();
    let retry = retry_after.map(|v| v.to_string()).unwrap_or_default();
    if retry_after.is_some() {
        extra.push(("Retry-After", retry));
    }
    json_response(status, &error_body(code, message, request_id), &extra)
}

fn create(gateway: &Gateway, headers: &HashMap<String, String>, raw: &[u8], request_id: &str) -> Response {
    let caller = headers.get("x-caller-id").map(|s| s.as_str()).unwrap_or("");
    let key = headers.get("idempotency-key").map(|s| s.as_str()).unwrap_or("");
    if !valid_caller(caller) {
        return json_response(400, &error_body("invalid_argument", "X-Caller-Id 不合法", request_id), &[]);
    }
    if !valid_idempotency_key(key) {
        return json_response(400, &error_body("invalid_argument", "Idempotency-Key 不合法", request_id), &[]);
    }
    let parsed = match http_util::decode_json(raw) {
        Err(()) => return json_response(400, &error_body("invalid_argument", "正文不是 JSON", request_id), &[]),
        Ok(value) => value.unwrap_or(serde_json::Value::Null),
    };
    let fields = match validate_order_body(&parsed) {
        Err(message) => return json_response(400, &error_body("invalid_argument", &message, request_id), &[]),
        Ok(fields) => fields,
    };
    let (allowed, retry_after) = gateway.bucket.take();
    if !allowed {
        return reject(gateway, 429, "rate_limited", "令牌桶已空", request_id, Some(retry_after));
    }
    let (pending, paused) = {
        let gate = gateway.gate.lock().expect("gate");
        (gate.jobs_pending, gate.paused)
    };
    if pending >= JOBS_PENDING_LIMIT {
        return reject(gateway, 429, "rate_limited", "待处理任务已达上限", request_id, Some(1));
    }
    {
        let mut gate = gateway.gate.lock().expect("gate");
        if paused {
            return reject(gateway, 503, "unavailable", "主存储正在切换", request_id, None);
        }
        if gate.inflight >= GATEWAY_INFLIGHT_LIMIT {
            return reject(gateway, 429, "rate_limited", "在途请求已满", request_id, Some(1));
        }
        gate.inflight += 1;
    }
    gateway.metrics.set("orange_inflight", gateway.gate.lock().expect("gate").inflight);
    let deadline = now_ms() + ORDER_DEADLINE_MS;
    let deadline_text = deadline.to_string();
    let forward_headers = [("X-Caller-Id", caller), ("Idempotency-Key", key), ("X-Request-Id", request_id), ("X-Deadline-Ms", deadline_text.as_str())];
    let mut response = None;
    for _ in 0..gateway.orders.len() {
        let Some(target) = gateway.pick() else {
            response = Some(reject(gateway, 503, "unavailable", "没有就绪的订单副本", request_id, None));
            break;
        };
        match http_util::http_request("POST", &format!("{target}/internal/orders"), Some(&fields), &forward_headers, (ORDER_DEADLINE_MS as f64 / 1000.0) + 0.2) {
            Err(_) => {
                gateway.metrics.inc("orange_errors_total", 1);
                continue;
            }
            Ok((status, response_headers, body)) => {
                let mut extra = vec![("X-Request-Id", request_id.to_string())];
                if let Some(retry) = response_headers.get("retry-after") {
                    extra.push(("Retry-After", retry.clone()));
                }
                let content_type = response_headers.get("content-type").cloned().unwrap_or_else(|| "application/json".into());
                extra.push(("Content-Type", content_type));
                log_event("gateway_forward", request_id, "info", &[("status", json!(status))]);
                response = Some(Response { status, headers: extra.into_iter().map(|(k, v)| (k.to_string(), v)).collect(), body });
                break;
            }
        }
    }
    let response = response.unwrap_or_else(|| reject(gateway, 503, "unavailable", "没有就绪的订单副本", request_id, None));
    {
        let mut gate = gateway.gate.lock().expect("gate");
        gate.inflight -= 1;
        gateway.metrics.set("orange_inflight", gate.inflight);
    }
    response
}

fn forward_get(gateway: &Gateway, route: &str, query: &HashMap<String, String>, headers: &HashMap<String, String>, request_id: &str) -> Response {
    let caller = headers.get("x-caller-id").map(|s| s.as_str()).unwrap_or("");
    if !valid_caller(caller) {
        return json_response(400, &error_body("invalid_argument", "X-Caller-Id 不合法", request_id), &[]);
    }
    if route == "/internal/orders" && query.get("idempotency_key").is_none_or(|v| v.is_empty()) {
        return json_response(400, &error_body("invalid_argument", "缺少 idempotency_key", request_id), &[]);
    }
    let Some(target) = gateway.pick() else {
        return json_response(503, &error_body("unavailable", "没有就绪的订单副本", request_id), &[]);
    };
    let query_text = if query.is_empty() {
        String::new()
    } else {
        let mut parts = Vec::new();
        for (key, value) in query {
            parts.push(format!("{}={}", http_util::encode(key), http_util::encode(value)));
        }
        format!("?{}", parts.join("&"))
    };
    let deadline = (now_ms() + ORDER_DEADLINE_MS).to_string();
    match http_util::http_request(
        "GET",
        &format!("{target}{route}{query_text}"),
        None,
        &[("X-Caller-Id", caller), ("X-Request-Id", request_id), ("X-Deadline-Ms", deadline.as_str())],
        1.0,
    ) {
        Err(_) => json_response(503, &error_body("unavailable", "没有就绪的订单副本", request_id), &[]),
        Ok((status, _, body)) => Response {
            status,
            headers: vec![
                ("Content-Type".into(), "application/json; charset=utf-8".into()),
                ("X-Request-Id".into(), request_id.into()),
            ],
            body,
        },
    }
}

fn handle(gateway: &Gateway, method: String, path: String, headers: HashMap<String, String>, raw: Vec<u8>) -> Response {
    let request_id = header_request_id(&headers);
    let (route, query) = http_util::split_path(&path);
    if method == "GET" && route == "/health" {
        return empty_response(200);
    }
    if method == "GET" && route == "/ready" {
        return empty_response(if gateway.ready() { 200 } else { 503 });
    }
    if method == "GET" && route == "/metrics" {
        gateway.metrics.set("orange_inflight", gateway.gate.lock().expect("gate").inflight);
        return text_response(200, &gateway.metrics.render(&["orange_errors_total", "orange_inflight"]));
    }
    if method == "POST" && route == "/v1/orders" {
        return create(gateway, &headers, &raw, &request_id);
    }
    if method == "GET" && route == "/v1/orders" {
        return forward_get(gateway, "/internal/orders", &query, &headers, &request_id);
    }
    if method == "GET" && route.starts_with("/v1/orders/") {
        let order_id = &route["/v1/orders/".len()..];
        return forward_get(gateway, &format!("/internal/orders/{order_id}"), &HashMap::new(), &headers, &request_id);
    }
    json_response(404, &error_body("not_found", "没有这个路径", &request_id), &[])
}

pub fn main(args: &[String]) {
    let port: u16 = common::flag(args, "--port").and_then(|v| v.parse().ok()).unwrap_or(8080);
    let supervisor = common::flag(args, "--supervisor").unwrap_or("http://127.0.0.1:8090").to_string();
    let orders: Vec<String> = common::flag(args, "--orders").unwrap_or("").split(',').filter(|item| !item.is_empty()).map(|s| s.to_string()).collect();
    let root = common::flag_or(args, "--root", ".");
    let name = common::flag_or(args, "--process-name", "gateway");
    common::set_context(name, root);
    common::install_signals();
    common::start_fault_reader();
    let gateway = Arc::new(Gateway {
        supervisor,
        orders,
        bucket: TokenBucket::new(),
        metrics: Metrics::new(),
        gate: Mutex::new(Gate { ready: HashMap::new(), paused: false, jobs_pending: 0, inflight: 0, cursor: 0 }),
    });
    let poller = gateway.clone();
    thread::spawn(move || poller.poll());
    let handler = gateway.clone();
    http_util::serve(port, move |method, path, headers, raw| handle(&handler, method, path, headers, raw));
    log_event("process_started", "", "info", &[("port", json!(port))]);
    http_util::block_until_stopped();
}
