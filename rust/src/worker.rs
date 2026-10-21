//! 对照 `orange/worker.py`。工人只向当前主存储领取履约任务。

use crate::common::{self, header_request_id, log_event, new_id, FULFILL_TIMEOUT_SEC};
use crate::http_util::{self, empty_response, text_response, Response};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

struct View {
    leader: String,
    checked_at: f64,
}

pub struct Worker {
    name: String,
    supervisor: String,
    view: Mutex<View>,
    errors: Mutex<i64>,
}

impl Worker {
    fn refresh(self: &Arc<Self>) {
        while !common::stop_requested() {
            if let Ok((200, _, raw)) = http_util::http_request("GET", &format!("{}/internal/leader", self.supervisor), None, &[], 0.3) {
                if let Ok(Some(body)) = http_util::decode_json(&raw) {
                    let mut view = self.view.lock().expect("leader");
                    view.leader = body.get("leader").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    view.checked_at = common::monotonic();
                }
            }
            thread::sleep(Duration::from_millis(200));
        }
    }

    fn leader_now(&self) -> String {
        let view = self.view.lock().expect("leader");
        if !view.leader.is_empty() && common::monotonic() - view.checked_at < 1.0 {
            view.leader.clone()
        } else {
            String::new()
        }
    }

    fn run(self: &Arc<Self>) {
        while !common::stop_requested() {
            let leader = self.leader_now();
            if leader.is_empty() {
                thread::sleep(Duration::from_millis(200));
                continue;
            }
            let request_id = new_id("req_");
            let claimed = http_util::http_request(
                "POST",
                &format!("{leader}/internal/jobs/claim"),
                Some(&json!({"worker_id": self.name})),
                &[("X-Request-Id", request_id.as_str())],
                1.0,
            );
            let (status, _, raw) = match claimed {
                Err(_) => {
                    *self.errors.lock().expect("errors") += 1;
                    thread::sleep(Duration::from_millis(200));
                    continue;
                }
                Ok(value) => value,
            };
            if status == 204 {
                thread::sleep(Duration::from_millis(200));
                continue;
            }
            if status != 200 || raw.is_empty() {
                *self.errors.lock().expect("errors") += 1;
                thread::sleep(Duration::from_millis(200));
                continue;
            }
            let Ok(Some(job)) = http_util::decode_json(&raw) else {
                *self.errors.lock().expect("errors") += 1;
                continue;
            };
            let (ok, error) = fulfill(&job, &request_id);
            let path = if ok { "/ack" } else { "/fail" };
            let body = if ok { None } else { Some(json!({"retryable": true, "error": error})) };
            let job_id = job["job_id"].as_str().unwrap_or("");
            if http_util::http_request("POST", &format!("{leader}/internal/jobs/{job_id}{path}"), body.as_ref(), &[("X-Request-Id", request_id.as_str())], 1.0).is_err() {
                *self.errors.lock().expect("errors") += 1;
            }
        }
    }
}

fn fulfill(job: &Value, request_id: &str) -> (bool, String) {
    let url = job.get("fulfill_url").and_then(|v| v.as_str()).unwrap_or("");
    if url.is_empty() {
        return (true, String::new());
    }
    let payload = json!({
        "event": "fulfill",
        "order_id": job["order_id"],
        "caller_id": job["caller_id"],
        "external_ref": job.get("external_ref").cloned().unwrap_or(Value::Null),
        "sku": job["sku"],
        "qty": job["qty"],
    });
    match http_util::http_request("POST", url, Some(&payload), &[("X-Request-Id", request_id)], FULFILL_TIMEOUT_SEC) {
        Err(err) => (false, err.to_string()),
        Ok((status, _, _)) if (200..300).contains(&status) || status == 409 => (true, String::new()),
        Ok((status, _, _)) => (false, format!("履约地址返回 {status}")),
    }
}

fn handle(worker: &Worker, method: String, path: String, _headers: HashMap<String, String>, _raw: Vec<u8>) -> Response {
    let _ = header_request_id(&_headers);
    let (route, _) = http_util::split_path(&path);
    if method == "GET" && route == "/health" {
        return empty_response(200);
    }
    if method == "GET" && route == "/ready" {
        return empty_response(if worker.leader_now().is_empty() { 503 } else { 200 });
    }
    if method == "GET" && route == "/metrics" {
        return text_response(200, &format!("orange_errors_total {}\n", worker.errors.lock().expect("errors")));
    }
    empty_response(404)
}

pub fn main(args: &[String]) {
    let name = common::flag(args, "--process-name").unwrap_or("worker-1").to_string();
    let port: u16 = common::flag(args, "--port").and_then(|v| v.parse().ok()).unwrap_or(8401);
    let supervisor = common::flag(args, "--supervisor").unwrap_or("http://127.0.0.1:8090").to_string();
    let root = common::flag_or(args, "--root", ".");
    common::set_context(&name, root);
    common::install_signals();
    common::start_fault_reader();
    let worker = Arc::new(Worker {
        name,
        supervisor,
        view: Mutex::new(View { leader: String::new(), checked_at: 0.0 }),
        errors: Mutex::new(0),
    });
    let refresher = worker.clone();
    let runner = worker.clone();
    thread::spawn(move || refresher.refresh());
    thread::spawn(move || runner.run());
    let handler = worker.clone();
    http_util::serve(port, move |method, path, headers, raw| handle(&handler, method, path, headers, raw));
    log_event("process_started", "", "info", &[("port", json!(port))]);
    http_util::block_until_stopped();
}
