//! 对照 `orange/recommender.py`。可失败的推荐旁路，不访问存储。

use crate::common::{self, error_body, fault_delay, header_request_id, log_event};
use crate::http_util::{self, empty_response, json_response, text_response, Response};
use serde_json::json;
use std::collections::HashMap;

fn recommendation_for(sku: &str) -> String {
    if sku == "gear-1" {
        "配套护具".to_string()
    } else {
        format!("相关:{sku}")
    }
}

fn handle(method: String, path: String, headers: HashMap<String, String>, _raw: Vec<u8>) -> Response {
    let request_id = header_request_id(&headers);
    let (route, query) = http_util::split_path(&path);
    if method == "GET" && route == "/health" {
        return empty_response(200);
    }
    if method == "GET" && route == "/ready" {
        return empty_response(200);
    }
    if method == "GET" && route == "/metrics" {
        return text_response(200, "orange_errors_total 0\n");
    }
    if method == "GET" && route == "/internal/recommend" {
        let sku = query.get("sku").cloned().unwrap_or_default();
        if sku.is_empty() {
            return json_response(400, &error_body("invalid_argument", "缺少 sku", &request_id), &[]);
        }
        log_event("recommend_served", &request_id, "info", &[("sku", json!(sku))]);
        fault_delay("recommender");
        return json_response(200, &json!({"recommendation": recommendation_for(&sku)}), &[]);
    }
    json_response(404, &error_body("not_found", "没有这个路径", &request_id), &[])
}

pub fn main(args: &[String]) {
    let port: u16 = common::flag(args, "--port").and_then(|v| v.parse().ok()).unwrap_or(8200);
    let root = common::flag_or(args, "--root", ".");
    let name = common::flag_or(args, "--process-name", "recommender");
    common::set_context(name, root);
    common::install_signals();
    common::start_fault_reader();
    http_util::serve(port, handle);
    log_event("process_started", "", "info", &[("port", json!(port))]);
    http_util::block_until_stopped();
}
