//! 对照 `tests/test_acceptance.py`。同一组 AC-1 到 AC-12，打在 Rust 监督者上。
//! 端口是固定的，所以这一组测试串行执行。不要和 Python 进程同时跑。

use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const GATEWAY: &str = "http://127.0.0.1:8080";
const STATUS: &str = "http://127.0.0.1:8090/v1/status";

static PORTS: Mutex<()> = Mutex::new(());

fn lock_ports() -> std::sync::MutexGuard<'static, ()> {
    PORTS.lock().unwrap_or_else(|err| err.into_inner())
}

fn http(method: &str, url: &str, body: Option<&Value>, headers: &[(&str, &str)], timeout: f64) -> (u16, HashMap<String, String>, Vec<u8>) {
    match http_result(method, url, body, headers, timeout) {
        Ok(value) => value,
        Err(_) => (0, HashMap::new(), Vec::new()),
    }
}

fn http_result(method: &str, url: &str, body: Option<&Value>, headers: &[(&str, &str)], timeout: f64) -> std::io::Result<(u16, HashMap<String, String>, Vec<u8>)> {
    let rest = url.strip_prefix("http://").expect("http");
    let (authority, path) = match rest.split_once('/') {
        Some((host, path)) => (host, format!("/{path}")),
        None => (rest, "/".to_string()),
    };
    let (host, port) = authority.rsplit_once(':').expect("port");
    let addr = format!("{host}:{port}");
    let mut stream = TcpStream::connect_timeout(&addr.parse().unwrap(), Duration::from_secs_f64(timeout.max(0.05)))?;
    stream.set_read_timeout(Some(Duration::from_secs_f64(timeout)))?;
    stream.set_write_timeout(Some(Duration::from_secs_f64(timeout)))?;
    let payload = body.map(|value| value.to_string().into_bytes()).unwrap_or_default();
    let mut request = format!("{method} {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n");
    if body.is_some() {
        request.push_str("Content-Type: application/json\r\n");
        request.push_str(&format!("Content-Length: {}\r\n", payload.len()));
    }
    for (key, value) in headers {
        request.push_str(&format!("{key}: {value}\r\n"));
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes())?;
    if body.is_some() {
        stream.write_all(&payload)?;
    }
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(n) => buffer.extend_from_slice(&chunk[..n]),
        }
    }
    let head_end = buffer.windows(4).position(|w| w == b"\r\n\r\n").unwrap_or(buffer.len());
    let head = String::from_utf8_lossy(&buffer[..head_end]).to_string();
    let mut lines = head.split("\r\n");
    let status = lines.next().unwrap_or("").split_whitespace().nth(1).unwrap_or("0").parse().unwrap_or(0);
    let mut headers_out = HashMap::new();
    for line in lines {
        if let Some((key, value)) = line.split_once(':') {
            headers_out.insert(key.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }
    let mut response = if head_end + 4 <= buffer.len() { buffer[head_end + 4..].to_vec() } else { Vec::new() };
    if let Some(length) = headers_out.get("content-length").and_then(|v| v.parse::<usize>().ok()) {
        response.truncate(length);
    }
    Ok((status, headers_out, response))
}

fn decode(raw: &[u8]) -> Option<Value> {
    if raw.is_empty() {
        return None;
    }
    match serde_json::from_slice(raw) {
        Ok(Value::Null) => None,
        Ok(value) => Some(value),
        Err(_) => None,
    }
}

fn post_order(sku: &str, qty: i64, key: &str, extra: Option<Value>) -> (u16, HashMap<String, String>, Option<Value>, String) {
    let mut body = json!({"sku": sku, "qty": qty});
    if let Some(Value::Object(map)) = extra {
        for (key, value) in map {
            body[key] = value;
        }
    }
    let request_id = format!("req-{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
    let (status, headers, raw) = http(
        "POST",
        &format!("{GATEWAY}/v1/orders"),
        Some(&body),
        &[("X-Caller-Id", "gear-card"), ("Idempotency-Key", key), ("X-Request-Id", &request_id)],
        3.0,
    );
    (status, headers, decode(&raw), request_id)
}

struct Fulfill {
    calls: Arc<Mutex<Vec<String>>>,
    applied: Arc<Mutex<Vec<String>>>,
    released: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    port: u16,
}

impl Fulfill {
    fn start(status: u16, delay: f64, hang_first: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let applied = Arc::new(Mutex::new(Vec::new()));
        let released = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let calls_t = calls.clone();
        let applied_t = applied.clone();
        let released_t = released.clone();
        let stop_t = stop.clone();
        std::thread::spawn(move || {
            listener.set_nonblocking(true).ok();
            while !stop_t.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let calls_t = calls_t.clone();
                        let applied_t = applied_t.clone();
                        let released_t = released_t.clone();
                        std::thread::spawn(move || {
                            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                            let mut buf = Vec::new();
                            let mut chunk = [0u8; 2048];
                            loop {
                                match stream.read(&mut chunk) {
                                    Ok(0) | Err(_) => break,
                                    Ok(n) => {
                                        buf.extend_from_slice(&chunk[..n]);
                                        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                                            break;
                                        }
                                    }
                                }
                            }
                            let head_end = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap_or(0);
                            let head = String::from_utf8_lossy(&buf[..head_end]);
                            let lowered = head.to_ascii_lowercase();
                            let length = lowered.lines().find_map(|line| line.strip_prefix("content-length:")).and_then(|v| v.trim().parse::<usize>().ok()).unwrap_or(0);
                            let mut body = if head_end + 4 <= buf.len() { buf[head_end + 4..].to_vec() } else { Vec::new() };
                            while body.len() < length {
                                match stream.read(&mut chunk) {
                                    Ok(0) | Err(_) => break,
                                    Ok(n) => body.extend_from_slice(&chunk[..n]),
                                }
                            }
                            let payload: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                            let order_id = payload["order_id"].as_str().unwrap_or("").to_string();
                            let first = {
                                let mut calls = calls_t.lock().unwrap();
                                calls.push(order_id.clone());
                                calls.len() == 1
                            };
                            if hang_first && first {
                                let start = Instant::now();
                                while !released_t.load(Ordering::SeqCst) && start.elapsed() < Duration::from_secs(30) {
                                    std::thread::sleep(Duration::from_millis(20));
                                }
                            } else if delay > 0.0 {
                                std::thread::sleep(Duration::from_secs_f64(delay));
                            }
                            let data = b"{}";
                            let response = format!("HTTP/1.1 {status} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", data.len());
                            let _ = stream.write_all(response.as_bytes());
                            let _ = stream.write_all(data);
                            if (200..300).contains(&status) || status == 409 {
                                let mut applied = applied_t.lock().unwrap();
                                if !applied.contains(&order_id) {
                                    applied.push(order_id);
                                }
                            }
                        });
                    }
                    Err(_) => std::thread::sleep(Duration::from_millis(10)),
                }
            }
        });
        Self { calls, applied, released, stop, port }
    }

    fn url(&self) -> String {
        format!("http://127.0.0.1:{}/hooks/orange-fulfill", self.port)
    }

    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
    }

    fn close(&self) {
        self.release();
        self.stop.store(true, Ordering::SeqCst);
    }
}

struct Cluster {
    root: String,
    child: Option<std::process::Child>,
}

impl Cluster {
    fn start() -> Self {
        let root = format!("/tmp/orange-rust-{}-{}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
        std::fs::create_dir_all(&root).unwrap();
        let bin = env!("CARGO_BIN_EXE_orange");
        let child = Command::new(bin)
            .arg("--root")
            .arg(&root)
            .stdin(Stdio::null())
            .process_group(0)
            .spawn()
            .expect("启动监督者");
        let mut cluster = Self { root, child: Some(child) };
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            if http("GET", &format!("{GATEWAY}/ready"), None, &[], 0.3).0 == 200 {
                return cluster;
            }
            if cluster.child.as_mut().unwrap().try_wait().ok().flatten().is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        panic!("{}", cluster.diagnostics());
    }

    fn status(&self) -> Value {
        let (_, _, raw) = http("GET", STATUS, None, &[], 1.0);
        decode(&raw).unwrap_or(Value::Null)
    }

    fn write_faults(&self, payload: &Value) {
        std::fs::write(format!("{}/runtime/faults.json", self.root), payload.to_string()).unwrap();
        std::thread::sleep(Duration::from_millis(500));
    }

    fn logs(&self, name: &str) -> String {
        std::fs::read_to_string(format!("{}/runtime/logs/{name}.log", self.root)).unwrap_or_default()
    }

    fn diagnostics(&self) -> String {
        let directory = format!("{}/runtime/logs", self.root);
        let mut chunks = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&directory) {
            let mut names: Vec<_> = entries.filter_map(|e| e.ok()).map(|e| e.path()).collect();
            names.sort();
            for path in names {
                let name = path.file_name().unwrap().to_string_lossy();
                chunks.push(format!("----- {name} -----\n{}", std::fs::read_to_string(&path).unwrap_or_default()));
            }
        }
        if chunks.is_empty() { "监督者没有留下日志".into() } else { chunks.join("\n") }
    }

    fn stop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let pid = child.id() as i32;
            unsafe {
                libc::killpg(pid, libc::SIGTERM);
            }
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                if child.try_wait().ok().flatten().is_some() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            if child.try_wait().ok().flatten().is_none() {
                unsafe {
                    libc::killpg(pid, libc::SIGKILL);
                }
                let _ = child.wait();
            }
        }
        std::thread::sleep(Duration::from_millis(300));
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.stop();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn faults(store_delay: i64, recommend_delay: i64, drop_a: bool) -> Value {
    json!({
        "delay_ms": {"store": store_delay, "recommender": recommend_delay},
        "pause_replication": false,
        "drop_heartbeat": {"store-a": drop_a, "store-b": false},
        "exit_after_requests": {}
    })
}

#[test]
fn ac1_same_key_returns_one_order() {
    let _guard = lock_ports();
    let cluster = Cluster::start();
    let (status, _, first, _) = post_order("gear-1", 1, "same-key-0001", None);
    assert_eq!(status, 201, "{first:?}\n{}", cluster.diagnostics());
    let (status, _, second, _) = post_order("gear-1", 1, "same-key-0001", None);
    assert_eq!(status, 201, "{second:?}");
    assert_eq!(first.unwrap()["order_id"], second.unwrap()["order_id"]);
}

#[test]
fn ac2_same_key_different_sku_conflicts() {
    let _guard = lock_ports();
    let cluster = Cluster::start();
    let (status, _, first, _) = post_order("gear-1", 1, "same-key-0002", None);
    assert_eq!(status, 201, "{first:?}\n{}", cluster.diagnostics());
    let first = first.unwrap();
    let (status, _, second, _) = post_order("gear-2", 1, "same-key-0002", None);
    assert_eq!(status, 409, "{second:?}");
    assert_eq!(second.unwrap()["error"]["code"], "idempotency_mismatch");
    let (got, _, raw) = http("GET", &format!("{GATEWAY}/v1/orders/{}", first["order_id"].as_str().unwrap()), None, &[("X-Caller-Id", "gear-card")], 2.0);
    assert_eq!(got, 200);
    assert_eq!(decode(&raw).unwrap()["sku"], "gear-1");
}

#[test]
fn ac3_stopped_order_replica_is_restarted() {
    let _guard = lock_ports();
    let cluster = Cluster::start();
    let before = cluster.status();
    let pid = before["pids"]["order-2"].as_u64().unwrap() as i32;
    unsafe { libc::kill(pid, libc::SIGTERM); }
    let mut saw_down = false;
    let mut restored = false;
    let deadline = Instant::now() + Duration::from_secs(8);
    while Instant::now() < deadline {
        let view = cluster.status();
        let ready = view["ready"]["order-2"].as_bool().unwrap_or(false);
        saw_down = saw_down || !ready;
        if ready && view["pids"]["order-2"].as_u64() != Some(pid as u64) {
            restored = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let (status, _, body, _) = post_order("gear-1", 1, "after-order-b", None);
    assert_eq!(status, 201, "{body:?}");
    assert!(saw_down, "{}", cluster.diagnostics());
    assert!(restored, "{}", cluster.diagnostics());
}

#[test]
fn ac4_stopped_recommender_degrades_order() {
    let _guard = lock_ports();
    let cluster = Cluster::start();
    let pid = cluster.status()["pids"]["recommender"].as_u64().unwrap() as i32;
    unsafe { libc::kill(pid, libc::SIGTERM); }
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut status = 0;
    let mut body = None;
    while Instant::now() < deadline {
        let (got, _, parsed, _) = post_order("gear-1", 1, &format!("degrade-{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()), None);
        status = got;
        body = parsed;
        if status == 201 && body.as_ref().map(|v| v["degraded"] == true && v["recommendation"].is_null()).unwrap_or(false) {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(status, 201, "{body:?}\n{}", cluster.diagnostics());
    assert_eq!(body.unwrap()["degraded"], true);
}

#[test]
fn ac5_breaker_stops_calling_recommender() {
    let _guard = lock_ports();
    let cluster = Cluster::start();
    cluster.write_faults(&faults(0, 500, false));
    for index in 0..12 {
        let (status, _, body, _) = post_order("gear-1", 1, &format!("breaker-{index:02}-key"), None);
        assert_eq!(status, 201, "{body:?}\n{}", cluster.diagnostics());
    }
    std::thread::sleep(Duration::from_millis(300));
    let before = cluster.logs("recommender").matches("\"event\":\"recommend_served\"").count();
    let (status, _, body, _) = post_order("gear-1", 1, "breaker-after-open", None);
    assert_eq!(status, 201, "{body:?}");
    std::thread::sleep(Duration::from_millis(300));
    let after = cluster.logs("recommender").matches("\"event\":\"recommend_served\"").count();
    assert_eq!(before, after, "{}", cluster.logs("recommender"));
    let orders = cluster.logs("order-1") + &cluster.logs("order-2");
    assert!(orders.contains("\"event\":\"breaker_open\""), "{orders}");
}

#[test]
fn ac6_empty_bucket_returns_retry_after() {
    let _guard = lock_ports();
    let cluster = Cluster::start();
    cluster.write_faults(&faults(350, 0, false));
    let holders = Arc::new(Mutex::new(Vec::new()));
    let mut threads = Vec::new();
    for index in 0..8 {
        let holders = holders.clone();
        threads.push(std::thread::spawn(move || {
            let result = post_order("gear-1", 1, &format!("hold-{index:02}-xxxx"), None);
            holders.lock().unwrap().push(result);
        }));
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let (_, _, raw) = http("GET", &format!("{GATEWAY}/metrics"), None, &[], 1.0);
        if String::from_utf8_lossy(&raw).contains("orange_inflight 8") {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let mut results = Vec::new();
    for index in 0..40 {
        results.push(post_order("gear-1", 1, &format!("bucket-{index:02}-xx"), None));
    }
    for thread in threads {
        thread.join().ok();
    }
    let limited: Vec<_> = results.iter().filter(|item| item.0 == 429 && item.2.as_ref().and_then(|v| v["error"]["message"].as_str()) == Some("令牌桶已空")).collect();
    assert!(!limited.is_empty(), "{results:?}\n{}", cluster.diagnostics());
    assert!(limited[0].1.contains_key("retry-after"), "{:?}", limited[0].1);
}

#[test]
fn ac7_claimed_job_is_retried_once() {
    let _guard = lock_ports();
    let cluster = Cluster::start();
    let fulfill = Fulfill::start(200, 0.0, true);
    let (status, _, body, _) = post_order("gear-1", 1, "visibility-timeout", Some(json!({"fulfill_url": fulfill.url()})));
    assert_eq!(status, 201, "{body:?}\n{}", cluster.diagnostics());
    let body = body.unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && fulfill.calls.lock().unwrap().is_empty() {
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(!fulfill.calls.lock().unwrap().is_empty(), "{}", cluster.diagnostics());
    let worker_pid = cluster.status()["pids"]["worker-1"].as_u64().unwrap() as i32;
    unsafe { libc::kill(worker_pid, libc::SIGTERM); }
    fulfill.release();
    let deadline = Instant::now() + Duration::from_secs(25);
    let mut state = String::new();
    while Instant::now() < deadline {
        let (got, _, raw) = http("GET", &format!("{GATEWAY}/v1/orders/{}", body["order_id"].as_str().unwrap()), None, &[("X-Caller-Id", "gear-card")], 2.0);
        assert_eq!(got, 200, "{}", cluster.diagnostics());
        state = decode(&raw).unwrap()["state"].as_str().unwrap_or("").to_string();
        if state == "fulfilled" {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert_eq!(state, "fulfilled", "{}", cluster.diagnostics());
    assert_eq!(fulfill.applied.lock().unwrap().len(), 1);
    fulfill.close();
}

#[test]
fn ac8_repeated_fulfill_failure_goes_dead() {
    let _guard = lock_ports();
    let cluster = Cluster::start();
    let fulfill = Fulfill::start(500, 0.0, false);
    let (status, _, body, _) = post_order("gear-1", 1, "dead-letter-0001", Some(json!({"fulfill_url": fulfill.url()})));
    assert_eq!(status, 201, "{body:?}");
    let body = body.unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut state = String::new();
    while Instant::now() < deadline {
        let (_, _, raw) = http("GET", &format!("{GATEWAY}/v1/orders/{}", body["order_id"].as_str().unwrap()), None, &[("X-Caller-Id", "gear-card")], 2.0);
        let view = decode(&raw).unwrap_or(Value::Null);
        state = view["state"].as_str().unwrap_or("").to_string();
        if state == "fulfillment_failed" {
            assert_eq!(view["fulfillment"]["status"], "dead");
            assert!(view["fulfillment"]["attempts"].as_i64().unwrap_or(0) >= 5);
            fulfill.close();
            return;
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    fulfill.close();
    panic!("{}\nstate={state}", cluster.diagnostics());
}

#[test]
fn ac9_leader_heartbeat_loss_promotes_follower() {
    let _guard = lock_ports();
    let cluster = Cluster::start();
    let original = cluster.status()["leader"].as_str().unwrap().to_string();
    cluster.write_faults(&faults(0, 0, true));
    let deadline = Instant::now() + Duration::from_secs(8);
    let mut promoted = false;
    while Instant::now() < deadline {
        let view = cluster.status();
        if view["leader"].as_str() != Some(original.as_str()) && view["epoch"].as_i64().unwrap_or(1) > 1 {
            promoted = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(promoted, "{}", cluster.diagnostics());
    cluster.write_faults(&faults(0, 0, false));
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut fenced = false;
    while Instant::now() < deadline {
        let (status, _, raw) = http(
            "POST",
            &format!("{original}/internal/append"),
            Some(&json!({"epoch": 1, "caller_id": "gear-card", "idempotency_key": "fenced-write-01", "order": {"sku": "gear-1", "qty": 1, "external_ref": null, "fulfill_url": null}})),
            &[],
            1.0,
        );
        if status == 409 && decode(&raw).and_then(|v| v["error"]["code"].as_str().map(|s| s == "epoch_fenced")).unwrap_or(false) {
            fenced = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(fenced, "{}", cluster.diagnostics());
    let (status, _, body, _) = post_order("gear-1", 1, "after-promotion", None);
    assert_eq!(status, 201, "{body:?}\n{}", cluster.diagnostics());
    assert!(body.unwrap()["epoch"].as_i64().unwrap_or(0) > 1);
}

#[test]
fn ac10_pending_limit_rejects_new_orders() {
    let _guard = lock_ports();
    let cluster = Cluster::start();
    let fulfill = Fulfill::start(500, 0.9, false);
    let mut limited = false;
    let mut first = None;
    for index in 0..70 {
        let (status, _, body, _) = post_order("gear-1", 1, &format!("pile-{index:02}-order"), Some(json!({"fulfill_url": fulfill.url()})));
        if status == 201 {
            if first.is_none() {
                first = body;
            }
            continue;
        }
        let message = body.as_ref().and_then(|v| v["error"]["message"].as_str()).unwrap_or("");
        if status == 429 && message == "待处理任务已达上限" {
            limited = true;
            break;
        }
        if status == 429 && message == "令牌桶已空" {
            std::thread::sleep(Duration::from_millis(200));
            continue;
        }
        panic!("意外响应 {status} {body:?}\n{}", cluster.diagnostics());
    }
    assert!(limited, "{}", cluster.diagnostics());
    assert!(first.is_some());
    let before = cluster.logs("store-a").matches("\"event\":\"job_claimed\"").count();
    let deadline = Instant::now() + Duration::from_secs(8);
    let mut progressed = false;
    while Instant::now() < deadline {
        if cluster.logs("store-a").matches("\"event\":\"job_claimed\"").count() > before {
            progressed = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    assert!(progressed, "{}", cluster.diagnostics());
    fulfill.close();
}

#[test]
fn ac11_backlog_scales_workers_without_flapping() {
    let _guard = lock_ports();
    let cluster = Cluster::start();
    let fulfill = Fulfill::start(500, 0.9, false);
    for index in 0..40 {
        loop {
            let (status, _, body, _) = post_order("gear-1", 1, &format!("scale-{index:02}-order"), Some(json!({"fulfill_url": fulfill.url()})));
            if status == 429 && body.as_ref().and_then(|v| v["error"]["message"].as_str()) == Some("令牌桶已空") {
                std::thread::sleep(Duration::from_millis(200));
                continue;
            }
            assert_eq!(status, 201, "{body:?}\n{}", cluster.diagnostics());
            break;
        }
    }
    let deadline = Instant::now() + Duration::from_secs(40);
    while Instant::now() < deadline {
        if cluster.status()["workers"].as_i64().unwrap_or(0) >= 3 {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(cluster.status()["workers"].as_i64().unwrap_or(0) >= 3, "{}", cluster.diagnostics());
    std::thread::sleep(Duration::from_secs(10));
    assert_eq!(cluster.status()["workers"].as_i64().unwrap_or(0), 3, "{}", cluster.diagnostics());
    fulfill.close();
}

#[test]
fn ac12_request_id_spans_gateway_order_and_store() {
    let _guard = lock_ports();
    let cluster = Cluster::start();
    cluster.write_faults(&faults(200, 0, false));
    let request_id = "req-trace-ac12-0001";
    let (status, _, raw) = http(
        "POST",
        &format!("{GATEWAY}/v1/orders"),
        Some(&json!({"sku": "gear-1", "qty": 1})),
        &[("X-Caller-Id", "gear-card"), ("Idempotency-Key", "trace-request-001"), ("X-Request-Id", request_id)],
        3.0,
    );
    assert_eq!(status, 201, "{}", String::from_utf8_lossy(&raw));
    std::thread::sleep(Duration::from_millis(300));
    let gateway = cluster.logs("gateway");
    let orders = cluster.logs("order-1") + &cluster.logs("order-2");
    let stores = cluster.logs("store-a") + &cluster.logs("store-b");
    assert!(gateway.contains(request_id), "{gateway}");
    assert!(orders.contains(request_id), "{orders}");
    assert!(stores.contains(request_id), "{stores}");
}
