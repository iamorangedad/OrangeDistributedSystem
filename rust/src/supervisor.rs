//! 对照 `orange/supervisor.py`。拉起进程、公布主存储、提升从存储，并按积压加减工人。

use crate::common::{self, error_body, header_request_id, log_event, HEARTBEAT_SEC, LEADER_LOST_SEC, PROMOTION_PAUSE_SEC, SCALE_COOLDOWN_SEC, SCALE_IN_HOLD_SEC, SCALE_OUT_HOLD_SEC, SCALE_OUT_PENDING, WORKER_MAX, WORKER_MIN};
use crate::http_util::{self, empty_response, json_response, Response};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const STORE_A: &str = "http://127.0.0.1:8301";
const STORE_B: &str = "http://127.0.0.1:8302";
const SUPERVISOR_URL: &str = "http://127.0.0.1:8090";

struct Inner {
    leader: String,
    epoch: i64,
    jobs_pending: i64,
    gateway_paused: bool,
    last_promotion_error: Option<String>,
    applied: HashMap<String, i64>,
    last_beat: HashMap<String, f64>,
    seen: HashSet<String>,
    ready: HashMap<String, bool>,
    pids: HashMap<String, u32>,
    last_error: HashMap<String, String>,
    expected: Vec<String>,
    worker_count: i64,
    high_since: Option<f64>,
    zero_since: Option<f64>,
    last_scale: f64,
    promoting: bool,
    armed: bool,
}

pub struct Supervisor {
    root: String,
    inner: Mutex<Inner>,
    procs: Mutex<HashMap<String, Child>>,
}

impl Supervisor {
    fn new(root: &str) -> Self {
        let mut applied = HashMap::new();
        applied.insert("store-a".into(), 0);
        applied.insert("store-b".into(), 0);
        Self {
            root: root.to_string(),
            inner: Mutex::new(Inner {
                leader: STORE_A.into(),
                epoch: 1,
                jobs_pending: 0,
                gateway_paused: false,
                last_promotion_error: None,
                applied,
                last_beat: HashMap::new(),
                seen: HashSet::new(),
                ready: HashMap::new(),
                pids: HashMap::new(),
                last_error: HashMap::new(),
                expected: ["gateway", "order-1", "order-2", "recommender", "store-a", "store-b", "worker-1"]
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
                worker_count: 1,
                high_since: None,
                zero_since: None,
                last_scale: 0.0,
                promoting: false,
                armed: false,
            }),
            procs: Mutex::new(HashMap::new()),
        }
    }

    fn command(&self, name: &str) -> Vec<String> {
        let exe = std::env::current_exe().expect("当前程序").display().to_string();
        let common = vec!["--root".to_string(), self.root.clone()];
        let data = format!("{}/runtime/data", self.root);
        let mut args = match name {
            "store-a" => vec![
                exe, "store".into(), "--node-id".into(), "store-a".into(), "--port".into(), "8301".into(), "--role".into(), "leader".into(),
                "--peer".into(), STORE_B.into(), "--data".into(), format!("{data}/store-a.sqlite"), "--supervisor".into(), SUPERVISOR_URL.into(),
            ],
            "store-b" => vec![
                exe, "store".into(), "--node-id".into(), "store-b".into(), "--port".into(), "8302".into(), "--role".into(), "follower".into(),
                "--peer".into(), STORE_A.into(), "--data".into(), format!("{data}/store-b.sqlite"), "--supervisor".into(), SUPERVISOR_URL.into(),
            ],
            "gateway" => vec![
                exe, "gateway".into(), "--port".into(), "8080".into(), "--supervisor".into(), SUPERVISOR_URL.into(),
                "--orders".into(), "http://127.0.0.1:8101,http://127.0.0.1:8102".into(), "--process-name".into(), "gateway".into(),
            ],
            "order-1" => order_cmd(&exe, "order-1", 8101),
            "order-2" => order_cmd(&exe, "order-2", 8102),
            "recommender" => vec![exe, "recommender".into(), "--port".into(), "8200".into(), "--process-name".into(), "recommender".into()],
            other if other.starts_with("worker-") => {
                let index: u16 = other.split('-').nth(1).and_then(|v| v.parse().ok()).unwrap_or(1);
                vec![
                    exe, "worker".into(), "--process-name".into(), other.into(), "--port".into(), (8400 + index).to_string(),
                    "--supervisor".into(), SUPERVISOR_URL.into(),
                ]
            }
            other => panic!("未知进程 {other}"),
        };
        args.extend(common);
        args
    }

    fn spawn(&self, name: &str) {
        {
            let mut procs = self.procs.lock().expect("procs");
            if let Some(child) = procs.get_mut(name) {
                if child.try_wait().ok().flatten().is_none() {
                    return;
                }
            }
        }
        let args = self.command(name);
        let mut command = Command::new(&args[0]);
        command.args(&args[1..]).env("ORANGE_ROOT", &self.root).env("ORANGE_PROCESS", name);
        match command.spawn() {
            Ok(child) => {
                let pid = child.id();
                self.procs.lock().expect("procs").insert(name.to_string(), child);
                self.inner.lock().expect("sup").pids.insert(name.to_string(), pid);
                log_event("spawned", "", "info", &[("process_name", json!(name)), ("pid", json!(pid))]);
            }
            Err(err) => {
                self.inner.lock().expect("sup").last_error.insert(name.to_string(), err.to_string());
            }
        }
    }

    fn stop_process(&self, name: &str) {
        let mut procs = self.procs.lock().expect("procs");
        if let Some(child) = procs.get_mut(name) {
            if child.try_wait().ok().flatten().is_none() {
                let pid = child.id() as i32;
                unsafe {
                    libc::kill(pid, libc::SIGTERM);
                }
                let started = Instant::now();
                loop {
                    if child.try_wait().ok().flatten().is_some() {
                        break;
                    }
                    if started.elapsed() > Duration::from_secs(2) {
                        let _ = child.kill();
                        let _ = child.wait();
                        break;
                    }
                    thread::sleep(Duration::from_millis(20));
                }
            }
        }
        drop(procs);
        self.inner.lock().expect("sup").ready.insert(name.to_string(), false);
    }

    fn ensure(&self) {
        let names = self.inner.lock().expect("sup").expected.clone();
        for name in names {
            let mut restart = false;
            {
                let mut procs = self.procs.lock().expect("procs");
                if let Some(child) = procs.get_mut(&name) {
                    if let Ok(Some(status)) = child.try_wait() {
                        let code = status.code().unwrap_or(-1);
                        self.inner.lock().expect("sup").last_error.insert(name.clone(), format!("退出码 {code}"));
                        self.inner.lock().expect("sup").ready.insert(name.clone(), false);
                        log_event("process_exited", "", "error", &[("process_name", json!(name)), ("code", json!(code))]);
                        procs.remove(&name);
                        restart = true;
                    }
                } else {
                    restart = true;
                }
            }
            if restart {
                if self.inner.lock().expect("sup").last_error.contains_key(&name) {
                    thread::sleep(Duration::from_millis(500));
                }
                self.spawn(&name);
            }
        }
    }

    fn probe_ready(&self) {
        let names = self.inner.lock().expect("sup").expected.clone();
        for name in names {
            let Some(port) = port_of(&name) else { continue };
            let ready = http_util::http_request("GET", &format!("http://127.0.0.1:{port}/ready"), None, &[], 0.3)
                .map(|(status, _, _)| status == 200)
                .unwrap_or(false);
            self.inner.lock().expect("sup").ready.insert(name, ready);
        }
    }

    fn note_heartbeat(&self, body: &Value) {
        let node_id = body.get("node_id").and_then(|v| v.as_str()).unwrap_or("");
        if node_id != "store-a" && node_id != "store-b" {
            return;
        }
        let mut inner = self.inner.lock().expect("sup");
        inner.last_beat.insert(node_id.to_string(), common::monotonic());
        inner.seen.insert(node_id.to_string());
        inner.applied.insert(node_id.to_string(), body.get("applied_index").and_then(|v| v.as_i64()).unwrap_or(0));
        let role = body.get("role").and_then(|v| v.as_str()).unwrap_or("");
        let epoch = body.get("epoch").and_then(|v| v.as_i64()).unwrap_or(0);
        if role == "leader" && epoch >= inner.epoch {
            inner.epoch = epoch;
            inner.leader = if node_id == "store-a" { STORE_A } else { STORE_B }.to_string();
            inner.jobs_pending = body.get("jobs_pending").and_then(|v| v.as_i64()).unwrap_or(0);
        }
        if inner.seen.len() >= 2 {
            inner.armed = true;
        }
    }

    fn maybe_promote(&self) {
        let plan = {
            let mut inner = self.inner.lock().expect("sup");
            if !inner.armed || inner.promoting {
                return;
            }
            let now = common::monotonic();
            let leader_id = if inner.leader == STORE_A { "store-a" } else { "store-b" };
            let follower_id = if leader_id == "store-a" { "store-b" } else { "store-a" };
            let (Some(leader_seen), Some(follower_seen)) = (inner.last_beat.get(leader_id).copied(), inner.last_beat.get(follower_id).copied()) else {
                return;
            };
            if now - leader_seen <= LEADER_LOST_SEC || now - follower_seen > LEADER_LOST_SEC {
                return;
            }
            inner.promoting = true;
            inner.gateway_paused = true;
            let follower_url = if follower_id == "store-b" { STORE_B } else { STORE_A };
            Some((follower_url.to_string(), inner.epoch + 1, inner.leader.clone()))
        };
        let Some((follower_url, new_epoch, old_leader)) = plan else {
            return;
        };
        let started = common::monotonic();
        let result = http_util::http_request(
            "POST",
            &format!("{follower_url}/internal/promote"),
            Some(&json!({"new_epoch": new_epoch, "peer": old_leader})),
            &[],
            PROMOTION_PAUSE_SEC,
        );
        let mut inner = self.inner.lock().expect("sup");
        match result {
            Ok((200, _, raw)) => match http_util::decode_json(&raw) {
                Ok(Some(body)) => {
                    inner.epoch = body.get("epoch").and_then(|v| v.as_i64()).unwrap_or(new_epoch);
                    inner.leader = follower_url;
                    inner.last_promotion_error = None;
                    let epoch = inner.epoch;
                    let leader = inner.leader.clone();
                    log_event("promoted", "", "info", &[("epoch", json!(epoch)), ("leader", json!(leader))]);
                }
                _ => {
                    inner.last_promotion_error = Some(format!("提升失败 HTTP 200"));
                    log_event("promoted", "", "error", &[("message", json!(inner.last_promotion_error.clone().unwrap_or_default()))]);
                }
            },
            Ok((status, _, _)) => {
                inner.last_promotion_error = Some(format!("提升失败 HTTP {status}"));
                log_event("promoted", "", "error", &[("message", json!(inner.last_promotion_error.clone().unwrap_or_default()))]);
            }
            Err(err) => {
                inner.last_promotion_error = Some(err.to_string());
                log_event("promoted", "", "error", &[("message", json!(err.to_string()))]);
            }
        }
        if common::monotonic() - started > PROMOTION_PAUSE_SEC && inner.last_promotion_error.is_none() {
            inner.last_promotion_error = Some("切换超过 2 秒".into());
        }
        inner.gateway_paused = false;
        inner.promoting = false;
    }

    fn maybe_scale(&self) {
        let action = {
            let mut inner = self.inner.lock().expect("sup");
            let now = common::monotonic();
            let pending = inner.jobs_pending;
            let mut action = None;
            if pending > SCALE_OUT_PENDING {
                inner.zero_since = None;
                if inner.high_since.is_none() {
                    inner.high_since = Some(now);
                }
                let cooled = now - inner.last_scale >= SCALE_COOLDOWN_SEC;
                if inner.high_since.map(|t| now - t >= SCALE_OUT_HOLD_SEC).unwrap_or(false) && cooled && inner.worker_count < WORKER_MAX {
                    inner.worker_count += 1;
                    let name = format!("worker-{}", inner.worker_count);
                    if !inner.expected.contains(&name) {
                        inner.expected.push(name.clone());
                    }
                    inner.last_scale = now;
                    inner.high_since = None;
                    log_event("scaled", "", "info", &[("workers", json!(inner.worker_count)), ("direction", json!("out"))]);
                    action = Some(("out", name));
                }
            } else {
                inner.high_since = None;
            }
            if pending == 0 {
                if inner.zero_since.is_none() {
                    inner.zero_since = Some(now);
                }
                let cooled = now - inner.last_scale >= SCALE_COOLDOWN_SEC;
                if inner.zero_since.map(|t| now - t >= SCALE_IN_HOLD_SEC).unwrap_or(false) && cooled && inner.worker_count > WORKER_MIN {
                    let name = format!("worker-{}", inner.worker_count);
                    inner.expected.retain(|item| item != &name);
                    inner.worker_count -= 1;
                    inner.last_scale = now;
                    inner.zero_since = None;
                    log_event("scaled", "", "info", &[("workers", json!(inner.worker_count)), ("direction", json!("in"))]);
                    action = Some(("in", name));
                }
            } else {
                inner.zero_since = None;
            }
            action
        };
        match action {
            Some(("out", name)) => self.spawn(&name),
            Some(("in", name)) => self.stop_process(&name),
            _ => {}
        }
    }

    fn status(&self) -> Value {
        let inner = self.inner.lock().expect("sup");
        let mut ready = serde_json::Map::new();
        for name in &inner.expected {
            ready.insert(name.clone(), json!(inner.ready.get(name).copied().unwrap_or(false)));
        }
        json!({
            "leader": inner.leader,
            "epoch": inner.epoch,
            "applied_index": inner.applied,
            "workers": inner.worker_count,
            "jobs_pending": inner.jobs_pending,
            "gateway_paused": inner.gateway_paused,
            "ready": ready,
            "last_promotion_error": inner.last_promotion_error,
            "pids": inner.pids,
            "last_error": inner.last_error,
        })
    }

    fn run(self: &Arc<Self>) {
        while !common::stop_requested() {
            self.ensure();
            self.probe_ready();
            self.maybe_promote();
            self.maybe_scale();
            thread::sleep(Duration::from_secs_f64(HEARTBEAT_SEC));
        }
        let names: Vec<String> = self.procs.lock().expect("procs").keys().cloned().collect();
        for name in names {
            self.stop_process(&name);
        }
    }
}

fn order_cmd(exe: &str, name: &str, port: u16) -> Vec<String> {
    vec![
        exe.into(), "order".into(), "--process-name".into(), name.into(), "--port".into(), port.to_string(),
        "--supervisor".into(), SUPERVISOR_URL.into(), "--recommender".into(), "http://127.0.0.1:8200".into(),
    ]
}

fn port_of(name: &str) -> Option<u16> {
    Some(match name {
        "gateway" => 8080,
        "order-1" => 8101,
        "order-2" => 8102,
        "recommender" => 8200,
        "store-a" => 8301,
        "store-b" => 8302,
        "worker-1" => 8401,
        "worker-2" => 8402,
        "worker-3" => 8403,
        _ => return None,
    })
}

fn handle(supervisor: &Supervisor, method: String, path: String, headers: HashMap<String, String>, raw: Vec<u8>) -> Response {
    let request_id = header_request_id(&headers);
    let (route, _) = http_util::split_path(&path);
    if method == "GET" && route == "/health" {
        return empty_response(200);
    }
    if method == "GET" && route == "/v1/status" {
        return json_response(200, &supervisor.status(), &[]);
    }
    if method == "GET" && route == "/internal/leader" {
        let inner = supervisor.inner.lock().expect("sup");
        return json_response(200, &json!({"leader": inner.leader, "epoch": inner.epoch}), &[]);
    }
    if method == "GET" && route == "/internal/gateway-control" {
        let inner = supervisor.inner.lock().expect("sup");
        return json_response(200, &json!({"paused": inner.gateway_paused, "jobs_pending": inner.jobs_pending}), &[]);
    }
    if method == "POST" && route == "/internal/heartbeat" {
        let body = http_util::decode_json(&raw).ok().flatten().unwrap_or(Value::Null);
        supervisor.note_heartbeat(&body);
        let inner = supervisor.inner.lock().expect("sup");
        return json_response(200, &json!({"leader": inner.leader, "epoch": inner.epoch}), &[]);
    }
    json_response(404, &error_body("not_found", "没有这个路径", &request_id), &[])
}

pub fn main(args: &[String]) {
    let root = common::flag_or(args, "--root", ".").to_string();
    common::set_context("supervisor", &root);
    common::install_signals();
    let _ = fs::create_dir_all(format!("{root}/runtime/data"));
    let _ = fs::create_dir_all(format!("{root}/runtime/logs"));
    let faults = format!("{root}/runtime/faults.json");
    if !std::path::Path::new(&faults).exists() {
        let _ = fs::write(
            &faults,
            "{\"delay_ms\":{\"store\":0,\"recommender\":0},\"pause_replication\":false,\"drop_heartbeat\":{\"store-a\":false,\"store-b\":false},\"exit_after_requests\":{}}\n",
        );
    }
    common::start_fault_reader();
    let supervisor = Arc::new(Supervisor::new(&root));
    let handler = supervisor.clone();
    http_util::serve(8090, move |method, path, headers, raw| handle(&handler, method, path, headers, raw));
    log_event("process_started", "", "info", &[("port", json!(8090))]);
    supervisor.run();
}
