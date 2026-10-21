//! 对照 `orange/common.py`。参数、令牌桶、熔断、日志和故障文件。

use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const TOKEN_CAPACITY: i64 = 30;
pub const TOKEN_REFILL_PER_SEC: f64 = 10.0;
pub const GATEWAY_INFLIGHT_LIMIT: i64 = 8;
pub const JOBS_PENDING_LIMIT: i64 = 50;
pub const ORDER_DEADLINE_MS: i64 = 800;
pub const STORE_CALL_CAP_MS: i64 = 400;
pub const RECOMMEND_BUDGET_MS: i64 = 80;
pub const BREAKER_WINDOW_SEC: f64 = 10.0;
pub const BREAKER_FAILURES: usize = 5;
pub const BREAKER_OPEN_SEC: f64 = 5.0;
pub const VISIBILITY_MS: i64 = 15_000;
pub const FULFILL_TIMEOUT_SEC: f64 = 1.0;
pub const JOB_MAX_ATTEMPTS: i64 = 5;
pub const BACKOFF_BASE_MS: i64 = 100;
pub const BACKOFF_CAP_MS: i64 = 5_000;
pub const HEARTBEAT_SEC: f64 = 0.2;
pub const LEADER_LOST_SEC: f64 = 1.0;
pub const SCALE_OUT_PENDING: i64 = 10;
pub const SCALE_OUT_HOLD_SEC: f64 = 5.0;
pub const SCALE_IN_HOLD_SEC: f64 = 20.0;
pub const SCALE_COOLDOWN_SEC: f64 = 15.0;
pub const WORKER_MIN: i64 = 1;
pub const WORKER_MAX: i64 = 3;
pub const PROMOTION_PAUSE_SEC: f64 = 2.0;
pub const FAULT_POLL_SEC: f64 = 0.2;

static PROCESS: Mutex<String> = Mutex::new(String::new());
static ROOT: Mutex<String> = Mutex::new(String::new());
static FAULTS: Mutex<Value> = Mutex::new(Value::Null);
static FAULT_ERROR_LOGGED: AtomicBool = AtomicBool::new(false);
static REQUEST_COUNT: AtomicU64 = AtomicU64::new(0);
static STOP: AtomicBool = AtomicBool::new(false);
static RNG: AtomicU64 = AtomicU64::new(0x1234_5678_9abc_def0);

pub fn set_context(process: &str, root: &str) {
    *PROCESS.lock().expect("process") = process.to_string();
    *ROOT.lock().expect("root") = root.to_string();
}

pub fn process_name() -> String {
    PROCESS.lock().expect("process").clone()
}

pub fn root_dir() -> String {
    let value = ROOT.lock().expect("root").clone();
    if value.is_empty() {
        std::env::current_dir().map(|p| p.display().to_string()).unwrap_or_else(|_| ".".into())
    } else {
        value
    }
}

pub fn stop_requested() -> bool {
    STOP.load(Ordering::SeqCst)
}

#[allow(dead_code)]
pub fn request_stop() {
    STOP.store(true, Ordering::SeqCst);
}

extern "C" fn on_signal(_: libc::c_int) {
    STOP.store(true, Ordering::SeqCst);
}

pub fn install_signals() {
    unsafe {
        libc::signal(libc::SIGTERM, on_signal as *const () as libc::sighandler_t);
        libc::signal(libc::SIGINT, on_signal as *const () as libc::sighandler_t);
    }
}

pub fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

pub fn monotonic() -> f64 {
    static START: OnceLock<std::time::Instant> = OnceLock::new();
    START.get_or_init(std::time::Instant::now).elapsed().as_secs_f64()
}

pub fn iso_now() -> String {
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    let days = secs / 86_400;
    let tod = secs % 86_400;
    let (year, month, day) = civil_from_days(days as i64);
    format!("{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z", tod / 3600, (tod % 3600) / 60, tod % 60)
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    (year, m as i64, d as i64)
}

pub fn new_id(prefix: &str) -> String {
    let mut bytes = [0u8; 16];
    let tick = RNG.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed) ^ monotonic().to_bits();
    bytes[..8].copy_from_slice(&tick.to_le_bytes());
    let tick2 = RNG.fetch_add(1, Ordering::Relaxed) ^ now_ms() as u64;
    bytes[8..].copy_from_slice(&tick2.to_le_bytes());
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!("{prefix}{hex}")
}

pub fn log_event(event: &str, request_id: &str, level: &str, fields: &[(&str, Value)]) {
    let mut map = Map::new();
    map.insert("ts".into(), json!(iso_now()));
    map.insert("level".into(), json!(level));
    map.insert("process".into(), json!(process_name()));
    map.insert("event".into(), json!(event));
    map.insert("request_id".into(), json!(request_id));
    for (key, value) in fields {
        map.insert((*key).to_string(), value.clone());
    }
    let line = Value::Object(map).to_string();
    println!("{line}");
    let _ = std::io::stdout().flush();
    let directory = format!("{}/runtime/logs", root_dir());
    let _ = fs::create_dir_all(&directory);
    let path = format!("{directory}/{}.log", process_name());
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{line}");
    }
}

fn faults_path() -> String {
    format!("{}/runtime/faults.json", root_dir())
}

pub fn current_faults() -> Value {
    FAULTS.lock().expect("faults").clone()
}

fn load_faults_once() {
    let path = faults_path();
    match fs::read_to_string(&path) {
        Ok(text) => match serde_json::from_str::<Value>(&text) {
            Ok(Value::Object(_)) => {
                *FAULTS.lock().expect("faults") = serde_json::from_str(&text).unwrap_or(Value::Null);
                FAULT_ERROR_LOGGED.store(false, Ordering::SeqCst);
            }
            _ => note_fault_error("故障文件必须是对象"),
        },
        Err(_) => note_fault_error("故障文件不存在"),
    }
}

fn note_fault_error(message: &str) {
    *FAULTS.lock().expect("faults") = json!({});
    if !FAULT_ERROR_LOGGED.swap(true, Ordering::SeqCst) {
        log_event("faults_invalid", "", "error", &[("message", json!(message))]);
    }
}

pub fn start_fault_reader() {
    load_faults_once();
    thread::spawn(|| loop {
        thread::sleep(Duration::from_secs_f64(FAULT_POLL_SEC));
        if stop_requested() {
            break;
        }
        load_faults_once();
    });
}

pub fn fault_delay(kind: &str) {
    let faults = current_faults();
    let ms = faults.get("delay_ms").and_then(|v| v.get(kind)).and_then(|v| v.as_i64()).unwrap_or(0);
    if ms > 0 {
        thread::sleep(Duration::from_millis(ms as u64));
    }
}

pub fn pause_replication() -> bool {
    current_faults().get("pause_replication").and_then(|v| v.as_bool()).unwrap_or(false)
}

pub fn drop_heartbeat(node_id: &str) -> bool {
    current_faults()
        .get("drop_heartbeat")
        .and_then(|v| v.get(node_id))
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

pub fn note_http_request() {
    let limits = current_faults().get("exit_after_requests").cloned().unwrap_or(Value::Null);
    let limit = limits.get(process_name()).and_then(|v| v.as_i64()).unwrap_or(0);
    if limit <= 0 {
        return;
    }
    let count = REQUEST_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
    if count >= limit as u64 {
        log_event("exit_after_requests", "", "info", &[("limit", json!(limit))]);
        std::process::exit(0);
    }
}

pub struct TokenBucket {
    capacity: f64,
    rate: f64,
    tokens: Mutex<(f64, f64)>,
}

impl TokenBucket {
    pub fn new() -> Self {
        Self {
            capacity: TOKEN_CAPACITY as f64,
            rate: TOKEN_REFILL_PER_SEC,
            tokens: Mutex::new((TOKEN_CAPACITY as f64, monotonic())),
        }
    }

    pub fn take(&self) -> (bool, i64) {
        let mut guard = self.tokens.lock().expect("bucket");
        let now = monotonic();
        let filled = (guard.0 + (now - guard.1) * self.rate).min(self.capacity);
        guard.1 = now;
        if filled >= 1.0 {
            guard.0 = filled - 1.0;
            return (true, 0);
        }
        guard.0 = filled;
        if self.rate <= 0.0 {
            return (false, 1);
        }
        let retry = ((1.0 - filled) / self.rate).ceil().max(1.0) as i64;
        (false, retry)
    }
}

pub struct CircuitBreaker {
    inner: Mutex<BreakerInner>,
}

struct BreakerInner {
    failures: Vec<f64>,
    state: &'static str,
    open_until: f64,
}

impl CircuitBreaker {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(BreakerInner { failures: Vec::new(), state: "closed", open_until: 0.0 }),
        }
    }

    pub fn is_open(&self) -> bool {
        let guard = self.inner.lock().expect("breaker");
        guard.state == "open" && monotonic() < guard.open_until
    }

    pub fn allow(&self) -> bool {
        let mut guard = self.inner.lock().expect("breaker");
        let now = monotonic();
        if guard.state == "open" {
            if now < guard.open_until {
                return false;
            }
            guard.state = "half";
            return true;
        }
        if guard.state == "half" {
            return false;
        }
        true
    }

    pub fn record(&self, success: bool) {
        let mut guard = self.inner.lock().expect("breaker");
        let now = monotonic();
        if guard.state == "half" {
            if success {
                guard.state = "closed";
                guard.failures.clear();
            } else {
                guard.state = "open";
                guard.open_until = now + BREAKER_OPEN_SEC;
            }
            return;
        }
        if success {
            return;
        }
        guard.failures.retain(|item| now - item <= BREAKER_WINDOW_SEC);
        guard.failures.push(now);
        if guard.failures.len() >= BREAKER_FAILURES {
            guard.state = "open";
            guard.open_until = now + BREAKER_OPEN_SEC;
            guard.failures.clear();
        }
    }
}

pub fn backoff_ms(attempt: i64) -> i64 {
    let cap = BACKOFF_CAP_MS.min(BACKOFF_BASE_MS * (1_i64 << (attempt - 1).max(0).min(16)));
    if cap <= 0 {
        return 0;
    }
    (rnd() % (cap as u64 + 1)) as i64
}

fn rnd() -> u64 {
    let mut x = RNG.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

pub fn canonical_body(sku: &str, qty: i64, external_ref: &str, fulfill_url: &str) -> String {
    format!(
        "{{\"external_ref\":{},\"fulfill_url\":{},\"qty\":{},\"sku\":{}}}",
        serde_json::to_string(external_ref).unwrap_or_else(|_| "\"\"".into()),
        serde_json::to_string(fulfill_url).unwrap_or_else(|_| "\"\"".into()),
        qty,
        serde_json::to_string(sku).unwrap_or_else(|_| "\"\"".into()),
    )
}

pub fn valid_caller(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 32
        && value.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

pub fn valid_idempotency_key(value: &str) -> bool {
    (8..=128).contains(&value.len()) && value.chars().all(|c| (32..=126).contains(&(c as u32)))
}

pub fn valid_fulfill_url(value: Option<&str>) -> bool {
    let Some(value) = value.filter(|v| !v.is_empty()) else {
        return true;
    };
    let Some(rest) = value.strip_prefix("http://127.0.0.1:") else {
        return false;
    };
    if value.contains('@') {
        return false;
    }
    let (port, path) = rest.split_once('/').unwrap_or((rest, ""));
    !port.is_empty() && port.chars().all(|c| c.is_ascii_digit()) && port != "0" && !path.contains(' ')
}

pub fn validate_order_body(data: &Value) -> Result<Value, String> {
    let Some(obj) = data.as_object() else {
        return Err("正文必须是 JSON 对象".into());
    };
    let sku = obj.get("sku").and_then(|v| v.as_str()).unwrap_or("");
    if !(1..=64).contains(&sku.len()) {
        return Err("sku 必须是 1 到 64 个字符".into());
    }
    if obj.get("qty").is_some_and(|v| v.is_boolean()) {
        return Err("qty 必须是 1 到 100 的整数".into());
    }
    let qty = obj.get("qty").and_then(|v| v.as_i64()).unwrap_or(0);
    if !(1..=100).contains(&qty) {
        return Err("qty 必须是 1 到 100 的整数".into());
    }
    let external_ref = obj.get("external_ref");
    if let Some(value) = external_ref {
        if !value.is_null() && (value.as_str().is_none_or(|s| s.len() > 128)) {
            return Err("external_ref 最长 128 个字符".into());
        }
    }
    let fulfill = obj.get("fulfill_url");
    if let Some(value) = fulfill {
        if !value.is_null() && value.as_str().is_none() {
            return Err("fulfill_url 必须是字符串".into());
        }
        if !valid_fulfill_url(value.as_str()) {
            return Err("fulfill_url 只能是 http://127.0.0.1 上的地址".into());
        }
    }
    Ok(json!({
        "sku": sku,
        "qty": qty,
        "external_ref": external_ref.filter(|v| !v.is_null()).cloned().unwrap_or(Value::Null),
        "fulfill_url": fulfill.filter(|v| !v.is_null()).cloned().unwrap_or(Value::Null),
    }))
}

pub fn error_body(code: &str, message: &str, request_id: &str) -> Value {
    json!({"error": {"code": code, "message": message, "request_id": request_id}})
}

pub fn header_request_id(headers: &HashMap<String, String>) -> String {
    headers.get("x-request-id").filter(|v| !v.is_empty()).cloned().unwrap_or_else(|| new_id("req_"))
}

pub fn flag<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.iter().position(|item| item == name).and_then(|index| args.get(index + 1)).map(|s| s.as_str())
}

pub fn flag_or<'a>(args: &'a [String], name: &str, default: &'a str) -> &'a str {
    flag(args, name).unwrap_or(default)
}

pub struct Metrics {
    values: Mutex<HashMap<String, i64>>,
}

impl Metrics {
    pub fn new() -> Self {
        Self { values: Mutex::new(HashMap::new()) }
    }

    pub fn inc(&self, name: &str, amount: i64) {
        let mut values = self.values.lock().expect("metrics");
        *values.entry(name.into()).or_insert(0) += amount;
    }

    pub fn set(&self, name: &str, value: i64) {
        self.values.lock().expect("metrics").insert(name.into(), value);
    }

    pub fn render(&self, names: &[&str]) -> String {
        let values = self.values.lock().expect("metrics");
        let mut lines = Vec::new();
        for name in names {
            lines.push(format!("{name} {}", values.get(*name).copied().unwrap_or(0)));
        }
        lines.join("\n") + "\n"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_body_sorts_keys() {
        let body = canonical_body("gear-1", 1, "cart", "");
        assert_eq!(body, r#"{"external_ref":"cart","fulfill_url":"","qty":1,"sku":"gear-1"}"#);
    }

    #[test]
    fn qty_rejects_bool() {
        let err = validate_order_body(&json!({"sku": "gear-1", "qty": true})).unwrap_err();
        assert!(err.contains("qty"));
    }

    #[test]
    fn fulfill_url_stays_on_loopback() {
        assert!(valid_fulfill_url(Some("http://127.0.0.1:9000/hooks")));
        assert!(!valid_fulfill_url(Some("https://127.0.0.1:9000/hooks")));
        assert!(!valid_fulfill_url(Some("http://127.0.0.1/hooks")));
    }

    #[test]
    fn token_bucket_rejects_when_empty() {
        let bucket = TokenBucket { capacity: 1.0, rate: 0.0, tokens: Mutex::new((1.0, monotonic())) };
        assert!(bucket.take().0);
        assert!(!bucket.take().0);
    }

    #[test]
    fn breaker_opens_after_five_failures() {
        let breaker = CircuitBreaker::new();
        for _ in 0..5 {
            assert!(breaker.allow());
            breaker.record(false);
        }
        assert!(!breaker.allow());
    }
}
