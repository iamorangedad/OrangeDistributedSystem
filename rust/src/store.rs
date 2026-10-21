//! 对照 `orange/store.py`。订单、任务和复制日志在同一个 sqlite 事务里提交。

use crate::common::{self, backoff_ms, canonical_body, drop_heartbeat, error_body, fault_delay, header_request_id, iso_now, log_event, new_id, now_ms, pause_replication, JOB_MAX_ATTEMPTS, VISIBILITY_MS};
use crate::http_util::{self, empty_response, json_response, text_response, Response};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    caller_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    external_ref TEXT,
    sku TEXT NOT NULL,
    qty INTEGER NOT NULL,
    fulfill_url TEXT,
    state TEXT NOT NULL,
    recommendation TEXT,
    degraded INTEGER NOT NULL,
    epoch INTEGER NOT NULL,
    log_index INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    response_json TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    UNIQUE (caller_id, idempotency_key)
);
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    visible_after INTEGER NOT NULL,
    worker_id TEXT,
    last_error TEXT,
    log_index INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS replication_log (
    log_index INTEGER PRIMARY KEY,
    epoch INTEGER NOT NULL,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL,
    committed_at TEXT NOT NULL
);
";

struct State {
    conn: Connection,
    node_id: String,
    peer: String,
    follower_applied: i64,
    replication_started: bool,
    metrics_errors: i64,
    metrics_pending: i64,
    metrics_lag: i64,
}

pub struct Store {
    state: Mutex<State>,
    supervisor: String,
    stop: AtomicBool,
}

struct OrderRow {
    response_json: String,
    request_hash: String,
    caller_id: String,
    external_ref: Option<String>,
    sku: String,
    qty: i64,
    fulfill_url: Option<String>,
    log_index: i64,
    order_id: String,
}

#[allow(dead_code)]
struct JobRow {
    job_id: String,
    order_id: String,
    state: String,
    attempts: i64,
    visible_after: i64,
    worker_id: Option<String>,
    last_error: Option<String>,
}

fn begin(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE")
}

fn commit(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch("COMMIT")
}

fn rollback(conn: &Connection) {
    let _ = conn.execute_batch("ROLLBACK");
}

impl State {
    fn open(node_id: &str, role: &str, peer: &str, path: &str) -> Self {
        if let Some(parent) = Path::new(path).parent() {
            let _ = fs::create_dir_all(parent);
        }
        let conn = Connection::open(path).expect("打开 sqlite");
        let _ = conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get::<_, String>(0));
        conn.execute_batch("PRAGMA synchronous=FULL").expect("synchronous");
        conn.execute_batch(SCHEMA).expect("schema");
        let state = Self {
            conn,
            node_id: node_id.to_string(),
            peer: peer.to_string(),
            follower_applied: 0,
            replication_started: false,
            metrics_errors: 0,
            metrics_pending: 0,
            metrics_lag: 0,
        };
        state.ensure_meta(role);
        state
    }

    fn ensure_meta(&self, role: &str) {
        let count: i64 = self.conn.query_row("SELECT COUNT(*) FROM meta", [], |row| row.get(0)).unwrap_or(0);
        if count > 0 {
            return;
        }
        for (key, value) in [("role", role), ("epoch", "1"), ("applied_index", "0"), ("node_id", self.node_id.as_str())] {
            self.conn.execute("INSERT INTO meta(key, value) VALUES (?1, ?2)", params![key, value]).expect("meta");
        }
    }

    fn meta(&self, key: &str) -> String {
        self.conn
            .query_row("SELECT value FROM meta WHERE key = ?1", params![key], |row| row.get(0))
            .unwrap_or_default()
    }

    fn set_meta(&self, key: &str, value: &str) {
        self.conn
            .execute(
                "INSERT INTO meta(key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                params![key, value],
            )
            .expect("set meta");
    }

    fn role(&self) -> String {
        self.meta("role")
    }

    fn epoch(&self) -> i64 {
        self.meta("epoch").parse().unwrap_or(1)
    }

    fn applied(&self) -> i64 {
        self.meta("applied_index").parse().unwrap_or(0)
    }

    fn is_leader(&self) -> bool {
        self.role() == "leader"
    }

    fn refresh_metrics(&mut self) {
        self.metrics_pending = self
            .conn
            .query_row("SELECT COUNT(*) FROM jobs WHERE state = 'pending'", [], |row| row.get(0))
            .unwrap_or(0);
        if self.is_leader() {
            self.metrics_lag = (self.applied() - self.follower_applied).max(0);
        } else {
            self.metrics_lag = 0;
        }
    }

    fn append_log(&self, kind: &str, payload: &Value) -> i64 {
        let index = self.applied() + 1;
        self.conn
            .execute(
                "INSERT INTO replication_log(log_index, epoch, kind, payload, committed_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![index, self.epoch(), kind, payload.to_string(), iso_now()],
            )
            .expect("append log");
        self.set_meta("applied_index", &index.to_string());
        index
    }

    fn order(&self, order_id: &str) -> Option<OrderRow> {
        self.conn
            .query_row("SELECT * FROM orders WHERE order_id = ?1", params![order_id], |row| Ok(read_order(row)))
            .ok()
    }

    fn job_for_order(&self, order_id: &str) -> Option<JobRow> {
        self.conn
            .query_row("SELECT * FROM jobs WHERE order_id = ?1", params![order_id], |row| Ok(read_job(row)))
            .ok()
    }

    fn by_key(&self, caller_id: &str, key: &str) -> Option<OrderRow> {
        self.conn
            .query_row(
                "SELECT * FROM orders WHERE caller_id = ?1 AND idempotency_key = ?2",
                params![caller_id, key],
                |row| Ok(read_order(row)),
            )
            .ok()
    }

    fn write_snapshot_row(&self, order_id: &str, body: &Value) {
        let degraded = if body.get("degraded").and_then(|v| v.as_bool()).unwrap_or(false) { 1 } else { 0 };
        let recommendation = body.get("recommendation").and_then(|v| v.as_str());
        self.conn
            .execute(
                "UPDATE orders SET response_json = ?1, state = ?2, recommendation = ?3, degraded = ?4 WHERE order_id = ?5",
                params![body.to_string(), body["state"].as_str().unwrap_or(""), recommendation, degraded, order_id],
            )
            .expect("snapshot");
    }

    fn require_leader(&mut self, request_id: &str) -> Option<Response> {
        if self.is_leader() {
            return None;
        }
        self.metrics_errors += 1;
        log_event("write_fenced", request_id, "info", &[("epoch", json!(self.epoch()))]);
        Some(json_response(
            409,
            &json!({"error": {"code": "epoch_fenced", "message": "当前节点不接受写入", "request_id": request_id, "epoch": self.epoch()}}),
            &[],
        ))
    }

    fn fenced(&mut self, request_id: &str, expected: i64) -> Response {
        self.metrics_errors += 1;
        log_event("write_fenced", request_id, "info", &[("epoch", json!(self.epoch())), ("expected", json!(expected))]);
        json_response(
            409,
            &json!({"error": {"code": "epoch_fenced", "message": "写入代际已过期", "request_id": request_id, "epoch": self.epoch()}}),
            &[],
        )
    }

    fn release_expired(&mut self) {
        let now = now_ms();
        let rows: Vec<JobRow> = self
            .conn
            .prepare("SELECT * FROM jobs WHERE state = 'inflight' AND visible_after <= ?1")
            .and_then(|mut stmt| stmt.query_map(params![now], |row| Ok(read_job(row)))?.collect())
            .unwrap_or_default();
        for row in rows {
            begin(&self.conn).expect("begin");
            let result = (|| -> rusqlite::Result<()> {
                self.conn.execute(
                    "UPDATE jobs SET state = 'pending', worker_id = NULL WHERE job_id = ?1 AND state = 'inflight'",
                    params![row.job_id],
                )?;
                if let (Some(order), Some(job)) = (self.order(&row.order_id), self.job_for_order(&row.order_id)) {
                    let mut body: Value = serde_json::from_str(&order.response_json).unwrap_or(Value::Null);
                    body["state"] = json!("accepted");
                    body["fulfillment"] = fulfillment_of(&job);
                    self.write_snapshot_row(&order.order_id, &body);
                    self.append_log("job_reset", &json!({"job_id": row.job_id, "order_id": row.order_id, "response": body}));
                }
                Ok(())
            })();
            match result {
                Ok(()) => commit(&self.conn).expect("commit"),
                Err(_) => rollback(&self.conn),
            }
            log_event(
                "job_retry",
                "",
                "info",
                &[("order_id", json!(row.order_id)), ("job_id", json!(row.job_id)), ("reason", json!("visibility_timeout"))],
            );
        }
    }

    fn entries_after(&self, index: i64) -> Vec<Value> {
        let mut stmt = self.conn.prepare("SELECT * FROM replication_log WHERE log_index > ?1 ORDER BY log_index").expect("log");
        let rows = stmt
            .query_map(params![index], |row| {
                let payload: String = row.get("payload")?;
                Ok(json!({
                    "log_index": row.get::<_, i64>("log_index")?,
                    "epoch": row.get::<_, i64>("epoch")?,
                    "kind": row.get::<_, String>("kind")?,
                    "payload": serde_json::from_str::<Value>(&payload).unwrap_or(Value::Null),
                    "committed_at": row.get::<_, String>("committed_at")?,
                }))
            })
            .expect("map");
        rows.filter_map(|row| row.ok()).collect()
    }

    fn apply_one(&self, kind: &str, payload: &Value, epoch: i64) {
        if kind == "order_created" {
            let order = &payload["order"];
            let job = &payload["job"];
            let response = &order["response"];
            let degraded = if response.get("degraded").and_then(|v| v.as_bool()).unwrap_or(false) { 1 } else { 0 };
            self.conn
                .execute(
                    "INSERT OR REPLACE INTO orders(
                        order_id, caller_id, idempotency_key, external_ref, sku, qty, fulfill_url,
                        state, recommendation, degraded, epoch, log_index, created_at, response_json, request_hash
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 0, ?12, ?13, ?14)",
                    params![
                        order["order_id"].as_str().unwrap_or(""),
                        order["caller_id"].as_str().unwrap_or(""),
                        order["idempotency_key"].as_str().unwrap_or(""),
                        json_str(order.get("external_ref")),
                        order["sku"].as_str().unwrap_or(""),
                        order["qty"].as_i64().unwrap_or(0),
                        json_str(order.get("fulfill_url")),
                        response["state"].as_str().unwrap_or(""),
                        json_str(response.get("recommendation")),
                        degraded,
                        epoch,
                        order["created_at"].as_str().unwrap_or(""),
                        response.to_string(),
                        order["request_hash"].as_str().unwrap_or(""),
                    ],
                )
                .expect("apply order");
            self.conn
                .execute(
                    "INSERT OR REPLACE INTO jobs(job_id, order_id, kind, state, attempts, visible_after, worker_id, last_error, log_index)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0)",
                    params![
                        job["job_id"].as_str().unwrap_or(""),
                        job["order_id"].as_str().unwrap_or(""),
                        job["kind"].as_str().unwrap_or("fulfill"),
                        job["state"].as_str().unwrap_or("pending"),
                        job["attempts"].as_i64().unwrap_or(0),
                        job["visible_after"].as_i64().unwrap_or(0),
                        json_str(job.get("worker_id")),
                        json_str(job.get("last_error")),
                    ],
                )
                .expect("apply job");
            return;
        }
        if payload.get("response").is_some() && payload.get("order_id").and_then(|v| v.as_str()).is_some() {
            let body = &payload["response"];
            let order_id = payload["order_id"].as_str().unwrap_or("");
            self.write_snapshot_row(order_id, body);
            if kind != "order_snapshot" {
                let status = body["fulfillment"]["status"].as_str().unwrap_or("");
                let attempts = body["fulfillment"]["attempts"].as_i64().unwrap_or(0);
                self.conn
                    .execute(
                        "UPDATE jobs SET state = ?1, attempts = ?2, last_error = ?3, worker_id = ?4 WHERE order_id = ?5",
                        params![status, attempts, json_str(body["fulfillment"].get("last_error")), json_str(payload.get("worker_id")), order_id],
                    )
                    .expect("apply job status");
            }
        }
    }
}

fn json_str(value: Option<&Value>) -> Option<String> {
    value.and_then(|v| v.as_str()).map(|s| s.to_string())
}

fn read_order(row: &rusqlite::Row<'_>) -> OrderRow {
    OrderRow {
        order_id: row.get("order_id").unwrap_or_default(),
        caller_id: row.get("caller_id").unwrap_or_default(),
        external_ref: row.get("external_ref").ok().flatten(),
        sku: row.get("sku").unwrap_or_default(),
        qty: row.get("qty").unwrap_or(0),
        fulfill_url: row.get("fulfill_url").ok().flatten(),
        log_index: row.get("log_index").unwrap_or(0),
        response_json: row.get("response_json").unwrap_or_default(),
        request_hash: row.get("request_hash").unwrap_or_default(),
    }
}

fn read_job(row: &rusqlite::Row<'_>) -> JobRow {
    JobRow {
        job_id: row.get("job_id").unwrap_or_default(),
        order_id: row.get("order_id").unwrap_or_default(),
        state: row.get("state").unwrap_or_default(),
        attempts: row.get("attempts").unwrap_or(0),
        visible_after: row.get("visible_after").unwrap_or(0),
        worker_id: row.get("worker_id").ok().flatten(),
        last_error: row.get("last_error").ok().flatten(),
    }
}

fn fulfillment_of(job: &JobRow) -> Value {
    json!({"status": job.state, "attempts": job.attempts, "last_error": job.last_error})
}

fn public_body(order: &OrderRow, request_id: &str) -> Value {
    let mut saved: Value = serde_json::from_str(&order.response_json).unwrap_or(Value::Null);
    if !request_id.is_empty() {
        saved["request_id"] = json!(request_id);
    }
    saved
}

fn text_or_empty(value: Option<&Value>) -> String {
    value.and_then(|v| v.as_str()).unwrap_or("").to_string()
}

impl Store {
    pub fn new(node_id: &str, role: &str, peer: &str, path: &str, supervisor: &str) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::open(node_id, role, peer, path)),
            supervisor: supervisor.to_string(),
            stop: AtomicBool::new(false),
        })
    }

    fn stopped(&self) -> bool {
        self.stop.load(Ordering::SeqCst) || common::stop_requested()
    }

    fn append(&self, payload: &Value, request_id: &str) -> Response {
        fault_delay("store");
        let expected = payload.get("epoch").and_then(|v| v.as_i64()).unwrap_or(0);
        let caller_id = payload["caller_id"].as_str().unwrap_or("");
        let key = payload["idempotency_key"].as_str().unwrap_or("");
        let fields = &payload["order"];
        let digest = canonical_body(
            fields["sku"].as_str().unwrap_or(""),
            fields["qty"].as_i64().unwrap_or(0),
            &text_or_empty(fields.get("external_ref")),
            &text_or_empty(fields.get("fulfill_url")),
        );
        let mut state = self.state.lock().expect("store");
        if let Some(refused) = state.require_leader(request_id) {
            return refused;
        }
        if expected != state.epoch() {
            return state.fenced(request_id, expected);
        }
        begin(&state.conn).expect("begin");
        let existing = state.by_key(caller_id, key);
        if let Some(existing) = existing {
            commit(&state.conn).expect("commit");
            if existing.request_hash != digest {
                return json_response(409, &error_body("idempotency_mismatch", "幂等键与已有订单正文不一致", request_id), &[]);
            }
            let body = public_body(&existing, request_id);
            let epoch = state.epoch();
            return json_response(200, &json!({"created": false, "order": body, "log_index": existing.log_index, "epoch": epoch}), &[]);
        }
        let order_id = new_id("ord_");
        let job_id = new_id("job_");
        let created_at = iso_now();
        let epoch = state.epoch();
        let order_state = json!({
            "order_id": order_id,
            "caller_id": caller_id,
            "external_ref": fields.get("external_ref").cloned().unwrap_or(Value::Null),
            "sku": fields["sku"].as_str().unwrap_or(""),
            "qty": fields["qty"].as_i64().unwrap_or(0),
            "state": "accepted",
            "recommendation": Value::Null,
            "degraded": true,
            "fulfillment": {"status": "pending", "attempts": 0, "last_error": Value::Null},
            "epoch": epoch,
            "request_id": request_id,
        });
        let job = json!({
            "job_id": job_id,
            "order_id": order_id,
            "kind": "fulfill",
            "state": "pending",
            "attempts": 0,
            "visible_after": 0,
            "worker_id": Value::Null,
            "last_error": Value::Null,
        });
        let inserted = (|| -> rusqlite::Result<()> {
            state.conn.execute(
                "INSERT INTO orders(
                    order_id, caller_id, idempotency_key, external_ref, sku, qty, fulfill_url,
                    state, recommendation, degraded, epoch, log_index, created_at, response_json, request_hash
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'accepted', NULL, 1, ?8, 0, ?9, ?10, ?11)",
                params![
                    order_id,
                    caller_id,
                    key,
                    json_str(fields.get("external_ref")),
                    fields["sku"].as_str().unwrap_or(""),
                    fields["qty"].as_i64().unwrap_or(0),
                    json_str(fields.get("fulfill_url")),
                    epoch,
                    created_at,
                    order_state.to_string(),
                    digest,
                ],
            )?;
            state.conn.execute(
                "INSERT INTO jobs(job_id, order_id, kind, state, attempts, visible_after, worker_id, last_error, log_index)
                 VALUES (?1, ?2, 'fulfill', 'pending', 0, 0, NULL, NULL, 0)",
                params![job_id, order_id],
            )?;
            Ok(())
        })();
        if inserted.is_err() {
            rollback(&state.conn);
            return json_response(500, &error_body("unavailable", "进程内部错误", request_id), &[]);
        }
        let log_payload = json!({"order": {
            "order_id": order_id,
            "caller_id": caller_id,
            "idempotency_key": key,
            "external_ref": fields.get("external_ref").cloned().unwrap_or(Value::Null),
            "sku": fields["sku"].as_str().unwrap_or(""),
            "qty": fields["qty"].as_i64().unwrap_or(0),
            "fulfill_url": fields.get("fulfill_url").cloned().unwrap_or(Value::Null),
            "created_at": created_at,
            "request_hash": digest,
            "response": order_state,
        }, "job": job});
        let index = state.append_log("order_created", &log_payload);
        let mut order_state = log_payload["order"]["response"].clone();
        order_state["epoch"] = json!(state.epoch());
        state.conn.execute("UPDATE orders SET log_index = ?1, epoch = ?2 WHERE order_id = ?3", params![index, state.epoch(), order_id]).ok();
        state.conn.execute("UPDATE jobs SET log_index = ?1 WHERE job_id = ?2", params![index, job_id]).ok();
        state.conn.execute("UPDATE orders SET response_json = ?1 WHERE order_id = ?2", params![order_state.to_string(), order_id]).ok();
        if commit(&state.conn).is_err() {
            rollback(&state.conn);
            return json_response(500, &error_body("unavailable", "进程内部错误", request_id), &[]);
        }
        state.refresh_metrics();
        let epoch = state.epoch();
        drop(state);
        log_event("order_accepted", request_id, "info", &[("order_id", json!(order_id)), ("epoch", json!(epoch)), ("degraded", json!(true))]);
        json_response(200, &json!({"created": true, "order": order_state, "log_index": index, "epoch": epoch}), &[])
    }

    fn read_order(&self, order_id: &str, caller_id: &str, request_id: &str) -> Response {
        let mut state = self.state.lock().expect("store");
        if let Some(refused) = state.require_leader(request_id) {
            return refused;
        }
        match state.order(order_id) {
            Some(order) if order.caller_id == caller_id => json_response(200, &public_body(&order, request_id), &[]),
            _ => json_response(404, &error_body("not_found", "没有这笔订单", request_id), &[]),
        }
    }

    fn read_by_key(&self, caller_id: &str, key: &str, request_id: &str) -> Response {
        let mut state = self.state.lock().expect("store");
        if let Some(refused) = state.require_leader(request_id) {
            return refused;
        }
        match state.by_key(caller_id, key) {
            None => json_response(404, &error_body("not_found", "没有这笔订单", request_id), &[]),
            Some(order) => {
                let mut body = public_body(&order, request_id);
                body["request_hash"] = json!(order.request_hash);
                json_response(200, &body, &[])
            }
        }
    }

    fn save_snapshot(&self, payload: &Value, request_id: &str) -> Response {
        let order_id = payload["order_id"].as_str().unwrap_or("");
        let mut state = self.state.lock().expect("store");
        if let Some(refused) = state.require_leader(request_id) {
            return refused;
        }
        if payload.get("epoch").and_then(|v| v.as_i64()).unwrap_or(0) != state.epoch() {
            return state.fenced(request_id, payload.get("epoch").and_then(|v| v.as_i64()).unwrap_or(0));
        }
        let Some(order) = state.order(order_id) else {
            return json_response(404, &error_body("not_found", "没有这笔订单", request_id), &[]);
        };
        if state.job_for_order(order_id).is_none() {
            return json_response(404, &error_body("not_found", "没有这笔订单", request_id), &[]);
        }
        let mut body: Value = serde_json::from_str(&order.response_json).unwrap_or(Value::Null);
        body["recommendation"] = payload.get("recommendation").cloned().unwrap_or(Value::Null);
        body["degraded"] = json!(payload.get("degraded").and_then(|v| v.as_bool()).unwrap_or(false));
        body["request_id"] = json!(request_id);
        body["epoch"] = json!(state.epoch());
        begin(&state.conn).expect("begin");
        state.write_snapshot_row(order_id, &body);
        state.append_log("order_snapshot", &json!({"order_id": order_id, "response": body}));
        commit(&state.conn).expect("commit");
        json_response(200, &body, &[])
    }

    fn claim(&self, worker_id: &str, request_id: &str) -> Response {
        let mut state = self.state.lock().expect("store");
        if let Some(refused) = state.require_leader(request_id) {
            return refused;
        }
        state.release_expired();
        let now = now_ms();
        let row = state
            .conn
            .query_row(
                "SELECT * FROM jobs WHERE state = 'pending' AND visible_after <= ?1 ORDER BY job_id LIMIT 1",
                params![now],
                |row| Ok(read_job(row)),
            )
            .ok();
        let Some(row) = row else {
            return empty_response(204);
        };
        let attempts = row.attempts + 1;
        let visible = now + VISIBILITY_MS;
        begin(&state.conn).expect("begin");
        state
            .conn
            .execute(
                "UPDATE jobs SET state = 'inflight', attempts = ?1, visible_after = ?2, worker_id = ?3 WHERE job_id = ?4",
                params![attempts, visible, worker_id, row.job_id],
            )
            .expect("claim");
        let order = state.order(&row.order_id).expect("order");
        let job = state.job_for_order(&row.order_id).expect("job");
        let body = {
            let mut saved: Value = serde_json::from_str(&order.response_json).unwrap_or(json!({}));
            saved["fulfillment"] = fulfillment_of(&job);
            saved["request_id"] = json!(request_id);
            saved["epoch"] = json!(state.epoch());
            saved
        };
        // 对照 Python snapshot()：用当前任务行重写履约段，订单状态字段保持原样。
        let snapshot = snapshot_body(&order, &job, request_id, state.epoch());
        state.write_snapshot_row(&order.order_id, &snapshot);
        state.append_log(
            "job_claimed",
            &json!({"job_id": row.job_id, "order_id": row.order_id, "response": snapshot, "worker_id": worker_id}),
        );
        commit(&state.conn).expect("commit");
        state.refresh_metrics();
        let claimed = state.job_for_order(&row.order_id).expect("claimed");
        let order = state.order(&row.order_id).expect("order");
        let response = json!({
            "job_id": claimed.job_id,
            "order_id": order.order_id,
            "caller_id": order.caller_id,
            "external_ref": order.external_ref,
            "sku": order.sku,
            "qty": order.qty,
            "fulfill_url": order.fulfill_url,
            "attempts": claimed.attempts,
        });
        let order_id = order.order_id.clone();
        drop(state);
        let _ = body;
        log_event("job_claimed", request_id, "info", &[("order_id", json!(order_id)), ("worker_id", json!(worker_id))]);
        json_response(200, &response, &[])
    }

    fn finish(&self, job_id: &str, request_id: &str, success: bool, error: Option<&str>) -> Response {
        let mut state = self.state.lock().expect("store");
        if let Some(refused) = state.require_leader(request_id) {
            return refused;
        }
        let Some(job) = ({
            let id = job_id.to_string();
            state.conn.query_row("SELECT * FROM jobs WHERE job_id = ?1", params![id], |row| Ok(read_job(row))).ok()
        }) else {
            return json_response(404, &error_body("not_found", "没有这个任务", request_id), &[]);
        };
        begin(&state.conn).expect("begin");
        let (kind, event) = if success {
            state.conn.execute("UPDATE jobs SET state = 'done', last_error = NULL, worker_id = NULL WHERE job_id = ?1", params![job_id]).ok();
            ("job_done", "job_done")
        } else if job.attempts >= JOB_MAX_ATTEMPTS {
            state.conn.execute("UPDATE jobs SET state = 'dead', last_error = ?1, worker_id = NULL WHERE job_id = ?2", params![error, job_id]).ok();
            ("job_dead", "job_dead")
        } else {
            let wait = backoff_ms(job.attempts);
            state
                .conn
                .execute(
                    "UPDATE jobs SET state = 'pending', last_error = ?1, worker_id = NULL, visible_after = ?2 WHERE job_id = ?3",
                    params![error, now_ms() + wait, job_id],
                )
                .ok();
            ("job_reset", "job_retry")
        };
        let job = state.conn.query_row("SELECT * FROM jobs WHERE job_id = ?1", params![job_id], |row| Ok(read_job(row))).expect("job");
        let order = state.order(&job.order_id).expect("order");
        let state_name = match job.state.as_str() {
            "done" => "fulfilled",
            "dead" => "fulfillment_failed",
            _ => "accepted",
        };
        let mut body: Value = serde_json::from_str(&order.response_json).unwrap_or(json!({}));
        body["state"] = json!(state_name);
        body["fulfillment"] = fulfillment_of(&job);
        body["epoch"] = json!(state.epoch());
        state.conn.execute("UPDATE orders SET state = ?1 WHERE order_id = ?2", params![state_name, order.order_id]).ok();
        state.write_snapshot_row(&order.order_id, &body);
        state.append_log(kind, &json!({"job_id": job_id, "order_id": order.order_id, "response": body}));
        commit(&state.conn).expect("commit");
        state.refresh_metrics();
        let order_id = order.order_id.clone();
        drop(state);
        log_event(event, request_id, "info", &[("order_id", json!(order_id)), ("job_id", json!(job_id))]);
        json_response(200, &body, &[])
    }

    fn promote(&self, new_epoch: i64, peer: &str, request_id: &str) -> Response {
        let mut state = self.state.lock().expect("store");
        if new_epoch <= state.epoch() && state.is_leader() {
            return json_response(200, &json!({"epoch": state.epoch(), "role": state.role()}), &[]);
        }
        begin(&state.conn).expect("begin");
        state.set_meta("role", "leader");
        state.set_meta("epoch", &new_epoch.to_string());
        state.append_log("epoch_promoted", &json!({"epoch": new_epoch, "node_id": state.node_id}));
        commit(&state.conn).expect("commit");
        state.peer = peer.to_string();
        state.follower_applied = 0;
        state.refresh_metrics();
        let node_id = state.node_id.clone();
        drop(state);
        log_event("promoted", request_id, "info", &[("epoch", json!(new_epoch)), ("node_id", json!(node_id))]);
        json_response(200, &json!({"epoch": new_epoch, "role": "leader"}), &[])
    }

    fn step_down(&self, new_epoch: i64, leader: &str) {
        let mut state = self.state.lock().expect("store");
        if state.is_leader() && new_epoch <= state.epoch() {
            return;
        }
        if !state.is_leader() && new_epoch == state.epoch() && state.peer == leader {
            return;
        }
        begin(&state.conn).expect("begin");
        state.conn.execute_batch("DELETE FROM orders; DELETE FROM jobs; DELETE FROM replication_log;").ok();
        state.set_meta("role", "follower");
        state.set_meta("epoch", &new_epoch.to_string());
        state.set_meta("applied_index", "0");
        commit(&state.conn).expect("commit");
        state.peer = leader.to_string();
        drop(state);
        log_event("stepped_down", "", "info", &[("epoch", json!(new_epoch)), ("leader", json!(leader))]);
    }

    fn apply_remote(&self, entries: &[Value]) -> (bool, String) {
        let mut state = self.state.lock().expect("store");
        begin(&state.conn).expect("begin");
        for entry in entries {
            let expected = state.applied() + 1;
            let index = entry["log_index"].as_i64().unwrap_or(0);
            if index != expected {
                rollback(&state.conn);
                return (false, "gap".into());
            }
            let kind = entry["kind"].as_str().unwrap_or("");
            let epoch = entry["epoch"].as_i64().unwrap_or(0);
            state.apply_one(kind, &entry["payload"], epoch);
            state
                .conn
                .execute(
                    "INSERT INTO replication_log(log_index, epoch, kind, payload, committed_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![index, epoch, kind, entry["payload"].to_string(), entry.get("committed_at").and_then(|v| v.as_str()).unwrap_or(&iso_now()).to_string()],
                )
                .expect("remote log");
            state.set_meta("applied_index", &index.to_string());
            if kind == "epoch_promoted" {
                let promoted = entry["payload"]["epoch"].as_i64().unwrap_or(epoch);
                state.set_meta("epoch", &promoted.to_string());
                state.set_meta("role", "follower");
            }
        }
        commit(&state.conn).expect("commit");
        state.refresh_metrics();
        (true, String::new())
    }

    fn stats(&self) -> Value {
        let mut state = self.state.lock().expect("store");
        state.refresh_metrics();
        json!({
            "node_id": state.node_id,
            "role": state.role(),
            "epoch": state.epoch(),
            "applied_index": state.applied(),
            "jobs_pending": state.metrics_pending,
            "replicate_lag": state.metrics_lag,
        })
    }

    fn heartbeat_loop(self: &Arc<Self>) {
        while !self.stopped() {
            thread::sleep(Duration::from_secs_f64(common::HEARTBEAT_SEC));
            let node_id = self.state.lock().expect("store").node_id.clone();
            if drop_heartbeat(&node_id) {
                continue;
            }
            let stats = self.stats();
            let epoch = self.state.lock().expect("store").epoch();
            let leader_now = self.state.lock().expect("store").is_leader();
            match http_util::http_request("POST", &format!("{}/internal/heartbeat", self.supervisor), Some(&stats), &[], 0.5) {
                Ok((200, _, raw)) => {
                    if let Ok(Some(view)) = http_util::decode_json(&raw) {
                        let leader = view.get("leader").and_then(|v| v.as_str()).unwrap_or("");
                        let remote_epoch = view.get("epoch").and_then(|v| v.as_i64()).unwrap_or(0);
                        if !leader.is_empty() && remote_epoch > epoch && leader_now {
                            self.step_down(remote_epoch, leader);
                        } else if !leader.is_empty() && !leader_now {
                            self.state.lock().expect("store").peer = leader.to_string();
                        }
                    }
                }
                _ => continue,
            }
        }
    }

    fn replicate_loop(self: &Arc<Self>) {
        self.state.lock().expect("store").replication_started = true;
        while !self.stopped() {
            thread::sleep(Duration::from_secs_f64(common::HEARTBEAT_SEC));
            if self.state.lock().expect("store").is_leader() {
                self.push();
            } else {
                self.pull();
            }
        }
    }

    fn push(&self) {
        if pause_replication() {
            return;
        }
        let (entries, peer, follower_applied) = {
            let state = self.state.lock().expect("store");
            if state.peer.is_empty() {
                return;
            }
            (state.entries_after(state.follower_applied), state.peer.clone(), state.follower_applied)
        };
        if entries.is_empty() {
            self.state.lock().expect("store").refresh_metrics();
            return;
        }
        match http_util::http_request("POST", &format!("{peer}/internal/replicate"), Some(&json!({"entries": entries})), &[], 0.5) {
            Ok((200, _, raw)) => {
                if let Ok(Some(body)) = http_util::decode_json(&raw) {
                    let mut state = self.state.lock().expect("store");
                    state.follower_applied = body.get("applied_index").and_then(|v| v.as_i64()).unwrap_or(state.follower_applied);
                }
            }
            Ok((409, _, raw)) => {
                if let Ok(Some(body)) = http_util::decode_json(&raw) {
                    let mut state = self.state.lock().expect("store");
                    state.follower_applied = body.get("applied_index").and_then(|v| v.as_i64()).unwrap_or(0);
                }
            }
            Err(err) => {
                let lag = {
                    let state = self.state.lock().expect("store");
                    state.applied() - follower_applied
                };
                log_event("replicate_lag", "", "error", &[("message", json!(err.to_string())), ("lag", json!(lag))]);
            }
            _ => {}
        }
        let lag = {
            let mut state = self.state.lock().expect("store");
            state.refresh_metrics();
            state.metrics_lag
        };
        if lag != 0 {
            log_event("replicate_lag", "", "info", &[("lag", json!(lag))]);
        }
    }

    fn pull(&self) {
        let (peer, applied) = {
            let state = self.state.lock().expect("store");
            (state.peer.clone(), state.applied())
        };
        if peer.is_empty() {
            return;
        }
        let Ok((200, _, raw)) = http_util::http_request("GET", &format!("{peer}/internal/log?after={applied}"), None, &[], 0.5) else {
            return;
        };
        let Ok(Some(body)) = http_util::decode_json(&raw) else {
            return;
        };
        let entries = body.get("entries").and_then(|v| v.as_array()).cloned().unwrap_or_default();
        if !entries.is_empty() {
            self.apply_remote(&entries);
        }
    }

    fn sweep_loop(self: &Arc<Self>) {
        while !self.stopped() {
            thread::sleep(Duration::from_secs_f64(common::HEARTBEAT_SEC));
            let mut state = self.state.lock().expect("store");
            if !state.is_leader() {
                continue;
            }
            state.release_expired();
            state.refresh_metrics();
        }
    }

    fn metrics_text(&self) -> String {
        let stats = self.stats();
        let state = self.state.lock().expect("store");
        let leader = state.is_leader();
        format!(
            "orange_errors_total {}\norange_jobs_pending {}\norange_replicate_lag {}\n",
            state.metrics_errors,
            if leader { stats["jobs_pending"].as_i64().unwrap_or(0) } else { 0 },
            if leader { stats["replicate_lag"].as_i64().unwrap_or(0) } else { 0 },
        )
    }

    fn ready(&self) -> bool {
        let state = self.state.lock().expect("store");
        state.is_leader() || state.replication_started
    }
}

fn snapshot_body(order: &OrderRow, job: &JobRow, request_id: &str, epoch: i64) -> Value {
    let mut saved: Value = serde_json::from_str(&order.response_json).unwrap_or(json!({}));
    saved["fulfillment"] = fulfillment_of(job);
    saved["request_id"] = json!(request_id);
    saved["epoch"] = json!(epoch);
    let _ = epoch;
    saved
}

pub fn handler(store: &Store, method: String, path: String, headers: HashMap<String, String>, raw: Vec<u8>) -> Response {
    let request_id = header_request_id(&headers);
    let (route, query) = http_util::split_path(&path);
    if method == "GET" && route == "/health" {
        return empty_response(200);
    }
    if method == "GET" && route == "/ready" {
        return empty_response(if store.ready() { 200 } else { 503 });
    }
    if method == "GET" && route == "/metrics" {
        return text_response(200, &store.metrics_text());
    }
    if method == "GET" && route == "/internal/stats" {
        return json_response(200, &store.stats(), &[]);
    }
    if method == "GET" && route == "/internal/log" {
        let after = query.get("after").and_then(|v| v.parse().ok()).unwrap_or(0);
        let entries = store.state.lock().expect("store").entries_after(after);
        let applied = store.state.lock().expect("store").applied();
        return json_response(200, &json!({"entries": entries, "applied_index": applied}), &[]);
    }
    if method == "POST" && route == "/internal/replicate" {
        let body = http_util::decode_json(&raw).ok().flatten().unwrap_or(Value::Null);
        let entries = body.get("entries").and_then(|v| v.as_array()).cloned().unwrap_or_default();
        let (ok, reason) = store.apply_remote(&entries);
        let applied = store.state.lock().expect("store").applied();
        return json_response(if ok { 200 } else { 409 }, &json!({"applied_index": applied, "error": reason}), &[]);
    }
    if method == "POST" && route == "/internal/promote" {
        let body = http_util::decode_json(&raw).ok().flatten().unwrap_or(Value::Null);
        let peer = body.get("peer").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let peer = if peer.is_empty() { store.state.lock().expect("store").peer.clone() } else { peer };
        return store.promote(body["new_epoch"].as_i64().unwrap_or(0), &peer, &request_id);
    }
    if method == "POST" && route == "/internal/append" {
        let body = http_util::decode_json(&raw).ok().flatten().unwrap_or(json!({}));
        return store.append(&body, &request_id);
    }
    if method == "POST" && route == "/internal/snapshot" {
        let body = http_util::decode_json(&raw).ok().flatten().unwrap_or(json!({}));
        return store.save_snapshot(&body, &request_id);
    }
    if method == "GET" && route == "/internal/orders/by-key" {
        return store.read_by_key(query.get("caller_id").map(|s| s.as_str()).unwrap_or(""), query.get("idempotency_key").map(|s| s.as_str()).unwrap_or(""), &request_id);
    }
    if method == "GET" && route.starts_with("/internal/orders/") {
        let id = route.rsplit('/').next().unwrap_or("");
        return store.read_order(id, query.get("caller_id").map(|s| s.as_str()).unwrap_or(""), &request_id);
    }
    if method == "POST" && route == "/internal/jobs/claim" {
        let body = http_util::decode_json(&raw).ok().flatten().unwrap_or(json!({}));
        let worker = body.get("worker_id").and_then(|v| v.as_str()).unwrap_or("worker");
        return store.claim(worker, &request_id);
    }
    if method == "POST" && route.starts_with("/internal/jobs/") && route.ends_with("/ack") {
        let job_id = route.split('/').nth(3).unwrap_or("");
        return store.finish(job_id, &request_id, true, None);
    }
    if method == "POST" && route.starts_with("/internal/jobs/") && route.ends_with("/fail") {
        let job_id = route.split('/').nth(3).unwrap_or("").to_string();
        let body = http_util::decode_json(&raw).ok().flatten().unwrap_or(json!({}));
        let error = body.get("error").and_then(|v| v.as_str()).unwrap_or("履约失败").to_string();
        return store.finish(&job_id, &request_id, false, Some(&error));
    }
    json_response(404, &error_body("not_found", "没有这个路径", &request_id), &[])
}

pub fn main(args: &[String]) {
    let node_id = common::flag(args, "--node-id").unwrap_or("store");
    let port: u16 = common::flag(args, "--port").and_then(|v| v.parse().ok()).unwrap_or(8301);
    let role = common::flag(args, "--role").unwrap_or("follower");
    let peer = common::flag(args, "--peer").unwrap_or("");
    let data = common::flag(args, "--data").unwrap_or("store.sqlite");
    let supervisor = common::flag(args, "--supervisor").unwrap_or("http://127.0.0.1:8090");
    let root = common::flag_or(args, "--root", ".");
    common::set_context(node_id, root);
    common::install_signals();
    common::start_fault_reader();
    let store = Store::new(node_id, role, peer, data, supervisor);
    let heartbeat = store.clone();
    let replicate = store.clone();
    let sweep = store.clone();
    thread::spawn(move || heartbeat.heartbeat_loop());
    thread::spawn(move || replicate.replicate_loop());
    thread::spawn(move || sweep.sweep_loop());
    let handler_store = store.clone();
    http_util::serve(port, move |method, path, headers, raw| handler(&handler_store, method, path, headers, raw));
    log_event("process_started", "", "info", &[("role", json!(role)), ("port", json!(port))]);
    http_util::block_until_stopped();
}
