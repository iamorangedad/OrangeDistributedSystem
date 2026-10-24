"""主存储与从存储。订单、任务和复制日志在同一个 sqlite 事务里提交。"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import threading
import time
from typing import Any

from common import (
    HEARTBEAT_SEC,
    JOB_MAX_ATTEMPTS,
    PROCESS_NAME,
    VISIBILITY_MS,
    backoff_ms,
    canonical_body,
    current_faults,
    decode_json,
    drop_heartbeat,
    empty_response,
    error_body,
    fault_delay,
    header_request_id,
    http_request,
    install_term_handler,
    iso_now,
    json_response,
    log_event,
    new_id,
    now_ms,
    pause_replication,
    serve,
    split_path,
    start_fault_reader,
    text_response,
)

SCHEMA = """
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
"""


def fulfillment_of(job: sqlite3.Row) -> dict[str, Any]:
    return {
        "status": job["state"],
        "attempts": job["attempts"],
        "last_error": job["last_error"],
    }


def snapshot(order: sqlite3.Row, job: sqlite3.Row, request_id: str) -> dict[str, Any]:
    return {
        "order_id": order["order_id"],
        "caller_id": order["caller_id"],
        "external_ref": order["external_ref"],
        "sku": order["sku"],
        "qty": order["qty"],
        "state": order["state"],
        "recommendation": order["recommendation"],
        "degraded": bool(order["degraded"]),
        "fulfillment": fulfillment_of(job),
        "epoch": order["epoch"],
        "request_id": request_id,
    }


class Store:
    def __init__(self, node_id: str, role: str, peer: str, path: str, supervisor: str) -> None:
        self.node_id = node_id
        self.peer = peer
        self.supervisor = supervisor
        self.follower_applied = 0
        self.running = True
        self.replication_loop_started = False
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.executescript(SCHEMA)
        self._ensure_meta(role)
        self.metrics_errors = 0
        self.metrics_pending = 0
        self.metrics_lag = 0

    def _ensure_meta(self, role: str) -> None:
        existing = self.conn.execute("SELECT key FROM meta").fetchall()
        if existing:
            return
        for key, value in (
            ("role", role),
            ("epoch", "1"),
            ("applied_index", "0"),
            ("node_id", self.node_id),
        ):
            self.conn.execute("INSERT INTO meta(key, value) VALUES (?, ?)", (key, value))

    def meta(self, key: str) -> str:
        row = self.conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return "" if row is None else str(row["value"])

    def set_meta(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    def role(self) -> str:
        return self.meta("role")

    def epoch(self) -> int:
        return int(self.meta("epoch") or "1")

    def applied(self) -> int:
        return int(self.meta("applied_index") or "0")

    def is_leader(self) -> bool:
        return self.role() == "leader"

    def refresh_metrics(self) -> None:
        row = self.conn.execute("SELECT COUNT(*) AS n FROM jobs WHERE state = 'pending'").fetchone()
        self.metrics_pending = int(row["n"])
        if self.is_leader():
            self.metrics_lag = max(0, self.applied() - self.follower_applied)
        else:
            self.metrics_lag = 0

    def _append_log(self, kind: str, payload: dict[str, Any]) -> int:
        index = self.applied() + 1
        self.conn.execute(
            "INSERT INTO replication_log(log_index, epoch, kind, payload, committed_at) VALUES (?, ?, ?, ?, ?)",
            (index, self.epoch(), kind, json.dumps(payload, ensure_ascii=False), iso_now()),
        )
        self.set_meta("applied_index", str(index))
        return index

    def _order(self, order_id: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone()

    def _job_for_order(self, order_id: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM jobs WHERE order_id = ?", (order_id,)).fetchone()

    def _by_key(self, caller_id: str, key: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM orders WHERE caller_id = ? AND idempotency_key = ?",
            (caller_id, key),
        ).fetchone()

    def _write_snapshot_row(self, order_id: str, body: dict[str, Any]) -> None:
        self.conn.execute(
            "UPDATE orders SET response_json = ?, state = ?, recommendation = ?, degraded = ? WHERE order_id = ?",
            (
                json.dumps(body, ensure_ascii=False),
                body["state"],
                body["recommendation"],
                1 if body["degraded"] else 0,
                order_id,
            ),
        )

    def public_body(self, order: sqlite3.Row, request_id: str | None = None) -> dict[str, Any]:
        saved = json.loads(order["response_json"])
        if request_id:
            saved["request_id"] = request_id
        return saved

    def require_leader(self, request_id: str) -> tuple[int, dict[str, str], bytes] | None:
        if self.is_leader():
            return None
        self.metrics_errors += 1
        log_event("write_fenced", request_id, epoch=self.epoch())
        return json_response(
            409,
            {
                "error": {
                    "code": "epoch_fenced",
                    "message": "当前节点不接受写入",
                    "request_id": request_id,
                    "epoch": self.epoch(),
                }
            },
        )

    def append(self, payload: dict[str, Any], request_id: str) -> tuple[int, dict[str, str], bytes]:
        fault_delay("store")
        expected = int(payload.get("epoch") or 0)
        caller_id = payload["caller_id"]
        key = payload["idempotency_key"]
        fields = payload["order"]
        digest = canonical_body(fields["sku"], fields["qty"], fields.get("external_ref"), fields.get("fulfill_url"))
        with self.lock:
            refused = self.require_leader(request_id)
            if refused:
                return refused
            if expected != self.epoch():
                self.metrics_errors += 1
                log_event("write_fenced", request_id, epoch=self.epoch(), expected=expected)
                return json_response(
                    409,
                    {
                        "error": {
                            "code": "epoch_fenced",
                            "message": "写入代际已过期",
                            "request_id": request_id,
                            "epoch": self.epoch(),
                        }
                    },
                )
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                existing = self._by_key(caller_id, key)
                if existing is not None:
                    self.conn.execute("COMMIT")
                    if existing["request_hash"] != digest:
                        return json_response(
                            409,
                            error_body("idempotency_mismatch", "幂等键与已有订单正文不一致", request_id),
                        )
                    body = self.public_body(existing, request_id)
                    return json_response(200, {"created": False, "order": body, "log_index": existing["log_index"], "epoch": self.epoch()})
                order_id = new_id("ord_")
                job_id = new_id("job_")
                created_at = iso_now()
                job = {
                    "job_id": job_id,
                    "order_id": order_id,
                    "kind": "fulfill",
                    "state": "pending",
                    "attempts": 0,
                    "visible_after": 0,
                    "worker_id": None,
                    "last_error": None,
                }
                order_state = {
                    "order_id": order_id,
                    "caller_id": caller_id,
                    "external_ref": fields.get("external_ref"),
                    "sku": fields["sku"],
                    "qty": fields["qty"],
                    "state": "accepted",
                    "recommendation": None,
                    "degraded": True,
                    "fulfillment": {
                        "status": "pending",
                        "attempts": 0,
                        "last_error": None,
                    },
                    "epoch": self.epoch(),
                    "request_id": request_id,
                }
                self.conn.execute(
                    """INSERT INTO orders(
                        order_id, caller_id, idempotency_key, external_ref, sku, qty, fulfill_url,
                        state, recommendation, degraded, epoch, log_index, created_at, response_json, request_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'accepted', NULL, 1, ?, 0, ?, ?, ?)""",
                    (
                        order_id,
                        caller_id,
                        key,
                        fields.get("external_ref"),
                        fields["sku"],
                        fields["qty"],
                        fields.get("fulfill_url"),
                        self.epoch(),
                        created_at,
                        json.dumps(order_state, ensure_ascii=False),
                        digest,
                    ),
                )
                self.conn.execute(
                    """INSERT INTO jobs(job_id, order_id, kind, state, attempts, visible_after, worker_id, last_error, log_index)
                       VALUES (?, ?, 'fulfill', 'pending', 0, 0, NULL, NULL, 0)""",
                    (job_id, order_id),
                )
                log_payload = {
                    "order": {
                        "order_id": order_id,
                        "caller_id": caller_id,
                        "idempotency_key": key,
                        "external_ref": fields.get("external_ref"),
                        "sku": fields["sku"],
                        "qty": fields["qty"],
                        "fulfill_url": fields.get("fulfill_url"),
                        "created_at": created_at,
                        "request_hash": digest,
                        "response": order_state,
                    },
                    "job": job,
                }
                index = self._append_log("order_created", log_payload)
                self.conn.execute(
                    "UPDATE orders SET log_index = ?, epoch = ? WHERE order_id = ?",
                    (index, self.epoch(), order_id),
                )
                self.conn.execute("UPDATE jobs SET log_index = ? WHERE job_id = ?", (index, job_id))
                order_state["epoch"] = self.epoch()
                self.conn.execute(
                    "UPDATE orders SET response_json = ? WHERE order_id = ?",
                    (json.dumps(order_state, ensure_ascii=False), order_id),
                )
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
            self.refresh_metrics()
        log_event("order_accepted", request_id, order_id=order_id, epoch=self.epoch(), degraded=True)
        return json_response(200, {"created": True, "order": order_state, "log_index": index, "epoch": self.epoch()})

    def read_order(self, order_id: str, caller_id: str, request_id: str) -> tuple[int, dict[str, str], bytes]:
        with self.lock:
            if not self.is_leader():
                return self.require_leader(request_id)  # type: ignore[return-value]
            order = self._order(order_id)
            if order is None or order["caller_id"] != caller_id:
                return json_response(404, error_body("not_found", "没有这笔订单", request_id))
            return json_response(200, self.public_body(order, request_id))

    def read_by_key(self, caller_id: str, key: str, request_id: str) -> tuple[int, dict[str, str], bytes]:
        with self.lock:
            if not self.is_leader():
                return self.require_leader(request_id)  # type: ignore[return-value]
            order = self._by_key(caller_id, key)
            if order is None:
                return json_response(404, error_body("not_found", "没有这笔订单", request_id))
            body = self.public_body(order, request_id)
            body["request_hash"] = order["request_hash"]
            return json_response(200, body)

    def save_snapshot(self, payload: dict[str, Any], request_id: str) -> tuple[int, dict[str, str], bytes]:
        order_id = payload["order_id"]
        with self.lock:
            refused = self.require_leader(request_id)
            if refused:
                return refused
            if int(payload.get("epoch") or 0) != self.epoch():
                return json_response(
                    409,
                    {
                        "error": {
                            "code": "epoch_fenced",
                            "message": "写入代际已过期",
                            "request_id": request_id,
                            "epoch": self.epoch(),
                        }
                    },
                )
            order = self._order(order_id)
            job = None if order is None else self._job_for_order(order_id)
            if order is None or job is None:
                return json_response(404, error_body("not_found", "没有这笔订单", request_id))
            body = json.loads(order["response_json"])
            body["recommendation"] = payload.get("recommendation")
            body["degraded"] = bool(payload.get("degraded"))
            body["request_id"] = request_id
            body["epoch"] = self.epoch()
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                self._write_snapshot_row(order_id, body)
                self._append_log(
                    "order_snapshot",
                    {"order_id": order_id, "response": body},
                )
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
        return json_response(200, body)

    def claim(self, worker_id: str, request_id: str) -> tuple[int, dict[str, str], bytes]:
        with self.lock:
            refused = self.require_leader(request_id)
            if refused:
                return refused
            self._release_expired()
            row = self.conn.execute(
                """SELECT * FROM jobs WHERE state = 'pending' AND visible_after <= ?
                   ORDER BY job_id LIMIT 1""",
                (now_ms(),),
            ).fetchone()
            if row is None:
                return empty_response(204)
            attempts = int(row["attempts"]) + 1
            visible = now_ms() + VISIBILITY_MS
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                self.conn.execute(
                    """UPDATE jobs SET state = 'inflight', attempts = ?, visible_after = ?, worker_id = ?, log_index = log_index
                       WHERE job_id = ?""",
                    (attempts, visible, worker_id, row["job_id"]),
                )
                order = self._order(row["order_id"])
                job = self._job_for_order(row["order_id"])
                assert order is not None and job is not None
                body = snapshot(order, job, request_id)
                self._write_snapshot_row(order["order_id"], body)
                self._append_log(
                    "job_claimed",
                    {"job_id": row["job_id"], "order_id": row["order_id"], "response": body, "worker_id": worker_id},
                )
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
            self.refresh_metrics()
            claimed = self._job_for_order(row["order_id"])
            order = self._order(row["order_id"])
        log_event("job_claimed", request_id, order_id=row["order_id"], worker_id=worker_id)
        return json_response(
            200,
            {
                "job_id": claimed["job_id"],
                "order_id": order["order_id"],
                "caller_id": order["caller_id"],
                "external_ref": order["external_ref"],
                "sku": order["sku"],
                "qty": order["qty"],
                "fulfill_url": order["fulfill_url"],
                "attempts": claimed["attempts"],
            },
        )

    def ack(self, job_id: str, request_id: str) -> tuple[int, dict[str, str], bytes]:
        return self._finish(job_id, request_id, success=True, error=None)

    def fail(self, job_id: str, request_id: str, error: str) -> tuple[int, dict[str, str], bytes]:
        return self._finish(job_id, request_id, success=False, error=error)

    def _finish(self, job_id: str, request_id: str, success: bool, error: str | None) -> tuple[int, dict[str, str], bytes]:
        with self.lock:
            refused = self.require_leader(request_id)
            if refused:
                return refused
            job = self.conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            if job is None:
                return json_response(404, error_body("not_found", "没有这个任务", request_id))
            order = self._order(job["order_id"])
            assert order is not None
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                if success:
                    self.conn.execute(
                        "UPDATE jobs SET state = 'done', last_error = NULL, worker_id = NULL WHERE job_id = ?",
                        (job_id,),
                    )
                    kind = "job_done"
                    event = "job_done"
                elif int(job["attempts"]) >= JOB_MAX_ATTEMPTS:
                    self.conn.execute(
                        "UPDATE jobs SET state = 'dead', last_error = ?, worker_id = NULL WHERE job_id = ?",
                        (error, job_id),
                    )
                    kind = "job_dead"
                    event = "job_dead"
                else:
                    wait = backoff_ms(int(job["attempts"]))
                    self.conn.execute(
                        """UPDATE jobs SET state = 'pending', last_error = ?, worker_id = NULL, visible_after = ?
                           WHERE job_id = ?""",
                        (error, now_ms() + wait, job_id),
                    )
                    kind = "job_reset"
                    event = "job_retry"
                job = self.conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
                order = self._order(job["order_id"])
                assert job is not None and order is not None
                if job["state"] == "done":
                    state = "fulfilled"
                elif job["state"] == "dead":
                    state = "fulfillment_failed"
                else:
                    state = "accepted"
                body = json.loads(order["response_json"])
                body["state"] = state
                body["fulfillment"] = fulfillment_of(job)
                body["epoch"] = self.epoch()
                self.conn.execute("UPDATE orders SET state = ? WHERE order_id = ?", (state, order["order_id"]))
                self._write_snapshot_row(order["order_id"], body)
                self._append_log(kind, {"job_id": job_id, "order_id": order["order_id"], "response": body})
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
            self.refresh_metrics()
        log_event(event, request_id, order_id=order["order_id"], job_id=job_id)
        return json_response(200, body)

    def _release_expired(self) -> None:
        rows = self.conn.execute(
            "SELECT * FROM jobs WHERE state = 'inflight' AND visible_after <= ?",
            (now_ms(),),
        ).fetchall()
        for row in rows:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                self.conn.execute(
                    "UPDATE jobs SET state = 'pending', worker_id = NULL WHERE job_id = ? AND state = 'inflight'",
                    (row["job_id"],),
                )
                order = self._order(row["order_id"])
                job = self._job_for_order(row["order_id"])
                if order is not None and job is not None:
                    body = json.loads(order["response_json"])
                    body["state"] = "accepted"
                    body["fulfillment"] = fulfillment_of(job)
                    self._write_snapshot_row(order["order_id"], body)
                    self._append_log("job_reset", {"job_id": row["job_id"], "order_id": row["order_id"], "response": body})
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
            log_event("job_retry", order_id=row["order_id"], job_id=row["job_id"], reason="visibility_timeout")

    def promote(self, new_epoch: int, peer: str, request_id: str) -> tuple[int, dict[str, str], bytes]:
        with self.lock:
            if new_epoch <= self.epoch() and self.is_leader():
                return json_response(200, {"epoch": self.epoch(), "role": self.role()})
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                self.set_meta("role", "leader")
                self.set_meta("epoch", str(new_epoch))
                self._append_log("epoch_promoted", {"epoch": new_epoch, "node_id": self.node_id})
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
            self.peer = peer
            self.follower_applied = 0
            self.refresh_metrics()
        log_event("promoted", request_id, epoch=new_epoch, node_id=self.node_id)
        return json_response(200, {"epoch": new_epoch, "role": "leader"})

    def step_down(self, new_epoch: int, leader: str) -> None:
        with self.lock:
            if self.is_leader() and new_epoch <= self.epoch():
                return
            if not self.is_leader() and new_epoch == self.epoch() and self.peer == leader:
                return
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                self.conn.execute("DELETE FROM orders")
                self.conn.execute("DELETE FROM jobs")
                self.conn.execute("DELETE FROM replication_log")
                self.set_meta("role", "follower")
                self.set_meta("epoch", str(new_epoch))
                self.set_meta("applied_index", "0")
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
            self.peer = leader
        log_event("stepped_down", epoch=new_epoch, leader=leader)

    def entries_after(self, index: int) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM replication_log WHERE log_index > ? ORDER BY log_index",
                (index,),
            ).fetchall()
        return [
            {
                "log_index": row["log_index"],
                "epoch": row["epoch"],
                "kind": row["kind"],
                "payload": json.loads(row["payload"]),
                "committed_at": row["committed_at"],
            }
            for row in rows
        ]

    def apply_remote(self, entries: list[dict[str, Any]]) -> tuple[bool, str]:
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                for entry in entries:
                    expected = self.applied() + 1
                    if int(entry["log_index"]) != expected:
                        self.conn.execute("ROLLBACK")
                        return False, "gap"
                    self._apply_payload(entry["kind"], entry["payload"], int(entry["epoch"]))
                    self.conn.execute(
                        "INSERT INTO replication_log(log_index, epoch, kind, payload, committed_at) VALUES (?, ?, ?, ?, ?)",
                        (
                            int(entry["log_index"]),
                            int(entry["epoch"]),
                            entry["kind"],
                            json.dumps(entry["payload"], ensure_ascii=False),
                            entry.get("committed_at") or iso_now(),
                        ),
                    )
                    self.set_meta("applied_index", str(entry["log_index"]))
                    if entry["kind"] == "epoch_promoted":
                        self.set_meta("epoch", str(entry["payload"]["epoch"]))
                        self.set_meta("role", "follower")
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
            self.refresh_metrics()
        return True, ""

    def _apply_payload(self, kind: str, payload: dict[str, Any], epoch: int) -> None:
        if kind == "order_created":
            order = payload["order"]
            job = payload["job"]
            response = order["response"]
            self.conn.execute(
                """INSERT OR REPLACE INTO orders(
                    order_id, caller_id, idempotency_key, external_ref, sku, qty, fulfill_url,
                    state, recommendation, degraded, epoch, log_index, created_at, response_json, request_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)""",
                (
                    order["order_id"],
                    order["caller_id"],
                    order["idempotency_key"],
                    order.get("external_ref"),
                    order["sku"],
                    order["qty"],
                    order.get("fulfill_url"),
                    response["state"],
                    response.get("recommendation"),
                    1 if response.get("degraded") else 0,
                    epoch,
                    order["created_at"],
                    json.dumps(response, ensure_ascii=False),
                    order["request_hash"],
                ),
            )
            self.conn.execute(
                """INSERT OR REPLACE INTO jobs(job_id, order_id, kind, state, attempts, visible_after, worker_id, last_error, log_index)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)""",
                (
                    job["job_id"],
                    job["order_id"],
                    job["kind"],
                    job["state"],
                    job["attempts"],
                    job["visible_after"],
                    job.get("worker_id"),
                    job.get("last_error"),
                ),
            )
            return
        if "response" in payload and payload.get("order_id"):
            body = payload["response"]
            self._write_snapshot_row(payload["order_id"], body)
            if kind != "order_snapshot":
                status = body["fulfillment"]["status"]
                self.conn.execute(
                    """UPDATE jobs SET state = ?, attempts = ?, last_error = ?, worker_id = ?
                       WHERE order_id = ?""",
                    (
                        status,
                        body["fulfillment"]["attempts"],
                        body["fulfillment"]["last_error"],
                        payload.get("worker_id"),
                        payload["order_id"],
                    ),
                )

    def stats(self) -> dict[str, Any]:
        with self.lock:
            self.refresh_metrics()
            return {
                "node_id": self.node_id,
                "role": self.role(),
                "epoch": self.epoch(),
                "applied_index": self.applied(),
                "jobs_pending": self.metrics_pending,
                "replicate_lag": self.metrics_lag,
            }

    def heartbeat_loop(self) -> None:
        while self.running:
            time.sleep(HEARTBEAT_SEC)
            if drop_heartbeat(self.node_id):
                continue
            stats = self.stats()
            try:
                status, _, raw = http_request(
                    "POST",
                    self.supervisor + "/internal/heartbeat",
                    stats,
                    timeout=0.5,
                )
                if status == 200 and raw:
                    view = decode_json(raw)
                    leader = view.get("leader")
                    epoch = int(view.get("epoch") or 0)
                    if leader and epoch > self.epoch() and self.is_leader():
                        self.step_down(epoch, leader)
                    elif leader and not self.is_leader():
                        self.peer = leader
            except Exception:
                continue

    def replicate_loop(self) -> None:
        self.replication_loop_started = True
        while self.running:
            time.sleep(HEARTBEAT_SEC)
            if self.is_leader():
                self._push()
            else:
                self._pull()

    def _push(self) -> None:
        if pause_replication() or not self.peer:
            return
        entries = self.entries_after(self.follower_applied)
        if not entries:
            with self.lock:
                self.refresh_metrics()
            return
        try:
            status, _, raw = http_request(
                "POST",
                self.peer + "/internal/replicate",
                {"entries": entries},
                timeout=0.5,
            )
            if status == 200 and raw:
                body = decode_json(raw)
                self.follower_applied = int(body.get("applied_index") or self.follower_applied)
            elif status == 409 and raw:
                body = decode_json(raw)
                self.follower_applied = int(body.get("applied_index") or 0)
        except Exception as exc:
            log_event("replicate_lag", level="error", message=str(exc), lag=self.applied() - self.follower_applied)
        with self.lock:
            self.refresh_metrics()
        if self.metrics_lag:
            log_event("replicate_lag", lag=self.metrics_lag)

    def _pull(self) -> None:
        if not self.peer:
            return
        try:
            status, _, raw = http_request(
                "GET",
                f"{self.peer}/internal/log?after={self.applied()}",
                timeout=0.5,
            )
            if status != 200 or not raw:
                return
            entries = decode_json(raw).get("entries") or []
            if entries:
                self.apply_remote(entries)
        except Exception:
            return

    def sweep_loop(self) -> None:
        while self.running:
            time.sleep(HEARTBEAT_SEC)
            if not self.is_leader():
                continue
            with self.lock:
                self._release_expired()
                self.refresh_metrics()


def build_handler(store: Store):
    def handler(method: str, path: str, headers: dict[str, str], raw: bytes):
        request_id = header_request_id(headers)
        route, query = split_path(path)
        if method == "GET" and route == "/health":
            return empty_response(200)
        if method == "GET" and route == "/ready":
            if store.is_leader() or store.replication_loop_started:
                return empty_response(200)
            return empty_response(503)
        if method == "GET" and route == "/metrics":
            stats = store.stats()
            text = (
                f"orange_errors_total {store.metrics_errors}\n"
                f"orange_jobs_pending {store.metrics_pending if store.is_leader() else 0}\n"
                f"orange_replicate_lag {store.metrics_lag if store.is_leader() else 0}\n"
            )
            return text_response(200, text)
        if method == "GET" and route == "/internal/stats":
            return json_response(200, store.stats())
        if method == "GET" and route == "/internal/log":
            after = int(query.get("after") or 0)
            return json_response(200, {"entries": store.entries_after(after), "applied_index": store.applied()})
        if method == "POST" and route == "/internal/replicate":
            body = decode_json(raw) or {}
            ok, reason = store.apply_remote(body.get("entries") or [])
            code = 200 if ok else 409
            return json_response(code, {"applied_index": store.applied(), "error": reason})
        if method == "POST" and route == "/internal/promote":
            body = decode_json(raw) or {}
            return store.promote(int(body["new_epoch"]), body.get("peer") or store.peer, request_id)
        if method == "POST" and route == "/internal/append":
            return store.append(decode_json(raw) or {}, request_id)
        if method == "POST" and route == "/internal/snapshot":
            return store.save_snapshot(decode_json(raw) or {}, request_id)
        if method == "GET" and route == "/internal/orders/by-key":
            return store.read_by_key(query.get("caller_id", ""), query.get("idempotency_key", ""), request_id)
        if method == "GET" and route.startswith("/internal/orders/"):
            return store.read_order(route.split("/")[-1], query.get("caller_id", ""), request_id)
        if method == "POST" and route == "/internal/jobs/claim":
            body = decode_json(raw) or {}
            return store.claim(body.get("worker_id") or "worker", request_id)
        if method == "POST" and route.startswith("/internal/jobs/") and route.endswith("/ack"):
            job_id = route.split("/")[3]
            return store.ack(job_id, request_id)
        if method == "POST" and route.startswith("/internal/jobs/") and route.endswith("/fail"):
            job_id = route.split("/")[3]
            body = decode_json(raw) or {}
            return store.fail(job_id, request_id, str(body.get("error") or "履约失败"))
        return json_response(404, error_body("not_found", "没有这个路径", request_id))

    return handler


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--role", choices=("leader", "follower"), required=True)
    parser.add_argument("--peer", required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--supervisor", required=True)
    parser.add_argument("--root", default=os.getcwd())
    args = parser.parse_args()
    os.environ["ORANGE_ROOT"] = args.root
    os.environ["ORANGE_PROCESS"] = args.node_id
    # 模块导入时已读过环境变量，这里把进程名和根目录写回 common。
    import common

    common.PROCESS_NAME = args.node_id
    common.ROOT = args.root
    start_fault_reader()
    store = Store(args.node_id, args.role, args.peer, args.data, args.supervisor)
    threading.Thread(target=store.heartbeat_loop, name="heartbeat", daemon=True).start()
    threading.Thread(target=store.replicate_loop, name="replicate", daemon=True).start()
    threading.Thread(target=store.sweep_loop, name="sweep", daemon=True).start()
    server = serve(args.port, build_handler(store))
    install_term_handler(server)
    log_event("process_started", role=args.role, port=args.port)
    server.serve_forever()


if __name__ == "__main__":
    main()
