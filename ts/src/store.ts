import fs from "fs";
import path from "path";
import Database from "better-sqlite3";
import {
  asBool,
  asInt,
  asMap,
  asString,
  backoffMS,
  canonicalBody,
  decodeJSON,
  dropHeartbeat,
  errorBody,
  faultDelay,
  heartbeatMs,
  isoNow,
  jobMaxAttempts,
  logEvent,
  marshal,
  newID,
  nowMS,
  pauseReplication,
  setContext,
  sleep,
  startFaultReader,
  visibilityMs,
  type JsonMap,
} from "./common";
import {
  emptyResponse,
  headerRequestID,
  httpRequest,
  jsonResponse,
  onStop,
  serve,
  splitPath,
  textResponse,
  type Handler,
  type ResponseMessage,
} from "./http";

const schema = `
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
`;

interface OrderRow {
  order_id: string;
  caller_id: string;
  idempotency_key: string;
  external_ref: string | null;
  sku: string;
  qty: number;
  fulfill_url: string | null;
  state: string;
  recommendation: string | null;
  degraded: number;
  epoch: number;
  log_index: number;
  created_at: string;
  response_json: string;
  request_hash: string;
}

interface JobRow {
  job_id: string;
  order_id: string;
  kind: string;
  state: string;
  attempts: number;
  visible_after: number;
  worker_id: string | null;
  last_error: string | null;
  log_index: number;
}

const orderSQL = `SELECT order_id, caller_id, idempotency_key, external_ref, sku, qty, fulfill_url, state, recommendation, degraded, epoch, log_index, created_at, response_json, request_hash`;
const jobSQL = `SELECT job_id, order_id, kind, state, attempts, visible_after, worker_id, last_error, log_index`;

class Store {
  private followerApplied = 0;
  running = true;
  private replicationStarted = false;
  private metricsErrors = 0;
  private metricsPending = 0;
  private metricsLag = 0;

  constructor(
    readonly nodeID: string,
    private peer: string,
    readonly supervisor: string,
    private readonly db: Database.Database,
  ) {}

  private exec(sql: string, ...args: unknown[]): void {
    this.db.prepare(sql).run(...args);
  }

  private one<T>(sql: string, ...args: unknown[]): T | undefined {
    return this.db.prepare(sql).get(...args) as T | undefined;
  }

  private meta(key: string): string {
    const row = this.one<{ value: string }>("SELECT value FROM meta WHERE key = ?", key);
    return row?.value ?? "";
  }

  private setMeta(key: string, value: string): void {
    this.exec("INSERT INTO meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", key, value);
  }

  role(): string {
    return this.meta("role");
  }

  epoch(): number {
    const value = Number.parseInt(this.meta("epoch"), 10);
    return value || 1;
  }

  applied(): number {
    return Number.parseInt(this.meta("applied_index"), 10) || 0;
  }

  isLeader(): boolean {
    return this.role() === "leader";
  }

  private refreshMetrics(): void {
    const row = this.one<{ n: number }>("SELECT COUNT(*) AS n FROM jobs WHERE state = 'pending'");
    this.metricsPending = row?.n ?? 0;
    if (this.isLeader()) this.metricsLag = Math.max(0, this.applied() - this.followerApplied);
    else this.metricsLag = 0;
  }

  private appendLog(kind: string, payload: unknown): number {
    const index = this.applied() + 1;
    this.exec(
      "INSERT INTO replication_log(log_index, epoch, kind, payload, committed_at) VALUES (?, ?, ?, ?, ?)",
      index, this.epoch(), kind, marshal(payload), isoNow(),
    );
    this.setMeta("applied_index", String(index));
    return index;
  }

  private orderByID(id: string): OrderRow | undefined {
    return this.one<OrderRow>(`${orderSQL} FROM orders WHERE order_id = ?`, id);
  }

  private orderByKey(callerID: string, key: string): OrderRow | undefined {
    return this.one<OrderRow>(`${orderSQL} FROM orders WHERE caller_id = ? AND idempotency_key = ?`, callerID, key);
  }

  private jobForOrder(orderID: string): JobRow | undefined {
    return this.one<JobRow>(`${jobSQL} FROM jobs WHERE order_id = ?`, orderID);
  }

  private jobByID(id: string): JobRow | undefined {
    return this.one<JobRow>(`${jobSQL} FROM jobs WHERE job_id = ?`, id);
  }

  private fulfillmentOf(job: JobRow): JsonMap {
    return { status: job.state, attempts: job.attempts, last_error: job.last_error };
  }

  private snapshotBody(order: OrderRow, job: JobRow, requestID: string): JsonMap {
    return {
      order_id: order.order_id,
      caller_id: order.caller_id,
      external_ref: order.external_ref,
      sku: order.sku,
      qty: order.qty,
      state: order.state,
      recommendation: order.recommendation,
      degraded: order.degraded !== 0,
      fulfillment: this.fulfillmentOf(job),
      epoch: order.epoch,
      request_id: requestID,
    };
  }

  private writeSnapshot(orderID: string, body: JsonMap): void {
    this.exec(
      "UPDATE orders SET response_json = ?, state = ?, recommendation = ?, degraded = ? WHERE order_id = ?",
      marshal(body), asString(body["state"]), body["recommendation"] ?? null, asBool(body["degraded"]) ? 1 : 0, orderID,
    );
  }

  private publicBody(order: OrderRow, requestID: string): JsonMap {
    let body: JsonMap = {};
    try {
      body = asMap(decodeJSON(order.response_json)) ?? {};
    } catch {
      body = {};
    }
    if (requestID) body["request_id"] = requestID;
    return body;
  }

  private fenced(epoch: number, requestID: string, message: string): ResponseMessage {
    return jsonResponse(409, { error: { code: "epoch_fenced", message, request_id: requestID, epoch } });
  }

  private requireLeader(requestID: string): [ResponseMessage, boolean] {
    if (this.isLeader()) return [{ status: 0, headers: {}, body: null }, true];
    this.metricsErrors++;
    logEvent("write_fenced", requestID, "info", { epoch: this.epoch() });
    return [this.fenced(this.epoch(), requestID, "当前节点不接受写入"), false];
  }

  appendOrder(payload: JsonMap, requestID: string): ResponseMessage {
    const expected = asInt(payload["epoch"]);
    const callerID = asString(payload["caller_id"]);
    const key = asString(payload["idempotency_key"]);
    const fields = asMap(payload["order"]) ?? {};
    const digest = canonicalBody(asString(fields["sku"]), asInt(fields["qty"]), fields["external_ref"], fields["fulfill_url"]);
    const [refused, ok] = this.requireLeader(requestID);
    if (!ok) return refused;
    if (expected !== this.epoch()) {
      this.metricsErrors++;
      logEvent("write_fenced", requestID, "info", { epoch: this.epoch(), expected });
      return this.fenced(this.epoch(), requestID, "写入代际已过期");
    }
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const existing = this.orderByKey(callerID, key);
      if (existing) {
        this.db.exec("COMMIT");
        if (existing.request_hash !== digest) {
          return jsonResponse(409, errorBody("idempotency_mismatch", "幂等键与已有订单正文不一致", requestID));
        }
        return jsonResponse(200, { created: false, order: this.publicBody(existing, requestID), log_index: existing.log_index, epoch: this.epoch() });
      }
      const orderID = newID("ord_");
      const jobID = newID("job_");
      const createdAt = isoNow();
      const job = {
        job_id: jobID, order_id: orderID, kind: "fulfill", state: "pending",
        attempts: 0, visible_after: 0, worker_id: null, last_error: null,
      };
      const orderState: JsonMap = {
        order_id: orderID, caller_id: callerID, external_ref: fields["external_ref"] ?? null, sku: fields["sku"],
        qty: asInt(fields["qty"]), state: "accepted", recommendation: null, degraded: true,
        fulfillment: { status: "pending", attempts: 0, last_error: null },
        epoch: this.epoch(), request_id: requestID,
      };
      this.exec(
        `INSERT INTO orders(
          order_id, caller_id, idempotency_key, external_ref, sku, qty, fulfill_url,
          state, recommendation, degraded, epoch, log_index, created_at, response_json, request_hash
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'accepted', NULL, 1, ?, 0, ?, ?, ?)`,
        orderID, callerID, key, fields["external_ref"] ?? null, fields["sku"], asInt(fields["qty"]), fields["fulfill_url"] ?? null,
        this.epoch(), createdAt, marshal(orderState), digest,
      );
      this.exec(
        `INSERT INTO jobs(job_id, order_id, kind, state, attempts, visible_after, worker_id, last_error, log_index)
         VALUES (?, ?, 'fulfill', 'pending', 0, 0, NULL, NULL, 0)`,
        jobID, orderID,
      );
      const index = this.appendLog("order_created", {
        order: {
          order_id: orderID, caller_id: callerID, idempotency_key: key, external_ref: fields["external_ref"] ?? null,
          sku: fields["sku"], qty: asInt(fields["qty"]), fulfill_url: fields["fulfill_url"] ?? null,
          created_at: createdAt, request_hash: digest, response: orderState,
        },
        job,
      });
      this.exec("UPDATE orders SET log_index = ?, epoch = ? WHERE order_id = ?", index, this.epoch(), orderID);
      this.exec("UPDATE jobs SET log_index = ? WHERE job_id = ?", index, jobID);
      orderState["epoch"] = this.epoch();
      this.exec("UPDATE orders SET response_json = ? WHERE order_id = ?", marshal(orderState), orderID);
      this.db.exec("COMMIT");
      this.refreshMetrics();
      logEvent("order_accepted", requestID, "info", { order_id: orderID, epoch: this.epoch(), degraded: true });
      return jsonResponse(200, { created: true, order: orderState, log_index: index, epoch: this.epoch() });
    } catch (error) {
      this.rollback();
      throw error;
    }
  }

  private rollback(): void {
    try {
      this.db.exec("ROLLBACK");
    } catch {
      // 事务可能已经结束。
    }
  }

  readOrder(orderID: string, callerID: string, requestID: string): ResponseMessage {
    const [refused, ok] = this.requireLeader(requestID);
    if (!ok) return refused;
    const order = this.orderByID(orderID);
    if (!order || order.caller_id !== callerID) return jsonResponse(404, errorBody("not_found", "没有这笔订单", requestID));
    return jsonResponse(200, this.publicBody(order, requestID));
  }

  readByKey(callerID: string, key: string, requestID: string): ResponseMessage {
    const [refused, ok] = this.requireLeader(requestID);
    if (!ok) return refused;
    const order = this.orderByKey(callerID, key);
    if (!order) return jsonResponse(404, errorBody("not_found", "没有这笔订单", requestID));
    const body = this.publicBody(order, requestID);
    body["request_hash"] = order.request_hash;
    return jsonResponse(200, body);
  }

  saveSnapshot(payload: JsonMap, requestID: string): ResponseMessage {
    const orderID = asString(payload["order_id"]);
    const [refused, ok] = this.requireLeader(requestID);
    if (!ok) return refused;
    if (asInt(payload["epoch"]) !== this.epoch()) return this.fenced(this.epoch(), requestID, "写入代际已过期");
    const order = this.orderByID(orderID);
    const job = this.jobForOrder(orderID);
    if (!order || !job) return jsonResponse(404, errorBody("not_found", "没有这笔订单", requestID));
    const body = this.publicBody(order, "");
    body["recommendation"] = payload["recommendation"];
    body["degraded"] = asBool(payload["degraded"]);
    body["request_id"] = requestID;
    body["epoch"] = this.epoch();
    this.db.exec("BEGIN IMMEDIATE");
    try {
      this.writeSnapshot(orderID, body);
      this.appendLog("order_snapshot", { order_id: orderID, response: body });
      this.db.exec("COMMIT");
    } catch (error) {
      this.rollback();
      throw error;
    }
    return jsonResponse(200, body);
  }

  claim(workerID: string, requestID: string): ResponseMessage {
    const [refused, ok] = this.requireLeader(requestID);
    if (!ok) return refused;
    this.releaseExpired();
    const row = this.one<JobRow>(`${jobSQL} FROM jobs WHERE state = 'pending' AND visible_after <= ? ORDER BY job_id LIMIT 1`, nowMS());
    if (!row) return emptyResponse(204);
    const attempts = row.attempts + 1;
    const visible = nowMS() + visibilityMs;
    this.db.exec("BEGIN IMMEDIATE");
    try {
      this.exec("UPDATE jobs SET state = 'inflight', attempts = ?, visible_after = ?, worker_id = ? WHERE job_id = ?", attempts, visible, workerID, row.job_id);
      const order = this.orderByID(row.order_id);
      const job = this.jobForOrder(row.order_id);
      if (!order || !job) throw new Error("订单或任务丢失");
      const body = this.snapshotBody(order, job, requestID);
      this.writeSnapshot(order.order_id, body);
      this.appendLog("job_claimed", { job_id: row.job_id, order_id: row.order_id, response: body, worker_id: workerID });
      this.db.exec("COMMIT");
    } catch (error) {
      this.rollback();
      throw error;
    }
    this.refreshMetrics();
    const claimed = this.jobForOrder(row.order_id);
    const order = this.orderByID(row.order_id);
    if (!claimed || !order) return emptyResponse(204);
    logEvent("job_claimed", requestID, "info", { order_id: row.order_id, worker_id: workerID });
    return jsonResponse(200, {
      job_id: claimed.job_id, order_id: order.order_id, caller_id: order.caller_id,
      external_ref: order.external_ref, sku: order.sku, qty: order.qty, fulfill_url: order.fulfill_url, attempts: claimed.attempts,
    });
  }

  finish(jobID: string, requestID: string, success: boolean, failure: string): ResponseMessage {
    const [refused, ok] = this.requireLeader(requestID);
    if (!ok) return refused;
    const current = this.jobByID(jobID);
    if (!current) return jsonResponse(404, errorBody("not_found", "没有这个任务", requestID));
    this.db.exec("BEGIN IMMEDIATE");
    let kind = "";
    let event = "";
    try {
      if (success) {
        this.exec("UPDATE jobs SET state = 'done', last_error = NULL, worker_id = NULL WHERE job_id = ?", jobID);
        kind = "job_done";
        event = "job_done";
      } else if (current.attempts >= jobMaxAttempts) {
        this.exec("UPDATE jobs SET state = 'dead', last_error = ?, worker_id = NULL WHERE job_id = ?", failure, jobID);
        kind = "job_dead";
        event = "job_dead";
      } else {
        const wait = backoffMS(current.attempts);
        this.exec("UPDATE jobs SET state = 'pending', last_error = ?, worker_id = NULL, visible_after = ? WHERE job_id = ?", failure, nowMS() + wait, jobID);
        kind = "job_reset";
        event = "job_retry";
      }
      const job = this.jobByID(jobID);
      const order = job ? this.orderByID(job.order_id) : undefined;
      if (!job || !order) throw new Error("订单或任务丢失");
      let state = "accepted";
      if (job.state === "done") state = "fulfilled";
      else if (job.state === "dead") state = "fulfillment_failed";
      const body = this.publicBody(order, "");
      body["state"] = state;
      body["fulfillment"] = this.fulfillmentOf(job);
      body["epoch"] = this.epoch();
      this.exec("UPDATE orders SET state = ? WHERE order_id = ?", state, order.order_id);
      this.writeSnapshot(order.order_id, body);
      this.appendLog(kind, { job_id: jobID, order_id: order.order_id, response: body });
      this.db.exec("COMMIT");
      this.refreshMetrics();
      logEvent(event, requestID, "info", { order_id: order.order_id, job_id: jobID });
      return jsonResponse(200, body);
    } catch (error) {
      this.rollback();
      throw error;
    }
  }

  private releaseExpired(): void {
    const rows = this.db.prepare(`${jobSQL} FROM jobs WHERE state = 'inflight' AND visible_after <= ?`).all(nowMS()) as JobRow[];
    for (const row of rows) {
      this.db.exec("BEGIN IMMEDIATE");
      try {
        this.exec("UPDATE jobs SET state = 'pending', worker_id = NULL WHERE job_id = ? AND state = 'inflight'", row.job_id);
        const order = this.orderByID(row.order_id);
        const job = this.jobForOrder(row.order_id);
        if (order && job) {
          const body = this.publicBody(order, "");
          body["state"] = "accepted";
          body["fulfillment"] = this.fulfillmentOf(job);
          this.writeSnapshot(order.order_id, body);
          this.appendLog("job_reset", { job_id: row.job_id, order_id: row.order_id, response: body });
        }
        this.db.exec("COMMIT");
        logEvent("job_retry", "", "info", { order_id: row.order_id, job_id: row.job_id, reason: "visibility_timeout" });
      } catch {
        this.rollback();
      }
    }
  }

  promote(newEpoch: number, peer: string, requestID: string): ResponseMessage {
    if (!peer) peer = this.peer;
    if (newEpoch <= this.epoch() && this.isLeader()) return jsonResponse(200, { epoch: this.epoch(), role: this.role() });
    this.db.exec("BEGIN IMMEDIATE");
    try {
      this.setMeta("role", "leader");
      this.setMeta("epoch", String(newEpoch));
      this.appendLog("epoch_promoted", { epoch: newEpoch, node_id: this.nodeID });
      this.db.exec("COMMIT");
    } catch (error) {
      this.rollback();
      throw error;
    }
    this.peer = peer;
    this.followerApplied = 0;
    this.refreshMetrics();
    logEvent("promoted", requestID, "info", { epoch: newEpoch, node_id: this.nodeID });
    return jsonResponse(200, { epoch: newEpoch, role: "leader" });
  }

  stepDown(newEpoch: number, leader: string): void {
    if (this.isLeader() && newEpoch <= this.epoch()) return;
    if (!this.isLeader() && newEpoch === this.epoch() && this.peer === leader) return;
    try {
      this.db.exec("BEGIN IMMEDIATE");
    } catch {
      return;
    }
    try {
      this.exec("DELETE FROM orders");
      this.exec("DELETE FROM jobs");
      this.exec("DELETE FROM replication_log");
      this.setMeta("role", "follower");
      this.setMeta("epoch", String(newEpoch));
      this.setMeta("applied_index", "0");
      this.db.exec("COMMIT");
      this.peer = leader;
      logEvent("stepped_down", "", "info", { epoch: newEpoch, leader });
    } catch {
      this.rollback();
    }
  }

  entriesAfter(index: number): JsonMap[] {
    const rows = this.db.prepare("SELECT log_index, epoch, kind, payload, committed_at FROM replication_log WHERE log_index > ? ORDER BY log_index").all(index) as Array<{
      log_index: number; epoch: number; kind: string; payload: string; committed_at: string;
    }>;
    const entries: JsonMap[] = [];
    for (const row of rows) {
      let decoded: unknown = null;
      try {
        decoded = decodeJSON(row.payload);
      } catch {
        decoded = null;
      }
      entries.push({ log_index: row.log_index, epoch: row.epoch, kind: row.kind, payload: decoded, committed_at: row.committed_at });
    }
    return entries;
  }

  applyRemote(entries: unknown[]): [boolean, string] {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      for (const item of entries) {
        const entry = asMap(item) ?? {};
        const expected = this.applied() + 1;
        if (asInt(entry["log_index"]) !== expected) {
          this.db.exec("ROLLBACK");
          return [false, "gap"];
        }
        this.applyPayload(asString(entry["kind"]), asMap(entry["payload"]) ?? {}, asInt(entry["epoch"]));
        const committed = asString(entry["committed_at"]) || isoNow();
        this.exec(
          "INSERT INTO replication_log(log_index, epoch, kind, payload, committed_at) VALUES (?, ?, ?, ?, ?)",
          asInt(entry["log_index"]), asInt(entry["epoch"]), entry["kind"], marshal(entry["payload"]), committed,
        );
        this.setMeta("applied_index", String(asInt(entry["log_index"])));
        if (asString(entry["kind"]) === "epoch_promoted") {
          this.setMeta("epoch", String(asInt(asMap(entry["payload"])?.["epoch"])));
          this.setMeta("role", "follower");
        }
      }
      this.db.exec("COMMIT");
    } catch (error) {
      this.rollback();
      throw error;
    }
    this.refreshMetrics();
    return [true, ""];
  }

  private applyPayload(kind: string, payload: JsonMap, epoch: number): void {
    if (kind === "order_created") {
      const order = asMap(payload["order"]) ?? {};
      const job = asMap(payload["job"]) ?? {};
      const response = asMap(order["response"]) ?? {};
      this.exec(
        `INSERT OR REPLACE INTO orders(
          order_id, caller_id, idempotency_key, external_ref, sku, qty, fulfill_url,
          state, recommendation, degraded, epoch, log_index, created_at, response_json, request_hash
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)`,
        order["order_id"], order["caller_id"], order["idempotency_key"], order["external_ref"] ?? null, order["sku"], asInt(order["qty"]), order["fulfill_url"] ?? null,
        response["state"], response["recommendation"] ?? null, asBool(response["degraded"]) ? 1 : 0, epoch, order["created_at"], marshal(response), order["request_hash"],
      );
      this.exec(
        `INSERT OR REPLACE INTO jobs(job_id, order_id, kind, state, attempts, visible_after, worker_id, last_error, log_index)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)`,
        job["job_id"], job["order_id"], job["kind"], job["state"], asInt(job["attempts"]), asInt(job["visible_after"]), job["worker_id"] ?? null, job["last_error"] ?? null,
      );
      return;
    }
    if (payload["order_id"] != null && payload["response"] != null) {
      const body = asMap(payload["response"]) ?? {};
      this.writeSnapshot(asString(payload["order_id"]), body);
      if (kind !== "order_snapshot") {
        const fulfillment = asMap(body["fulfillment"]) ?? {};
        this.exec(
          "UPDATE jobs SET state = ?, attempts = ?, last_error = ?, worker_id = ? WHERE order_id = ?",
          fulfillment["status"], asInt(fulfillment["attempts"]), fulfillment["last_error"] ?? null, payload["worker_id"] ?? null, payload["order_id"],
        );
      }
    }
  }

  stats(): JsonMap {
    this.refreshMetrics();
    return {
      node_id: this.nodeID, role: this.role(), epoch: this.epoch(), applied_index: this.applied(),
      jobs_pending: this.metricsPending, replicate_lag: this.metricsLag,
    };
  }

  async heartbeatLoop(): Promise<void> {
    while (this.running) {
      await sleep(heartbeatMs);
      if (!this.running) return;
      if (dropHeartbeat(this.nodeID)) continue;
      try {
        const view = this.stats();
        const result = await httpRequest("POST", this.supervisor + "/internal/heartbeat", view, null, 500);
        if (result.error || result.status !== 200) continue;
        let parsed: JsonMap | null = null;
        try {
          parsed = asMap(decodeJSON(result.body));
        } catch {
          continue;
        }
        const leader = asString(parsed?.["leader"]);
        const epoch = asInt(parsed?.["epoch"]);
        const leaderNow = this.isLeader();
        const current = this.epoch();
        if (leader && epoch > current && leaderNow) this.stepDown(epoch, leader);
        else if (leader && !leaderNow) this.peer = leader;
      } catch (error) {
        logEvent("heartbeat_error", "", "error", { message: error instanceof Error ? error.message : String(error) });
      }
    }
  }

  async replicateLoop(): Promise<void> {
    this.replicationStarted = true;
    while (this.running) {
      await sleep(heartbeatMs);
      if (!this.running) return;
      if (this.isLeader()) await this.push();
      else await this.pull();
    }
  }

  private async push(): Promise<void> {
    if (pauseReplication()) return;
    const peer = this.peer;
    const from = this.followerApplied;
    if (!peer) return;
    const entries = this.entriesAfter(from);
    if (entries.length === 0) {
      this.refreshMetrics();
      return;
    }
    const result = await httpRequest("POST", peer + "/internal/replicate", { entries }, null, 500);
    if (result.error) {
      const lag = this.applied() - this.followerApplied;
      logEvent("replicate_lag", "", "error", { message: result.error.message, lag });
    } else if (result.status === 200 || result.status === 409) {
      try {
        const body = asMap(decodeJSON(result.body));
        this.followerApplied = asInt(body?.["applied_index"]);
      } catch {
        // 对端没有给出 applied_index。
      }
    }
    this.refreshMetrics();
    if (this.metricsLag !== 0) logEvent("replicate_lag", "", "info", { lag: this.metricsLag });
  }

  private async pull(): Promise<void> {
    const peer = this.peer;
    const index = this.applied();
    if (!peer) return;
    const result = await httpRequest("GET", peer + "/internal/log?after=" + index, null, null, 500);
    if (result.error || result.status !== 200 || result.body.length === 0) return;
    try {
      const body = asMap(decodeJSON(result.body));
      const entries = body?.["entries"];
      if (Array.isArray(entries) && entries.length > 0) this.applyRemote(entries);
    } catch {
      // 复制响应暂时不可读。
    }
  }

  async sweepLoop(): Promise<void> {
    while (this.running) {
      await sleep(heartbeatMs);
      if (!this.running) return;
      if (this.isLeader()) {
        this.releaseExpired();
        this.refreshMetrics();
      }
    }
  }

  ready(): boolean {
    return this.isLeader() || this.replicationStarted;
  }

  errorCount(): number {
    return this.metricsErrors;
  }
}

function openStore(nodeID: string, role: string, peer: string, file: string, supervisor: string): Store {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const db = new Database(file);
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = FULL");
  db.pragma("busy_timeout = 5000");
  db.exec(schema);
  const store = new Store(nodeID, peer, supervisor, db);
  const count = db.prepare("SELECT COUNT(*) AS n FROM meta").get() as { n: number };
  if (count.n === 0) {
    const insert = db.prepare("INSERT INTO meta(key, value) VALUES (?, ?)");
    for (const [key, value] of [["role", role], ["epoch", "1"], ["applied_index", "0"], ["node_id", nodeID]] as Array<[string, string]>) {
      insert.run(key, value);
    }
  }
  return store;
}

function jobIDFrom(route: string): string {
  const parts = route.split("/");
  return parts.length < 4 ? "" : parts[3];
}

function handler(node: Store): Handler {
  return async (method, requestPath, headers, raw) => {
    const requestID = headerRequestID(headers);
    const [route, query] = splitPath(requestPath);
    let payload: JsonMap = {};
    if (raw.length > 0) {
      try {
        payload = asMap(decodeJSON(raw)) ?? {};
      } catch {
        payload = {};
      }
    }
    if (method === "GET" && route === "/health") return emptyResponse(200);
    if (method === "GET" && route === "/ready") return node.ready() ? emptyResponse(200) : emptyResponse(503);
    if (method === "GET" && route === "/metrics") {
      const view = node.stats();
      const leader = asString(view["role"]) === "leader";
      const pending = leader ? asInt(view["jobs_pending"]) : 0;
      const lag = leader ? asInt(view["replicate_lag"]) : 0;
      return textResponse(200, `orange_errors_total ${node.errorCount()}\norange_jobs_pending ${pending}\norange_replicate_lag ${lag}\n`);
    }
    if (method === "GET" && route === "/internal/stats") return jsonResponse(200, node.stats());
    if (method === "GET" && route === "/internal/log") {
      const after = asInt(query["after"] ? Number(query["after"]) : 0);
      return jsonResponse(200, { entries: node.entriesAfter(after), applied_index: node.applied() });
    }
    if (method === "POST" && route === "/internal/replicate") {
      const entries = Array.isArray(payload["entries"]) ? payload["entries"] as unknown[] : [];
      const [ok, reason] = node.applyRemote(entries);
      return jsonResponse(ok ? 200 : 409, { applied_index: node.applied(), error: reason });
    }
    if (method === "POST" && route === "/internal/promote") {
      const next = asString(payload["peer"]);
      return node.promote(asInt(payload["new_epoch"]), next, requestID);
    }
    if (method === "POST" && route === "/internal/append") {
      await faultDelay("store");
      return node.appendOrder(payload, requestID);
    }
    if (method === "POST" && route === "/internal/snapshot") return node.saveSnapshot(payload, requestID);
    if (method === "GET" && route === "/internal/orders/by-key") return node.readByKey(query["caller_id"] || "", query["idempotency_key"] || "", requestID);
    if (method === "GET" && route.startsWith("/internal/orders/")) return node.readOrder(route.slice(route.lastIndexOf("/") + 1), query["caller_id"] || "", requestID);
    if (method === "POST" && route === "/internal/jobs/claim") {
      const workerID = asString(payload["worker_id"]) || "worker";
      return node.claim(workerID, requestID);
    }
    if (method === "POST" && route.startsWith("/internal/jobs/") && route.endsWith("/ack")) return node.finish(jobIDFrom(route), requestID, true, "");
    if (method === "POST" && route.startsWith("/internal/jobs/") && route.endsWith("/fail")) {
      const message = asString(payload["error"]) || "履约失败";
      return node.finish(jobIDFrom(route), requestID, false, message);
    }
    return jsonResponse(404, errorBody("not_found", "没有这个路径", requestID));
  };
}

export async function runStore(flags: Map<string, string>): Promise<void> {
  const nodeID = flags.get("node-id") ?? "";
  const port = Number(flags.get("port") ?? "0");
  const role = flags.get("role") ?? "";
  const peer = flags.get("peer") ?? "";
  const data = flags.get("data") ?? "";
  const supervisor = flags.get("supervisor") ?? "";
  const root = flags.get("root") ?? ".";
  setContext(nodeID, root);
  startFaultReader();
  const node = openStore(nodeID, role, peer, data, supervisor);
  onStop(() => {
    node.running = false;
  });
  void node.heartbeatLoop();
  void node.replicateLoop();
  void node.sweepLoop();
  logEvent("process_started", "", "info", { role, port });
  await serve(port, handler(node));
}
