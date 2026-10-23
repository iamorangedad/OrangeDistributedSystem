import { spawn, type ChildProcess } from "child_process";
import fs from "fs";
import http from "http";
import os from "os";
import path from "path";
import assert from "node:assert/strict";
import { describe, test } from "node:test";

const gateway = "http://127.0.0.1:8080";
const bin = process.env.ORANGE_BIN || "";

interface Result {
  status: number;
  headers: Record<string, string>;
  body: Record<string, unknown>;
  raw: string;
}

function text(node: Record<string, unknown> | undefined, key: string): string {
  const value = node?.[key];
  return typeof value === "string" ? value : "";
}

function numberValue(value: unknown): number {
  return typeof value === "number" ? value : 0;
}

function asMap(value: unknown): Record<string, unknown> {
  if (value && typeof value === "object" && !Array.isArray(value)) return value as Record<string, unknown>;
  return {};
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function request(method: string, rawURL: string, body: unknown, headers: Record<string, string> | null, timeoutMs: number): Promise<Result> {
  return new Promise((resolve) => {
    let payload: Buffer | undefined;
    if (body !== null && body !== undefined) payload = Buffer.from(JSON.stringify(body));
    const url = new URL(rawURL);
    const req = http.request(
      {
        protocol: url.protocol,
        hostname: url.hostname,
        port: url.port,
        path: `${url.pathname}${url.search}`,
        method,
        headers: {
          Connection: "close",
          ...(payload ? { "Content-Type": "application/json", "Content-Length": String(payload.length) } : {}),
          ...(headers ?? {}),
        },
        agent: false,
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (chunk: Buffer) => chunks.push(chunk));
        res.on("end", () => {
          const raw = Buffer.concat(chunks).toString("utf8");
          const out: Record<string, string> = {};
          for (const [key, value] of Object.entries(res.headers)) {
            if (Array.isArray(value)) out[key] = value[0] ?? "";
            else if (typeof value === "string") out[key] = value;
          }
          let parsed: Record<string, unknown> = {};
          if (raw) {
            try {
              const value = JSON.parse(raw) as unknown;
              if (value && typeof value === "object" && !Array.isArray(value)) parsed = value as Record<string, unknown>;
            } catch {
              parsed = {};
            }
          }
          resolve({ status: res.statusCode ?? 0, headers: out, body: parsed, raw });
        });
      },
    );
    req.setTimeout(timeoutMs, () => req.destroy(new Error("timeout")));
    req.on("error", (error) => resolve({ status: 0, headers: {}, body: {}, raw: error.message }));
    if (payload) req.end(payload);
    else req.end();
  });
}

function post(sku: string, key: string, extra?: Record<string, unknown>): Promise<Result> {
  return request("POST", gateway + "/v1/orders", { sku, qty: 1, ...(extra ?? {}) }, {
    "X-Caller-Id": "gear-card",
    "Idempotency-Key": key,
    "X-Request-Id": "req-" + Date.now().toString() + "-" + Math.floor(Math.random() * 1000),
  }, 3000);
}

class Cluster {
  constructor(readonly root: string, readonly child: ChildProcess, readonly logPath: string) {}

  async status(): Promise<Record<string, unknown>> {
    const result = await request("GET", "http://127.0.0.1:8090/v1/status", null, null, 1000);
    return result.body;
  }

  async faults(storeDelay: number, recommendDelay: number, dropA: boolean): Promise<void> {
    const payload = {
      delay_ms: { store: storeDelay, recommender: recommendDelay },
      pause_replication: false,
      drop_heartbeat: { "store-a": dropA, "store-b": false },
      exit_after_requests: {},
    };
    fs.writeFileSync(path.join(this.root, "runtime", "faults.json"), JSON.stringify(payload));
    await sleep(600);
  }

  logs(name: string): string {
    try {
      return fs.readFileSync(path.join(this.root, "runtime", "logs", name + ".log"), "utf8");
    } catch {
      return "";
    }
  }

  diagnostics(): string {
    const parts: string[] = [];
    try {
      parts.push(fs.readFileSync(this.logPath, "utf8"));
    } catch {
      // 监督者还没有标准输出。
    }
    const directory = path.join(this.root, "runtime", "logs");
    if (fs.existsSync(directory)) {
      for (const entry of fs.readdirSync(directory)) {
        parts.push("----- " + entry + " -----\n" + fs.readFileSync(path.join(directory, entry), "utf8"));
      }
    }
    return parts.join("\n") || "监督者没有留下日志";
  }

  async stop(): Promise<void> {
    if (this.child.pid) {
      try {
        process.kill(-this.child.pid, "SIGTERM");
      } catch {
        // 进程组可能已经退出。
      }
      const exited = new Promise<void>((resolve) => this.child.once("exit", () => resolve()));
      const done = await Promise.race([exited.then(() => true), sleep(8000).then(() => false)]);
      if (!done && this.child.pid) {
        try {
          process.kill(-this.child.pid, "SIGKILL");
        } catch {
          // 已经不在了。
        }
        await exited;
      }
    }
    await sleep(300);
    fs.rmSync(this.root, { recursive: true, force: true });
  }
}

async function startCluster(): Promise<Cluster> {
  assert.ok(bin, "缺少 ORANGE_BIN");
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "orange-ts-"));
  fs.mkdirSync(path.join(root, "runtime"), { recursive: true });
  const logPath = path.join(root, "runtime", "supervisor.out");
  const logFd = fs.openSync(logPath, "w");
  const child = spawn(bin, ["supervisor", "--root", root], { detached: true, stdio: ["ignore", logFd, logFd] });
  const item = new Cluster(root, child, logPath);
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    const ready = await request("GET", gateway + "/ready", null, null, 1000);
    if (ready.status === 200) return item;
    if (child.exitCode !== null) break;
    await sleep(200);
  }
  const textBody = item.diagnostics();
  await item.stop();
  throw new Error(textBody);
}

class Fulfill {
  readonly url: string;
  private calls: string[] = [];
  private applied: string[] = [];
  private releaseWaiters: Array<() => void> = [];
  private released = false;
  constructor(private readonly server: http.Server, address: string) {
    this.url = "http://" + address + "/hooks/orange-fulfill";
  }

  release(): void {
    if (this.released) return;
    this.released = true;
    for (const wake of this.releaseWaiters) wake();
  }

  count(): number {
    return this.calls.length;
  }

  appliedCount(): number {
    return this.applied.length;
  }

  note(orderID: string, first: boolean): Promise<void> {
    this.calls.push(orderID);
    return new Promise((resolve) => {
      if (this.hangFirst && first) {
        if (this.released) resolve();
        else {
          const timer = setTimeout(resolve, 30_000);
          this.releaseWaiters.push(() => {
            clearTimeout(timer);
            resolve();
          });
        }
      } else if (this.delayMs > 0) setTimeout(resolve, this.delayMs);
      else resolve();
    });
  }

  mark(orderID: string, status: number): void {
    if ((status >= 200 && status < 300) || status === 409) {
      if (!this.applied.includes(orderID)) this.applied.push(orderID);
    }
  }

  hangFirst = false;
  delayMs = 0;

  close(): void {
    this.release();
    this.server.close();
  }
}

function startFulfill(status: number, delayMs: number, hangFirst: boolean): Promise<Fulfill> {
  return new Promise((resolve, reject) => {
    let item: Fulfill | undefined;
    const server = http.createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk: Buffer) => chunks.push(chunk));
      req.on("end", () => {
        void (async () => {
          if (!item) return;
          let orderID = "";
          try {
            const payload = JSON.parse(Buffer.concat(chunks).toString("utf8")) as { order_id?: string };
            orderID = payload.order_id || "";
          } catch {
            orderID = "";
          }
          const first = item.count() === 0;
          await item.note(orderID, first);
          if (!res.writableEnded) {
            res.writeHead(status, { "Content-Length": "2" });
            res.end("{}");
          }
          item.mark(orderID, status);
        })();
      });
    });
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        reject(new Error("listen"));
        return;
      }
      item = new Fulfill(server, `127.0.0.1:${address.port}`);
      item.hangFirst = hangFirst;
      item.delayMs = delayMs;
      resolve(item);
    });
  });
}

describe("acceptance", { concurrency: 1 }, () => {
  test("AC-1 same key returns the same order", async () => {
    const cluster = await startCluster();
    try {
      const first = await post("gear-1", "same-key-0001");
      const second = await post("gear-1", "same-key-0001");
      assert.equal(first.status, 201, first.raw + cluster.diagnostics());
      assert.equal(second.status, 201, second.raw);
      assert.equal(text(first.body, "order_id"), text(second.body, "order_id"));
    } finally {
      await cluster.stop();
    }
  });

  test("AC-2 mismatched body conflicts", async () => {
    const cluster = await startCluster();
    try {
      const first = await post("gear-1", "same-key-0002");
      assert.equal(first.status, 201, first.raw + cluster.diagnostics());
      const second = await post("gear-2", "same-key-0002");
      assert.equal(second.status, 409);
      assert.equal(text(asMap(second.body["error"]), "code"), "idempotency_mismatch");
      const got = await request("GET", gateway + "/v1/orders/" + text(first.body, "order_id"), null, { "X-Caller-Id": "gear-card" }, 2000);
      assert.equal(got.status, 200, got.raw);
      assert.equal(text(got.body, "sku"), "gear-1");
    } finally {
      await cluster.stop();
    }
  });

  test("AC-3 order process restarts", async () => {
    const cluster = await startCluster();
    try {
      const pid = numberValue(asMap((await cluster.status())["pids"])["order-2"]);
      process.kill(pid, "SIGTERM");
      let sawDown = false;
      let restored = false;
      const deadline = Date.now() + 8000;
      while (Date.now() < deadline) {
        const view = await cluster.status();
        const ready = asMap(view["ready"])["order-2"] === true;
        sawDown = sawDown || !ready;
        const next = numberValue(asMap(view["pids"])["order-2"]);
        if (ready && next !== pid) {
          restored = true;
          break;
        }
        await sleep(100);
      }
      const body = await post("gear-1", "after-order-b");
      assert.equal(body.status, 201, body.raw + cluster.diagnostics());
      assert.equal(sawDown, true);
      assert.equal(restored, true);
    } finally {
      await cluster.stop();
    }
  });

  test("AC-4 recommender loss degrades", async () => {
    const cluster = await startCluster();
    try {
      const pid = numberValue(asMap((await cluster.status())["pids"])["recommender"]);
      process.kill(pid, "SIGTERM");
      let body = await post("gear-1", "degrade-" + Date.now());
      const deadline = Date.now() + 3000;
      while (Date.now() < deadline) {
        body = await post("gear-1", "degrade-" + Date.now());
        if (body.status === 201 && body.body["degraded"] === true && body.body["recommendation"] === null) break;
        await sleep(50);
      }
      assert.equal(body.status, 201, body.raw + cluster.diagnostics());
      assert.equal(body.body["degraded"], true);
      assert.equal(body.body["recommendation"], null);
    } finally {
      await cluster.stop();
    }
  });

  test("AC-5 breaker stops recommendation", async () => {
    const cluster = await startCluster();
    try {
      await cluster.faults(0, 500, false);
      for (let i = 0; i < 12; i++) {
        const body = await post("gear-1", `breaker-${String(i).padStart(2, "0")}-key`);
        assert.equal(body.status, 201, body.raw + cluster.diagnostics());
      }
      await sleep(300);
      const before = cluster.logs("recommender").split('"event":"recommend_served"').length - 1;
      const afterOrder = await post("gear-1", "breaker-after-open");
      assert.equal(afterOrder.status, 201, afterOrder.raw);
      await sleep(300);
      const after = cluster.logs("recommender").split('"event":"recommend_served"').length - 1;
      assert.equal(before, after, cluster.logs("recommender"));
      assert.ok((cluster.logs("order-1") + cluster.logs("order-2")).includes('"event":"breaker_open"'));
    } finally {
      await cluster.stop();
    }
  });

  test("AC-6 token bucket limits", async () => {
    const cluster = await startCluster();
    try {
      await cluster.faults(350, 0, false);
      const holding: Array<Promise<Result>> = [];
      for (let i = 0; i < 8; i++) holding.push(post("gear-1", `hold-${String(i).padStart(2, "0")}-xxxx`));
      const deadline = Date.now() + 2000;
      while (Date.now() < deadline) {
        const metrics = await request("GET", gateway + "/metrics", null, null, 1000);
        if (metrics.raw.includes("orange_inflight 8")) break;
        await sleep(20);
      }
      const results = await Promise.all(Array.from({ length: 40 }, (_, index) => post("gear-1", `bucket-${String(index).padStart(2, "0")}-xx`)));
      await Promise.all(holding);
      const limited = results.some((item) => item.status === 429 && text(asMap(item.body["error"]), "message") === "令牌桶已空" && item.headers["retry-after"]);
      assert.equal(limited, true, results.map((item) => item.status + " " + text(asMap(item.body["error"]), "message")).join("; "));
    } finally {
      await cluster.stop();
    }
  });

  test("AC-7 visibility timeout retries fulfillment", async () => {
    const cluster = await startCluster();
    const fulfill = await startFulfill(200, 0, true);
    try {
      const body = await post("gear-1", "visibility-timeout", { fulfill_url: fulfill.url });
      assert.equal(body.status, 201, body.raw + cluster.diagnostics());
      const seen = Date.now() + 5000;
      while (Date.now() < seen && fulfill.count() === 0) await sleep(50);
      assert.ok(fulfill.count() > 0, cluster.diagnostics());
      const pid = numberValue(asMap((await cluster.status())["pids"])["worker-1"]);
      process.kill(pid, "SIGTERM");
      fulfill.release();
      let state = "";
      const deadline = Date.now() + 25_000;
      while (Date.now() < deadline) {
        const got = await request("GET", gateway + "/v1/orders/" + text(body.body, "order_id"), null, { "X-Caller-Id": "gear-card" }, 2000);
        assert.equal(got.status, 200, got.raw);
        state = text(got.body, "state");
        if (state === "fulfilled") break;
        await sleep(500);
      }
      assert.equal(state, "fulfilled", cluster.diagnostics());
      assert.equal(fulfill.appliedCount(), 1);
    } finally {
      fulfill.close();
      await cluster.stop();
    }
  });

  test("AC-8 dead letter", async () => {
    const cluster = await startCluster();
    const fulfill = await startFulfill(500, 0, false);
    try {
      const body = await post("gear-1", "dead-letter-0001", { fulfill_url: fulfill.url });
      assert.equal(body.status, 201, body.raw);
      const deadline = Date.now() + 20_000;
      let done = false;
      while (Date.now() < deadline) {
        const got = await request("GET", gateway + "/v1/orders/" + text(body.body, "order_id"), null, { "X-Caller-Id": "gear-card" }, 2000);
        if (text(got.body, "state") === "fulfillment_failed") {
          const fulfillment = asMap(got.body["fulfillment"]);
          assert.equal(text(fulfillment, "status"), "dead");
          assert.ok(numberValue(fulfillment["attempts"]) >= 5, got.raw);
          done = true;
          break;
        }
        await sleep(300);
      }
      assert.equal(done, true, cluster.diagnostics());
    } finally {
      fulfill.close();
      await cluster.stop();
    }
  });

  test("AC-9 epoch fences the old leader", async () => {
    const cluster = await startCluster();
    try {
      const original = text(await cluster.status(), "leader");
      await cluster.faults(0, 0, true);
      let promoted = false;
      let deadline = Date.now() + 15000;
      while (Date.now() < deadline) {
        const view = await cluster.status();
        if (text(view, "leader") !== original && numberValue(view["epoch"]) > 1) {
          promoted = true;
          break;
        }
        await sleep(200);
      }
      assert.equal(promoted, true, cluster.diagnostics());
      await cluster.faults(0, 0, false);
      let fenced = false;
      deadline = Date.now() + 5000;
      const payload = { epoch: 1, caller_id: "gear-card", idempotency_key: "fenced-write-01", order: { sku: "gear-1", qty: 1, external_ref: null, fulfill_url: null } };
      while (Date.now() < deadline) {
        const result = await request("POST", original + "/internal/append", payload, null, 1000);
        if (result.status === 409 && text(asMap(result.body["error"]), "code") === "epoch_fenced") {
          fenced = true;
          break;
        }
        await sleep(200);
      }
      assert.equal(fenced, true, cluster.diagnostics());
      const created = await post("gear-1", "after-promotion");
      assert.equal(created.status, 201, created.raw + cluster.diagnostics());
      assert.ok(numberValue(created.body["epoch"]) > 1);
    } finally {
      await cluster.stop();
    }
  });

  test("AC-10 backlog rejects and workers continue", async () => {
    const cluster = await startCluster();
    const fulfill = await startFulfill(500, 900, false);
    try {
      let limited = false;
      let created = false;
      for (let i = 0; i < 70; i++) {
        const body = await post("gear-1", `pile-${String(i).padStart(2, "0")}-order`, { fulfill_url: fulfill.url });
        if (body.status === 201) {
          created = true;
          continue;
        }
        const message = text(asMap(body.body["error"]), "message");
        if (body.status === 429 && message === "待处理任务已达上限") {
          limited = true;
          break;
        }
        if (body.status === 429 && message === "令牌桶已空") {
          await sleep(200);
          continue;
        }
        assert.fail(body.status + " " + body.raw + cluster.diagnostics());
      }
      assert.equal(limited && created, true, cluster.diagnostics());
      const before = cluster.logs("store-a").split('"event":"job_claimed"').length - 1;
      const deadline = Date.now() + 8000;
      let grew = false;
      while (Date.now() < deadline) {
        if (cluster.logs("store-a").split('"event":"job_claimed"').length - 1 > before) {
          grew = true;
          break;
        }
        await sleep(300);
      }
      assert.equal(grew, true, cluster.diagnostics());
    } finally {
      fulfill.close();
      await cluster.stop();
    }
  });

  test("AC-11 scales workers to three", async () => {
    const cluster = await startCluster();
    const fulfill = await startFulfill(500, 900, false);
    try {
      for (let i = 0; i < 40; i++) {
        for (;;) {
          const body = await post("gear-1", `scale-${String(i).padStart(2, "0")}-order`, { fulfill_url: fulfill.url });
          if (body.status === 429 && text(asMap(body.body["error"]), "message") === "令牌桶已空") {
            await sleep(200);
            continue;
          }
          assert.equal(body.status, 201, body.raw + cluster.diagnostics());
          break;
        }
      }
      const deadline = Date.now() + 40_000;
      while (Date.now() < deadline) {
        if (numberValue((await cluster.status())["workers"]) >= 3) break;
        await sleep(500);
      }
      assert.ok(numberValue((await cluster.status())["workers"]) >= 3, cluster.diagnostics());
      await sleep(10_000);
      assert.equal(numberValue((await cluster.status())["workers"]), 3, cluster.diagnostics());
    } finally {
      fulfill.close();
      await cluster.stop();
    }
  });

  test("AC-12 request id crosses processes", async () => {
    const cluster = await startCluster();
    try {
      await cluster.faults(200, 0, false);
      const requestID = "req-trace-ac12-0001";
      const result = await request("POST", gateway + "/v1/orders", { sku: "gear-1", qty: 1 }, {
        "X-Caller-Id": "gear-card",
        "Idempotency-Key": "trace-request-001",
        "X-Request-Id": requestID,
      }, 3000);
      assert.equal(result.status, 201, result.raw);
      await sleep(300);
      assert.ok(cluster.logs("gateway").includes(requestID), cluster.logs("gateway"));
      assert.ok((cluster.logs("order-1") + cluster.logs("order-2")).includes(requestID));
      assert.ok((cluster.logs("store-a") + cluster.logs("store-b")).includes(requestID), cluster.diagnostics());
    } finally {
      await cluster.stop();
    }
  });
});
