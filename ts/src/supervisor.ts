import { spawn, type ChildProcess } from "child_process";
import fs from "fs";
import path from "path";
import {
  asInt,
  asMap,
  asString,
  decodeJSON,
  errorBody,
  heartbeatMs,
  leaderLostMs,
  logEvent,
  promotionPauseMs,
  scaleCooldownMs,
  scaleInHoldMs,
  scaleOutHoldMs,
  scaleOutPending,
  scriptPath,
  setContext,
  sleep,
  startFaultReader,
  workerMax,
  workerMin,
  type JsonMap,
} from "./common";
import {
  emptyResponse,
  headerRequestID,
  httpRequest,
  jsonResponse,
  serve,
  splitPath,
  stopped,
  stopSignal,
} from "./http";

const storeA = "http://127.0.0.1:8301";
const storeB = "http://127.0.0.1:8302";
const supervisorURL = "http://127.0.0.1:8090";

interface ChildProc {
  child: ChildProcess;
  code: number;
  finished: boolean;
}

class Supervisor {
  private procs = new Map<string, ChildProc>();
  private leader = storeA;
  private epoch = 1;
  private jobsPending = 0;
  private gatewayPaused = false;
  private lastPromotionError: unknown = null;
  private applied: Record<string, number> = { "store-a": 0, "store-b": 0 };
  private lastBeat: Record<string, number> = {};
  private seen: Record<string, boolean> = {};
  private ready: Record<string, boolean> = {};
  private pids: Record<string, number> = {};
  private lastError: Record<string, string> = {};
  private expected = ["gateway", "order-1", "order-2", "recommender", "store-a", "store-b", "worker-1"];
  private workerCount = 1;
  private highSince = 0;
  private zeroSince = 0;
  private highSet = false;
  private zeroSet = false;
  private lastScale = 0;
  private promoting = false;
  private armed = false;

  constructor(private readonly root: string, private readonly exe: string, private readonly script: string) {}

  private command(name: string): string[] {
    const data = path.join(this.root, "runtime", "data");
    const common = ["--root", this.root];
    const base = [this.exe, this.script];
    if (name === "store-a") return [...base, "store", "--node-id", "store-a", "--port", "8301", "--role", "leader", "--peer", storeB, "--data", path.join(data, "store-a.sqlite"), "--supervisor", supervisorURL, ...common];
    if (name === "store-b") return [...base, "store", "--node-id", "store-b", "--port", "8302", "--role", "follower", "--peer", storeA, "--data", path.join(data, "store-b.sqlite"), "--supervisor", supervisorURL, ...common];
    if (name === "gateway") return [...base, "gateway", "--port", "8080", "--supervisor", supervisorURL, "--orders", "http://127.0.0.1:8101,http://127.0.0.1:8102", "--process-name", "gateway", ...common];
    if (name === "order-1") return [...base, "order", "--process-name", "order-1", "--port", "8101", "--supervisor", supervisorURL, "--recommender", "http://127.0.0.1:8200", ...common];
    if (name === "order-2") return [...base, "order", "--process-name", "order-2", "--port", "8102", "--supervisor", supervisorURL, "--recommender", "http://127.0.0.1:8200", ...common];
    if (name === "recommender") return [...base, "recommender", "--port", "8200", "--process-name", "recommender", ...common];
    const index = Number(name.slice("worker-".length));
    return [...base, "worker", "--process-name", name, "--port", String(8400 + index), "--supervisor", supervisorURL, ...common];
  }

  private spawnChild(name: string): void {
    if (stopped()) return;
    const existing = this.procs.get(name);
    if (existing && !existing.finished) return;
    const args = this.command(name);
    const child = spawn(args[0], args.slice(1), {
      stdio: "inherit",
      env: { ...process.env, ORANGE_ROOT: this.root, ORANGE_PROCESS: name },
    });
    const proc: ChildProc = { child, code: 0, finished: false };
    child.on("exit", (code) => {
      proc.code = code ?? 1;
      proc.finished = true;
    });
    child.on("error", (error) => {
      this.lastError[name] = error.message;
      proc.code = 1;
      proc.finished = true;
    });
    this.procs.set(name, proc);
    if (child.pid) this.pids[name] = child.pid;
    logEvent("spawned", "", "info", { process_name: name, pid: child.pid ?? 0 });
  }

  private async stopProcess(name: string): Promise<void> {
    const proc = this.procs.get(name);
    if (!proc || proc.finished) return;
    proc.child.kill("SIGTERM");
    const exited = new Promise<void>((resolve) => proc.child.once("exit", () => resolve()));
    const timeout = sleep(2000).then(() => false);
    const done = await Promise.race([exited.then(() => true), timeout]);
    if (!done && !proc.finished) proc.child.kill("SIGKILL");
    if (!proc.finished) await exited;
    this.ready[name] = false;
  }

  private async stopAll(): Promise<void> {
    for (const name of [...this.procs.keys()]) await this.stopProcess(name);
  }

  private async ensure(): Promise<void> {
    for (const name of [...this.expected]) {
      const proc = this.procs.get(name);
      if (proc?.finished) {
        this.lastError[name] = "退出码 " + proc.code;
        this.ready[name] = false;
        logEvent("process_exited", "", "error", { process_name: name, code: proc.code });
        this.procs.delete(name);
        await sleep(500);
      }
      const current = this.procs.get(name);
      if (!current || current.finished) this.spawnChild(name);
    }
  }

  private async probeReady(): Promise<void> {
    const ports: Record<string, number> = {
      gateway: 8080, "order-1": 8101, "order-2": 8102, recommender: 8200,
      "store-a": 8301, "store-b": 8302, "worker-1": 8401, "worker-2": 8402, "worker-3": 8403,
    };
    for (const name of [...this.expected]) {
      const port = ports[name] ?? 0;
      if (!port) continue;
      const result = await httpRequest("GET", `http://127.0.0.1:${port}/ready`, null, null, 300);
      this.ready[name] = result.error === null && result.status === 200;
    }
  }

  noteHeartbeat(body: JsonMap): void {
    const nodeID = asString(body["node_id"]);
    if (nodeID !== "store-a" && nodeID !== "store-b") return;
    this.lastBeat[nodeID] = Date.now();
    this.seen[nodeID] = true;
    this.applied[nodeID] = asInt(body["applied_index"]);
    if (asString(body["role"]) === "leader" && asInt(body["epoch"]) >= this.epoch) {
      this.epoch = asInt(body["epoch"]);
      this.leader = nodeID === "store-a" ? storeA : storeB;
      this.jobsPending = asInt(body["jobs_pending"]);
    }
    if (Object.keys(this.seen).length >= 2) this.armed = true;
  }

  private async maybePromote(): Promise<void> {
    if (!this.armed || this.promoting) return;
    const leaderID = this.leader === storeA ? "store-a" : "store-b";
    const followerID = this.leader === storeA ? "store-b" : "store-a";
    const leaderSeen = this.lastBeat[leaderID] ?? 0;
    const followerSeen = this.lastBeat[followerID] ?? 0;
    if (!leaderSeen || !followerSeen || Date.now() - leaderSeen <= leaderLostMs || Date.now() - followerSeen > leaderLostMs) {
      return;
    }
    this.promoting = true;
    this.gatewayPaused = true;
    const followerURL = followerID === "store-b" ? storeB : storeA;
    const newEpoch = this.epoch + 1;
    const oldLeader = this.leader;
    const started = Date.now();
    try {
      const result = await httpRequest("POST", followerURL + "/internal/promote", { new_epoch: newEpoch, peer: oldLeader }, null, promotionPauseMs);
      if (result.error) {
        this.lastPromotionError = result.error.message;
        logEvent("promoted", "", "error", { message: result.error.message });
        return;
      }
      if (result.status !== 200 || result.body.length === 0) {
        this.lastPromotionError = "提升失败 HTTP " + result.status;
        logEvent("promoted", "", "error", { message: this.lastPromotionError });
        return;
      }
      let body: JsonMap | null = null;
      try {
        body = asMap(decodeJSON(result.body));
      } catch {
        body = null;
      }
      let epoch = asInt(body?.["epoch"]);
      if (epoch === 0) epoch = newEpoch;
      this.epoch = epoch;
      this.leader = followerURL;
      this.lastPromotionError = null;
      logEvent("promoted", "", "info", { epoch: this.epoch, leader: this.leader });
    } finally {
      if (Date.now() - started > promotionPauseMs && this.lastPromotionError == null) this.lastPromotionError = "切换超过 2 秒";
      this.gatewayPaused = false;
      this.promoting = false;
    }
  }

  private async maybeScale(): Promise<void> {
    const pending = this.jobsPending;
    const now = Date.now();
    let action = "";
    let name = "";
    if (pending > scaleOutPending) {
      this.zeroSet = false;
      if (!this.highSet) {
        this.highSince = now;
        this.highSet = true;
      }
      if (now - this.highSince >= scaleOutHoldMs && now - this.lastScale >= scaleCooldownMs && this.workerCount < workerMax) {
        this.workerCount++;
        name = "worker-" + this.workerCount;
        if (!this.expected.includes(name)) this.expected.push(name);
        this.lastScale = now;
        this.highSet = false;
        action = "out";
      }
    } else this.highSet = false;
    if (pending === 0) {
      if (!this.zeroSet) {
        this.zeroSince = now;
        this.zeroSet = true;
      }
      if (now - this.zeroSince >= scaleInHoldMs && now - this.lastScale >= scaleCooldownMs && this.workerCount > workerMin) {
        name = "worker-" + this.workerCount;
        this.expected = this.expected.filter((item) => item !== name);
        this.workerCount--;
        this.lastScale = now;
        this.zeroSet = false;
        action = "in";
      }
    } else this.zeroSet = false;
    const workers = this.workerCount;
    if (action === "out") {
      this.spawnChild(name);
      logEvent("scaled", "", "info", { workers, direction: "out" });
    } else if (action === "in") {
      await this.stopProcess(name);
      logEvent("scaled", "", "info", { workers, direction: "in" });
    }
  }

  leaderView(): JsonMap {
    return { leader: this.leader, epoch: this.epoch };
  }

  controlView(): JsonMap {
    return { paused: this.gatewayPaused, jobs_pending: this.jobsPending };
  }

  status(): JsonMap {
    const ready: JsonMap = {};
    for (const name of this.expected) ready[name] = this.ready[name] === true;
    return {
      leader: this.leader,
      epoch: this.epoch,
      applied_index: { ...this.applied },
      workers: this.workerCount,
      jobs_pending: this.jobsPending,
      gateway_paused: this.gatewayPaused,
      ready,
      last_promotion_error: this.lastPromotionError,
      pids: { ...this.pids },
      last_error: { ...this.lastError },
    };
  }

  async loop(): Promise<void> {
    while (!stopped()) {
      await this.ensure();
      await this.probeReady();
      await this.maybePromote();
      await this.maybeScale();
      await Promise.race([sleep(heartbeatMs), stopSignal()]);
    }
    await this.stopAll();
  }
}

export async function runSupervisor(flags: Map<string, string>): Promise<void> {
  const root = flags.get("root") ?? ".";
  setContext("supervisor", root);
  fs.mkdirSync(path.join(root, "runtime", "data"), { recursive: true });
  fs.mkdirSync(path.join(root, "runtime", "logs"), { recursive: true });
  const faults = path.join(root, "runtime", "faults.json");
  if (!fs.existsSync(faults)) {
    fs.writeFileSync(faults, "{\"delay_ms\":{\"store\":0,\"recommender\":0},\"pause_replication\":false,\"drop_heartbeat\":{\"store-a\":false,\"store-b\":false},\"exit_after_requests\":{}}\n");
  }
  startFaultReader();
  const item = new Supervisor(root, process.execPath, scriptPath());
  void serve(8090, (method, requestPath, headers, raw) => {
    const requestID = headerRequestID(headers);
    const [route] = splitPath(requestPath);
    if (method === "GET" && route === "/health") return emptyResponse(200);
    if (method === "GET" && route === "/v1/status") return jsonResponse(200, item.status());
    if (method === "GET" && route === "/internal/leader") return jsonResponse(200, item.leaderView());
    if (method === "GET" && route === "/internal/gateway-control") return jsonResponse(200, item.controlView());
    if (method === "POST" && route === "/internal/heartbeat") {
      let parsed: JsonMap = {};
      try {
        parsed = asMap(decodeJSON(raw)) ?? {};
      } catch {
        parsed = {};
      }
      item.noteHeartbeat(parsed);
      return jsonResponse(200, item.leaderView());
    }
    return jsonResponse(404, errorBody("not_found", "没有这个路径", requestID));
  }).then(() => undefined, (error) => {
    logEvent("handler_error", "", "error", { message: error instanceof Error ? error.message : String(error) });
  });
  logEvent("process_started", "", "info", { port: 8090 });
  await sleep(50);
  await item.loop();
}
