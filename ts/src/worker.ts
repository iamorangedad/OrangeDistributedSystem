import {
  asMap,
  asString,
  decodeJSON,
  fulfillTimeoutMs,
  heartbeatMs,
  logEvent,
  newID,
  setContext,
  sleep,
  startFaultReader,
} from "./common";
import { emptyResponse, httpRequest, onStop, serve, splitPath, textResponse, type Handler } from "./http";

class Worker {
  private leader = "";
  private checkedAt = 0;
  private errors = 0;
  private stopping = false;

  constructor(readonly name: string, private readonly supervisor: string) {}

  stop(): void {
    this.stopping = true;
  }

  async refresh(): Promise<void> {
    while (!this.stopping) {
      const result = await httpRequest("GET", this.supervisor + "/internal/leader", null, null, 300);
      if (!result.error && result.status === 200) {
        try {
          const body = asMap(decodeJSON(result.body));
          this.leader = asString(body?.["leader"]);
          this.checkedAt = Date.now();
        } catch {
          // 监督者这次没有给出可读的主地址。
        }
      }
      await sleep(heartbeatMs);
    }
  }

  leaderNow(): string {
    if (this.leader && Date.now() - this.checkedAt < 1000) return this.leader;
    return "";
  }

  errorCount(): number {
    return this.errors;
  }

  async run(): Promise<void> {
    while (!this.stopping) {
      const leader = this.leaderNow();
      if (!leader) {
        await sleep(heartbeatMs);
        continue;
      }
      const requestID = newID("req_");
      const result = await httpRequest("POST", leader + "/internal/jobs/claim", { worker_id: this.name }, { "X-Request-Id": requestID }, 1000);
      if (result.error || result.status !== 200 || result.body.length === 0) {
        if (result.status !== 204) this.errors++;
        await sleep(heartbeatMs);
        continue;
      }
      let object: ReturnType<typeof asMap> = null;
      try {
        object = asMap(decodeJSON(result.body));
      } catch {
        object = null;
      }
      if (!object) {
        this.errors++;
        await sleep(heartbeatMs);
        continue;
      }
      const [ok, message] = await this.fulfill(object, requestID);
      const path = ok ? "/ack" : "/fail";
      const body = ok ? null : { retryable: true, error: message };
      const done = await httpRequest("POST", leader + "/internal/jobs/" + asString(object["job_id"]) + path, body, { "X-Request-Id": requestID }, 1000);
      if (done.error) this.errors++;
    }
  }

  async fulfill(job: Record<string, unknown>, requestID: string): Promise<[boolean, string]> {
    const target = asString(job["fulfill_url"]);
    if (!target) return [true, ""];
    const result = await httpRequest(
      "POST",
      target,
      {
        event: "fulfill",
        order_id: job["order_id"],
        caller_id: job["caller_id"],
        external_ref: job["external_ref"],
        sku: job["sku"],
        qty: job["qty"],
      },
      { "X-Request-Id": requestID },
      fulfillTimeoutMs,
    );
    if (result.error) return [false, result.error.message];
    if ((result.status >= 200 && result.status < 300) || result.status === 409) return [true, ""];
    return [false, "履约地址返回 " + result.status];
  }
}

function handler(worker: Worker): Handler {
  return (method, path) => {
    const [route] = splitPath(path);
    if (method === "GET" && route === "/health") return emptyResponse(200);
    if (method === "GET" && route === "/ready") return worker.leaderNow() ? emptyResponse(200) : emptyResponse(503);
    if (method === "GET" && route === "/metrics") return textResponse(200, "orange_errors_total " + worker.errorCount() + "\n");
    return emptyResponse(404);
  };
}

export async function runWorker(flags: Map<string, string>): Promise<void> {
  const name = flags.get("process-name") ?? "";
  const port = Number(flags.get("port") ?? "0");
  const supervisor = flags.get("supervisor") ?? "";
  const root = flags.get("root") ?? ".";
  setContext(name, root);
  startFaultReader();
  const item = new Worker(name, supervisor);
  void item.refresh();
  void item.run();
  logEvent("process_started", "", "info", { port });
  onStop(() => item.stop());
  await serve(port, handler(item));
}
