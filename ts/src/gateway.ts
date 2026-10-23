import {
  Metrics,
  TokenBucket,
  asBool,
  asInt,
  asMap,
  decodeJSON,
  errorBody,
  gatewayInflightLimit,
  heartbeatMs,
  jobsPendingLimit,
  logEvent,
  nowMS,
  orderDeadlineMs,
  queryEscape,
  setContext,
  sleep,
  startFaultReader,
  tokenCapacity,
  tokenRefillPerSec,
  validCaller,
  validIdempotencyKey,
  validateOrderBody,
} from "./common";
import {
  emptyResponse,
  headerRequestID,
  httpRequest,
  jsonNumberText,
  jsonResponse,
  onStop,
  serve,
  splitPath,
  textResponse,
  type Handler,
  type ResponseMessage,
} from "./http";

class Gateway {
  readonly bucket = new TokenBucket(tokenCapacity, tokenRefillPerSec);
  readonly metrics = new Metrics();
  private inflight = 0;
  private cursor = 0;
  private orderReady = new Map<string, boolean>();
  private paused = false;
  private jobsPending = 0;
  private stopping = false;

  constructor(private readonly supervisor: string, private readonly orders: string[]) {
    for (const item of orders) this.orderReady.set(item, false);
  }

  stop(): void {
    this.stopping = true;
  }

  inflightCount(): number {
    return this.inflight;
  }

  async poll(): Promise<void> {
    while (!this.stopping) {
      for (const item of this.orders) {
        const result = await httpRequest("GET", item + "/ready", null, null, 300);
        this.orderReady.set(item, result.error === null && result.status === 200);
      }
      const control = await httpRequest("GET", this.supervisor + "/internal/gateway-control", null, null, 300);
      if (control.error === null && control.status === 200) {
        try {
          const parsed = asMap(decodeJSON(control.body));
          this.paused = asBool(parsed?.["paused"]);
          this.jobsPending = asInt(parsed?.["jobs_pending"]);
        } catch {
          // 控制面暂时没有一份可读的 JSON。
        }
      }
      await sleepUntil(heartbeatMs, () => this.stopping);
    }
  }

  pick(): string {
    if (this.paused) return "";
    const ready = this.orders.filter((item) => this.orderReady.get(item));
    if (ready.length === 0) return "";
    const chosen = ready[this.cursor % ready.length];
    this.cursor++;
    return chosen;
  }

  ready(): boolean {
    if (this.paused) return false;
    for (const item of this.orderReady.values()) if (item) return true;
    return false;
  }

  reject(status: number, code: string, message: string, requestID: string, retryAfter: number, withRetry: boolean): ResponseMessage {
    this.metrics.inc("orange_errors_total", 1);
    if (code === "rate_limited") logEvent("rate_limited", requestID, "info", { message });
    const extra: Record<string, string> = {};
    if (withRetry) extra["Retry-After"] = jsonNumberText(retryAfter);
    return jsonResponse(status, errorBody(code, message, requestID), extra);
  }

  async createOrder(headers: Record<string, string>, raw: Buffer, requestID: string): Promise<ResponseMessage> {
    const caller = headers["x-caller-id"] || "";
    const key = headers["idempotency-key"] || "";
    if (!validCaller(caller)) return jsonResponse(400, errorBody("invalid_argument", "X-Caller-Id 不合法", requestID));
    if (!validIdempotencyKey(key)) return jsonResponse(400, errorBody("invalid_argument", "Idempotency-Key 不合法", requestID));
    let parsed: unknown;
    try {
      parsed = decodeJSON(raw);
    } catch {
      return jsonResponse(400, errorBody("invalid_argument", "正文不是 JSON", requestID));
    }
    const [fields, message] = validateOrderBody(parsed);
    if (!fields) return jsonResponse(400, errorBody("invalid_argument", message, requestID));
    const [allowed, retryAfter] = this.bucket.take();
    if (!allowed) return this.reject(429, "rate_limited", "令牌桶已空", requestID, retryAfter, true);
    if (this.jobsPending >= jobsPendingLimit) return this.reject(429, "rate_limited", "待处理任务已达上限", requestID, 1, true);
    if (this.paused) return this.reject(503, "unavailable", "主存储正在切换", requestID, 0, false);
    if (this.inflight >= gatewayInflightLimit) return this.reject(429, "rate_limited", "在途请求已满", requestID, 1, true);
    this.inflight++;
    this.metrics.set("orange_inflight", this.inflight);
    try {
      const deadline = nowMS() + orderDeadlineMs;
      const forward = {
        "X-Caller-Id": caller,
        "Idempotency-Key": key,
        "X-Request-Id": requestID,
        "X-Deadline-Ms": jsonNumberText(deadline),
      };
      for (let attempt = 0; attempt < this.orders.length; attempt++) {
        const target = this.pick();
        if (!target) return this.reject(503, "unavailable", "没有就绪的订单副本", requestID, 0, false);
        const result = await httpRequest("POST", target + "/internal/orders", fields, forward, orderDeadlineMs + 200);
        if (result.error) {
          this.metrics.inc("orange_errors_total", 1);
          continue;
        }
        const extra: Record<string, string> = { "X-Request-Id": requestID };
        if (result.headers["retry-after"]) extra["Retry-After"] = result.headers["retry-after"];
        extra["Content-Type"] = result.headers["content-type"] || "application/json";
        logEvent("gateway_forward", requestID, "info", { status: result.status });
        return { status: result.status, headers: extra, body: result.body };
      }
      return this.reject(503, "unavailable", "没有就绪的订单副本", requestID, 0, false);
    } finally {
      this.inflight--;
      this.metrics.set("orange_inflight", this.inflight);
    }
  }

  async forwardGet(route: string, query: Record<string, string>, headers: Record<string, string>, requestID: string): Promise<ResponseMessage> {
    const caller = headers["x-caller-id"] || "";
    if (!validCaller(caller)) return jsonResponse(400, errorBody("invalid_argument", "X-Caller-Id 不合法", requestID));
    if (route === "/internal/orders" && !query["idempotency_key"]) {
      return jsonResponse(400, errorBody("invalid_argument", "缺少 idempotency_key", requestID));
    }
    const target = this.pick();
    if (!target) return jsonResponse(503, errorBody("unavailable", "没有就绪的订单副本", requestID));
    const pairs = Object.entries(query).map(([key, value]) => `${queryEscape(key)}=${queryEscape(value)}`);
    const queryText = pairs.length > 0 ? "?" + pairs.join("&") : "";
    const result = await httpRequest(
      "GET",
      target + route + queryText,
      null,
      {
        "X-Caller-Id": caller,
        "X-Request-Id": requestID,
        "X-Deadline-Ms": jsonNumberText(nowMS() + orderDeadlineMs),
      },
      1000,
    );
    if (result.error) return jsonResponse(503, errorBody("unavailable", "没有就绪的订单副本", requestID));
    return {
      status: result.status,
      headers: { "Content-Type": "application/json; charset=utf-8", "X-Request-Id": requestID },
      body: result.body,
    };
  }
}

async function sleepUntil(ms: number, done: () => boolean): Promise<void> {
  let left = ms;
  while (left > 0 && !done()) {
    await sleep(Math.min(50, left));
    left -= 50;
  }
}

function handler(gate: Gateway): Handler {
  return async (method, path, headers, raw) => {
    const requestID = headerRequestID(headers);
    const [route, query] = splitPath(path);
    if (method === "GET" && route === "/health") return emptyResponse(200);
    if (method === "GET" && route === "/ready") return gate.ready() ? emptyResponse(200) : emptyResponse(503);
    if (method === "GET" && route === "/metrics") {
      gate.metrics.set("orange_inflight", gate.inflightCount());
      return textResponse(200, gate.metrics.render(["orange_errors_total", "orange_inflight"]));
    }
    if (method === "POST" && route === "/v1/orders") return gate.createOrder(headers, raw, requestID);
    if (method === "GET" && route === "/v1/orders") return gate.forwardGet("/internal/orders", query, headers, requestID);
    if (method === "GET" && route.startsWith("/v1/orders/")) {
      return gate.forwardGet("/internal/orders/" + route.slice("/v1/orders/".length), {}, headers, requestID);
    }
    return jsonResponse(404, errorBody("not_found", "没有这个路径", requestID));
  };
}

export async function runGateway(flags: Map<string, string>): Promise<void> {
  const port = Number(flags.get("port") ?? "8080");
  const supervisor = flags.get("supervisor") ?? "";
  const root = flags.get("root") ?? ".";
  const name = flags.get("process-name") ?? "gateway";
  setContext(name, root);
  startFaultReader();
  const listed = (flags.get("orders") ?? "").split(",").filter((item) => item !== "");
  const gate = new Gateway(supervisor, listed);
  void gate.poll();
  logEvent("process_started", "", "info", { port });
  onStop(() => gate.stop());
  await serve(port, handler(gate));
}
