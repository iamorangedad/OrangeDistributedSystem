import {
  CircuitBreaker,
  Metrics,
  asBool,
  asInt,
  asMap,
  asString,
  canonicalBody,
  decodeJSON,
  errorBody,
  heartbeatMs,
  logEvent,
  nowMS,
  orderDeadlineMs,
  queryEscape,
  recommendBudgetMs,
  setContext,
  sleep,
  startFaultReader,
  storeCallCapMs,
  validCaller,
  validIdempotencyKey,
  validateOrderBody,
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

class OrderService {
  readonly breaker = new CircuitBreaker();
  readonly metrics = new Metrics();
  private leader = "";
  private epoch = 1;
  private leaderOK = false;
  private checkedAt = 0;
  private stopping = false;

  constructor(readonly name: string, private readonly supervisor: string, readonly recommender: string) {}

  stop(): void {
    this.stopping = true;
  }

  async refreshOnce(): Promise<void> {
    const result = await httpRequest("GET", this.supervisor + "/internal/leader", null, null, 300);
    if (result.error || result.status !== 200) return;
    let parsed: JsonMap | null = null;
    try {
      parsed = asMap(decodeJSON(result.body));
    } catch {
      return;
    }
    const leader = asString(parsed?.["leader"]);
    let ready = false;
    if (leader) {
      const probe = await httpRequest("GET", leader + "/ready", null, null, 300);
      ready = probe.error === null && probe.status === 200;
    }
    this.leader = leader;
    const epoch = asInt(parsed?.["epoch"]);
    if (epoch !== 0) this.epoch = epoch;
    this.leaderOK = ready;
    this.checkedAt = Date.now();
  }

  async refreshLoop(): Promise<void> {
    while (!this.stopping) {
      await this.refreshOnce();
      await sleep(heartbeatMs);
    }
  }

  currentLeader(): [string, number, boolean] {
    const fresh = this.checkedAt !== 0 && Date.now() - this.checkedAt < 1000 && this.leader !== "";
    return [this.leader, this.epoch, fresh && this.leaderOK];
  }
}

async function storeCall(method: string, rawURL: string, requestID: string, deadline: number, body: unknown): Promise<[number, unknown, Error | null]> {
  const remaining = deadline - nowMS();
  if (remaining <= 0) return [504, null, null];
  const timeout = Math.min(remaining, storeCallCapMs);
  const result = await httpRequest(method, rawURL, body, { "X-Request-Id": requestID, "X-Deadline-Ms": String(deadline) }, timeout);
  if (result.error) return [0, null, result.error];
  try {
    return [result.status, decodeJSON(result.body), null];
  } catch {
    return [result.status, null, null];
  }
}

function headerMS(headers: Record<string, string>, key: string): number {
  const parsed = Number.parseInt(headers[key] || "", 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

function orderIDFrom(route: string): string {
  const slash = route.lastIndexOf("/");
  return slash < 0 ? route : route.slice(slash + 1);
}

async function proxyRead(service: OrderService, route: string, query: Record<string, string>, headers: Record<string, string>, requestID: string): Promise<ResponseMessage> {
  const caller = headers["x-caller-id"] || "";
  if (!validCaller(caller)) return jsonResponse(400, errorBody("invalid_argument", "X-Caller-Id 不合法", requestID));
  const [leader, , ok] = service.currentLeader();
  let deadline = headerMS(headers, "x-deadline-ms");
  if (deadline === 0) deadline = nowMS() + orderDeadlineMs;
  if (!ok) return jsonResponse(503, errorBody("unavailable", "主存储不可用", requestID));
  const rawURL = route === "/internal/orders"
    ? `${leader}/internal/orders/by-key?caller_id=${queryEscape(caller)}&idempotency_key=${queryEscape(query["idempotency_key"] || "")}`
    : `${leader}/internal/orders/${orderIDFrom(route)}?caller_id=${queryEscape(caller)}`;
  const [status, body, error] = await storeCall("GET", rawURL, requestID, deadline, null);
  if (error) {
    service.metrics.inc("orange_errors_total", 1);
    return jsonResponse(503, errorBody("unavailable", "主存储不可用", requestID));
  }
  if (status === 504 || body === null) return jsonResponse(504, errorBody("deadline_exceeded", "超过截止时间", requestID));
  const object = asMap(body);
  if (object) {
    delete object["request_hash"];
    return jsonResponse(status, object);
  }
  return jsonResponse(status, body);
}

async function createAccepted(service: OrderService, headers: Record<string, string>, raw: Buffer, requestID: string): Promise<ResponseMessage> {
  const caller = headers["x-caller-id"] || "";
  const key = headers["idempotency-key"] || "";
  const deadline = headerMS(headers, "x-deadline-ms");
  if (!validCaller(caller)) return jsonResponse(400, errorBody("invalid_argument", "X-Caller-Id 不合法", requestID));
  if (!validIdempotencyKey(key)) return jsonResponse(400, errorBody("invalid_argument", "Idempotency-Key 不合法", requestID));
  if (deadline !== 0 && nowMS() >= deadline) {
    logEvent("deadline_exceeded", requestID, "info", null);
    return jsonResponse(504, errorBody("deadline_exceeded", "超过截止时间", requestID));
  }
  let parsed: unknown;
  try {
    parsed = decodeJSON(raw);
  } catch {
    return jsonResponse(400, errorBody("invalid_argument", "正文不是 JSON", requestID));
  }
  const [fields, message] = validateOrderBody(parsed);
  if (!fields) return jsonResponse(400, errorBody("invalid_argument", message, requestID));
  service.metrics.inc("orange_inflight", 1);
  try {
    const replay = await lookupExisting(service, caller, key, fields, requestID, deadline);
    if (replay.status !== 0) return replay;
    return await writeNew(service, caller, key, fields, requestID, deadline);
  } finally {
    service.metrics.inc("orange_inflight", -1);
  }
}

async function lookupExisting(service: OrderService, caller: string, key: string, fields: JsonMap, requestID: string, deadline: number): Promise<ResponseMessage> {
  const [leader, , ok] = service.currentLeader();
  if (!ok) return jsonResponse(503, errorBody("unavailable", "主存储不可用", requestID));
  if (deadline === 0) deadline = nowMS() + orderDeadlineMs;
  const rawURL = `${leader}/internal/orders/by-key?caller_id=${queryEscape(caller)}&idempotency_key=${queryEscape(key)}`;
  const [status, body, error] = await storeCall("GET", rawURL, requestID, deadline, null);
  if (error) return { status: 0, headers: {}, body: null };
  const object = asMap(body);
  if (status === 200 && object && object["order_id"] !== undefined && object["order_id"] !== null) {
    const digest = canonicalBody(asString(fields["sku"]), asInt(fields["qty"]), fields["external_ref"], fields["fulfill_url"]);
    if (asString(object["request_hash"]) !== digest) {
      return jsonResponse(409, errorBody("idempotency_mismatch", "幂等键与已有订单正文不一致", requestID));
    }
    delete object["request_hash"];
    logEvent("order_replayed", requestID, "info", { order_id: object["order_id"] });
    return jsonResponse(201, object);
  }
  return { status: 0, headers: {}, body: null };
}

async function recommendText(service: OrderService, sku: string, requestID: string): Promise<string> {
  if (!service.breaker.allow()) {
    logEvent("breaker_open", requestID, "info", null);
    service.metrics.set("orange_breaker_open", 1);
    return "";
  }
  const result = await httpRequest("GET", service.recommender + "/internal/recommend?sku=" + queryEscape(sku), null, { "X-Request-Id": requestID }, recommendBudgetMs);
  if (result.error) {
    service.breaker.record(false);
    logEvent("recommend_failed", requestID, "info", { message: result.error.message });
    return "";
  }
  if (result.status !== 200) {
    service.breaker.record(false);
    logEvent("recommend_failed", requestID, "info", { status: result.status });
    return "";
  }
  let text = "";
  try {
    text = asString(asMap(decodeJSON(result.body))?.["recommendation"]);
  } catch {
    text = "";
  }
  if (!text) {
    service.breaker.record(false);
    logEvent("recommend_failed", requestID, "info", { status: result.status });
    return "";
  }
  service.breaker.record(true);
  service.metrics.set("orange_breaker_open", 0);
  return text;
}

async function writeNew(service: OrderService, caller: string, key: string, fields: JsonMap, requestID: string, deadline: number): Promise<ResponseMessage> {
  const started = Date.now();
  const pending = recommendText(service, asString(fields["sku"]), requestID);
  const [stored, failed] = await callAppend(service, caller, key, fields, requestID, deadline);
  if (failed.status !== 0) {
    await pending.catch(() => "");
    return failed;
  }
  const remaining = recommendBudgetMs - (Date.now() - started);
  const text = remaining > 0
    ? await Promise.race([pending, sleep(remaining).then(() => "")])
    : await Promise.race([pending, Promise.resolve("")]);
  if (!text) {
    logEvent("order_accepted", requestID, "info", { order_id: stored?.["order_id"], epoch: stored?.["epoch"], degraded: true });
    return jsonResponse(201, stored);
  }
  const [leader, epoch, ok] = service.currentLeader();
  if (!ok || !stored) return jsonResponse(201, stored);
  let snapDeadline = deadline;
  if (snapDeadline === 0) snapDeadline = nowMS() + 200;
  const [status, body, error] = await storeCall("POST", leader + "/internal/snapshot", requestID, snapDeadline, {
    order_id: stored["order_id"],
    epoch,
    recommendation: text,
    degraded: false,
  });
  if (!error && status === 200) {
    const object = asMap(body);
    if (object) {
      logEvent("order_accepted", requestID, "info", { order_id: object["order_id"], epoch: object["epoch"], degraded: false });
      return jsonResponse(201, object);
    }
  }
  return jsonResponse(201, stored);
}

async function callAppend(service: OrderService, caller: string, key: string, fields: JsonMap, requestID: string, deadline: number): Promise<[JsonMap | null, ResponseMessage]> {
  const last = jsonResponse(503, errorBody("unavailable", "主存储不可用", requestID));
  for (let attempt = 0; attempt < 2; attempt++) {
    let [leader, epoch, ok] = service.currentLeader();
    if (!ok) {
      await service.refreshOnce();
      [leader, epoch, ok] = service.currentLeader();
    }
    if (!ok) return [null, last];
    if (deadline === 0) deadline = nowMS() + orderDeadlineMs;
    const [status, body, error] = await storeCall("POST", leader + "/internal/append", requestID, deadline, {
      epoch,
      caller_id: caller,
      idempotency_key: key,
      order: fields,
    });
    if (error) {
      service.metrics.inc("orange_errors_total", 1);
      logEvent("deadline_exceeded", requestID, "info", null);
      return [null, jsonResponse(504, errorBody("deadline_exceeded", "超过截止时间", requestID))];
    }
    const object = asMap(body);
    if (status === 409 && asString(asMap(object?.["error"])?.["code"]) === "epoch_fenced") {
      logEvent("write_fenced", requestID, "info", { epoch: asMap(object?.["error"])?.["epoch"] });
      await service.refreshOnce();
      continue;
    }
    if (status === 409) return [null, jsonResponse(409, body)];
    if (status === 200 && object && object["order"] != null) {
      const order = asMap(object["order"]);
      if (!asBool(object["created"])) logEvent("order_replayed", requestID, "info", { order_id: order?.["order_id"] });
      return [order, { status: 0, headers: {}, body: null }];
    }
    const payload = body ?? errorBody("unavailable", "主存储不可用", requestID);
    return [null, jsonResponse(status || 503, payload)];
  }
  return [null, jsonResponse(503, errorBody("unavailable", "主存储正在切换", requestID))];
}

function handler(service: OrderService): Handler {
  return async (method, path, headers, raw) => {
    const requestID = headerRequestID(headers);
    const [route, query] = splitPath(path);
    if (method === "GET" && route === "/health") return emptyResponse(200);
    if (method === "GET" && route === "/ready") {
      const [, , ok] = service.currentLeader();
      return ok ? emptyResponse(200) : emptyResponse(503);
    }
    if (method === "GET" && route === "/metrics") {
      service.metrics.set("orange_breaker_open", service.breaker.open() ? 1 : 0);
      return textResponse(200, service.metrics.render(["orange_errors_total", "orange_inflight", "orange_breaker_open"]));
    }
    if (method === "GET" && (route === "/internal/orders" || (route.startsWith("/internal/orders/") && route.length > "/internal/orders/".length))) {
      return proxyRead(service, route, query, headers, requestID);
    }
    if (method === "POST" && route === "/internal/orders") return createAccepted(service, headers, raw, requestID);
    return jsonResponse(404, errorBody("not_found", "没有这个路径", requestID));
  };
}

export async function runOrder(flags: Map<string, string>): Promise<void> {
  const name = flags.get("process-name") ?? "";
  const port = Number(flags.get("port") ?? "0");
  const supervisor = flags.get("supervisor") ?? "";
  const recommender = flags.get("recommender") ?? "";
  const root = flags.get("root") ?? ".";
  setContext(name, root);
  startFaultReader();
  const service = new OrderService(name, supervisor, recommender);
  void service.refreshLoop();
  logEvent("process_started", "", "info", { port });
  onStop(() => service.stop());
  await serve(port, handler(service));
}
