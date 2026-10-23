import http from "http";
import { errorBody, logEvent, marshal, newID, noteHTTPRequest, type JsonMap } from "./common";

export interface HttpResult {
  status: number;
  headers: Record<string, string>;
  body: Buffer;
  error: Error | null;
}

export interface ResponseMessage {
  status: number;
  headers: Record<string, string>;
  body: Buffer | null;
}

export type Handler = (method: string, path: string, headers: Record<string, string>, raw: Buffer) => ResponseMessage | Promise<ResponseMessage>;

export function jsonResponse(status: number, payload: unknown, extra?: Record<string, string> | null): ResponseMessage {
  const headers: Record<string, string> = { "Content-Type": "application/json; charset=utf-8" };
  if (extra) {
    for (const [key, value] of Object.entries(extra)) headers[key] = value;
  }
  return { status, headers, body: Buffer.from(marshal(payload)) };
}

export function textResponse(status: number, text: string): ResponseMessage {
  return { status, headers: { "Content-Type": "text/plain; charset=utf-8" }, body: Buffer.from(text) };
}

export function emptyResponse(status: number): ResponseMessage {
  return { status, headers: {}, body: null };
}

let currentServer: http.Server | null = null;

export function httpRequest(method: string, rawURL: string, body: unknown, headers: Record<string, string> | null, timeoutMs: number): Promise<HttpResult> {
  return new Promise((resolve) => {
    let settled = false;
    const finish = (result: HttpResult) => {
      if (settled) return;
      settled = true;
      resolve(result);
    };
    let payload: Buffer | undefined;
    if (body !== null && body !== undefined) payload = Buffer.from(marshal(body));
    let url: URL;
    try {
      url = new URL(rawURL);
    } catch (error) {
      finish({ status: 0, headers: {}, body: Buffer.alloc(0), error: error instanceof Error ? error : new Error(String(error)) });
      return;
    }
    const requestHeaders: Record<string, string> = { Connection: "close" };
    if (payload) {
      requestHeaders["Content-Type"] = "application/json";
      requestHeaders["Content-Length"] = String(payload.length);
    }
    if (headers) {
      for (const [key, value] of Object.entries(headers)) requestHeaders[key] = value;
    }
    const req = http.request(
      {
        protocol: url.protocol,
        hostname: url.hostname,
        port: url.port,
        path: `${url.pathname}${url.search}`,
        method,
        headers: requestHeaders,
        agent: false,
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (chunk: Buffer) => chunks.push(chunk));
        res.on("end", () => {
          const out: Record<string, string> = {};
          for (const [key, value] of Object.entries(res.headers)) {
            if (Array.isArray(value)) out[key.toLowerCase()] = value[0] ?? "";
            else if (typeof value === "string") out[key.toLowerCase()] = value;
          }
          finish({ status: res.statusCode ?? 0, headers: out, body: Buffer.concat(chunks), error: null });
        });
      },
    );
    req.on("error", (error) => finish({ status: 0, headers: {}, body: Buffer.alloc(0), error }));
    req.setTimeout(timeoutMs, () => req.destroy(new Error("timeout")));
    if (payload) req.end(payload);
    else req.end();
  });
}

export function splitPath(raw: string): [string, Record<string, string>] {
  const url = new URL(raw, "http://127.0.0.1");
  const query: Record<string, string> = {};
  for (const [key, value] of url.searchParams) query[key] = value;
  return [url.pathname || "/", query];
}

export function headerRequestID(headers: Record<string, string>): string {
  if (headers["x-request-id"]) return headers["x-request-id"];
  return newID("req_");
}

let stopping = false;
const stopWaiters: Array<() => void> = [];
const stopHooks: Array<() => void> = [];

export function onStop(hook: () => void): void {
  stopHooks.push(hook);
}

export function requestStop(): void {
  if (stopping) return;
  stopping = true;
  for (const hook of stopHooks) hook();
  for (const wake of stopWaiters) wake();
  shutdownServer();
}

export function stopped(): boolean {
  return stopping;
}

export function stopSignal(): Promise<void> {
  if (stopping) return Promise.resolve();
  return new Promise((resolve) => stopWaiters.push(resolve));
}

export function installSignals(): void {
  process.on("SIGTERM", () => requestStop());
  process.on("SIGINT", () => requestStop());
}

export function serve(port: number, handler: Handler): Promise<void> {
  const server = http.createServer((req, res) => {
    void handleRequest(req, res, handler);
  });
  currentServer = server;
  server.on("clientError", (_error, socket) => socket.destroy());
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", () => {
      server.off("error", reject);
      server.on("close", () => resolve());
    });
  });
}

async function handleRequest(req: http.IncomingMessage, res: http.ServerResponse, handler: Handler): Promise<void> {
  let wrote = false;
  const requestPath = req.url || "/";
  try {
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(chunk as Buffer);
    const headers: Record<string, string> = {};
    for (const [key, value] of Object.entries(req.headers)) {
      if (Array.isArray(value)) headers[key.toLowerCase()] = value[0] ?? "";
      else if (typeof value === "string") headers[key.toLowerCase()] = value;
    }
    const result = await handler(req.method || "GET", requestPath, headers, Buffer.concat(chunks));
    for (const [key, value] of Object.entries(result.headers)) res.setHeader(key, value);
    res.setHeader("Connection", "close");
    const status = result.status || 500;
    wrote = true;
    res.writeHead(status);
    if (result.body && result.body.length > 0) res.end(result.body);
    else res.end();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    logEvent("handler_error", req.headers["x-request-id"] ? String(req.headers["x-request-id"]) : "", "error", {
      message,
      path: requestPath,
    } satisfies JsonMap);
    if (!wrote) {
      const payload = Buffer.from(marshal(errorBody("unavailable", "进程内部错误", String(req.headers["x-request-id"] || ""))));
      res.setHeader("Content-Type", "application/json");
      res.setHeader("Connection", "close");
      res.writeHead(500);
      res.end(payload);
    }
  } finally {
    noteHTTPRequest();
  }
}

export function shutdownServer(): void {
  const server = currentServer;
  if (!server) return;
  server.close();
}

export function jsonNumberText(value: number): string {
  return marshal(value);
}
