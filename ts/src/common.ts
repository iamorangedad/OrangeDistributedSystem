import crypto from "crypto";
import fs from "fs";
import path from "path";

export const tokenCapacity = 30;
export const tokenRefillPerSec = 10;
export const gatewayInflightLimit = 8;
export const jobsPendingLimit = 50;
export const orderDeadlineMs = 800;
export const storeCallCapMs = 400;
export const recommendBudgetMs = 80;
export const breakerWindowMs = 10_000;
export const breakerFailures = 5;
export const breakerOpenMs = 5_000;
export const visibilityMs = 15_000;
export const fulfillTimeoutMs = 1_000;
export const jobMaxAttempts = 5;
export const backoffBaseMs = 100;
export const backoffCapMs = 5_000;
export const heartbeatMs = 200;
export const leaderLostMs = 1_000;
export const scaleOutPending = 10;
export const scaleOutHoldMs = 5_000;
export const scaleInHoldMs = 20_000;
export const scaleCooldownMs = 15_000;
export const workerMin = 1;
export const workerMax = 3;
export const promotionPauseMs = 2_000;
export const faultPollMs = 200;

const callerPattern = /^[a-z0-9-]{1,32}$/;

export class JsonNumber {
  constructor(readonly text: string) {}
  toJSON(): number {
    return Number(this.text);
  }
}

export type JsonMap = Record<string, any>;

let processName = "orange";
let rootDir = ".";

export function setContext(process: string, root: string): void {
  processName = process;
  rootDir = root;
}

export function rootPath(): string {
  return rootDir;
}

export function processLabel(): string {
  return processName;
}

export function nowMS(): number {
  return Date.now();
}

export function isoNow(): string {
  return new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
}

export function newID(prefix: string): string {
  return prefix + crypto.randomBytes(16).toString("hex");
}

export function marshal(value: unknown): string {
  return JSON.stringify(value);
}

class Parser {
  private index = 0;
  constructor(private readonly raw: string) {}

  parseValue(): unknown {
    this.skip();
    if (this.eof()) throw new Error("正文不是 JSON");
    const char = this.raw[this.index];
    if (char === "{") return this.parseObject();
    if (char === "[") return this.parseArray();
    if (char === '"') return this.parseString();
    if (char === "t") return this.consume("true", true);
    if (char === "f") return this.consume("false", false);
    if (char === "n") return this.consume("null", null);
    if (char === "-" || (char >= "0" && char <= "9")) return this.parseNumber();
    throw new Error("正文不是 JSON");
  }

  eof(): boolean {
    this.skip();
    return this.index >= this.raw.length;
  }

  private skip(): void {
    while (this.index < this.raw.length && " \n\r\t".includes(this.raw[this.index])) this.index++;
  }

  private consume(text: string, value: unknown): unknown {
    if (this.raw.slice(this.index, this.index + text.length) !== text) throw new Error("正文不是 JSON");
    this.index += text.length;
    return value;
  }

  private parseObject(): JsonMap {
    this.index++;
    const object: JsonMap = {};
    this.skip();
    if (this.raw[this.index] === "}") {
      this.index++;
      return object;
    }
    while (this.index < this.raw.length) {
      this.skip();
      if (this.raw[this.index] !== '"') throw new Error("正文不是 JSON");
      const key = this.parseString();
      this.skip();
      if (this.raw[this.index] !== ":") throw new Error("正文不是 JSON");
      this.index++;
      object[key] = this.parseValue();
      this.skip();
      if (this.raw[this.index] === "}") {
        this.index++;
        return object;
      }
      if (this.raw[this.index] !== ",") throw new Error("正文不是 JSON");
      this.index++;
    }
    throw new Error("正文不是 JSON");
  }

  private parseArray(): unknown[] {
    this.index++;
    const items: unknown[] = [];
    this.skip();
    if (this.raw[this.index] === "]") {
      this.index++;
      return items;
    }
    while (this.index < this.raw.length) {
      items.push(this.parseValue());
      this.skip();
      if (this.raw[this.index] === "]") {
        this.index++;
        return items;
      }
      if (this.raw[this.index] !== ",") throw new Error("正文不是 JSON");
      this.index++;
    }
    throw new Error("正文不是 JSON");
  }

  private parseString(): string {
    this.index++;
    let out = "";
    while (this.index < this.raw.length) {
      const char = this.raw[this.index++];
      if (char === '"') return out;
      if (char === "\\") {
        const escaped = this.raw[this.index++];
        if (escaped === "u") {
          const hex = this.raw.slice(this.index, this.index + 4);
          if (!/^[0-9a-fA-F]{4}$/.test(hex)) throw new Error("正文不是 JSON");
          out += String.fromCharCode(parseInt(hex, 16));
          this.index += 4;
          continue;
        }
        const mapped: Record<string, string> = { '"': '"', "\\": "\\", "/": "/", b: "\b", f: "\f", n: "\n", r: "\r", t: "\t" };
        if (!(escaped in mapped)) throw new Error("正文不是 JSON");
        out += mapped[escaped];
        continue;
      }
      if (char < " ") throw new Error("正文不是 JSON");
      out += char;
    }
    throw new Error("正文不是 JSON");
  }

  private parseNumber(): JsonNumber {
    const start = this.index;
    if (this.raw[this.index] === "-") this.index++;
    if (this.raw[this.index] === "0") this.index++;
    else if (this.raw[this.index] >= "1" && this.raw[this.index] <= "9") {
      while (this.raw[this.index] >= "0" && this.raw[this.index] <= "9") this.index++;
    } else throw new Error("正文不是 JSON");
    if (this.raw[this.index] === ".") {
      this.index++;
      if (!(this.raw[this.index] >= "0" && this.raw[this.index] <= "9")) throw new Error("正文不是 JSON");
      while (this.raw[this.index] >= "0" && this.raw[this.index] <= "9") this.index++;
    }
    if (this.raw[this.index] === "e" || this.raw[this.index] === "E") {
      this.index++;
      if (this.raw[this.index] === "+" || this.raw[this.index] === "-") this.index++;
      if (!(this.raw[this.index] >= "0" && this.raw[this.index] <= "9")) throw new Error("正文不是 JSON");
      while (this.raw[this.index] >= "0" && this.raw[this.index] <= "9") this.index++;
    }
    return new JsonNumber(this.raw.slice(start, this.index));
  }
}

export function decodeJSON(raw: string | Buffer): unknown {
  const text = typeof raw === "string" ? raw : raw.toString("utf8");
  if (text.length === 0) return null;
  const parser = new Parser(text);
  const value = parser.parseValue();
  if (!parser.eof()) throw new Error("正文不是 JSON");
  return value;
}

export function asMap(value: unknown): JsonMap | null {
  if (value === null || typeof value !== "object" || Array.isArray(value) || value instanceof JsonNumber) return null;
  return value as JsonMap;
}

export function asString(value: unknown): string {
  return typeof value === "string" ? value : "";
}

export function asInt(value: unknown): number {
  if (value instanceof JsonNumber) {
    if (!/[.eE]/.test(value.text)) {
      const parsed = Number(value.text);
      if (Number.isSafeInteger(parsed)) return parsed;
    }
    const parsed = Number(value.text);
    return Number.isFinite(parsed) ? Math.trunc(parsed) : 0;
  }
  if (typeof value === "number") return Number.isFinite(value) ? Math.trunc(value) : 0;
  if (typeof value === "bigint") return Number(value);
  return 0;
}

export function asBool(value: unknown): boolean {
  return value === true;
}

export function logEvent(event: string, requestID: string, level: string, fields?: JsonMap | null): void {
  const record: JsonMap = {
    ts: isoNow(),
    level: level || "info",
    process: processName,
    event,
    request_id: requestID,
  };
  if (fields) {
    for (const [key, value] of Object.entries(fields)) record[key] = value;
  }
  const line = marshal(record);
  console.log(line);
  const directory = path.join(rootDir, "runtime", "logs");
  fs.mkdirSync(directory, { recursive: true });
  fs.appendFileSync(path.join(directory, processName + ".log"), line + "\n");
}

export function faultsPath(): string {
  return path.join(rootDir, "runtime", "faults.json");
}

let faults: JsonMap = {};
let faultErrorLogged = false;
let requestCount = 0;

export function currentFaults(): JsonMap {
  const parsed = JSON.parse(JSON.stringify(faults)) as unknown;
  return asMap(parsed) ?? {};
}

export function loadFaultsOnce(): void {
  let raw: string;
  try {
    raw = fs.readFileSync(faultsPath(), "utf8");
  } catch {
    faults = {};
    if (!faultErrorLogged) {
      faultErrorLogged = true;
      logEvent("faults_invalid", "", "error", { message: "故障文件不存在" });
    }
    return;
  }
  try {
    const parsed = decodeJSON(raw);
    const object = asMap(parsed);
    if (!object) {
      noteFaultError("faults 必须是对象");
      return;
    }
    faults = object;
    faultErrorLogged = false;
  } catch (error) {
    noteFaultError(error instanceof Error ? error.message : String(error));
  }
}

function noteFaultError(message: string): void {
  const logged = faultErrorLogged;
  faults = {};
  faultErrorLogged = true;
  if (!logged) logEvent("faults_invalid", "", "error", { message });
}

export function startFaultReader(): void {
  loadFaultsOnce();
  const timer = setInterval(loadFaultsOnce, faultPollMs);
  timer.unref();
}

export async function faultDelay(kind: string): Promise<void> {
  const delay = asMap(currentFaults()["delay_ms"]);
  const ms = asInt(delay?.[kind]);
  if (ms > 0) await sleep(ms);
}

export function pauseReplication(): boolean {
  return asBool(currentFaults()["pause_replication"]);
}

export function dropHeartbeat(nodeID: string): boolean {
  const dropped = asMap(currentFaults()["drop_heartbeat"]);
  return asBool(dropped?.[nodeID]);
}

export function noteHTTPRequest(): void {
  const limits = asMap(currentFaults()["exit_after_requests"]);
  const limit = asInt(limits?.[processName]);
  if (limit <= 0) return;
  requestCount++;
  if (requestCount >= limit) {
    logEvent("exit_after_requests", "", "info", { limit });
    process.exit(0);
  }
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export class TokenBucket {
  private tokens: number;
  private updated = Date.now();

  constructor(private readonly capacity: number, private readonly rate: number) {
    this.tokens = capacity;
  }

  take(): [boolean, number] {
    const now = Date.now();
    const elapsed = (now - this.updated) / 1000;
    this.tokens = Math.min(this.capacity, this.tokens + elapsed * this.rate);
    this.updated = now;
    if (this.tokens >= 1) {
      this.tokens--;
      return [true, 0];
    }
    if (this.rate <= 0) return [false, 1];
    let retry = Math.ceil((1 - this.tokens) / this.rate);
    if (retry < 1) retry = 1;
    return [false, retry];
  }
}

export class CircuitBreaker {
  private failures: number[] = [];
  private state: "closed" | "open" | "half" = "closed";
  private openUntil = 0;

  open(): boolean {
    return this.state === "open" && Date.now() < this.openUntil;
  }

  allow(): boolean {
    const now = Date.now();
    if (this.state === "open") {
      if (now < this.openUntil) return false;
      this.state = "half";
      return true;
    }
    if (this.state === "half") return false;
    return true;
  }

  record(success: boolean): void {
    const now = Date.now();
    if (this.state === "half") {
      if (success) {
        this.state = "closed";
        this.failures = [];
      } else {
        this.state = "open";
        this.openUntil = now + breakerOpenMs;
      }
      return;
    }
    if (success) return;
    this.failures = this.failures.filter((item) => now - item <= breakerWindowMs);
    this.failures.push(now);
    if (this.failures.length >= breakerFailures) {
      this.state = "open";
      this.openUntil = now + breakerOpenMs;
      this.failures = [];
    }
  }
}

export function backoffMS(attempt: number): number {
  let shift = attempt - 1;
  if (shift < 0) shift = 0;
  if (shift > 16) shift = 16;
  let cap = backoffBaseMs * 2 ** shift;
  if (cap > backoffCapMs || cap <= 0) cap = backoffCapMs;
  return Math.floor(Math.random() * (cap + 1));
}

export function canonicalBody(sku: string, qty: number, externalRef: unknown, fulfillURL: unknown): string {
  const payload = {
    external_ref: typeof externalRef === "string" ? externalRef : "",
    fulfill_url: typeof fulfillURL === "string" ? fulfillURL : "",
    qty,
    sku,
  };
  return marshal(payload);
}

export function validCaller(value: string): boolean {
  return callerPattern.test(value);
}

export function validIdempotencyKey(value: string): boolean {
  if (value.length < 8 || value.length > 128) return false;
  for (const char of value) {
    const code = char.codePointAt(0) ?? 0;
    if (code < 32 || code > 126) return false;
  }
  return true;
}

export function validFulfillURL(value: unknown): boolean {
  if (value === null || value === undefined) return true;
  if (typeof value !== "string") return false;
  if (value === "") return true;
  return fulfillURLText(value);
}

function fulfillURLText(text: string): boolean {
  if (!text.startsWith("http://")) return false;
  const rest = text.slice("http://".length);
  const slash = rest.indexOf("/");
  const authority = slash >= 0 ? rest.slice(0, slash) : rest;
  if (authority.includes("@") || authority === "") return false;
  const colon = authority.lastIndexOf(":");
  if (colon <= 0) return false;
  const host = authority.slice(0, colon);
  const port = authority.slice(colon + 1);
  if (host !== "127.0.0.1" || port === "" || port === "0") return false;
  return /^[0-9]+$/.test(port);
}

export function validateOrderBody(data: unknown): [JsonMap | null, string] {
  const object = asMap(data);
  if (!object) return [null, "正文必须是 JSON 对象"];
  const sku = object["sku"];
  if (typeof sku !== "string" || sku.length === 0 || Buffer.byteLength(sku) > 64) return [null, "sku 必须是 1 到 64 个字符"];
  if (typeof object["qty"] === "boolean") return [null, "qty 必须是 1 到 100 的整数"];
  const qtyValue = object["qty"];
  if (!(qtyValue instanceof JsonNumber) || /[.eE]/.test(qtyValue.text)) return [null, "qty 必须是 1 到 100 的整数"];
  const qty = Number(qtyValue.text);
  if (!Number.isSafeInteger(qty) || qty < 1 || qty > 100) return [null, "qty 必须是 1 到 100 的整数"];
  const external = object["external_ref"];
  if (external !== undefined && external !== null) {
    if (typeof external !== "string" || Buffer.byteLength(external) > 128) return [null, "external_ref 最长 128 个字符"];
  }
  const fulfill = object["fulfill_url"];
  if (fulfill !== undefined && fulfill !== null && typeof fulfill !== "string") return [null, "fulfill_url 必须是字符串"];
  if (!validFulfillURL(fulfill === undefined ? null : fulfill)) return [null, "fulfill_url 只能是 http://127.0.0.1 上的地址"];
  let fulfillValue: string | null = null;
  if (typeof fulfill === "string" && fulfill !== "") fulfillValue = fulfill;
  return [{ sku, qty, external_ref: external === undefined ? null : external, fulfill_url: fulfillValue }, ""];
}

export class Metrics {
  private values: Record<string, number> = {};

  inc(name: string, amount: number): void {
    this.values[name] = (this.values[name] ?? 0) + amount;
  }

  set(name: string, value: number): void {
    this.values[name] = value;
  }

  render(names: string[]): string {
    return names.map((name) => `${name} ${this.values[name] ?? 0}`).join("\n") + "\n";
  }
}

export function errorBody(code: string, message: string, requestID: string): JsonMap {
  return { error: { code, message, request_id: requestID } };
}

export function queryEscape(value: string): string {
  let out = "";
  for (const byte of Buffer.from(value)) {
    const unreserved =
      (byte >= 0x41 && byte <= 0x5a) ||
      (byte >= 0x61 && byte <= 0x7a) ||
      (byte >= 0x30 && byte <= 0x39) ||
      byte === 0x2d ||
      byte === 0x2e ||
      byte === 0x5f ||
      byte === 0x7e;
    if (unreserved) out += String.fromCharCode(byte);
    else if (byte === 0x20) out += "+";
    else out += "%" + byte.toString(16).toUpperCase().padStart(2, "0");
  }
  return out;
}

export function scriptPath(): string {
  return path.resolve(process.argv[1] || "dist/main.js");
}
