"""截止时间、令牌桶、熔断、退避、日志、故障文件和本机 HTTP。"""

from __future__ import annotations

import json
import math
import os
import random
import re
import signal
import threading
import time
import uuid
from datetime import datetime, timezone
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import urlsplit

# 技术方案里的参数。改行为先改这里。
TOKEN_CAPACITY = 30
TOKEN_REFILL_PER_SEC = 10
GATEWAY_INFLIGHT_LIMIT = 8
JOBS_PENDING_LIMIT = 50
ORDER_DEADLINE_MS = 800
STORE_CALL_CAP_MS = 400
RECOMMEND_BUDGET_MS = 80
BREAKER_WINDOW_SEC = 10
BREAKER_FAILURES = 5
BREAKER_OPEN_SEC = 5
VISIBILITY_MS = 15_000
FULFILL_TIMEOUT_SEC = 1
JOB_MAX_ATTEMPTS = 5
BACKOFF_BASE_MS = 100
BACKOFF_CAP_MS = 5_000
HEARTBEAT_SEC = 0.2
LEADER_LOST_SEC = 1.0
SCALE_OUT_PENDING = 10
SCALE_OUT_HOLD_SEC = 5
SCALE_IN_HOLD_SEC = 20
SCALE_COOLDOWN_SEC = 15
WORKER_MIN = 1
WORKER_MAX = 3
PROMOTION_PAUSE_SEC = 2
FAULT_POLL_SEC = 0.2

CALLER_RE = re.compile(r"^[a-z0-9-]{1,32}$")
PROCESS_NAME = os.environ.get("ORANGE_PROCESS", "orange")
ROOT = os.environ.get("ORANGE_ROOT", os.getcwd())

_log_lock = threading.Lock()
_fault_lock = threading.Lock()
_faults: dict[str, Any] = {}
_fault_error_logged = False
_request_count = 0
_request_count_lock = threading.Lock()


def now_ms() -> int:
    return int(time.time() * 1000)


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def new_id(prefix: str) -> str:
    return prefix + uuid.uuid4().hex


def log_event(event: str, request_id: str = "", level: str = "info", **fields: Any) -> None:
    record = {
        "ts": iso_now(),
        "level": level,
        "process": PROCESS_NAME,
        "event": event,
        "request_id": request_id or "",
    }
    record.update(fields)
    line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
    with _log_lock:
        print(line, flush=True)
        directory = os.path.join(ROOT, "runtime", "logs")
        os.makedirs(directory, exist_ok=True)
        path = os.path.join(directory, PROCESS_NAME + ".log")
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")


def faults_path() -> str:
    return os.path.join(ROOT, "runtime", "faults.json")


def current_faults() -> dict[str, Any]:
    with _fault_lock:
        return json.loads(json.dumps(_faults))


def _load_faults_once() -> None:
    global _faults, _fault_error_logged
    path = faults_path()
    try:
        with open(path, encoding="utf-8") as handle:
            loaded = json.load(handle)
        if not isinstance(loaded, dict):
            raise ValueError("faults 必须是对象")
        with _fault_lock:
            _faults = loaded
            _fault_error_logged = False
    except FileNotFoundError:
        with _fault_lock:
            _faults = {}
            if not _fault_error_logged:
                _fault_error_logged = True
                log_event("faults_invalid", level="error", message="故障文件不存在")
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        with _fault_lock:
            _faults = {}
            if not _fault_error_logged:
                _fault_error_logged = True
                log_event("faults_invalid", level="error", message=str(exc))


def start_fault_reader() -> None:
    _load_faults_once()

    def loop() -> None:
        while True:
            time.sleep(FAULT_POLL_SEC)
            _load_faults_once()

    threading.Thread(target=loop, name="faults", daemon=True).start()


def fault_delay(kind: str) -> None:
    delay = current_faults().get("delay_ms", {})
    if isinstance(delay, dict):
        ms = int(delay.get(kind) or 0)
        if ms > 0:
            time.sleep(ms / 1000)


def pause_replication() -> bool:
    return bool(current_faults().get("pause_replication"))


def drop_heartbeat(node_id: str) -> bool:
    dropped = current_faults().get("drop_heartbeat", {})
    return bool(isinstance(dropped, dict) and dropped.get(node_id))


def note_http_request() -> None:
    """处理完一个请求后计数。达到故障文件里的次数就退出。"""
    global _request_count
    limits = current_faults().get("exit_after_requests", {})
    if not isinstance(limits, dict):
        return
    limit = int(limits.get(PROCESS_NAME) or 0)
    if limit <= 0:
        return
    with _request_count_lock:
        _request_count += 1
        hit = _request_count >= limit
    if hit:
        log_event("exit_after_requests", limit=limit)
        os._exit(0)


class TokenBucket:
    def __init__(self, capacity: int = TOKEN_CAPACITY, rate: float = TOKEN_REFILL_PER_SEC) -> None:
        self.capacity = capacity
        self.rate = rate
        self.tokens = float(capacity)
        self.updated = time.monotonic()
        self._lock = threading.Lock()

    def take(self) -> tuple[bool, int]:
        with self._lock:
            now = time.monotonic()
            self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
            self.updated = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True, 0
            if self.rate <= 0:
                return False, 1
            retry_after = max(1, math.ceil((1 - self.tokens) / self.rate))
            return False, retry_after


class CircuitBreaker:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._failures: list[float] = []
        self._state = "closed"
        self._open_until = 0.0

    @property
    def open(self) -> bool:
        with self._lock:
            return self._state == "open" and time.monotonic() < self._open_until

    def allow(self) -> bool:
        with self._lock:
            now = time.monotonic()
            if self._state == "open":
                if now < self._open_until:
                    return False
                self._state = "half"
                return True
            if self._state == "half":
                return False
            return True

    def record(self, success: bool) -> None:
        with self._lock:
            now = time.monotonic()
            if self._state == "half":
                if success:
                    self._state = "closed"
                    self._failures.clear()
                else:
                    self._state = "open"
                    self._open_until = now + BREAKER_OPEN_SEC
                return
            if success:
                return
            self._failures = [item for item in self._failures if now - item <= BREAKER_WINDOW_SEC]
            self._failures.append(now)
            if len(self._failures) >= BREAKER_FAILURES:
                self._state = "open"
                self._open_until = now + BREAKER_OPEN_SEC
                self._failures.clear()


def backoff_ms(attempt: int) -> int:
    """第 attempt 次失败后的全抖动等待。attempt 从 1 开始。"""
    cap = min(BACKOFF_CAP_MS, BACKOFF_BASE_MS * (2 ** max(0, attempt - 1)))
    return random.randint(0, cap)


def canonical_body(sku: str, qty: int, external_ref: str | None, fulfill_url: str | None) -> str:
    payload = {
        "external_ref": external_ref or "",
        "fulfill_url": fulfill_url or "",
        "qty": qty,
        "sku": sku,
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def valid_caller(value: str | None) -> bool:
    return bool(value and CALLER_RE.fullmatch(value))


def valid_idempotency_key(value: str | None) -> bool:
    if not value or not 8 <= len(value) <= 128:
        return False
    return all(32 <= ord(char) <= 126 for char in value)


def valid_fulfill_url(value: str | None) -> bool:
    if value is None or value == "":
        return True
    parts = urlsplit(value)
    if parts.scheme != "http" or parts.hostname != "127.0.0.1":
        return False
    if parts.username or parts.password or not parts.port:
        return False
    return True


def validate_order_body(data: Any) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(data, dict):
        return None, "正文必须是 JSON 对象"
    sku = data.get("sku")
    qty = data.get("qty")
    if not isinstance(sku, str) or not 1 <= len(sku) <= 64:
        return None, "sku 必须是 1 到 64 个字符"
    if isinstance(qty, bool) or not isinstance(qty, int) or not 1 <= qty <= 100:
        return None, "qty 必须是 1 到 100 的整数"
    external_ref = data.get("external_ref")
    if external_ref is not None and (not isinstance(external_ref, str) or len(external_ref) > 128):
        return None, "external_ref 最长 128 个字符"
    fulfill_url = data.get("fulfill_url")
    if fulfill_url is not None and not isinstance(fulfill_url, str):
        return None, "fulfill_url 必须是字符串"
    if not valid_fulfill_url(fulfill_url):
        return None, "fulfill_url 只能是 http://127.0.0.1 上的地址"
    return {
        "sku": sku,
        "qty": qty,
        "external_ref": external_ref,
        "fulfill_url": fulfill_url or None,
    }, None


def http_request(
    method: str,
    url: str,
    body: Any = None,
    headers: dict[str, str] | None = None,
    timeout: float = 2.0,
) -> tuple[int, dict[str, str], bytes]:
    parts = urlsplit(url)
    connection = HTTPConnection(parts.hostname, parts.port, timeout=timeout)
    path = parts.path or "/"
    if parts.query:
        path += "?" + parts.query
    payload = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
    hdrs = {} if body is None else {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)
    try:
        connection.request(method, path, body=payload, headers=hdrs)
        response = connection.getresponse()
        raw = response.read()
        header_map = {key.lower(): value for key, value in response.getheaders()}
        return response.status, header_map, raw
    finally:
        connection.close()


def decode_json(raw: bytes) -> Any:
    if not raw:
        return None
    return json.loads(raw.decode("utf-8"))


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.values: dict[str, int] = {}

    def inc(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self.values[name] = self.values.get(name, 0) + amount

    def set(self, name: str, value: int) -> None:
        with self._lock:
            self.values[name] = value

    def get(self, name: str) -> int:
        with self._lock:
            return self.values.get(name, 0)

    def render(self, names: list[str]) -> str:
        with self._lock:
            lines = [f"{name} {int(self.values.get(name, 0))}" for name in names]
        return "\n".join(lines) + "\n"


def error_body(code: str, message: str, request_id: str) -> dict[str, Any]:
    return {"error": {"code": code, "message": message, "request_id": request_id}}


Handler = Callable[[str, dict[str, str], dict[str, str], bytes], tuple[int, dict[str, str], bytes]]


def serve(port: int, handler: Handler) -> ThreadingHTTPServer:
    class HandlerImpl(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _dispatch(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b""
            headers = {key.lower(): value for key, value in self.headers.items()}
            path = self.path
            try:
                status, extra, payload = handler(self.command, path, headers, raw)
            except Exception as exc:  # noqa: BLE001
                log_event("handler_error", level="error", message=str(exc), path=path)
                status = 500
                extra = {"Content-Type": "application/json"}
                payload = json.dumps(
                    error_body("unavailable", "进程内部错误", headers.get("x-request-id", ""))
                ).encode("utf-8")
            self.send_response(status)
            extra = dict(extra)
            extra.setdefault("Content-Length", str(len(payload)))
            extra.setdefault("Connection", "close")
            for key, value in extra.items():
                self.send_header(key, value)
            self.end_headers()
            if payload:
                self.wfile.write(payload)
            note_http_request()

        def do_GET(self) -> None:  # noqa: N802
            self._dispatch()

        def do_POST(self) -> None:  # noqa: N802
            self._dispatch()

        def log_message(self, fmt: str, *args: Any) -> None:
            return

    class ReuseServer(ThreadingHTTPServer):
        allow_reuse_address = True

    server = ReuseServer(("127.0.0.1", port), HandlerImpl)
    server.daemon_threads = True
    return server


def install_term_handler(server: ThreadingHTTPServer) -> None:
    def handle(signum: int, frame: Any) -> None:
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, handle)
    signal.signal(signal.SIGINT, handle)


def json_response(status: int, payload: Any, headers: dict[str, str] | None = None) -> tuple[int, dict[str, str], bytes]:
    extra = {"Content-Type": "application/json; charset=utf-8"}
    if headers:
        extra.update(headers)
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    return status, extra, raw


def text_response(status: int, text: str) -> tuple[int, dict[str, str], bytes]:
    return status, {"Content-Type": "text/plain; charset=utf-8"}, text.encode("utf-8")


def empty_response(status: int) -> tuple[int, dict[str, str], bytes]:
    return status, {}, b""


def split_path(path: str) -> tuple[str, dict[str, str]]:
    from urllib.parse import parse_qs

    parts = urlsplit(path)
    query = {key: values[-1] for key, values in parse_qs(parts.query).items()}
    return parts.path, query


def header_request_id(headers: dict[str, str]) -> str:
    incoming = headers.get("x-request-id")
    if incoming:
        return incoming
    return new_id("req_")
