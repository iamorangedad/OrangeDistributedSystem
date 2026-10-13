"""唯一对外入口。限流、截止时间，并把请求分给就绪的订单副本。"""

from __future__ import annotations

import argparse
import os
import threading
import time

from orange.common import (
    GATEWAY_INFLIGHT_LIMIT,
    JOBS_PENDING_LIMIT,
    ORDER_DEADLINE_MS,
    Metrics,
    TokenBucket,
    decode_json,
    empty_response,
    error_body,
    header_request_id,
    http_request,
    install_term_handler,
    json_response,
    log_event,
    now_ms,
    serve,
    split_path,
    start_fault_reader,
    text_response,
    valid_caller,
    valid_idempotency_key,
    validate_order_body,
)


class Gateway:
    def __init__(self, supervisor: str, orders: list[str]) -> None:
        self.supervisor = supervisor
        self.orders = orders
        self.bucket = TokenBucket()
        self.metrics = Metrics()
        self.inflight = 0
        self.cursor = 0
        self.order_ready = {url: False for url in orders}
        self.paused = False
        self.jobs_pending = 0
        self._lock = threading.Lock()
        self._stop = False

    def poll(self) -> None:
        while not self._stop:
            for url in self.orders:
                try:
                    status, _, _ = http_request("GET", url + "/ready", timeout=0.3)
                    ready = status == 200
                except Exception:
                    ready = False
                with self._lock:
                    self.order_ready[url] = ready
            try:
                status, _, raw = http_request("GET", self.supervisor + "/internal/gateway-control", timeout=0.3)
                if status == 200 and raw:
                    body = decode_json(raw)
                    with self._lock:
                        self.paused = bool(body.get("paused"))
                        self.jobs_pending = int(body.get("jobs_pending") or 0)
            except Exception:
                pass
            time.sleep(0.2)

    def pick(self) -> str | None:
        with self._lock:
            ready = [url for url in self.orders if self.order_ready.get(url)]
            if not ready or self.paused:
                return None
            chosen = ready[self.cursor % len(ready)]
            self.cursor += 1
            return chosen

    def ready(self) -> bool:
        with self._lock:
            return (not self.paused) and any(self.order_ready.values())


def build_handler(gateway: Gateway):
    def handler(method: str, path: str, headers: dict[str, str], raw: bytes):
        request_id = header_request_id(headers)
        route, query = split_path(path)
        if method == "GET" and route == "/health":
            return empty_response(200)
        if method == "GET" and route == "/ready":
            return empty_response(200 if gateway.ready() else 503)
        if method == "GET" and route == "/metrics":
            gateway.metrics.set("orange_inflight", gateway.inflight)
            return text_response(200, gateway.metrics.render(["orange_errors_total", "orange_inflight"]))
        if method == "POST" and route == "/v1/orders":
            return create(gateway, headers, raw, request_id)
        if method == "GET" and route == "/v1/orders":
            return forward_get(gateway, "/internal/orders", query, headers, request_id)
        if method == "GET" and route.startswith("/v1/orders/"):
            order_id = route[len("/v1/orders/") :]
            return forward_get(gateway, "/internal/orders/" + order_id, {}, headers, request_id)
        return json_response(404, error_body("not_found", "没有这个路径", request_id))

    return handler


def reject(gateway: Gateway, status: int, code: str, message: str, request_id: str, retry_after: int | None = None):
    gateway.metrics.inc("orange_errors_total")
    if code == "rate_limited":
        log_event("rate_limited", request_id, message=message)
    headers = {}
    if retry_after is not None:
        headers["Retry-After"] = str(retry_after)
    return json_response(status, error_body(code, message, request_id), headers)


def create(gateway: Gateway, headers: dict[str, str], raw: bytes, request_id: str):
    caller = headers.get("x-caller-id", "")
    key = headers.get("idempotency-key", "")
    if not valid_caller(caller):
        return json_response(400, error_body("invalid_argument", "X-Caller-Id 不合法", request_id))
    if not valid_idempotency_key(key):
        return json_response(400, error_body("invalid_argument", "Idempotency-Key 不合法", request_id))
    try:
        parsed = decode_json(raw) if raw else None
    except Exception:
        return json_response(400, error_body("invalid_argument", "正文不是 JSON", request_id))
    fields, message = validate_order_body(parsed)
    if fields is None:
        return json_response(400, error_body("invalid_argument", message or "正文不合法", request_id))

    allowed, retry_after = gateway.bucket.take()
    if not allowed:
        return reject(gateway, 429, "rate_limited", "令牌桶已空", request_id, retry_after)
    with gateway._lock:
        pending = gateway.jobs_pending
        paused = gateway.paused
    if pending >= JOBS_PENDING_LIMIT:
        return reject(gateway, 429, "rate_limited", "待处理任务已达上限", request_id, 1)
    with gateway._lock:
        if paused:
            return reject(gateway, 503, "unavailable", "主存储正在切换", request_id)
        if gateway.inflight >= GATEWAY_INFLIGHT_LIMIT:
            return reject(gateway, 429, "rate_limited", "在途请求已满", request_id, 1)
        gateway.inflight += 1
    gateway.metrics.set("orange_inflight", gateway.inflight)
    try:
        deadline = now_ms() + ORDER_DEADLINE_MS
        forward_headers = {
            "X-Caller-Id": caller,
            "Idempotency-Key": key,
            "X-Request-Id": request_id,
            "X-Deadline-Ms": str(deadline),
        }
        for _ in range(len(gateway.orders)):
            target = gateway.pick()
            if target is None:
                return reject(gateway, 503, "unavailable", "没有就绪的订单副本", request_id)
            try:
                status, response_headers, body = http_request(
                    "POST",
                    target + "/internal/orders",
                    body=fields,
                    headers=forward_headers,
                    timeout=(ORDER_DEADLINE_MS / 1000) + 0.2,
                )
            except Exception:
                gateway.metrics.inc("orange_errors_total")
                continue
            extra = {"X-Request-Id": request_id}
            if "retry-after" in response_headers:
                extra["Retry-After"] = response_headers["retry-after"]
            content_type = response_headers.get("content-type", "application/json")
            log_event("gateway_forward", request_id, status=status)
            return status, {"Content-Type": content_type, **extra}, body
        return reject(gateway, 503, "unavailable", "没有就绪的订单副本", request_id)
    finally:
        with gateway._lock:
            gateway.inflight -= 1
        gateway.metrics.set("orange_inflight", gateway.inflight)


def forward_get(gateway: Gateway, route: str, query: dict[str, str], headers: dict[str, str], request_id: str):
    caller = headers.get("x-caller-id", "")
    if not valid_caller(caller):
        return json_response(400, error_body("invalid_argument", "X-Caller-Id 不合法", request_id))
    if route == "/internal/orders" and not query.get("idempotency_key"):
        return json_response(400, error_body("invalid_argument", "缺少 idempotency_key", request_id))
    target = gateway.pick()
    if target is None:
        return json_response(503, error_body("unavailable", "没有就绪的订单副本", request_id))
    query_text = ""
    if query:
        from urllib.parse import urlencode

        query_text = "?" + urlencode(query)
    deadline = now_ms() + ORDER_DEADLINE_MS
    try:
        status, _, body = http_request(
            "GET",
            target + route + query_text,
            headers={
                "X-Caller-Id": caller,
                "X-Request-Id": request_id,
                "X-Deadline-Ms": str(deadline),
            },
            timeout=1,
        )
    except Exception:
        return json_response(503, error_body("unavailable", "没有就绪的订单副本", request_id))
    return status, {"Content-Type": "application/json; charset=utf-8", "X-Request-Id": request_id}, body


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--supervisor", required=True)
    parser.add_argument("--orders", required=True, help="逗号分隔的订单进程地址")
    parser.add_argument("--root", default=os.getcwd())
    parser.add_argument("--process-name", default="gateway")
    args = parser.parse_args()
    import orange.common as common

    common.PROCESS_NAME = args.process_name
    common.ROOT = args.root
    start_fault_reader()
    gateway = Gateway(args.supervisor, [item for item in args.orders.split(",") if item])
    threading.Thread(target=gateway.poll, name="poll", daemon=True).start()
    server = serve(args.port, build_handler(gateway))
    install_term_handler(server)
    log_event("process_started", port=args.port)
    server.serve_forever()


if __name__ == "__main__":
    main()
