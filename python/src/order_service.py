"""无状态订单进程。幂等、截止时间、推荐熔断都在这里。"""

from __future__ import annotations

import argparse
import os
import threading
import time
from typing import Any
from urllib.parse import quote

from common import (
    RECOMMEND_BUDGET_MS,
    STORE_CALL_CAP_MS,
    CircuitBreaker,
    Metrics,
    canonical_body,
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


class OrderService:
    def __init__(self, name: str, supervisor: str, recommender: str) -> None:
        self.name = name
        self.supervisor = supervisor
        self.recommender = recommender
        self.breaker = CircuitBreaker()
        self.metrics = Metrics()
        self.leader = ""
        self.epoch = 1
        self.leader_ok = False
        self.checked_at = 0.0
        self._lock = threading.Lock()
        self._stop = False

    def refresh_leader(self) -> None:
        while not self._stop:
            try:
                status, _, raw = http_request("GET", self.supervisor + "/internal/leader", timeout=0.3)
                if status == 200 and raw:
                    body = decode_json(raw)
                    leader = body.get("leader") or ""
                    epoch = int(body.get("epoch") or 0)
                    ready = False
                    if leader:
                        probe, _, _ = http_request("GET", leader + "/ready", timeout=0.3)
                        ready = probe == 200
                    with self._lock:
                        self.leader = leader
                        self.epoch = epoch or self.epoch
                        self.leader_ok = ready
                        self.checked_at = time.monotonic()
            except Exception:
                pass
            time.sleep(0.2)

    def current_leader(self) -> tuple[str, int, bool]:
        with self._lock:
            fresh = time.monotonic() - self.checked_at < 1 and bool(self.leader)
            return self.leader, self.epoch, fresh and self.leader_ok

    def ready(self) -> bool:
        _, _, ok = self.current_leader()
        return ok


def _store_call(method: str, url: str, request_id: str, deadline_ms: int, body: Any = None) -> tuple[int, Any]:
    remaining_ms = deadline_ms - now_ms()
    if remaining_ms <= 0:
        return 504, None
    timeout = min(remaining_ms, STORE_CALL_CAP_MS) / 1000
    headers = {"X-Request-Id": request_id, "X-Deadline-Ms": str(deadline_ms)}
    status, _, raw = http_request(method, url, body=body, headers=headers, timeout=timeout)
    return status, decode_json(raw) if raw else None


def build_handler(service: OrderService):
    def handler(method: str, path: str, headers: dict[str, str], raw: bytes):
        request_id = header_request_id(headers)
        route, query = split_path(path)
        if method == "GET" and route == "/health":
            return empty_response(200)
        if method == "GET" and route == "/ready":
            return empty_response(200 if service.ready() else 503)
        if method == "GET" and route == "/metrics":
            service.metrics.set("orange_breaker_open", 1 if service.breaker.open else 0)
            return text_response(
                200,
                service.metrics.render(["orange_errors_total", "orange_inflight", "orange_breaker_open"]),
            )
        if method == "GET" and (route.startswith("/internal/orders/") or route == "/internal/orders"):
            return proxy_read(service, route, query, headers, request_id)
        if method == "POST" and route == "/internal/orders":
            return create_order(service, headers, raw, request_id)
        return json_response(404, error_body("not_found", "没有这个路径", request_id))

    return handler


def proxy_read(service: OrderService, route: str, query: dict[str, str], headers: dict[str, str], request_id: str):
    caller = headers.get("x-caller-id", "")
    if not valid_caller(caller):
        return json_response(400, error_body("invalid_argument", "X-Caller-Id 不合法", request_id))
    leader, _, ok = service.current_leader()
    deadline = int(headers.get("x-deadline-ms") or now_ms() + 800)
    if not ok:
        return json_response(503, error_body("unavailable", "主存储不可用", request_id))
    if route == "/internal/orders":
        key = query.get("idempotency_key", "")
        url = f"{leader}/internal/orders/by-key?caller_id={quote(caller)}&idempotency_key={quote(key)}"
    else:
        order_id = route.split("/")[-1]
        url = f"{leader}/internal/orders/{order_id}?caller_id={quote(caller)}"
    try:
        status, body = _store_call("GET", url, request_id, deadline)
    except Exception:
        service.metrics.inc("orange_errors_total")
        return json_response(503, error_body("unavailable", "主存储不可用", request_id))
    if status == 504 or body is None:
        return json_response(504, error_body("deadline_exceeded", "超过截止时间", request_id))
    if isinstance(body, dict):
        body.pop("request_hash", None)
    return json_response(status, body)


def create_order(service: OrderService, headers: dict[str, str], raw: bytes, request_id: str):
    caller = headers.get("x-caller-id", "")
    key = headers.get("idempotency-key", "")
    deadline = int(headers.get("x-deadline-ms") or 0)
    if not valid_caller(caller):
        return json_response(400, error_body("invalid_argument", "X-Caller-Id 不合法", request_id))
    if not valid_idempotency_key(key):
        return json_response(400, error_body("invalid_argument", "Idempotency-Key 不合法", request_id))
    if deadline and now_ms() >= deadline:
        log_event("deadline_exceeded", request_id)
        return json_response(504, error_body("deadline_exceeded", "超过截止时间", request_id))
    try:
        parsed = decode_json(raw)
    except Exception:
        return json_response(400, error_body("invalid_argument", "正文不是 JSON", request_id))
    fields, message = validate_order_body(parsed)
    if fields is None:
        return json_response(400, error_body("invalid_argument", message or "正文不合法", request_id))

    service.metrics.inc("orange_inflight")
    try:
        replay = lookup_existing(service, caller, key, fields, request_id, deadline)
        if replay is not None:
            return replay
        return write_new(service, caller, key, fields, request_id, deadline)
    finally:
        service.metrics.inc("orange_inflight", -1)


def lookup_existing(service: OrderService, caller: str, key: str, fields: dict[str, Any], request_id: str, deadline: int):
    leader, _, ok = service.current_leader()
    if not ok:
        return json_response(503, error_body("unavailable", "主存储不可用", request_id))
    url = f"{leader}/internal/orders/by-key?caller_id={quote(caller)}&idempotency_key={quote(key)}"
    try:
        status, body = _store_call("GET", url, request_id, deadline or now_ms() + 800)
    except Exception:
        return None
    if status == 200 and isinstance(body, dict) and body.get("order_id"):
        digest = canonical_body(fields["sku"], fields["qty"], fields.get("external_ref"), fields.get("fulfill_url"))
        if body.get("request_hash") != digest:
            return json_response(409, error_body("idempotency_mismatch", "幂等键与已有订单正文不一致", request_id))
        body.pop("request_hash", None)
        log_event("order_replayed", request_id, order_id=body["order_id"])
        return json_response(201, body)
    return None


def write_new(service: OrderService, caller: str, key: str, fields: dict[str, Any], request_id: str, deadline: int):
    started = time.monotonic()
    recommend: dict[str, Any] = {}

    def ask_recommend() -> None:
        if not service.breaker.allow():
            log_event("breaker_open", request_id)
            service.metrics.set("orange_breaker_open", 1)
            return
        try:
            status, _, raw = http_request(
                "GET",
                f"{service.recommender}/internal/recommend?sku={quote(fields['sku'])}",
                headers={"X-Request-Id": request_id},
                timeout=RECOMMEND_BUDGET_MS / 1000,
            )
            if status != 200:
                service.breaker.record(False)
                log_event("recommend_failed", request_id, status=status)
                return
            body = decode_json(raw) or {}
            text = body.get("recommendation")
            if not isinstance(text, str) or not text:
                service.breaker.record(False)
                log_event("recommend_failed", request_id, status=status)
                return
            service.breaker.record(True)
            recommend["text"] = text
            service.metrics.set("orange_breaker_open", 0)
        except Exception as exc:
            service.breaker.record(False)
            log_event("recommend_failed", request_id, message=str(exc))

    thread = threading.Thread(target=ask_recommend, daemon=True)
    thread.start()
    stored = call_append(service, caller, key, fields, request_id, deadline)
    if isinstance(stored, tuple):
        return stored
    remaining = (RECOMMEND_BUDGET_MS / 1000) - (time.monotonic() - started)
    if remaining > 0:
        thread.join(remaining)
    if "text" not in recommend:
        log_event(
            "order_accepted",
            request_id,
            order_id=stored["order_id"],
            epoch=stored["epoch"],
            degraded=True,
        )
        return json_response(201, stored)
    leader, epoch, ok = service.current_leader()
    if not ok:
        return json_response(201, stored)
    try:
        status, body = _store_call(
            "POST",
            leader + "/internal/snapshot",
            request_id,
            deadline or now_ms() + 200,
            {"order_id": stored["order_id"], "epoch": epoch, "recommendation": recommend["text"], "degraded": False},
        )
    except Exception:
        body = None
        status = 0
    if status == 200 and isinstance(body, dict):
        log_event("order_accepted", request_id, order_id=body["order_id"], epoch=body["epoch"], degraded=False)
        return json_response(201, body)
    return json_response(201, stored)


def call_append(service: OrderService, caller: str, key: str, fields: dict[str, Any], request_id: str, deadline: int):
    last_error = json_response(503, error_body("unavailable", "主存储不可用", request_id))
    for _ in range(2):
        leader, epoch, ok = service.current_leader()
        if not ok:
            service.refresh_once()
            leader, epoch, ok = service.current_leader()
        if not ok:
            return last_error
        payload = {
            "epoch": epoch,
            "caller_id": caller,
            "idempotency_key": key,
            "order": fields,
        }
        try:
            status, body = _store_call("POST", leader + "/internal/append", request_id, deadline or now_ms() + 800, payload)
        except Exception:
            service.metrics.inc("orange_errors_total")
            log_event("deadline_exceeded", request_id)
            return json_response(504, error_body("deadline_exceeded", "超过截止时间", request_id))
        if status == 409 and isinstance(body, dict) and body.get("error", {}).get("code") == "epoch_fenced":
            log_event("write_fenced", request_id, epoch=body["error"].get("epoch"))
            service.refresh_once()
            continue
        if status == 409:
            return json_response(409, body)
        if status == 200 and isinstance(body, dict) and "order" in body:
            if not body.get("created"):
                log_event("order_replayed", request_id, order_id=body["order"]["order_id"])
            return body["order"]
        last_error = json_response(status or 503, body or error_body("unavailable", "主存储不可用", request_id))
        return last_error
    return json_response(503, error_body("unavailable", "主存储正在切换", request_id))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--process-name", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--supervisor", required=True)
    parser.add_argument("--recommender", required=True)
    parser.add_argument("--root", default=os.getcwd())
    args = parser.parse_args()
    import common

    common.PROCESS_NAME = args.process_name
    common.ROOT = args.root
    start_fault_reader()
    service = OrderService(args.process_name, args.supervisor, args.recommender)
    threading.Thread(target=service.refresh_leader, name="leader", daemon=True).start()
    # 给 refresh_once 用同一个循环里的一次拉取。
    service.refresh_once = lambda: None

    def refresh_once() -> None:
        try:
            status, _, raw = http_request("GET", service.supervisor + "/internal/leader", timeout=0.3)
            if status != 200 or not raw:
                return
            body = decode_json(raw)
            leader = body.get("leader") or ""
            ready = False
            if leader:
                probe, _, _ = http_request("GET", leader + "/ready", timeout=0.3)
                ready = probe == 200
            with service._lock:
                service.leader = leader
                service.epoch = int(body.get("epoch") or service.epoch)
                service.leader_ok = ready
                service.checked_at = time.monotonic()
        except Exception:
            return

    service.refresh_once = refresh_once  # type: ignore[method-assign]
    server = serve(args.port, build_handler(service))
    install_term_handler(server)
    log_event("process_started", port=args.port)
    server.serve_forever()


if __name__ == "__main__":
    main()
