"""工人只向当前主存储领取履约任务。"""

from __future__ import annotations

import argparse
import os
import threading
import time

from orange.common import (
    FULFILL_TIMEOUT_SEC,
    decode_json,
    empty_response,
    header_request_id,
    http_request,
    install_term_handler,
    log_event,
    new_id,
    serve,
    split_path,
    start_fault_reader,
    text_response,
)


class Worker:
    def __init__(self, name: str, supervisor: str) -> None:
        self.name = name
        self.supervisor = supervisor
        self.leader = ""
        self.checked_at = 0.0
        self.errors = 0
        self._stop = False
        self._lock = threading.Lock()

    def refresh(self) -> None:
        while not self._stop:
            try:
                status, _, raw = http_request("GET", self.supervisor + "/internal/leader", timeout=0.3)
                if status == 200 and raw:
                    body = decode_json(raw)
                    with self._lock:
                        self.leader = body.get("leader") or ""
                        self.checked_at = time.monotonic()
            except Exception:
                pass
            time.sleep(0.2)

    def leader_now(self) -> str:
        with self._lock:
            if self.leader and time.monotonic() - self.checked_at < 1:
                return self.leader
            return ""

    def run(self) -> None:
        while not self._stop:
            leader = self.leader_now()
            if not leader:
                time.sleep(0.2)
                continue
            request_id = new_id("req_")
            try:
                status, _, raw = http_request(
                    "POST",
                    leader + "/internal/jobs/claim",
                    {"worker_id": self.name},
                    headers={"X-Request-Id": request_id},
                    timeout=1,
                )
            except Exception:
                self.errors += 1
                time.sleep(0.2)
                continue
            if status == 204:
                time.sleep(0.2)
                continue
            if status != 200 or not raw:
                self.errors += 1
                time.sleep(0.2)
                continue
            job = decode_json(raw)
            ok, error = self.fulfill(job, request_id)
            path = "/ack" if ok else "/fail"
            body = None if ok else {"retryable": True, "error": error}
            try:
                http_request(
                    "POST",
                    f"{leader}/internal/jobs/{job['job_id']}{path}",
                    body,
                    headers={"X-Request-Id": request_id},
                    timeout=1,
                )
            except Exception:
                self.errors += 1

    def fulfill(self, job: dict, request_id: str) -> tuple[bool, str]:
        url = job.get("fulfill_url")
        if not url:
            return True, ""
        payload = {
            "event": "fulfill",
            "order_id": job["order_id"],
            "caller_id": job["caller_id"],
            "external_ref": job.get("external_ref"),
            "sku": job["sku"],
            "qty": job["qty"],
        }
        try:
            status, _, _ = http_request(
                "POST",
                url,
                payload,
                headers={"X-Request-Id": request_id},
                timeout=FULFILL_TIMEOUT_SEC,
            )
        except Exception as exc:
            return False, str(exc)
        if 200 <= status < 300 or status == 409:
            return True, ""
        return False, f"履约地址返回 {status}"


def build_handler(worker: Worker):
    def handler(method: str, path: str, headers: dict[str, str], raw: bytes):
        route, _ = split_path(path)
        if method == "GET" and route == "/health":
            return empty_response(200)
        if method == "GET" and route == "/ready":
            return empty_response(200 if worker.leader_now() else 503)
        if method == "GET" and route == "/metrics":
            return text_response(200, f"orange_errors_total {worker.errors}\n")
        return empty_response(404)

    return handler


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--process-name", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--supervisor", required=True)
    parser.add_argument("--root", default=os.getcwd())
    args = parser.parse_args()
    import orange.common as common

    common.PROCESS_NAME = args.process_name
    common.ROOT = args.root
    start_fault_reader()
    worker = Worker(args.process_name, args.supervisor)
    threading.Thread(target=worker.refresh, name="leader", daemon=True).start()
    threading.Thread(target=worker.run, name="loop", daemon=True).start()
    server = serve(args.port, build_handler(worker))
    install_term_handler(server)
    log_event("process_started", port=args.port)
    server.serve_forever()


if __name__ == "__main__":
    main()
