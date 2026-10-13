"""拉起进程、公布主存储、在心跳中断时提升从存储，并按积压加减工人。"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import threading
import time
from typing import Any

from orange.common import (
    HEARTBEAT_SEC,
    LEADER_LOST_SEC,
    PROMOTION_PAUSE_SEC,
    SCALE_COOLDOWN_SEC,
    SCALE_IN_HOLD_SEC,
    SCALE_OUT_HOLD_SEC,
    SCALE_OUT_PENDING,
    WORKER_MAX,
    WORKER_MIN,
    decode_json,
    empty_response,
    error_body,
    header_request_id,
    http_request,
    json_response,
    log_event,
    serve,
    split_path,
    start_fault_reader,
)


STORE_A = "http://127.0.0.1:8301"
STORE_B = "http://127.0.0.1:8302"
SUPERVISOR_URL = "http://127.0.0.1:8090"


class Supervisor:
    def __init__(self, root: str) -> None:
        self.root = root
        self.leader = STORE_A
        self.epoch = 1
        self.jobs_pending = 0
        self.gateway_paused = False
        self.last_promotion_error: str | None = None
        self.applied: dict[str, int] = {"store-a": 0, "store-b": 0}
        self.last_beat: dict[str, float] = {}
        self.seen: set[str] = set()
        self.ready: dict[str, bool] = {}
        self.pids: dict[str, int] = {}
        self.last_error: dict[str, str] = {}
        self.procs: dict[str, subprocess.Popen[bytes]] = {}
        self.expected = ["gateway", "order-1", "order-2", "recommender", "store-a", "store-b", "worker-1"]
        self.worker_count = 1
        self.high_since: float | None = None
        self.zero_since: float | None = None
        self.last_scale = 0.0
        self.promoting = False
        self.armed = False
        self._lock = threading.Lock()

    def command(self, name: str) -> list[str]:
        python = sys.executable
        common = ["--root", self.root]
        data = os.path.join(self.root, "runtime", "data")
        if name == "store-a":
            return [
                python, "-m", "orange.store",
                "--node-id", "store-a", "--port", "8301", "--role", "leader",
                "--peer", STORE_B, "--data", os.path.join(data, "store-a.sqlite"),
                "--supervisor", SUPERVISOR_URL, *common,
            ]
        if name == "store-b":
            return [
                python, "-m", "orange.store",
                "--node-id", "store-b", "--port", "8302", "--role", "follower",
                "--peer", STORE_A, "--data", os.path.join(data, "store-b.sqlite"),
                "--supervisor", SUPERVISOR_URL, *common,
            ]
        if name == "gateway":
            return [
                python, "-m", "orange.gateway",
                "--port", "8080", "--supervisor", SUPERVISOR_URL,
                "--orders", "http://127.0.0.1:8101,http://127.0.0.1:8102",
                "--process-name", "gateway", *common,
            ]
        if name == "order-1":
            return self._order("order-1", 8101) + common
        if name == "order-2":
            return self._order("order-2", 8102) + common
        if name == "recommender":
            return [python, "-m", "orange.recommender", "--port", "8200", "--process-name", "recommender", *common]
        if name.startswith("worker-"):
            index = int(name.split("-")[1])
            return [
                python, "-m", "orange.worker",
                "--process-name", name, "--port", str(8400 + index),
                "--supervisor", SUPERVISOR_URL, *common,
            ]
        raise KeyError(name)

    def _order(self, name: str, port: int) -> list[str]:
        return [
            sys.executable, "-m", "orange.order_service",
            "--process-name", name, "--port", str(port),
            "--supervisor", SUPERVISOR_URL, "--recommender", "http://127.0.0.1:8200",
        ]

    def spawn(self, name: str) -> None:
        if name in self.procs and self.procs[name].poll() is None:
            return
        env = os.environ.copy()
        env["ORANGE_ROOT"] = self.root
        env["ORANGE_PROCESS"] = name
        proc = subprocess.Popen(self.command(name), env=env)
        self.procs[name] = proc
        self.pids[name] = proc.pid
        log_event("spawned", process_name=name, pid=proc.pid)

    def stop_process(self, name: str) -> None:
        proc = self.procs.get(name)
        if proc is None or proc.poll() is not None:
            return
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
        self.ready[name] = False

    def ensure(self) -> None:
        for name in list(self.expected):
            proc = self.procs.get(name)
            if proc is not None and proc.poll() is not None:
                self.last_error[name] = f"退出码 {proc.returncode}"
                self.ready[name] = False
                log_event("process_exited", level="error", process_name=name, code=proc.returncode)
                self.procs.pop(name, None)
                time.sleep(0.5)
            if name not in self.procs or self.procs[name].poll() is not None:
                try:
                    self.spawn(name)
                except Exception as exc:
                    self.last_error[name] = str(exc)

    def probe_ready(self) -> None:
        ports = {
            "gateway": 8080,
            "order-1": 8101,
            "order-2": 8102,
            "recommender": 8200,
            "store-a": 8301,
            "store-b": 8302,
            "worker-1": 8401,
            "worker-2": 8402,
            "worker-3": 8403,
        }
        for name in list(self.expected):
            port = ports.get(name)
            if port is None:
                continue
            try:
                status, _, _ = http_request("GET", f"http://127.0.0.1:{port}/ready", timeout=0.3)
                self.ready[name] = status == 200
            except Exception:
                self.ready[name] = False

    def note_heartbeat(self, body: dict[str, Any]) -> None:
        node_id = str(body.get("node_id") or "")
        if node_id not in ("store-a", "store-b"):
            return
        self.last_beat[node_id] = time.monotonic()
        self.seen.add(node_id)
        self.applied[node_id] = int(body.get("applied_index") or 0)
        role = body.get("role")
        epoch = int(body.get("epoch") or 0)
        if role == "leader" and epoch >= self.epoch:
            self.epoch = epoch
            self.leader = STORE_A if node_id == "store-a" else STORE_B
            self.jobs_pending = int(body.get("jobs_pending") or 0)
        if len(self.seen) >= 2:
            self.armed = True

    def maybe_promote(self) -> None:
        if not self.armed or self.promoting:
            return
        now = time.monotonic()
        leader_id = "store-a" if self.leader == STORE_A else "store-b"
        follower_id = "store-b" if leader_id == "store-a" else "store-a"
        leader_seen = self.last_beat.get(leader_id)
        follower_seen = self.last_beat.get(follower_id)
        if leader_seen is None or follower_seen is None:
            return
        if now - leader_seen <= LEADER_LOST_SEC or now - follower_seen > LEADER_LOST_SEC:
            return
        self.promoting = True
        self.gateway_paused = True
        started = time.monotonic()
        follower_url = STORE_B if follower_id == "store-b" else STORE_A
        old_leader = self.leader
        try:
            status, _, raw = http_request(
                "POST",
                follower_url + "/internal/promote",
                {"new_epoch": self.epoch + 1, "peer": old_leader},
                timeout=PROMOTION_PAUSE_SEC,
            )
            if status != 200 or not raw:
                self.last_promotion_error = f"提升失败 HTTP {status}"
                log_event("promoted", level="error", message=self.last_promotion_error)
                return
            body = decode_json(raw)
            self.epoch = int(body.get("epoch") or self.epoch + 1)
            self.leader = follower_url
            self.last_promotion_error = None
            log_event("promoted", epoch=self.epoch, leader=self.leader)
        except Exception as exc:
            self.last_promotion_error = str(exc)
            log_event("promoted", level="error", message=str(exc))
        finally:
            if time.monotonic() - started > PROMOTION_PAUSE_SEC:
                self.last_promotion_error = self.last_promotion_error or "切换超过 2 秒"
            self.gateway_paused = False
            self.promoting = False

    def maybe_scale(self) -> None:
        now = time.monotonic()
        pending = self.jobs_pending
        if pending > SCALE_OUT_PENDING:
            self.zero_since = None
            if self.high_since is None:
                self.high_since = now
            cooled = now - self.last_scale >= SCALE_COOLDOWN_SEC
            if (
                now - self.high_since >= SCALE_OUT_HOLD_SEC
                and cooled
                and self.worker_count < WORKER_MAX
            ):
                self.worker_count += 1
                name = f"worker-{self.worker_count}"
                if name not in self.expected:
                    self.expected.append(name)
                self.spawn(name)
                self.last_scale = now
                self.high_since = None
                log_event("scaled", workers=self.worker_count, direction="out")
        else:
            self.high_since = None
        if pending == 0:
            if self.zero_since is None:
                self.zero_since = now
            cooled = now - self.last_scale >= SCALE_COOLDOWN_SEC
            if (
                self.zero_since is not None
                and now - self.zero_since >= SCALE_IN_HOLD_SEC
                and cooled
                and self.worker_count > WORKER_MIN
            ):
                name = f"worker-{self.worker_count}"
                if name in self.expected:
                    self.expected.remove(name)
                self.stop_process(name)
                self.worker_count -= 1
                self.last_scale = now
                self.zero_since = None
                log_event("scaled", workers=self.worker_count, direction="in")
        else:
            self.zero_since = None

    def status(self) -> dict[str, Any]:
        return {
            "leader": self.leader,
            "epoch": self.epoch,
            "applied_index": dict(self.applied),
            "workers": self.worker_count,
            "jobs_pending": self.jobs_pending,
            "gateway_paused": self.gateway_paused,
            "ready": {name: bool(self.ready.get(name)) for name in self.expected},
            "last_promotion_error": self.last_promotion_error,
            "pids": dict(self.pids),
            "last_error": dict(self.last_error),
        }

    def loop(self, stop: threading.Event) -> None:
        while not stop.is_set():
            self.ensure()
            self.probe_ready()
            self.maybe_promote()
            self.maybe_scale()
            stop.wait(HEARTBEAT_SEC)


def build_handler(supervisor: Supervisor):
    def handler(method: str, path: str, headers: dict[str, str], raw: bytes):
        request_id = header_request_id(headers)
        route, _ = split_path(path)
        if method == "GET" and route == "/health":
            return empty_response(200)
        if method == "GET" and route == "/v1/status":
            return json_response(200, supervisor.status())
        if method == "GET" and route == "/internal/leader":
            return json_response(200, {"leader": supervisor.leader, "epoch": supervisor.epoch})
        if method == "GET" and route == "/internal/gateway-control":
            return json_response(
                200,
                {"paused": supervisor.gateway_paused, "jobs_pending": supervisor.jobs_pending},
            )
        if method == "POST" and route == "/internal/heartbeat":
            body = decode_json(raw) or {}
            supervisor.note_heartbeat(body)
            return json_response(200, {"leader": supervisor.leader, "epoch": supervisor.epoch})
        return json_response(404, error_body("not_found", "没有这个路径", request_id))

    return handler


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=os.getcwd())
    args = parser.parse_args()
    import orange.common as common

    common.PROCESS_NAME = "supervisor"
    common.ROOT = args.root
    os.makedirs(os.path.join(args.root, "runtime", "data"), exist_ok=True)
    os.makedirs(os.path.join(args.root, "runtime", "logs"), exist_ok=True)
    faults = os.path.join(args.root, "runtime", "faults.json")
    if not os.path.exists(faults):
        with open(faults, "w", encoding="utf-8") as handle:
            handle.write(
                '{"delay_ms":{"store":0,"recommender":0},"pause_replication":false,'
                '"drop_heartbeat":{"store-a":false,"store-b":false},"exit_after_requests":{}}\n'
            )
    start_fault_reader()
    supervisor = Supervisor(args.root)
    stop = threading.Event()

    def handle_signal(signum: int, frame: Any) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    server = serve(8090, build_handler(supervisor))
    threading.Thread(target=server.serve_forever, name="http-8090", daemon=True).start()
    log_event("process_started", port=8090)
    try:
        supervisor.loop(stop)
    finally:
        for name in list(supervisor.procs):
            supervisor.stop_process(name)
        threading.Thread(target=server.shutdown, daemon=True).start()


if __name__ == "__main__":
    main()
