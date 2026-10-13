"""按技术方案里的 AC-1 到 AC-12 启动整套进程。"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from orange.common import decode_json, http_request


GATEWAY = "http://127.0.0.1:8080"
STATUS = "http://127.0.0.1:8090/v1/status"


def post_order(sku: str = "gear-1", qty: int = 1, key: str | None = None, **extra):
    body = {"sku": sku, "qty": qty}
    body.update(extra)
    headers = {
        "X-Caller-Id": "gear-card",
        "Idempotency-Key": key or f"key-{time.time_ns()}",
        "X-Request-Id": extra.pop("request_id", None) or f"req-{time.time_ns()}",
    }
    # request_id 不是订单字段。上面 pop 已经从 extra 拿走之前 body.update 先执行了。
    body.pop("request_id", None)
    status, response_headers, raw = http_request("POST", GATEWAY + "/v1/orders", body, headers, timeout=3)
    return status, response_headers, decode_json(raw) if raw else None, headers["X-Request-Id"]


class FulfillServer:
    def __init__(self, status: int = 200, delay: float = 0.0, hang_first: bool = False) -> None:
        self.status = status
        self.delay = delay
        self.hang_first = hang_first
        self.calls: list[str] = []
        self.applied: list[str] = []
        self._lock = threading.Lock()
        self._released = threading.Event()

        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length)
                payload = json.loads(raw.decode("utf-8"))
                order_id = payload["order_id"]
                with owner._lock:
                    owner.calls.append(order_id)
                    first = len(owner.calls) == 1
                if owner.hang_first and first:
                    owner._released.wait(30)
                elif owner.delay:
                    time.sleep(owner.delay)
                code = owner.status
                data = b"{}"
                self.send_response(code)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                if 200 <= code < 300 or code == 409:
                    with owner._lock:
                        if order_id not in owner.applied:
                            owner.applied.append(order_id)

            def log_message(self, fmt: str, *args) -> None:
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/hooks/orange-fulfill"

    def release(self) -> None:
        self._released.set()

    def close(self) -> None:
        self.release()
        self.server.shutdown()


class Cluster:
    def __init__(self) -> None:
        self.root = tempfile.mkdtemp(prefix="orange-")
        self.proc: subprocess.Popen[bytes] | None = None

    def start(self) -> None:
        env = os.environ.copy()
        env["ORANGE_ROOT"] = self.root
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "orange.supervisor", "--root", self.root],
            env=env,
            start_new_session=True,
        )
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                status, _, _ = http_request("GET", GATEWAY + "/ready", timeout=0.3)
                if status == 200:
                    return
            except Exception:
                pass
            if self.proc.poll() is not None:
                break
            time.sleep(0.2)
        raise RuntimeError(self.diagnostics())

    def status(self) -> dict:
        _, _, raw = http_request("GET", STATUS, timeout=1)
        return decode_json(raw)

    def write_faults(self, payload: dict) -> None:
        path = os.path.join(self.root, "runtime", "faults.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        time.sleep(0.5)

    def logs(self, name: str) -> str:
        path = os.path.join(self.root, "runtime", "logs", name + ".log")
        if not os.path.exists(path):
            return ""
        with open(path, encoding="utf-8") as handle:
            return handle.read()

    def diagnostics(self) -> str:
        chunks = []
        directory = os.path.join(self.root, "runtime", "logs")
        if os.path.isdir(directory):
            for name in sorted(os.listdir(directory)):
                chunks.append(f"----- {name} -----\n{self.logs(name[:-4])}")
        return "\n".join(chunks) or "监督者没有留下日志"

    def stop(self) -> None:
        if self.proc is None or self.proc.poll() is not None:
            return
        try:
            os.killpg(self.proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(self.proc.pid, signal.SIGKILL)
            self.proc.wait(timeout=3)
        time.sleep(0.3)


class AcceptanceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.cluster = Cluster()
        self.cluster.start()
        self.fulfill: FulfillServer | None = None

    def tearDown(self) -> None:
        if self.fulfill is not None:
            self.fulfill.close()
        self.cluster.stop()

    def test_ac1_same_key_returns_one_order(self) -> None:
        status, _, first, _ = post_order(key="same-key-0001")
        self.assertEqual(status, 201, first)
        status, _, second, _ = post_order(key="same-key-0001")
        self.assertEqual(status, 201, second)
        self.assertEqual(first["order_id"], second["order_id"])

    def test_ac2_same_key_different_sku_conflicts(self) -> None:
        status, _, first, _ = post_order(sku="gear-1", key="same-key-0002")
        self.assertEqual(status, 201, first)
        status, _, second, _ = post_order(sku="gear-2", key="same-key-0002")
        self.assertEqual(status, 409, second)
        self.assertEqual(second["error"]["code"], "idempotency_mismatch")
        got, _, raw = http_request(
            "GET",
            GATEWAY + "/v1/orders/" + first["order_id"],
            headers={"X-Caller-Id": "gear-card"},
            timeout=2,
        )
        self.assertEqual(got, 200)
        self.assertEqual(decode_json(raw)["sku"], "gear-1")

    def test_ac3_stopped_order_replica_is_restarted(self) -> None:
        before = self.cluster.status()
        pid = before["pids"]["order-2"]
        os.kill(pid, signal.SIGTERM)
        saw_down = False
        restored = False
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            view = self.cluster.status()
            ready = view["ready"].get("order-2", False)
            saw_down = saw_down or not ready
            if ready and view["pids"].get("order-2") != pid:
                restored = True
                break
            time.sleep(0.1)
        status, _, body, _ = post_order(key="after-order-b")
        self.assertEqual(status, 201, body)
        self.assertTrue(saw_down, self.cluster.diagnostics())
        self.assertTrue(restored, self.cluster.diagnostics())

    def test_ac4_stopped_recommender_degrades_order(self) -> None:
        pid = self.cluster.status()["pids"]["recommender"]
        os.kill(pid, signal.SIGTERM)
        status = 0
        body = None
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            status, _, body, _ = post_order(key=f"degrade-{time.time_ns()}")
            if status == 201 and body and body.get("degraded") is True and body.get("recommendation") is None:
                break
            time.sleep(0.05)
        self.assertEqual(status, 201, body)
        self.assertTrue(body["degraded"])
        self.assertIsNone(body["recommendation"])

    def test_ac5_breaker_stops_calling_recommender(self) -> None:
        self.cluster.write_faults(
            {
                "delay_ms": {"store": 0, "recommender": 500},
                "pause_replication": False,
                "drop_heartbeat": {"store-a": False, "store-b": False},
                "exit_after_requests": {},
            }
        )
        for index in range(12):
            status, _, body, _ = post_order(key=f"breaker-{index:02d}-key")
            self.assertEqual(status, 201, body)
        time.sleep(0.3)
        before = self.cluster.logs("recommender").count('"event":"recommend_served"')
        status, _, body, _ = post_order(key="breaker-after-open")
        self.assertEqual(status, 201, body)
        time.sleep(0.3)
        after = self.cluster.logs("recommender").count('"event":"recommend_served"')
        self.assertEqual(before, after, self.cluster.logs("recommender"))
        self.assertIn('"event":"breaker_open"', self.cluster.logs("order-1") + self.cluster.logs("order-2"))

    def test_ac6_empty_bucket_returns_retry_after(self) -> None:
        # 先占满 8 个在途名额，后续请求会立刻返回，从而在令牌补充前打空桶。
        self.cluster.write_faults(
            {
                "delay_ms": {"store": 350, "recommender": 0},
                "pause_replication": False,
                "drop_heartbeat": {"store-a": False, "store-b": False},
                "exit_after_requests": {},
            }
        )
        holders: list[tuple] = []

        def hold(index: int) -> None:
            holders.append(post_order(key=f"hold-{index:02d}-xxxx"))

        threads = [threading.Thread(target=hold, args=(index,)) for index in range(8)]
        for thread in threads:
            thread.start()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            _, _, raw = http_request("GET", GATEWAY + "/metrics", timeout=1)
            if raw and b"orange_inflight 8" in raw:
                break
            time.sleep(0.02)
        results = [post_order(key=f"bucket-{index:02d}-xx") for index in range(40)]
        for thread in threads:
            thread.join(timeout=3)
        limited = [item for item in results if item[0] == 429 and item[2]["error"]["message"] == "令牌桶已空"]
        self.assertTrue(limited, results + holders)
        headers = {key.lower(): value for key, value in limited[0][1].items()}
        self.assertIn("retry-after", headers)

    def test_ac7_claimed_job_is_retried_once(self) -> None:
        self.fulfill = FulfillServer(hang_first=True)
        status, _, body, _ = post_order(key="visibility-timeout", fulfill_url=self.fulfill.url)
        self.assertEqual(status, 201, body)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not self.fulfill.calls:
            time.sleep(0.05)
        self.assertTrue(self.fulfill.calls, self.cluster.diagnostics())
        worker_pid = self.cluster.status()["pids"]["worker-1"]
        os.kill(worker_pid, signal.SIGTERM)
        self.fulfill.release()
        deadline = time.monotonic() + 25
        state = ""
        while time.monotonic() < deadline:
            got, _, raw = http_request(
                "GET",
                GATEWAY + "/v1/orders/" + body["order_id"],
                headers={"X-Caller-Id": "gear-card"},
                timeout=2,
            )
            self.assertEqual(got, 200)
            state = decode_json(raw)["state"]
            if state == "fulfilled":
                break
            time.sleep(0.5)
        self.assertEqual(state, "fulfilled", self.cluster.diagnostics())
        self.assertEqual(len(self.fulfill.applied), 1)

    def test_ac8_repeated_fulfill_failure_goes_dead(self) -> None:
        self.fulfill = FulfillServer(status=500)
        status, _, body, _ = post_order(key="dead-letter-0001", fulfill_url=self.fulfill.url)
        self.assertEqual(status, 201, body)
        deadline = time.monotonic() + 20
        state = ""
        while time.monotonic() < deadline:
            _, _, raw = http_request(
                "GET",
                GATEWAY + "/v1/orders/" + body["order_id"],
                headers={"X-Caller-Id": "gear-card"},
                timeout=2,
            )
            view = decode_json(raw)
            state = view["state"]
            if state == "fulfillment_failed":
                self.assertEqual(view["fulfillment"]["status"], "dead")
                self.assertGreaterEqual(view["fulfillment"]["attempts"], 5)
                return
            time.sleep(0.3)
        self.fail(self.cluster.diagnostics() + f"\nstate={state}")

    def test_ac9_leader_heartbeat_loss_promotes_follower(self) -> None:
        original = self.cluster.status()["leader"]
        self.cluster.write_faults(
            {
                "delay_ms": {"store": 0, "recommender": 0},
                "pause_replication": False,
                "drop_heartbeat": {"store-a": True, "store-b": False},
                "exit_after_requests": {},
            }
        )
        promoted = None
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            view = self.cluster.status()
            if view["leader"] != original and view["epoch"] > 1:
                promoted = view
                break
            time.sleep(0.2)
        self.assertIsNotNone(promoted, self.cluster.diagnostics())
        self.cluster.write_faults(
            {
                "delay_ms": {"store": 0, "recommender": 0},
                "pause_replication": False,
                "drop_heartbeat": {"store-a": False, "store-b": False},
                "exit_after_requests": {},
            }
        )
        fenced = False
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            status, _, raw = http_request(
                "POST",
                original + "/internal/append",
                {
                    "epoch": 1,
                    "caller_id": "gear-card",
                    "idempotency_key": "fenced-write-01",
                    "order": {"sku": "gear-1", "qty": 1, "external_ref": None, "fulfill_url": None},
                },
                timeout=1,
            )
            if status == 409 and raw and decode_json(raw)["error"]["code"] == "epoch_fenced":
                fenced = True
                break
            time.sleep(0.2)
        self.assertTrue(fenced, self.cluster.diagnostics())
        status, _, body, _ = post_order(key="after-promotion")
        self.assertEqual(status, 201, body)
        self.assertGreater(body["epoch"], 1)

    def test_ac10_pending_limit_rejects_new_orders(self) -> None:
        self.fulfill = FulfillServer(status=500, delay=0.9)
        created = None
        first = None
        limited = False
        for index in range(70):
            status, _, body, _ = post_order(key=f"pile-{index:02d}-order", fulfill_url=self.fulfill.url)
            if status == 201:
                if first is None:
                    first = body
                created = body
                continue
            if status == 429 and body["error"]["message"] == "待处理任务已达上限":
                limited = True
                break
            if status == 429 and body["error"]["message"] == "令牌桶已空":
                time.sleep(0.2)
                continue
            self.fail(f"意外响应 {status} {body}\n{self.cluster.diagnostics()}")
        self.assertTrue(limited, self.cluster.diagnostics())
        self.assertIsNotNone(first)
        before = self.cluster.logs("store-a").count('"event":"job_claimed"')
        deadline = time.monotonic() + 8
        progressed = False
        while time.monotonic() < deadline:
            if self.cluster.logs("store-a").count('"event":"job_claimed"') > before:
                progressed = True
                break
            time.sleep(0.3)
        self.assertTrue(progressed, self.cluster.diagnostics())

    def test_ac11_backlog_scales_workers_without_flapping(self) -> None:
        self.fulfill = FulfillServer(status=500, delay=0.9)
        for index in range(40):
            while True:
                status, _, body, _ = post_order(key=f"scale-{index:02d}-order", fulfill_url=self.fulfill.url)
                if status == 429 and body["error"]["message"] == "令牌桶已空":
                    time.sleep(0.2)
                    continue
                self.assertEqual(status, 201, body)
                break
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            if self.cluster.status()["workers"] >= 3:
                break
            time.sleep(0.5)
        self.assertGreaterEqual(self.cluster.status()["workers"], 3, self.cluster.diagnostics())
        time.sleep(10)
        self.assertEqual(self.cluster.status()["workers"], 3, self.cluster.diagnostics())

    def test_ac12_request_id_spans_gateway_order_and_store(self) -> None:
        self.cluster.write_faults(
            {
                "delay_ms": {"store": 200, "recommender": 0},
                "pause_replication": False,
                "drop_heartbeat": {"store-a": False, "store-b": False},
                "exit_after_requests": {},
            }
        )
        request_id = "req-trace-ac12-0001"
        status, _, raw = http_request(
            "POST",
            GATEWAY + "/v1/orders",
            {"sku": "gear-1", "qty": 1},
            headers={
                "X-Caller-Id": "gear-card",
                "Idempotency-Key": "trace-request-001",
                "X-Request-Id": request_id,
            },
            timeout=3,
        )
        self.assertEqual(status, 201, raw)
        time.sleep(0.3)
        gateway = self.cluster.logs("gateway")
        orders = self.cluster.logs("order-1") + self.cluster.logs("order-2")
        stores = self.cluster.logs("store-a") + self.cluster.logs("store-b")
        self.assertIn(request_id, gateway, gateway)
        self.assertIn(request_id, orders, orders)
        self.assertIn(request_id, stores, stores)


if __name__ == "__main__":
    unittest.main()
