"""直接使用标准库的 ThreadingHTTPServer。"""

import threading
import unittest
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class ThreadingHTTPServerTest(unittest.TestCase):
    def test_two_requests_are_handled_together(self) -> None:
        both_entered = threading.Barrier(2)

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self) -> None:  # noqa: N802
                both_entered.wait(timeout=2)
                body = b"ok"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, fmt: str, *args: object) -> None:
                return

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        errors: list[BaseException] = []
        statuses: list[int] = []

        def request() -> None:
            connection = HTTPConnection("127.0.0.1", server.server_address[1], timeout=3)
            try:
                connection.request("GET", "/")
                response = connection.getresponse()
                response.read()
                statuses.append(response.status)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)
            finally:
                connection.close()

        workers = [threading.Thread(target=request) for _ in range(2)]
        try:
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join(timeout=4)
            self.assertEqual(errors, [])
            self.assertEqual(statuses, [200, 200])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
