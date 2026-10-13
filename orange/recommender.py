"""可失败的推荐旁路。不访问存储。"""

from __future__ import annotations

import argparse
import os

from orange.common import (
    decode_json,
    empty_response,
    error_body,
    fault_delay,
    header_request_id,
    install_term_handler,
    json_response,
    log_event,
    serve,
    split_path,
    start_fault_reader,
    text_response,
)

CATALOG = {"gear-1": "配套护具"}


def recommendation_for(sku: str) -> str:
    return CATALOG.get(sku, f"相关:{sku}")


def build_handler():
    def handler(method: str, path: str, headers: dict[str, str], raw: bytes):
        request_id = header_request_id(headers)
        route, query = split_path(path)
        if method == "GET" and route == "/health":
            return empty_response(200)
        if method == "GET" and route == "/ready":
            return empty_response(200)
        if method == "GET" and route == "/metrics":
            return text_response(200, "orange_errors_total 0\n")
        if method == "GET" and route == "/internal/recommend":
            sku = query.get("sku") or ""
            if not sku:
                return json_response(400, error_body("invalid_argument", "缺少 sku", request_id))
            log_event("recommend_served", request_id, sku=sku)
            fault_delay("recommender")
            text = recommendation_for(sku)
            return json_response(200, {"recommendation": text})
        return json_response(404, error_body("not_found", "没有这个路径", request_id))

    return handler


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8200)
    parser.add_argument("--root", default=os.getcwd())
    parser.add_argument("--process-name", default="recommender")
    args = parser.parse_args()
    import orange.common as common

    common.PROCESS_NAME = args.process_name
    common.ROOT = args.root
    start_fault_reader()
    server = serve(args.port, build_handler())
    install_term_handler(server)
    log_event("process_started", port=args.port)
    server.serve_forever()


if __name__ == "__main__":
    main()
