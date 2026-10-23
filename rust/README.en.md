# OrangeOrder in Rust

[中文](README.md)

This implementation does the same job as Python, Java, C++, and Go: a single-machine, multi-process order system. Ports, error codes, log events, and AC-1 through AC-12 follow the [technical design](../docs/en/technical-design.md). Language differences are in [ADR 0004](../docs/en/adr/0004-stdlib-sqlite.md).

Rust does not use a web framework or an async runtime. Each process is threads and a lock. HTTP is parsed on a blocking `TcpListener`. sqlite comes from `rusqlite`, bundled into the binary. The code you read is order acceptance, replication, and the breaker, not a framework lifecycle.

The supervisor re-executes the same binary with a subcommand. Run one language at a time. All six stacks bind 8080, 8090, 8101, and the other fixed ports.

## Run

From the repository root:

```bash
cargo run --manifest-path rust/Cargo.toml -- --root .
```

Callers still use only `http://127.0.0.1:8080`.

```bash
cargo test --manifest-path rust/Cargo.toml -- --test-threads=1
```

The acceptance test starts the supervisor and every child. It takes about a minute or two. `--test-threads=1` is required because the ports are fixed.

## File map across six implementations

| Role | Python | Rust | Java | C++ | Go | TypeScript |
| --- | --- | --- | --- | --- | --- | --- |
| Entry | `python/src/orange/__main__.py` | `rust/src/main.rs` | `java/src/orange/Main.java` | `cpp/src/main.cpp` | `go/main.go` | `ts/src/main.ts` |
| Shared library | `python/src/orange/common.py` | `rust/src/common.rs`, `http_util.rs` | `Common.java`, `HttpUtil.java`, `Json.java` | `cpp/src/common.cpp`, `http.cpp`, `json.cpp` | `go/common.go`, `http.go` | `ts/src/common.ts`, `http.ts` |
| Gateway | `python/src/orange/gateway.py` | `rust/src/gateway.rs` | `java/src/orange/Gateway.java` | `cpp/src/gateway.cpp` | `go/gateway.go` | `ts/src/gateway.ts` |
| Order | `python/src/orange/order_service.py` | `rust/src/order.rs` | `java/src/orange/OrderService.java` | `cpp/src/order.cpp` | `go/order.go` | `ts/src/order.ts` |
| Recommender | `python/src/orange/recommender.py` | `rust/src/recommender.rs` | `java/src/orange/Recommender.java` | `cpp/src/recommender.cpp` | `go/recommender.go` | `ts/src/recommender.ts` |
| Store | `python/src/orange/store.py` | `rust/src/store.rs` | `java/src/orange/Store.java` | `cpp/src/store.cpp` | `go/store.go` | `ts/src/store.ts` |
| Worker | `python/src/orange/worker.py` | `rust/src/worker.rs` | `java/src/orange/Worker.java` | `cpp/src/worker.cpp` | `go/worker.go` | `ts/src/worker.ts` |
| Supervisor | `python/src/orange/supervisor.py` | `rust/src/supervisor.rs` | `java/src/orange/Supervisor.java` | `cpp/src/supervisor.cpp` | `go/supervisor.go` | `ts/src/supervisor.ts` |
| Acceptance | `python/tests/` | `rust/tests/acceptance.rs` | `java/test/orange/Acceptance.java` | `cpp/tests/acceptance.cpp` | `go/acceptance/` | `ts/src/acceptance.test.ts` |

Java classes live in `java/src/orange/`. Behavior follows the parameter table in the technical design.
