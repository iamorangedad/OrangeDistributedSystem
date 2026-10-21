# Orange in Go

[中文](README.md)

This implementation does the same job as Python, Rust, Java, and C++: a single-machine, multi-process order system. Ports, error codes, log events, and AC-1 through AC-12 follow the [technical design](../docs/en/technical-design.md). Language differences are in [ADR 0004](../docs/en/adr/0004-stdlib-sqlite.md).

Go does not use a web framework. HTTP is the standard `net/http`. Each process is goroutines plus a lock. sqlite is the pure-Go module `modernc.org/sqlite`, so cgo is not required. Module checksums are in `go.sum`.

Run one language at a time. All five stacks bind 8080, 8090, 8101, and the other fixed ports.

## Run

Go 1.22 is required. From the repository root:

```bash
go/run.sh --root .
```

Acceptance:

```bash
go/run.sh test
```

`go/run.sh` builds `go/bin/orange`. With no subcommand the program is the supervisor. It re-executes itself as `store`, `gateway`, `order`, `recommender`, or `worker`.

## File map across five implementations

| Role | Python | Rust | Java | C++ | Go |
| --- | --- | --- | --- | --- | --- |
| Entry | `python/src/orange/__main__.py` | `rust/src/main.rs` | `java/src/orange/Main.java` | `cpp/src/main.cpp` | `go/main.go` |
| Shared library | `python/src/orange/common.py` | `rust/src/common.rs`, `http_util.rs` | `Common.java`, `HttpUtil.java`, `Json.java` | `cpp/src/common.cpp`, `http.cpp`, `json.cpp` | `go/common.go`, `http.go` |
| Gateway | `python/src/orange/gateway.py` | `rust/src/gateway.rs` | `java/src/orange/Gateway.java` | `cpp/src/gateway.cpp` | `go/gateway.go` |
| Order | `python/src/orange/order_service.py` | `rust/src/order.rs` | `java/src/orange/OrderService.java` | `cpp/src/order.cpp` | `go/order.go` |
| Recommender | `python/src/orange/recommender.py` | `rust/src/recommender.rs` | `java/src/orange/Recommender.java` | `cpp/src/recommender.cpp` | `go/recommender.go` |
| Store | `python/src/orange/store.py` | `rust/src/store.rs` | `java/src/orange/Store.java` | `cpp/src/store.cpp` | `go/store.go` |
| Worker | `python/src/orange/worker.py` | `rust/src/worker.rs` | `java/src/orange/Worker.java` | `cpp/src/worker.cpp` | `go/worker.go` |
| Supervisor | `python/src/orange/supervisor.py` | `rust/src/supervisor.rs` | `java/src/orange/Supervisor.java` | `cpp/src/supervisor.cpp` | `go/supervisor.go` |
| Acceptance | `python/tests/` | `rust/tests/acceptance.rs` | `java/test/orange/Acceptance.java` | `cpp/tests/acceptance.cpp` | `go/acceptance/` |

Java classes live in `java/src/orange/`. Behavior follows the parameter table in the technical design.
