# OrangeOrder in C++

[中文](README.md)

This implementation does the same job as Python, Rust, Java, and Go: a single-machine, multi-process order system. Ports, error codes, log events, and AC-1 through AC-12 follow the [technical design](../docs/en/technical-design.md). Language differences are in [ADR 0004](../docs/en/adr/0004-stdlib-sqlite.md).

C++ does not use a web framework. HTTP is POSIX sockets, one thread per connection, with hand-written JSON. The store is still one lock plus sqlite. The machine has `libsqlite3.so.0` but no `sqlite3.h` and no unversioned `libsqlite3.so` for the linker. The build script downloads the 3.37.2 header into `cpp/third_party/` and links `libsqlite3.so.0` directly. The header and build output are not committed.

Run one language at a time. All five stacks bind 8080, 8090, 8101, and the other fixed ports.

## Run

g++ and `libsqlite3` are required. From the repository root:

```bash
cpp/build.sh --root .
```

Acceptance:

```bash
cpp/build.sh test
```

`cpp/build.sh` compiles `cpp/build/orange`. With no subcommand the program is the supervisor. It re-executes itself as `store`, `gateway`, `order`, `recommender`, or `worker`.

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
