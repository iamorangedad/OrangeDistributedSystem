# OrangeOrder in Java

[中文](README.md)

This implementation does the same job as Python, Rust, C++, and Go: a single-machine, multi-process order system. Ports, error codes, log events, and AC-1 through AC-12 follow the [technical design](../docs/en/technical-design.md). Language differences are in [ADR 0004](../docs/en/adr/0004-stdlib-sqlite.md).

Java does not use a web framework. HTTP is the JDK `com.sun.net.httpserver`. JSON is the hand-written `Json` class. Each store process still uses one lock. The JDK has no sqlite, so the only extra dependency is `sqlite-jdbc`. The build script downloads the jar when it is missing. The jar is not committed.

The supervisor starts `orange.Main` on the same classpath and selects the process with a subcommand. Run one language at a time. All six stacks bind 8080, 8090, 8101, and the other fixed ports.

## Run

JDK 21 is required. The script uses `JAVA_HOME`, or `~/.local/jdk` when that variable is unset.

From the repository root:

```bash
java/build.sh --root .
```

Callers still use only `http://127.0.0.1:8080`.

```bash
java/build.sh test
```

The acceptance test starts the supervisor and every child. It takes about a minute or two. The ports are fixed, so two stacks cannot listen together.

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
