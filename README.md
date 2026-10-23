# OrangeOrder

OrangeOrder is a single-machine, multi-process order system for learning resilience and elasticity in distributed systems, and for giving another project a stable place to submit orders.

Current status: **design 0.1 is implemented.** The caller owns the catalog, prices, and cart. This system accepts the order, stores it, degrades when recommendation is unavailable, fulfills asynchronously, and fails over between a leader and a follower.

## Documents

| Document | Contents |
| --- | --- |
| [Requirements history](docs/requirements-history.md) | The original elasticity table, the review, and later requirement changes |
| [Overview](docs/overview.md) | Background, goals, boundaries, and terms |
| [Architecture](docs/architecture.md) | Context, process diagram, sequences, and state machines |
| [Technical design](docs/technical-design.md) | Choices, write rules, replication, scaling, parameters, and acceptance tests |
| [API](docs/api.md) | Public semantics and inter-process interfaces |
| [OpenAPI](docs/openapi.yaml) | Machine-readable public contract |
| [Data model](docs/data-model.md) | Tables, the replication log, and states |
| [Integration guide](docs/integration.md) | How another project calls OrangeOrder |
| [ADR 0001 Single leader](docs/adr/0001-single-leader.md) | Asynchronous replication, with an epoch that fences the old leader |
| [ADR 0002 Job table](docs/adr/0002-job-table.md) | The job table is the queue |
| [ADR 0003 Witness](docs/adr/0003-supervisor-witness.md) | Only the supervisor may promote a store |
| [ADR 0004 Stack](docs/adr/0004-stdlib-sqlite.md) | Six implementations, each with its own sqlite |
| [ADR 0005 Integration](docs/adr/0005-sync-poll-integration.md) | Synchronous submit, poll for fulfillment, optional callback |
| [Interview walkthrough](docs/interview.md) | How to use this system to answer “design a distributed system” |
| [中文版 README](README.zh.md) | 本页的中文说明。设计文档的中文版在 `docs/zh` |

The Chinese copies are in [`docs/zh`](docs/zh/overview.md). Integrators start with the [integration guide](docs/integration.md) and [OpenAPI](docs/openapi.yaml). Implementation follows the parameter table in the [technical design](docs/technical-design.md).

## Processes

Callers use only the gateway at `127.0.0.1:8080`. The other ports are internal.

| Process | Port | Role |
| --- | --- | --- |
| Gateway | 8080 | Rate limit, deadline, and dispatch to a ready replica |
| Order A / B | 8101 / 8102 | Stateless order acceptance |
| Recommender | 8200 | A side path that may fail |
| Leader / follower store | 8301 / 8302 at startup | Single-leader writes; the follower replicates asynchronously. After failover the roles swap and the ports stay |
| Supervisor | 8090 | Probe, restart, publish the current leader, and scale workers |
| Workers | 8401 and up | 1 to 3. The port serves only probes and metrics; fulfillment talks to the leader store |

## Six languages

The same design is implemented six times, so the repository can be read as a language comparison. Python is the original. Rust, Java, C++, Go, and TypeScript repeat the same processes, ports, errors, and acceptance tests.

| | Code | Start | Tests |
| --- | --- | --- | --- |
| Python | `python/src/orange/` | `python/run.sh --root .` | `python/run.sh test` |
| Rust | `rust/src/` | `cargo run --manifest-path rust/Cargo.toml -- --root .` | `cargo test --manifest-path rust/Cargo.toml -- --test-threads=1` |
| Java | `java/src/orange/` | `java/build.sh --root .` | `java/build.sh test` |
| C++ | `cpp/src/` | `cpp/build.sh --root .` | `cpp/build.sh test` |
| Go | `go/` | `go/run.sh --root .` | `go/run.sh test` |
| TypeScript | `ts/src/` | `ts/run.sh --root .` | `ts/run.sh test` |

All six stacks listen on the same ports. Run one at a time. Each language directory has an English `README.md` and a Chinese `README.zh.md`.

## File map

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
