# OrangeOrder

OrangeOrder is a single-machine, multi-process order system for learning resilience and elasticity in distributed systems, and for giving another project a stable place to submit orders.

Current status: **design 0.1 is implemented.** The caller owns the catalog, prices, and cart. This system accepts the order, stores it, degrades when recommendation is unavailable, fulfills asynchronously, and fails over between a leader and a follower.

## Documents

| Document | Contents |
| --- | --- |
| [中文版 README](README.zh.md) | 本页的中文说明 |
| [Requirements history](docs/en/requirements-history.md) | The original elasticity table, the review, and later requirement changes |
| [Overview](docs/en/overview.md) | Background, goals, boundaries, and terms |
| [Architecture](docs/en/architecture.md) | Context, process diagram, sequences, and state machines |
| [Technical design](docs/en/technical-design.md) | Choices, write rules, replication, scaling, parameters, and acceptance tests |
| [API](docs/en/api.md) | Public semantics and inter-process interfaces |
| [OpenAPI](docs/en/openapi.yaml) | Machine-readable public contract, in English |
| [Data model](docs/en/data-model.md) | Tables, the replication log, and states |
| [Integration guide](docs/en/integration.md) | How another project calls OrangeOrder |
| [ADR 0001 Single leader](docs/en/adr/0001-single-leader.md) | Asynchronous replication, with an epoch that fences the old leader |
| [ADR 0002 Job table](docs/en/adr/0002-job-table.md) | The job table is the queue |
| [ADR 0003 Witness](docs/en/adr/0003-supervisor-witness.md) | Only the supervisor may promote a store |
| [ADR 0004 Stack](docs/en/adr/0004-stdlib-sqlite.md) | Five implementations, each with its own sqlite |
| [ADR 0005 Integration](docs/en/adr/0005-sync-poll-integration.md) | Synchronous submit, poll for fulfillment, optional callback |
| [Interview walkthrough](docs/en/interview.md) | How to use this system to answer “design a distributed system” |

The Chinese originals stay in `docs/` without the `en` segment. Integrators start with the [integration guide](docs/en/integration.md) and [OpenAPI](docs/en/openapi.yaml). Implementation follows the parameter table in the [technical design](docs/en/technical-design.md).

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

## Five languages

The same design is implemented five times, so the repository can be read as a language comparison. Python is the original. Rust, Java, C++, and Go repeat the same processes, ports, errors, and acceptance tests.

| | Code | Start | Tests |
| --- | --- | --- | --- |
| Python | `python/src/orange/` | `python/run.sh --root .` | `python/run.sh test` |
| Rust | `rust/src/` | `cargo run --manifest-path rust/Cargo.toml -- --root .` | `cargo test --manifest-path rust/Cargo.toml -- --test-threads=1` |
| Java | `java/src/orange/` | `java/build.sh --root .` | `java/build.sh test` |
| C++ | `cpp/src/` | `cpp/build.sh --root .` | `cpp/build.sh test` |
| Go | `go/` | `go/run.sh --root .` | `go/run.sh test` |

All five stacks listen on the same ports. Run one at a time. The file-by-file maps are in [python/README.en.md](python/README.en.md), [rust/README.en.md](rust/README.en.md), [java/README.en.md](java/README.en.md), [cpp/README.en.md](cpp/README.en.md), and [go/README.en.md](go/README.en.md). Chinese copies sit beside them as `README.md`.
