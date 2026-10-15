# Orange Order System

Orange is a single-machine, multi-process order system for learning resilience and elasticity in distributed systems, and for giving another project a stable place to submit orders.

Current status: **design 0.1 is implemented.** The caller owns the catalog, prices, and cart. This system accepts the order, stores it, degrades when recommendation is unavailable, fulfills asynchronously, and fails over between a leader and a follower.

## Documents

| Document | Contents |
| --- | --- |
| [中文版 README](README.zh.md) | 本页的中文说明 |
| [Requirements history](docs/requirements-history.md) | The original elasticity table, the review, and later requirement changes |
| [Overview](docs/overview.md) | Background, goals, boundaries, and terms |
| [Architecture](docs/architecture.md) | Context, process diagram, sequences, and state machines |
| [Technical design](docs/technical-design.md) | Choices, write rules, replication, scaling, parameters, and acceptance tests |
| [API](docs/api.md) | Public semantics and inter-process interfaces |
| [OpenAPI](docs/openapi.yaml) | Machine-readable public contract |
| [Data model](docs/data-model.md) | Tables, the replication log, and states |
| [Integration guide](docs/integration.md) | How another project calls Orange |
| [ADR 0001 Single leader](docs/adr/0001-single-leader.md) | Asynchronous replication, with an epoch that fences the old leader |
| [ADR 0002 Job table](docs/adr/0002-job-table.md) | The job table is the queue |
| [ADR 0003 Witness](docs/adr/0003-supervisor-witness.md) | Only the supervisor may promote a store |
| [ADR 0004 Stack](docs/adr/0004-stdlib-sqlite.md) | The Python standard library and sqlite |
| [ADR 0005 Integration](docs/adr/0005-sync-poll-integration.md) | Synchronous submit, poll for fulfillment, optional callback |
| [Interview walkthrough](docs/interview.md) | How to use this system to answer “design a distributed system” |

Integrators start with the [integration guide](docs/integration.md) and [OpenAPI](docs/openapi.yaml). Implementation follows the parameter table in the [technical design](docs/technical-design.md).

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
