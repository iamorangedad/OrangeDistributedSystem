# Overview

Chinese: [系统概述](../overview.md).

| Item | Value |
| --- | --- |
| Status | Design 0.1 is implemented |
| Version | 0.1 |
| Date | 2026-10-10 |

## Background

The learning goal is a small distributed system that still has the important organs: it tolerates faults, it grows and shrinks with backlog, and faults can be injected on this machine so the result is visible.

Another project needs order submission. Orange makes “submit one order” its own system. Callers integrate over HTTP. The catalog and payment stay out of Orange.

## Problems

One order submission meets all of these constraints:

- Retrying the same request must not create two orders.
- When recommendation is slow or gone, the order still succeeds, and the caller is told the result is degraded.
- Fulfillment may finish after acceptance. A rising backlog adds capacity. Past the protection line, new orders are rejected.
- When the leader store process disappears, the system switches to the follower. The old process must not write after it returns.
- One request can be followed from the gateway to the store and the worker with the same `request_id`.

## Goals

1. Provide `POST /v1/orders` and `GET /v1/orders/{order_id}` for another project.
2. On one machine, with several processes, demonstrate rate limiting, replica removal, timeouts, a circuit breaker, degradation, at-least-once delivery, a dead letter, single-leader replication, epoch fencing, and adding or removing workers by backlog.
3. Every class of fault has a written external result.

## Boundary

Orange owns:

- Accepting and persisting an order
- Idempotency
- An optional recommendation field
- Asynchronous fulfillment, and a caller-supplied local fulfillment URL
- Leader/follower replication and failover

The caller owns:

- Catalog, price, stock, and cart
- Whether this business order is allowed
- The business action at the fulfillment URL (stock decrement, user notification, and so on)
- Recording Orange’s `order_id`, or the caller’s own `external_ref`, in the caller’s data

Version one does not include multi-region active-active, multi-leader writes, CQRS, Raft/Paxos, a separate message broker, container orchestration, a service mesh, an edge CDN, a hosted chaos platform, autoscaling by CPU or latency, or caller authentication. Any local process that can reach `127.0.0.1:8080` can submit an order.

## Constraints

- Deploy on one machine. The supervisor starts the processes.
- Five implementations share one contract: ports, errors, logs, and acceptance tests. Each store node has one sqlite file. Language differences are in [ADR 0004](adr/0004-stdlib-sqlite.md).
- The only writer is the store that holds the current epoch. See [ADR 0001](adr/0001-single-leader.md).
- There are exactly two order replicas. Elasticity applies only to workers, to drain fulfillment backlog.
- External integration is synchronous HTTP plus polling. Version one has no webhook. See [ADR 0005](adr/0005-sync-poll-integration.md).

## Terms

| Term | Meaning |
| --- | --- |
| Caller | The other project that integrates with Orange |
| Order | One accepted submission |
| Job | Fulfillment work attached to an order, stored in the leader’s job table |
| epoch | A monotonically increasing write generation. Writes from an older generation are rejected |
| Leader store | The only node that may confirm a write |
| Follower store | A replica that applies the leader log asynchronously and may lag |
| Degraded | The order is saved and the recommendation is missing. `degraded` is `true` |
| Dead letter | A job that exceeded its retries and will not run automatically |
| Witness | The supervisor. Only it may promote a new leader |
| Deadline | The absolute end time the gateway sets for one request and passes downstream |

## How to read

- To integrate: [Integration guide](integration.md) → [OpenAPI](openapi.yaml)
- To see the shape: [Architecture](architecture.md)
- To implement: [Technical design](technical-design.md) → [Data model](data-model.md) → [API](api.md)
- To ask why: [ADRs](adr/)
- To see where the requirements came from: [Requirements history](requirements-history.md)
- To answer an interview with this design: [Interview walkthrough](interview.md)
