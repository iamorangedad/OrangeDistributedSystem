# Requirements history

Chinese: [需求与变更记录](../requirements-history.md).

| Item | Value |
| --- | --- |
| Status | History. The current specification is the design documents, not the early plans in this file |
| Version | 0.1 |
| Date | 2026-10-11 |
| Source | The 2026-10-10 checklist “how to design an elastic distributed system,” and the Elastic System Review canvas from the same day |

The Elastic System Review first lived only in a canvas beside the chat. It was not in the repository. This file turns it into requirements history: the original table, what the review did to that table, and what the design documents changed after the review.

Current behavior, parameters, and interfaces follow the [overview](overview.md), the [technical design](technical-design.md), and the [integration guide](integration.md).

## Original requirements

On 2026-10-10 the project had no code. The input was an elasticity checklist. The goal was self-study, kept small, while retaining the main mechanisms.

| Id | Group | Original requirement |
| --- | --- | --- |
| R0-1 | Principle | Elasticity has two meanings. Resilience: keep serving when some nodes fail, the network partitions, or a downstream crashes, and allow graceful degradation |
| R0-2 | Principle | Elasticity: scale out and in with traffic spikes or swings |
| R0-3 | Ingress | Smart load balancing and a global rate limit. Edge CDN and a smart gateway (examples: Envoy, Azure Front Door). Token bucket or leaky bucket, so traffic does not crush the backend |
| R0-4 | Compute | Split the business into stateless applications. Use Kubernetes HPA, scaling in milliseconds to seconds by CPU, memory, or custom request latency |
| R0-5 | Communication | Use a message queue (examples: Kafka, Event Hubs) to absorb bursts. RPC must set a timeout and exponential backoff with jitter, to avoid a retry storm |
| R0-6 | Isolation | Breaker: when a downstream fails continuously, open the breaker and return a preset or fallback response |
| R0-7 | Isolation | Bulkhead: isolate thread pools or resource pools of critical and non-critical work |
| R0-8 | Isolation | Degradation: protect the core path. Example: allow payment, temporarily turn off non-core recommendation |
| R0-9 | Storage | Deploy across availability zones and regions. Read/write split and multiple replicas. Choose, per business, between Raft/Paxos strong consistency and CQRS plus eventual consistency. Tolerate partitions |
| R0-10 | Observation | Unified metrics, logs, and distributed tracing (example: OpenTelemetry). Automatic alerts for SLI/SLO |
| R0-11 | Chaos | Inject faults in staging or production (example: Chaos Studio: cut the network, kill pods, high database latency) and verify elasticity and self-healing before an incident |
| R0-12 | Self-healing | Liveness and readiness probes, dead-letter retry, automatic failover |

## Change CHG-1: review canvas

Date: 2026-10-10. Record name: Elastic System Review.

The judgment is only for a self-study first version. It does not say whether those products are useful in production. Of 10 rows, 3 were adopted, 6 narrowed, and 1 rewritten.

Conclusion: splitting elasticity into resilience and elasticity holds. The storage row listed single-leader replication, multi-leader, Raft/Paxos, and CQRS as a menu that could all be turned on. Version one had to become one write rule. The other mechanisms hold. In the implementation, processes, tables, and a fault switch replace CDN, Kafka, Kubernetes, and a chaos platform. The business is a multi-process order system on one machine, named Orange. Payment, stock, and accounts are out. Recommendation is a side path that may fail. Fulfillment is a later side effect.

| Id | Original | Judgment | After the review |
| --- | --- | --- | --- |
| R1-1 | R0-1 | Adopt | When a node crashes, a call slows, or a replica disconnects, the core write path still has a definite result |
| R1-2 | R0-2 | Adopt | Replica count follows job backlog, with a cooldown |
| R1-3 | R0-3 | Narrow | Keep one entrance and a token bucket. With one gateway, the local bucket is the global limit |
| R1-4 | R0-4 | Narrow | Order processes do not store business state. The scale signal is seconds of backlog, not an instantaneous latency |
| R1-5 | R0-5 | Narrow | A deadline is passed along the call. Retries are only for retryable side effects, and only after an idempotency key exists |
| R1-6 | R0-6, R0-7, R0-8 | Narrow | The breaker wraps only recommendation. A failed write returns an error. A preset success is not a fallback |
| R1-7 | R0-9 | Rewrite | Single leader, multi-leader, consensus, and a split read model are four roads. Version one keeps only a single leader and an epoch |
| R1-8 | R0-10 | Narrow | Tie one request together with request_id, and expose backlog, errors, replication lag, and breaker state |
| R1-9 | R0-11 | Narrow | The fault switch lives in a local experiment, to check the agreed behavior |
| R1-10 | R0-12 | Adopt | Separate liveness and readiness. Dead letter, process restart, and fenced failover all stay |

The review added four items the original table did not have, without which the design is incomplete:

| Id | Added requirement |
| --- | --- |
| R1-11 | An idempotency key. Otherwise retry and failover write the same order twice |
| R1-12 | A deadline passed along the call. If each call sets its own timeout, the total wait can still stall the entrance |
| R1-13 | A bounded backlog. The queue only absorbs bursts. Past the cap, new requests are rejected |
| R1-14 | Write fencing. After the old leader returns, the new epoch must be able to reject it |

The review explicitly did not copy: millisecond scaling from instantaneous latency; returning a preset success whenever a downstream looks wrong; multi-region active-active and multi-leader; injecting faults in staging or production.

Part of the plan formed in the review was later changed by CHG-2. The canvas text is kept below for comparison.

Data path: client → gateway → order replica → leader store → follower store. Recommendation leaves from the order process and may fail. Workers claim jobs from the leader store. The supervisor probes, restarts, publishes the leader, and adds or removes workers by backlog. It is not on the write-confirmation path.

Write rules: only the current leader published by the supervisor accepts writes. The order row and the job row commit together and fsync. The follower may lag. Read-your-writes goes to the leader. On failover the follower writes the new epoch before it accepts writes. If the supervisor stops, the current leader keeps serving and does not elect another leader by itself.

Side-effect rules: the job table is the queue. After a claim there is a visibility timeout, and a later claim must be idempotent on the job id. Repeated failure becomes a dead letter. When the backlog passes the cap, the gateway returns 429 for new orders.

| Process | Count written in the review | Duty | After it stops |
| --- | --- | --- | --- |
| Gateway | 1 | Token bucket, deadline, skip a replica that is not ready | Entrance stops; the supervisor starts it |
| Order | 2, at most 3 | Check the idempotency key, write the leader, wait briefly for recommendation | The gateway removes that replica |
| Recommender | 1 | A recommendation that may be absent | The order still succeeds and is marked degraded |
| Worker | 1–3 | Claim fulfillment jobs; back off on failure | After timeout the job returns to pending |
| Leader store | 1 | The only writer | The supervisor promotes the follower and changes the epoch |
| Follower store | 1 | Receive confirmed log entries | The lagging replica is gone; writes stay on the leader |
| Supervisor | 1 | Probe, restart, publish the leader, scale workers by backlog | Election and scaling stop; the current leader keeps writing |

Behaviors the review required version one to demonstrate:

| Id | Injected fault | Agreed result |
| --- | --- | --- |
| R1-15 | Kill one order process | The other replica keeps accepting; the supervisor starts it again |
| R1-16 | Recommendation is slow or exits | Wait at most 80ms; recommendation is empty; degraded is true |
| R1-17 | Leader store exits | The follower accepts writes at a new epoch; after the old leader returns, its writes are rejected |
| R1-18 | A worker exits mid-job | After the visibility timeout the job can be claimed again, and it takes effect once |
| R1-19 | Submit the same idempotency key again | Return the first result; do not create a second order |
| R1-20 | The request passes the deadline | The order process stops calling downstream and returns a timeout to the gateway |
| R1-21 | Pending jobs keep piling up | Add workers first, up to 3; if still over the cap, new orders get 429 |
| R1-22 | A poison job fails repeatedly | After the attempt limit it becomes a dead letter |

Correspondences and parameters from the review:

| Original wording | What the review mapped it to |
| --- | --- |
| Edge gateway and a global rate limit | One gateway process, token bucket capacity 30, refill 10 per second |
| Stateless and horizontal scale | Order processes are replaceable; worker count follows backlog, cooldown 15 seconds |
| Message queue absorbs bursts | Job table: a cap, visibility timeout 15 seconds, dead letter |
| Timeout and exponential backoff | Entrance deadline 800ms; worker retry starts at 100ms, caps at 5 seconds, full jitter |
| Breaker, bulkhead, degradation | Recommendation opens for 5 seconds after 5 consecutive failures; the gateway allows at most 8 in-flight orders |
| Replication and partition tradeoff | Confirm after the leader fsync; the follower may lag; during a partition, prefer not to elect |
| Metrics, logs, tracing | JSON logs carry request_id; four counters: backlog, errors, lag, breaker |
| Chaos and self-healing | `runtime/faults.json` injects delay, exit, and paused replication; liveness and readiness are separate |

The implementation order was seven stages: single-process idempotency, gateway and two replicas, recommendation and breaker, job table and dead letter, leader/follower and epoch, add and remove workers by backlog, fault file and the four counters. Version one does not include multi-region active-active, multi-leader, CQRS, Raft/Paxos, Kafka, Kubernetes, Envoy, CDN, an OpenTelemetry Collector, a hosted chaos platform, or autoscaling by CPU or latency. Raft waits until single-leader failover can be demonstrated, and then it is a separate exercise.

The agreement that day was: the supervisor starts the processes, and each store node has one sqlite file. The only language written then was Python. The same contract later gained Rust, Java, C++, and Go. See CHG-3 at the end.

## Change CHG-2: design documents relative to the review canvas

Date: later on 2026-10-10. Two triggers: another project needed OrangeOrder as an order entrance; turning the canvas into an implementable design required a few counts and semantics to be fixed.

This change does not overturn the judgments in CHG-1. The rows below are only the requirements that moved relative to the canvas.

| Id | Relative to the canvas | Requirement after the change | Where it landed |
| --- | --- | --- | --- |
| R2-1 | The canvas only said the client places an order, with no external boundary | The caller owns catalog, price, stock, and cart. OrangeOrder does not interpret sku and does not store an amount | [Overview](overview.md), [Integration guide](integration.md) |
| R2-2 | The canvas had no caller identity | Every request carries `X-Caller-Id`. An idempotency key is scoped to one caller | [API](api.md) |
| R2-3 | The canvas had no caller reference or fulfillment URL | Add optional `external_ref`. `fulfill_url` is optional and only `http://127.0.0.1` is allowed. If omitted, the worker marks the job done locally | [ADR 0005](adr/0005-sync-poll-integration.md) |
| R2-4 | The canvas had no query method | Fulfillment progress is polled with GET. Version one has no webhook | [Integration guide](integration.md) |
| R2-5 | R1-2 and the process table said orders are “2, at most 3,” and the scale target was vague | Order replicas are fixed at 2, for resilience only. Only workers are added and removed automatically, from 1 to 3 | [Technical design](technical-design.md) |
| R2-6 | R1-19 said “return the first result” | The same key does not create a second order. Replay returns the current snapshot. Fulfillment status may have advanced. Item, quantity, and `external_ref` do not change | [Data model](data-model.md) |
| R2-7 | R1-13 and R1-21 only said the backlog passed a cap | Add one worker only when pending jobs stay above 10 for 5 seconds. At 50 pending, new orders return 429. Extra workers are removed only after pending stays at 0 for 20 seconds. Cooldown is 15 seconds | [Technical design](technical-design.md) |
| R2-8 | Observation listed four counters | Five metrics are required: errors, in-flight, pending jobs, replication lag, and whether the breaker is open | [Technical design](technical-design.md) |
| R2-9 | Workers had no port on the canvas | Workers from 8401 up serve only probes and metrics. For the business they only call the leader outbound | [Technical design](technical-design.md) |
| R2-10 | R1-17 stopped at “the old epoch is rejected” | Add the loss window before replication: an order that returned 201 but was not replicated may be absent on the new leader. The caller POSTs again with the same key. During a switch the gateway pauses for at most 2 seconds | [ADR 0001](adr/0001-single-leader.md) |
| R2-11 | The canvas said recommendation waits at most 80ms, without ordering it against the store call | The 80ms for recommendation starts together with the store call. After the store returns, only the remaining time is waited | [Technical design](technical-design.md) |
| R2-12 | The canvas did not write the caller’s own timeout | The caller’s HTTP timeout should be 1.5 seconds, a little longer than the internal 800ms, and the same idempotency key retries 429, 503, 504, and connection failure | [Integration guide](integration.md) |

## Requirements that stay in force after the changes

These remain from the original table or from CHG-1. CHG-2 did not overturn them:

- Design resilience and elasticity separately.
- One gateway, a local token bucket, stateless order processes.
- Idempotency key, deadline, bounded backlog, epoch write fencing.
- Breaker and degradation apply only to recommendation. A failed order returns an error.
- The job table provides burst absorption, visibility timeout, backoff, and dead letter.
- Confirm after the single leader fsyncs. The follower may lag. The supervisor is the only witness.
- Liveness and readiness are separate. Fault injection is only the local `runtime/faults.json`.
- Version one still does not include CDN, Envoy, Kafka, Kubernetes, multi-leader, CQRS, Raft, multi-region active-active, or a hosted chaos platform.

## Change CHG-3: five languages implement one contract

Date: 2026-10-21. Trigger: the same order design should be readable across languages, not only in the original Python.

This change does not alter ports, errors, logs, parameters, or AC-1 through AC-12. The external contract still carries no type from a particular language.

| Id | Relative to CHG-2 | Requirement after the change | Where it landed |
| --- | --- | --- | --- |
| R3-1 | The implementation agreement mentioned only the Python standard library | Five implementations side by side: Python, Rust, Java, C++, and Go. Run one at a time, because the ports are the same | [ADR 0004](adr/0004-stdlib-sqlite.md), each language README |
| R3-2 | The technical design named the runtime as the Python standard library | Language differences stay in ADR 0004 and the language READMEs. The technical design keeps only language-neutral parameters and behavior | [Technical design](technical-design.md) |

## Change CHG-4: the project is renamed OrangeOrder

Date: 2026-10-22. Trigger: the repository name OrangeDistributedSystem described a one-machine order system as a distributed platform.

This change renames the project as presented to readers. Ports, errors, logs, parameters, acceptance tests, and the module name in all five languages stay the same. The Python package, Java package, Rust package, Go module, and executable remain `orange`.

| Id | Relative to CHG-3 | Requirement after the change | Where it landed |
| --- | --- | --- | --- |
| R4-1 | The public name was Orange, and the repository was OrangeDistributedSystem | The project name is OrangeOrder. Documents, the API title, and diagrams use that name | Homepage, design docs, OpenAPI |
