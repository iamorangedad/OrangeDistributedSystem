# Technical design

Chinese: [技术方案](../technical-design.md).

| Item | Value |
| --- | --- |
| Status | Design 0.1 is implemented |
| Version | 0.1 |
| Date | 2026-10-10 |

This document is the source for implementation. Structure and state machines are in the [architecture](architecture.md). Tables are in the [data model](data-model.md). External semantics are in the [API](api.md).

## Choices

| Choice | Decision |
| --- | --- |
| Language and runtime | Five implementations follow this table. Language differences are in [ADR 0004](adr/0004-stdlib-sqlite.md) and do not enter the external contract |
| Persistence | One sqlite file per store node, `PRAGMA synchronous=FULL` |
| Process model | The supervisor starts independent processes. They listen only on `127.0.0.1` |
| Queue | The job table on the leader store. No message broker is deployed |
| Discovery | The supervisor publishes the leader address. Order replica addresses are fixed |
| Observation | One JSON log line per event, plus the counters listed below |
| Fault injection | `runtime/faults.json`, read periodically by each process |

Reasons are in [ADR 0004](adr/0004-stdlib-sqlite.md) and [ADR 0002](adr/0002-job-table.md).

## Processes and ports

| Process | Listen | Count |
| --- | --- | --- |
| supervisor | 127.0.0.1:8090 | 1 |
| gateway | 127.0.0.1:8080 | 1 |
| order | 127.0.0.1:8101 and :8102 | Fixed at 2 |
| recommender | 127.0.0.1:8200 | 1 |
| store | 127.0.0.1:8301 starts as leader, 8302 as follower; roles swap after promotion | 2 |
| worker | 127.0.0.1:8401 and up, probes and metrics only; fulfillment calls the leader outbound | 1 to 3 |

Order replicas stay at two so one can exit and orders continue. That is resilience, not elasticity. Elasticity changes only the worker count: add consumers when fulfillment is slow, and remove them when idle.

The supervisor keeps a desired process table. A process that exits is started again. Repeated start failures keep the last error on the status page, so the loop does not spin with no gap.

## Write path

1. The gateway checks the token bucket. No token means 429 and `Retry-After`.
2. The gateway checks the pending job count published from the leader. At 50 it returns 429. The supervisor reads that number from the leader and caches it. The cache may be up to 1 second old.
3. In-flight orders at the gateway stay at or below 8. Above that, 429.
4. The gateway round-robins ready order replicas. Readiness is defined below. If none are ready, 503.
5. The gateway writes `X-Deadline-Ms` (absolute Unix milliseconds) and `X-Request-Id`. If the caller omitted a request id, the gateway generates one.
6. If the order process is already past the deadline, it returns 504 and does not call the store.
7. The order process looks up `caller_id + idempotency_key` on the leader. A hit replays or returns 409, as in the [data model](data-model.md).
8. A first request does two things in parallel: append the order and job to the leader, and call recommendation for at most 80ms.
9. The leader checks the epoch. A mismatch is 409 `epoch_fenced`. The order process asks the supervisor for the latest leader address and tries once more. If that still fails, the caller gets 503.
10. The leader puts the order, the job, and the replication log in one transaction, commits with `synchronous=FULL`, and only then responds.
11. The 80ms recommendation budget starts when the store call is issued in parallel. After the store returns, only the time left in that window is spent waiting for recommendation. Then the final snapshot is written and 201 is returned. If the window is already gone, the degraded snapshot from the first transaction stays.

The store-call timeout is the smaller of the remaining deadline and 400ms.

## Recommendation side path

The recommender returns a fixed-format string for a `sku`. The content may be a static demonstration map. It does not touch the store.

Each order process has its own breaker:

- A 10-second window. 5 failures open it
- After 5 seconds open, one probe is allowed
- A failed probe opens it for another 5 seconds
- Timeouts, connection failures, and non-2xx responses all count as failures

While the breaker is open, the network call is skipped and this order has `degraded=true`. The two order processes do not share a breaker. One side can be open while the other still calls recommendation.

## Jobs and workers

The job table is the queue. A worker loops:

1. Claim at most one `pending` job whose `visible_after` has arrived, from the current leader.
2. If there is no job, wait 200ms and try again.
3. If the order has `fulfill_url`, POST the fulfillment event with a 1-second timeout. 2xx or 409 is success.
4. If there is no `fulfill_url`, treat the job as a local success.
5. On success, ack. On failure, fail. The store puts a retryable failure back to `pending` and sets the next `visible_after`.
6. When `attempts` reaches 5, the job becomes `dead` and the order `state` becomes `fulfillment_failed`.

Backoff: after the nth failure, wait a full-jitter delay of `min(5s, 100ms * 2^(n-1))`, a uniform value from 0 through that cap, inclusive. The first failure waits too.

Visibility timeout is 15 seconds. If the worker crashes before ack, another worker can claim the job after `visible_after`. The fulfillment endpoint must therefore be idempotent on `order_id`.

Claim, reset, completion, and dead letter each append a replication log entry, so a new leader can continue from those states.

## Replication and failover

After commit, the leader pushes the log to the follower asynchronously. That push does not block the confirmation already returned to the order process. The follower accepts only a contiguous `log_index`. On push failure, the leader retries every 200ms and exposes `orange_replicate_lag` (leader log sequence minus the follower’s applied sequence).

Heartbeats: leader and follower report `node_id`, `role`, `epoch`, and `applied_index` to the supervisor every 200ms.

Promotion requires all of:

- The current leader’s heartbeat has been missing for more than 1 second
- The supervisor itself is running
- The follower’s heartbeat is still arriving

Promotion steps:

1. The supervisor tells the gateway not to forward new orders to order processes (gateway readiness becomes false), so a write does not hit the old leader during the switch. This lasts until the new leader is published, at most 2 seconds. On timeout the gateway resumes and the status page records a failed switch.
2. The supervisor calls `POST /internal/promote` on the follower. The body is `new_epoch = known epoch + 1`.
3. The follower writes `role=leader` and the new epoch into `meta`, appends `epoch_promoted`, fsyncs, and only then replies.
4. The supervisor publishes the new leader address and epoch. The next write from an order process uses the new address.
5. If the old process returns and sees a higher published epoch, it marks itself the follower, pulls the log from the new leader, and stops accepting writes.

Log entries still only on the old leader’s disk are not merged into the new leader. An order that already returned 201 but was not replicated does not exist on the new leader. The caller POSTs again with the same idempotency key, as the integration guide says. This window is accepted on purpose. In exchange, the leader can still take orders while the follower is down.

The listen ports of the two store nodes do not change. What changes is the “current leader address” the supervisor publishes. Do not hard-code 8301 as the leader forever.

## Scaling and backpressure

| Condition | Action |
| --- | --- |
| `pending` jobs above 10 for 5 seconds, and more than 15 seconds since the last scale action | Add one worker, up to 3 |
| `pending` is 0 for 20 seconds, and there is more than one worker | Stop one worker |
| `pending` reaches 50 | The gateway returns 429 for new orders; accepted orders keep being fulfilled |

Cooldown is counted separately in each direction, so a worker just added is not immediately removed. The supervisor does not add or remove processes because of CPU or a single latency sample.

## Probes

| Path | Meaning | On failure |
| --- | --- | --- |
| `GET /health` | The process can handle HTTP | The supervisor restarts that process |
| `GET /ready` | It can do its job | Remove it from the call path. Do not restart it for this |

Readiness:

- Gateway: at least one of the two order processes is ready, and the gateway itself is not paused for a switch.
- Order: it received a leader address from the supervisor within the last 1 second, and that address’s `/ready` is true.
- Leader store: this process’s role is leader, and the epoch equals the epoch it has fsynced.
- Follower store: the replication loop is running. The follower does not accept order writes.
- Recommender: the process can answer. Not ready only affects degradation. It does not affect gateway readiness.
- Worker: it knows the current leader address. Workers are not load-balanced and do not need to be removed by the gateway.

“Cannot reach the leader store” is not ready. It is not a liveness failure. The order process is removed from rotation and is not restarted in a loop.

## Observation

Each log line is one JSON object:

```json
{"ts":"2026-10-10T00:00:00Z","level":"info","process":"order-1","event":"order_accepted","request_id":"req_...","order_id":"ord_...","epoch":1,"degraded":false}
```

Required fields: `ts`, `level`, `process`, `event`, and `request_id` (empty string when there is no request).

Implementations emit at least these events: `rate_limited`, `order_accepted`, `order_replayed`, `recommend_failed`, `breaker_open`, `deadline_exceeded`, `write_fenced`, `job_claimed`, `job_done`, `job_retry`, `job_dead`, `promoted`, `replicate_lag`.

Each process serves `GET /metrics`. The body is plain text, one `name value` per line. The names are fixed:

| Metric | Who reports it | Meaning |
| --- | --- | --- |
| orange_errors_total | Each process | Operations that process counts as failures |
| orange_inflight | Gateway, order | Orders being handled now |
| orange_jobs_pending | Leader store | Jobs waiting to be claimed |
| orange_replicate_lag | Leader store | Log entries not yet applied on the follower |
| orange_breaker_open | Order | 1 if the breaker is open, otherwise 0 |

The supervisor’s `GET /v1/status` summarizes the current leader address, epoch, both stores’ applied_index, worker count, each process’s readiness, and the last promotion error.

## Fault injection

File `runtime/faults.json`. Each process reads it about every 200ms. A missing file or illegal JSON means no faults, plus one error log.

```json
{
  "delay_ms": {"store": 0, "recommender": 0},
  "pause_replication": false,
  "drop_heartbeat": {"store-a": false},
  "exit_after_requests": {"order-1": 0}
}
```

| Field | Effect |
| --- | --- |
| delay_ms.store | The leader sleeps before handling an append |
| delay_ms.recommender | Recommendation sleeps before answering |
| pause_replication | The leader stops pushing the log |
| drop_heartbeat.store-a | That store node stops heartbeats, to demonstrate failover |
| exit_after_requests | That process exits after this many requests; 0 means never |

Demonstrate failover with `drop_heartbeat`. Do not delete the sqlite file first.

## Parameters

Keep these numbers in one place. Change the table before changing the behavior.

| Parameter | Value |
| --- | --- |
| Token bucket capacity | 30 |
| Token refill | 10 per second |
| Gateway in-flight limit | 8 |
| Pending jobs that reject new orders | 50 |
| Order deadline | 800ms |
| Store call cap | The smaller of the remaining time and 400ms |
| Recommendation wait | 80ms |
| Breaker | 5 failures within 10 seconds, open for 5 seconds |
| Job visibility timeout | 15 seconds |
| Fulfillment HTTP timeout | 1 second |
| Maximum job attempts | 5 |
| Backoff | Starts at 100ms, doubles, caps at 5 seconds, full jitter |
| Heartbeat interval | 200ms |
| Leader considered lost | 1 second |
| Add one worker | pending above 10 for 5 seconds |
| Stop one worker | pending is 0 for 20 seconds |
| Scale cooldown | 15 seconds |
| Worker count | At least 1, at most 3 |
| Order replicas | Fixed at 2 |

## Code layout

The five implementations sit side by side. Mechanisms stay in each language’s process code, not in a web framework or a container orchestrator. File maps are in each language’s README.

```text
python/   rust/   java/   cpp/   go/
runtime/faults.json
runtime/data/
```

## Build order

At the end of each stage the system can run on its own and demonstrate that row.

| Stage | Delivered | Demonstrates |
| --- | --- | --- |
| 1 | Single-process accept and sqlite idempotency | The same key does not create two orders; a conflicting body returns 409 |
| 2 | Gateway, two order replicas, probes, token bucket | Stopping one replica still accepts orders; a full bucket returns 429 |
| 3 | Recommendation, 80ms cap, breaker | Recommendation exit returns 201 with degraded true |
| 4 | Job table, worker, backoff, dead letter, fulfill_url | A worker that exits mid-job is claimed again; repeated failure becomes a dead letter |
| 5 | Follower, epoch, supervisor switch | After heartbeats stop, a new epoch accepts writes; the old epoch is rejected |
| 6 | Add and remove workers by backlog | After the backlog rises, workers become 3; after it drains, they return to 1 |
| 7 | Metrics, JSON logs, fault file | One injected delay can be tied together with request_id |

## Acceptance

| Id | Action | Pass |
| --- | --- | --- |
| AC-1 | POST twice with the same key and body | One order_id, both responses 201 |
| AC-2 | Same key, different sku | The second response is 409; the stored order is still the first |
| AC-3 | Stop order B | New orders are still 201; the status page shows B not ready, then started again |
| AC-4 | Stop recommendation | 201, degraded true, recommendation null |
| AC-5 | Make recommendation fail repeatedly | The order process opens its breaker; later requests do not connect to recommendation |
| AC-6 | Exhaust tokens | 429, with Retry-After |
| AC-7 | Worker exits after claim | Within 15 seconds the job returns to pending and is claimed again; fulfillment takes effect once |
| AC-8 | Fulfillment URL keeps returning 500 | After 5 attempts the order becomes fulfillment_failed |
| AC-9 | Leader stops heartbeats | The follower accepts writes at a higher epoch; writes after the old process returns are rejected |
| AC-10 | Create 50 unfinished jobs | New orders get 429; workers keep processing old jobs |
| AC-11 | Backlog stays above 10 | Worker count rises to 3, and does not oscillate inside the cooldown |
| AC-12 | Inject store delay | The same request_id appears in gateway, order, and store logs |

## Risks

| Risk | Version one |
| --- | --- |
| Leader crashes after confirm and before replication | The record may exist only in the old file. The caller retries with the same key. Version one does not build a majority |
| Caller fails during the 2 seconds the supervisor pauses the gateway | Return 503. The same key may be retried |
| The two order processes disagree about the breaker | Accepted. A shared breaker needs extra state and is not worth version one |
| Wall clock affects when a job is visible | It only changes how early or late a retry happens. It does not change order sequence |
| No authentication | Listen only on 127.0.0.1. Do not expose the gateway to other machines |
| fulfill_url used to reach an arbitrary host | Only `http://127.0.0.1` is allowed |
