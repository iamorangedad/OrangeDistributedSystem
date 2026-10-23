# Interview walkthrough: designing a distributed system

Chinese: [面试讲解](zh/interview.md).

| Item | Value |
| --- | --- |
| Status | A talk track. Behavior, parameters, and interfaces follow the design documents |
| Version | 0.1 |
| Date | 2026-10-14 |

This note shows how to answer “design a distributed system” with OrangeOrder. It is not a second specification. Parameters, fault details, and acceptance follow the [technical design](technical-design.md). Structure follows the [architecture](architecture.md). The reasons are in the [ADRs](adr/).

The interview tests whether you draw the boundary first and then make failure behavior definite. OrangeOrder is a multi-process order system on one machine. Idempotency, degradation, asynchronous fulfillment, single-leader failover, and scaling by backlog are rules that can be demonstrated and questioned.

## Opening

Spend about thirty seconds on the scope, then let the other person pick a line:

> I built an order system. The caller owns catalog, price, and stock. I only accept the order, persist it, optionally recommend, fulfill asynchronously, and fail over when the leader store dies. There are two external APIs: `POST /v1/orders` and `GET /v1/orders/{id}`. The caller talks only to the gateway at `127.0.0.1:8080`.
>
> The write path is: the gateway applies a rate limit and a deadline, then round-robins two stateless order processes. The order process commits the order and the fulfillment job in one transaction on the leader store, and returns 201 only after `fsync`. Recommendation is a side path. If it fails, the order still succeeds and is marked degraded. A worker later claims the job from the leader and fulfills it. The supervisor probes, publishes the current leader, fails over, and adds or removes workers.
>
> I deliberately skipped Raft, multi-leader, a separate message queue, and multi-region active-active. The point is what a partition costs under single-leader asynchronous replication.

That sentence covers scope, the write path, and the tradeoff. The six questions below are the usual follow-ups.

## Six questions

### If the same request is retried, do we get two orders?

The idempotency key is `caller_id + idempotency_key`, with a unique constraint on the order row. There is no extra table. The same key and the same body return the first response snapshot, without another store write and without another recommendation call. A different body returns `409 idempotency_mismatch` and leaves the original order alone.

The body digest compares the canonical JSON of `sku`, `qty`, `external_ref`, and `fulfill_url`. `order_id`, the item, and the quantity do not change on replay. Fulfillment status may advance. “No second order” and “the snapshot is frozen at the first response” are two different statements.

### If recommendation is down, does the order still succeed?

Yes, and the caller must be told it is degraded. The order process does two things in parallel: write the leader store, and wait at most 80ms for recommendation. If the store fails, the whole order fails and the recommendation is discarded. If the store succeeds and recommendation times out, the result is `201`, `degraded=true`, and an empty `recommendation`.

Each order process has its own breaker: 5 failures within 10 seconds open it, and after 5 seconds it allows one probe. While open, it does not connect to recommendation and degrades directly. The two replicas do not share a breaker. A shared breaker would be another piece of state, and version one does not add it.

### Why is fulfillment not in the order response? If the worker crashes, is the job still there?

Acceptance and fulfillment are split. The order row, the job row, and one `order_created` log entry commit in one sqlite transaction, so a confirmed order cannot lose its job. The job table is the queue. There is no separate broker. See [ADR 0002](adr/0002-job-table.md).

On claim, the job becomes `inflight` and `visible_after` is 15 seconds later. If the worker crashes before ack, the job returns to `pending` after the timeout and someone else can claim it. That is at-least-once, so the fulfillment endpoint must be idempotent on `order_id`. Failures retry with exponential backoff and full jitter. The fifth attempt becomes a dead letter and the order becomes `fulfillment_failed`. There is no separate dead-letter process.

At 50 pending jobs, the gateway returns 429 for new orders. Orders already accepted keep being fulfilled. The queue absorbs bursts. It does not accept without bound.

### What if the leader store is gone? Can two processes write at once?

Only one leader may confirm a write at a time. Leader and follower heartbeat the supervisor every 200ms. If the leader heartbeat is missing for more than 1 second and the follower is still alive, the supervisor promotes the follower to `epoch + 1`. The follower fsyncs the new epoch into its own database before it accepts writes. Writes from the old epoch get `409 epoch_fenced`. The order process asks the supervisor for the new address once and retries with the same idempotency key.

The supervisor is the only witness. The two store nodes do not promote themselves when the other heartbeat disappears, or a network break would create two leaders. If the supervisor itself is down, it does not elect a new leader and the current leader keeps serving. If the supervisor and the leader both fail, the system stops accepting orders. The choice is: do not have two writers, and let availability yield. See [ADR 0001](adr/0001-single-leader.md) and [ADR 0003](adr/0003-supervisor-witness.md).

When the switch starts, the gateway pauses new orders for at most 2 seconds so a write does not hit the old leader in that instant. During the pause the caller sees 503 and retries with the same key.

### After a 201, can this order still be missing?

Yes. Replication is asynchronous and does not block the confirmation already returned. The follower accepts only a contiguous `log_index`. If the leader disappears before replication, the unpushed tail stays in the old file and is not merged into the new leader. The caller POSTs again with the same idempotency key.

That loss window is the price of availability: when the follower is down, the leader can still take orders. Read-your-writes goes to the current leader. The follower does not serve reads. If 201 waited for the follower to confirm, a stopped follower would stop orders, and the difference between “replica lag” and “lost tail” would disappear.

### When traffic rises, what scales, and where is the protection line?

Order replicas are fixed at 2. That is resilience: one replica exits, the other keeps accepting, and the supervisor starts the failed one again. Scaling applies only to workers. `pending` above 10 for 5 seconds adds one worker, up to 3. A backlog of 0 for 20 seconds scales back in, leaving at least 1. The cooldown is 15 seconds, counted separately in each direction, so a worker just added is not immediately removed. Scaling does not use CPU or a single latency sample.

The gateway also has three backpressure gates: a token bucket that refills 10 per second with capacity 30; at most 8 in-flight orders; pending jobs at 50. All three return 429. Before forwarding, the gateway writes an absolute deadline, 800ms from now by default. The order process caps the remaining time at 400ms for the store. A timeout returns 504 and starts no new downstream call.

## How one request is tied together

The gateway writes `X-Request-Id` and `X-Deadline-Ms`. The same `request_id` appears in the JSON logs of the gateway, the order process, and the store. The supervisor’s `GET /v1/status` reports the current leader address, epoch, both `applied_index` values, the worker count, and the last promotion error.

Faults are injected with `runtime/faults.json`: slow down recommendation, pause replication, or drop one store’s heartbeat. The failover demo stops the heartbeat. It does not delete the sqlite file.

If the other person wants a demo, these three are enough:

| Action | External result | Mechanism |
| --- | --- | --- |
| POST twice with the same key | The same `order_id`, both 201 | Idempotency |
| Stop recommendation | 201, `degraded=true` | A side-path failure does not sink the main path |
| Stop the leader heartbeat | The follower accepts writes at a higher epoch; the old epoch is rejected | Witness plus fencing |

The full acceptance set is AC-1 through AC-12 in the technical design.

## Boundaries to hold

Hold three lines so the demonstration is not described as a production plan:

1. This is multi-process on one machine, so timeouts, generations, and job claims live in the code. It is not a performance sample and not a production active-active design.
2. State the consistency plainly: one writer, an asynchronous replica, failover can lose the unreplicated tail, and the caller fills it back with the idempotency key. Do not call it strong consistency or Raft.
3. Every number has a reason, and the reason is the parameter table: recommendation 80ms, visibility 15 seconds, leader loss at 1 second, backlog protection at 50. If the other person changes a constraint, change the consequence of that rule instead of adding another component.
