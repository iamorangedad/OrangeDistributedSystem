# ADR 0002: The job table is the queue

Chinese: [中文](../../adr/0002-job-table.md).

- Status: accepted
- Date: 2026-10-10

## Context

Fulfillment must happen after acceptance, and the job must survive a worker crash. A separate message broker can deliver, acknowledge, and pile up, but that process also needs persistence and failover. The lesson then becomes “get Kafka running” instead of the queue semantics.

If the order and the job are written in two steps, the order can be confirmed without a job, or a job can exist without an order.

## Decision

The leader’s `jobs` table is the queue. The order row, the job row, and one `order_created` log entry commit in the same sqlite transaction.

A worker claims a job by moving it to `inflight` and setting `visible_after` 15 seconds ahead. When that time passes, the job can be claimed again. The handler is idempotent on `order_id`.

After 5 attempts the job becomes `dead` and the order becomes `fulfillment_failed`. There is no separate dead-letter process.

When pending jobs reach 50, the gateway rejects new orders. The queue absorbs bursts. It does not accept without bound.

## Consequences

On store failover, job state travels with the replication log. Workers claim only from the current leader.

One fewer middleware system that would need its own high availability. Visibility timeout, retry, and dead letter are still on the path.

When the backlog is high, order writes and fulfillment contend for the same sqlite file. The demonstration rate is about 10 new orders per second, and that contention is acceptable.

## Rejected

- An in-memory queue plus a separate outbox process. A restart drops messages that were taken and not acknowledged, and then needs another recovery path.
- Kafka or a compatible protocol. Operations and vocabulary would cover acknowledgement, retry, and dead letter.
