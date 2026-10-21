# ADR 0001: Single leader, fenced by epoch

Chinese: [中文](../../adr/0001-single-leader.md).

- Status: accepted
- Date: 2026-10-10

## Context

The store must keep accepting orders after a process disappears, and two processes must not confirm writes at the same time. Options include a single leader with an async replica, synchronous replication to the follower, multi-leader, and Raft/Paxos.

A learning project also has to say what a partition gives up. The caller needs to know when an order that already received 201 might still be missing.

## Decision

At any moment only one leader store may confirm a write. Confirmation happens after the leader transaction commits with `synchronous=FULL`. The follower applies the replication log asynchronously and may lag.

Each failover increments the epoch. The follower fsyncs the new epoch into its own database before it accepts writes. Appends from an old epoch are rejected.

The supervisor is the only role that may start a promotion. While the supervisor is down, no new leader is chosen and the current leader keeps serving.

An order already returned to the caller, but not yet replicated, can remain only in the old file after failover. The caller POSTs again with the same idempotency key. Version one does not merge that tail back into the new leader.

## Consequences

When the follower is down, the leader can still take orders. If the leader disappears before replication, at most the unreplicated tail is lost.

Read-your-writes goes to the current leader. The follower does not serve order reads.

Raft, multi-leader, and multi-region active-active wait until this failover can be demonstrated.

## Rejected

- Waiting for the follower to confirm before returning 201. If the follower stops, orders stop, and the difference between “replica lag” and “lost tail” never shows up.
- Multiple leaders writing at once. Conflict merge would cover the order path.
- Implementing Raft in version one. Elections, log matching, and membership changes would cover the business path.
