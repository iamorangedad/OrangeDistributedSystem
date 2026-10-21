# ADR 0003: The supervisor is the election witness

Chinese: [中文](../../adr/0003-supervisor-witness.md).

- Status: accepted
- Date: 2026-10-10

## Context

With only heartbeats and no third party, a network break between leader and follower can make both believe they should accept writes. An order system must not have two writers.

The supervisor also starts processes and adds or removes workers by backlog. It already holds the list of who the leader is.

## Decision

Only the supervisor may call the store promotion API. A store node does not promote itself because the other node’s heartbeat disappeared.

Order processes and workers use only the leader address the supervisor publishes. A write to an old generation returns `epoch_fenced`. The order process or worker refreshes the address and tries once more.

When a switch starts, the supervisor pauses new orders at the gateway for at most 2 seconds. After the new leader is published, it resumes. During the pause, new orders get 503.

If the supervisor itself stops heartbeating or exits, the current leader does not change. Orders already in progress finish. If the leader also exits during that time, the system stops accepting orders until the supervisor returns and promotes by the same rules.

## Consequences

There is one witness. If the supervisor and the leader fail in sequence, availability yields to “do not have two leaders.” That is version one’s explicit choice for a partition.

Gateway, order replicas, and workers are started and scaled in the same process as leader election. An experiment reads one status page.

## Rejected

- Letting the store nodes elect each other. Two nodes cannot form a majority during a partition. The result is either two leaders or both waiting.
- Making the witness a third store replica and implementing Raft. That is a separate exercise, not part of the order path.
