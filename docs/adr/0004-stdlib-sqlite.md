# ADR 0004: Six implementations and sqlite

Chinese: [中文](../zh/adr/0004-stdlib-sqlite.md).

- Status: accepted
- Date: 2026-10-10, extended to five languages on 2026-10-21, TypeScript added on 2026-10-23

## Context

The system runs on this machine so timeouts, the breaker, replication, and fault injection can be read step by step. After a web framework, an ORM, or a container orchestrator is introduced, a demonstrated fault has to be sorted into business rule or framework behavior first.

The data size is a demonstration: about 10 requests per second through the token bucket, and a job backlog cap of 50.

The same behavior is later read across languages. The external contract must not leak one language’s types.

## Decision

Six implementations share the same ports, errors, logs, and acceptance tests. Each store node has one sqlite file, accessed with SQL by that process. Replication pushes `replication_log` at the application layer. It does not copy the database file and does not use a sqlite cluster mode.

The supervisor starts processes directly. There is no container manifest. Only one language runs at a time, because all six bind the same ports.

| Language | How a process runs | HTTP and JSON | sqlite |
| --- | --- | --- | --- |
| Python | `python -m orange.xxx`, standard library | Standard-library HTTP and `json` | Standard-library `sqlite3` |
| Rust | Subcommands of one binary, threads, no async runtime | Blocking HTTP parsed in process, `serde_json` | `rusqlite`, sqlite bundled in the binary |
| Java | Subcommands of the same `orange.Main` | JDK `HttpServer`, hand-written `Json` | `sqlite-jdbc`, downloaded by the build, not committed |
| C++ | Subcommands of one binary, one thread per connection | POSIX sockets, hand-written JSON | System `libsqlite3.so.0`; the header is downloaded by the build |
| Go | Subcommands of one binary, goroutines and a lock | Standard-library `net/http` | `modernc.org/sqlite`, pure Go, no cgo |
| TypeScript | Subcommands of one Node process, the event loop | Standard-library `node:http` and `JSON` | `better-sqlite3`, compiled at install, not committed |

The file map is on the [homepage](../../README.md).

## Consequences

The mechanisms live in the modules themselves: token bucket, deadline, breaker, backoff, logs, and the fault file. Connection handling is enough for a local demonstration. It is not a performance sample.

Callers read OpenAPI and the fault behavior in the [technical design](../technical-design.md). Starting a different language does not change the request or the response.

## Rejected

- Treating an off-the-shelf gateway and database cluster as “distributed.” It runs, but timeouts, generations, and job claims are invisible.
- Putting version one on Kubernetes. Replicas, probes, and scaling would become cluster configuration instead of rules in this code.
- Keeping only one language. A side-by-side reading would not show which file holds the same rule in another runtime.
