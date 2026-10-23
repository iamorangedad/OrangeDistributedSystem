# OrangeOrder in TypeScript

[中文](README.zh.md)

This copy does the same thing as Python, Rust, Java, C++, and Go: a multi-process order system on one machine. Ports, error codes, log events, and AC-1 through AC-12 follow the [technical design](../docs/technical-design.md). Language differences are in [ADR 0004](../docs/adr/0004-stdlib-sqlite.md).

TypeScript runs on Node, without a web framework. HTTP is the standard-library `node:http`. The recommendation and the deadline wait together on the event loop. Store transactions use synchronous `better-sqlite3`, so the lock is still the write itself. `better-sqlite3` is compiled by `npm install` and is not committed.

Run one language at a time. All six stacks bind 8080, 8090, 8101, and the other fixed ports.

## Run

Node 20 is required. From the repository root:

```bash
ts/run.sh --root .
```

Acceptance:

```bash
ts/run.sh test
```

`ts/run.sh` installs dependencies, compiles into `ts/dist/`, and runs `node dist/main.js`. With no subcommand the program is the supervisor. It starts `store`, `gateway`, `order`, `recommender`, and `worker` again as itself.

The file map for all six implementations is on the [homepage](../README.md).
