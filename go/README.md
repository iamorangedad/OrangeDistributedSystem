# OrangeOrder in Go

[中文](README.zh.md)

This implementation does the same job as Python, Rust, Java, and C++: a single-machine, multi-process order system. Ports, error codes, log events, and AC-1 through AC-12 follow the [technical design](../docs/technical-design.md). Language differences are in [ADR 0004](../docs/adr/0004-stdlib-sqlite.md).

Go does not use a web framework. HTTP is the standard `net/http`. Each process is goroutines plus a lock. sqlite is the pure-Go module `modernc.org/sqlite`, so cgo is not required. Module checksums are in `go.sum`.

Run one language at a time. All six stacks bind 8080, 8090, 8101, and the other fixed ports.

## Run

Go 1.22 is required. From the repository root:

```bash
go/run.sh --root .
```

Acceptance:

```bash
go/run.sh test
```

`go/run.sh` builds `go/bin/orange`. With no subcommand the program is the supervisor. It re-executes itself as `store`, `gateway`, `order`, `recommender`, or `worker`.

The file map for all six implementations is on the [homepage](../README.md).
