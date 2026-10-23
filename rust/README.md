# OrangeOrder in Rust

[中文](README.zh.md)

This implementation does the same job as Python, Java, C++, and Go: a single-machine, multi-process order system. Ports, error codes, log events, and AC-1 through AC-12 follow the [technical design](../docs/technical-design.md). Language differences are in [ADR 0004](../docs/adr/0004-stdlib-sqlite.md).

Rust does not use a web framework or an async runtime. Each process is threads and a lock. HTTP is parsed on a blocking `TcpListener`. sqlite comes from `rusqlite`, bundled into the binary. The code you read is order acceptance, replication, and the breaker, not a framework lifecycle.

The supervisor re-executes the same binary with a subcommand. Run one language at a time. All six stacks bind 8080, 8090, 8101, and the other fixed ports.

## Run

From the repository root:

```bash
cargo run --manifest-path rust/Cargo.toml -- --root .
```

Callers still use only `http://127.0.0.1:8080`.

```bash
cargo test --manifest-path rust/Cargo.toml -- --test-threads=1
```

The acceptance test starts the supervisor and every child. It takes about a minute or two. `--test-threads=1` is required because the ports are fixed.

The file map for all six implementations is on the [homepage](../README.md).
