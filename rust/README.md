# OrangeOrder in Rust

[中文](README.zh.md)

Rust does not use an async runtime. Each process is threads and a lock. HTTP is parsed on a blocking `TcpListener`. sqlite comes from `rusqlite`, bundled into the binary. The code you read is order acceptance, replication, and the breaker, not a framework lifecycle.

## Run

```bash
cargo run --manifest-path rust/Cargo.toml -- --root .
```

```bash
cargo test --manifest-path rust/Cargo.toml -- --test-threads=1
```

Shared rules for ports, acceptance, and the file map are on the [homepage](../README.md).
