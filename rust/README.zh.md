# OrangeOrder 的 Rust 实现

[English](README.md)

Rust 不用异步运行时。每个进程是线程加锁，HTTP 在阻塞的 `TcpListener` 上自己解析。sqlite 用 `rusqlite`，随二进制带上。这样读代码时看到的是接单、复制和熔断，而不是框架的生命周期。

## 怎么跑

```bash
cargo run --manifest-path rust/Cargo.toml -- --root .
```

```bash
cargo test --manifest-path rust/Cargo.toml -- --test-threads=1
```

端口、验收和文件对照的共同规则在 [首页](../README.zh.md)。
