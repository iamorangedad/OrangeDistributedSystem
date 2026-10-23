# OrangeOrder 的 Rust 实现

[English](README.md)

这一份和 Python、Java、C++、Go 做同一件事：单机上的多进程下单。对外端口、错误码、日志事件和 AC-1 到 AC-12 都对齐 [技术方案](../docs/zh/technical-design.md)。语言差异见 [ADR 0004](../docs/zh/adr/0004-stdlib-sqlite.md)。

Rust 这边不用 Web 框架，也不用异步运行时。每个进程仍是线程加锁，HTTP 用 `TcpListener` 自己解析。sqlite 用 `rusqlite`，随二进制带上。这样读代码时看到的是接单、复制和熔断，而不是框架的生命周期。

监督者拉起同一个可执行文件的不同子命令。一次只启动一种语言。六套都绑定 8080、8090、8101 这些端口。

## 怎么跑

在仓库根目录：

```bash
cargo run --manifest-path rust/Cargo.toml -- --root .
```

调用方仍然只访问 `http://127.0.0.1:8080`。

```bash
cargo test --manifest-path rust/Cargo.toml -- --test-threads=1
```

验收测试会真的拉起监督者和全部子进程，大约需要一两分钟。`--test-threads=1` 是因为端口固定，不能两套一起听。

六种实现的文件对照在 [首页](../README.zh.md)。
