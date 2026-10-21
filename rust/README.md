# Orange 的 Rust 实现

这一份和 `python/src/orange/` 里的 Python 实现做同一件事：单机上的多进程下单。对外端口、错误码、日志事件和 AC-1 到 AC-12 都对齐技术方案。Python、Java、C++ 都保留，四边可以对照着读。

Rust 这边故意不用 Web 框架，也不用异步运行时。每个进程仍是一个线程加一把锁，HTTP 用标准库的 `TcpListener` 自己解析。这样读代码时看到的是接单、复制和熔断，而不是框架的生命周期。

一次只启动一种语言。Python、Rust、Java、C++ 都绑定 8080、8090、8101 这些端口。Java 版在 `java/`，C++ 版在 `cpp/`。

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

## 和 Python 怎么对应

监督者拉起的是同一个可执行文件的不同子命令，相当于 Python 的 `python -m orange.xxx`。

| 子命令 | Python 模块 | Rust 文件 | 做什么 |
| --- | --- | --- | --- |
| `supervisor` | `python/src/orange/supervisor.py` | `rust/src/supervisor.rs` | 拉起进程、公布主存储、提升、加减工人 |
| `gateway` | `python/src/orange/gateway.py` | `rust/src/gateway.rs` | 令牌桶、在途上限、转发给就绪副本 |
| `order` | `python/src/orange/order_service.py` | `rust/src/order.rs` | 幂等、截止时间、推荐熔断 |
| `recommender` | `python/src/orange/recommender.py` | `rust/src/recommender.rs` | 可失败的推荐旁路 |
| `store` | `python/src/orange/store.py` | `rust/src/store.rs` | sqlite 订单、任务、复制日志、epoch |
| `worker` | `python/src/orange/worker.py` | `rust/src/worker.rs` | 向主存储领取任务并回调 |
| （公共部分） | `python/src/orange/common.py` | `rust/src/common.rs`、`rust/src/http_util.rs` | 参数、日志、限流、熔断、本机 HTTP |

读的时候建议先打开 Python 文件，再打开同一行表格里的 Rust 文件。行为以 `docs/technical-design.md` 的参数表为准。
