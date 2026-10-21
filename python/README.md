# Orange 的 Python 实现

这一份是最初的实现，和 `rust/`、`java/` 做同一件事：单机上的多进程下单。对外端口、错误码、日志事件和 AC-1 到 AC-12 都对齐技术方案。

Python 只用标准库和 sqlite3。代码放在 `python/src/orange/`，测试放在 `python/tests/`，和 Rust 的 `rust/src/`、`rust/tests/` 以及 Java 的 `java/src/orange/`、`java/test/` 同一层。包名仍是 `orange`。

一次只启动一种语言。三套都绑定 8080、8090、8101 这些端口。

## 怎么跑

在仓库根目录：

```bash
python/run.sh --root .
```

调用方仍然只访问 `http://127.0.0.1:8080`。

```bash
python/run.sh test
```

验收测试会真的拉起监督者和全部子进程，大约需要一两分钟。端口固定，不能两套一起听。

## 和另外两种语言怎么对应

监督者拉起的是 `python -m orange.xxx`。`python/run.sh` 会把 `python/src` 放进 `PYTHONPATH`。

| 模块 | Python 文件 | Rust 文件 | Java 类 | 做什么 |
| --- | --- | --- | --- | --- |
| `orange.supervisor` | `python/src/orange/supervisor.py` | `rust/src/supervisor.rs` | `Supervisor` | 拉起进程、公布主存储、提升、加减工人 |
| `orange.gateway` | `python/src/orange/gateway.py` | `rust/src/gateway.rs` | `Gateway` | 令牌桶、在途上限、转发给就绪副本 |
| `orange.order_service` | `python/src/orange/order_service.py` | `rust/src/order.rs` | `OrderService` | 幂等、截止时间、推荐熔断 |
| `orange.recommender` | `python/src/orange/recommender.py` | `rust/src/recommender.rs` | `Recommender` | 可失败的推荐旁路 |
| `orange.store` | `python/src/orange/store.py` | `rust/src/store.rs` | `Store` | sqlite 订单、任务、复制日志、epoch |
| `orange.worker` | `python/src/orange/worker.py` | `rust/src/worker.rs` | `Worker` | 向主存储领取任务并回调 |
| （公共部分） | `python/src/orange/common.py` | `rust/src/common.rs`、`rust/src/http_util.rs` | `Common`、`HttpUtil`、`Json` | 参数、日志、限流、熔断、本机 HTTP |

行为以 `docs/technical-design.md` 的参数表为准。
