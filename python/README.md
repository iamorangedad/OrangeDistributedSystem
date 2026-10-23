# OrangeOrder 的 Python 实现

[English](README.en.md)

这一份是最初的实现，和 `rust/`、`java/`、`cpp/`、`go/` 做同一件事：单机上的多进程下单。对外端口、错误码、日志事件和 AC-1 到 AC-12 都对齐 [技术方案](../docs/technical-design.md)。语言差异见 [ADR 0004](../docs/adr/0004-stdlib-sqlite.md)。

Python 只用标准库和 sqlite3。代码放在 `python/src/orange/`，测试放在 `python/tests/`。包名是 `orange`。监督者拉起 `python -m orange.xxx`。`python/run.sh` 会把 `python/src` 放进 `PYTHONPATH`。

一次只启动一种语言。五套都绑定 8080、8090、8101 这些端口。

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

## 五种实现的文件对照

| 职责 | Python | Rust | Java | C++ | Go |
| --- | --- | --- | --- | --- | --- |
| 入口 | `python/src/orange/__main__.py` | `rust/src/main.rs` | `java/src/orange/Main.java` | `cpp/src/main.cpp` | `go/main.go` |
| 公共库 | `python/src/orange/common.py` | `rust/src/common.rs`、`http_util.rs` | `Common.java`、`HttpUtil.java`、`Json.java` | `cpp/src/common.cpp`、`http.cpp`、`json.cpp` | `go/common.go`、`http.go` |
| 网关 | `python/src/orange/gateway.py` | `rust/src/gateway.rs` | `java/src/orange/Gateway.java` | `cpp/src/gateway.cpp` | `go/gateway.go` |
| 订单 | `python/src/orange/order_service.py` | `rust/src/order.rs` | `java/src/orange/OrderService.java` | `cpp/src/order.cpp` | `go/order.go` |
| 推荐 | `python/src/orange/recommender.py` | `rust/src/recommender.rs` | `java/src/orange/Recommender.java` | `cpp/src/recommender.cpp` | `go/recommender.go` |
| 存储 | `python/src/orange/store.py` | `rust/src/store.rs` | `java/src/orange/Store.java` | `cpp/src/store.cpp` | `go/store.go` |
| 工人 | `python/src/orange/worker.py` | `rust/src/worker.rs` | `java/src/orange/Worker.java` | `cpp/src/worker.cpp` | `go/worker.go` |
| 监督者 | `python/src/orange/supervisor.py` | `rust/src/supervisor.rs` | `java/src/orange/Supervisor.java` | `cpp/src/supervisor.cpp` | `go/supervisor.go` |
| 验收 | `python/tests/` | `rust/tests/acceptance.rs` | `java/test/orange/Acceptance.java` | `cpp/tests/acceptance.cpp` | `go/acceptance/` |

Java 类都在 `java/src/orange/`。行为以技术方案的参数表为准。
