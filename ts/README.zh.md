# OrangeOrder 的 TypeScript 实现

[English](README.md)

这一份和 Python、Rust、Java、C++、Go 做同一件事：单机上的多进程下单。对外端口、错误码、日志事件和 AC-1 到 AC-12 都对齐 [技术方案](../docs/zh/technical-design.md)。语言差异见 [ADR 0004](../docs/zh/adr/0004-stdlib-sqlite.md)。

TypeScript 跑在 Node 上，不用 Web 框架。HTTP 用标准库 `node:http`。推荐和截止时间在事件循环上并行等待。存储事务用同步的 `better-sqlite3`，所以一把锁仍然是这次写入本身，不另起线程。`better-sqlite3` 在 `npm install` 时编译，不进版本库。

一次只启动一种语言。六套都绑定 8080、8090、8101 这些端口。

## 怎么跑

需要 Node 20。在仓库根目录：

```bash
ts/run.sh --root .
```

验收：

```bash
ts/run.sh test
```

`ts/run.sh` 会安装依赖、编译到 `ts/dist/`，再执行 `node dist/main.js`。不带子命令时，这个程序就是监督者；`store`、`gateway`、`order`、`recommender`、`worker` 由它再拉起自己。

## 六种实现的文件对照

| 职责 | Python | Rust | Java | C++ | Go | TypeScript |
| --- | --- | --- | --- | --- | --- | --- |
| 入口 | `python/src/orange/__main__.py` | `rust/src/main.rs` | `java/src/orange/Main.java` | `cpp/src/main.cpp` | `go/main.go` | `ts/src/main.ts` |
| 公共库 | `python/src/orange/common.py` | `rust/src/common.rs`、`http_util.rs` | `Common.java`、`HttpUtil.java`、`Json.java` | `cpp/src/common.cpp`、`http.cpp`、`json.cpp` | `go/common.go`、`http.go` | `ts/src/common.ts`、`http.ts` |
| 网关 | `python/src/orange/gateway.py` | `rust/src/gateway.rs` | `java/src/orange/Gateway.java` | `cpp/src/gateway.cpp` | `go/gateway.go` | `ts/src/gateway.ts` |
| 订单 | `python/src/orange/order_service.py` | `rust/src/order.rs` | `java/src/orange/OrderService.java` | `cpp/src/order.cpp` | `go/order.go` | `ts/src/order.ts` |
| 推荐 | `python/src/orange/recommender.py` | `rust/src/recommender.rs` | `java/src/orange/Recommender.java` | `cpp/src/recommender.cpp` | `go/recommender.go` | `ts/src/recommender.ts` |
| 存储 | `python/src/orange/store.py` | `rust/src/store.rs` | `java/src/orange/Store.java` | `cpp/src/store.cpp` | `go/store.go` | `ts/src/store.ts` |
| 工人 | `python/src/orange/worker.py` | `rust/src/worker.rs` | `java/src/orange/Worker.java` | `cpp/src/worker.cpp` | `go/worker.go` | `ts/src/worker.ts` |
| 监督者 | `python/src/orange/supervisor.py` | `rust/src/supervisor.rs` | `java/src/orange/Supervisor.java` | `cpp/src/supervisor.cpp` | `go/supervisor.go` | `ts/src/supervisor.ts` |
| 验收 | `python/tests/` | `rust/tests/acceptance.rs` | `java/test/orange/Acceptance.java` | `cpp/tests/acceptance.cpp` | `go/acceptance/` | `ts/src/acceptance.test.ts` |

Java 类都在 `java/src/orange/`。行为以技术方案的参数表为准。
