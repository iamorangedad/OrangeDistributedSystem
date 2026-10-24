# OrangeOrder 下单系统

[English](README.md)

OrangeOrder 是一个跑在单机上的多进程下单系统，用来学习分布式里的容错和伸缩，并给另一个项目提供稳定的下单入口。

当前状态：**设计 0.1 已实现。** 调用方拥有商品、价格和购物车；本系统负责接单、保存、降级、异步履约和主从切换。

## 文档

| 文档 | 内容 |
| --- | --- |
| [需求与变更记录](docs/zh/requirements-history.md) | 原始弹性表、评审结论，以及之后的需求变更 |
| [系统概述](docs/zh/overview.md) | 背景、目标、边界、术语 |
| [架构设计](docs/zh/architecture.md) | 上下文、进程图、时序、状态机 |
| [技术方案](docs/zh/technical-design.md) | 选型、写入规则、复制、伸缩、参数、验收 |
| [接口说明](docs/zh/api.md) | 对外语义与进程间接口 |
| [OpenAPI](docs/zh/openapi.yaml) | 机器可读的对外契约 |
| [数据模型](docs/zh/data-model.md) | 表、日志、状态 |
| [集成指南](docs/zh/integration.md) | 另一个项目如何接入 |
| [ADR 0001 单主](docs/zh/adr/0001-single-leader.md) | 异步复制，用 epoch 隔离旧主 |
| [ADR 0002 任务表](docs/zh/adr/0002-job-table.md) | 用任务表充当队列 |
| [ADR 0003 见证人](docs/zh/adr/0003-supervisor-witness.md) | 只有监督者能提升主存储 |
| [ADR 0004 技术栈](docs/zh/adr/0004-stdlib-sqlite.md) | 六种实现与 sqlite |
| [ADR 0005 集成方式](docs/zh/adr/0005-sync-poll-integration.md) | 同步下单、轮询履约、可选回调 |
| [面试讲解](docs/zh/interview.md) | 用本系统回答「如何设计一个分布式系统」 |

英文设计文档在 [docs](docs/overview.md)。集成方读 [集成指南](docs/zh/integration.md) 和 [OpenAPI](docs/zh/openapi.yaml) 即可开始对接。实现时以 [技术方案](docs/zh/technical-design.md) 里的参数表为准。

## 进程一览

调用方只访问网关 `127.0.0.1:8080`。其余端口是系统内部的。

| 进程 | 端口 | 角色 |
| --- | --- | --- |
| 网关 | 8080 | 限流、截止时间、分发到就绪副本 |
| 订单 A / B | 8101 / 8102 | 无状态接单 |
| 推荐 | 8200 | 可失败的旁路 |
| 主存储 / 从存储 | 启动时 8301 / 8302 | 单主写入，从节点异步复制；切换后角色对调，端口不变 |
| 监督者 | 8090 | 探活、拉起、公布主节点、加减工人 |
| 工人 | 8401 起 | 1 到 3 个；端口只提供探针和指标，履约时访问主存储 |

## 六种语言

同一份设计写了六遍，用来对照着学语言。Python 是最初的实现。Rust、Java、C++、Go 和 TypeScript 重复同样的进程、端口、错误和验收。

| | 代码 | 启动 | 测试 |
| --- | --- | --- | --- |
| Python | `python/src/` | `python/run.sh --root .` | `python/run.sh test` |
| Rust | `rust/src/` | `cargo run --manifest-path rust/Cargo.toml -- --root .` | `cargo test --manifest-path rust/Cargo.toml -- --test-threads=1` |
| Java | `java/src/` | `java/build.sh --root .` | `java/build.sh test` |
| C++ | `cpp/src/` | `cpp/build.sh --root .` | `cpp/build.sh test` |
| Go | `go/` | `go/run.sh --root .` | `go/run.sh test` |
| TypeScript | `ts/src/` | `ts/run.sh --root .` | `ts/run.sh test` |

六套实现占用 8080、8090、8101 以及其余固定端口，一次只跑一套。调用方只访问 `http://127.0.0.1:8080`。下面的启动和测试命令都在仓库根目录执行。端口、错误码、日志事件和 AC-1 到 AC-12 以 [技术方案](docs/zh/technical-design.md) 为准。语言差异见 [ADR 0004](docs/zh/adr/0004-stdlib-sqlite.md)。六套都不用 Web 框架。

验收会拉起监督者和全部子进程，大约一两分钟。端口固定，不能两套一起听。Rust 的测试因此加上 `--test-threads=1`。

Python 用 `python -m` 拉起每个进程，例如 `python -m supervisor`。其余五种是一个程序：不带子命令时它就是监督者，再以 `store`、`gateway`、`order`、`recommender`、`worker` 拉起自己。

每个语言目录里，`README.md` 是英文说明，`README.zh.md` 是中文说明。

## 文件对照

| 职责 | Python | Rust | Java | C++ | Go | TypeScript |
| --- | --- | --- | --- | --- | --- | --- |
| 入口 | `python/src/supervisor.py` | `rust/src/main.rs` | `java/src/Main.java` | `cpp/src/main.cpp` | `go/main.go` | `ts/src/main.ts` |
| 公共库 | `python/src/common.py` | `rust/src/common.rs`、`http_util.rs` | `Common.java`、`HttpUtil.java`、`Json.java` | `cpp/src/common.cpp`、`http.cpp`、`json.cpp` | `go/common.go`、`http.go` | `ts/src/common.ts`、`http.ts` |
| 网关 | `python/src/gateway.py` | `rust/src/gateway.rs` | `java/src/Gateway.java` | `cpp/src/gateway.cpp` | `go/gateway.go` | `ts/src/gateway.ts` |
| 订单 | `python/src/order_service.py` | `rust/src/order.rs` | `java/src/OrderService.java` | `cpp/src/order.cpp` | `go/order.go` | `ts/src/order.ts` |
| 推荐 | `python/src/recommender.py` | `rust/src/recommender.rs` | `java/src/Recommender.java` | `cpp/src/recommender.cpp` | `go/recommender.go` | `ts/src/recommender.ts` |
| 存储 | `python/src/store.py` | `rust/src/store.rs` | `java/src/Store.java` | `cpp/src/store.cpp` | `go/store.go` | `ts/src/store.ts` |
| 工人 | `python/src/worker.py` | `rust/src/worker.rs` | `java/src/Worker.java` | `cpp/src/worker.cpp` | `go/worker.go` | `ts/src/worker.ts` |
| 监督者 | `python/src/supervisor.py` | `rust/src/supervisor.rs` | `java/src/Supervisor.java` | `cpp/src/supervisor.cpp` | `go/supervisor.go` | `ts/src/supervisor.ts` |
| 验收 | `python/tests/` | `rust/tests/acceptance.rs` | `java/test/Acceptance.java` | `cpp/tests/acceptance.cpp` | `go/acceptance/` | `ts/src/acceptance.test.ts` |

Java 类都在 `java/src/`。行为以技术方案的参数表为准。
