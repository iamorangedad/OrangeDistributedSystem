# Orange 下单系统

Orange 是一个跑在单机上的多进程下单系统，用来学习分布式里的容错和伸缩，并给另一个项目提供稳定的下单入口。

当前状态：**设计 0.1 已实现。** 调用方拥有商品、价格和购物车；本系统负责接单、保存、降级、异步履约和主从切换。

## 文档

| 文档 | 内容 |
| --- | --- |
| [需求与变更记录](docs/requirements-history.md) | 原始弹性表、评审结论，以及之后的需求变更 |
| [系统概述](docs/overview.md) | 背景、目标、边界、术语 |
| [架构设计](docs/architecture.md) | 上下文、进程图、时序、状态机 |
| [技术方案](docs/technical-design.md) | 选型、写入规则、复制、伸缩、参数、验收 |
| [接口说明](docs/api.md) | 对外语义与进程间接口 |
| [OpenAPI](docs/openapi.yaml) | 机器可读的对外契约 |
| [数据模型](docs/data-model.md) | 表、日志、状态 |
| [集成指南](docs/integration.md) | 另一个项目如何接入 |
| [ADR 0001 单主](docs/adr/0001-single-leader.md) | 异步复制，用 epoch 隔离旧主 |
| [ADR 0002 任务表](docs/adr/0002-job-table.md) | 用任务表充当队列 |
| [ADR 0003 见证人](docs/adr/0003-supervisor-witness.md) | 只有监督者能提升主存储 |
| [ADR 0004 技术栈](docs/adr/0004-stdlib-sqlite.md) | Python 标准库与 sqlite |
| [ADR 0005 集成方式](docs/adr/0005-sync-poll-integration.md) | 同步下单、轮询履约、可选回调 |
| [面试讲解](docs/interview.md) | 用本系统回答「如何设计一个分布式系统」 |

集成方读 [集成指南](docs/integration.md) 和 [OpenAPI](docs/openapi.yaml) 即可开始对接。实现时以 [技术方案](docs/technical-design.md) 里的参数表为准。

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

## 三种语言

同一份设计写了三遍，用来对照着学语言。Python 是最初的实现。Rust 和 Java 重复同样的进程、端口、错误和验收。

| | Python | Rust | Java |
| --- | --- | --- | --- |
| 代码 | `orange/` | `rust/src/` | `java/src/orange/` |
| 启动 | `python3 -m orange.supervisor --root .` | `cargo run --manifest-path rust/Cargo.toml -- --root .` | `java/build.sh --root .` |
| 测试 | `python3 -m unittest tests.test_common tests.test_acceptance` | `cargo test --manifest-path rust/Cargo.toml -- --test-threads=1` | `java/build.sh test` |

三套实现占用同一组端口，一次只跑一套。文件对照表在 [rust/README.md](rust/README.md) 和 [java/README.md](java/README.md)。
