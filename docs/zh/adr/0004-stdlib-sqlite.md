# ADR 0004：六种实现与 sqlite

英文版：[English](../en/adr/0004-stdlib-sqlite.md)。

- 状态：已接受
- 日期：2026-10-10，2026-10-21 扩到五种语言，2026-10-23 加上 TypeScript

## 背景

系统要跑在当前这台机器上，方便单步阅读超时、熔断、复制和故障注入。引入 Web 框架、ORM 或容器编排之后，演示故障时要先分辨是业务规则还是框架行为。

数据量是演示规模：令牌桶每秒约 10 个请求，任务积压上限 50。

同一份行为后来要用多种语言对照着读。对外契约不能泄漏某一种语言的类型。

## 决策

六种实现服从同一份端口、错误、日志和验收。每个存储节点一个 sqlite 文件，由该进程用 SQL 访问。复制在应用层按 `replication_log` 推送，不复制数据库文件，也不使用 sqlite 的集群方案。

进程由监督者直接拉起，不写容器清单。一次只启动一种语言，因为六套都绑定同一组端口。

| 语言 | 进程怎么跑 | HTTP 与 JSON | sqlite |
| --- | --- | --- | --- |
| Python | `python -m orange.xxx`，标准库 | 标准库 HTTP 与 `json` | 标准库 `sqlite3` |
| Rust | 一个二进制的子命令，线程，不用异步运行时 | 自己解析的阻塞 HTTP，`serde_json` | `rusqlite`，随二进制带上 sqlite |
| Java | 同一个 `orange.Main` 的子命令 | JDK 的 `HttpServer`，手写 `Json` | `sqlite-jdbc`，构建时下载，不进版本库 |
| C++ | 一个二进制的子命令，每个连接一个线程 | POSIX 套接字，手写 JSON | 系统的 `libsqlite3.so.0`，头文件由构建脚本下载 |
| Go | 一个二进制的子命令，goroutine 加锁 | 标准库 `net/http` | `modernc.org/sqlite`，纯 Go，不使用 cgo |
| TypeScript | 一个 Node 进程的子命令，事件循环 | 标准库 `node:http`，`JSON` | `better-sqlite3`，安装时编译，不进版本库 |

文件对照写在 `python/README.md`、`rust/README.md`、`java/README.md`、`cpp/README.md`、`go/README.md` 和 `ts/README.md`。

## 后果

机制都在自己的模块里：令牌桶、截止时间、熔断、退避、日志和故障文件。连接处理足够支撑本机演示，不作为性能样本。

调用方只看 OpenAPI 和 [技术方案](../technical-design.md) 里的故障行为。换一种语言启动，请求和响应不变。

## 放弃的方案

- 用现成的网关和数据库集群充当「分布式」。能跑起来，但超时、代际和任务领取都看不见。
- 第一版上 Kubernetes。副本、探针和扩缩容会变成集群配置，而不是这段代码里的规则。
- 只保留一种语言。对照着读时，看不出同一条规则在不同运行时里落在哪一个文件。
