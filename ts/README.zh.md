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

六种实现的文件对照在 [首页](../README.zh.md)。
