# OrangeOrder 的 Go 实现

[English](README.md)

这一份和 Python、Rust、Java、C++ 做同一件事：单机上的多进程下单。对外端口、错误码、日志事件和 AC-1 到 AC-12 都对齐 [技术方案](../docs/zh/technical-design.md)。语言差异见 [ADR 0004](../docs/zh/adr/0004-stdlib-sqlite.md)。

Go 这边不用 Web 框架。HTTP 用标准库的 `net/http`，每个进程仍是 goroutine 加一把锁。sqlite 用纯 Go 的 `modernc.org/sqlite`，这样不依赖 cgo。模块依赖写在 `go.sum` 里。

一次只启动一种语言。六套都绑定 8080、8090、8101 这些端口。

## 怎么跑

需要 Go 1.22。在仓库根目录：

```bash
go/run.sh --root .
```

验收：

```bash
go/run.sh test
```

`go/run.sh` 先编译出 `go/bin/orange`。不带子命令时，这个程序就是监督者；`store`、`gateway`、`order`、`recommender`、`worker` 由它再拉起自己。

六种实现的文件对照在 [首页](../README.zh.md)。
