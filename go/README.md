# Orange 的 Go 实现

这一份和 `python/src/orange/`、`rust/`、`java/`、`cpp/` 做同一件事：单机上的多进程下单。对外端口、错误码、日志事件和 AC-1 到 AC-12 都对齐技术方案。另外四种语言都保留，五边可以对照着读。

Go 这边不用 Web 框架。HTTP 用标准库的 `net/http`，每个进程仍是 goroutine 加一把锁。sqlite 用纯 Go 的 `modernc.org/sqlite`，这样不依赖 cgo。模块依赖写在 `go.sum` 里。

一次只启动一种语言。五套都绑定 8080、8090、8101 这些端口。

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

## 文件对照

| Go | Python | 做什么 |
| --- | --- | --- |
| `go/common.go` | `python/src/orange/common.py` | 参数、日志、限流、熔断、故障文件 |
| `go/http.go` | `python/src/orange/common.py` 里的 HTTP | 监听和调用 |
| `go/store.go` | `python/src/orange/store.py` | 主从存储 |
| `go/gateway.go` | `python/src/orange/gateway.py` | 网关 |
| `go/order.go` | `python/src/orange/order_service.py` | 接单 |
| `go/recommender.go` | `python/src/orange/recommender.py` | 推荐 |
| `go/worker.go` | `python/src/orange/worker.py` | 履约工人 |
| `go/supervisor.go` | `python/src/orange/supervisor.py` | 监督者 |
| `go/main.go` | `python/src/orange/__main__.py` | 子命令入口 |
| `go/acceptance/` | `python/tests/` | AC-1 到 AC-12 |
