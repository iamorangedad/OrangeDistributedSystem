# OrangeOrder 的 Go 实现

[English](README.md)

HTTP 用标准库的 `net/http`。每个进程是 goroutine 加一把锁。sqlite 用纯 Go 的 `modernc.org/sqlite`，这样不依赖 cgo。模块校验和写在 `go.sum` 里。

## 怎么跑

需要 Go 1.22。

```bash
go/run.sh --root .
```

```bash
go/run.sh test
```

`go/run.sh` 先编译出 `go/bin/orange`。

端口、验收和文件对照的共同规则在 [首页](../README.zh.md)。
