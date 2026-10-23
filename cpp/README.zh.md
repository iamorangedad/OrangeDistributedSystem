# OrangeOrder 的 C++ 实现

[English](README.md)

这一份和 Python、Rust、Java、Go 做同一件事：单机上的多进程下单。对外端口、错误码、日志事件和 AC-1 到 AC-12 都对齐 [技术方案](../docs/zh/technical-design.md)。语言差异见 [ADR 0004](../docs/zh/adr/0004-stdlib-sqlite.md)。

C++ 这边不用 Web 框架。HTTP 用 POSIX 套接字，每个连接一个线程，JSON 是手写的。存储仍是一把锁加 sqlite。系统里已经有 `libsqlite3.so.0`，但没有 `sqlite3.h`，也没有给链接器看的 `libsqlite3.so`。构建脚本会下载 3.37.2 的头文件到 `cpp/third_party/`，再直接链接 `libsqlite3.so.0`。头文件和编译产物都不进版本库。

一次只启动一种语言。六套都绑定 8080、8090、8101 这些端口。

## 怎么跑

需要 g++ 和 `libsqlite3`。在仓库根目录：

```bash
cpp/build.sh --root .
```

验收：

```bash
cpp/build.sh test
```

`cpp/build.sh` 先编译出 `cpp/build/orange`。不带子命令时，这个程序就是监督者；`store`、`gateway`、`order`、`recommender`、`worker` 由它再拉起自己。

六种实现的文件对照在 [首页](../README.zh.md)。
