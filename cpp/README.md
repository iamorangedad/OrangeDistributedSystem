# Orange 的 C++ 实现

这一份和 `python/src/orange/`、`rust/`、`java/`、`go/` 做同一件事：单机上的多进程下单。对外端口、错误码、日志事件和 AC-1 到 AC-12 都对齐技术方案。另外四种语言都保留，五边可以对照着读。

C++ 这边不用 Web 框架。HTTP 用 POSIX 套接字，每个连接一个线程，JSON 是手写的 `Json`。存储仍是一把锁加 sqlite。系统里已经有 `libsqlite3.so.0`，但没有 `sqlite3.h`，也没有给链接器看的 `libsqlite3.so`。构建脚本会下载 3.37.2 的头文件到 `cpp/third_party/`，再直接链接 `libsqlite3.so.0`。头文件和编译产物都不进版本库。

一次只启动一种语言。五套都绑定 8080、8090、8101 这些端口。Go 版在 `go/`。

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

## 文件对照

| C++ | Python | 做什么 |
| --- | --- | --- |
| `cpp/src/json.cpp` | `python/src/orange/common.py` 里的 JSON | 紧凑 JSON |
| `cpp/src/common.cpp` | `python/src/orange/common.py` | 参数、日志、限流、熔断、故障文件 |
| `cpp/src/http.cpp` | `python/src/orange/common.py` 里的 HTTP | 监听和调用 |
| `cpp/src/store.cpp` | `python/src/orange/store.py` | 主从存储 |
| `cpp/src/gateway.cpp` | `python/src/orange/gateway.py` | 网关 |
| `cpp/src/order.cpp` | `python/src/orange/order_service.py` | 接单 |
| `cpp/src/recommender.cpp` | `python/src/orange/recommender.py` | 推荐 |
| `cpp/src/worker.cpp` | `python/src/orange/worker.py` | 履约工人 |
| `cpp/src/supervisor.cpp` | `python/src/orange/supervisor.py` | 监督者 |
| `cpp/src/main.cpp` | `python/src/orange/__main__.py` | 子命令入口 |
| `cpp/tests/acceptance.cpp` | `python/tests/` | 单元检查和 AC-1 到 AC-12 |
