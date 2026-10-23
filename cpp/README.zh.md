# OrangeOrder 的 C++ 实现

[English](README.md)

HTTP 用 POSIX 套接字，每个连接一个线程，JSON 是手写的。存储仍是一把锁加 sqlite。系统里已经有 `libsqlite3.so.0`，但没有 `sqlite3.h`，也没有给链接器看的 `libsqlite3.so`。构建脚本会下载 3.37.2 的头文件到 `cpp/third_party/`，再直接链接 `libsqlite3.so.0`。头文件和编译产物都不进版本库。

## 怎么跑

需要 g++ 和 `libsqlite3`。

```bash
cpp/build.sh --root .
```

```bash
cpp/build.sh test
```

`cpp/build.sh` 先编译出 `cpp/build/orange`。

端口、验收和文件对照的共同规则在 [首页](../README.zh.md)。
