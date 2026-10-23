# OrangeOrder 的 Java 实现

[English](README.md)

这一份和 Python、Rust、C++、Go 做同一件事：单机上的多进程下单。对外端口、错误码、日志事件和 AC-1 到 AC-12 都对齐 [技术方案](../docs/zh/technical-design.md)。语言差异见 [ADR 0004](../docs/zh/adr/0004-stdlib-sqlite.md)。

Java 这边不用 Web 框架。HTTP 用 JDK 自带的 `com.sun.net.httpserver`，JSON 是手写的 `Json`，每个存储进程仍是一把锁。JDK 没有 sqlite，所以只多一个依赖：`sqlite-jdbc`。构建脚本会在缺少 jar 时下载它，jar 不进版本库。

监督者拉起同一个 classpath 上的 `orange.Main`，用子命令区分进程。一次只启动一种语言。六套都绑定 8080、8090、8101 这些端口。

## 怎么跑

需要 JDK 21。脚本使用 `JAVA_HOME`；没有设置时用 `~/.local/jdk`。

在仓库根目录：

```bash
java/build.sh --root .
```

调用方仍然只访问 `http://127.0.0.1:8080`。

```bash
java/build.sh test
```

验收测试会真的拉起监督者和全部子进程，大约需要一两分钟。端口固定，不能两套一起听。

六种实现的文件对照在 [首页](../README.zh.md)。
