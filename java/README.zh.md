# OrangeOrder 的 Java 实现

[English](README.md)

HTTP 用 JDK 自带的 `com.sun.net.httpserver`。JSON 是手写的 `Json`。每个存储进程仍是一把锁。JDK 没有 sqlite，所以只多一个依赖：`sqlite-jdbc`。构建脚本会在缺少 jar 时下载它，jar 不进版本库。

## 怎么跑

需要 JDK 21。脚本使用 `JAVA_HOME`；没有设置时用 `~/.local/jdk`。

```bash
java/build.sh --root .
```

```bash
java/build.sh test
```

端口、验收和文件对照的共同规则在 [首页](../README.zh.md)。
