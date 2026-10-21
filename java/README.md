# Orange 的 Java 实现

这一份和 `python/src/orange/` 里的 Python、`rust/` 里的 Rust、`cpp/` 里的 C++ 做同一件事：单机上的多进程下单。对外端口、错误码、日志事件和 AC-1 到 AC-12 都对齐技术方案。另外三种语言都保留，四边可以对照着读。

Java 这边不用 Web 框架。HTTP 用 JDK 自带的 `com.sun.net.httpserver`，JSON 是手写的 `Json`，每个存储进程仍是一把锁。JDK 没有 sqlite，所以只多一个依赖：`sqlite-jdbc`。构建脚本会在缺少 jar 时下载它，jar 不进版本库。

一次只启动一种语言。四套都绑定 8080、8090、8101 这些端口。

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

## 和 Python 怎么对应

监督者拉起的是同一个 classpath 上的 `orange.Main`，用子命令区分进程，相当于 Python 的 `python -m orange.xxx`。

| 子命令 | Python 模块 | Java 类 | 做什么 |
| --- | --- | --- | --- |
| `supervisor` | `python/src/orange/supervisor.py` | `Supervisor` | 拉起进程、公布主存储、提升、加减工人 |
| `gateway` | `python/src/orange/gateway.py` | `Gateway` | 令牌桶、在途上限、转发给就绪副本 |
| `order` | `python/src/orange/order_service.py` | `OrderService` | 幂等、截止时间、推荐熔断 |
| `recommender` | `python/src/orange/recommender.py` | `Recommender` | 可失败的推荐旁路 |
| `store` | `python/src/orange/store.py` | `Store` | sqlite 订单、任务、复制日志、epoch |
| `worker` | `python/src/orange/worker.py` | `Worker` | 向主存储领取任务并回调 |
| （公共部分） | `python/src/orange/common.py` | `Common`、`HttpUtil`、`Json` | 参数、日志、限流、熔断、本机 HTTP |

读的时候建议先打开 Python 文件，再打开同一行表格里的 Java 类。行为以 `docs/technical-design.md` 的参数表为准。
