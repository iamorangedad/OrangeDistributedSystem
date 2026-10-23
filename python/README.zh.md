# OrangeOrder 的 Python 实现

[English](README.md)

这一份是最初的实现，和 `rust/`、`java/`、`cpp/`、`go/`、`ts/` 做同一件事：单机上的多进程下单。对外端口、错误码、日志事件和 AC-1 到 AC-12 都对齐 [技术方案](../docs/zh/technical-design.md)。语言差异见 [ADR 0004](../docs/zh/adr/0004-stdlib-sqlite.md)。

Python 只用标准库和 sqlite3。代码放在 `python/src/orange/`，测试放在 `python/tests/`。包名是 `orange`。监督者拉起 `python -m orange.xxx`。`python/run.sh` 会把 `python/src` 放进 `PYTHONPATH`。

一次只启动一种语言。六套都绑定 8080、8090、8101 这些端口。

## 怎么跑

在仓库根目录：

```bash
python/run.sh --root .
```

调用方仍然只访问 `http://127.0.0.1:8080`。

```bash
python/run.sh test
```

验收测试会真的拉起监督者和全部子进程，大约需要一两分钟。端口固定，不能两套一起听。

六种实现的文件对照在 [首页](../README.zh.md)。
