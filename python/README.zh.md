# OrangeOrder 的 Python 实现

[English](README.md)

这一份是最初的实现。Python 只用标准库和 sqlite3。代码直接放在 `python/src/`，测试放在 `python/tests/`。`python/run.sh` 会把 `python/src` 放进 `PYTHONPATH`，再执行 `python -m supervisor`。

## 怎么跑

```bash
python/run.sh --root .
```

```bash
python/run.sh test
```

端口、验收和文件对照的共同规则在 [首页](../README.zh.md)。
