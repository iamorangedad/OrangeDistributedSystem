# OrangeOrder 的 TypeScript 实现

[English](README.md)

TypeScript 跑在 Node 上。HTTP 用标准库 `node:http`。推荐和截止时间在事件循环上并行等待。存储事务用同步的 `better-sqlite3`，所以一把锁仍然是这次写入本身，不另起线程。`better-sqlite3` 在 `npm install` 时编译，不进版本库。

## 怎么跑

需要 Node 20。

```bash
ts/run.sh --root .
```

```bash
ts/run.sh test
```

`ts/run.sh` 会安装依赖、编译到 `ts/dist/`，再执行 `node dist/main.js`。

端口、验收和文件对照的共同规则在 [首页](../README.zh.md)。
