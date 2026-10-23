# 接口说明

英文版：[API](en/api.md)。

| 项 | 值 |
| --- | --- |
| 状态 | 设计 0.1 已实现 |
| 版本 | 0.1 |
| 日期 | 2026-10-10 |

对外契约以 [OpenAPI](openapi.yaml) 为准。本文补充语义，并定义进程之间的内部接口。内部接口不承诺给另一个项目使用。

## 公共约定

对外基地址：`http://127.0.0.1:8080`。

| 头 | 方向 | 要求 |
| --- | --- | --- |
| X-Caller-Id | 请求 | 对外业务接口必填。`[a-z0-9-]{1,32}` |
| Idempotency-Key | 请求 | `POST /v1/orders` 必填。可见 ASCII，长度 8 到 128 |
| X-Request-Id | 请求可选，响应必有 | 未提供时网关生成 `req_` 加 32 位十六进制 |
| Content-Type | 请求 | 带正文时为 `application/json` |
| Retry-After | 响应 | 仅 429。秒数，整数 |

错误正文：

```json
{
  "error": {
    "code": "rate_limited",
    "message": "令牌桶已空",
    "request_id": "req_0123456789abcdef0123456789abcdef"
  }
}
```

| HTTP | code | 含义 |
| --- | --- | --- |
| 400 | invalid_argument | 字段、请求头或 fulfill_url 不合法 |
| 404 | not_found | 该调用方名下没有这笔订单 |
| 409 | idempotency_mismatch | 钥匙已用过，正文不同 |
| 409 | epoch_fenced | 仅内部。写到了旧代际 |
| 429 | rate_limited | 令牌用尽、在途已满，或待处理任务达到 50 |
| 503 | unavailable | 没有就绪副本，或主存储切换未完成 |
| 504 | deadline_exceeded | 超过这次请求的截止时间 |

未知字段忽略。数字必须是 JSON 数字，不能是字符串。

## 对外接口

### POST /v1/orders

创建订单，或重放已有订单。成功为 201，包括幂等重放。

请求体：

| 字段 | 类型 | 必填 | 约束 |
| --- | --- | --- | --- |
| sku | string | 是 | 1 到 64 字符 |
| qty | integer | 是 | 1 到 100 |
| external_ref | string | 否 | 最多 128 字符 |
| fulfill_url | string | 否 | `http://127.0.0.1:<port><path>`，不含用户名和密码 |

201 的正文见 [集成指南](integration.md) 的示例。`fulfillment.status` 取 `pending`、`inflight`、`done`、`dead`。`fulfillment.attempts` 是已开始次数。`fulfillment.last_error` 没有错误时为 null。

### GET /v1/orders/{order_id}

返回该调用方的当前快照。路径中的 `order_id` 必须整段匹配。

### GET /v1/orders?idempotency_key={key}

用钥匙找回订单。缺少查询参数时 400。找不到时 404。

### GET /health 与 GET /ready

无正文要求。200 表示通过，503 表示不通过。网关的这两个路径给监督者用，调用方可以用它们判断入口是否可发送下单。

### GET /metrics

文本，每行一个 `名字 数值`。名字见 [技术方案](technical-design.md) 的观测一节。

### GET /v1/status

只由监督者在 8090 提供，网关不转发。供实验者查看，不是下单契约的一部分。

```json
{
  "leader": "http://127.0.0.1:8302",
  "epoch": 2,
  "applied_index": {"store-a": 10, "store-b": 14},
  "workers": 1,
  "jobs_pending": 0,
  "gateway_paused": false,
  "ready": {"gateway": true, "order-1": true, "order-2": true, "store-a": true, "store-b": true, "recommender": true},
  "last_promotion_error": null
}
```

## 内部接口

内部请求同样携带 `X-Request-Id` 和 `X-Deadline-Ms`。存储不信任调用方自称的 epoch 来提升自己；提升只接受监督者。

### 监督者

| 方法与路径 | 调用者 | 作用 |
| --- | --- | --- |
| GET /internal/leader | 订单、工人 | 返回 `{leader, epoch}` |
| POST /internal/heartbeat | 两个存储 | 正文含 node_id、role、epoch、applied_index |
| POST /internal/promote | 监督者调用存储 | 正文 `{new_epoch}`。存储 fsync 后才返回 200 |

### 订单进程

`POST /internal/orders` 接收网关转发的对外正文，另加调用方、钥匙、截止时间和请求号。响应与对外 201 相同，错误码也相同。

`GET /internal/recommend?sku=` 由订单进程调用推荐。200 正文 `{"recommendation":"..."}`。超过 80ms 由订单进程放弃，不把推荐的慢当作下单失败。

### 存储

| 方法与路径 | 作用 |
| --- | --- |
| POST /internal/append | 首次下单。正文含期望 epoch、调用方、钥匙、正文摘要、订单字段。成功返回 order_id、log_index、epoch |
| GET /internal/orders/by-key | 查询幂等记录 |
| GET /internal/orders/{order_id} | 读取快照 |
| POST /internal/snapshot | 写入推荐结果和 degraded，更新 response_json |
| POST /internal/jobs/claim | 正文 `{worker_id}`。200 返回任务，204 表示当前没有 |
| POST /internal/jobs/{job_id}/ack | 完成任务并推进订单状态 |
| POST /internal/jobs/{job_id}/fail | 正文 `{retryable, error}` |
| POST /internal/replicate | 从存储接收一批连续日志 |
| GET /internal/log?after={index} | 从存储补拉 `log_index > after` 的日志 |

`POST /internal/append` 在 epoch 低于当前值时返回 409 `epoch_fenced`，正文附带存储自己的 epoch。钥匙已存在且摘要相同则返回已有订单，不新增 log_index。摘要不同返回 409 `idempotency_mismatch`。

claim、ack、fail 只接受当前主存储上的请求。从存储对这些路径返回 409 `epoch_fenced`。

## 履约回调

这是 OrangeOrder 调用外部项目，不是外部项目调用 OrangeOrder。正文和状态码约定写在 [集成指南](integration.md)。超时 1 秒，不跟随重定向。
