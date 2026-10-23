# 集成指南

英文版：[Integration guide](../integration.md)。

| 项 | 值 |
| --- | --- |
| 状态 | 设计 0.1 已实现 |
| 版本 | 0.1 |
| 日期 | 2026-10-10 |
| 读者 | 要调用 OrangeOrder 的另一个项目 |

机器可读契约是 [OpenAPI](openapi.yaml)。本文说明调用方要遵守的行为。

## 职责分界

| 事情 | 谁做 |
| --- | --- |
| 商品、价格、库存是否够、要不要卖 | 调用方，在调用 OrangeOrder 之前决定 |
| 接单、幂等、保存、主从切换 | OrangeOrder |
| 推荐文案 | OrangeOrder 的旁路。缺失时订单仍然有效 |
| 履约业务动作 | 调用方，如果提供了 `fulfill_url` |
| 记住两边的单号 | 调用方保存 `order_id`，OrangeOrder 保存 `external_ref` |

OrangeOrder 不校验 `sku` 是否存在，也不保存金额。

## 接入

1. 选定一个 `caller_id`，只含小写字母、数字和连字符，长度 1 到 32。放到每个请求的 `X-Caller-Id`。
2. 只访问 `http://127.0.0.1:8080`。不要直接访问 8101、8301 等内部端口。
3. 每次新的业务下单生成一个新的 `Idempotency-Key`。同一次尝试的所有重试共用这一把钥匙。
4. 可选：把调用方自己的单号放进 `external_ref`。
5. 可选：提供 `fulfill_url`，用来接收履约调用。
6. 保存响应里的 `order_id`。之后用 GET 看履约是否完成。

## 创建订单

```http
POST /v1/orders HTTP/1.1
Host: 127.0.0.1:8080
Content-Type: application/json
X-Caller-Id: gear-card
Idempotency-Key: cart-88-attempt-1

{
  "sku": "gear-1",
  "qty": 1,
  "external_ref": "cart-88",
  "fulfill_url": "http://127.0.0.1:9000/hooks/orange-fulfill"
}
```

成功时 HTTP 201：

```json
{
  "order_id": "ord_0123456789abcdef0123456789abcdef",
  "caller_id": "gear-card",
  "external_ref": "cart-88",
  "sku": "gear-1",
  "qty": 1,
  "state": "accepted",
  "recommendation": null,
  "degraded": true,
  "fulfillment": {
    "status": "pending",
    "attempts": 0,
    "last_error": null
  },
  "epoch": 1,
  "request_id": "req_0123456789abcdef0123456789abcdef"
}
```

`degraded=true` 表示推荐没有取到，订单已经接受。调用方应当把 201 当成下单成功，把 `degraded` 当成附加信息。

`state=accepted` 只表示 OrangeOrder 已经保存订单。履约要看 `fulfillment.status`，或者等 `state` 变成 `fulfilled` / `fulfillment_failed`。

## 幂等

钥匙的范围是「同一个 `X-Caller-Id`」。不同调用方可以碰巧用到相同字符串。

- 钥匙相同、正文相同：不会产生第二笔订单。响应仍是 201，主体是这笔订单的当前快照。
- 钥匙相同、`sku`、`qty`、`external_ref` 或 `fulfill_url` 有任一不同：409，错误码 `idempotency_mismatch`。换一把新钥匙，或改回原来的正文。
- 调用方超时但没有读到响应：用同一把钥匙再 POST 一次。这是规定的重试方式。

第一版的令牌桶是网关全局的，每秒补充 10 个，最多攒 30 个。共享这台网关的调用方会互相影响。打满时 HTTP 429，响应头 `Retry-After` 给出秒数。

## 调用方超时与重试

OrangeOrder 内部给一次下单 800ms。调用方自己的 HTTP 超时建议设为 **1.5 秒**，略长于内部截止时间，以便收到明确的错误码而不是只看到连接断开。

| 结果 | 调用方动作 |
| --- | --- |
| 201 | 成功。即使 `degraded` 为 true |
| 400 | 修改请求，不要原样重试 |
| 404 | 单号不存在，不要当作临时故障重试创建 |
| 409 `idempotency_mismatch` | 停止重试，检查钥匙和正文 |
| 429 | 等待 `Retry-After` 后，用同一把钥匙重试 |
| 503 | 用同一把钥匙重试。主存储切换时会出现 |
| 504 | 用同一把钥匙重试 |
| 连接失败 | 用同一把钥匙重试 |

不要在没拿住钥匙的情况下，因为超时就生成一把新钥匙再提交。那样可能产生两笔订单。

## 查询

```http
GET /v1/orders/ord_0123456789abcdef0123456789abcdef HTTP/1.1
X-Caller-Id: gear-card
```

只能查到该 `caller_id` 名下的订单，否则 404。

如果调用方丢掉了 `order_id`，但还留着钥匙：

```http
GET /v1/orders?idempotency_key=cart-88-attempt-1 HTTP/1.1
X-Caller-Id: gear-card
```

### 状态怎么读

| state | fulfillment.status | 含义 |
| --- | --- | --- |
| accepted | pending | 已接单，还没有工人领取 |
| accepted | inflight | 工人正在履约，可能重试 |
| fulfilled | done | 履约完成 |
| fulfillment_failed | dead | 不再自动重试，需要人工看 `last_error` |

轮询间隔建议 1 秒，履约完成或失败后停止。第一版没有 webhook。

## 履约回调

请求里的 `fulfill_url` 可省略。省略时 OrangeOrder 在本地把任务标成完成，用来单独运行和演示。

提供时，工人向该地址 POST：

```json
{
  "event": "fulfill",
  "order_id": "ord_0123456789abcdef0123456789abcdef",
  "caller_id": "gear-card",
  "external_ref": "cart-88",
  "sku": "gear-1",
  "qty": 1
}
```

约定：

- 地址的主机必须是 `127.0.0.1`，方案必须是 `http`。其他地址在创建订单时会被 400 拒绝。
- 返回 2xx：OrangeOrder 把订单标为 `fulfilled`。
- 返回 409：调用方表示这笔 `order_id` 已经处理过。OrangeOrder 也视为成功。
- 其他状态或超时：OrangeOrder 稍后重试，最多 5 次，然后订单变为 `fulfillment_failed`。
- 同一次履约可能送达多次。调用方用 `order_id` 去重。

## 故障时调用方会看到什么

| 情况 | 调用方可见的结果 |
| --- | --- |
| 推荐服务挂了 | 201，`degraded=true`，`recommendation` 为 null |
| 一个订单进程挂了 | 通常仍是 201，由另一个副本处理 |
| 两个订单进程都未就绪 | 503 |
| 请求超过内部截止时间 | 504。用同一把钥匙重试 |
| 新订单太密或履约积压超过 50 | 429 |
| 主存储刚刚换代 | 503。用同一把钥匙重试，成功后 `epoch` 可能变大 |
| 主存储在复制前崩溃并发生切换 | 极少数已向调用方返回 201 的订单，可能不在新主上。调用方以 GET 为准；GET 404 时用同一把钥匙重新 POST |

最后一行是第一版的数据窗口：成功响应发生在主存储 fsync 之后，但复制是异步的。切换若赶在复制之前，新主没有这笔订单。集成时把 GET 或同一把钥匙的再次 POST 当作核对手段，不要只相信「当时收到过 201」而不再查询。

## 最小示例

```bash
curl -sS -D - \
  -H 'Content-Type: application/json' \
  -H 'X-Caller-Id: gear-card' \
  -H 'Idempotency-Key: cart-88-attempt-1' \
  -d '{"sku":"gear-1","qty":1,"external_ref":"cart-88"}' \
  http://127.0.0.1:8080/v1/orders
```

先按首页启动其中一种语言，一次只启动一套。网关在 `127.0.0.1:8080` 就绪之后，上面的命令才会有响应。
