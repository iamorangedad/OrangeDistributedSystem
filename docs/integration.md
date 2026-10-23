# Integration guide

Chinese: [集成指南](zh/integration.md).

| Item | Value |
| --- | --- |
| Status | Design 0.1 is implemented |
| Version | 0.1 |
| Date | 2026-10-10 |
| Reader | Another project that calls OrangeOrder |

The machine-readable contract is [OpenAPI](openapi.yaml). This document describes the behavior the caller must follow.

## Who does what

| Work | Who |
| --- | --- |
| Catalog, price, whether stock is enough, whether to sell | The caller, before calling OrangeOrder |
| Accept, idempotency, persistence, leader failover | OrangeOrder |
| Recommendation text | A side path in OrangeOrder. If it is missing, the order is still valid |
| The business action of fulfillment | The caller, if it supplied `fulfill_url` |
| Remembering both ids | The caller stores `order_id`. OrangeOrder stores `external_ref` |

OrangeOrder does not check that `sku` exists, and it does not store a price.

## Connect

1. Choose a `caller_id` of lowercase letters, digits, and hyphens, length 1 to 32. Put it on every request as `X-Caller-Id`.
2. Call only `http://127.0.0.1:8080`. Do not call internal ports such as 8101 or 8301.
3. Generate a new `Idempotency-Key` for each new business order. Every retry of that attempt reuses the same key.
4. Optional: put the caller’s own reference in `external_ref`.
5. Optional: supply `fulfill_url` to receive the fulfillment call.
6. Save `order_id` from the response. Later, GET it to see whether fulfillment finished.

## Create an order

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

On success, HTTP 201:

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

`degraded=true` means recommendation was not obtained and the order was still accepted. The caller should treat 201 as success and `degraded` as extra information.

`state=accepted` only means OrangeOrder has saved the order. Fulfillment is `fulfillment.status`, or waiting until `state` becomes `fulfilled` or `fulfillment_failed`.

## Idempotency

A key is scoped to one `X-Caller-Id`. Different callers may happen to use the same string.

- Same key, same body: no second order. The response is still 201, and the body is the current snapshot of that order.
- Same key, and any of `sku`, `qty`, `external_ref`, or `fulfill_url` differs: 409, error code `idempotency_mismatch`. Use a new key, or send the original body again.
- The caller timed out and never read a response: POST again with the same key. That is the specified retry.

Version one’s token bucket is global on the gateway. It refills 10 tokens per second and holds at most 30. Callers that share this gateway affect each other. When it is empty, HTTP 429, and `Retry-After` gives seconds.

## Caller timeout and retry

OrangeOrder gives one order submission 800ms internally. The caller’s own HTTP timeout should be **1.5 seconds**, a little longer than the internal deadline, so the caller receives a definite error code instead of only a dropped connection.

| Result | Caller action |
| --- | --- |
| 201 | Success, even when `degraded` is true |
| 400 | Change the request. Do not retry it unchanged |
| 404 | The id does not exist. Do not retry creation as a transient fault |
| 409 `idempotency_mismatch` | Stop retrying. Check the key and the body |
| 429 | Wait for `Retry-After`, then retry with the same key |
| 503 | Retry with the same key. This happens during a leader switch |
| 504 | Retry with the same key |
| Connection failure | Retry with the same key |

Do not mint a new key because of a timeout when the original key was not retained. That can create two orders.

## Query

```http
GET /v1/orders/ord_0123456789abcdef0123456789abcdef HTTP/1.1
X-Caller-Id: gear-card
```

Only orders under that `caller_id` are visible. Otherwise the result is 404.

If the caller lost `order_id` but still has the key:

```http
GET /v1/orders?idempotency_key=cart-88-attempt-1 HTTP/1.1
X-Caller-Id: gear-card
```

### How to read status

| state | fulfillment.status | Meaning |
| --- | --- | --- |
| accepted | pending | Accepted, no worker has claimed it |
| accepted | inflight | A worker is fulfilling it, and may retry |
| fulfilled | done | Fulfillment finished |
| fulfillment_failed | dead | No more automatic retries. A person should read `last_error` |

Poll about once a second, and stop after fulfillment succeeds or fails. Version one has no webhook.

## Fulfillment callback

`fulfill_url` may be omitted. OrangeOrder then marks the job done locally, which is enough to run and demonstrate the system alone.

When it is set, the worker POSTs:

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

Rules:

- The host must be `127.0.0.1` and the scheme must be `http`. Any other address is rejected with 400 when the order is created.
- 2xx: OrangeOrder marks the order `fulfilled`.
- 409: the caller says this `order_id` was already handled. OrangeOrder also treats that as success.
- Any other status or a timeout: OrangeOrder retries later, at most 5 times, then the order becomes `fulfillment_failed`.
- The same fulfillment may be delivered more than once. The caller deduplicates by `order_id`.

## What the caller sees during a fault

| Situation | What the caller sees |
| --- | --- |
| Recommendation is down | 201, `degraded=true`, `recommendation` is null |
| One order process is down | Usually still 201, handled by the other replica |
| Both order processes are not ready | 503 |
| The request passed the internal deadline | 504. Retry with the same key |
| New orders are too dense, or fulfillment backlog exceeds 50 | 429 |
| The leader just changed generation | 503. Retry with the same key. After success, `epoch` may be larger |
| The leader crashed before replication and a failover happened | A few orders that already returned 201 may be absent on the new leader. Trust GET. If GET is 404, POST again with the same key |

The last row is version one’s data window: the success response happens after the leader fsync, but replication is asynchronous. If failover happens before replication, the new leader does not have that order. Treat GET, or another POST with the same key, as the check. Do not trust “a 201 was received once” and never look again.

## Smallest example

```bash
curl -sS -D - \
  -H 'Content-Type: application/json' \
  -H 'X-Caller-Id: gear-card' \
  -H 'Idempotency-Key: cart-88-attempt-1' \
  -d '{"sku":"gear-1","qty":1,"external_ref":"cart-88"}' \
  http://127.0.0.1:8080/v1/orders
```

Start one language from the homepage first, and only one stack. The command above gets a response after the gateway is ready at `127.0.0.1:8080`.
