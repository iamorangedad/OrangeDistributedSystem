# API

Chinese: [接口说明](../api.md).

| Item | Value |
| --- | --- |
| Status | Design 0.1 is implemented |
| Version | 0.1 |
| Date | 2026-10-10 |

The external contract is [OpenAPI](openapi.yaml). This document adds semantics and defines the interfaces between processes. Internal interfaces are not promised to another project.

## Shared rules

External base URL: `http://127.0.0.1:8080`.

| Header | Direction | Rule |
| --- | --- | --- |
| X-Caller-Id | Request | Required on external business APIs. `[a-z0-9-]{1,32}` |
| Idempotency-Key | Request | Required on `POST /v1/orders`. Visible ASCII, length 8 to 128 |
| X-Request-Id | Optional on request, required on response | If omitted, the gateway generates `req_` plus 32 hex digits |
| Content-Type | Request | `application/json` when a body is present |
| Retry-After | Response | Only on 429. Integer seconds |

Error body:

```json
{
  "error": {
    "code": "rate_limited",
    "message": "令牌桶已空",
    "request_id": "req_0123456789abcdef0123456789abcdef"
  }
}
```

| HTTP | code | Meaning |
| --- | --- | --- |
| 400 | invalid_argument | A field, header, or fulfill_url is illegal |
| 404 | not_found | This caller has no such order |
| 409 | idempotency_mismatch | The key was used with a different body |
| 409 | epoch_fenced | Internal only. The write hit an old generation |
| 429 | rate_limited | Tokens are gone, in-flight is full, or pending jobs reached 50 |
| 503 | unavailable | No ready replica, or the leader switch is not finished |
| 504 | deadline_exceeded | This request passed its deadline |

Unknown fields are ignored. Numbers must be JSON numbers, not strings.

## External API

### POST /v1/orders

Create an order, or replay an existing one. Success is 201, including an idempotent replay.

Request body:

| Field | Type | Required | Constraint |
| --- | --- | --- | --- |
| sku | string | yes | 1 to 64 characters |
| qty | integer | yes | 1 to 100 |
| external_ref | string | no | At most 128 characters |
| fulfill_url | string | no | `http://127.0.0.1:<port><path>`, no username or password |

The 201 body is the example in the [integration guide](integration.md). `fulfillment.status` is `pending`, `inflight`, `done`, or `dead`. `fulfillment.attempts` is how many times the job has started. `fulfillment.last_error` is null when there is no error.

### GET /v1/orders/{order_id}

Returns the current snapshot for that caller. `order_id` in the path must match the whole segment.

### GET /v1/orders?idempotency_key={key}

Finds an order by key. A missing query parameter is 400. Not found is 404.

### GET /health and GET /ready

No body is required. 200 means pass, 503 means fail. On the gateway these paths are for the supervisor. A caller may use them to decide whether the entrance can take an order.

### GET /metrics

Text, one `name value` per line. Names are in the observation section of the [technical design](technical-design.md).

### GET /v1/status

Served only by the supervisor on 8090. The gateway does not forward it. It is for the experimenter, not part of the order contract.

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

## Internal API

Internal requests also carry `X-Request-Id` and `X-Deadline-Ms`. A store does not promote itself because a caller claims an epoch. Promotion accepts only the supervisor.

### Supervisor

| Method and path | Caller | Effect |
| --- | --- | --- |
| GET /internal/leader | Order, worker | Returns `{leader, epoch}` |
| POST /internal/heartbeat | Both stores | Body has node_id, role, epoch, applied_index |
| POST /internal/promote | Supervisor calls a store | Body `{new_epoch}`. The store returns 200 only after fsync |

### Order process

`POST /internal/orders` receives the external body forwarded by the gateway, plus caller, key, deadline, and request id. The response matches the external 201, and so do the error codes.

`GET /internal/recommend?sku=` is the order process calling recommendation. A 200 body is `{"recommendation":"..."}`. After 80ms the order process gives up. A slow recommendation is not an order failure.

### Store

| Method and path | Effect |
| --- | --- |
| POST /internal/append | First submission. Body has the expected epoch, caller, key, body digest, and order fields. Success returns order_id, log_index, epoch |
| GET /internal/orders/by-key | Look up the idempotency record |
| GET /internal/orders/{order_id} | Read the snapshot |
| POST /internal/snapshot | Write recommendation and degraded, update response_json |
| POST /internal/jobs/claim | Body `{worker_id}`. 200 returns a job, 204 means none is visible |
| POST /internal/jobs/{job_id}/ack | Finish the job and advance the order state |
| POST /internal/jobs/{job_id}/fail | Body `{retryable, error}` |
| POST /internal/replicate | Follower receives a contiguous batch of log entries |
| GET /internal/log?after={index} | Follower pulls entries with `log_index > after` |

`POST /internal/append` returns 409 `epoch_fenced` when the epoch is below the current value, and the body includes the store’s own epoch. If the key exists and the digest matches, it returns the existing order and does not add a log_index. A different digest returns 409 `idempotency_mismatch`.

claim, ack, and fail are accepted only on the current leader. The follower returns 409 `epoch_fenced` for those paths.

## Fulfillment callback

This is Orange calling the other project, not the other project calling Orange. The body and status codes are in the [integration guide](integration.md). The timeout is 1 second. Redirects are not followed.
