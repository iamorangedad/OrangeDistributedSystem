# ADR 0005: Synchronous submit and polling; the fulfillment URL is optional

Chinese: [中文](../../adr/0005-sync-poll-integration.md).

- Status: accepted
- Date: 2026-10-10

## Context

Another project needs order submission, but its catalog, price, and stock rules do not belong to OrangeOrder. The two sides need a narrow boundary so OrangeOrder can be demonstrated alone and the caller can attach later.

Callers usually want a notification when fulfillment finishes. If version one also builds webhook registration, signatures, retries, and unsubscribe, the integration surface becomes wider than the order path.

## Decision

The caller creates an order synchronously through the gateway and stores `order_id`. Whether fulfillment is done is decided by GET of the current snapshot. The suggested poll interval is 1 second.

`X-Caller-Id` only distinguishes tenant-style callers. It is not authentication. The service binds only the local loopback address.

`fulfill_url` is optional. When it is set, the worker POSTs a fulfillment event to that address on `http://127.0.0.1`, and the caller deduplicates by `order_id`. When it is omitted, the worker marks the job done locally, and OrangeOrder can still run by itself.

The same caller, the same idempotency key, and the same body do not create a second order.

## Consequences

Integration does not need a long-lived connection, and the caller does not have to implement a callback first. Real fulfillment adds one local HTTP handler.

Without a callback, the caller polls. That is acceptable for a demonstration and for single-machine integration.

OrangeOrder does not check whether `sku` exists in the caller’s catalog, so the caller must finish its own checks before POST.

## Rejected

- A signed webhook in version one. Delivery state, failure alarms, and an offline caller become a second system.
- Letting the caller write sqlite directly. That drops the idempotency contract, the generation, and degradation, and binds integration to the table layout.
- Putting price and stock inside OrangeOrder. The boundary would grow into a commerce system and cover the distributed-systems line.
