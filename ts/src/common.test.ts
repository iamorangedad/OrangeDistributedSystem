import assert from "node:assert/strict";
import test from "node:test";
import { CircuitBreaker, JsonNumber, TokenBucket, canonicalBody, validFulfillURL, validateOrderBody } from "./common";

test("canonical body sorts keys", () => {
  const text = canonicalBody("gear-1", 1, null, null);
  assert.equal(text, '{"external_ref":"","fulfill_url":"","qty":1,"sku":"gear-1"}');
});

test("fulfill url stays on loopback", () => {
  assert.equal(validFulfillURL(null), true);
  assert.equal(validFulfillURL(""), true);
  assert.equal(validFulfillURL("http://127.0.0.1:9000/hooks"), true);
  for (const sample of ["http://127.0.0.1/hooks", "https://127.0.0.1:9000/hooks", "http://user:pass@127.0.0.1:9000/hooks", "http://example.com:9000/hooks"]) {
    assert.equal(validFulfillURL(sample), false, sample);
  }
});

test("qty must be an integer token", () => {
  assert.notEqual(validateOrderBody({ sku: "gear-1", qty: "1" })[1], "");
  assert.notEqual(validateOrderBody({ sku: "gear-1", qty: true })[1], "");
  assert.equal(validateOrderBody({ sku: "gear-1", qty: new JsonNumber("1") })[1], "");
  assert.notEqual(validateOrderBody({ sku: "gear-1", qty: new JsonNumber("1.0") })[1], "");
});

test("token bucket rejects when empty", () => {
  const bucket = new TokenBucket(2, 0);
  assert.equal(bucket.take()[0], true);
  assert.equal(bucket.take()[0], true);
  const [ok, retry] = bucket.take();
  assert.equal(ok, false);
  assert.ok(retry >= 1);
});

test("breaker opens after five failures", () => {
  const breaker = new CircuitBreaker();
  for (let i = 0; i < 5; i++) {
    assert.equal(breaker.allow(), true);
    breaker.record(false);
  }
  assert.equal(breaker.allow(), false);
});
