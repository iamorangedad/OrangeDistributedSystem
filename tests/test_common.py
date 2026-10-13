"""不启动进程也能核对的规则。"""

import unittest

from orange.common import (
    CircuitBreaker,
    TokenBucket,
    canonical_body,
    valid_fulfill_url,
    validate_order_body,
)


class CommonTest(unittest.TestCase):
    def test_canonical_body_sorts_keys(self) -> None:
        text = canonical_body("gear-1", 1, None, None)
        self.assertEqual(text, '{"external_ref":"","fulfill_url":"","qty":1,"sku":"gear-1"}')

    def test_fulfill_url_stays_on_loopback(self) -> None:
        self.assertTrue(valid_fulfill_url(None))
        self.assertTrue(valid_fulfill_url("http://127.0.0.1:9000/hooks"))
        self.assertFalse(valid_fulfill_url("http://127.0.0.1/hooks"))
        self.assertFalse(valid_fulfill_url("https://127.0.0.1:9000/hooks"))
        self.assertFalse(valid_fulfill_url("http://user:pass@127.0.0.1:9000/hooks"))
        self.assertFalse(valid_fulfill_url("http://example.com:9000/hooks"))

    def test_qty_must_be_an_integer(self) -> None:
        body, message = validate_order_body({"sku": "gear-1", "qty": "1"})
        self.assertIsNone(body)
        self.assertIsNotNone(message)
        body, message = validate_order_body({"sku": "gear-1", "qty": True})
        self.assertIsNone(body)

    def test_token_bucket_rejects_when_empty(self) -> None:
        bucket = TokenBucket(capacity=2, rate=0)
        self.assertTrue(bucket.take()[0])
        self.assertTrue(bucket.take()[0])
        allowed, retry_after = bucket.take()
        self.assertFalse(allowed)
        self.assertGreaterEqual(retry_after, 1)

    def test_breaker_opens_after_five_failures(self) -> None:
        breaker = CircuitBreaker()
        for _ in range(5):
            self.assertTrue(breaker.allow())
            breaker.record(False)
        self.assertFalse(breaker.allow())


if __name__ == "__main__":
    unittest.main()
