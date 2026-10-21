package main

import (
	"testing"
)

func TestCanonicalBodySortsKeys(t *testing.T) {
	text := canonicalBody("gear-1", 1, nil, nil)
	if text != `{"external_ref":"","fulfill_url":"","qty":1,"sku":"gear-1"}` {
		t.Fatalf("canonical %s", text)
	}
}

func TestFulfillURLStaysOnLoopback(t *testing.T) {
	if !validFulfillURL(nil) || !validFulfillURL("") || !validFulfillURL("http://127.0.0.1:9000/hooks") {
		t.Fatal("loopback")
	}
	for _, sample := range []string{"http://127.0.0.1/hooks", "https://127.0.0.1:9000/hooks", "http://user:pass@127.0.0.1:9000/hooks", "http://example.com:9000/hooks"} {
		if validFulfillURL(sample) {
			t.Fatalf("accepted %s", sample)
		}
	}
}

func TestQtyMustBeInteger(t *testing.T) {
	if _, message := validateOrderBody(map[string]any{"sku": "gear-1", "qty": "1"}); message == "" {
		t.Fatal("qty string")
	}
	if _, message := validateOrderBody(map[string]any{"sku": "gear-1", "qty": true}); message == "" {
		t.Fatal("qty bool")
	}
}

func TestTokenBucketRejectsWhenEmpty(t *testing.T) {
	bucket := newTokenBucket(2, 0)
	if ok, _ := bucket.take(); !ok {
		t.Fatal("first")
	}
	if ok, _ := bucket.take(); !ok {
		t.Fatal("second")
	}
	ok, retry := bucket.take()
	if ok || retry < 1 {
		t.Fatalf("empty %v %d", ok, retry)
	}
}

func TestBreakerOpensAfterFiveFailures(t *testing.T) {
	breaker := newBreaker()
	for i := 0; i < 5; i++ {
		if !breaker.allow() {
			t.Fatal("allow")
		}
		breaker.record(false)
	}
	if breaker.allow() {
		t.Fatal("open")
	}
}
