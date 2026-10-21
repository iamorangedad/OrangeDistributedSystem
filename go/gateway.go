package main

import (
	"strings"
	"sync"
	"time"
)

type gateway struct {
	supervisor  string
	orders      []string
	bucket      *tokenBucket
	metrics     *metrics
	inflight    int
	cursor      int
	orderReady  map[string]bool
	paused      bool
	jobsPending int64
	mu          sync.Mutex
	stop        chan struct{}
}

func newGateway(supervisor string, orders []string) *gateway {
	ready := map[string]bool{}
	for _, item := range orders {
		ready[item] = false
	}
	return &gateway{
		supervisor: supervisor,
		orders:     orders,
		bucket:     newTokenBucket(tokenCapacity, tokenRefillPerSec),
		metrics:    newMetrics(),
		orderReady: ready,
		stop:       make(chan struct{}),
	}
}

func (g *gateway) poll() {
	for {
		select {
		case <-g.stop:
			return
		default:
		}
		for _, item := range g.orders {
			status, _, _, err := httpRequest("GET", item+"/ready", nil, nil, 300*time.Millisecond)
			ready := err == nil && status == 200
			g.mu.Lock()
			g.orderReady[item] = ready
			g.mu.Unlock()
		}
		status, _, raw, err := httpRequest("GET", g.supervisor+"/internal/gateway-control", nil, nil, 300*time.Millisecond)
		if err == nil && status == 200 {
			body, _ := decodeJSON(raw)
			parsed := asMap(body)
			g.mu.Lock()
			g.paused = asBool(parsed["paused"])
			g.jobsPending = asInt(parsed["jobs_pending"])
			g.mu.Unlock()
		}
		time.Sleep(heartbeat)
	}
}

func (g *gateway) pick() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.paused {
		return ""
	}
	var ready []string
	for _, item := range g.orders {
		if g.orderReady[item] {
			ready = append(ready, item)
		}
	}
	if len(ready) == 0 {
		return ""
	}
	chosen := ready[g.cursor%len(ready)]
	g.cursor++
	return chosen
}

func (g *gateway) ready() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.paused {
		return false
	}
	for _, item := range g.orderReady {
		if item {
			return true
		}
	}
	return false
}

func reject(g *gateway, status int, code, message, requestID string, retryAfter int, withRetry bool) response {
	g.metrics.inc("orange_errors_total", 1)
	if code == "rate_limited" {
		logEvent("rate_limited", requestID, "info", map[string]any{"message": message})
	}
	extra := map[string]string{}
	if withRetry {
		extra["Retry-After"] = jsonNumberText(retryAfter)
	}
	return jsonResponse(status, errorBody(code, message, requestID), extra)
}

func jsonNumberText(value int) string {
	return strings.TrimSpace(string(marshal(value)))
}

func createOrder(g *gateway, headers map[string]string, raw []byte, requestID string) response {
	caller := headers["x-caller-id"]
	key := headers["idempotency-key"]
	if !validCaller(caller) {
		return jsonResponse(400, errorBody("invalid_argument", "X-Caller-Id 不合法", requestID), nil)
	}
	if !validIdempotencyKey(key) {
		return jsonResponse(400, errorBody("invalid_argument", "Idempotency-Key 不合法", requestID), nil)
	}
	parsed, err := decodeJSON(raw)
	if err != nil {
		return jsonResponse(400, errorBody("invalid_argument", "正文不是 JSON", requestID), nil)
	}
	fields, message := validateOrderBody(parsed)
	if fields == nil {
		return jsonResponse(400, errorBody("invalid_argument", message, requestID), nil)
	}
	allowed, retryAfter := g.bucket.take()
	if !allowed {
		return reject(g, 429, "rate_limited", "令牌桶已空", requestID, retryAfter, true)
	}
	g.mu.Lock()
	pending := g.jobsPending
	paused := g.paused
	g.mu.Unlock()
	if pending >= jobsPendingLimit {
		return reject(g, 429, "rate_limited", "待处理任务已达上限", requestID, 1, true)
	}
	g.mu.Lock()
	if paused {
		g.mu.Unlock()
		return reject(g, 503, "unavailable", "主存储正在切换", requestID, 0, false)
	}
	if g.inflight >= gatewayInflightLimit {
		g.mu.Unlock()
		return reject(g, 429, "rate_limited", "在途请求已满", requestID, 1, true)
	}
	g.inflight++
	current := g.inflight
	g.mu.Unlock()
	g.metrics.set("orange_inflight", int64(current))
	defer func() {
		g.mu.Lock()
		g.inflight--
		current = g.inflight
		g.mu.Unlock()
		g.metrics.set("orange_inflight", int64(current))
	}()
	deadline := nowMS() + orderDeadlineMs
	forward := map[string]string{
		"X-Caller-Id": caller, "Idempotency-Key": key, "X-Request-Id": requestID,
		"X-Deadline-Ms": jsonNumberText(int(deadline)),
	}
	for range g.orders {
		target := g.pick()
		if target == "" {
			return reject(g, 503, "unavailable", "没有就绪的订单副本", requestID, 0, false)
		}
		status, responseHeaders, body, err := httpRequest("POST", target+"/internal/orders", fields, forward, time.Duration(orderDeadlineMs)*time.Millisecond+200*time.Millisecond)
		if err != nil {
			g.metrics.inc("orange_errors_total", 1)
			continue
		}
		extra := map[string]string{"X-Request-Id": requestID}
		if retry := responseHeaders["retry-after"]; retry != "" {
			extra["Retry-After"] = retry
		}
		contentType := responseHeaders["content-type"]
		if contentType == "" {
			contentType = "application/json"
		}
		extra["Content-Type"] = contentType
		logEvent("gateway_forward", requestID, "info", map[string]any{"status": status})
		return response{status: status, headers: extra, body: body}
	}
	return reject(g, 503, "unavailable", "没有就绪的订单副本", requestID, 0, false)
}

func forwardGet(g *gateway, route string, query, headers map[string]string, requestID string) response {
	caller := headers["x-caller-id"]
	if !validCaller(caller) {
		return jsonResponse(400, errorBody("invalid_argument", "X-Caller-Id 不合法", requestID), nil)
	}
	if route == "/internal/orders" && query["idempotency_key"] == "" {
		return jsonResponse(400, errorBody("invalid_argument", "缺少 idempotency_key", requestID), nil)
	}
	target := g.pick()
	if target == "" {
		return jsonResponse(503, errorBody("unavailable", "没有就绪的订单副本", requestID), nil)
	}
	queryText := ""
	if len(query) > 0 {
		values := make([]string, 0, len(query))
		for key, value := range query {
			values = append(values, queryEscape(key)+"="+queryEscape(value))
		}
		queryText = "?" + strings.Join(values, "&")
	}
	status, _, body, err := httpRequest("GET", target+route+queryText, nil, map[string]string{
		"X-Caller-Id": caller, "X-Request-Id": requestID, "X-Deadline-Ms": jsonNumberText(int(nowMS() + orderDeadlineMs)),
	}, time.Second)
	if err != nil {
		return jsonResponse(503, errorBody("unavailable", "没有就绪的订单副本", requestID), nil)
	}
	return response{status: status, headers: map[string]string{"Content-Type": "application/json; charset=utf-8", "X-Request-Id": requestID}, body: body}
}

func gatewayHandler(g *gateway) handlerFunc {
	return func(method, path string, headers map[string]string, raw []byte) response {
		requestID := headerRequestID(headers)
		route, query := splitPath(path)
		switch {
		case method == "GET" && route == "/health":
			return emptyResponse(200)
		case method == "GET" && route == "/ready":
			if g.ready() {
				return emptyResponse(200)
			}
			return emptyResponse(503)
		case method == "GET" && route == "/metrics":
			g.mu.Lock()
			g.metrics.set("orange_inflight", int64(g.inflight))
			g.mu.Unlock()
			return textResponse(200, g.metrics.render([]string{"orange_errors_total", "orange_inflight"}))
		case method == "POST" && route == "/v1/orders":
			return createOrder(g, headers, raw, requestID)
		case method == "GET" && route == "/v1/orders":
			return forwardGet(g, "/internal/orders", query, headers, requestID)
		case method == "GET" && strings.HasPrefix(route, "/v1/orders/"):
			return forwardGet(g, "/internal/orders/"+route[len("/v1/orders/"):], map[string]string{}, headers, requestID)
		default:
			return jsonResponse(404, errorBody("not_found", "没有这个路径", requestID), nil)
		}
	}
}

func runGateway(args []string) {
	flags := newFlagSet()
	port := flags.Int("port", 8080, "")
	supervisor := flags.String("supervisor", "", "")
	orders := flags.String("orders", "", "")
	root := flags.String("root", ".", "")
	name := flags.String("process-name", "gateway", "")
	parseFlags(flags, args)
	setContext(*name, *root)
	startFaultReader()
	var listed []string
	for _, item := range strings.Split(*orders, ",") {
		if item != "" {
			listed = append(listed, item)
		}
	}
	gate := newGateway(*supervisor, listed)
	go gate.poll()
	logEvent("process_started", "", "info", map[string]any{"port": *port})
	if err := serve(*port, gatewayHandler(gate)); err != nil {
		panic(err)
	}
}
