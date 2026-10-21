package main

import (
	"strconv"
	"sync"
	"time"
)

type orderService struct {
	name        string
	supervisor  string
	recommender string
	breaker     *circuitBreaker
	metrics     *metrics
	leader      string
	epoch       int64
	leaderOK    bool
	checkedAt   time.Time
	mu          sync.Mutex
}

func (s *orderService) refreshOnce() {
	status, _, raw, err := httpRequest("GET", s.supervisor+"/internal/leader", nil, nil, 300*time.Millisecond)
	if err != nil || status != 200 {
		return
	}
	body, _ := decodeJSON(raw)
	parsed := asMap(body)
	leader := asString(parsed["leader"])
	ready := false
	if leader != "" {
		probe, _, _, probeErr := httpRequest("GET", leader+"/ready", nil, nil, 300*time.Millisecond)
		ready = probeErr == nil && probe == 200
	}
	s.mu.Lock()
	s.leader = leader
	if epoch := asInt(parsed["epoch"]); epoch != 0 {
		s.epoch = epoch
	}
	s.leaderOK = ready
	s.checkedAt = time.Now()
	s.mu.Unlock()
}

func (s *orderService) refreshLoop() {
	for {
		s.refreshOnce()
		time.Sleep(heartbeat)
	}
}

func (s *orderService) currentLeader() (string, int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fresh := !s.checkedAt.IsZero() && time.Since(s.checkedAt) < time.Second && s.leader != ""
	return s.leader, s.epoch, fresh && s.leaderOK
}

func storeCall(method, rawURL, requestID string, deadline int64, body any) (int, any, error) {
	remaining := deadline - nowMS()
	if remaining <= 0 {
		return 504, nil, nil
	}
	timeout := remaining
	if timeout > storeCallCapMs {
		timeout = storeCallCapMs
	}
	status, _, raw, err := httpRequest(method, rawURL, body, map[string]string{
		"X-Request-Id": requestID, "X-Deadline-Ms": strconv.FormatInt(deadline, 10),
	}, time.Duration(timeout)*time.Millisecond)
	if err != nil {
		return 0, nil, err
	}
	parsed, _ := decodeJSON(raw)
	return status, parsed, nil
}

func proxyRead(service *orderService, route string, query, headers map[string]string, requestID string) response {
	caller := headers["x-caller-id"]
	if !validCaller(caller) {
		return jsonResponse(400, errorBody("invalid_argument", "X-Caller-Id 不合法", requestID), nil)
	}
	leader, _, ok := service.currentLeader()
	deadline := headerMS(headers, "x-deadline-ms")
	if deadline == 0 {
		deadline = nowMS() + orderDeadlineMs
	}
	if !ok {
		return jsonResponse(503, errorBody("unavailable", "主存储不可用", requestID), nil)
	}
	var rawURL string
	if route == "/internal/orders" {
		rawURL = leader + "/internal/orders/by-key?caller_id=" + queryEscape(caller) + "&idempotency_key=" + queryEscape(query["idempotency_key"])
	} else {
		orderID := route[stringsLast(route):]
		rawURL = leader + "/internal/orders/" + orderID + "?caller_id=" + queryEscape(caller)
	}
	status, body, err := storeCall("GET", rawURL, requestID, deadline, nil)
	if err != nil {
		service.metrics.inc("orange_errors_total", 1)
		return jsonResponse(503, errorBody("unavailable", "主存储不可用", requestID), nil)
	}
	if status == 504 || body == nil {
		return jsonResponse(504, errorBody("deadline_exceeded", "超过截止时间", requestID), nil)
	}
	if object := asMap(body); object != nil {
		delete(object, "request_hash")
		return jsonResponse(status, object, nil)
	}
	return jsonResponse(status, body, nil)
}

func stringsLast(route string) int {
	for i := len(route) - 1; i >= 0; i-- {
		if route[i] == '/' {
			return i + 1
		}
	}
	return 0
}

func headerMS(headers map[string]string, key string) int64 {
	value, _ := strconv.ParseInt(headers[key], 10, 64)
	return value
}

func createAccepted(service *orderService, headers map[string]string, raw []byte, requestID string) response {
	caller := headers["x-caller-id"]
	key := headers["idempotency-key"]
	deadline := headerMS(headers, "x-deadline-ms")
	if !validCaller(caller) {
		return jsonResponse(400, errorBody("invalid_argument", "X-Caller-Id 不合法", requestID), nil)
	}
	if !validIdempotencyKey(key) {
		return jsonResponse(400, errorBody("invalid_argument", "Idempotency-Key 不合法", requestID), nil)
	}
	if deadline != 0 && nowMS() >= deadline {
		logEvent("deadline_exceeded", requestID, "info", nil)
		return jsonResponse(504, errorBody("deadline_exceeded", "超过截止时间", requestID), nil)
	}
	parsed, err := decodeJSON(raw)
	if err != nil {
		return jsonResponse(400, errorBody("invalid_argument", "正文不是 JSON", requestID), nil)
	}
	fields, message := validateOrderBody(parsed)
	if fields == nil {
		return jsonResponse(400, errorBody("invalid_argument", message, requestID), nil)
	}
	service.metrics.inc("orange_inflight", 1)
	defer service.metrics.inc("orange_inflight", -1)
	if replay := lookupExisting(service, caller, key, fields, requestID, deadline); replay.status != 0 {
		return replay
	}
	return writeNew(service, caller, key, fields, requestID, deadline)
}

func lookupExisting(service *orderService, caller, key string, fields map[string]any, requestID string, deadline int64) response {
	leader, _, ok := service.currentLeader()
	if !ok {
		return jsonResponse(503, errorBody("unavailable", "主存储不可用", requestID), nil)
	}
	if deadline == 0 {
		deadline = nowMS() + orderDeadlineMs
	}
	rawURL := leader + "/internal/orders/by-key?caller_id=" + queryEscape(caller) + "&idempotency_key=" + queryEscape(key)
	status, body, err := storeCall("GET", rawURL, requestID, deadline, nil)
	if err != nil {
		return response{}
	}
	object := asMap(body)
	if status == 200 && object["order_id"] != nil {
		digest := canonicalBody(asString(fields["sku"]), asInt(fields["qty"]), fields["external_ref"], fields["fulfill_url"])
		if asString(object["request_hash"]) != digest {
			return jsonResponse(409, errorBody("idempotency_mismatch", "幂等键与已有订单正文不一致", requestID), nil)
		}
		delete(object, "request_hash")
		logEvent("order_replayed", requestID, "info", map[string]any{"order_id": object["order_id"]})
		return jsonResponse(201, object, nil)
	}
	return response{}
}

func writeNew(service *orderService, caller, key string, fields map[string]any, requestID string, deadline int64) response {
	started := time.Now()
	var recommendText string
	var recommendMu sync.Mutex
	done := make(chan struct{})
	go func() {
		defer close(done)
		if !service.breaker.allow() {
			logEvent("breaker_open", requestID, "info", nil)
			service.metrics.set("orange_breaker_open", 1)
			return
		}
		status, _, raw, err := httpRequest("GET", service.recommender+"/internal/recommend?sku="+queryEscape(asString(fields["sku"])), nil, map[string]string{"X-Request-Id": requestID}, time.Duration(recommendBudgetMs)*time.Millisecond)
		if err != nil {
			service.breaker.record(false)
			logEvent("recommend_failed", requestID, "info", map[string]any{"message": err.Error()})
			return
		}
		if status != 200 {
			service.breaker.record(false)
			logEvent("recommend_failed", requestID, "info", map[string]any{"status": status})
			return
		}
		body, _ := decodeJSON(raw)
		text := asString(asMap(body)["recommendation"])
		if text == "" {
			service.breaker.record(false)
			logEvent("recommend_failed", requestID, "info", map[string]any{"status": status})
			return
		}
		service.breaker.record(true)
		recommendMu.Lock()
		recommendText = text
		recommendMu.Unlock()
		service.metrics.set("orange_breaker_open", 0)
	}()
	stored, failed := callAppend(service, caller, key, fields, requestID, deadline)
	if failed.status != 0 {
		<-done
		return failed
	}
	remaining := time.Duration(recommendBudgetMs)*time.Millisecond - time.Since(started)
	if remaining > 0 {
		select {
		case <-done:
		case <-time.After(remaining):
		}
	}
	recommendMu.Lock()
	text := recommendText
	recommendMu.Unlock()
	if text == "" {
		logEvent("order_accepted", requestID, "info", map[string]any{"order_id": stored["order_id"], "epoch": stored["epoch"], "degraded": true})
		return jsonResponse(201, stored, nil)
	}
	leader, epoch, ok := service.currentLeader()
	if !ok {
		return jsonResponse(201, stored, nil)
	}
	snapDeadline := deadline
	if snapDeadline == 0 {
		snapDeadline = nowMS() + 200
	}
	status, body, err := storeCall("POST", leader+"/internal/snapshot", requestID, snapDeadline, map[string]any{
		"order_id": stored["order_id"], "epoch": epoch, "recommendation": text, "degraded": false,
	})
	if err == nil && status == 200 {
		if object := asMap(body); object != nil {
			logEvent("order_accepted", requestID, "info", map[string]any{"order_id": object["order_id"], "epoch": object["epoch"], "degraded": false})
			return jsonResponse(201, object, nil)
		}
	}
	return jsonResponse(201, stored, nil)
}

func callAppend(service *orderService, caller, key string, fields map[string]any, requestID string, deadline int64) (map[string]any, response) {
	last := jsonResponse(503, errorBody("unavailable", "主存储不可用", requestID), nil)
	for range 2 {
		leader, epoch, ok := service.currentLeader()
		if !ok {
			service.refreshOnce()
			leader, epoch, ok = service.currentLeader()
		}
		if !ok {
			return nil, last
		}
		if deadline == 0 {
			deadline = nowMS() + orderDeadlineMs
		}
		status, body, err := storeCall("POST", leader+"/internal/append", requestID, deadline, map[string]any{
			"epoch": epoch, "caller_id": caller, "idempotency_key": key, "order": fields,
		})
		if err != nil {
			service.metrics.inc("orange_errors_total", 1)
			logEvent("deadline_exceeded", requestID, "info", nil)
			return nil, jsonResponse(504, errorBody("deadline_exceeded", "超过截止时间", requestID), nil)
		}
		object := asMap(body)
		if status == 409 && asString(asMap(object["error"])["code"]) == "epoch_fenced" {
			logEvent("write_fenced", requestID, "info", map[string]any{"epoch": asMap(object["error"])["epoch"]})
			service.refreshOnce()
			continue
		}
		if status == 409 {
			return nil, jsonResponse(409, body, nil)
		}
		if status == 200 && object["order"] != nil {
			order := asMap(object["order"])
			if !asBool(object["created"]) {
				logEvent("order_replayed", requestID, "info", map[string]any{"order_id": order["order_id"]})
			}
			return order, response{}
		}
		if body == nil {
			body = errorBody("unavailable", "主存储不可用", requestID)
		}
		code := status
		if code == 0 {
			code = 503
		}
		return nil, jsonResponse(code, body, nil)
	}
	return nil, jsonResponse(503, errorBody("unavailable", "主存储正在切换", requestID), nil)
}

func orderHandler(service *orderService) handlerFunc {
	return func(method, path string, headers map[string]string, raw []byte) response {
		requestID := headerRequestID(headers)
		route, query := splitPath(path)
		switch {
		case method == "GET" && route == "/health":
			return emptyResponse(200)
		case method == "GET" && route == "/ready":
			_, _, ok := service.currentLeader()
			if ok {
				return emptyResponse(200)
			}
			return emptyResponse(503)
		case method == "GET" && route == "/metrics":
			open := 0
			if service.breaker.open() {
				open = 1
			}
			service.metrics.set("orange_breaker_open", int64(open))
			return textResponse(200, service.metrics.render([]string{"orange_errors_total", "orange_inflight", "orange_breaker_open"}))
		case method == "GET" && (route == "/internal/orders" || len(route) > len("/internal/orders/") && route[:len("/internal/orders/")] == "/internal/orders/"):
			return proxyRead(service, route, query, headers, requestID)
		case method == "POST" && route == "/internal/orders":
			return createAccepted(service, headers, raw, requestID)
		default:
			return jsonResponse(404, errorBody("not_found", "没有这个路径", requestID), nil)
		}
	}
}

func runOrder(args []string) {
	flags := newFlagSet()
	name := flags.String("process-name", "", "")
	port := flags.Int("port", 0, "")
	supervisor := flags.String("supervisor", "", "")
	recommender := flags.String("recommender", "", "")
	root := flags.String("root", ".", "")
	parseFlags(flags, args)
	setContext(*name, *root)
	startFaultReader()
	service := &orderService{name: *name, supervisor: *supervisor, recommender: *recommender, breaker: newBreaker(), metrics: newMetrics(), epoch: 1}
	go service.refreshLoop()
	logEvent("process_started", "", "info", map[string]any{"port": *port})
	if err := serve(*port, orderHandler(service)); err != nil {
		panic(err)
	}
}
