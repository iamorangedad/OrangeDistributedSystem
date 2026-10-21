package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	mrand "math/rand"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	tokenCapacity        = 30
	tokenRefillPerSec    = 10.0
	gatewayInflightLimit = 8
	jobsPendingLimit     = 50
	orderDeadlineMs      = 800
	storeCallCapMs       = 400
	recommendBudgetMs    = 80
	breakerWindow        = 10 * time.Second
	breakerFailures      = 5
	breakerOpen          = 5 * time.Second
	visibility           = 15 * time.Second
	fulfillTimeout       = time.Second
	jobMaxAttempts       = 5
	backoffBaseMs        = 100
	backoffCapMs         = 5000
	heartbeat            = 200 * time.Millisecond
	leaderLost           = time.Second
	scaleOutPending      = 10
	scaleOutHold         = 5 * time.Second
	scaleInHold          = 20 * time.Second
	scaleCooldown        = 15 * time.Second
	workerMin            = 1
	workerMax            = 3
	promotionPause       = 2 * time.Second
	faultPoll            = 200 * time.Millisecond
)

var callerPattern = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)

var (
	processName = "orange"
	rootDir     = "."
	logMu       sync.Mutex

	faultMu          sync.Mutex
	faults           map[string]any
	faultErrorLogged bool
	requestCount     int
	requestCountMu   sync.Mutex
)

func setContext(process, root string) {
	processName = process
	rootDir = root
}

func nowMS() int64 { return time.Now().UnixMilli() }

func isoNow() string { return time.Now().UTC().Format("2006-01-02T15:04:05Z") }

func newID(prefix string) string {
	buf := make([]byte, 16)
	_, _ = rand.Read(buf)
	return prefix + hex.EncodeToString(buf)
}

func marshal(v any) []byte {
	raw, err := json.Marshal(v)
	if err != nil {
		return []byte("null")
	}
	return raw
}

func decodeJSON(raw []byte) (any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}

func asInt(v any) int64 {
	switch n := v.(type) {
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			f, ferr := n.Float64()
			if ferr != nil {
				return 0
			}
			return int64(f)
		}
		return i
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	default:
		return 0
	}
}

func asBool(v any) bool {
	b, _ := v.(bool)
	return b
}

func logEvent(event, requestID, level string, fields map[string]any) {
	if level == "" {
		level = "info"
	}
	record := map[string]any{
		"ts":         isoNow(),
		"level":      level,
		"process":    processName,
		"event":      event,
		"request_id": requestID,
	}
	for key, value := range fields {
		record[key] = value
	}
	line := string(marshal(record))
	logMu.Lock()
	defer logMu.Unlock()
	fmt.Println(line)
	directory := filepath.Join(rootDir, "runtime", "logs")
	_ = os.MkdirAll(directory, 0o755)
	file, err := os.OpenFile(filepath.Join(directory, processName+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	_, _ = file.WriteString(line + "\n")
	_ = file.Close()
}

func faultsPath() string { return filepath.Join(rootDir, "runtime", "faults.json") }

func currentFaults() map[string]any {
	faultMu.Lock()
	defer faultMu.Unlock()
	raw := marshal(faults)
	parsed, err := decodeJSON(raw)
	if err != nil || parsed == nil {
		return map[string]any{}
	}
	if m, ok := parsed.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}

func loadFaultsOnce() {
	raw, err := os.ReadFile(faultsPath())
	if err != nil {
		faultMu.Lock()
		faults = map[string]any{}
		if !faultErrorLogged {
			faultErrorLogged = true
			faultMu.Unlock()
			logEvent("faults_invalid", "", "error", map[string]any{"message": "故障文件不存在"})
			return
		}
		faultMu.Unlock()
		return
	}
	parsed, err := decodeJSON(raw)
	if err != nil {
		noteFaultError(err.Error())
		return
	}
	object, ok := parsed.(map[string]any)
	if !ok {
		noteFaultError("faults 必须是对象")
		return
	}
	faultMu.Lock()
	faults = object
	faultErrorLogged = false
	faultMu.Unlock()
}

func noteFaultError(message string) {
	faultMu.Lock()
	faults = map[string]any{}
	logged := faultErrorLogged
	faultErrorLogged = true
	faultMu.Unlock()
	if !logged {
		logEvent("faults_invalid", "", "error", map[string]any{"message": message})
	}
}

func startFaultReader() {
	loadFaultsOnce()
	go func() {
		for {
			time.Sleep(faultPoll)
			loadFaultsOnce()
		}
	}()
}

func faultDelay(kind string) {
	delay := asMap(currentFaults()["delay_ms"])
	ms := asInt(delay[kind])
	if ms > 0 {
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func pauseReplication() bool { return asBool(currentFaults()["pause_replication"]) }

func dropHeartbeat(nodeID string) bool {
	dropped := asMap(currentFaults()["drop_heartbeat"])
	return asBool(dropped[nodeID])
}

func noteHTTPRequest() {
	limits := asMap(currentFaults()["exit_after_requests"])
	limit := asInt(limits[processName])
	if limit <= 0 {
		return
	}
	requestCountMu.Lock()
	requestCount++
	hit := requestCount >= int(limit)
	requestCountMu.Unlock()
	if hit {
		logEvent("exit_after_requests", "", "info", map[string]any{"limit": limit})
		os.Exit(0)
	}
}

type tokenBucket struct {
	capacity float64
	rate     float64
	tokens   float64
	updated  time.Time
	mu       sync.Mutex
}

func newTokenBucket(capacity, rate float64) *tokenBucket {
	return &tokenBucket{capacity: capacity, rate: rate, tokens: capacity, updated: time.Now()}
}

func (b *tokenBucket) take() (bool, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(b.updated).Seconds()
	b.tokens = math.Min(b.capacity, b.tokens+elapsed*b.rate)
	b.updated = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	if b.rate <= 0 {
		return false, 1
	}
	retry := int(math.Ceil((1 - b.tokens) / b.rate))
	if retry < 1 {
		retry = 1
	}
	return false, retry
}

type circuitBreaker struct {
	mu        sync.Mutex
	failures  []time.Time
	state     string
	openUntil time.Time
}

func newBreaker() *circuitBreaker { return &circuitBreaker{state: "closed"} }

func (b *circuitBreaker) open() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state == "open" && time.Now().Before(b.openUntil)
}

func (b *circuitBreaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	if b.state == "open" {
		if now.Before(b.openUntil) {
			return false
		}
		b.state = "half"
		return true
	}
	if b.state == "half" {
		return false
	}
	return true
}

func (b *circuitBreaker) record(success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	if b.state == "half" {
		if success {
			b.state = "closed"
			b.failures = nil
		} else {
			b.state = "open"
			b.openUntil = now.Add(breakerOpen)
		}
		return
	}
	if success {
		return
	}
	kept := b.failures[:0]
	for _, item := range b.failures {
		if now.Sub(item) <= breakerWindow {
			kept = append(kept, item)
		}
	}
	b.failures = append(kept, now)
	if len(b.failures) >= breakerFailures {
		b.state = "open"
		b.openUntil = now.Add(breakerOpen)
		b.failures = nil
	}
}

func backoffMS(attempt int) int {
	shift := attempt - 1
	if shift < 0 {
		shift = 0
	}
	if shift > 16 {
		shift = 16
	}
	cap := backoffBaseMs << shift
	if cap > backoffCapMs || cap <= 0 {
		cap = backoffCapMs
	}
	return mrand.Intn(cap + 1)
}

func canonicalBody(sku string, qty int64, externalRef, fulfillURL any) string {
	external := ""
	if text, ok := externalRef.(string); ok {
		external = text
	}
	fulfill := ""
	if text, ok := fulfillURL.(string); ok {
		fulfill = text
	}
	payload := struct {
		ExternalRef string `json:"external_ref"`
		FulfillURL  string `json:"fulfill_url"`
		Qty         int64  `json:"qty"`
		SKU         string `json:"sku"`
	}{external, fulfill, qty, sku}
	return string(marshal(payload))
}

func validCaller(value string) bool { return callerPattern.MatchString(value) }

func validIdempotencyKey(value string) bool {
	if len(value) < 8 || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if char < 32 || char > 126 {
			return false
		}
	}
	return true
}

func validFulfillURL(value any) bool {
	if value == nil {
		return true
	}
	text, ok := value.(string)
	if !ok {
		return false
	}
	if text == "" {
		return true
	}
	return fulfillURLText(text)
}

func fulfillURLText(text string) bool {
	if !strings.HasPrefix(text, "http://") {
		return false
	}
	rest := strings.TrimPrefix(text, "http://")
	slash := strings.IndexByte(rest, '/')
	authority := rest
	if slash >= 0 {
		authority = rest[:slash]
	}
	if strings.Contains(authority, "@") || authority == "" {
		return false
	}
	colon := strings.LastIndexByte(authority, ':')
	if colon <= 0 {
		return false
	}
	host := authority[:colon]
	port := authority[colon+1:]
	if host != "127.0.0.1" || port == "" || port == "0" {
		return false
	}
	for _, char := range port {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func validateOrderBody(data any) (map[string]any, string) {
	object, ok := data.(map[string]any)
	if !ok {
		return nil, "正文必须是 JSON 对象"
	}
	sku, skuOK := object["sku"].(string)
	if !skuOK || sku == "" || len(sku) > 64 {
		return nil, "sku 必须是 1 到 64 个字符"
	}
	if _, isBool := object["qty"].(bool); isBool {
		return nil, "qty 必须是 1 到 100 的整数"
	}
	number, isNumber := object["qty"].(json.Number)
	if !isNumber || strings.ContainsAny(number.String(), ".eE") {
		return nil, "qty 必须是 1 到 100 的整数"
	}
	qty, err := number.Int64()
	if err != nil || qty < 1 || qty > 100 {
		return nil, "qty 必须是 1 到 100 的整数"
	}
	external := object["external_ref"]
	if external != nil {
		text, isString := external.(string)
		if !isString || len(text) > 128 {
			return nil, "external_ref 最长 128 个字符"
		}
	}
	fulfill := object["fulfill_url"]
	if fulfill != nil {
		if _, isString := fulfill.(string); !isString {
			return nil, "fulfill_url 必须是字符串"
		}
	}
	if !validFulfillURL(fulfill) {
		return nil, "fulfill_url 只能是 http://127.0.0.1 上的地址"
	}
	var fulfillValue any
	if text, isString := fulfill.(string); isString && text != "" {
		fulfillValue = text
	}
	return map[string]any{
		"sku":          sku,
		"qty":          qty,
		"external_ref": external,
		"fulfill_url":  fulfillValue,
	}, ""
}

type metrics struct {
	mu     sync.Mutex
	values map[string]int64
}

func newMetrics() *metrics { return &metrics{values: map[string]int64{}} }

func (m *metrics) inc(name string, amount int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values[name] += amount
}

func (m *metrics) set(name string, value int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values[name] = value
}

func (m *metrics) render(names []string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var b strings.Builder
	for _, name := range names {
		fmt.Fprintf(&b, "%s %d\n", name, m.values[name])
	}
	return b.String()
}

func errorBody(code, message, requestID string) map[string]any {
	return map[string]any{"error": map[string]any{
		"code":       code,
		"message":    message,
		"request_id": requestID,
	}}
}
