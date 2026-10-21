package acceptance

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const gateway = "http://127.0.0.1:8080"

func TestMain(m *testing.M) {
	if os.Getenv("ORANGE_BIN") == "" {
		fmt.Fprintln(os.Stderr, "缺少 ORANGE_BIN")
		os.Exit(2)
	}
	os.Exit(m.Run())
}

func TestAcceptance(t *testing.T) {
	tests := []struct {
		name string
		fn   func(*testing.T, *cluster)
	}{
		{"AC-1", ac1}, {"AC-2", ac2}, {"AC-3", ac3}, {"AC-4", ac4}, {"AC-5", ac5},
		{"AC-6", ac6}, {"AC-7", ac7}, {"AC-8", ac8}, {"AC-9", ac9}, {"AC-10", ac10},
		{"AC-11", ac11}, {"AC-12", ac12},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			item := startCluster(t)
			test.fn(t, item)
		})
	}
}

func ac1(t *testing.T, cluster *cluster) {
	first := post(t, "gear-1", "same-key-0001", nil)
	second := post(t, "gear-1", "same-key-0001", nil)
	if first.status != 201 || second.status != 201 {
		t.Fatalf("%d %s %d %s %s", first.status, first.raw, second.status, second.raw, cluster.diagnostics())
	}
	if text(first.body, "order_id") != text(second.body, "order_id") {
		t.Fatalf("id %s %s", first.raw, second.raw)
	}
}

func ac2(t *testing.T, cluster *cluster) {
	first := post(t, "gear-1", "same-key-0002", nil)
	if first.status != 201 {
		t.Fatal(first.raw, cluster.diagnostics())
	}
	second := post(t, "gear-2", "same-key-0002", nil)
	if second.status != 409 || text(asMap(second.body["error"]), "code") != "idempotency_mismatch" {
		t.Fatal(second.raw)
	}
	got := request(t, "GET", gateway+"/v1/orders/"+text(first.body, "order_id"), nil, map[string]string{"X-Caller-Id": "gear-card"}, 2*time.Second)
	if got.status != 200 || text(got.body, "sku") != "gear-1" {
		t.Fatal(got.raw, cluster.diagnostics())
	}
}

func ac3(t *testing.T, cluster *cluster) {
	pid := int(number(asMap(cluster.status()["pids"])["order-2"]))
	_ = syscall.Kill(pid, syscall.SIGTERM)
	sawDown, restored := false, false
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		view := cluster.status()
		ready := asBool(asMap(view["ready"])["order-2"])
		sawDown = sawDown || !ready
		next := int(number(asMap(view["pids"])["order-2"]))
		if ready && next != pid {
			restored = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	body := post(t, "gear-1", "after-order-b", nil)
	if body.status != 201 || !sawDown || !restored {
		t.Fatal(body.raw, sawDown, restored, cluster.diagnostics())
	}
}

func ac4(t *testing.T, cluster *cluster) {
	pid := int(number(asMap(cluster.status()["pids"])["recommender"]))
	_ = syscall.Kill(pid, syscall.SIGTERM)
	var body result
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		body = post(t, "gear-1", "degrade-"+strconv.FormatInt(time.Now().UnixNano(), 10), nil)
		if body.status == 201 && asBool(body.body["degraded"]) && body.body["recommendation"] == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if body.status != 201 || !asBool(body.body["degraded"]) || body.body["recommendation"] != nil {
		t.Fatal(body.raw, cluster.diagnostics())
	}
}

func ac5(t *testing.T, cluster *cluster) {
	cluster.faults(0, 500, false)
	for i := 0; i < 12; i++ {
		body := post(t, "gear-1", fmt.Sprintf("breaker-%02d-key", i), nil)
		if body.status != 201 {
			t.Fatal(body.raw, cluster.diagnostics())
		}
	}
	time.Sleep(300 * time.Millisecond)
	before := strings.Count(cluster.logs("recommender"), `"event":"recommend_served"`)
	afterOrder := post(t, "gear-1", "breaker-after-open", nil)
	if afterOrder.status != 201 {
		t.Fatal(afterOrder.raw)
	}
	time.Sleep(300 * time.Millisecond)
	after := strings.Count(cluster.logs("recommender"), `"event":"recommend_served"`)
	if before != after {
		t.Fatal(cluster.logs("recommender"))
	}
	if !strings.Contains(cluster.logs("order-1")+cluster.logs("order-2"), `"event":"breaker_open"`) {
		t.Fatal(cluster.logs("order-1"), cluster.logs("order-2"))
	}
}

func ac6(t *testing.T, cluster *cluster) {
	cluster.faults(350, 0, false)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			post(t, "gear-1", fmt.Sprintf("hold-%02d-xxxx", index), nil)
		}(i)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		metrics := request(t, "GET", gateway+"/metrics", nil, nil, time.Second)
		if strings.Contains(metrics.raw, "orange_inflight 8") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	results := make([]result, 40)
	var burst sync.WaitGroup
	for i := 0; i < 40; i++ {
		burst.Add(1)
		go func(index int) {
			defer burst.Done()
			results[index] = post(t, "gear-1", fmt.Sprintf("bucket-%02d-xx", index), nil)
		}(i)
	}
	burst.Wait()
	wg.Wait()
	limited, retry := false, false
	summary := ""
	for _, item := range results {
		summary += strconv.Itoa(item.status) + " " + text(asMap(item.body["error"]), "message") + "; "
		if item.status == 429 && text(asMap(item.body["error"]), "message") == "令牌桶已空" {
			limited = true
			_, retry = item.headers["Retry-After"]
		}
	}
	if !limited || !retry {
		t.Fatal(summary)
	}
}

func ac7(t *testing.T, cluster *cluster) {
	fulfill := startFulfill(t, 200, 0, true)
	body := post(t, "gear-1", "visibility-timeout", map[string]any{"fulfill_url": fulfill.url})
	if body.status != 201 {
		t.Fatal(body.raw, cluster.diagnostics())
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && fulfill.count() == 0 {
		time.Sleep(50 * time.Millisecond)
	}
	if fulfill.count() == 0 {
		t.Fatal(cluster.diagnostics())
	}
	pid := int(number(asMap(cluster.status()["pids"])["worker-1"]))
	_ = syscall.Kill(pid, syscall.SIGTERM)
	fulfill.release()
	deadline = time.Now().Add(25 * time.Second)
	state := ""
	for time.Now().Before(deadline) {
		got := request(t, "GET", gateway+"/v1/orders/"+text(body.body, "order_id"), nil, map[string]string{"X-Caller-Id": "gear-card"}, 2*time.Second)
		if got.status != 200 {
			t.Fatal(got.raw, cluster.diagnostics())
		}
		state = text(got.body, "state")
		if state == "fulfilled" {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if state != "fulfilled" || fulfill.appliedCount() != 1 {
		t.Fatal(state, fulfill.appliedCount(), cluster.diagnostics())
	}
}

func ac8(t *testing.T, cluster *cluster) {
	fulfill := startFulfill(t, 500, 0, false)
	body := post(t, "gear-1", "dead-letter-0001", map[string]any{"fulfill_url": fulfill.url})
	if body.status != 201 {
		t.Fatal(body.raw)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		got := request(t, "GET", gateway+"/v1/orders/"+text(body.body, "order_id"), nil, map[string]string{"X-Caller-Id": "gear-card"}, 2*time.Second)
		if text(got.body, "state") == "fulfillment_failed" {
			fulfillment := asMap(got.body["fulfillment"])
			if text(fulfillment, "status") != "dead" || number(fulfillment["attempts"]) < 5 {
				t.Fatal(got.raw)
			}
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal(cluster.diagnostics())
}

func ac9(t *testing.T, cluster *cluster) {
	original := text(cluster.status(), "leader")
	cluster.faults(0, 0, true)
	promoted := false
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		view := cluster.status()
		if text(view, "leader") != original && number(view["epoch"]) > 1 {
			promoted = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !promoted {
		t.Fatal(cluster.diagnostics())
	}
	cluster.faults(0, 0, false)
	fenced := false
	deadline = time.Now().Add(5 * time.Second)
	payload := map[string]any{"epoch": 1, "caller_id": "gear-card", "idempotency_key": "fenced-write-01", "order": map[string]any{"sku": "gear-1", "qty": 1, "external_ref": nil, "fulfill_url": nil}}
	for time.Now().Before(deadline) {
		result := request(t, "POST", original+"/internal/append", payload, nil, time.Second)
		if result.status == 409 && text(asMap(result.body["error"]), "code") == "epoch_fenced" {
			fenced = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !fenced {
		t.Fatal(cluster.diagnostics())
	}
	created := post(t, "gear-1", "after-promotion", nil)
	if created.status != 201 || number(created.body["epoch"]) <= 1 {
		t.Fatal(created.raw, cluster.diagnostics())
	}
}

func ac10(t *testing.T, cluster *cluster) {
	fulfill := startFulfill(t, 500, 900*time.Millisecond, false)
	limited, created := false, false
	for i := 0; i < 70; i++ {
		body := post(t, "gear-1", fmt.Sprintf("pile-%02d-order", i), map[string]any{"fulfill_url": fulfill.url})
		if body.status == 201 {
			created = true
			continue
		}
		message := text(asMap(body.body["error"]), "message")
		if body.status == 429 && message == "待处理任务已达上限" {
			limited = true
			break
		}
		if body.status == 429 && message == "令牌桶已空" {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		t.Fatal(body.status, body.raw, cluster.diagnostics())
	}
	if !limited || !created {
		t.Fatal(cluster.diagnostics())
	}
	before := strings.Count(cluster.logs("store-a"), `"event":"job_claimed"`)
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Count(cluster.logs("store-a"), `"event":"job_claimed"`) > before {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal(cluster.diagnostics())
}

func ac11(t *testing.T, cluster *cluster) {
	fulfill := startFulfill(t, 500, 900*time.Millisecond, false)
	for i := 0; i < 40; i++ {
		for {
			body := post(t, "gear-1", fmt.Sprintf("scale-%02d-order", i), map[string]any{"fulfill_url": fulfill.url})
			if body.status == 429 && text(asMap(body.body["error"]), "message") == "令牌桶已空" {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			if body.status != 201 {
				t.Fatal(body.raw, cluster.diagnostics())
			}
			break
		}
	}
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) {
		if number(cluster.status()["workers"]) >= 3 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if number(cluster.status()["workers"]) < 3 {
		t.Fatal(cluster.diagnostics())
	}
	time.Sleep(10 * time.Second)
	if number(cluster.status()["workers"]) != 3 {
		t.Fatal(cluster.diagnostics())
	}
}

func ac12(t *testing.T, cluster *cluster) {
	cluster.faults(200, 0, false)
	requestID := "req-trace-ac12-0001"
	result := request(t, "POST", gateway+"/v1/orders", map[string]any{"sku": "gear-1", "qty": 1}, map[string]string{
		"X-Caller-Id": "gear-card", "Idempotency-Key": "trace-request-001", "X-Request-Id": requestID,
	}, 3*time.Second)
	if result.status != 201 {
		t.Fatal(result.raw)
	}
	time.Sleep(300 * time.Millisecond)
	if !strings.Contains(cluster.logs("gateway"), requestID) {
		t.Fatal(cluster.logs("gateway"))
	}
	if !strings.Contains(cluster.logs("order-1")+cluster.logs("order-2"), requestID) {
		t.Fatal("orders")
	}
	if !strings.Contains(cluster.logs("store-a")+cluster.logs("store-b"), requestID) {
		t.Fatal("stores")
	}
}

type result struct {
	status  int
	headers map[string]string
	body    map[string]any
	raw     string
}

func post(t *testing.T, sku, key string, extra map[string]any) result {
	t.Helper()
	body := map[string]any{"sku": sku, "qty": 1}
	for key, value := range extra {
		body[key] = value
	}
	return request(t, "POST", gateway+"/v1/orders", body, map[string]string{
		"X-Caller-Id": "gear-card", "Idempotency-Key": key, "X-Request-Id": "req-" + strconv.FormatInt(time.Now().UnixNano(), 10),
	}, 3*time.Second)
}

func request(t *testing.T, method, rawURL string, body any, headers map[string]string, timeout time.Duration) result {
	t.Helper()
	var payload io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		payload = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, rawURL, payload)
	if err != nil {
		t.Fatal(err)
	}
	req.Close = true
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return result{raw: err.Error()}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := result{status: resp.StatusCode, headers: map[string]string{}, raw: string(raw)}
	for key, values := range resp.Header {
		if len(values) > 0 {
			out.headers[key] = values[0]
		}
	}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &out.body)
	}
	if out.body == nil {
		out.body = map[string]any{}
	}
	return out
}

func text(node map[string]any, key string) string {
	value, _ := node[key].(string)
	return value
}

func number(value any) float64 {
	switch n := value.(type) {
	case float64:
		return n
	case json.Number:
		f, _ := n.Float64()
		return f
	default:
		return 0
	}
}

func asMap(value any) map[string]any {
	m, _ := value.(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}

func asBool(value any) bool {
	flag, _ := value.(bool)
	return flag
}

type cluster struct {
	t    *testing.T
	root string
	cmd  *exec.Cmd
}

func startCluster(t *testing.T) *cluster {
	t.Helper()
	root, err := os.MkdirTemp("", "orange-go-")
	if err != nil {
		t.Fatal(err)
	}
	_ = os.MkdirAll(filepath.Join(root, "runtime"), 0o755)
	logFile, err := os.Create(filepath.Join(root, "runtime", "supervisor.out"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Getenv("ORANGE_BIN"), "supervisor", "--root", root)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	item := &cluster{t: t, root: root, cmd: cmd}
	t.Cleanup(func() {
		item.stop()
		_ = logFile.Close()
	})
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		ready := request(t, "GET", gateway+"/ready", nil, nil, time.Second)
		if ready.status == 200 {
			return item
		}
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	text := item.diagnostics()
	item.stop()
	t.Fatal(text)
	return item
}

func (c *cluster) status() map[string]any {
	result := request(c.t, "GET", "http://127.0.0.1:8090/v1/status", nil, nil, time.Second)
	return result.body
}

func (c *cluster) faults(storeDelay, recommendDelay int, dropA bool) {
	payload := map[string]any{
		"delay_ms":            map[string]any{"store": storeDelay, "recommender": recommendDelay},
		"pause_replication":   false,
		"drop_heartbeat":      map[string]any{"store-a": dropA, "store-b": false},
		"exit_after_requests": map[string]any{},
	}
	raw, _ := json.Marshal(payload)
	if err := os.WriteFile(filepath.Join(c.root, "runtime", "faults.json"), raw, 0o644); err != nil {
		c.t.Fatal(err)
	}
	time.Sleep(600 * time.Millisecond)
}

func (c *cluster) logs(name string) string {
	raw, err := os.ReadFile(filepath.Join(c.root, "runtime", "logs", name+".log"))
	if err != nil {
		return ""
	}
	return string(raw)
}

func (c *cluster) diagnostics() string {
	var b strings.Builder
	if raw, err := os.ReadFile(filepath.Join(c.root, "runtime", "supervisor.out")); err == nil {
		b.Write(raw)
	}
	directory := filepath.Join(c.root, "runtime", "logs")
	entries, _ := os.ReadDir(directory)
	for _, entry := range entries {
		b.WriteString("----- " + entry.Name() + " -----\n")
		raw, _ := os.ReadFile(filepath.Join(directory, entry.Name()))
		b.Write(raw)
		b.WriteByte('\n')
	}
	if b.Len() == 0 {
		return "监督者没有留下日志"
	}
	return b.String()
}

func (c *cluster) stop() {
	if c.cmd == nil || c.cmd.Process == nil {
		os.RemoveAll(c.root)
		return
	}
	_ = syscall.Kill(-c.cmd.Process.Pid, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		_, _ = c.cmd.Process.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		_ = syscall.Kill(-c.cmd.Process.Pid, syscall.SIGKILL)
		<-done
	}
	time.Sleep(300 * time.Millisecond)
	_ = os.RemoveAll(c.root)
	c.cmd = nil
}

type fulfillServer struct {
	url      string
	server   *http.Server
	listener net.Listener
	mu       sync.Mutex
	calls    []string
	applied  []string
	releaseC chan struct{}
	once     sync.Once
}

func startFulfill(t *testing.T, status int, delay time.Duration, hangFirst bool) *fulfillServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	item := &fulfillServer{listener: listener, releaseC: make(chan struct{}), url: "http://" + listener.Addr().String() + "/hooks/orange-fulfill"}
	item.server = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var payload map[string]any
		_ = json.Unmarshal(raw, &payload)
		orderID, _ := payload["order_id"].(string)
		item.mu.Lock()
		first := len(item.calls) == 0
		item.calls = append(item.calls, orderID)
		item.mu.Unlock()
		if hangFirst && first {
			select {
			case <-item.releaseC:
			case <-time.After(30 * time.Second):
			}
		} else if delay > 0 {
			time.Sleep(delay)
		}
		w.Header().Set("Content-Length", "2")
		w.WriteHeader(status)
		_, _ = w.Write([]byte("{}"))
		if (status >= 200 && status < 300) || status == 409 {
			item.mu.Lock()
			if !contains(item.applied, orderID) {
				item.applied = append(item.applied, orderID)
			}
			item.mu.Unlock()
		}
	})}
	go item.server.Serve(listener)
	t.Cleanup(item.close)
	return item
}

func (f *fulfillServer) release() { f.once.Do(func() { close(f.releaseC) }) }

func (f *fulfillServer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fulfillServer) appliedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.applied)
}

func (f *fulfillServer) close() {
	f.release()
	_ = f.server.Close()
}

func contains(items []string, name string) bool {
	for _, item := range items {
		if item == name {
			return true
		}
	}
	return false
}
