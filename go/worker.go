package main

import (
	"strconv"
	"sync"
	"time"
)

type worker struct {
	name       string
	supervisor string
	leader     string
	checkedAt  time.Time
	errors     int64
	mu         sync.Mutex
}

func (w *worker) refresh() {
	for {
		status, _, raw, err := httpRequest("GET", w.supervisor+"/internal/leader", nil, nil, 300*time.Millisecond)
		if err == nil && status == 200 {
			body, _ := decodeJSON(raw)
			w.mu.Lock()
			w.leader = asString(asMap(body)["leader"])
			w.checkedAt = time.Now()
			w.mu.Unlock()
		}
		time.Sleep(heartbeat)
	}
}

func (w *worker) leaderNow() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.leader != "" && time.Since(w.checkedAt) < time.Second {
		return w.leader
	}
	return ""
}

func (w *worker) run() {
	for {
		leader := w.leaderNow()
		if leader == "" {
			time.Sleep(heartbeat)
			continue
		}
		requestID := newID("req_")
		status, _, raw, err := httpRequest("POST", leader+"/internal/jobs/claim", map[string]any{"worker_id": w.name}, map[string]string{"X-Request-Id": requestID}, time.Second)
		if err != nil || status != 200 || len(raw) == 0 {
			if status != 204 {
				w.errors++
			}
			time.Sleep(heartbeat)
			continue
		}
		job, _ := decodeJSON(raw)
		object := asMap(job)
		ok, message := w.fulfill(object, requestID)
		path := "/ack"
		var body any
		if !ok {
			path = "/fail"
			body = map[string]any{"retryable": true, "error": message}
		}
		if _, _, _, err := httpRequest("POST", leader+"/internal/jobs/"+asString(object["job_id"])+path, body, map[string]string{"X-Request-Id": requestID}, time.Second); err != nil {
			w.errors++
		}
	}
}

func (w *worker) fulfill(job map[string]any, requestID string) (bool, string) {
	target := asString(job["fulfill_url"])
	if target == "" {
		return true, ""
	}
	status, _, _, err := httpRequest("POST", target, map[string]any{
		"event": "fulfill", "order_id": job["order_id"], "caller_id": job["caller_id"],
		"external_ref": job["external_ref"], "sku": job["sku"], "qty": job["qty"],
	}, map[string]string{"X-Request-Id": requestID}, fulfillTimeout)
	if err != nil {
		return false, err.Error()
	}
	if (status >= 200 && status < 300) || status == 409 {
		return true, ""
	}
	return false, "履约地址返回 " + strconv.Itoa(status)
}

func workerHandler(w *worker) handlerFunc {
	return func(method, path string, headers map[string]string, raw []byte) response {
		route, _ := splitPath(path)
		switch {
		case method == "GET" && route == "/health":
			return emptyResponse(200)
		case method == "GET" && route == "/ready":
			if w.leaderNow() != "" {
				return emptyResponse(200)
			}
			return emptyResponse(503)
		case method == "GET" && route == "/metrics":
			return textResponse(200, "orange_errors_total "+strconv.FormatInt(w.errors, 10)+"\n")
		default:
			return emptyResponse(404)
		}
	}
}

func runWorker(args []string) {
	flags := newFlagSet()
	name := flags.String("process-name", "", "")
	port := flags.Int("port", 0, "")
	supervisor := flags.String("supervisor", "", "")
	root := flags.String("root", ".", "")
	parseFlags(flags, args)
	setContext(*name, *root)
	startFaultReader()
	item := &worker{name: *name, supervisor: *supervisor}
	go item.refresh()
	go item.run()
	logEvent("process_started", "", "info", map[string]any{"port": *port})
	if err := serve(*port, workerHandler(item)); err != nil {
		panic(err)
	}
}
