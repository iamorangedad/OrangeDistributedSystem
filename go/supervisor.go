package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const (
	storeA        = "http://127.0.0.1:8301"
	storeB        = "http://127.0.0.1:8302"
	supervisorURL = "http://127.0.0.1:8090"
)

type childProc struct {
	cmd  *exec.Cmd
	done chan struct{}
	code int
}

func (c *childProc) finished() (bool, int) {
	select {
	case <-c.done:
		return true, c.code
	default:
		return false, 0
	}
}

type supervisor struct {
	root               string
	exe                string
	mu                 sync.Mutex
	procs              map[string]*childProc
	leader             string
	epoch              int64
	jobsPending        int64
	gatewayPaused      bool
	lastPromotionError any
	applied            map[string]int64
	lastBeat           map[string]time.Time
	seen               map[string]bool
	ready              map[string]bool
	pids               map[string]int
	lastError          map[string]string
	expected           []string
	workerCount        int
	highSince          time.Time
	zeroSince          time.Time
	highSet            bool
	zeroSet            bool
	lastScale          time.Time
	promoting          bool
	armed              bool
}

func newSupervisor(root, exe string) *supervisor {
	return &supervisor{
		root: root, exe: exe, procs: map[string]*childProc{}, leader: storeA, epoch: 1,
		applied: map[string]int64{"store-a": 0, "store-b": 0}, lastBeat: map[string]time.Time{},
		seen: map[string]bool{}, ready: map[string]bool{}, pids: map[string]int{}, lastError: map[string]string{},
		expected:    []string{"gateway", "order-1", "order-2", "recommender", "store-a", "store-b", "worker-1"},
		workerCount: 1,
	}
}

func (s *supervisor) command(name string) []string {
	data := filepath.Join(s.root, "runtime", "data")
	common := []string{"--root", s.root}
	switch name {
	case "store-a":
		return append([]string{s.exe, "store", "--node-id", "store-a", "--port", "8301", "--role", "leader", "--peer", storeB, "--data", filepath.Join(data, "store-a.sqlite"), "--supervisor", supervisorURL}, common...)
	case "store-b":
		return append([]string{s.exe, "store", "--node-id", "store-b", "--port", "8302", "--role", "follower", "--peer", storeA, "--data", filepath.Join(data, "store-b.sqlite"), "--supervisor", supervisorURL}, common...)
	case "gateway":
		return append([]string{s.exe, "gateway", "--port", "8080", "--supervisor", supervisorURL, "--orders", "http://127.0.0.1:8101,http://127.0.0.1:8102", "--process-name", "gateway"}, common...)
	case "order-1":
		return append([]string{s.exe, "order", "--process-name", "order-1", "--port", "8101", "--supervisor", supervisorURL, "--recommender", "http://127.0.0.1:8200"}, common...)
	case "order-2":
		return append([]string{s.exe, "order", "--process-name", "order-2", "--port", "8102", "--supervisor", supervisorURL, "--recommender", "http://127.0.0.1:8200"}, common...)
	case "recommender":
		return append([]string{s.exe, "recommender", "--port", "8200", "--process-name", "recommender"}, common...)
	default:
		index, _ := strconv.Atoi(name[len("worker-"):])
		return append([]string{s.exe, "worker", "--process-name", name, "--port", strconv.Itoa(8400 + index), "--supervisor", supervisorURL}, common...)
	}
}

func (s *supervisor) spawn(name string) {
	if stopped() {
		return
	}
	if proc := s.procs[name]; proc != nil {
		if done, _ := proc.finished(); !done {
			return
		}
	}
	cmd := exec.Command(s.command(name)[0], s.command(name)[1:]...)
	cmd.Env = append(os.Environ(), "ORANGE_ROOT="+s.root, "ORANGE_PROCESS="+name)
	if err := cmd.Start(); err != nil {
		s.mu.Lock()
		s.lastError[name] = err.Error()
		s.mu.Unlock()
		return
	}
	proc := &childProc{cmd: cmd, done: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		if exit, ok := err.(*exec.ExitError); ok {
			proc.code = exit.ExitCode()
		} else if err != nil {
			proc.code = 1
		}
		close(proc.done)
	}()
	s.procs[name] = proc
	s.mu.Lock()
	s.pids[name] = cmd.Process.Pid
	s.mu.Unlock()
	logEvent("spawned", "", "info", map[string]any{"process_name": name, "pid": cmd.Process.Pid})
}

func (s *supervisor) stopProcess(name string) {
	proc := s.procs[name]
	if proc == nil {
		return
	}
	if done, _ := proc.finished(); done {
		return
	}
	_ = proc.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-proc.done:
	case <-time.After(2 * time.Second):
		_ = proc.cmd.Process.Kill()
		<-proc.done
	}
	s.mu.Lock()
	s.ready[name] = false
	s.mu.Unlock()
}

func (s *supervisor) stopAll() {
	names := make([]string, 0, len(s.procs))
	for name := range s.procs {
		names = append(names, name)
	}
	for _, name := range names {
		s.stopProcess(name)
	}
}

func (s *supervisor) ensure() {
	s.mu.Lock()
	names := append([]string{}, s.expected...)
	s.mu.Unlock()
	for _, name := range names {
		if proc := s.procs[name]; proc != nil {
			if done, code := proc.finished(); done {
				s.mu.Lock()
				s.lastError[name] = "退出码 " + strconv.Itoa(code)
				s.ready[name] = false
				s.mu.Unlock()
				logEvent("process_exited", "", "error", map[string]any{"process_name": name, "code": code})
				delete(s.procs, name)
				time.Sleep(500 * time.Millisecond)
			}
		}
		proc := s.procs[name]
		if proc == nil {
			s.spawn(name)
			continue
		}
		if done, _ := proc.finished(); done {
			s.spawn(name)
		}
	}
}

func (s *supervisor) probeReady() {
	s.mu.Lock()
	names := append([]string{}, s.expected...)
	s.mu.Unlock()
	ports := map[string]int{
		"gateway": 8080, "order-1": 8101, "order-2": 8102, "recommender": 8200,
		"store-a": 8301, "store-b": 8302, "worker-1": 8401, "worker-2": 8402, "worker-3": 8403,
	}
	for _, name := range names {
		port := ports[name]
		if port == 0 {
			continue
		}
		status, _, _, err := httpRequest("GET", "http://127.0.0.1:"+strconv.Itoa(port)+"/ready", nil, nil, 300*time.Millisecond)
		s.mu.Lock()
		s.ready[name] = err == nil && status == 200
		s.mu.Unlock()
	}
}

func (s *supervisor) noteHeartbeat(body map[string]any) {
	nodeID := asString(body["node_id"])
	if nodeID != "store-a" && nodeID != "store-b" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastBeat[nodeID] = time.Now()
	s.seen[nodeID] = true
	s.applied[nodeID] = asInt(body["applied_index"])
	if asString(body["role"]) == "leader" && asInt(body["epoch"]) >= s.epoch {
		s.epoch = asInt(body["epoch"])
		if nodeID == "store-a" {
			s.leader = storeA
		} else {
			s.leader = storeB
		}
		s.jobsPending = asInt(body["jobs_pending"])
	}
	if len(s.seen) >= 2 {
		s.armed = true
	}
}

func (s *supervisor) maybePromote() {
	s.mu.Lock()
	if !s.armed || s.promoting {
		s.mu.Unlock()
		return
	}
	leaderID := "store-b"
	followerID := "store-a"
	if s.leader == storeA {
		leaderID = "store-a"
		followerID = "store-b"
	}
	leaderSeen, leaderOK := s.lastBeat[leaderID]
	followerSeen, followerOK := s.lastBeat[followerID]
	if !leaderOK || !followerOK || time.Since(leaderSeen) <= leaderLost || time.Since(followerSeen) > leaderLost {
		s.mu.Unlock()
		return
	}
	s.promoting = true
	s.gatewayPaused = true
	followerURL := storeA
	if followerID == "store-b" {
		followerURL = storeB
	}
	newEpoch := s.epoch + 1
	oldLeader := s.leader
	s.mu.Unlock()
	started := time.Now()
	defer func() {
		s.mu.Lock()
		if time.Since(started) > promotionPause && s.lastPromotionError == nil {
			s.lastPromotionError = "切换超过 2 秒"
		}
		s.gatewayPaused = false
		s.promoting = false
		s.mu.Unlock()
	}()
	status, _, raw, err := httpRequest("POST", followerURL+"/internal/promote", map[string]any{"new_epoch": newEpoch, "peer": oldLeader}, nil, promotionPause)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.lastPromotionError = err.Error()
		logEvent("promoted", "", "error", map[string]any{"message": err.Error()})
		return
	}
	if status != 200 || len(raw) == 0 {
		s.lastPromotionError = "提升失败 HTTP " + strconv.Itoa(status)
		logEvent("promoted", "", "error", map[string]any{"message": s.lastPromotionError})
		return
	}
	body, _ := decodeJSON(raw)
	epoch := asInt(asMap(body)["epoch"])
	if epoch == 0 {
		epoch = newEpoch
	}
	s.epoch = epoch
	s.leader = followerURL
	s.lastPromotionError = nil
	logEvent("promoted", "", "info", map[string]any{"epoch": s.epoch, "leader": s.leader})
}

func (s *supervisor) maybeScale() {
	s.mu.Lock()
	pending := s.jobsPending
	now := time.Now()
	var action, name string
	if pending > scaleOutPending {
		s.zeroSet = false
		if !s.highSet {
			s.highSince = now
			s.highSet = true
		}
		if now.Sub(s.highSince) >= scaleOutHold && now.Sub(s.lastScale) >= scaleCooldown && s.workerCount < workerMax {
			s.workerCount++
			name = "worker-" + strconv.Itoa(s.workerCount)
			if !contains(s.expected, name) {
				s.expected = append(s.expected, name)
			}
			s.lastScale = now
			s.highSet = false
			action = "out"
		}
	} else {
		s.highSet = false
	}
	if pending == 0 {
		if !s.zeroSet {
			s.zeroSince = now
			s.zeroSet = true
		}
		if now.Sub(s.zeroSince) >= scaleInHold && now.Sub(s.lastScale) >= scaleCooldown && s.workerCount > workerMin {
			name = "worker-" + strconv.Itoa(s.workerCount)
			s.expected = removeName(s.expected, name)
			s.workerCount--
			s.lastScale = now
			s.zeroSet = false
			action = "in"
		}
	} else {
		s.zeroSet = false
	}
	workers := s.workerCount
	s.mu.Unlock()
	if action == "out" {
		s.spawn(name)
		logEvent("scaled", "", "info", map[string]any{"workers": workers, "direction": "out"})
	} else if action == "in" {
		s.stopProcess(name)
		logEvent("scaled", "", "info", map[string]any{"workers": workers, "direction": "in"})
	}
}

func contains(items []string, name string) bool {
	for _, item := range items {
		if item == name {
			return true
		}
	}
	return false
}

func removeName(items []string, name string) []string {
	var out []string
	for _, item := range items {
		if item != name {
			out = append(out, item)
		}
	}
	return out
}

func (s *supervisor) status() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	ready := map[string]any{}
	for _, name := range s.expected {
		ready[name] = s.ready[name]
	}
	applied := map[string]any{}
	for key, value := range s.applied {
		applied[key] = value
	}
	pids := map[string]any{}
	for key, value := range s.pids {
		pids[key] = value
	}
	errors := map[string]any{}
	for key, value := range s.lastError {
		errors[key] = value
	}
	return map[string]any{
		"leader": s.leader, "epoch": s.epoch, "applied_index": applied, "workers": s.workerCount,
		"jobs_pending": s.jobsPending, "gateway_paused": s.gatewayPaused, "ready": ready,
		"last_promotion_error": s.lastPromotionError, "pids": pids, "last_error": errors,
	}
}

func (s *supervisor) loop() {
	for !stopped() {
		s.ensure()
		s.probeReady()
		s.maybePromote()
		s.maybeScale()
		select {
		case <-stopCh:
		case <-time.After(heartbeat):
		}
	}
	s.stopAll()
}

func supervisorHandler(s *supervisor) handlerFunc {
	return func(method, path string, headers map[string]string, raw []byte) response {
		requestID := headerRequestID(headers)
		route, _ := splitPath(path)
		switch {
		case method == "GET" && route == "/health":
			return emptyResponse(200)
		case method == "GET" && route == "/v1/status":
			return jsonResponse(200, s.status(), nil)
		case method == "GET" && route == "/internal/leader":
			s.mu.Lock()
			body := map[string]any{"leader": s.leader, "epoch": s.epoch}
			s.mu.Unlock()
			return jsonResponse(200, body, nil)
		case method == "GET" && route == "/internal/gateway-control":
			s.mu.Lock()
			body := map[string]any{"paused": s.gatewayPaused, "jobs_pending": s.jobsPending}
			s.mu.Unlock()
			return jsonResponse(200, body, nil)
		case method == "POST" && route == "/internal/heartbeat":
			parsed, _ := decodeJSON(raw)
			s.noteHeartbeat(asMap(parsed))
			s.mu.Lock()
			body := map[string]any{"leader": s.leader, "epoch": s.epoch}
			s.mu.Unlock()
			return jsonResponse(200, body, nil)
		default:
			return jsonResponse(404, errorBody("not_found", "没有这个路径", requestID), nil)
		}
	}
}

func runSupervisor(args []string) {
	flags := newFlagSet()
	root := flags.String("root", ".", "")
	parseFlags(flags, args)
	setContext("supervisor", *root)
	_ = os.MkdirAll(filepath.Join(*root, "runtime", "data"), 0o755)
	_ = os.MkdirAll(filepath.Join(*root, "runtime", "logs"), 0o755)
	faults := filepath.Join(*root, "runtime", "faults.json")
	if _, err := os.Stat(faults); err != nil {
		_ = os.WriteFile(faults, []byte("{\"delay_ms\":{\"store\":0,\"recommender\":0},\"pause_replication\":false,\"drop_heartbeat\":{\"store-a\":false,\"store-b\":false},\"exit_after_requests\":{}}\n"), 0o644)
	}
	startFaultReader()
	exe, err := os.Executable()
	if err != nil {
		exe = os.Args[0]
	}
	item := newSupervisor(*root, exe)
	go func() {
		logEvent("process_started", "", "info", map[string]any{"port": 8090})
		_ = serve(8090, supervisorHandler(item))
	}()
	time.Sleep(50 * time.Millisecond)
	item.loop()
}
