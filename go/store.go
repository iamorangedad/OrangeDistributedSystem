package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    caller_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    external_ref TEXT,
    sku TEXT NOT NULL,
    qty INTEGER NOT NULL,
    fulfill_url TEXT,
    state TEXT NOT NULL,
    recommendation TEXT,
    degraded INTEGER NOT NULL,
    epoch INTEGER NOT NULL,
    log_index INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    response_json TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    UNIQUE (caller_id, idempotency_key)
);
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    visible_after INTEGER NOT NULL,
    worker_id TEXT,
    last_error TEXT,
    log_index INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS replication_log (
    log_index INTEGER PRIMARY KEY,
    epoch INTEGER NOT NULL,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL,
    committed_at TEXT NOT NULL
);
`

type orderRow struct {
	orderID, callerID, idempotencyKey, sku, state, createdAt, responseJSON, requestHash string
	externalRef, fulfillURL, recommendation                                             sql.NullString
	qty, degraded, epoch, logIndex                                                      int64
}

type jobRow struct {
	jobID, orderID, kind, state string
	attempts, visibleAfter      int64
	workerID, lastError         sql.NullString
	logIndex                    int64
}

type store struct {
	nodeID             string
	peer               string
	supervisor         string
	conn               *sql.Conn
	mu                 sync.Mutex
	followerApplied    int64
	running            atomic.Bool
	replicationStarted atomic.Bool
	metricsErrors      int64
	metricsPending     int64
	metricsLag         int64
}

func openStore(nodeID, role, peer, path, supervisor string) (*store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	conn, err := db.Conn(context.Background())
	if err != nil {
		return nil, err
	}
	store := &store{nodeID: nodeID, peer: peer, supervisor: supervisor, conn: conn}
	store.running.Store(true)
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := conn.ExecContext(context.Background(), pragma); err != nil {
			return nil, err
		}
	}
	for _, piece := range strings.Split(schema, ";") {
		statement := strings.TrimSpace(piece)
		if statement == "" || !strings.Contains(strings.ToUpper(statement), "CREATE") {
			continue
		}
		if _, err := conn.ExecContext(context.Background(), statement); err != nil {
			return nil, err
		}
	}
	if err := store.ensureMeta(role); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *store) exec(query string, args ...any) error {
	_, err := s.conn.ExecContext(context.Background(), query, args...)
	return err
}

func (s *store) ensureMeta(role string) error {
	row := s.conn.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM meta")
	var count int
	if err := row.Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	for _, item := range [][2]string{{"role", role}, {"epoch", "1"}, {"applied_index", "0"}, {"node_id", s.nodeID}} {
		if err := s.exec("INSERT INTO meta(key, value) VALUES (?, ?)", item[0], item[1]); err != nil {
			return err
		}
	}
	return nil
}

func (s *store) meta(key string) string {
	var value string
	err := s.conn.QueryRowContext(context.Background(), "SELECT value FROM meta WHERE key = ?", key).Scan(&value)
	if err != nil {
		return ""
	}
	return value
}

func (s *store) setMeta(key, value string) error {
	return s.exec("INSERT INTO meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", key, value)
}

func (s *store) role() string { return s.meta("role") }

func (s *store) epoch() int64 {
	value, _ := strconv.ParseInt(s.meta("epoch"), 10, 64)
	if value == 0 {
		return 1
	}
	return value
}

func (s *store) applied() int64 {
	value, _ := strconv.ParseInt(s.meta("applied_index"), 10, 64)
	return value
}

func (s *store) isLeader() bool { return s.role() == "leader" }

func (s *store) refreshMetrics() {
	var pending int64
	_ = s.conn.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM jobs WHERE state = 'pending'").Scan(&pending)
	s.metricsPending = pending
	if s.isLeader() {
		lag := s.applied() - s.followerApplied
		if lag < 0 {
			lag = 0
		}
		s.metricsLag = lag
	} else {
		s.metricsLag = 0
	}
}

func (s *store) appendLog(kind string, payload any) (int64, error) {
	index := s.applied() + 1
	err := s.exec(
		"INSERT INTO replication_log(log_index, epoch, kind, payload, committed_at) VALUES (?, ?, ?, ?, ?)",
		index, s.epoch(), kind, string(marshal(payload)), isoNow(),
	)
	if err != nil {
		return 0, err
	}
	if err := s.setMeta("applied_index", strconv.FormatInt(index, 10)); err != nil {
		return 0, err
	}
	return index, nil
}

func scanOrder(row *sql.Row) (orderRow, error) {
	var order orderRow
	err := row.Scan(
		&order.orderID, &order.callerID, &order.idempotencyKey, &order.externalRef, &order.sku, &order.qty,
		&order.fulfillURL, &order.state, &order.recommendation, &order.degraded, &order.epoch, &order.logIndex,
		&order.createdAt, &order.responseJSON, &order.requestHash,
	)
	return order, err
}

func (s *store) orderByID(id string) (orderRow, bool) {
	order, err := scanOrder(s.conn.QueryRowContext(context.Background(), "SELECT * FROM orders WHERE order_id = ?", id))
	return order, err == nil
}

func (s *store) orderByKey(callerID, key string) (orderRow, bool) {
	order, err := scanOrder(s.conn.QueryRowContext(context.Background(), "SELECT * FROM orders WHERE caller_id = ? AND idempotency_key = ?", callerID, key))
	return order, err == nil
}

func scanJob(row *sql.Row) (jobRow, error) {
	var job jobRow
	err := row.Scan(&job.jobID, &job.orderID, &job.kind, &job.state, &job.attempts, &job.visibleAfter, &job.workerID, &job.lastError, &job.logIndex)
	return job, err
}

func (s *store) jobForOrder(orderID string) (jobRow, bool) {
	job, err := scanJob(s.conn.QueryRowContext(context.Background(), "SELECT * FROM jobs WHERE order_id = ?", orderID))
	return job, err == nil
}

func (s *store) jobByID(id string) (jobRow, bool) {
	job, err := scanJob(s.conn.QueryRowContext(context.Background(), "SELECT * FROM jobs WHERE job_id = ?", id))
	return job, err == nil
}

func fulfillmentOf(job jobRow) map[string]any {
	var last any
	if job.lastError.Valid {
		last = job.lastError.String
	}
	return map[string]any{"status": job.state, "attempts": job.attempts, "last_error": last}
}

func snapshotBody(order orderRow, job jobRow, requestID string) map[string]any {
	var external, recommendation any
	if order.externalRef.Valid {
		external = order.externalRef.String
	}
	if order.recommendation.Valid {
		recommendation = order.recommendation.String
	}
	return map[string]any{
		"order_id": order.orderID, "caller_id": order.callerID, "external_ref": external,
		"sku": order.sku, "qty": order.qty, "state": order.state, "recommendation": recommendation,
		"degraded": order.degraded != 0, "fulfillment": fulfillmentOf(job), "epoch": order.epoch, "request_id": requestID,
	}
}

func (s *store) writeSnapshot(orderID string, body map[string]any) error {
	degraded := 0
	if asBool(body["degraded"]) {
		degraded = 1
	}
	_, err := s.conn.ExecContext(context.Background(),
		"UPDATE orders SET response_json = ?, state = ?, recommendation = ?, degraded = ? WHERE order_id = ?",
		string(marshal(body)), asString(body["state"]), body["recommendation"], degraded, orderID,
	)
	return err
}

func (s *store) publicBody(order orderRow, requestID string) map[string]any {
	parsed, _ := decodeJSON([]byte(order.responseJSON))
	body := asMap(parsed)
	if body == nil {
		body = map[string]any{}
	}
	if requestID != "" {
		body["request_id"] = requestID
	}
	return body
}

func fenced(epoch int64, requestID, message string) response {
	return jsonResponse(409, map[string]any{"error": map[string]any{
		"code": "epoch_fenced", "message": message, "request_id": requestID, "epoch": epoch,
	}}, nil)
}

func (s *store) requireLeader(requestID string) (response, bool) {
	if s.isLeader() {
		return response{}, true
	}
	s.metricsErrors++
	logEvent("write_fenced", requestID, "info", map[string]any{"epoch": s.epoch()})
	return fenced(s.epoch(), requestID, "当前节点不接受写入"), false
}

func (s *store) appendOrder(payload map[string]any, requestID string) response {
	faultDelay("store")
	expected := asInt(payload["epoch"])
	callerID := asString(payload["caller_id"])
	key := asString(payload["idempotency_key"])
	fields := asMap(payload["order"])
	digest := canonicalBody(asString(fields["sku"]), asInt(fields["qty"]), fields["external_ref"], fields["fulfill_url"])
	s.mu.Lock()
	defer s.mu.Unlock()
	if refused, ok := s.requireLeader(requestID); !ok {
		return refused
	}
	if expected != s.epoch() {
		s.metricsErrors++
		logEvent("write_fenced", requestID, "info", map[string]any{"epoch": s.epoch(), "expected": expected})
		return fenced(s.epoch(), requestID, "写入代际已过期")
	}
	if err := s.exec("BEGIN IMMEDIATE"); err != nil {
		panic(err)
	}
	var created bool
	var orderState map[string]any
	var index int64
	existing, found := s.orderByKey(callerID, key)
	if found {
		_ = s.exec("COMMIT")
		if existing.requestHash != digest {
			return jsonResponse(409, errorBody("idempotency_mismatch", "幂等键与已有订单正文不一致", requestID), nil)
		}
		return jsonResponse(200, map[string]any{
			"created": false, "order": s.publicBody(existing, requestID), "log_index": existing.logIndex, "epoch": s.epoch(),
		}, nil)
	}
	orderID := newID("ord_")
	jobID := newID("job_")
	createdAt := isoNow()
	job := map[string]any{
		"job_id": jobID, "order_id": orderID, "kind": "fulfill", "state": "pending",
		"attempts": 0, "visible_after": 0, "worker_id": nil, "last_error": nil,
	}
	orderState = map[string]any{
		"order_id": orderID, "caller_id": callerID, "external_ref": fields["external_ref"], "sku": fields["sku"],
		"qty": asInt(fields["qty"]), "state": "accepted", "recommendation": nil, "degraded": true,
		"fulfillment": map[string]any{"status": "pending", "attempts": 0, "last_error": nil},
		"epoch":       s.epoch(), "request_id": requestID,
	}
	_, err := s.conn.ExecContext(context.Background(), `INSERT INTO orders(
		order_id, caller_id, idempotency_key, external_ref, sku, qty, fulfill_url,
		state, recommendation, degraded, epoch, log_index, created_at, response_json, request_hash
	) VALUES (?, ?, ?, ?, ?, ?, ?, 'accepted', NULL, 1, ?, 0, ?, ?, ?)`,
		orderID, callerID, key, fields["external_ref"], fields["sku"], asInt(fields["qty"]), fields["fulfill_url"],
		s.epoch(), createdAt, string(marshal(orderState)), digest)
	if err != nil {
		_ = s.exec("ROLLBACK")
		panic(err)
	}
	if err := s.exec(`INSERT INTO jobs(job_id, order_id, kind, state, attempts, visible_after, worker_id, last_error, log_index)
		VALUES (?, ?, 'fulfill', 'pending', 0, 0, NULL, NULL, 0)`, jobID, orderID); err != nil {
		_ = s.exec("ROLLBACK")
		panic(err)
	}
	logPayload := map[string]any{"order": map[string]any{
		"order_id": orderID, "caller_id": callerID, "idempotency_key": key, "external_ref": fields["external_ref"],
		"sku": fields["sku"], "qty": asInt(fields["qty"]), "fulfill_url": fields["fulfill_url"],
		"created_at": createdAt, "request_hash": digest, "response": orderState,
	}, "job": job}
	index, err = s.appendLog("order_created", logPayload)
	if err != nil {
		_ = s.exec("ROLLBACK")
		panic(err)
	}
	if err := s.exec("UPDATE orders SET log_index = ?, epoch = ? WHERE order_id = ?", index, s.epoch(), orderID); err != nil {
		_ = s.exec("ROLLBACK")
		panic(err)
	}
	if err := s.exec("UPDATE jobs SET log_index = ? WHERE job_id = ?", index, jobID); err != nil {
		_ = s.exec("ROLLBACK")
		panic(err)
	}
	orderState["epoch"] = s.epoch()
	if err := s.exec("UPDATE orders SET response_json = ? WHERE order_id = ?", string(marshal(orderState)), orderID); err != nil {
		_ = s.exec("ROLLBACK")
		panic(err)
	}
	if err := s.exec("COMMIT"); err != nil {
		panic(err)
	}
	created = true
	s.refreshMetrics()
	logEvent("order_accepted", requestID, "info", map[string]any{"order_id": orderID, "epoch": s.epoch(), "degraded": true})
	_ = created
	return jsonResponse(200, map[string]any{"created": true, "order": orderState, "log_index": index, "epoch": s.epoch()}, nil)
}

func (s *store) readOrder(orderID, callerID, requestID string) response {
	s.mu.Lock()
	defer s.mu.Unlock()
	if refused, ok := s.requireLeader(requestID); !ok {
		return refused
	}
	order, found := s.orderByID(orderID)
	if !found || order.callerID != callerID {
		return jsonResponse(404, errorBody("not_found", "没有这笔订单", requestID), nil)
	}
	return jsonResponse(200, s.publicBody(order, requestID), nil)
}

func (s *store) readByKey(callerID, key, requestID string) response {
	s.mu.Lock()
	defer s.mu.Unlock()
	if refused, ok := s.requireLeader(requestID); !ok {
		return refused
	}
	order, found := s.orderByKey(callerID, key)
	if !found {
		return jsonResponse(404, errorBody("not_found", "没有这笔订单", requestID), nil)
	}
	body := s.publicBody(order, requestID)
	body["request_hash"] = order.requestHash
	return jsonResponse(200, body, nil)
}

func (s *store) saveSnapshot(payload map[string]any, requestID string) response {
	orderID := asString(payload["order_id"])
	s.mu.Lock()
	defer s.mu.Unlock()
	if refused, ok := s.requireLeader(requestID); !ok {
		return refused
	}
	if asInt(payload["epoch"]) != s.epoch() {
		return fenced(s.epoch(), requestID, "写入代际已过期")
	}
	order, found := s.orderByID(orderID)
	_, jobFound := s.jobForOrder(orderID)
	if !found || !jobFound {
		return jsonResponse(404, errorBody("not_found", "没有这笔订单", requestID), nil)
	}
	body := s.publicBody(order, "")
	body["recommendation"] = payload["recommendation"]
	body["degraded"] = asBool(payload["degraded"])
	body["request_id"] = requestID
	body["epoch"] = s.epoch()
	if err := s.exec("BEGIN IMMEDIATE"); err != nil {
		panic(err)
	}
	if err := s.writeSnapshot(orderID, body); err != nil {
		_ = s.exec("ROLLBACK")
		panic(err)
	}
	if _, err := s.appendLog("order_snapshot", map[string]any{"order_id": orderID, "response": body}); err != nil {
		_ = s.exec("ROLLBACK")
		panic(err)
	}
	if err := s.exec("COMMIT"); err != nil {
		panic(err)
	}
	return jsonResponse(200, body, nil)
}

func (s *store) claim(workerID, requestID string) response {
	s.mu.Lock()
	defer s.mu.Unlock()
	if refused, ok := s.requireLeader(requestID); !ok {
		return refused
	}
	s.releaseExpired()
	rows, err := s.conn.QueryContext(context.Background(),
		"SELECT * FROM jobs WHERE state = 'pending' AND visible_after <= ? ORDER BY job_id LIMIT 1", nowMS())
	if err != nil {
		panic(err)
	}
	if !rows.Next() {
		rows.Close()
		return emptyResponse(204)
	}
	var row jobRow
	if err := rows.Scan(&row.jobID, &row.orderID, &row.kind, &row.state, &row.attempts, &row.visibleAfter, &row.workerID, &row.lastError, &row.logIndex); err != nil {
		rows.Close()
		panic(err)
	}
	rows.Close()
	attempts := row.attempts + 1
	visible := nowMS() + visibility.Milliseconds()
	if err := s.exec("BEGIN IMMEDIATE"); err != nil {
		panic(err)
	}
	if err := s.exec("UPDATE jobs SET state = 'inflight', attempts = ?, visible_after = ?, worker_id = ? WHERE job_id = ?", attempts, visible, workerID, row.jobID); err != nil {
		_ = s.exec("ROLLBACK")
		panic(err)
	}
	order, _ := s.orderByID(row.orderID)
	job, _ := s.jobForOrder(row.orderID)
	body := snapshotBody(order, job, requestID)
	if err := s.writeSnapshot(order.orderID, body); err != nil {
		_ = s.exec("ROLLBACK")
		panic(err)
	}
	if _, err := s.appendLog("job_claimed", map[string]any{"job_id": row.jobID, "order_id": row.orderID, "response": body, "worker_id": workerID}); err != nil {
		_ = s.exec("ROLLBACK")
		panic(err)
	}
	if err := s.exec("COMMIT"); err != nil {
		panic(err)
	}
	s.refreshMetrics()
	claimed, _ := s.jobForOrder(row.orderID)
	order, _ = s.orderByID(row.orderID)
	var external, fulfill any
	if order.externalRef.Valid {
		external = order.externalRef.String
	}
	if order.fulfillURL.Valid {
		fulfill = order.fulfillURL.String
	}
	logEvent("job_claimed", requestID, "info", map[string]any{"order_id": row.orderID, "worker_id": workerID})
	return jsonResponse(200, map[string]any{
		"job_id": claimed.jobID, "order_id": order.orderID, "caller_id": order.callerID,
		"external_ref": external, "sku": order.sku, "qty": order.qty, "fulfill_url": fulfill, "attempts": claimed.attempts,
	}, nil)
}

func (s *store) finish(jobID, requestID string, success bool, failure string) response {
	s.mu.Lock()
	defer s.mu.Unlock()
	if refused, ok := s.requireLeader(requestID); !ok {
		return refused
	}
	job, found := s.jobByID(jobID)
	if !found {
		return jsonResponse(404, errorBody("not_found", "没有这个任务", requestID), nil)
	}
	if err := s.exec("BEGIN IMMEDIATE"); err != nil {
		panic(err)
	}
	var kind, event string
	if success {
		if err := s.exec("UPDATE jobs SET state = 'done', last_error = NULL, worker_id = NULL WHERE job_id = ?", jobID); err != nil {
			_ = s.exec("ROLLBACK")
			panic(err)
		}
		kind, event = "job_done", "job_done"
	} else if job.attempts >= jobMaxAttempts {
		if err := s.exec("UPDATE jobs SET state = 'dead', last_error = ?, worker_id = NULL WHERE job_id = ?", failure, jobID); err != nil {
			_ = s.exec("ROLLBACK")
			panic(err)
		}
		kind, event = "job_dead", "job_dead"
	} else {
		wait := int64(backoffMS(int(job.attempts)))
		if err := s.exec("UPDATE jobs SET state = 'pending', last_error = ?, worker_id = NULL, visible_after = ? WHERE job_id = ?", failure, nowMS()+wait, jobID); err != nil {
			_ = s.exec("ROLLBACK")
			panic(err)
		}
		kind, event = "job_reset", "job_retry"
	}
	job, _ = s.jobByID(jobID)
	order, _ := s.orderByID(job.orderID)
	state := "accepted"
	if job.state == "done" {
		state = "fulfilled"
	} else if job.state == "dead" {
		state = "fulfillment_failed"
	}
	body := s.publicBody(order, "")
	body["state"] = state
	body["fulfillment"] = fulfillmentOf(job)
	body["epoch"] = s.epoch()
	if err := s.exec("UPDATE orders SET state = ? WHERE order_id = ?", state, order.orderID); err != nil {
		_ = s.exec("ROLLBACK")
		panic(err)
	}
	if err := s.writeSnapshot(order.orderID, body); err != nil {
		_ = s.exec("ROLLBACK")
		panic(err)
	}
	if _, err := s.appendLog(kind, map[string]any{"job_id": jobID, "order_id": order.orderID, "response": body}); err != nil {
		_ = s.exec("ROLLBACK")
		panic(err)
	}
	if err := s.exec("COMMIT"); err != nil {
		panic(err)
	}
	s.refreshMetrics()
	logEvent(event, requestID, "info", map[string]any{"order_id": order.orderID, "job_id": jobID})
	return jsonResponse(200, body, nil)
}

func (s *store) releaseExpired() {
	rows, err := s.conn.QueryContext(context.Background(), "SELECT * FROM jobs WHERE state = 'inflight' AND visible_after <= ?", nowMS())
	if err != nil {
		return
	}
	var expired []jobRow
	for rows.Next() {
		var row jobRow
		if err := rows.Scan(&row.jobID, &row.orderID, &row.kind, &row.state, &row.attempts, &row.visibleAfter, &row.workerID, &row.lastError, &row.logIndex); err == nil {
			expired = append(expired, row)
		}
	}
	rows.Close()
	for _, row := range expired {
		if err := s.exec("BEGIN IMMEDIATE"); err != nil {
			continue
		}
		if err := s.exec("UPDATE jobs SET state = 'pending', worker_id = NULL WHERE job_id = ? AND state = 'inflight'", row.jobID); err != nil {
			_ = s.exec("ROLLBACK")
			continue
		}
		order, orderOK := s.orderByID(row.orderID)
		job, jobOK := s.jobForOrder(row.orderID)
		if orderOK && jobOK {
			body := s.publicBody(order, "")
			body["state"] = "accepted"
			body["fulfillment"] = fulfillmentOf(job)
			_ = s.writeSnapshot(order.orderID, body)
			_, _ = s.appendLog("job_reset", map[string]any{"job_id": row.jobID, "order_id": row.orderID, "response": body})
		}
		_ = s.exec("COMMIT")
		logEvent("job_retry", "", "info", map[string]any{"order_id": row.orderID, "job_id": row.jobID, "reason": "visibility_timeout"})
	}
}

func (s *store) promote(newEpoch int64, peer, requestID string) response {
	s.mu.Lock()
	defer s.mu.Unlock()
	if newEpoch <= s.epoch() && s.isLeader() {
		return jsonResponse(200, map[string]any{"epoch": s.epoch(), "role": s.role()}, nil)
	}
	if err := s.exec("BEGIN IMMEDIATE"); err != nil {
		panic(err)
	}
	if err := s.setMeta("role", "leader"); err != nil {
		_ = s.exec("ROLLBACK")
		panic(err)
	}
	if err := s.setMeta("epoch", strconv.FormatInt(newEpoch, 10)); err != nil {
		_ = s.exec("ROLLBACK")
		panic(err)
	}
	if _, err := s.appendLog("epoch_promoted", map[string]any{"epoch": newEpoch, "node_id": s.nodeID}); err != nil {
		_ = s.exec("ROLLBACK")
		panic(err)
	}
	if err := s.exec("COMMIT"); err != nil {
		panic(err)
	}
	s.peer = peer
	s.followerApplied = 0
	s.refreshMetrics()
	logEvent("promoted", requestID, "info", map[string]any{"epoch": newEpoch, "node_id": s.nodeID})
	return jsonResponse(200, map[string]any{"epoch": newEpoch, "role": "leader"}, nil)
}

func (s *store) stepDown(newEpoch int64, leader string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isLeader() && newEpoch <= s.epoch() {
		return
	}
	if !s.isLeader() && newEpoch == s.epoch() && s.peer == leader {
		return
	}
	if err := s.exec("BEGIN IMMEDIATE"); err != nil {
		return
	}
	_ = s.exec("DELETE FROM orders")
	_ = s.exec("DELETE FROM jobs")
	_ = s.exec("DELETE FROM replication_log")
	_ = s.setMeta("role", "follower")
	_ = s.setMeta("epoch", strconv.FormatInt(newEpoch, 10))
	_ = s.setMeta("applied_index", "0")
	_ = s.exec("COMMIT")
	s.peer = leader
	logEvent("stepped_down", "", "info", map[string]any{"epoch": newEpoch, "leader": leader})
}

func (s *store) entriesAfter(index int64) []any {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.conn.QueryContext(context.Background(), "SELECT log_index, epoch, kind, payload, committed_at FROM replication_log WHERE log_index > ? ORDER BY log_index", index)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var entries []any
	for rows.Next() {
		var logIndex, epoch int64
		var kind, payload, committed string
		if err := rows.Scan(&logIndex, &epoch, &kind, &payload, &committed); err != nil {
			continue
		}
		decoded, _ := decodeJSON([]byte(payload))
		entries = append(entries, map[string]any{
			"log_index": logIndex, "epoch": epoch, "kind": kind, "payload": decoded, "committed_at": committed,
		})
	}
	return entries
}

func (s *store) applyRemote(entries []any) (bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.exec("BEGIN IMMEDIATE"); err != nil {
		panic(err)
	}
	for _, item := range entries {
		entry := asMap(item)
		expected := s.applied() + 1
		if asInt(entry["log_index"]) != expected {
			_ = s.exec("ROLLBACK")
			return false, "gap"
		}
		if err := s.applyPayload(asString(entry["kind"]), asMap(entry["payload"]), asInt(entry["epoch"])); err != nil {
			_ = s.exec("ROLLBACK")
			panic(err)
		}
		committed := asString(entry["committed_at"])
		if committed == "" {
			committed = isoNow()
		}
		if err := s.exec(
			"INSERT INTO replication_log(log_index, epoch, kind, payload, committed_at) VALUES (?, ?, ?, ?, ?)",
			asInt(entry["log_index"]), asInt(entry["epoch"]), entry["kind"], string(marshal(entry["payload"])), committed,
		); err != nil {
			_ = s.exec("ROLLBACK")
			panic(err)
		}
		if err := s.setMeta("applied_index", strconv.FormatInt(asInt(entry["log_index"]), 10)); err != nil {
			_ = s.exec("ROLLBACK")
			panic(err)
		}
		if asString(entry["kind"]) == "epoch_promoted" {
			_ = s.setMeta("epoch", strconv.FormatInt(asInt(asMap(entry["payload"])["epoch"]), 10))
			_ = s.setMeta("role", "follower")
		}
	}
	if err := s.exec("COMMIT"); err != nil {
		panic(err)
	}
	s.refreshMetrics()
	return true, ""
}

func (s *store) applyPayload(kind string, payload map[string]any, epoch int64) error {
	if kind == "order_created" {
		order := asMap(payload["order"])
		job := asMap(payload["job"])
		response := asMap(order["response"])
		degraded := 0
		if asBool(response["degraded"]) {
			degraded = 1
		}
		_, err := s.conn.ExecContext(context.Background(), `INSERT OR REPLACE INTO orders(
			order_id, caller_id, idempotency_key, external_ref, sku, qty, fulfill_url,
			state, recommendation, degraded, epoch, log_index, created_at, response_json, request_hash
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)`,
			order["order_id"], order["caller_id"], order["idempotency_key"], order["external_ref"], order["sku"], asInt(order["qty"]), order["fulfill_url"],
			response["state"], response["recommendation"], degraded, epoch, order["created_at"], string(marshal(response)), order["request_hash"])
		if err != nil {
			return err
		}
		_, err = s.conn.ExecContext(context.Background(), `INSERT OR REPLACE INTO jobs(job_id, order_id, kind, state, attempts, visible_after, worker_id, last_error, log_index)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)`,
			job["job_id"], job["order_id"], job["kind"], job["state"], asInt(job["attempts"]), asInt(job["visible_after"]), job["worker_id"], job["last_error"])
		return err
	}
	if payload["order_id"] != nil && payload["response"] != nil {
		body := asMap(payload["response"])
		if err := s.writeSnapshot(asString(payload["order_id"]), body); err != nil {
			return err
		}
		if kind != "order_snapshot" {
			fulfillment := asMap(body["fulfillment"])
			_, err := s.conn.ExecContext(context.Background(), `UPDATE jobs SET state = ?, attempts = ?, last_error = ?, worker_id = ? WHERE order_id = ?`,
				fulfillment["status"], asInt(fulfillment["attempts"]), fulfillment["last_error"], payload["worker_id"], payload["order_id"])
			return err
		}
	}
	return nil
}

func (s *store) stats() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refreshMetrics()
	return map[string]any{
		"node_id": s.nodeID, "role": s.role(), "epoch": s.epoch(), "applied_index": s.applied(),
		"jobs_pending": s.metricsPending, "replicate_lag": s.metricsLag,
	}
}

func (s *store) heartbeatLoop() {
	for s.running.Load() {
		time.Sleep(heartbeat)
		if dropHeartbeat(s.nodeID) {
			continue
		}
		view := s.stats()
		status, _, raw, err := httpRequest("POST", s.supervisor+"/internal/heartbeat", view, nil, 500*time.Millisecond)
		if err != nil || status != 200 {
			continue
		}
		body, _ := decodeJSON(raw)
		parsed := asMap(body)
		leader := asString(parsed["leader"])
		epoch := asInt(parsed["epoch"])
		s.mu.Lock()
		leaderNow := s.isLeader()
		current := s.epoch()
		s.mu.Unlock()
		if leader != "" && epoch > current && leaderNow {
			s.stepDown(epoch, leader)
		} else if leader != "" && !leaderNow {
			s.mu.Lock()
			s.peer = leader
			s.mu.Unlock()
		}
	}
}

func (s *store) replicateLoop() {
	s.replicationStarted.Store(true)
	for s.running.Load() {
		time.Sleep(heartbeat)
		s.mu.Lock()
		leader := s.isLeader()
		s.mu.Unlock()
		if leader {
			s.push()
		} else {
			s.pull()
		}
	}
}

func (s *store) push() {
	if pauseReplication() {
		return
	}
	s.mu.Lock()
	peer := s.peer
	from := s.followerApplied
	s.mu.Unlock()
	if peer == "" {
		return
	}
	entries := s.entriesAfter(from)
	if len(entries) == 0 {
		s.mu.Lock()
		s.refreshMetrics()
		s.mu.Unlock()
		return
	}
	status, _, raw, err := httpRequest("POST", peer+"/internal/replicate", map[string]any{"entries": entries}, nil, 500*time.Millisecond)
	if err != nil {
		s.mu.Lock()
		lag := s.applied() - s.followerApplied
		s.mu.Unlock()
		logEvent("replicate_lag", "", "error", map[string]any{"message": err.Error(), "lag": lag})
	} else if status == 200 {
		body, _ := decodeJSON(raw)
		s.mu.Lock()
		s.followerApplied = asInt(asMap(body)["applied_index"])
		s.mu.Unlock()
	} else if status == 409 {
		body, _ := decodeJSON(raw)
		s.mu.Lock()
		s.followerApplied = asInt(asMap(body)["applied_index"])
		s.mu.Unlock()
	}
	s.mu.Lock()
	s.refreshMetrics()
	lag := s.metricsLag
	s.mu.Unlock()
	if lag != 0 {
		logEvent("replicate_lag", "", "info", map[string]any{"lag": lag})
	}
}

func (s *store) pull() {
	s.mu.Lock()
	peer := s.peer
	index := s.applied()
	s.mu.Unlock()
	if peer == "" {
		return
	}
	status, _, raw, err := httpRequest("GET", peer+"/internal/log?after="+strconv.FormatInt(index, 10), nil, nil, 500*time.Millisecond)
	if err != nil || status != 200 || len(raw) == 0 {
		return
	}
	body, _ := decodeJSON(raw)
	entries, _ := asMap(body)["entries"].([]any)
	if len(entries) > 0 {
		_, _ = s.applyRemote(entries)
	}
}

func (s *store) sweepLoop() {
	for s.running.Load() {
		time.Sleep(heartbeat)
		s.mu.Lock()
		leader := s.isLeader()
		if leader {
			s.releaseExpired()
			s.refreshMetrics()
		}
		s.mu.Unlock()
	}
}

func storeHandler(node *store) handlerFunc {
	return func(method, path string, headers map[string]string, raw []byte) response {
		requestID := headerRequestID(headers)
		route, query := splitPath(path)
		body, _ := decodeJSON(raw)
		payload := asMap(body)
		switch {
		case method == "GET" && route == "/health":
			return emptyResponse(200)
		case method == "GET" && route == "/ready":
			node.mu.Lock()
			ready := node.isLeader() || node.replicationStarted.Load()
			node.mu.Unlock()
			if ready {
				return emptyResponse(200)
			}
			return emptyResponse(503)
		case method == "GET" && route == "/metrics":
			view := node.stats()
			leader := asString(view["role"]) == "leader"
			pending, lag := int64(0), int64(0)
			if leader {
				pending = asInt(view["jobs_pending"])
				lag = asInt(view["replicate_lag"])
			}
			text := "orange_errors_total " + strconv.FormatInt(node.metricsErrors, 10) + "\n"
			text += "orange_jobs_pending " + strconv.FormatInt(pending, 10) + "\n"
			text += "orange_replicate_lag " + strconv.FormatInt(lag, 10) + "\n"
			return textResponse(200, text)
		case method == "GET" && route == "/internal/stats":
			return jsonResponse(200, node.stats(), nil)
		case method == "GET" && route == "/internal/log":
			after := asInt(json.Number(query["after"]))
			node.mu.Lock()
			applied := node.applied()
			node.mu.Unlock()
			return jsonResponse(200, map[string]any{"entries": node.entriesAfter(after), "applied_index": applied}, nil)
		case method == "POST" && route == "/internal/replicate":
			entries, _ := payload["entries"].([]any)
			ok, reason := node.applyRemote(entries)
			node.mu.Lock()
			applied := node.applied()
			node.mu.Unlock()
			status := 200
			if !ok {
				status = 409
			}
			return jsonResponse(status, map[string]any{"applied_index": applied, "error": reason}, nil)
		case method == "POST" && route == "/internal/promote":
			next := asString(payload["peer"])
			if next == "" {
				next = node.peer
			}
			return node.promote(asInt(payload["new_epoch"]), next, requestID)
		case method == "POST" && route == "/internal/append":
			return node.appendOrder(payload, requestID)
		case method == "POST" && route == "/internal/snapshot":
			return node.saveSnapshot(payload, requestID)
		case method == "GET" && route == "/internal/orders/by-key":
			return node.readByKey(query["caller_id"], query["idempotency_key"], requestID)
		case method == "GET" && strings.HasPrefix(route, "/internal/orders/"):
			return node.readOrder(route[strings.LastIndex(route, "/")+1:], query["caller_id"], requestID)
		case method == "POST" && route == "/internal/jobs/claim":
			workerID := asString(payload["worker_id"])
			if workerID == "" {
				workerID = "worker"
			}
			return node.claim(workerID, requestID)
		case method == "POST" && strings.HasPrefix(route, "/internal/jobs/") && strings.HasSuffix(route, "/ack"):
			return node.finish(jobIDFrom(route), requestID, true, "")
		case method == "POST" && strings.HasPrefix(route, "/internal/jobs/") && strings.HasSuffix(route, "/fail"):
			message := asString(payload["error"])
			if message == "" {
				message = "履约失败"
			}
			return node.finish(jobIDFrom(route), requestID, false, message)
		default:
			return jsonResponse(404, errorBody("not_found", "没有这个路径", requestID), nil)
		}
	}
}

func jobIDFrom(route string) string {
	parts := strings.Split(route, "/")
	if len(parts) < 4 {
		return ""
	}
	return parts[3]
}

func runStore(args []string) {
	flags := newFlagSet()
	nodeID := flags.String("node-id", "", "")
	port := flags.Int("port", 0, "")
	role := flags.String("role", "", "")
	peer := flags.String("peer", "", "")
	data := flags.String("data", "", "")
	supervisor := flags.String("supervisor", "", "")
	root := flags.String("root", ".", "")
	parseFlags(flags, args)
	setContext(*nodeID, *root)
	startFaultReader()
	node, err := openStore(*nodeID, *role, *peer, *data, *supervisor)
	if err != nil {
		panic(err)
	}
	go node.heartbeatLoop()
	go node.replicateLoop()
	go node.sweepLoop()
	logEvent("process_started", "", "info", map[string]any{"role": *role, "port": *port})
	if err := serve(*port, storeHandler(node)); err != nil {
		panic(err)
	}
}

func queryEscape(value string) string { return url.QueryEscape(value) }
