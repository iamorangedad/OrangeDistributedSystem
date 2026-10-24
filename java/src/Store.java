import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** 对照 orange/store.py。订单、任务和复制日志在同一个 sqlite 事务里提交。 */
final class Store {
    private static final String SCHEMA = """
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY, caller_id TEXT NOT NULL, idempotency_key TEXT NOT NULL,
                external_ref TEXT, sku TEXT NOT NULL, qty INTEGER NOT NULL, fulfill_url TEXT,
                state TEXT NOT NULL, recommendation TEXT, degraded INTEGER NOT NULL, epoch INTEGER NOT NULL,
                log_index INTEGER NOT NULL, created_at TEXT NOT NULL, response_json TEXT NOT NULL,
                request_hash TEXT NOT NULL, UNIQUE (caller_id, idempotency_key));
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY, order_id TEXT NOT NULL, kind TEXT NOT NULL, state TEXT NOT NULL,
                attempts INTEGER NOT NULL, visible_after INTEGER NOT NULL, worker_id TEXT, last_error TEXT,
                log_index INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS replication_log (
                log_index INTEGER PRIMARY KEY, epoch INTEGER NOT NULL, kind TEXT NOT NULL,
                payload TEXT NOT NULL, committed_at TEXT NOT NULL);
            """;

    private final String nodeId;
    private final String supervisor;
    private final Object lock = new Object();
    private final Connection conn;
    private String peer;
    private long followerApplied;
    private boolean replicationStarted;
    private long metricsErrors;
    private long metricsPending;
    private long metricsLag;

    Store(String nodeId, String role, String peer, String path, String supervisor) {
        this.nodeId = nodeId;
        this.peer = peer;
        this.supervisor = supervisor;
        try {
            Path file = Path.of(path);
            if (file.getParent() != null) {
                Files.createDirectories(file.getParent());
            }
            conn = DriverManager.getConnection("jdbc:sqlite:" + file.toAbsolutePath());
            try (Statement statement = conn.createStatement()) {
                try (ResultSet rs = statement.executeQuery("PRAGMA journal_mode=WAL")) {
                    rs.next();
                }
                statement.execute("PRAGMA synchronous=FULL");
                for (String sql : SCHEMA.split(";")) {
                    if (!sql.isBlank()) {
                        statement.executeUpdate(sql);
                    }
                }
            }
            ensureMeta(role);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    static void run(String[] args) {
        Common.setContext(Common.flag(args, "--node-id", "store"), Common.flag(args, "--root", "."));
        Common.startFaultReader();
        Store store = new Store(
                Common.flag(args, "--node-id", "store"),
                Common.flag(args, "--role", "follower"),
                Common.flag(args, "--peer", ""),
                Common.flag(args, "--data", "store.sqlite"),
                Common.flag(args, "--supervisor", "http://127.0.0.1:8090"));
        int port = Common.flagInt(args, "--port", 8301);
        daemon("heartbeat", store::heartbeatLoop);
        daemon("replicate", store::replicateLoop);
        daemon("sweep", store::sweepLoop);
        Common.logEvent("process_started", "", "role", Common.flag(args, "--role", "follower"), "port", port);
        HttpUtil.serve(port, store::handle);
    }

    private static void daemon(String name, Runnable task) {
        Thread thread = new Thread(task, name);
        thread.setDaemon(true);
        thread.start();
    }

    HttpUtil.Response handle(String method, String path, Map<String, String> headers, byte[] raw) {
        String requestId = headers.getOrDefault("x-request-id", Common.newId("req_"));
        String route = HttpUtil.routeOf(path);
        Map<String, String> query = HttpUtil.splitQuery(path);
        if ("GET".equals(method) && "/health".equals(route)) {
            return HttpUtil.Response.empty(200);
        }
        if ("GET".equals(method) && "/ready".equals(route)) {
            return HttpUtil.Response.empty(ready() ? 200 : 503);
        }
        if ("GET".equals(method) && "/metrics".equals(route)) {
            return HttpUtil.Response.text(200, metricsText());
        }
        if ("GET".equals(method) && "/internal/stats".equals(route)) {
            return HttpUtil.Response.json(200, stats());
        }
        if ("GET".equals(method) && "/internal/log".equals(route)) {
            long after = query.containsKey("after") ? Long.parseLong(query.get("after")) : 0;
            synchronized (lock) {
                return HttpUtil.Response.json(200, Json.obj("entries", entriesAfter(after), "applied_index", applied()));
            }
        }
        if ("POST".equals(method) && "/internal/replicate".equals(route)) {
            Object body = parse(raw);
            List<Object> entries = Json.asList(Json.get(body, "entries"));
            boolean ok = applyRemote(entries == null ? List.of() : entries);
            synchronized (lock) {
                return HttpUtil.Response.json(ok ? 200 : 409, Json.obj("applied_index", applied(), "error", ok ? "" : "gap"));
            }
        }
        if ("POST".equals(method) && "/internal/promote".equals(route)) {
            Object body = parse(raw);
            String nextPeer = Json.text(Json.get(body, "peer"));
            if (nextPeer == null || nextPeer.isEmpty()) {
                synchronized (lock) {
                    nextPeer = peer;
                }
            }
            Long epoch = Json.integer(Json.get(body, "new_epoch"));
            return promote(epoch == null ? 0 : epoch, nextPeer, requestId);
        }
        if ("POST".equals(method) && "/internal/append".equals(route)) {
            return append(parse(raw), requestId);
        }
        if ("POST".equals(method) && "/internal/snapshot".equals(route)) {
            return saveSnapshot(parse(raw), requestId);
        }
        if ("GET".equals(method) && "/internal/orders/by-key".equals(route)) {
            return readByKey(query.getOrDefault("caller_id", ""), query.getOrDefault("idempotency_key", ""), requestId);
        }
        if ("GET".equals(method) && route.startsWith("/internal/orders/")) {
            String id = route.substring(route.lastIndexOf('/') + 1);
            return readOrder(id, query.getOrDefault("caller_id", ""), requestId);
        }
        if ("POST".equals(method) && "/internal/jobs/claim".equals(route)) {
            Object body = parse(raw);
            String worker = Json.text(Json.get(body, "worker_id"));
            return claim(worker == null ? "worker" : worker, requestId);
        }
        if ("POST".equals(method) && route.startsWith("/internal/jobs/") && route.endsWith("/ack")) {
            return finish(route.split("/")[3], requestId, true, null);
        }
        if ("POST".equals(method) && route.startsWith("/internal/jobs/") && route.endsWith("/fail")) {
            Object body = parse(raw);
            String error = Json.text(Json.get(body, "error"));
            return finish(route.split("/")[3], requestId, false, error == null ? "履约失败" : error);
        }
        return HttpUtil.Response.json(404, Common.errorBody("not_found", "没有这个路径", requestId));
    }

    private HttpUtil.Response append(Object payload, String requestId) {
        Common.faultDelay("store");
        Long expected = Json.integer(Json.get(payload, "epoch"));
        String callerId = Json.text(Json.get(payload, "caller_id"));
        String key = Json.text(Json.get(payload, "idempotency_key"));
        Object fields = Json.get(payload, "order");
        String digest = Common.canonicalBody(Json.text(Json.get(fields, "sku")), Json.integer(Json.get(fields, "qty")), Common.textOrEmpty(Json.get(fields, "external_ref")), Common.textOrEmpty(Json.get(fields, "fulfill_url")));
        synchronized (lock) {
            HttpUtil.Response refused = requireLeader(requestId);
            if (refused != null) {
                return refused;
            }
            if (expected == null || expected != epoch()) {
                return fenced(requestId, expected == null ? 0 : expected);
            }
            OrderRow existing = byKey(callerId, key);
            if (existing != null) {
                if (!existing.requestHash.equals(digest)) {
                    return HttpUtil.Response.json(409, Common.errorBody("idempotency_mismatch", "幂等键与已有订单正文不一致", requestId));
                }
                Map<String, Object> body = publicBody(existing, requestId);
                return HttpUtil.Response.json(200, Json.obj("created", false, "order", body, "log_index", existing.logIndex, "epoch", epoch()));
            }
        }
        synchronized (lock) {
            HttpUtil.Response refused = requireLeader(requestId);
            if (refused != null) {
                return refused;
            }
            if (expected != epoch()) {
                return fenced(requestId, expected);
            }
            OrderRow raced = byKey(callerId, key);
            if (raced != null) {
                if (!raced.requestHash.equals(digest)) {
                    return HttpUtil.Response.json(409, Common.errorBody("idempotency_mismatch", "幂等键与已有订单正文不一致", requestId));
                }
                return HttpUtil.Response.json(200, Json.obj("created", false, "order", publicBody(raced, requestId), "log_index", raced.logIndex, "epoch", epoch()));
            }
            String orderId = Common.newId("ord_");
            String jobId = Common.newId("job_");
            String createdAt = Common.isoNow();
            long epoch = epoch();
            Map<String, Object> orderState = Json.obj(
                    "order_id", orderId, "caller_id", callerId, "external_ref", Json.get(fields, "external_ref"),
                    "sku", Json.text(Json.get(fields, "sku")), "qty", Json.integer(Json.get(fields, "qty")),
                    "state", "accepted", "recommendation", null, "degraded", true,
                    "fulfillment", Json.obj("status", "pending", "attempts", 0L, "last_error", null),
                    "epoch", epoch, "request_id", requestId);
            Map<String, Object> job = Json.obj(
                    "job_id", jobId, "order_id", orderId, "kind", "fulfill", "state", "pending",
                    "attempts", 0L, "visible_after", 0L, "worker_id", null, "last_error", null);
            final long[] index = new long[1];
            tx(() -> {
                update("INSERT INTO orders(order_id, caller_id, idempotency_key, external_ref, sku, qty, fulfill_url, state, recommendation, degraded, epoch, log_index, created_at, response_json, request_hash) VALUES (?,?,?,?,?,?,?,'accepted',NULL,1,?,0,?,?,?)",
                        orderId, callerId, key, Json.text(Json.get(fields, "external_ref")), Json.text(Json.get(fields, "sku")), Json.integer(Json.get(fields, "qty")), Json.text(Json.get(fields, "fulfill_url")), epoch, createdAt, Json.stringify(orderState), digest);
                update("INSERT INTO jobs(job_id, order_id, kind, state, attempts, visible_after, worker_id, last_error, log_index) VALUES (?,?,'fulfill','pending',0,0,NULL,NULL,0)", jobId, orderId);
                Map<String, Object> logPayload = Json.obj("order", Json.obj(
                        "order_id", orderId, "caller_id", callerId, "idempotency_key", key,
                        "external_ref", Json.get(fields, "external_ref"), "sku", Json.text(Json.get(fields, "sku")),
                        "qty", Json.integer(Json.get(fields, "qty")), "fulfill_url", Json.get(fields, "fulfill_url"),
                        "created_at", createdAt, "request_hash", digest, "response", orderState), "job", job);
                index[0] = appendLog("order_created", logPayload);
                orderState.put("epoch", epoch());
                update("UPDATE orders SET log_index = ?, epoch = ? WHERE order_id = ?", index[0], epoch(), orderId);
                update("UPDATE jobs SET log_index = ? WHERE job_id = ?", index[0], jobId);
                update("UPDATE orders SET response_json = ? WHERE order_id = ?", Json.stringify(orderState), orderId);
            });
            refreshMetrics();
            long committedEpoch = epoch();
            Common.logEvent("order_accepted", requestId, "order_id", orderId, "epoch", committedEpoch, "degraded", true);
            return HttpUtil.Response.json(200, Json.obj("created", true, "order", orderState, "log_index", index[0], "epoch", committedEpoch));
        }
    }

    private HttpUtil.Response readOrder(String orderId, String callerId, String requestId) {
        synchronized (lock) {
            HttpUtil.Response refused = requireLeader(requestId);
            if (refused != null) {
                return refused;
            }
            OrderRow order = order(orderId);
            if (order == null || !order.callerId.equals(callerId)) {
                return HttpUtil.Response.json(404, Common.errorBody("not_found", "没有这笔订单", requestId));
            }
            return HttpUtil.Response.json(200, publicBody(order, requestId));
        }
    }

    private HttpUtil.Response readByKey(String callerId, String key, String requestId) {
        synchronized (lock) {
            HttpUtil.Response refused = requireLeader(requestId);
            if (refused != null) {
                return refused;
            }
            OrderRow order = byKey(callerId, key);
            if (order == null) {
                return HttpUtil.Response.json(404, Common.errorBody("not_found", "没有这笔订单", requestId));
            }
            Map<String, Object> body = publicBody(order, requestId);
            body.put("request_hash", order.requestHash);
            return HttpUtil.Response.json(200, body);
        }
    }

    private HttpUtil.Response saveSnapshot(Object payload, String requestId) {
        String orderId = Json.text(Json.get(payload, "order_id"));
        synchronized (lock) {
            HttpUtil.Response refused = requireLeader(requestId);
            if (refused != null) {
                return refused;
            }
            Long expected = Json.integer(Json.get(payload, "epoch"));
            if (expected == null || expected != epoch()) {
                return fenced(requestId, expected == null ? 0 : expected);
            }
            OrderRow order = order(orderId);
            if (order == null || jobForOrder(orderId) == null) {
                return HttpUtil.Response.json(404, Common.errorBody("not_found", "没有这笔订单", requestId));
            }
            Map<String, Object> body = publicBody(order, requestId);
            body.put("recommendation", Json.get(payload, "recommendation"));
            body.put("degraded", Json.truth(Json.get(payload, "degraded")));
            body.put("request_id", requestId);
            body.put("epoch", epoch());
            Map<String, Object> saved = body;
            tx(() -> {
                writeSnapshot(orderId, saved);
                appendLog("order_snapshot", Json.obj("order_id", orderId, "response", saved));
            });
            return HttpUtil.Response.json(200, saved);
        }
    }

    private HttpUtil.Response claim(String workerId, String requestId) {
        synchronized (lock) {
            HttpUtil.Response refused = requireLeader(requestId);
            if (refused != null) {
                return refused;
            }
            releaseExpired();
            JobRow row = queryJob("SELECT * FROM jobs WHERE state = 'pending' AND visible_after <= ? ORDER BY job_id LIMIT 1", Common.nowMs());
            if (row == null) {
                return HttpUtil.Response.empty(204);
            }
            long attempts = row.attempts + 1;
            long visible = Common.nowMs() + Common.VISIBILITY_MS;
            tx(() -> {
                update("UPDATE jobs SET state = 'inflight', attempts = ?, visible_after = ?, worker_id = ? WHERE job_id = ?", attempts, visible, workerId, row.jobId);
                OrderRow order = order(row.orderId);
                JobRow job = jobForOrder(row.orderId);
                Map<String, Object> body = snapshot(order, job, requestId);
                writeSnapshot(order.orderId, body);
                appendLog("job_claimed", Json.obj("job_id", row.jobId, "order_id", row.orderId, "response", body, "worker_id", workerId));
            });
            refreshMetrics();
            JobRow claimed = jobForOrder(row.orderId);
            OrderRow order = order(row.orderId);
            Common.logEvent("job_claimed", requestId, "order_id", order.orderId, "worker_id", workerId);
            return HttpUtil.Response.json(200, Json.obj(
                    "job_id", claimed.jobId, "order_id", order.orderId, "caller_id", order.callerId,
                    "external_ref", order.externalRef, "sku", order.sku, "qty", order.qty,
                    "fulfill_url", order.fulfillUrl, "attempts", claimed.attempts));
        }
    }

    private HttpUtil.Response finish(String jobId, String requestId, boolean success, String error) {
        synchronized (lock) {
            HttpUtil.Response refused = requireLeader(requestId);
            if (refused != null) {
                return refused;
            }
            JobRow job = queryJob("SELECT * FROM jobs WHERE job_id = ?", jobId);
            if (job == null) {
                return HttpUtil.Response.json(404, Common.errorBody("not_found", "没有这个任务", requestId));
            }
            String[] event = new String[1];
            Map<String, Object>[] saved = new Map[1];
            String[] orderId = new String[1];
            tx(() -> {
                String kind;
                if (success) {
                    update("UPDATE jobs SET state = 'done', last_error = NULL, worker_id = NULL WHERE job_id = ?", jobId);
                    kind = "job_done";
                    event[0] = "job_done";
                } else if (job.attempts >= Common.JOB_MAX_ATTEMPTS) {
                    update("UPDATE jobs SET state = 'dead', last_error = ?, worker_id = NULL WHERE job_id = ?", error, jobId);
                    kind = "job_dead";
                    event[0] = "job_dead";
                } else {
                    long wait = Common.backoffMs(job.attempts);
                    update("UPDATE jobs SET state = 'pending', last_error = ?, worker_id = NULL, visible_after = ? WHERE job_id = ?", error, Common.nowMs() + wait, jobId);
                    kind = "job_reset";
                    event[0] = "job_retry";
                }
                JobRow current = queryJob("SELECT * FROM jobs WHERE job_id = ?", jobId);
                OrderRow order = order(current.orderId);
                String state = switch (current.state) {
                    case "done" -> "fulfilled";
                    case "dead" -> "fulfillment_failed";
                    default -> "accepted";
                };
                Map<String, Object> body = publicBody(order, requestId);
                body.put("state", state);
                body.put("fulfillment", fulfillmentOf(current));
                body.put("epoch", epoch());
                update("UPDATE orders SET state = ? WHERE order_id = ?", state, order.orderId);
                writeSnapshot(order.orderId, body);
                appendLog(kind, Json.obj("job_id", jobId, "order_id", order.orderId, "response", body));
                saved[0] = body;
                orderId[0] = order.orderId;
            });
            refreshMetrics();
            Common.logEvent(event[0], requestId, "order_id", orderId[0], "job_id", jobId);
            return HttpUtil.Response.json(200, saved[0]);
        }
    }

    private HttpUtil.Response promote(long newEpoch, String nextPeer, String requestId) {
        synchronized (lock) {
            if (newEpoch <= epoch() && isLeader()) {
                return HttpUtil.Response.json(200, Json.obj("epoch", epoch(), "role", role()));
            }
            tx(() -> {
                setMeta("role", "leader");
                setMeta("epoch", Long.toString(newEpoch));
                appendLog("epoch_promoted", Json.obj("epoch", newEpoch, "node_id", nodeId));
            });
            peer = nextPeer;
            followerApplied = 0;
            refreshMetrics();
            Common.logEvent("promoted", requestId, "epoch", newEpoch, "node_id", nodeId);
            return HttpUtil.Response.json(200, Json.obj("epoch", newEpoch, "role", "leader"));
        }
    }

    private void stepDown(long newEpoch, String leader) {
        synchronized (lock) {
            if (isLeader() && newEpoch <= epoch()) {
                return;
            }
            if (!isLeader() && newEpoch == epoch() && leader.equals(peer)) {
                return;
            }
            tx(() -> {
                update("DELETE FROM orders");
                update("DELETE FROM jobs");
                update("DELETE FROM replication_log");
                setMeta("role", "follower");
                setMeta("epoch", Long.toString(newEpoch));
                setMeta("applied_index", "0");
            });
            peer = leader;
        }
        Common.logEvent("stepped_down", "", "epoch", newEpoch, "leader", leader);
    }

    private boolean applyRemote(List<Object> entries) {
        synchronized (lock) {
            try {
                conn.setAutoCommit(false);
                for (Object item : entries) {
                    long expected = applied() + 1;
                    Long index = Json.integer(Json.get(item, "log_index"));
                    if (index == null || index != expected) {
                        conn.rollback();
                        conn.setAutoCommit(true);
                        return false;
                    }
                    String kind = Json.text(Json.get(item, "kind"));
                    long epoch = Json.integer(Json.get(item, "epoch"));
                    applyOne(kind, Json.get(item, "payload"), epoch);
                    String committed = Json.text(Json.get(item, "committed_at"));
                    update("INSERT INTO replication_log(log_index, epoch, kind, payload, committed_at) VALUES (?,?,?,?,?)",
                            index, epoch, kind, Json.stringify(Json.get(item, "payload")), committed == null ? Common.isoNow() : committed);
                    setMeta("applied_index", Long.toString(index));
                    if ("epoch_promoted".equals(kind)) {
                        Long promoted = Json.integer(Json.get(Json.get(item, "payload"), "epoch"));
                        setMeta("epoch", Long.toString(promoted == null ? epoch : promoted));
                        setMeta("role", "follower");
                    }
                }
                conn.commit();
                conn.setAutoCommit(true);
                refreshMetrics();
                return true;
            } catch (SQLException ex) {
                try {
                    conn.rollback();
                    conn.setAutoCommit(true);
                } catch (SQLException ignored) {
                    // 回滚失败时留下异常给下一次写入。
                }
                throw new IllegalStateException(ex);
            }
        }
    }

    private Map<String, Object> stats() {
        synchronized (lock) {
            refreshMetrics();
            return Json.obj("node_id", nodeId, "role", role(), "epoch", epoch(), "applied_index", applied(), "jobs_pending", metricsPending, "replicate_lag", metricsLag);
        }
    }

    private boolean ready() {
        synchronized (lock) {
            return isLeader() || replicationStarted;
        }
    }

    private String metricsText() {
        Map<String, Object> view = stats();
        synchronized (lock) {
            boolean leader = isLeader();
            long pending = leader ? Json.integer(view.get("jobs_pending")) : 0;
            long lag = leader ? Json.integer(view.get("replicate_lag")) : 0;
            return "orange_errors_total " + metricsErrors + "\norange_jobs_pending " + pending + "\norange_replicate_lag " + lag + "\n";
        }
    }

    private void heartbeatLoop() {
        while (!Common.stop) {
            Common.sleep(Common.HEARTBEAT_SEC);
            if (Common.dropHeartbeat(nodeId)) {
                continue;
            }
            Map<String, Object> view = stats();
            long epoch;
            boolean leader;
            synchronized (lock) {
                epoch = epoch();
                leader = isLeader();
            }
            try {
                HttpUtil.Result result = HttpUtil.request("POST", supervisor + "/internal/heartbeat", view, Map.of(), 0.5);
                if (result.status() == 200) {
                    Object body = HttpUtil.decodeJson(result.body());
                    String remoteLeader = Json.text(Json.get(body, "leader"));
                    Long remoteEpoch = Json.integer(Json.get(body, "epoch"));
                    if (remoteLeader != null && !remoteLeader.isEmpty() && remoteEpoch != null && remoteEpoch > epoch && leader) {
                        stepDown(remoteEpoch, remoteLeader);
                    } else if (remoteLeader != null && !remoteLeader.isEmpty() && !leader) {
                        synchronized (lock) {
                            peer = remoteLeader;
                        }
                    }
                }
            } catch (Exception ignored) {
                // 监督者暂时不在，下一拍再报。
            }
        }
    }

    private void replicateLoop() {
        synchronized (lock) {
            replicationStarted = true;
        }
        while (!Common.stop) {
            Common.sleep(Common.HEARTBEAT_SEC);
            boolean leader;
            synchronized (lock) {
                leader = isLeader();
            }
            if (leader) {
                push();
            } else {
                pull();
            }
        }
    }

    private void push() {
        if (Common.pauseReplication()) {
            return;
        }
        List<Object> entries;
        String target;
        long appliedAt;
        synchronized (lock) {
            if (peer == null || peer.isEmpty()) {
                return;
            }
            entries = entriesAfter(followerApplied);
            target = peer;
            appliedAt = followerApplied;
        }
        if (entries.isEmpty()) {
            synchronized (lock) {
                refreshMetrics();
            }
            return;
        }
        try {
            HttpUtil.Result result = HttpUtil.request("POST", target + "/internal/replicate", Json.obj("entries", entries), Map.of(), 0.5);
            Object body = HttpUtil.decodeJson(result.body());
            if (result.status() == 200 || result.status() == 409) {
                Long applied = Json.integer(Json.get(body, "applied_index"));
                if (applied != null) {
                    synchronized (lock) {
                        followerApplied = result.status() == 200 ? applied : applied;
                    }
                }
            }
        } catch (Exception ex) {
            long lag;
            synchronized (lock) {
                lag = applied() - appliedAt;
            }
            Common.logEvent("replicate_lag", "", "error", Json.obj("message", ex.toString(), "lag", lag));
        }
        long lag;
        synchronized (lock) {
            refreshMetrics();
            lag = metricsLag;
        }
        if (lag != 0) {
            Common.logEvent("replicate_lag", "", "lag", lag);
        }
    }

    private void pull() {
        String target;
        long after;
        synchronized (lock) {
            target = peer;
            after = applied();
        }
        if (target == null || target.isEmpty()) {
            return;
        }
        try {
            HttpUtil.Result result = HttpUtil.request("GET", target + "/internal/log?after=" + after, null, Map.of(), 0.5);
            if (result.status() != 200) {
                return;
            }
            List<Object> entries = Json.asList(Json.get(HttpUtil.decodeJson(result.body()), "entries"));
            if (entries != null && !entries.isEmpty()) {
                applyRemote(entries);
            }
        } catch (Exception ignored) {
            // 主存储暂时连不上。
        }
    }

    private void sweepLoop() {
        while (!Common.stop) {
            Common.sleep(Common.HEARTBEAT_SEC);
            synchronized (lock) {
                if (!isLeader()) {
                    continue;
                }
                releaseExpired();
                refreshMetrics();
            }
        }
    }

    private void releaseExpired() {
        List<JobRow> rows = queryJobs("SELECT * FROM jobs WHERE state = 'inflight' AND visible_after <= ?", Common.nowMs());
        for (JobRow row : rows) {
            tx(() -> {
                update("UPDATE jobs SET state = 'pending', worker_id = NULL WHERE job_id = ? AND state = 'inflight'", row.jobId);
                OrderRow order = order(row.orderId);
                JobRow job = jobForOrder(row.orderId);
                if (order != null && job != null) {
                    Map<String, Object> body = publicBody(order, "");
                    body.put("state", "accepted");
                    body.put("fulfillment", fulfillmentOf(job));
                    writeSnapshot(order.orderId, body);
                    appendLog("job_reset", Json.obj("job_id", row.jobId, "order_id", row.orderId, "response", body));
                }
            });
            Common.logEvent("job_retry", "", "order_id", row.orderId, "job_id", row.jobId, "reason", "visibility_timeout");
        }
    }

    private void applyOne(String kind, Object payload, long epoch) throws SQLException {
        if ("order_created".equals(kind)) {
            Object order = Json.get(payload, "order");
            Object job = Json.get(payload, "job");
            Object response = Json.get(order, "response");
            update("INSERT OR REPLACE INTO orders(order_id, caller_id, idempotency_key, external_ref, sku, qty, fulfill_url, state, recommendation, degraded, epoch, log_index, created_at, response_json, request_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?,0,?,?,?)",
                    Json.text(Json.get(order, "order_id")), Json.text(Json.get(order, "caller_id")), Json.text(Json.get(order, "idempotency_key")),
                    Json.text(Json.get(order, "external_ref")), Json.text(Json.get(order, "sku")), Json.integer(Json.get(order, "qty")),
                    Json.text(Json.get(order, "fulfill_url")), Json.text(Json.get(response, "state")), Json.text(Json.get(response, "recommendation")),
                    Json.truth(Json.get(response, "degraded")) ? 1 : 0, epoch, Json.text(Json.get(order, "created_at")), Json.stringify(response),
                    Json.text(Json.get(order, "request_hash")));
            update("INSERT OR REPLACE INTO jobs(job_id, order_id, kind, state, attempts, visible_after, worker_id, last_error, log_index) VALUES (?,?,?,?,?,?,?,?,0)",
                    Json.text(Json.get(job, "job_id")), Json.text(Json.get(job, "order_id")), Json.text(Json.get(job, "kind")),
                    Json.text(Json.get(job, "state")), Json.integer(Json.get(job, "attempts")), Json.integer(Json.get(job, "visible_after")),
                    Json.text(Json.get(job, "worker_id")), Json.text(Json.get(job, "last_error")));
            return;
        }
        if (Json.has(payload, "response") && Json.text(Json.get(payload, "order_id")) != null) {
            Object body = Json.get(payload, "response");
            String orderId = Json.text(Json.get(payload, "order_id"));
            writeSnapshot(orderId, Json.asMap(body));
            if (!"order_snapshot".equals(kind)) {
                Object fulfillment = Json.get(body, "fulfillment");
                update("UPDATE jobs SET state = ?, attempts = ?, last_error = ?, worker_id = ? WHERE order_id = ?",
                        Json.text(Json.get(fulfillment, "status")), Json.integer(Json.get(fulfillment, "attempts")),
                        Json.text(Json.get(fulfillment, "last_error")), Json.text(Json.get(payload, "worker_id")), orderId);
            }
        }
    }

    private Map<String, Object> snapshot(OrderRow order, JobRow job, String requestId) {
        return Json.obj(
                "order_id", order.orderId, "caller_id", order.callerId, "external_ref", order.externalRef,
                "sku", order.sku, "qty", order.qty, "state", order.state, "recommendation", order.recommendation,
                "degraded", order.degraded != 0, "fulfillment", fulfillmentOf(job), "epoch", order.epoch, "request_id", requestId);
    }

    private Map<String, Object> fulfillmentOf(JobRow job) {
        return Json.obj("status", job.state, "attempts", job.attempts, "last_error", job.lastError);
    }

    private Map<String, Object> publicBody(OrderRow order, String requestId) {
        Map<String, Object> saved = Json.asMap(Json.parse(order.responseJson));
        if (requestId != null && !requestId.isEmpty() && saved != null) {
            saved.put("request_id", requestId);
        }
        return saved == null ? Json.obj() : saved;
    }

    private void writeSnapshot(String orderId, Map<String, Object> body) throws SQLException {
        update("UPDATE orders SET response_json = ?, state = ?, recommendation = ?, degraded = ? WHERE order_id = ?",
                Json.stringify(body), Json.text(body.get("state")), Json.text(body.get("recommendation")), Json.truth(body.get("degraded")) ? 1 : 0, orderId);
    }

    private HttpUtil.Response requireLeader(String requestId) {
        if (isLeader()) {
            return null;
        }
        metricsErrors++;
        Common.logEvent("write_fenced", requestId, "epoch", epoch());
        return HttpUtil.Response.json(409, Json.obj("error", Json.obj("code", "epoch_fenced", "message", "当前节点不接受写入", "request_id", requestId, "epoch", epoch())));
    }

    private HttpUtil.Response fenced(String requestId, long expected) {
        metricsErrors++;
        Common.logEvent("write_fenced", requestId, "epoch", epoch(), "expected", expected);
        return HttpUtil.Response.json(409, Json.obj("error", Json.obj("code", "epoch_fenced", "message", "写入代际已过期", "request_id", requestId, "epoch", epoch())));
    }

    private void ensureMeta(String role) throws SQLException {
        try (Statement statement = conn.createStatement(); ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM meta")) {
            rs.next();
            if (rs.getLong(1) > 0) {
                return;
            }
        }
        for (String[] pair : new String[][] {{"role", role}, {"epoch", "1"}, {"applied_index", "0"}, {"node_id", nodeId}}) {
            update("INSERT INTO meta(key, value) VALUES (?, ?)", pair[0], pair[1]);
        }
    }

    private String meta(String key) {
        try (PreparedStatement statement = conn.prepareStatement("SELECT value FROM meta WHERE key = ?")) {
            statement.setString(1, key);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? rs.getString(1) : "";
            }
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private void setMeta(String key, String value) throws SQLException {
        update("INSERT INTO meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", key, value);
    }

    private String role() {
        return meta("role");
    }

    private long epoch() {
        try {
            return Long.parseLong(meta("epoch"));
        } catch (NumberFormatException ex) {
            return 1;
        }
    }

    private long applied() {
        try {
            return Long.parseLong(meta("applied_index"));
        } catch (NumberFormatException ex) {
            return 0;
        }
    }

    private boolean isLeader() {
        return "leader".equals(role());
    }

    private void refreshMetrics() {
        try (Statement statement = conn.createStatement(); ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM jobs WHERE state = 'pending'")) {
            rs.next();
            metricsPending = rs.getLong(1);
            metricsLag = isLeader() ? Math.max(0, applied() - followerApplied) : 0;
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private long appendLog(String kind, Object payload) throws SQLException {
        long index = applied() + 1;
        update("INSERT INTO replication_log(log_index, epoch, kind, payload, committed_at) VALUES (?,?,?,?,?)", index, epoch(), kind, Json.stringify(payload), Common.isoNow());
        setMeta("applied_index", Long.toString(index));
        return index;
    }

    private List<Object> entriesAfter(long index) {
        List<Object> entries = new ArrayList<>();
        try (PreparedStatement statement = conn.prepareStatement("SELECT * FROM replication_log WHERE log_index > ? ORDER BY log_index")) {
            statement.setLong(1, index);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    entries.add(Json.obj(
                            "log_index", rs.getLong("log_index"),
                            "epoch", rs.getLong("epoch"),
                            "kind", rs.getString("kind"),
                            "payload", Json.parse(rs.getString("payload")),
                            "committed_at", rs.getString("committed_at")));
                }
            }
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
        return entries;
    }

    private void tx(SqlWork work) {
        try {
            conn.setAutoCommit(false);
            try {
                work.run();
                conn.commit();
            } catch (Exception ex) {
                conn.rollback();
                if (ex instanceof RuntimeException runtime) {
                    throw runtime;
                }
                throw new IllegalStateException(ex);
            } finally {
                conn.setAutoCommit(true);
            }
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private void update(String sql, Object... params) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                statement.setObject(i + 1, params[i]);
            }
            statement.executeUpdate();
        }
    }

    private OrderRow order(String orderId) {
        return queryOrder("SELECT * FROM orders WHERE order_id = ?", orderId);
    }

    private OrderRow byKey(String callerId, String key) {
        return queryOrder("SELECT * FROM orders WHERE caller_id = ? AND idempotency_key = ?", callerId, key);
    }

    private JobRow jobForOrder(String orderId) {
        return queryJob("SELECT * FROM jobs WHERE order_id = ?", orderId);
    }

    private OrderRow queryOrder(String sql, Object... params) {
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            bind(statement, params);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                OrderRow row = new OrderRow();
                row.orderId = rs.getString("order_id");
                row.callerId = rs.getString("caller_id");
                row.externalRef = rs.getString("external_ref");
                row.sku = rs.getString("sku");
                row.qty = rs.getLong("qty");
                row.fulfillUrl = rs.getString("fulfill_url");
                row.state = rs.getString("state");
                row.recommendation = rs.getString("recommendation");
                row.degraded = rs.getLong("degraded");
                row.epoch = rs.getLong("epoch");
                row.logIndex = rs.getLong("log_index");
                row.responseJson = rs.getString("response_json");
                row.requestHash = rs.getString("request_hash");
                return row;
            }
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private JobRow queryJob(String sql, Object... params) {
        List<JobRow> rows = queryJobs(sql, params);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private List<JobRow> queryJobs(String sql, Object... params) {
        List<JobRow> rows = new ArrayList<>();
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            bind(statement, params);
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    JobRow row = new JobRow();
                    row.jobId = rs.getString("job_id");
                    row.orderId = rs.getString("order_id");
                    row.state = rs.getString("state");
                    row.attempts = rs.getLong("attempts");
                    row.visibleAfter = rs.getLong("visible_after");
                    row.workerId = rs.getString("worker_id");
                    row.lastError = rs.getString("last_error");
                    rows.add(row);
                }
            }
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
        return rows;
    }

    private static void bind(PreparedStatement statement, Object... params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            statement.setObject(i + 1, params[i]);
        }
    }

    private static Object parse(byte[] raw) {
        try {
            Object value = HttpUtil.decodeJson(raw);
            return value == null ? Json.obj() : value;
        } catch (RuntimeException ex) {
            return Json.obj();
        }
    }

    @FunctionalInterface
    private interface SqlWork {
        void run() throws SQLException;
    }

    private static final class OrderRow {
        String orderId;
        String callerId;
        String externalRef;
        String sku;
        String fulfillUrl;
        String state;
        String recommendation;
        String responseJson;
        String requestHash;
        long qty;
        long degraded;
        long epoch;
        long logIndex;
    }

    private static final class JobRow {
        String jobId;
        String orderId;
        String state;
        String workerId;
        String lastError;
        long attempts;
        long visibleAfter;
    }
}
