package orange;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/** 对照 orange/order_service.py。幂等、截止时间和推荐熔断都在这里。 */
final class OrderService {
    private final String supervisor;
    private final String recommender;
    private final Common.CircuitBreaker breaker = new Common.CircuitBreaker();
    private final Common.Metrics metrics = new Common.Metrics();
    private final Object lock = new Object();
    private String leader = "";
    private long epoch = 1;
    private boolean leaderOk;
    private double checkedAt;

    OrderService(String supervisor, String recommender) {
        this.supervisor = supervisor;
        this.recommender = recommender;
    }

    static void run(String[] args) {
        String name = Common.flag(args, "--process-name", "order");
        Common.setContext(name, Common.flag(args, "--root", "."));
        Common.startFaultReader();
        OrderService service = new OrderService(Common.flag(args, "--supervisor", "http://127.0.0.1:8090"), Common.flag(args, "--recommender", "http://127.0.0.1:8200"));
        Thread thread = new Thread(service::refreshLoop, "leader");
        thread.setDaemon(true);
        thread.start();
        int port = Common.flagInt(args, "--port", 8101);
        Common.logEvent("process_started", "", "port", port);
        HttpUtil.serve(port, (method, path, headers, raw) -> service.handle(method, path, headers, raw));
    }

    private HttpUtil.Response handle(String method, String path, Map<String, String> headers, byte[] raw) {
        String requestId = headers.getOrDefault("x-request-id", Common.newId("req_"));
        String route = HttpUtil.routeOf(path);
        if ("GET".equals(method) && "/health".equals(route)) {
            return HttpUtil.Response.empty(200);
        }
        if ("GET".equals(method) && "/ready".equals(route)) {
            return HttpUtil.Response.empty(currentLeader()[2] == 1 ? 200 : 503);
        }
        if ("GET".equals(method) && "/metrics".equals(route)) {
            metrics.set("orange_breaker_open", breaker.isOpen() ? 1 : 0);
            return HttpUtil.Response.text(200, metrics.render("orange_errors_total", "orange_inflight", "orange_breaker_open"));
        }
        if ("GET".equals(method) && (route.startsWith("/internal/orders/") || "/internal/orders".equals(route))) {
            return proxyRead(route, HttpUtil.splitQuery(path), headers, requestId);
        }
        if ("POST".equals(method) && "/internal/orders".equals(route)) {
            return createOrder(headers, raw, requestId);
        }
        return HttpUtil.Response.json(404, Common.errorBody("not_found", "没有这个路径", requestId));
    }

    private void refreshLoop() {
        while (!Common.stop) {
            refreshOnce();
            Common.sleep(0.2);
        }
    }

    private void refreshOnce() {
        try {
            HttpUtil.Result result = HttpUtil.request("GET", supervisor + "/internal/leader", null, Map.of(), 0.3);
            if (result.status() != 200) {
                return;
            }
            Object body = HttpUtil.decodeJson(result.body());
            String next = Json.text(Json.get(body, "leader"));
            if (next == null) {
                next = "";
            }
            boolean ready = false;
            if (!next.isEmpty()) {
                ready = HttpUtil.request("GET", next + "/ready", null, Map.of(), 0.3).status() == 200;
            }
            synchronized (lock) {
                leader = next;
                Long epochValue = Json.integer(Json.get(body, "epoch"));
                epoch = epochValue == null ? epoch : epochValue;
                leaderOk = ready;
                checkedAt = Common.monotonic();
            }
        } catch (Exception ignored) {
            // 监督者暂时不可用。
        }
    }

    private long[] currentLeader() {
        synchronized (lock) {
            boolean fresh = Common.monotonic() - checkedAt < 1 && !leader.isEmpty() && leaderOk;
            return new long[] {0, epoch, fresh ? 1 : 0};
        }
    }

    private String leaderNow() {
        synchronized (lock) {
            return leader;
        }
    }

    private long epochNow() {
        synchronized (lock) {
            return epoch;
        }
    }

    private HttpUtil.Result storeCall(String method, String url, String requestId, long deadlineMs, Object body) throws Exception {
        long remaining = deadlineMs - Common.nowMs();
        if (remaining <= 0) {
            return new HttpUtil.Result(504, Map.of(), new byte[0]);
        }
        double timeout = Math.min(remaining, Common.STORE_CALL_CAP_MS) / 1000.0;
        return HttpUtil.request(method, url, body, Map.of("X-Request-Id", requestId, "X-Deadline-Ms", Long.toString(deadlineMs)), timeout);
    }

    private HttpUtil.Response proxyRead(String route, Map<String, String> query, Map<String, String> headers, String requestId) {
        String caller = headers.getOrDefault("x-caller-id", "");
        if (!Common.validCaller(caller)) {
            return HttpUtil.Response.json(400, Common.errorBody("invalid_argument", "X-Caller-Id 不合法", requestId));
        }
        if (currentLeader()[2] != 1) {
            return HttpUtil.Response.json(503, Common.errorBody("unavailable", "主存储不可用", requestId));
        }
        long deadline = headers.containsKey("x-deadline-ms") ? Long.parseLong(headers.get("x-deadline-ms")) : Common.nowMs() + 800;
        String url = "/internal/orders".equals(route)
                ? leaderNow() + "/internal/orders/by-key?caller_id=" + HttpUtil.encode(caller) + "&idempotency_key=" + HttpUtil.encode(query.getOrDefault("idempotency_key", ""))
                : leaderNow() + "/internal/orders/" + route.substring(route.lastIndexOf('/') + 1) + "?caller_id=" + HttpUtil.encode(caller);
        try {
            HttpUtil.Result result = storeCall("GET", url, requestId, deadline, null);
            if (result.status() == 504 || result.body().length == 0) {
                return HttpUtil.Response.json(504, Common.errorBody("deadline_exceeded", "超过截止时间", requestId));
            }
            Object body = HttpUtil.decodeJson(result.body());
            Map<String, Object> map = Json.asMap(body);
            if (map != null) {
                map.remove("request_hash");
            }
            return HttpUtil.Response.json(result.status(), body);
        } catch (Exception ex) {
            metrics.inc("orange_errors_total", 1);
            return HttpUtil.Response.json(503, Common.errorBody("unavailable", "主存储不可用", requestId));
        }
    }

    private HttpUtil.Response createOrder(Map<String, String> headers, byte[] raw, String requestId) {
        String caller = headers.getOrDefault("x-caller-id", "");
        String key = headers.getOrDefault("idempotency-key", "");
        long deadline = headers.containsKey("x-deadline-ms") ? Long.parseLong(headers.get("x-deadline-ms")) : 0;
        if (!Common.validCaller(caller)) {
            return HttpUtil.Response.json(400, Common.errorBody("invalid_argument", "X-Caller-Id 不合法", requestId));
        }
        if (!Common.validIdempotencyKey(key)) {
            return HttpUtil.Response.json(400, Common.errorBody("invalid_argument", "Idempotency-Key 不合法", requestId));
        }
        if (deadline != 0 && Common.nowMs() >= deadline) {
            Common.logEvent("deadline_exceeded", requestId);
            return HttpUtil.Response.json(504, Common.errorBody("deadline_exceeded", "超过截止时间", requestId));
        }
        Object parsed;
        try {
            parsed = raw.length == 0 ? null : Json.parse(new String(raw, java.nio.charset.StandardCharsets.UTF_8));
        } catch (RuntimeException ex) {
            return HttpUtil.Response.json(400, Common.errorBody("invalid_argument", "正文不是 JSON", requestId));
        }
        Map<String, Object> fields = Common.validateOrderBody(parsed);
        if (Common.fieldError(fields) != null) {
            return HttpUtil.Response.json(400, Common.errorBody("invalid_argument", Common.fieldError(fields), requestId));
        }
        metrics.inc("orange_inflight", 1);
        try {
            HttpUtil.Response replay = lookupExisting(caller, key, fields, requestId, deadline);
            if (replay != null) {
                return replay;
            }
            return writeNew(caller, key, fields, requestId, deadline);
        } finally {
            metrics.inc("orange_inflight", -1);
        }
    }

    private HttpUtil.Response lookupExisting(String caller, String key, Map<String, Object> fields, String requestId, long deadline) {
        if (currentLeader()[2] != 1) {
            return HttpUtil.Response.json(503, Common.errorBody("unavailable", "主存储不可用", requestId));
        }
        try {
            HttpUtil.Result result = storeCall("GET", leaderNow() + "/internal/orders/by-key?caller_id=" + HttpUtil.encode(caller) + "&idempotency_key=" + HttpUtil.encode(key), requestId, deadline == 0 ? Common.nowMs() + 800 : deadline, null);
            if (result.status() != 200 || result.body().length == 0) {
                return null;
            }
            Map<String, Object> body = Json.asMap(HttpUtil.decodeJson(result.body()));
            if (body == null || body.get("order_id") == null) {
                return null;
            }
            String digest = Common.canonicalBody(Json.text(fields.get("sku")), Json.integer(fields.get("qty")), Common.textOrEmpty(fields.get("external_ref")), Common.textOrEmpty(fields.get("fulfill_url")));
            if (!digest.equals(body.get("request_hash"))) {
                return HttpUtil.Response.json(409, Common.errorBody("idempotency_mismatch", "幂等键与已有订单正文不一致", requestId));
            }
            body.remove("request_hash");
            Common.logEvent("order_replayed", requestId, "order_id", body.get("order_id"));
            return HttpUtil.Response.json(201, body);
        } catch (Exception ex) {
            return null;
        }
    }

    private HttpUtil.Response writeNew(String caller, String key, Map<String, Object> fields, String requestId, long deadline) {
        double started = Common.monotonic();
        AtomicReference<String> recommend = new AtomicReference<>();
        Thread thread = new Thread(() -> recommend.set(askRecommend(Json.text(fields.get("sku")), requestId)), "recommend");
        thread.setDaemon(true);
        thread.start();
        Object stored = callAppend(caller, key, fields, requestId, deadline);
        if (stored instanceof HttpUtil.Response response) {
            return response;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> order = (Map<String, Object>) stored;
        double remaining = Common.RECOMMEND_BUDGET_MS / 1000.0 - (Common.monotonic() - started);
        if (remaining > 0) {
            try {
                thread.join((long) (remaining * 1000));
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
        String text = recommend.get();
        if (text == null || text.isEmpty()) {
            Common.logEvent("order_accepted", requestId, "order_id", order.get("order_id"), "epoch", order.get("epoch"), "degraded", true);
            return HttpUtil.Response.json(201, order);
        }
        if (currentLeader()[2] != 1) {
            return HttpUtil.Response.json(201, order);
        }
        try {
            HttpUtil.Result result = storeCall("POST", leaderNow() + "/internal/snapshot", requestId, deadline == 0 ? Common.nowMs() + 200 : deadline,
                    Json.obj("order_id", order.get("order_id"), "epoch", epochNow(), "recommendation", text, "degraded", false));
            if (result.status() == 200 && result.body().length > 0) {
                Object body = HttpUtil.decodeJson(result.body());
                Common.logEvent("order_accepted", requestId, "order_id", Json.get(body, "order_id"), "epoch", Json.get(body, "epoch"), "degraded", false);
                return HttpUtil.Response.json(201, body);
            }
        } catch (Exception ignored) {
            // 推荐写不回去时，仍然返回已经接受的订单。
        }
        return HttpUtil.Response.json(201, order);
    }

    private String askRecommend(String sku, String requestId) {
        if (!breaker.allow()) {
            Common.logEvent("breaker_open", requestId);
            metrics.set("orange_breaker_open", 1);
            return null;
        }
        try {
            HttpUtil.Result result = HttpUtil.request("GET", recommender + "/internal/recommend?sku=" + HttpUtil.encode(sku), null, Map.of("X-Request-Id", requestId), Common.RECOMMEND_BUDGET_MS / 1000.0);
            if (result.status() != 200) {
                breaker.record(false);
                Common.logEvent("recommend_failed", requestId, "status", result.status());
                return null;
            }
            String text = Json.text(Json.get(HttpUtil.decodeJson(result.body()), "recommendation"));
            if (text == null || text.isEmpty()) {
                breaker.record(false);
                Common.logEvent("recommend_failed", requestId, "status", result.status());
                return null;
            }
            breaker.record(true);
            metrics.set("orange_breaker_open", 0);
            return text;
        } catch (Exception ex) {
            breaker.record(false);
            Common.logEvent("recommend_failed", requestId, "message", ex.toString());
            return null;
        }
    }

    private Object callAppend(String caller, String key, Map<String, Object> fields, String requestId, long deadline) {
        HttpUtil.Response last = HttpUtil.Response.json(503, Common.errorBody("unavailable", "主存储不可用", requestId));
        for (int attempt = 0; attempt < 2; attempt++) {
            if (currentLeader()[2] != 1) {
                refreshOnce();
            }
            if (currentLeader()[2] != 1) {
                return last;
            }
            try {
                HttpUtil.Result result = storeCall("POST", leaderNow() + "/internal/append", requestId, deadline == 0 ? Common.nowMs() + 800 : deadline,
                        Json.obj("epoch", epochNow(), "caller_id", caller, "idempotency_key", key, "order", fields));
                Object body = result.body().length == 0 ? null : HttpUtil.decodeJson(result.body());
                String code = Json.text(Json.get(Json.get(body, "error"), "code"));
                if (result.status() == 409 && "epoch_fenced".equals(code)) {
                    Common.logEvent("write_fenced", requestId, "epoch", Json.get(Json.get(body, "error"), "epoch"));
                    refreshOnce();
                    continue;
                }
                if (result.status() == 409) {
                    return HttpUtil.Response.json(409, body);
                }
                if (result.status() == 200 && Json.has(body, "order")) {
                    if (!Json.truth(Json.get(body, "created"))) {
                        Common.logEvent("order_replayed", requestId, "order_id", Json.get(Json.get(body, "order"), "order_id"));
                    }
                    return Json.get(body, "order");
                }
                return HttpUtil.Response.json(result.status() == 0 ? 503 : result.status(), body == null ? Common.errorBody("unavailable", "主存储不可用", requestId) : body);
            } catch (Exception ex) {
                metrics.inc("orange_errors_total", 1);
                Common.logEvent("deadline_exceeded", requestId);
                return HttpUtil.Response.json(504, Common.errorBody("deadline_exceeded", "超过截止时间", requestId));
            }
        }
        return HttpUtil.Response.json(503, Common.errorBody("unavailable", "主存储正在切换", requestId));
    }
}
