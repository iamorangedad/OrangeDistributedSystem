package orange;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 对照 orange/gateway.py。限流、截止时间，并把请求分给就绪的订单副本。 */
final class Gateway {
    private final String supervisor;
    private final List<String> orders;
    private final Common.TokenBucket bucket = new Common.TokenBucket();
    private final Common.Metrics metrics = new Common.Metrics();
    private final Object lock = new Object();
    private final Map<String, Boolean> ready = new LinkedHashMap<>();
    private boolean paused;
    private long jobsPending;
    private long inflight;
    private int cursor;

    Gateway(String supervisor, List<String> orders) {
        this.supervisor = supervisor;
        this.orders = orders;
    }

    static void run(String[] args) {
        Common.setContext(Common.flag(args, "--process-name", "gateway"), Common.flag(args, "--root", "."));
        Common.startFaultReader();
        List<String> orders = new ArrayList<>();
        for (String item : Common.flag(args, "--orders", "").split(",")) {
            if (!item.isEmpty()) {
                orders.add(item);
            }
        }
        Gateway gateway = new Gateway(Common.flag(args, "--supervisor", "http://127.0.0.1:8090"), orders);
        Thread thread = new Thread(gateway::poll, "poll");
        thread.setDaemon(true);
        thread.start();
        int port = Common.flagInt(args, "--port", 8080);
        Common.logEvent("process_started", "", "port", port);
        HttpUtil.serve(port, gateway::handle);
    }

    private void poll() {
        while (!Common.stop) {
            for (String url : orders) {
                boolean up = false;
                try {
                    up = HttpUtil.request("GET", url + "/ready", null, Map.of(), 0.3).status() == 200;
                } catch (Exception ignored) {
                    up = false;
                }
                synchronized (lock) {
                    ready.put(url, up);
                }
            }
            try {
                HttpUtil.Result result = HttpUtil.request("GET", supervisor + "/internal/gateway-control", null, Map.of(), 0.3);
                if (result.status() == 200 && result.body().length > 0) {
                    Object body = HttpUtil.decodeJson(result.body());
                    synchronized (lock) {
                        paused = Json.truth(Json.get(body, "paused"));
                        Long pending = Json.integer(Json.get(body, "jobs_pending"));
                        jobsPending = pending == null ? 0 : pending;
                    }
                }
            } catch (Exception ignored) {
                // 监督者暂时不在。
            }
            Common.sleep(0.2);
        }
    }

    private String pick() {
        synchronized (lock) {
            if (paused) {
                return null;
            }
            List<String> up = new ArrayList<>();
            for (String url : orders) {
                if (Boolean.TRUE.equals(ready.get(url))) {
                    up.add(url);
                }
            }
            if (up.isEmpty()) {
                return null;
            }
            String chosen = up.get(cursor % up.size());
            cursor++;
            return chosen;
        }
    }

    private boolean ready() {
        synchronized (lock) {
            return !paused && ready.containsValue(Boolean.TRUE);
        }
    }

    private HttpUtil.Response handle(String method, String path, Map<String, String> headers, byte[] raw) {
        String requestId = headers.getOrDefault("x-request-id", Common.newId("req_"));
        String route = HttpUtil.routeOf(path);
        if ("GET".equals(method) && "/health".equals(route)) {
            return HttpUtil.Response.empty(200);
        }
        if ("GET".equals(method) && "/ready".equals(route)) {
            return HttpUtil.Response.empty(ready() ? 200 : 503);
        }
        if ("GET".equals(method) && "/metrics".equals(route)) {
            synchronized (lock) {
                metrics.set("orange_inflight", inflight);
            }
            return HttpUtil.Response.text(200, metrics.render("orange_errors_total", "orange_inflight"));
        }
        if ("POST".equals(method) && "/v1/orders".equals(route)) {
            return create(headers, raw, requestId);
        }
        if ("GET".equals(method) && "/v1/orders".equals(route)) {
            return forwardGet("/internal/orders", HttpUtil.splitQuery(path), headers, requestId);
        }
        if ("GET".equals(method) && route.startsWith("/v1/orders/")) {
            return forwardGet("/internal/orders/" + route.substring("/v1/orders/".length()), Map.of(), headers, requestId);
        }
        return HttpUtil.Response.json(404, Common.errorBody("not_found", "没有这个路径", requestId));
    }

    private HttpUtil.Response reject(int status, String code, String message, String requestId, Long retryAfter) {
        metrics.inc("orange_errors_total", 1);
        if ("rate_limited".equals(code)) {
            Common.logEvent("rate_limited", requestId, "message", message);
        }
        if (retryAfter == null) {
            return HttpUtil.Response.json(status, Common.errorBody(code, message, requestId));
        }
        return HttpUtil.Response.json(status, Common.errorBody(code, message, requestId), "Retry-After", Long.toString(retryAfter));
    }

    private HttpUtil.Response create(Map<String, String> headers, byte[] raw, String requestId) {
        String caller = headers.getOrDefault("x-caller-id", "");
        String key = headers.getOrDefault("idempotency-key", "");
        if (!Common.validCaller(caller)) {
            return HttpUtil.Response.json(400, Common.errorBody("invalid_argument", "X-Caller-Id 不合法", requestId));
        }
        if (!Common.validIdempotencyKey(key)) {
            return HttpUtil.Response.json(400, Common.errorBody("invalid_argument", "Idempotency-Key 不合法", requestId));
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
        fields.remove("_ok");
        long[] token = bucket.take();
        if (token[0] == 0) {
            return reject(429, "rate_limited", "令牌桶已空", requestId, token[1]);
        }
        long pending;
        boolean pausedNow;
        synchronized (lock) {
            pending = jobsPending;
            pausedNow = paused;
        }
        if (pending >= Common.JOBS_PENDING_LIMIT) {
            return reject(429, "rate_limited", "待处理任务已达上限", requestId, 1L);
        }
        boolean held = false;
        synchronized (lock) {
            if (pausedNow) {
                return reject(503, "unavailable", "主存储正在切换", requestId, null);
            }
            if (inflight >= Common.GATEWAY_INFLIGHT_LIMIT) {
                return reject(429, "rate_limited", "在途请求已满", requestId, 1L);
            }
            inflight++;
            held = true;
            metrics.set("orange_inflight", inflight);
        }
        long deadline = Common.nowMs() + Common.ORDER_DEADLINE_MS;
        HttpUtil.Response response = null;
        try {
            for (int i = 0; i < orders.size(); i++) {
                String target = pick();
                if (target == null) {
                    response = reject(503, "unavailable", "没有就绪的订单副本", requestId, null);
                    break;
                }
                try {
                    HttpUtil.Result result = HttpUtil.request("POST", target + "/internal/orders", fields, Map.of(
                            "X-Caller-Id", caller, "Idempotency-Key", key, "X-Request-Id", requestId, "X-Deadline-Ms", Long.toString(deadline)),
                            Common.ORDER_DEADLINE_MS / 1000.0 + 0.2);
                    Map<String, String> extra = new LinkedHashMap<>();
                    extra.put("Content-Type", result.headers().getOrDefault("content-type", "application/json"));
                    extra.put("X-Request-Id", requestId);
                    if (result.headers().containsKey("retry-after")) {
                        extra.put("Retry-After", result.headers().get("retry-after"));
                    }
                    Common.logEvent("gateway_forward", requestId, "status", result.status());
                    response = new HttpUtil.Response(result.status(), extra, result.body());
                    break;
                } catch (Exception ex) {
                    metrics.inc("orange_errors_total", 1);
                }
            }
            if (response == null) {
                response = reject(503, "unavailable", "没有就绪的订单副本", requestId, null);
            }
            return response;
        } finally {
            if (held) {
                synchronized (lock) {
                    inflight--;
                    metrics.set("orange_inflight", inflight);
                }
            }
        }
    }

    private HttpUtil.Response forwardGet(String route, Map<String, String> query, Map<String, String> headers, String requestId) {
        String caller = headers.getOrDefault("x-caller-id", "");
        if (!Common.validCaller(caller)) {
            return HttpUtil.Response.json(400, Common.errorBody("invalid_argument", "X-Caller-Id 不合法", requestId));
        }
        if ("/internal/orders".equals(route) && (query.get("idempotency_key") == null || query.get("idempotency_key").isEmpty())) {
            return HttpUtil.Response.json(400, Common.errorBody("invalid_argument", "缺少 idempotency_key", requestId));
        }
        String target = pick();
        if (target == null) {
            return HttpUtil.Response.json(503, Common.errorBody("unavailable", "没有就绪的订单副本", requestId));
        }
        StringBuilder queryText = new StringBuilder();
        for (Map.Entry<String, String> entry : query.entrySet()) {
            queryText.append(queryText.isEmpty() ? '?' : '&').append(HttpUtil.encode(entry.getKey())).append('=').append(HttpUtil.encode(entry.getValue()));
        }
        try {
            HttpUtil.Result result = HttpUtil.request("GET", target + route + queryText, null, Map.of(
                    "X-Caller-Id", caller, "X-Request-Id", requestId, "X-Deadline-Ms", Long.toString(Common.nowMs() + Common.ORDER_DEADLINE_MS)), 1);
            return new HttpUtil.Response(result.status(), Map.of("Content-Type", "application/json; charset=utf-8", "X-Request-Id", requestId), result.body());
        } catch (Exception ex) {
            return HttpUtil.Response.json(503, Common.errorBody("unavailable", "没有就绪的订单副本", requestId));
        }
    }
}
