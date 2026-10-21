package orange;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** 对照 orange/common.py。参数、令牌桶、熔断、日志和故障文件。 */
final class Common {
    static final long TOKEN_CAPACITY = 30;
    static final double TOKEN_REFILL_PER_SEC = 10;
    static final long GATEWAY_INFLIGHT_LIMIT = 8;
    static final long JOBS_PENDING_LIMIT = 50;
    static final long ORDER_DEADLINE_MS = 800;
    static final long STORE_CALL_CAP_MS = 400;
    static final long RECOMMEND_BUDGET_MS = 80;
    static final double BREAKER_WINDOW_SEC = 10;
    static final int BREAKER_FAILURES = 5;
    static final double BREAKER_OPEN_SEC = 5;
    static final long VISIBILITY_MS = 15_000;
    static final double FULFILL_TIMEOUT_SEC = 1;
    static final long JOB_MAX_ATTEMPTS = 5;
    static final long BACKOFF_BASE_MS = 100;
    static final long BACKOFF_CAP_MS = 5_000;
    static final double HEARTBEAT_SEC = 0.2;
    static final double LEADER_LOST_SEC = 1;
    static final long SCALE_OUT_PENDING = 10;
    static final double SCALE_OUT_HOLD_SEC = 5;
    static final double SCALE_IN_HOLD_SEC = 20;
    static final double SCALE_COOLDOWN_SEC = 15;
    static final long WORKER_MIN = 1;
    static final long WORKER_MAX = 3;
    static final double PROMOTION_PAUSE_SEC = 2;
    static final double FAULT_POLL_SEC = 0.2;

    static volatile String processName = "";
    static volatile String root = ".";
    static volatile boolean stop;
    private static volatile Map<String, Object> faults = Map.of();
    private static final AtomicBoolean faultErrorLogged = new AtomicBoolean();
    private static final AtomicLong requestCount = new AtomicLong();
    private static final long STARTED = System.nanoTime();

    private Common() {}

    static void setContext(String process, String rootDir) {
        processName = process;
        root = rootDir == null || rootDir.isEmpty() ? "." : rootDir;
    }

    static long nowMs() {
        return System.currentTimeMillis();
    }

    static double monotonic() {
        return (System.nanoTime() - STARTED) / 1_000_000_000.0;
    }

    static String isoNow() {
        return Instant.now().truncatedTo(ChronoUnit.SECONDS).toString();
    }

    static String newId(String prefix) {
        return prefix + UUID.randomUUID().toString().replace("-", "");
    }

    static void logEvent(String event, String requestId, String level, Map<String, Object> fields) {
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("ts", isoNow());
        record.put("level", level);
        record.put("process", processName);
        record.put("event", event);
        record.put("request_id", requestId == null ? "" : requestId);
        if (fields != null) {
            record.putAll(fields);
        }
        String line = Json.stringify(record);
        System.out.println(line);
        try {
            Path directory = Path.of(root, "runtime", "logs");
            Files.createDirectories(directory);
            Files.writeString(directory.resolve(processName + ".log"), line + "\n", StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ignored) {
            // 日志写失败时仍保留标准输出。
        }
    }

    static void logEvent(String event, String requestId, Object... pairs) {
        logEvent(event, requestId, "info", Json.obj(pairs));
    }

    static Map<String, Object> currentFaults() {
        return faults;
    }

    static void startFaultReader() {
        loadFaults();
        Thread thread = new Thread(() -> {
            while (!stop) {
                sleep(FAULT_POLL_SEC);
                loadFaults();
            }
        }, "faults");
        thread.setDaemon(true);
        thread.start();
    }

    private static void loadFaults() {
        Path path = Path.of(root, "runtime", "faults.json");
        try {
            Object parsed = Json.parse(Files.readString(path, StandardCharsets.UTF_8));
            if (Json.asMap(parsed) == null) {
                noteFaultError("故障文件必须是对象");
                return;
            }
            faults = Json.asMap(parsed);
            faultErrorLogged.set(false);
        } catch (Exception ex) {
            noteFaultError(Files.exists(path) ? "故障文件不是 JSON" : "故障文件不存在");
        }
    }

    private static void noteFaultError(String message) {
        faults = Map.of();
        if (faultErrorLogged.compareAndSet(false, true)) {
            logEvent("faults_invalid", "", "error", Json.obj("message", message));
        }
    }

    static void faultDelay(String kind) {
        Object delay = Json.get(Json.get(currentFaults(), "delay_ms"), kind);
        Long ms = Json.integer(delay);
        if (ms != null && ms > 0) {
            sleep(ms / 1000.0);
        }
    }

    static boolean pauseReplication() {
        return Json.truth(Json.get(currentFaults(), "pause_replication"));
    }

    static boolean dropHeartbeat(String nodeId) {
        return Json.truth(Json.get(Json.get(currentFaults(), "drop_heartbeat"), nodeId));
    }

    static void noteHttpRequest() {
        Object limits = Json.get(currentFaults(), "exit_after_requests");
        Long limit = Json.integer(Json.get(limits, processName));
        if (limit == null || limit <= 0) {
            return;
        }
        if (requestCount.incrementAndGet() >= limit) {
            logEvent("exit_after_requests", "", "limit", limit);
            System.exit(0);
        }
    }

    static void sleep(double seconds) {
        try {
            Thread.sleep(Math.max(0, (long) (seconds * 1000)));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    static String flag(String[] args, String name, String fallback) {
        for (int i = 0; i < args.length - 1; i++) {
            if (name.equals(args[i])) {
                return args[i + 1];
            }
        }
        return fallback;
    }

    static int flagInt(String[] args, String name, int fallback) {
        try {
            return Integer.parseInt(flag(args, name, Integer.toString(fallback)));
        } catch (NumberFormatException ex) {
            return fallback;
        }
    }

    static String canonicalBody(String sku, long qty, String externalRef, String fulfillUrl) {
        return "{\"external_ref\":" + Json.quote(externalRef == null ? "" : externalRef)
                + ",\"fulfill_url\":" + Json.quote(fulfillUrl == null ? "" : fulfillUrl)
                + ",\"qty\":" + qty
                + ",\"sku\":" + Json.quote(sku) + "}";
    }

    static String textOrEmpty(Object value) {
        String text = Json.text(value);
        return text == null ? "" : text;
    }

    static boolean validCaller(String value) {
        if (value == null || value.isEmpty() || value.length() > 32) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (!(ch >= 'a' && ch <= 'z') && !(ch >= '0' && ch <= '9') && ch != '-') {
                return false;
            }
        }
        return true;
    }

    static boolean validIdempotencyKey(String value) {
        if (value == null || value.length() < 8 || value.length() > 128) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch < 32 || ch > 126) {
                return false;
            }
        }
        return true;
    }

    static boolean validFulfillUrl(String value) {
        if (value == null || value.isEmpty()) {
            return true;
        }
        if (!value.startsWith("http://127.0.0.1:") || value.indexOf('@') >= 0) {
            return false;
        }
        String rest = value.substring("http://127.0.0.1:".length());
        int slash = rest.indexOf('/');
        String port = slash < 0 ? rest : rest.substring(0, slash);
        String path = slash < 0 ? "" : rest.substring(slash + 1);
        if (port.isEmpty() || port.equals("0") || path.indexOf(' ') >= 0) {
            return false;
        }
        for (int i = 0; i < port.length(); i++) {
            if (!Character.isDigit(port.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    static Map<String, Object> validateOrderBody(Object data) {
        Map<String, Object> obj = Json.asMap(data);
        if (obj == null) {
            return errorOnly("正文必须是 JSON 对象");
        }
        String sku = Json.text(obj.get("sku"));
        if (sku == null || sku.isEmpty() || sku.length() > 64) {
            return errorOnly("sku 必须是 1 到 64 个字符");
        }
        Object qtyValue = obj.get("qty");
        if (qtyValue instanceof Boolean || !(qtyValue instanceof Long qty) || qty < 1 || qty > 100) {
            return errorOnly("qty 必须是 1 到 100 的整数");
        }
        if (obj.containsKey("external_ref") && obj.get("external_ref") != null) {
            String external = Json.text(obj.get("external_ref"));
            if (external == null || external.length() > 128) {
                return errorOnly("external_ref 最长 128 个字符");
            }
        }
        if (obj.containsKey("fulfill_url") && obj.get("fulfill_url") != null && Json.text(obj.get("fulfill_url")) == null) {
            return errorOnly("fulfill_url 必须是字符串");
        }
        String fulfill = Json.text(obj.get("fulfill_url"));
        if (!validFulfillUrl(fulfill)) {
            return errorOnly("fulfill_url 只能是 http://127.0.0.1 上的地址");
        }
        Map<String, Object> fields = Json.obj(
                "sku", sku,
                "qty", qtyValue,
                "external_ref", obj.get("external_ref"),
                "fulfill_url", fulfill == null || fulfill.isEmpty() ? null : fulfill);
        fields.put("_ok", Boolean.TRUE);
        return fields;
    }

    private static Map<String, Object> errorOnly(String message) {
        return Json.obj("_error", message);
    }

    static String fieldError(Map<String, Object> fields) {
        Object error = fields.get("_error");
        return error instanceof String text ? text : null;
    }

    static Map<String, Object> errorBody(String code, String message, String requestId) {
        return Json.obj("error", Json.obj("code", code, "message", message, "request_id", requestId));
    }

    static long backoffMs(long attempt) {
        long shift = Math.min(16, Math.max(0, attempt - 1));
        long cap = Math.min(BACKOFF_CAP_MS, BACKOFF_BASE_MS << shift);
        if (cap <= 0) {
            return 0;
        }
        return ThreadLocalRandom.current().nextLong(cap + 1);
    }

    static final class TokenBucket {
        private final double capacity;
        private final double rate;
        private double tokens;
        private double updated = monotonic();

        TokenBucket() {
            this(TOKEN_CAPACITY, TOKEN_REFILL_PER_SEC);
        }

        TokenBucket(double capacity, double rate) {
            this.capacity = capacity;
            this.rate = rate;
            this.tokens = capacity;
        }

        synchronized long[] take() {
            double now = monotonic();
            tokens = Math.min(capacity, tokens + (now - updated) * rate);
            updated = now;
            if (tokens >= 1) {
                tokens -= 1;
                return new long[] {1, 0};
            }
            if (rate <= 0) {
                return new long[] {0, 1};
            }
            long retry = (long) Math.max(1, Math.ceil((1 - tokens) / rate));
            return new long[] {0, retry};
        }
    }

    static final class CircuitBreaker {
        private final List<Double> failures = new ArrayList<>();
        private String state = "closed";
        private double openUntil;

        synchronized boolean isOpen() {
            return "open".equals(state) && monotonic() < openUntil;
        }

        synchronized boolean allow() {
            double now = monotonic();
            if ("open".equals(state)) {
                if (now < openUntil) {
                    return false;
                }
                state = "half";
                return true;
            }
            if ("half".equals(state)) {
                return false;
            }
            return true;
        }

        synchronized void record(boolean success) {
            double now = monotonic();
            if ("half".equals(state)) {
                if (success) {
                    state = "closed";
                    failures.clear();
                } else {
                    state = "open";
                    openUntil = now + BREAKER_OPEN_SEC;
                }
                return;
            }
            if (success) {
                return;
            }
            failures.removeIf(item -> now - item > BREAKER_WINDOW_SEC);
            failures.add(now);
            if (failures.size() >= BREAKER_FAILURES) {
                state = "open";
                openUntil = now + BREAKER_OPEN_SEC;
                failures.clear();
            }
        }
    }

    static final class Metrics {
        private final Map<String, Long> values = new LinkedHashMap<>();

        synchronized void inc(String name, long amount) {
            values.put(name, values.getOrDefault(name, 0L) + amount);
        }

        synchronized void set(String name, long value) {
            values.put(name, value);
        }

        synchronized String render(String... names) {
            StringBuilder out = new StringBuilder();
            for (String name : names) {
                out.append(name).append(' ').append(values.getOrDefault(name, 0L)).append('\n');
            }
            return out.toString();
        }
    }
}
