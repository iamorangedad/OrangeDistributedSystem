import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/** 对照 tests/test_acceptance.py。端口固定，所以测试串行执行。 */
public final class Acceptance {
    private static final String GATEWAY = "http://127.0.0.1:8080";
    private static int failed;

    public static void main(String[] args) throws Exception {
        unit();
        run("AC-1", Acceptance::ac1);
        run("AC-2", Acceptance::ac2);
        run("AC-3", Acceptance::ac3);
        run("AC-4", Acceptance::ac4);
        run("AC-5", Acceptance::ac5);
        run("AC-6", Acceptance::ac6);
        run("AC-7", Acceptance::ac7);
        run("AC-8", Acceptance::ac8);
        run("AC-9", Acceptance::ac9);
        run("AC-10", Acceptance::ac10);
        run("AC-11", Acceptance::ac11);
        run("AC-12", Acceptance::ac12);
        if (failed > 0) {
            System.err.println(failed + " 个验收失败");
            System.exit(1);
        }
        System.out.println("Java 验收通过");
    }

    private static void unit() {
        check("{\"external_ref\":\"cart\",\"fulfill_url\":\"\",\"qty\":1,\"sku\":\"gear-1\"}".equals(Common.canonicalBody("gear-1", 1, "cart", "")), "canonical");
        check(Common.fieldError(Common.validateOrderBody(Json.obj("sku", "gear-1", "qty", true))) != null, "qty bool");
        check(Common.validFulfillUrl("http://127.0.0.1:9000/hooks"), "loopback");
        check(!Common.validFulfillUrl("https://127.0.0.1:9000/hooks"), "https");
        check(!Common.validFulfillUrl("http://127.0.0.1/hooks"), "missing port");
        Common.TokenBucket bucket = new Common.TokenBucket(1, 0);
        check(bucket.take()[0] == 1, "token");
        check(bucket.take()[0] == 0, "token empty");
        Common.CircuitBreaker breaker = new Common.CircuitBreaker();
        for (int i = 0; i < 5; i++) {
            check(breaker.allow(), "breaker allow");
            breaker.record(false);
        }
        check(!breaker.allow(), "breaker open");
    }

    private static void ac1() throws Exception {
        try (Cluster cluster = Cluster.start()) {
            Result first = post("gear-1", 1, "same-key-0001", null);
            check(first.status == 201, "ac1 first " + first.body + cluster.diagnostics());
            Result second = post("gear-1", 1, "same-key-0001", null);
            check(second.status == 201, "ac1 second");
            check(String.valueOf(Json.get(first.body, "order_id")).equals(String.valueOf(Json.get(second.body, "order_id"))), "ac1 id");
        }
    }

    private static void ac2() throws Exception {
        try (Cluster cluster = Cluster.start()) {
            Result first = post("gear-1", 1, "same-key-0002", null);
            check(first.status == 201, "ac2 " + first.body + cluster.diagnostics());
            Result second = post("gear-2", 1, "same-key-0002", null);
            check(second.status == 409, "ac2 status " + second.body);
            check("idempotency_mismatch".equals(Json.text(Json.get(Json.get(second.body, "error"), "code"))), "ac2 code");
            HttpUtil.Result got = HttpUtil.request("GET", GATEWAY + "/v1/orders/" + Json.text(Json.get(first.body, "order_id")), null, Map.of("X-Caller-Id", "gear-card"), 2);
            check(got.status() == 200, "ac2 get");
            check("gear-1".equals(Json.text(Json.get(HttpUtil.decodeJson(got.body()), "sku"))), "ac2 sku");
        }
    }

    private static void ac3() throws Exception {
        try (Cluster cluster = Cluster.start()) {
            long pid = Json.integer(Json.get(Json.get(cluster.status(), "pids"), "order-2"));
            new ProcessBuilder("kill", "-TERM", Long.toString(pid)).start().waitFor();
            boolean sawDown = false;
            boolean restored = false;
            long deadline = System.nanoTime() + 8_000_000_000L;
            while (System.nanoTime() < deadline) {
                Object view = cluster.status();
                boolean ready = Json.truth(Json.get(Json.get(view, "ready"), "order-2"));
                sawDown = sawDown || !ready;
                Long next = Json.integer(Json.get(Json.get(view, "pids"), "order-2"));
                if (ready && next != null && next != pid) {
                    restored = true;
                    break;
                }
                Common.sleep(0.1);
            }
            Result body = post("gear-1", 1, "after-order-b", null);
            check(body.status == 201, "ac3 post " + body.body);
            check(sawDown, cluster.diagnostics());
            check(restored, cluster.diagnostics());
        }
    }

    private static void ac4() throws Exception {
        try (Cluster cluster = Cluster.start()) {
            long pid = Json.integer(Json.get(Json.get(cluster.status(), "pids"), "recommender"));
            new ProcessBuilder("kill", "-TERM", Long.toString(pid)).start().waitFor();
            long deadline = System.nanoTime() + 3_000_000_000L;
            Result body = null;
            while (System.nanoTime() < deadline) {
                body = post("gear-1", 1, "degrade-" + System.nanoTime(), null);
                if (body.status == 201 && Json.truth(Json.get(body.body, "degraded")) && Json.get(body.body, "recommendation") == null) {
                    break;
                }
                Common.sleep(0.05);
            }
            check(body != null && body.status == 201 && Json.truth(Json.get(body.body, "degraded")), "ac4 " + body + cluster.diagnostics());
        }
    }

    private static void ac5() throws Exception {
        try (Cluster cluster = Cluster.start()) {
            cluster.faults(0, 500, false);
            for (int i = 0; i < 12; i++) {
                Result body = post("gear-1", 1, String.format("breaker-%02d-key", i), null);
                check(body.status == 201, "ac5 " + body.body + cluster.diagnostics());
            }
            Common.sleep(0.3);
            int before = count(cluster.logs("recommender"), "\"event\":\"recommend_served\"");
            Result afterOrder = post("gear-1", 1, "breaker-after-open", null);
            check(afterOrder.status == 201, "ac5 after");
            Common.sleep(0.3);
            int after = count(cluster.logs("recommender"), "\"event\":\"recommend_served\"");
            check(before == after, cluster.logs("recommender"));
            check((cluster.logs("order-1") + cluster.logs("order-2")).contains("\"event\":\"breaker_open\""), "breaker");
        }
    }

    private static void ac6() throws Exception {
        try (Cluster cluster = Cluster.start()) {
            cluster.faults(350, 0, false);
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                int index = i;
                Thread thread = new Thread(() -> post("gear-1", 1, String.format("hold-%02d-xxxx", index), null));
                thread.start();
                threads.add(thread);
            }
            long deadline = System.nanoTime() + 2_000_000_000L;
            while (System.nanoTime() < deadline) {
                HttpUtil.Result metrics = httpQuiet("GET", GATEWAY + "/metrics", null, Map.of());
                if (metrics != null && new String(metrics.body(), StandardCharsets.UTF_8).contains("orange_inflight 8")) {
                    break;
                }
                Common.sleep(0.02);
            }
            // 40 个请求一起发出。Java 的 HTTP 客户端比 Python 慢，串行发送时每个在途请求会等存储延迟，令牌会在打空之前补上。
            List<Result> results = new CopyOnWriteArrayList<>();
            for (int i = 0; i < 40; i++) {
                int index = i;
                Thread thread = new Thread(() -> results.add(post("gear-1", 1, String.format("bucket-%02d-xx", index), null)));
                thread.start();
                threads.add(thread);
            }
            for (Thread thread : threads) {
                thread.join(5000);
            }
            boolean limited = false;
            Result sample = null;
            for (Result result : results) {
                if (result.status == 429 && "令牌桶已空".equals(Json.text(Json.get(Json.get(result.body, "error"), "message")))) {
                    limited = true;
                    sample = result;
                    break;
                }
            }
            StringBuilder summary = new StringBuilder();
            for (Result result : results) {
                summary.append(result.status).append(' ');
                Object error = Json.get(result.body, "error");
                summary.append(error == null ? "ok" : Json.text(Json.get(error, "message"))).append("; ");
            }
            check(limited, "ac6 " + summary);
            check(sample != null && sample.headers.containsKey("retry-after"), "ac6 retry");
        }
    }

    private static void ac7() throws Exception {
        try (Cluster cluster = Cluster.start(); Fulfill fulfill = Fulfill.start(200, 0, true)) {
            Result body = post("gear-1", 1, "visibility-timeout", Json.obj("fulfill_url", fulfill.url()));
            check(body.status == 201, "ac7 " + body.body + cluster.diagnostics());
            long deadline = System.nanoTime() + 5_000_000_000L;
            while (System.nanoTime() < deadline && fulfill.calls.isEmpty()) {
                Common.sleep(0.05);
            }
            check(!fulfill.calls.isEmpty(), cluster.diagnostics());
            long pid = Json.integer(Json.get(Json.get(cluster.status(), "pids"), "worker-1"));
            new ProcessBuilder("kill", "-TERM", Long.toString(pid)).start().waitFor();
            fulfill.release();
            deadline = System.nanoTime() + 25_000_000_000L;
            String state = "";
            while (System.nanoTime() < deadline) {
                HttpUtil.Result got = HttpUtil.request("GET", GATEWAY + "/v1/orders/" + Json.text(Json.get(body.body, "order_id")), null, Map.of("X-Caller-Id", "gear-card"), 2);
                check(got.status() == 200, cluster.diagnostics());
                state = Json.text(Json.get(HttpUtil.decodeJson(got.body()), "state"));
                if ("fulfilled".equals(state)) {
                    break;
                }
                Common.sleep(0.5);
            }
            check("fulfilled".equals(state), cluster.diagnostics());
            check(fulfill.applied.size() == 1, "ac7 applied");
        }
    }

    private static void ac8() throws Exception {
        try (Cluster cluster = Cluster.start(); Fulfill fulfill = Fulfill.start(500, 0, false)) {
            Result body = post("gear-1", 1, "dead-letter-0001", Json.obj("fulfill_url", fulfill.url()));
            check(body.status == 201, "ac8 " + body.body);
            long deadline = System.nanoTime() + 20_000_000_000L;
            while (System.nanoTime() < deadline) {
                HttpUtil.Result got = HttpUtil.request("GET", GATEWAY + "/v1/orders/" + Json.text(Json.get(body.body, "order_id")), null, Map.of("X-Caller-Id", "gear-card"), 2);
                Object view = HttpUtil.decodeJson(got.body());
                if ("fulfillment_failed".equals(Json.text(Json.get(view, "state")))) {
                    check("dead".equals(Json.text(Json.get(Json.get(view, "fulfillment"), "status"))), "ac8 dead");
                    check(Json.integer(Json.get(Json.get(view, "fulfillment"), "attempts")) >= 5, "ac8 attempts");
                    return;
                }
                Common.sleep(0.3);
            }
            check(false, cluster.diagnostics());
        }
    }

    private static void ac9() throws Exception {
        try (Cluster cluster = Cluster.start()) {
            String original = Json.text(Json.get(cluster.status(), "leader"));
            cluster.faults(0, 0, true);
            long deadline = System.nanoTime() + 8_000_000_000L;
            boolean promoted = false;
            while (System.nanoTime() < deadline) {
                Object view = cluster.status();
                Long epoch = Json.integer(Json.get(view, "epoch"));
                if (!original.equals(Json.text(Json.get(view, "leader"))) && epoch != null && epoch > 1) {
                    promoted = true;
                    break;
                }
                Common.sleep(0.2);
            }
            check(promoted, cluster.diagnostics());
            cluster.faults(0, 0, false);
            deadline = System.nanoTime() + 5_000_000_000L;
            boolean fenced = false;
            while (System.nanoTime() < deadline) {
                HttpUtil.Result result = HttpUtil.request("POST", original + "/internal/append", Json.obj(
                        "epoch", 1, "caller_id", "gear-card", "idempotency_key", "fenced-write-01",
                        "order", Json.obj("sku", "gear-1", "qty", 1, "external_ref", null, "fulfill_url", null)), Map.of(), 1);
                Object body = HttpUtil.decodeJson(result.body());
                if (result.status() == 409 && "epoch_fenced".equals(Json.text(Json.get(Json.get(body, "error"), "code")))) {
                    fenced = true;
                    break;
                }
                Common.sleep(0.2);
            }
            check(fenced, cluster.diagnostics());
            Result created = post("gear-1", 1, "after-promotion", null);
            check(created.status == 201, "ac9 " + created.body + cluster.diagnostics());
            check(Json.integer(Json.get(created.body, "epoch")) > 1, "ac9 epoch");
        }
    }

    private static void ac10() throws Exception {
        try (Cluster cluster = Cluster.start(); Fulfill fulfill = Fulfill.start(500, 0.9, false)) {
            boolean limited = false;
            boolean created = false;
            for (int i = 0; i < 70; i++) {
                Result body = post("gear-1", 1, String.format("pile-%02d-order", i), Json.obj("fulfill_url", fulfill.url()));
                if (body.status == 201) {
                    created = true;
                    continue;
                }
                String message = Json.text(Json.get(Json.get(body.body, "error"), "message"));
                if (body.status == 429 && "待处理任务已达上限".equals(message)) {
                    limited = true;
                    break;
                }
                if (body.status == 429 && "令牌桶已空".equals(message)) {
                    Common.sleep(0.2);
                    continue;
                }
                check(false, "意外响应 " + body.status + " " + body.body + cluster.diagnostics());
            }
            check(limited, cluster.diagnostics());
            check(created, "ac10 created");
            int before = count(cluster.logs("store-a"), "\"event\":\"job_claimed\"");
            long deadline = System.nanoTime() + 8_000_000_000L;
            boolean progressed = false;
            while (System.nanoTime() < deadline) {
                if (count(cluster.logs("store-a"), "\"event\":\"job_claimed\"") > before) {
                    progressed = true;
                    break;
                }
                Common.sleep(0.3);
            }
            check(progressed, cluster.diagnostics());
        }
    }

    private static void ac11() throws Exception {
        try (Cluster cluster = Cluster.start(); Fulfill fulfill = Fulfill.start(500, 0.9, false)) {
            for (int i = 0; i < 40; i++) {
                while (true) {
                    Result body = post("gear-1", 1, String.format("scale-%02d-order", i), Json.obj("fulfill_url", fulfill.url()));
                    if (body.status == 429 && "令牌桶已空".equals(Json.text(Json.get(Json.get(body.body, "error"), "message")))) {
                        Common.sleep(0.2);
                        continue;
                    }
                    check(body.status == 201, "ac11 " + body.body + cluster.diagnostics());
                    break;
                }
            }
            long deadline = System.nanoTime() + 40_000_000_000L;
            while (System.nanoTime() < deadline) {
                Long workers = Json.integer(Json.get(cluster.status(), "workers"));
                if (workers != null && workers >= 3) {
                    break;
                }
                Common.sleep(0.5);
            }
            check(Json.integer(Json.get(cluster.status(), "workers")) >= 3, cluster.diagnostics());
            Common.sleep(10);
            check(Json.integer(Json.get(cluster.status(), "workers")) == 3, cluster.diagnostics());
        }
    }

    private static void ac12() throws Exception {
        try (Cluster cluster = Cluster.start()) {
            cluster.faults(200, 0, false);
            String requestId = "req-trace-ac12-0001";
            HttpUtil.Result result = HttpUtil.request("POST", GATEWAY + "/v1/orders", Json.obj("sku", "gear-1", "qty", 1), Map.of(
                    "X-Caller-Id", "gear-card", "Idempotency-Key", "trace-request-001", "X-Request-Id", requestId), 3);
            check(result.status() == 201, new String(result.body(), StandardCharsets.UTF_8));
            Common.sleep(0.3);
            check(cluster.logs("gateway").contains(requestId), cluster.logs("gateway"));
            check((cluster.logs("order-1") + cluster.logs("order-2")).contains(requestId), "orders");
            check((cluster.logs("store-a") + cluster.logs("store-b")).contains(requestId), "stores");
        }
    }

    private static Result post(String sku, long qty, String key, Map<String, Object> extra) {
        Map<String, Object> body = Json.obj("sku", sku, "qty", qty);
        if (extra != null) {
            body.putAll(extra);
        }
        try {
            HttpUtil.Result result = HttpUtil.request("POST", GATEWAY + "/v1/orders", body, Map.of(
                    "X-Caller-Id", "gear-card", "Idempotency-Key", key, "X-Request-Id", "req-" + System.nanoTime()), 3);
            Object parsed = result.body().length == 0 ? null : HttpUtil.decodeJson(result.body());
            return new Result(result.status(), result.headers(), parsed);
        } catch (Exception ex) {
            return new Result(0, Map.of(), ex.toString());
        }
    }

    private static HttpUtil.Result httpQuiet(String method, String url, Object body, Map<String, String> headers) {
        try {
            return HttpUtil.request(method, url, body, headers, 1);
        } catch (Exception ex) {
            return null;
        }
    }

    private static int count(String text, String needle) {
        int found = 0;
        int from = 0;
        while (true) {
            int at = text.indexOf(needle, from);
            if (at < 0) {
                return found;
            }
            found++;
            from = at + needle.length();
        }
    }

    private static void run(String name, Case test) {
        try {
            test.run();
            System.out.println(name + " 通过");
        } catch (Throwable ex) {
            failed++;
            System.err.println(name + " 失败: " + ex.getMessage());
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private interface Case {
        void run() throws Exception;
    }

    private record Result(int status, Map<String, String> headers, Object body) {}

    private static final class Cluster implements AutoCloseable {
        final Path root;
        final Process process;

        Cluster(Path root, Process process) {
            this.root = root;
            this.process = process;
        }

        static Cluster start() throws Exception {
            Path root = Files.createTempDirectory("orange-java-");
            Files.createDirectories(root.resolve("runtime"));
            String javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString();
            ProcessBuilder builder = new ProcessBuilder(javaBin, "-cp", System.getProperty("java.class.path"), "Main", "--root", root.toString());
            builder.redirectErrorStream(true);
            builder.redirectOutput(root.resolve("runtime/supervisor.out").toFile());
            Process process = builder.start();
            Cluster cluster = new Cluster(root, process);
            long deadline = System.nanoTime() + 20_000_000_000L;
            while (System.nanoTime() < deadline) {
                HttpUtil.Result ready = httpQuiet("GET", GATEWAY + "/ready", null, Map.of());
                if (ready != null && ready.status() == 200) {
                    return cluster;
                }
                if (!process.isAlive()) {
                    break;
                }
                Common.sleep(0.2);
            }
            String diagnostics = cluster.diagnostics();
            cluster.close();
            throw new AssertionError(diagnostics);
        }

        Object status() throws Exception {
            return HttpUtil.decodeJson(HttpUtil.request("GET", "http://127.0.0.1:8090/v1/status", null, Map.of(), 1).body());
        }

        void faults(long storeDelay, long recommendDelay, boolean dropA) throws Exception {
            String json = Json.stringify(Json.obj(
                    "delay_ms", Json.obj("store", storeDelay, "recommender", recommendDelay),
                    "pause_replication", false,
                    "drop_heartbeat", Json.obj("store-a", dropA, "store-b", false),
                    "exit_after_requests", Json.obj()));
            Files.writeString(root.resolve("runtime/faults.json"), json);
            Common.sleep(0.5);
        }

        private String readOut() {
            Path path = root.resolve("runtime/supervisor.out");
            try {
                return Files.exists(path) ? "----- supervisor.out -----\n" + Files.readString(path) + "\n" : "监督者没有留下日志";
            } catch (Exception ex) {
                return ex.toString();
            }
        }

        String logs(String name) {
            Path path = root.resolve("runtime/logs/" + name + ".log");
            try {
                return Files.exists(path) ? Files.readString(path) : "";
            } catch (Exception ex) {
                return "";
            }
        }

        String diagnostics() {
            Path directory = root.resolve("runtime/logs");
            if (!Files.isDirectory(directory)) {
                return readOut();
            }
            try {
                StringBuilder out = new StringBuilder(readOut());
                Files.list(directory).sorted().forEach(path -> {
                    out.append("----- ").append(path.getFileName()).append(" -----\n");
                    try {
                        out.append(Files.readString(path)).append('\n');
                    } catch (Exception ignored) {
                        // 读不到的日志跳过。
                    }
                });
                return out.isEmpty() ? "监督者没有留下日志" : out.toString();
            } catch (Exception ex) {
                return ex.toString();
            }
        }

        @Override
        public void close() {
            if (process.isAlive()) {
                process.destroy();
                try {
                    if (!process.waitFor(8, java.util.concurrent.TimeUnit.SECONDS)) {
                        process.destroyForcibly();
                        process.waitFor(3, java.util.concurrent.TimeUnit.SECONDS);
                    }
                } catch (Exception ignored) {
                    process.destroyForcibly();
                }
            }
            Common.sleep(0.3);
            try {
                Files.walk(root).sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (Exception ignored) {
                        // 临时目录删不掉不影响验收。
                    }
                });
            } catch (Exception ignored) {
                // 同上。
            }
        }
    }

    private static final class Fulfill implements AutoCloseable {
        final ServerSocket socket;
        final List<String> calls = new CopyOnWriteArrayList<>();
        final List<String> applied = new CopyOnWriteArrayList<>();
        final AtomicBoolean released = new AtomicBoolean();
        final AtomicBoolean stop = new AtomicBoolean();

        Fulfill(ServerSocket socket) {
            this.socket = socket;
        }

        static Fulfill start(int status, double delay, boolean hangFirst) throws Exception {
            ServerSocket socket = new ServerSocket();
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress("127.0.0.1", 0));
            Fulfill fulfill = new Fulfill(socket);
            Thread thread = new Thread(() -> {
                while (!fulfill.stop.get()) {
                    try {
                        Socket client = socket.accept();
                        Thread worker = new Thread(() -> fulfill.handle(client, status, delay, hangFirst));
                        worker.setDaemon(true);
                        worker.start();
                    } catch (Exception ex) {
                        if (!fulfill.stop.get()) {
                            Common.sleep(0.01);
                        }
                    }
                }
            }, "fulfill");
            thread.setDaemon(true);
            thread.start();
            return fulfill;
        }

        String url() {
            return "http://127.0.0.1:" + socket.getLocalPort() + "/hooks/orange-fulfill";
        }

        void release() {
            released.set(true);
        }

        private void handle(Socket client, int status, double delay, boolean hangFirst) {
            try (client) {
                InputStream input = client.getInputStream();
                StringBuilder header = new StringBuilder();
                int previous = 0;
                int previous2 = 0;
                int previous3 = 0;
                while (true) {
                    int value = input.read();
                    if (value < 0) {
                        break;
                    }
                    header.append((char) value);
                    if (previous3 == '\r' && previous2 == '\n' && previous == '\r' && value == '\n') {
                        break;
                    }
                    previous3 = previous2;
                    previous2 = previous;
                    previous = value;
                }
                int length = 0;
                for (String line : header.toString().split("\r\n")) {
                    if (line.toLowerCase().startsWith("content-length:")) {
                        length = Integer.parseInt(line.substring("content-length:".length()).trim());
                    }
                }
                byte[] payloadBytes = input.readNBytes(length);
                String json = new String(payloadBytes, StandardCharsets.UTF_8);
                Object payload = Json.parse(json.trim().isEmpty() ? "{}" : json);
                String orderId = Json.text(Json.get(payload, "order_id"));
                boolean first = calls.isEmpty();
                calls.add(orderId == null ? "" : orderId);
                if (hangFirst && first) {
                    long deadline = System.nanoTime() + 30_000_000_000L;
                    while (!released.get() && System.nanoTime() < deadline) {
                        Common.sleep(0.02);
                    }
                } else if (delay > 0) {
                    Common.sleep(delay);
                }
                byte[] data = "{}".getBytes(StandardCharsets.UTF_8);
                String response = "HTTP/1.1 " + status + " OK\r\nContent-Length: " + data.length + "\r\nConnection: close\r\n\r\n";
                OutputStream output = client.getOutputStream();
                output.write(response.getBytes(StandardCharsets.US_ASCII));
                output.write(data);
                if ((status >= 200 && status < 300) || status == 409) {
                    if (orderId != null && !applied.contains(orderId)) {
                        applied.add(orderId);
                    }
                }
            } catch (Exception ignored) {
                // 调用方超时关掉连接时，这里结束即可。
            }
        }

        @Override
        public void close() {
            release();
            stop.set(true);
            try {
                socket.close();
            } catch (Exception ignored) {
                // 关闭监听套接字。
            }
        }
    }
}
