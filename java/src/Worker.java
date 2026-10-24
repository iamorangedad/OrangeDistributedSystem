import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** 对照 orange/worker.py。工人只向当前主存储领取履约任务。 */
final class Worker {
    private final String name;
    private final String supervisor;
    private final Object lock = new Object();
    private final AtomicInteger errors = new AtomicInteger();
    private String leader = "";
    private double checkedAt;

    Worker(String name, String supervisor) {
        this.name = name;
        this.supervisor = supervisor;
    }

    static void run(String[] args) {
        String name = Common.flag(args, "--process-name", "worker-1");
        Common.setContext(name, Common.flag(args, "--root", "."));
        Common.startFaultReader();
        Worker worker = new Worker(name, Common.flag(args, "--supervisor", "http://127.0.0.1:8090"));
        Thread refresh = new Thread(worker::refresh, "leader");
        Thread loop = new Thread(worker::loop, "loop");
        refresh.setDaemon(true);
        loop.setDaemon(true);
        refresh.start();
        loop.start();
        int port = Common.flagInt(args, "--port", 8401);
        Common.logEvent("process_started", "", "port", port);
        HttpUtil.serve(port, (method, path, headers, raw) -> worker.handle(method, path));
    }

    private void refresh() {
        while (!Common.stop) {
            try {
                HttpUtil.Result result = HttpUtil.request("GET", supervisor + "/internal/leader", null, Map.of(), 0.3);
                if (result.status() == 200 && result.body().length > 0) {
                    synchronized (lock) {
                        String next = Json.text(Json.get(HttpUtil.decodeJson(result.body()), "leader"));
                        leader = next == null ? "" : next;
                        checkedAt = Common.monotonic();
                    }
                }
            } catch (Exception ignored) {
                // 下一拍再问。
            }
            Common.sleep(0.2);
        }
    }

    private String leaderNow() {
        synchronized (lock) {
            if (!leader.isEmpty() && Common.monotonic() - checkedAt < 1) {
                return leader;
            }
            return "";
        }
    }

    private void loop() {
        while (!Common.stop) {
            String current = leaderNow();
            if (current.isEmpty()) {
                Common.sleep(0.2);
                continue;
            }
            String requestId = Common.newId("req_");
            try {
                HttpUtil.Result result = HttpUtil.request("POST", current + "/internal/jobs/claim", Json.obj("worker_id", name), Map.of("X-Request-Id", requestId), 1);
                if (result.status() == 204) {
                    Common.sleep(0.2);
                    continue;
                }
                if (result.status() != 200 || result.body().length == 0) {
                    errors.incrementAndGet();
                    Common.sleep(0.2);
                    continue;
                }
                Object job = HttpUtil.decodeJson(result.body());
                String error = fulfill(job, requestId);
                String path = error == null ? "/ack" : "/fail";
                Object body = error == null ? null : Json.obj("retryable", true, "error", error);
                HttpUtil.request("POST", current + "/internal/jobs/" + Json.text(Json.get(job, "job_id")) + path, body, Map.of("X-Request-Id", requestId), 1);
            } catch (Exception ex) {
                errors.incrementAndGet();
                Common.sleep(0.2);
            }
        }
    }

    private String fulfill(Object job, String requestId) {
        String url = Json.text(Json.get(job, "fulfill_url"));
        if (url == null || url.isEmpty()) {
            return null;
        }
        try {
            HttpUtil.Result result = HttpUtil.request("POST", url, Json.obj(
                    "event", "fulfill", "order_id", Json.get(job, "order_id"), "caller_id", Json.get(job, "caller_id"),
                    "external_ref", Json.get(job, "external_ref"), "sku", Json.get(job, "sku"), "qty", Json.get(job, "qty")),
                    Map.of("X-Request-Id", requestId), Common.FULFILL_TIMEOUT_SEC);
            if ((result.status() >= 200 && result.status() < 300) || result.status() == 409) {
                return null;
            }
            return "履约地址返回 " + result.status();
        } catch (Exception ex) {
            return ex.toString();
        }
    }

    private HttpUtil.Response handle(String method, String path) {
        String route = HttpUtil.routeOf(path);
        if ("GET".equals(method) && "/health".equals(route)) {
            return HttpUtil.Response.empty(200);
        }
        if ("GET".equals(method) && "/ready".equals(route)) {
            return HttpUtil.Response.empty(leaderNow().isEmpty() ? 503 : 200);
        }
        if ("GET".equals(method) && "/metrics".equals(route)) {
            return HttpUtil.Response.text(200, "orange_errors_total " + errors.get() + "\n");
        }
        return HttpUtil.Response.empty(404);
    }
}
