import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** 对照 orange/supervisor.py。拉起进程、公布主存储、提升从存储，并按积压加减工人。 */
final class Supervisor {
    static final String STORE_A = "http://127.0.0.1:8301";
    static final String STORE_B = "http://127.0.0.1:8302";
    static final String SUPERVISOR_URL = "http://127.0.0.1:8090";

    private final String root;
    private final Object stateLock = new Object();
    private final Object procLock = new Object();
    private final Map<String, Process> procs = new LinkedHashMap<>();
    private String leader = STORE_A;
    private long epoch = 1;
    private long jobsPending;
    private boolean gatewayPaused;
    private String lastPromotionError;
    private final Map<String, Long> applied = new LinkedHashMap<>();
    private final Map<String, Double> lastBeat = new LinkedHashMap<>();
    private final Set<String> seen = ConcurrentHashMap.newKeySet();
    private final Map<String, Boolean> ready = new LinkedHashMap<>();
    private final Map<String, Long> pids = new LinkedHashMap<>();
    private final Map<String, String> lastError = new LinkedHashMap<>();
    private final List<String> expected = new ArrayList<>(List.of("gateway", "order-1", "order-2", "recommender", "store-a", "store-b", "worker-1"));
    private long workerCount = 1;
    private Double highSince;
    private Double zeroSince;
    private double lastScale;
    private boolean promoting;
    private boolean armed;

    Supervisor(String root) {
        this.root = root;
        applied.put("store-a", 0L);
        applied.put("store-b", 0L);
    }

    static void run(String[] args) {
        String root = Common.flag(args, "--root", ".");
        Common.setContext("supervisor", root);
        try {
            Files.createDirectories(Path.of(root, "runtime", "data"));
            Files.createDirectories(Path.of(root, "runtime", "logs"));
            Path faults = Path.of(root, "runtime", "faults.json");
            if (!Files.exists(faults)) {
                Files.writeString(faults, "{\"delay_ms\":{\"store\":0,\"recommender\":0},\"pause_replication\":false,\"drop_heartbeat\":{\"store-a\":false,\"store-b\":false},\"exit_after_requests\":{}}\n");
            }
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
        Common.startFaultReader();
        Supervisor supervisor = new Supervisor(root);
        com.sun.net.httpserver.HttpServer server = HttpUtil.start(8090, supervisor::handle);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Common.stop = true;
            supervisor.stopAll();
            server.stop(0);
        }));
        Common.logEvent("process_started", "", "port", 8090);
        supervisor.loop();
    }

    private HttpUtil.Response handle(String method, String path, Map<String, String> headers, byte[] raw) {
        String requestId = headers.getOrDefault("x-request-id", Common.newId("req_"));
        String route = HttpUtil.routeOf(path);
        if ("GET".equals(method) && "/health".equals(route)) {
            return HttpUtil.Response.empty(200);
        }
        if ("GET".equals(method) && "/v1/status".equals(route)) {
            return HttpUtil.Response.json(200, status());
        }
        if ("GET".equals(method) && "/internal/leader".equals(route)) {
            synchronized (stateLock) {
                return HttpUtil.Response.json(200, Json.obj("leader", leader, "epoch", epoch));
            }
        }
        if ("GET".equals(method) && "/internal/gateway-control".equals(route)) {
            synchronized (stateLock) {
                return HttpUtil.Response.json(200, Json.obj("paused", gatewayPaused, "jobs_pending", jobsPending));
            }
        }
        if ("POST".equals(method) && "/internal/heartbeat".equals(route)) {
            Object body = raw.length == 0 ? Json.obj() : Json.parse(new String(raw, java.nio.charset.StandardCharsets.UTF_8));
            noteHeartbeat(body);
            synchronized (stateLock) {
                return HttpUtil.Response.json(200, Json.obj("leader", leader, "epoch", epoch));
            }
        }
        return HttpUtil.Response.json(404, Common.errorBody("not_found", "没有这个路径", requestId));
    }

    private void loop() {
        while (!Common.stop) {
            try {
                ensure();
                probeReady();
                maybePromote();
                maybeScale();
            } catch (Exception ex) {
                Common.logEvent("supervisor_error", "", "error", Json.obj("message", ex.toString()));
            }
            Common.sleep(Common.HEARTBEAT_SEC);
        }
        stopAll();
    }

    private List<String> command(String name) {
        String javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("java.class.path");
        List<String> args = new ArrayList<>();
        args.add(javaBin);
        args.add("-cp");
        args.add(classpath);
        args.add("Main");
        switch (name) {
            case "store-a" -> args.addAll(List.of("store", "--node-id", "store-a", "--port", "8301", "--role", "leader", "--peer", STORE_B, "--data", root + "/runtime/data/store-a.sqlite", "--supervisor", SUPERVISOR_URL));
            case "store-b" -> args.addAll(List.of("store", "--node-id", "store-b", "--port", "8302", "--role", "follower", "--peer", STORE_A, "--data", root + "/runtime/data/store-b.sqlite", "--supervisor", SUPERVISOR_URL));
            case "gateway" -> args.addAll(List.of("gateway", "--port", "8080", "--supervisor", SUPERVISOR_URL, "--orders", "http://127.0.0.1:8101,http://127.0.0.1:8102", "--process-name", "gateway"));
            case "order-1" -> args.addAll(List.of("order", "--process-name", "order-1", "--port", "8101", "--supervisor", SUPERVISOR_URL, "--recommender", "http://127.0.0.1:8200"));
            case "order-2" -> args.addAll(List.of("order", "--process-name", "order-2", "--port", "8102", "--supervisor", SUPERVISOR_URL, "--recommender", "http://127.0.0.1:8200"));
            case "recommender" -> args.addAll(List.of("recommender", "--port", "8200", "--process-name", "recommender"));
            default -> {
                if (name.startsWith("worker-")) {
                    int index = Integer.parseInt(name.substring("worker-".length()));
                    args.addAll(List.of("worker", "--process-name", name, "--port", Integer.toString(8400 + index), "--supervisor", SUPERVISOR_URL));
                } else {
                    throw new IllegalArgumentException(name);
                }
            }
        }
        args.add("--root");
        args.add(root);
        return args;
    }

    private void spawn(String name) {
        synchronized (procLock) {
            Process current = procs.get(name);
            if (current != null && current.isAlive()) {
                return;
            }
            try {
                if (Common.stop) {
                    return;
                }
                ProcessBuilder builder = new ProcessBuilder(command(name));
                builder.environment().put("ORANGE_ROOT", root);
                builder.environment().put("ORANGE_PROCESS", name);
                builder.inheritIO();
                Process process = builder.start();
                procs.put(name, process);
                synchronized (stateLock) {
                    pids.put(name, process.pid());
                }
                Common.logEvent("spawned", "", "process_name", name, "pid", process.pid());
            } catch (IOException ex) {
                synchronized (stateLock) {
                    lastError.put(name, ex.toString());
                }
            }
        }
    }

    private void stopProcess(String name) {
        Process process;
        synchronized (procLock) {
            process = procs.get(name);
        }
        if (process == null || !process.isAlive()) {
            synchronized (stateLock) {
                ready.put(name, false);
            }
            return;
        }
        process.destroy();
        try {
            if (!process.waitFor(2, java.util.concurrent.TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
        synchronized (stateLock) {
            ready.put(name, false);
        }
    }

    private void stopAll() {
        List<String> names;
        synchronized (procLock) {
            Common.stop = true;
            names = new ArrayList<>(procs.keySet());
        }
        for (String name : names) {
            stopProcess(name);
        }
    }

    private void ensure() {
        List<String> names;
        synchronized (stateLock) {
            names = new ArrayList<>(expected);
        }
        for (String name : names) {
            boolean restart = false;
            synchronized (procLock) {
                Process process = procs.get(name);
                if (process != null && !process.isAlive()) {
                    int code = process.exitValue();
                    synchronized (stateLock) {
                        lastError.put(name, "退出码 " + code);
                        ready.put(name, false);
                    }
                    Common.logEvent("process_exited", "", "error", Json.obj("process_name", name, "code", code));
                    procs.remove(name);
                    restart = true;
                } else if (process == null) {
                    restart = true;
                }
            }
            if (restart) {
                boolean knownFailure;
                synchronized (stateLock) {
                    knownFailure = lastError.containsKey(name);
                }
                if (knownFailure) {
                    Common.sleep(0.5);
                }
                spawn(name);
            }
        }
    }

    private void probeReady() {
        List<String> names;
        synchronized (stateLock) {
            names = new ArrayList<>(expected);
        }
        for (String name : names) {
            Integer port = portOf(name);
            if (port == null) {
                continue;
            }
            boolean up = false;
            try {
                up = HttpUtil.request("GET", "http://127.0.0.1:" + port + "/ready", null, Map.of(), 0.3).status() == 200;
            } catch (Exception ignored) {
                up = false;
            }
            synchronized (stateLock) {
                ready.put(name, up);
            }
        }
    }

    private void noteHeartbeat(Object body) {
        String nodeId = Json.text(Json.get(body, "node_id"));
        if (!"store-a".equals(nodeId) && !"store-b".equals(nodeId)) {
            return;
        }
        synchronized (stateLock) {
            lastBeat.put(nodeId, Common.monotonic());
            seen.add(nodeId);
            Long index = Json.integer(Json.get(body, "applied_index"));
            applied.put(nodeId, index == null ? 0L : index);
            if ("leader".equals(Json.text(Json.get(body, "role")))) {
                Long remoteEpoch = Json.integer(Json.get(body, "epoch"));
                if (remoteEpoch != null && remoteEpoch >= epoch) {
                    epoch = remoteEpoch;
                    leader = "store-a".equals(nodeId) ? STORE_A : STORE_B;
                    Long pending = Json.integer(Json.get(body, "jobs_pending"));
                    jobsPending = pending == null ? 0 : pending;
                }
            }
            if (seen.size() >= 2) {
                armed = true;
            }
        }
    }

    private void maybePromote() {
        String followerUrl;
        long newEpoch;
        String oldLeader;
        synchronized (stateLock) {
            if (!armed || promoting) {
                return;
            }
            double now = Common.monotonic();
            String leaderId = STORE_A.equals(leader) ? "store-a" : "store-b";
            String followerId = "store-a".equals(leaderId) ? "store-b" : "store-a";
            Double leaderSeen = lastBeat.get(leaderId);
            Double followerSeen = lastBeat.get(followerId);
            if (leaderSeen == null || followerSeen == null) {
                return;
            }
            if (now - leaderSeen <= Common.LEADER_LOST_SEC || now - followerSeen > Common.LEADER_LOST_SEC) {
                return;
            }
            promoting = true;
            gatewayPaused = true;
            followerUrl = "store-b".equals(followerId) ? STORE_B : STORE_A;
            newEpoch = epoch + 1;
            oldLeader = leader;
        }
        double started = Common.monotonic();
        try {
            HttpUtil.Result result = HttpUtil.request("POST", followerUrl + "/internal/promote", Json.obj("new_epoch", newEpoch, "peer", oldLeader), Map.of(), Common.PROMOTION_PAUSE_SEC);
            synchronized (stateLock) {
                if (result.status() == 200 && result.body().length > 0) {
                    Object body = HttpUtil.decodeJson(result.body());
                    Long promoted = Json.integer(Json.get(body, "epoch"));
                    epoch = promoted == null ? newEpoch : promoted;
                    leader = followerUrl;
                    lastPromotionError = null;
                    Common.logEvent("promoted", "", "epoch", epoch, "leader", leader);
                } else {
                    lastPromotionError = "提升失败 HTTP " + result.status();
                    Common.logEvent("promoted", "", "error", Json.obj("message", lastPromotionError));
                }
            }
        } catch (Exception ex) {
            synchronized (stateLock) {
                lastPromotionError = ex.toString();
            }
            Common.logEvent("promoted", "", "error", Json.obj("message", ex.toString()));
        } finally {
            synchronized (stateLock) {
                if (Common.monotonic() - started > Common.PROMOTION_PAUSE_SEC && lastPromotionError == null) {
                    lastPromotionError = "切换超过 2 秒";
                }
                gatewayPaused = false;
                promoting = false;
            }
        }
    }

    private void maybeScale() {
        String action = null;
        String name = null;
        synchronized (stateLock) {
            double now = Common.monotonic();
            if (jobsPending > Common.SCALE_OUT_PENDING) {
                zeroSince = null;
                if (highSince == null) {
                    highSince = now;
                }
                boolean cooled = now - lastScale >= Common.SCALE_COOLDOWN_SEC;
                if (now - highSince >= Common.SCALE_OUT_HOLD_SEC && cooled && workerCount < Common.WORKER_MAX) {
                    workerCount++;
                    name = "worker-" + workerCount;
                    if (!expected.contains(name)) {
                        expected.add(name);
                    }
                    lastScale = now;
                    highSince = null;
                    Common.logEvent("scaled", "", "workers", workerCount, "direction", "out");
                    action = "out";
                }
            } else {
                highSince = null;
            }
            if (jobsPending == 0) {
                if (zeroSince == null) {
                    zeroSince = now;
                }
                boolean cooled = now - lastScale >= Common.SCALE_COOLDOWN_SEC;
                if (zeroSince != null && now - zeroSince >= Common.SCALE_IN_HOLD_SEC && cooled && workerCount > Common.WORKER_MIN) {
                    name = "worker-" + workerCount;
                    expected.remove(name);
                    workerCount--;
                    lastScale = now;
                    zeroSince = null;
                    Common.logEvent("scaled", "", "workers", workerCount, "direction", "in");
                    action = "in";
                }
            } else {
                zeroSince = null;
            }
        }
        if ("out".equals(action)) {
            spawn(name);
        } else if ("in".equals(action)) {
            stopProcess(name);
        }
    }

    private Map<String, Object> status() {
        synchronized (stateLock) {
            Map<String, Object> readyView = new LinkedHashMap<>();
            for (String name : expected) {
                readyView.put(name, Boolean.TRUE.equals(ready.get(name)));
            }
            return Json.obj(
                    "leader", leader, "epoch", epoch, "applied_index", new LinkedHashMap<>(applied),
                    "workers", workerCount, "jobs_pending", jobsPending, "gateway_paused", gatewayPaused,
                    "ready", readyView, "last_promotion_error", lastPromotionError, "pids", new LinkedHashMap<>(pids),
                    "last_error", new LinkedHashMap<>(lastError));
        }
    }

    private static Integer portOf(String name) {
        return switch (name) {
            case "gateway" -> 8080;
            case "order-1" -> 8101;
            case "order-2" -> 8102;
            case "recommender" -> 8200;
            case "store-a" -> 8301;
            case "store-b" -> 8302;
            case "worker-1" -> 8401;
            case "worker-2" -> 8402;
            case "worker-3" -> 8403;
            default -> null;
        };
    }
}
