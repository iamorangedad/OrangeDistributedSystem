package orange;

import java.util.Map;

/** 对照 orange/recommender.py。可失败的推荐旁路，不访问存储。 */
final class Recommender {
    private Recommender() {}

    static void run(String[] args) {
        Common.setContext(Common.flag(args, "--process-name", "recommender"), Common.flag(args, "--root", "."));
        Common.startFaultReader();
        int port = Common.flagInt(args, "--port", 8200);
        Common.logEvent("process_started", "", "port", port);
        HttpUtil.serve(port, Recommender::handle);
    }

    private static HttpUtil.Response handle(String method, String path, Map<String, String> headers, byte[] raw) {
        String requestId = headers.getOrDefault("x-request-id", Common.newId("req_"));
        String route = HttpUtil.routeOf(path);
        if ("GET".equals(method) && "/health".equals(route)) {
            return HttpUtil.Response.empty(200);
        }
        if ("GET".equals(method) && "/ready".equals(route)) {
            return HttpUtil.Response.empty(200);
        }
        if ("GET".equals(method) && "/metrics".equals(route)) {
            return HttpUtil.Response.text(200, "orange_errors_total 0\n");
        }
        if ("GET".equals(method) && "/internal/recommend".equals(route)) {
            String sku = HttpUtil.splitQuery(path).getOrDefault("sku", "");
            if (sku.isEmpty()) {
                return HttpUtil.Response.json(400, Common.errorBody("invalid_argument", "缺少 sku", requestId));
            }
            Common.logEvent("recommend_served", requestId, "sku", sku);
            Common.faultDelay("recommender");
            String text = "gear-1".equals(sku) ? "配套护具" : "相关:" + sku;
            return HttpUtil.Response.json(200, Json.obj("recommendation", text));
        }
        return HttpUtil.Response.json(404, Common.errorBody("not_found", "没有这个路径", requestId));
    }
}
