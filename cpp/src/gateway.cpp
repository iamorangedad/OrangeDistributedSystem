#include "common.hpp"
#include "http.hpp"

#include <memory>
#include <thread>

namespace {
struct Gateway {
    std::string supervisor;
    std::vector<std::string> orders;
    TokenBucket bucket;
    Metrics metrics;
    int inflight = 0;
    int cursor = 0;
    std::map<std::string, bool> order_ready;
    bool paused = false;
    std::int64_t jobs_pending = 0;
    std::mutex lock;

    explicit Gateway(std::string supervisor_url, std::vector<std::string> order_urls)
        : supervisor(std::move(supervisor_url)), orders(std::move(order_urls)) {
        for (const auto& url : orders) order_ready[url] = false;
    }

    void poll() {
        while (!g_stop.load()) {
            for (const auto& url : orders) {
                bool ready = false;
                try {
                    ready = http_request("GET", url + "/ready", nullptr, {}, 0.3).status == 200;
                } catch (const std::exception&) {
                    ready = false;
                }
                std::lock_guard<std::mutex> guard(lock);
                order_ready[url] = ready;
            }
            try {
                HttpResult result = http_request("GET", supervisor + "/internal/gateway-control", nullptr, {}, 0.3);
                if (result.status == 200 && !result.body.empty()) {
                    Json body = Json::parse(result.body);
                    std::lock_guard<std::mutex> guard(lock);
                    paused = body.find("paused") && body.find("paused")->truth();
                    jobs_pending = body.find("jobs_pending") ? body.find("jobs_pending")->int_or(0) : 0;
                }
            } catch (const std::exception&) {
            }
            sleep_sec(0.2);
        }
    }

    std::string pick() {
        std::lock_guard<std::mutex> guard(lock);
        std::vector<std::string> ready;
        if (paused) return "";
        for (const auto& url : orders) {
            if (order_ready[url]) ready.push_back(url);
        }
        if (ready.empty()) return "";
        std::string chosen = ready[static_cast<size_t>(cursor % static_cast<int>(ready.size()))];
        cursor += 1;
        return chosen;
    }

    bool ready() {
        std::lock_guard<std::mutex> guard(lock);
        if (paused) return false;
        for (const auto& item : order_ready) {
            if (item.second) return true;
        }
        return false;
    }
};

HttpResponse reject(Gateway& gateway, int status, const std::string& code, const std::string& message, const std::string& request_id, int retry_after, bool with_retry) {
    gateway.metrics.inc("orange_errors_total");
    if (code == "rate_limited") log_event("rate_limited", request_id, {{"message", Json::str(message)}});
    std::vector<std::pair<std::string, std::string>> headers;
    if (with_retry) headers.push_back({"Retry-After", std::to_string(retry_after)});
    return HttpResponse::json(status, error_body(code, message, request_id), headers);
}

HttpResponse create(Gateway& gateway, const std::map<std::string, std::string>& headers, const std::string& raw, const std::string& request_id) {
    std::string caller = headers.count("x-caller-id") ? headers.at("x-caller-id") : "";
    std::string key = headers.count("idempotency-key") ? headers.at("idempotency-key") : "";
    if (!valid_caller(caller)) return HttpResponse::json(400, error_body("invalid_argument", "X-Caller-Id 不合法", request_id));
    if (!valid_idempotency_key(key)) return HttpResponse::json(400, error_body("invalid_argument", "Idempotency-Key 不合法", request_id));
    Json parsed;
    try {
        parsed = raw.empty() ? Json::nul() : Json::parse(raw);
    } catch (const std::exception&) {
        return HttpResponse::json(400, error_body("invalid_argument", "正文不是 JSON", request_id));
    }
    Json fields = validate_order_body(parsed);
    if (fields.is_string()) return HttpResponse::json(400, error_body("invalid_argument", fields.s, request_id));
    auto [allowed, retry_after] = gateway.bucket.take();
    if (!allowed) return reject(gateway, 429, "rate_limited", "令牌桶已空", request_id, retry_after, true);
    std::int64_t pending = 0;
    bool paused = false;
    {
        std::lock_guard<std::mutex> guard(gateway.lock);
        pending = gateway.jobs_pending;
        paused = gateway.paused;
    }
    if (pending >= kJobsPendingLimit) return reject(gateway, 429, "rate_limited", "待处理任务已达上限", request_id, 1, true);
    bool held = false;
    {
        std::lock_guard<std::mutex> guard(gateway.lock);
        if (paused) return reject(gateway, 503, "unavailable", "主存储正在切换", request_id, 0, false);
        if (gateway.inflight >= kGatewayInflightLimit) return reject(gateway, 429, "rate_limited", "在途请求已满", request_id, 1, true);
        gateway.inflight += 1;
        held = true;
        gateway.metrics.set("orange_inflight", gateway.inflight);
    }
    HttpResponse response = reject(gateway, 503, "unavailable", "没有就绪的订单副本", request_id, 0, false);
    try {
        std::int64_t deadline = now_ms() + kOrderDeadlineMs;
        std::map<std::string, std::string> forward = {{"X-Caller-Id", caller}, {"Idempotency-Key", key}, {"X-Request-Id", request_id}, {"X-Deadline-Ms", std::to_string(deadline)}};
        for (size_t n = 0; n < gateway.orders.size(); ++n) {
            std::string target = gateway.pick();
            if (target.empty()) {
                response = reject(gateway, 503, "unavailable", "没有就绪的订单副本", request_id, 0, false);
                break;
            }
            try {
                HttpResult result = http_request("POST", target + "/internal/orders", &fields, forward, kOrderDeadlineMs / 1000.0 + 0.2);
                std::vector<std::pair<std::string, std::string>> extra = {{"X-Request-Id", request_id}};
                if (result.headers.count("retry-after")) extra.push_back({"Retry-After", result.headers.at("retry-after")});
                std::string type = result.headers.count("content-type") ? result.headers.at("content-type") : "application/json";
                log_event("gateway_forward", request_id, {{"status", Json::integer(result.status)}});
                response = HttpResponse::raw(result.status, type, result.body, extra);
                break;
            } catch (const std::exception&) {
                gateway.metrics.inc("orange_errors_total");
            }
        }
    } catch (...) {
        if (held) {
            std::lock_guard<std::mutex> guard(gateway.lock);
            gateway.inflight -= 1;
            gateway.metrics.set("orange_inflight", gateway.inflight);
        }
        throw;
    }
    if (held) {
        std::lock_guard<std::mutex> guard(gateway.lock);
        gateway.inflight -= 1;
        gateway.metrics.set("orange_inflight", gateway.inflight);
    }
    return response;
}

HttpResponse forward_get(Gateway& gateway, const std::string& route, const std::map<std::string, std::string>& query, const std::map<std::string, std::string>& headers, const std::string& request_id) {
    std::string caller = headers.count("x-caller-id") ? headers.at("x-caller-id") : "";
    if (!valid_caller(caller)) return HttpResponse::json(400, error_body("invalid_argument", "X-Caller-Id 不合法", request_id));
    if (route == "/internal/orders" && !query.count("idempotency_key")) return HttpResponse::json(400, error_body("invalid_argument", "缺少 idempotency_key", request_id));
    std::string target = gateway.pick();
    if (target.empty()) return HttpResponse::json(503, error_body("unavailable", "没有就绪的订单副本", request_id));
    std::string query_text;
    for (const auto& item : query) {
        query_text += query_text.empty() ? "?" : "&";
        query_text += url_encode(item.first) + "=" + url_encode(item.second);
    }
    try {
        HttpResult result = http_request("GET", target + route + query_text, nullptr, {{"X-Caller-Id", caller}, {"X-Request-Id", request_id}, {"X-Deadline-Ms", std::to_string(now_ms() + kOrderDeadlineMs)}}, 1);
        return HttpResponse::raw(result.status, "application/json; charset=utf-8", result.body, {{"X-Request-Id", request_id}});
    } catch (const std::exception&) {
        return HttpResponse::json(503, error_body("unavailable", "没有就绪的订单副本", request_id));
    }
}
}  // namespace

int run_gateway(int argc, char** argv) {
    set_context(flag(argc, argv, "--process-name", "gateway"), flag(argc, argv, "--root", "."));
    start_fault_reader();
    std::vector<std::string> orders;
    std::string listed = flag(argc, argv, "--orders");
    size_t start = 0;
    while (start < listed.size()) {
        auto comma = listed.find(',', start);
        std::string item = listed.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
        if (!item.empty()) orders.push_back(item);
        if (comma == std::string::npos) break;
        start = comma + 1;
    }
    auto gateway = std::make_shared<Gateway>(flag(argc, argv, "--supervisor"), orders);
    std::thread([gateway] { gateway->poll(); }).detach();
    int port = flag_int(argc, argv, "--port", 8080);
    log_event("process_started", "", {{"port", Json::integer(port)}});
    serve(port, [gateway](const std::string& method, const std::string& path, const std::map<std::string, std::string>& headers, const std::string& raw) {
        std::string request_id = header_request_id(headers);
        auto [route, query] = split_path(path);
        if (method == "GET" && route == "/health") return HttpResponse::empty(200);
        if (method == "GET" && route == "/ready") return HttpResponse::empty(gateway->ready() ? 200 : 503);
        if (method == "GET" && route == "/metrics") {
            std::lock_guard<std::mutex> guard(gateway->lock);
            gateway->metrics.set("orange_inflight", gateway->inflight);
            return HttpResponse::text(200, gateway->metrics.render({"orange_errors_total", "orange_inflight"}));
        }
        if (method == "POST" && route == "/v1/orders") return create(*gateway, headers, raw, request_id);
        if (method == "GET" && route == "/v1/orders") return forward_get(*gateway, "/internal/orders", query, headers, request_id);
        if (method == "GET" && route.rfind("/v1/orders/", 0) == 0) return forward_get(*gateway, "/internal/orders/" + route.substr(std::string("/v1/orders/").size()), {}, headers, request_id);
        return HttpResponse::json(404, error_body("not_found", "没有这个路径", request_id));
    });
    return 0;
}
