#include "common.hpp"
#include "http.hpp"

#include <algorithm>
#include <memory>
#include <thread>
#include <tuple>

namespace {
struct OrderService {
    std::string name;
    std::string supervisor;
    std::string recommender;
    CircuitBreaker breaker;
    Metrics metrics;
    std::string leader;
    std::int64_t epoch = 1;
    bool leader_ok = false;
    double checked_at = 0;
    std::mutex lock;

    void refresh_once() {
        try {
            HttpResult result = http_request("GET", supervisor + "/internal/leader", nullptr, {}, 0.3);
            if (result.status != 200 || result.body.empty()) return;
            Json body = Json::parse(result.body);
            std::string next = body.find("leader") && body.find("leader")->is_string() ? body.find("leader")->s : "";
            bool ready = false;
            if (!next.empty()) {
                HttpResult probe = http_request("GET", next + "/ready", nullptr, {}, 0.3);
                ready = probe.status == 200;
            }
            std::lock_guard<std::mutex> guard(lock);
            leader = next;
            if (body.find("epoch") && body.find("epoch")->is_int()) epoch = body.find("epoch")->i;
            leader_ok = ready;
            checked_at = monotonic();
        } catch (const std::exception&) {
        }
    }

    void refresh_loop() {
        while (!g_stop.load()) {
            refresh_once();
            sleep_sec(0.2);
        }
    }

    std::tuple<std::string, std::int64_t, bool> current_leader() {
        std::lock_guard<std::mutex> guard(lock);
        bool fresh = monotonic() - checked_at < 1 && !leader.empty();
        return {leader, epoch, fresh && leader_ok};
    }

    HttpResult store_call(const std::string& method, const std::string& url, const std::string& request_id, std::int64_t deadline, const Json* body) {
        std::int64_t remaining = deadline - now_ms();
        if (remaining <= 0) {
            HttpResult timed;
            timed.status = 504;
            return timed;
        }
        double timeout = std::min<std::int64_t>(remaining, kStoreCallCapMs) / 1000.0;
        return http_request(method, url, body, {{"X-Request-Id", request_id}, {"X-Deadline-Ms", std::to_string(deadline)}}, timeout);
    }
};

HttpResponse proxy_read(OrderService& service, const std::string& route, const std::map<std::string, std::string>& query, const std::map<std::string, std::string>& headers, const std::string& request_id) {
    std::string caller = headers.count("x-caller-id") ? headers.at("x-caller-id") : "";
    if (!valid_caller(caller)) return HttpResponse::json(400, error_body("invalid_argument", "X-Caller-Id 不合法", request_id));
    auto [leader, epoch, ok] = service.current_leader();
    (void)epoch;
    std::int64_t deadline = headers.count("x-deadline-ms") ? std::stoll(headers.at("x-deadline-ms")) : now_ms() + 800;
    if (!ok) return HttpResponse::json(503, error_body("unavailable", "主存储不可用", request_id));
    std::string url;
    if (route == "/internal/orders") {
        std::string key = query.count("idempotency_key") ? query.at("idempotency_key") : "";
        url = leader + "/internal/orders/by-key?caller_id=" + url_encode(caller) + "&idempotency_key=" + url_encode(key);
    } else {
        url = leader + "/internal/orders/" + route.substr(route.rfind('/') + 1) + "?caller_id=" + url_encode(caller);
    }
    try {
        HttpResult result = service.store_call("GET", url, request_id, deadline, nullptr);
        if (result.status == 504 || result.body.empty()) return HttpResponse::json(504, error_body("deadline_exceeded", "超过截止时间", request_id));
        Json body = Json::parse(result.body);
        if (body.is_object()) {
            body.o.erase(std::remove_if(body.o.begin(), body.o.end(), [](const auto& item) { return item.first == "request_hash"; }), body.o.end());
        }
        return HttpResponse::json(result.status, body);
    } catch (const std::exception&) {
        service.metrics.inc("orange_errors_total");
        return HttpResponse::json(503, error_body("unavailable", "主存储不可用", request_id));
    }
}

struct AppendOut {
    bool error = false;
    HttpResponse response;
    Json order;
};

AppendOut call_append(OrderService& service, const std::string& caller, const std::string& key, const Json& fields, const std::string& request_id, std::int64_t deadline) {
    AppendOut last;
    last.error = true;
    last.response = HttpResponse::json(503, error_body("unavailable", "主存储不可用", request_id));
    for (int attempt = 0; attempt < 2; ++attempt) {
        auto [leader, epoch, ok] = service.current_leader();
        if (!ok) {
            service.refresh_once();
            std::tie(leader, epoch, ok) = service.current_leader();
        }
        if (!ok) return last;
        Json payload = Json::obj({{"epoch", Json::integer(epoch)}, {"caller_id", Json::str(caller)}, {"idempotency_key", Json::str(key)}, {"order", fields}});
        try {
            HttpResult result = service.store_call("POST", leader + "/internal/append", request_id, deadline ? deadline : now_ms() + 800, &payload);
            Json body = result.body.empty() ? Json::nul() : Json::parse(result.body);
            if (result.status == 409 && body.is_object() && body.find("error") && body.find("error")->find("code") && body.find("error")->find("code")->s == "epoch_fenced") {
                log_event("write_fenced", request_id, {{"epoch", body.find("error")->find("epoch") ? *body.find("error")->find("epoch") : Json::nul()}});
                service.refresh_once();
                continue;
            }
            if (result.status == 409) {
                last.error = true;
                last.response = HttpResponse::json(409, body);
                return last;
            }
            if (result.status == 200 && body.is_object() && body.find("order")) {
                if (!(body.find("created") && body.find("created")->truth())) {
                    log_event("order_replayed", request_id, {{"order_id", Json::str(body.find("order")->find("order_id")->s)}});
                }
                last.error = false;
                last.order = *body.find("order");
                return last;
            }
            last.error = true;
            last.response = HttpResponse::json(result.status ? result.status : 503, body.is_null() ? error_body("unavailable", "主存储不可用", request_id) : body);
            return last;
        } catch (const std::exception&) {
            service.metrics.inc("orange_errors_total");
            log_event("deadline_exceeded", request_id, {});
            last.error = true;
            last.response = HttpResponse::json(504, error_body("deadline_exceeded", "超过截止时间", request_id));
            return last;
        }
    }
    last.error = true;
    last.response = HttpResponse::json(503, error_body("unavailable", "主存储正在切换", request_id));
    return last;
}

HttpResponse write_new(OrderService& service, const std::string& caller, const std::string& key, const Json& fields, const std::string& request_id, std::int64_t deadline) {
    double started = monotonic();
    std::string recommendation;
    std::mutex recommend_lock;
    std::thread ask([&] {
        if (!service.breaker.allow()) {
            log_event("breaker_open", request_id, {});
            service.metrics.set("orange_breaker_open", 1);
            return;
        }
        try {
            std::string sku = fields.find("sku")->s;
            HttpResult result = http_request("GET", service.recommender + "/internal/recommend?sku=" + url_encode(sku), nullptr, {{"X-Request-Id", request_id}}, kRecommendBudgetMs / 1000.0);
            if (result.status != 200) {
                service.breaker.record(false);
                log_event("recommend_failed", request_id, {{"status", Json::integer(result.status)}});
                return;
            }
            Json body = result.body.empty() ? Json::obj({}) : Json::parse(result.body);
            if (!body.find("recommendation") || !body.find("recommendation")->is_string() || body.find("recommendation")->s.empty()) {
                service.breaker.record(false);
                log_event("recommend_failed", request_id, {{"status", Json::integer(result.status)}});
                return;
            }
            service.breaker.record(true);
            std::lock_guard<std::mutex> guard(recommend_lock);
            recommendation = body.find("recommendation")->s;
            service.metrics.set("orange_breaker_open", 0);
        } catch (const std::exception& ex) {
            service.breaker.record(false);
            log_event("recommend_failed", request_id, {{"message", Json::str(ex.what())}});
        }
    });
    AppendOut stored = call_append(service, caller, key, fields, request_id, deadline);
    if (stored.error) {
        ask.detach();
        return stored.response;
    }
    double remaining = (kRecommendBudgetMs / 1000.0) - (monotonic() - started);
    if (remaining > 0) ask.join();
    else ask.detach();
    std::string text;
    {
        std::lock_guard<std::mutex> guard(recommend_lock);
        text = recommendation;
    }
    if (text.empty()) {
        log_event("order_accepted", request_id, {{"order_id", Json::str(stored.order.find("order_id")->s)}, {"epoch", *stored.order.find("epoch")}, {"degraded", Json::boolean(true)}});
        return HttpResponse::json(201, stored.order);
    }
    auto [leader, epoch, ok] = service.current_leader();
    if (!ok) return HttpResponse::json(201, stored.order);
    try {
        Json payload = Json::obj({{"order_id", Json::str(stored.order.find("order_id")->s)}, {"epoch", Json::integer(epoch)}, {"recommendation", Json::str(text)}, {"degraded", Json::boolean(false)}});
        HttpResult result = service.store_call("POST", leader + "/internal/snapshot", request_id, deadline ? deadline : now_ms() + 200, &payload);
        if (result.status == 200 && !result.body.empty()) {
            Json body = Json::parse(result.body);
            log_event("order_accepted", request_id, {{"order_id", Json::str(body.find("order_id")->s)}, {"epoch", *body.find("epoch")}, {"degraded", Json::boolean(false)}});
            return HttpResponse::json(201, body);
        }
    } catch (const std::exception&) {
    }
    return HttpResponse::json(201, stored.order);
}

HttpResponse lookup_existing(OrderService& service, const std::string& caller, const std::string& key, const Json& fields, const std::string& request_id, std::int64_t deadline) {
    auto [leader, epoch, ok] = service.current_leader();
    (void)epoch;
    if (!ok) return HttpResponse::json(503, error_body("unavailable", "主存储不可用", request_id));
    std::string url = leader + "/internal/orders/by-key?caller_id=" + url_encode(caller) + "&idempotency_key=" + url_encode(key);
    try {
        HttpResult result = service.store_call("GET", url, request_id, deadline ? deadline : now_ms() + 800, nullptr);
        if (result.status == 200 && !result.body.empty()) {
            Json body = Json::parse(result.body);
            if (body.find("order_id") && body.find("order_id")->is_string()) {
                std::string digest = canonical_body(fields.find("sku")->s, fields.find("qty")->i, fields.find("external_ref") ? *fields.find("external_ref") : Json::nul(), fields.find("fulfill_url") ? *fields.find("fulfill_url") : Json::nul());
                if (!body.find("request_hash") || body.find("request_hash")->s != digest) {
                    return HttpResponse::json(409, error_body("idempotency_mismatch", "幂等键与已有订单正文不一致", request_id));
                }
                body.o.erase(std::remove_if(body.o.begin(), body.o.end(), [](const auto& item) { return item.first == "request_hash"; }), body.o.end());
                log_event("order_replayed", request_id, {{"order_id", Json::str(body.find("order_id")->s)}});
                return HttpResponse::json(201, body);
            }
        }
    } catch (const std::exception&) {
        return HttpResponse::empty(0);
    }
    return HttpResponse::empty(0);
}

HttpResponse create_order(OrderService& service, const std::map<std::string, std::string>& headers, const std::string& raw, const std::string& request_id) {
    std::string caller = headers.count("x-caller-id") ? headers.at("x-caller-id") : "";
    std::string key = headers.count("idempotency-key") ? headers.at("idempotency-key") : "";
    std::int64_t deadline = headers.count("x-deadline-ms") ? std::stoll(headers.at("x-deadline-ms")) : 0;
    if (!valid_caller(caller)) return HttpResponse::json(400, error_body("invalid_argument", "X-Caller-Id 不合法", request_id));
    if (!valid_idempotency_key(key)) return HttpResponse::json(400, error_body("invalid_argument", "Idempotency-Key 不合法", request_id));
    if (deadline && now_ms() >= deadline) {
        log_event("deadline_exceeded", request_id, {});
        return HttpResponse::json(504, error_body("deadline_exceeded", "超过截止时间", request_id));
    }
    Json parsed;
    try {
        parsed = raw.empty() ? Json::nul() : Json::parse(raw);
    } catch (const std::exception&) {
        return HttpResponse::json(400, error_body("invalid_argument", "正文不是 JSON", request_id));
    }
    Json fields = validate_order_body(parsed);
    if (fields.is_string()) return HttpResponse::json(400, error_body("invalid_argument", fields.s, request_id));
    service.metrics.inc("orange_inflight");
    HttpResponse response;
    HttpResponse replay = lookup_existing(service, caller, key, fields, request_id, deadline);
    if (replay.status != 0) response = std::move(replay);
    else response = write_new(service, caller, key, fields, request_id, deadline);
    service.metrics.inc("orange_inflight", -1);
    return response;
}
}  // namespace

int run_order(int argc, char** argv) {
    std::string name = flag(argc, argv, "--process-name");
    set_context(name, flag(argc, argv, "--root", "."));
    start_fault_reader();
    auto service = std::make_shared<OrderService>();
    service->name = name;
    service->supervisor = flag(argc, argv, "--supervisor");
    service->recommender = flag(argc, argv, "--recommender");
    std::thread([service] { service->refresh_loop(); }).detach();
    int port = flag_int(argc, argv, "--port", 0);
    log_event("process_started", "", {{"port", Json::integer(port)}});
    serve(port, [service](const std::string& method, const std::string& path, const std::map<std::string, std::string>& headers, const std::string& raw) {
        std::string request_id = header_request_id(headers);
        auto [route, query] = split_path(path);
        if (method == "GET" && route == "/health") return HttpResponse::empty(200);
        if (method == "GET" && route == "/ready") {
            auto [leader, epoch, ok] = service->current_leader();
            (void)leader;
            (void)epoch;
            return HttpResponse::empty(ok ? 200 : 503);
        }
        if (method == "GET" && route == "/metrics") {
            service->metrics.set("orange_breaker_open", service->breaker.is_open() ? 1 : 0);
            return HttpResponse::text(200, service->metrics.render({"orange_errors_total", "orange_inflight", "orange_breaker_open"}));
        }
        if (method == "GET" && (route.rfind("/internal/orders/", 0) == 0 || route == "/internal/orders")) return proxy_read(*service, route, query, headers, request_id);
        if (method == "POST" && route == "/internal/orders") return create_order(*service, headers, raw, request_id);
        return HttpResponse::json(404, error_body("not_found", "没有这个路径", request_id));
    });
    return 0;
}
