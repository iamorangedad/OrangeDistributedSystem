#include "common.hpp"
#include "http.hpp"

namespace {
std::string recommendation_for(const std::string& sku) {
    if (sku == "gear-1") return "配套护具";
    return "相关:" + sku;
}
}  // namespace

int run_recommender(int argc, char** argv) {
    set_context(flag(argc, argv, "--process-name", "recommender"), flag(argc, argv, "--root", "."));
    start_fault_reader();
    int port = flag_int(argc, argv, "--port", 8200);
    log_event("process_started", "", {{"port", Json::integer(port)}});
    serve(port, [](const std::string& method, const std::string& path, const std::map<std::string, std::string>& headers, const std::string&) {
        std::string request_id = header_request_id(headers);
        auto [route, query] = split_path(path);
        if (method == "GET" && route == "/health") return HttpResponse::empty(200);
        if (method == "GET" && route == "/ready") return HttpResponse::empty(200);
        if (method == "GET" && route == "/metrics") return HttpResponse::text(200, "orange_errors_total 0\n");
        if (method == "GET" && route == "/internal/recommend") {
            std::string sku = query.count("sku") ? query.at("sku") : "";
            if (sku.empty()) return HttpResponse::json(400, error_body("invalid_argument", "缺少 sku", request_id));
            log_event("recommend_served", request_id, {{"sku", Json::str(sku)}});
            fault_delay("recommender");
            return HttpResponse::json(200, Json::obj({{"recommendation", Json::str(recommendation_for(sku))}}));
        }
        return HttpResponse::json(404, error_body("not_found", "没有这个路径", request_id));
    });
    return 0;
}
