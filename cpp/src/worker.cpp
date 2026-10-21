#include "common.hpp"
#include "http.hpp"

#include <memory>
#include <thread>

namespace {
struct Worker {
    std::string name;
    std::string supervisor;
    std::string leader;
    double checked_at = 0;
    std::int64_t errors = 0;
    std::mutex lock;

    void refresh() {
        while (!g_stop.load()) {
            try {
                HttpResult result = http_request("GET", supervisor + "/internal/leader", nullptr, {}, 0.3);
                if (result.status == 200 && !result.body.empty()) {
                    Json body = Json::parse(result.body);
                    std::lock_guard<std::mutex> guard(lock);
                    leader = body.find("leader") && body.find("leader")->is_string() ? body.find("leader")->s : "";
                    checked_at = monotonic();
                }
            } catch (const std::exception&) {
            }
            sleep_sec(0.2);
        }
    }

    std::string leader_now() {
        std::lock_guard<std::mutex> guard(lock);
        if (!leader.empty() && monotonic() - checked_at < 1) return leader;
        return "";
    }

    std::pair<bool, std::string> fulfill(const Json& job, const std::string& request_id) {
        if (!job.find("fulfill_url") || job.find("fulfill_url")->is_null() || job.find("fulfill_url")->text_or_empty().empty()) return {true, ""};
        Json payload = Json::obj({
            {"event", Json::str("fulfill")}, {"order_id", *job.find("order_id")}, {"caller_id", *job.find("caller_id")},
            {"external_ref", job.find("external_ref") ? *job.find("external_ref") : Json::nul()},
            {"sku", *job.find("sku")}, {"qty", *job.find("qty")},
        });
        try {
            HttpResult result = http_request("POST", job.find("fulfill_url")->s, &payload, {{"X-Request-Id", request_id}}, kFulfillTimeoutSec);
            if ((result.status >= 200 && result.status < 300) || result.status == 409) return {true, ""};
            return {false, "履约地址返回 " + std::to_string(result.status)};
        } catch (const std::exception& ex) {
            return {false, ex.what()};
        }
    }

    void run() {
        while (!g_stop.load()) {
            std::string current = leader_now();
            if (current.empty()) {
                sleep_sec(0.2);
                continue;
            }
            std::string request_id = new_id("req_");
            HttpResult result;
            try {
                Json body = Json::obj({{"worker_id", Json::str(name)}});
                result = http_request("POST", current + "/internal/jobs/claim", &body, {{"X-Request-Id", request_id}}, 1);
            } catch (const std::exception&) {
                errors += 1;
                sleep_sec(0.2);
                continue;
            }
            if (result.status == 204) {
                sleep_sec(0.2);
                continue;
            }
            if (result.status != 200 || result.body.empty()) {
                errors += 1;
                sleep_sec(0.2);
                continue;
            }
            Json job = Json::parse(result.body);
            auto [ok, error] = fulfill(job, request_id);
            std::string path = ok ? "/ack" : "/fail";
            Json fail_body = Json::obj({{"retryable", Json::boolean(true)}, {"error", Json::str(error)}});
            try {
                http_request("POST", current + "/internal/jobs/" + job.find("job_id")->s + path, ok ? nullptr : &fail_body, {{"X-Request-Id", request_id}}, 1);
            } catch (const std::exception&) {
                errors += 1;
            }
        }
    }
};
}  // namespace

int run_worker(int argc, char** argv) {
    set_context(flag(argc, argv, "--process-name"), flag(argc, argv, "--root", "."));
    start_fault_reader();
    auto worker = std::make_shared<Worker>();
    worker->name = flag(argc, argv, "--process-name");
    worker->supervisor = flag(argc, argv, "--supervisor");
    std::thread([worker] { worker->refresh(); }).detach();
    std::thread([worker] { worker->run(); }).detach();
    int port = flag_int(argc, argv, "--port", 0);
    log_event("process_started", "", {{"port", Json::integer(port)}});
    serve(port, [worker](const std::string& method, const std::string& path, const std::map<std::string, std::string>&, const std::string&) {
        auto [route, query] = split_path(path);
        (void)query;
        if (method == "GET" && route == "/health") return HttpResponse::empty(200);
        if (method == "GET" && route == "/ready") return HttpResponse::empty(worker->leader_now().empty() ? 503 : 200);
        if (method == "GET" && route == "/metrics") return HttpResponse::text(200, "orange_errors_total " + std::to_string(worker->errors) + "\n");
        return HttpResponse::empty(404);
    });
    return 0;
}
