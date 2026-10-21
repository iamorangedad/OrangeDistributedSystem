#include "common.hpp"
#include "http.hpp"

#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <map>
#include <memory>
#include <thread>

extern char** environ;

namespace {
const char* kStoreA = "http://127.0.0.1:8301";
const char* kStoreB = "http://127.0.0.1:8302";
const char* kSupervisor = "http://127.0.0.1:8090";

struct Child {
    pid_t pid = -1;
};

struct Supervisor {
    std::string root;
    std::string executable;
    std::mutex state_lock;
    std::mutex proc_lock;
    std::map<std::string, Child> procs;
    std::string leader = kStoreA;
    std::int64_t epoch = 1;
    std::int64_t jobs_pending = 0;
    bool gateway_paused = false;
    Json last_promotion_error = Json::nul();
    std::map<std::string, std::int64_t> applied{{"store-a", 0}, {"store-b", 0}};
    std::map<std::string, double> last_beat;
    std::map<std::string, bool> seen;
    std::map<std::string, bool> ready;
    std::map<std::string, std::int64_t> pids;
    std::map<std::string, std::string> last_error;
    std::vector<std::string> expected{"gateway", "order-1", "order-2", "recommender", "store-a", "store-b", "worker-1"};
    int worker_count = 1;
    double high_since = -1;
    double zero_since = -1;
    double last_scale = 0;
    bool promoting = false;
    bool armed = false;

    std::vector<std::string> command(const std::string& name) const {
        std::string data = root + "/runtime/data";
        if (name == "store-a") return {executable, "store", "--node-id", "store-a", "--port", "8301", "--role", "leader", "--peer", kStoreB, "--data", data + "/store-a.sqlite", "--supervisor", kSupervisor, "--root", root};
        if (name == "store-b") return {executable, "store", "--node-id", "store-b", "--port", "8302", "--role", "follower", "--peer", kStoreA, "--data", data + "/store-b.sqlite", "--supervisor", kSupervisor, "--root", root};
        if (name == "gateway") return {executable, "gateway", "--port", "8080", "--supervisor", kSupervisor, "--orders", "http://127.0.0.1:8101,http://127.0.0.1:8102", "--process-name", "gateway", "--root", root};
        if (name == "order-1") return {executable, "order", "--process-name", "order-1", "--port", "8101", "--supervisor", kSupervisor, "--recommender", "http://127.0.0.1:8200", "--root", root};
        if (name == "order-2") return {executable, "order", "--process-name", "order-2", "--port", "8102", "--supervisor", kSupervisor, "--recommender", "http://127.0.0.1:8200", "--root", root};
        if (name == "recommender") return {executable, "recommender", "--port", "8200", "--process-name", "recommender", "--root", root};
        int index = std::stoi(name.substr(std::string("worker-").size()));
        return {executable, "worker", "--process-name", name, "--port", std::to_string(8400 + index), "--supervisor", kSupervisor, "--root", root};
    }

    void spawn(const std::string& name) {
        std::lock_guard<std::mutex> guard(proc_lock);
        if (g_stop.load()) return;
        auto found = procs.find(name);
        if (found != procs.end()) {
            int status = 0;
            if (waitpid(found->second.pid, &status, WNOHANG) == 0) return;
        }
        auto args = command(name);
        std::vector<char*> argv;
        for (auto& arg : args) argv.push_back(arg.data());
        argv.push_back(nullptr);
        pid_t pid = 0;
        if (posix_spawn(&pid, argv[0], nullptr, nullptr, argv.data(), environ) != 0) {
            std::lock_guard<std::mutex> state(state_lock);
            last_error[name] = "spawn failed";
            return;
        }
        procs[name] = Child{pid};
        std::lock_guard<std::mutex> state(state_lock);
        pids[name] = pid;
        log_event("spawned", "", {{"process_name", Json::str(name)}, {"pid", Json::integer(pid)}});
    }

    void stop_process(const std::string& name) {
        pid_t pid = -1;
        {
            std::lock_guard<std::mutex> guard(proc_lock);
            auto found = procs.find(name);
            if (found == procs.end()) {
                std::lock_guard<std::mutex> state(state_lock);
                ready[name] = false;
                return;
            }
            pid = found->second.pid;
        }
        kill(pid, SIGTERM);
        for (int n = 0; n < 40; ++n) {
            int status = 0;
            if (waitpid(pid, &status, WNOHANG) == pid) break;
            sleep_sec(0.05);
        }
        int status = 0;
        if (waitpid(pid, &status, WNOHANG) == 0) {
            kill(pid, SIGKILL);
            waitpid(pid, &status, 0);
        }
        std::lock_guard<std::mutex> state(state_lock);
        ready[name] = false;
    }

    void stop_all() {
        std::vector<std::string> names;
        {
            std::lock_guard<std::mutex> guard(proc_lock);
            for (const auto& item : procs) names.push_back(item.first);
        }
        for (const auto& name : names) stop_process(name);
    }

    void ensure() {
        std::vector<std::string> names;
        {
            std::lock_guard<std::mutex> state(state_lock);
            names = expected;
        }
        for (const auto& name : names) {
            bool restart = false;
            {
                std::lock_guard<std::mutex> guard(proc_lock);
                auto found = procs.find(name);
                if (found != procs.end()) {
                    int status = 0;
                    pid_t result = waitpid(found->second.pid, &status, WNOHANG);
                    if (result == found->second.pid) {
                        int code = WIFEXITED(status) ? WEXITSTATUS(status) : 1;
                        {
                            std::lock_guard<std::mutex> state(state_lock);
                            last_error[name] = "退出码 " + std::to_string(code);
                            ready[name] = false;
                        }
                        log_event("process_exited", "", {{"process_name", Json::str(name)}, {"code", Json::integer(code)}}, "error");
                        procs.erase(found);
                        restart = true;
                    }
                } else {
                    restart = true;
                }
            }
            if (restart) {
                bool known = false;
                {
                    std::lock_guard<std::mutex> state(state_lock);
                    known = last_error.count(name) > 0;
                }
                if (known) sleep_sec(0.5);
                spawn(name);
            }
        }
    }

    void probe_ready() {
        std::vector<std::string> names;
        {
            std::lock_guard<std::mutex> state(state_lock);
            names = expected;
        }
        for (const auto& name : names) {
            int port = 0;
            if (name == "gateway") port = 8080;
            else if (name == "order-1") port = 8101;
            else if (name == "order-2") port = 8102;
            else if (name == "recommender") port = 8200;
            else if (name == "store-a") port = 8301;
            else if (name == "store-b") port = 8302;
            else if (name.rfind("worker-", 0) == 0) port = 8400 + std::stoi(name.substr(7));
            if (!port) continue;
            bool up = false;
            try {
                up = http_request("GET", "http://127.0.0.1:" + std::to_string(port) + "/ready", nullptr, {}, 0.3).status == 200;
            } catch (const std::exception&) {
                up = false;
            }
            std::lock_guard<std::mutex> state(state_lock);
            ready[name] = up;
        }
    }

    void note_heartbeat(const Json& body) {
        std::string node = body.find("node_id") ? body.find("node_id")->text_or_empty() : "";
        if (node != "store-a" && node != "store-b") return;
        std::lock_guard<std::mutex> state(state_lock);
        last_beat[node] = monotonic();
        seen[node] = true;
        applied[node] = body.find("applied_index") ? body.find("applied_index")->int_or(0) : 0;
        if (body.find("role") && body.find("role")->text_or_empty() == "leader") {
            std::int64_t remote = body.find("epoch") ? body.find("epoch")->int_or(0) : 0;
            if (remote >= epoch) {
                epoch = remote;
                leader = node == "store-a" ? kStoreA : kStoreB;
                jobs_pending = body.find("jobs_pending") ? body.find("jobs_pending")->int_or(0) : 0;
            }
        }
        if (seen.size() >= 2) armed = true;
    }

    void maybe_promote() {
        std::string follower_url;
        std::int64_t new_epoch = 0;
        std::string old_leader;
        {
            std::lock_guard<std::mutex> state(state_lock);
            if (!armed || promoting) return;
            double now = monotonic();
            std::string leader_id = leader == kStoreA ? "store-a" : "store-b";
            std::string follower_id = leader_id == "store-a" ? "store-b" : "store-a";
            if (!last_beat.count(leader_id) || !last_beat.count(follower_id)) return;
            if (now - last_beat[leader_id] <= kLeaderLostSec || now - last_beat[follower_id] > kLeaderLostSec) return;
            promoting = true;
            gateway_paused = true;
            follower_url = follower_id == "store-b" ? kStoreB : kStoreA;
            new_epoch = epoch + 1;
            old_leader = leader;
        }
        double started = monotonic();
        try {
            Json payload = Json::obj({{"new_epoch", Json::integer(new_epoch)}, {"peer", Json::str(old_leader)}});
            HttpResult result = http_request("POST", follower_url + "/internal/promote", &payload, {}, kPromotionPauseSec);
            std::lock_guard<std::mutex> state(state_lock);
            if (result.status == 200 && !result.body.empty()) {
                Json body = Json::parse(result.body);
                epoch = body.find("epoch") ? body.find("epoch")->int_or(new_epoch) : new_epoch;
                leader = follower_url;
                last_promotion_error = Json::nul();
                log_event("promoted", "", {{"epoch", Json::integer(epoch)}, {"leader", Json::str(leader)}});
            } else {
                last_promotion_error = Json::str("提升失败 HTTP " + std::to_string(result.status));
                log_event("promoted", "", {{"message", last_promotion_error}}, "error");
            }
        } catch (const std::exception& ex) {
            std::lock_guard<std::mutex> state(state_lock);
            last_promotion_error = Json::str(ex.what());
            log_event("promoted", "", {{"message", last_promotion_error}}, "error");
        }
        std::lock_guard<std::mutex> state(state_lock);
        if (monotonic() - started > kPromotionPauseSec && last_promotion_error.is_null()) last_promotion_error = Json::str("切换超过 2 秒");
        gateway_paused = false;
        promoting = false;
    }

    void maybe_scale() {
        std::string action;
        std::string name;
        {
            std::lock_guard<std::mutex> state(state_lock);
            double now = monotonic();
            if (jobs_pending > kScaleOutPending) {
                zero_since = -1;
                if (high_since < 0) high_since = now;
                if (now - high_since >= kScaleOutHoldSec && now - last_scale >= kScaleCooldownSec && worker_count < kWorkerMax) {
                    worker_count += 1;
                    name = "worker-" + std::to_string(worker_count);
                    if (std::find(expected.begin(), expected.end(), name) == expected.end()) expected.push_back(name);
                    last_scale = now;
                    high_since = -1;
                    log_event("scaled", "", {{"workers", Json::integer(worker_count)}, {"direction", Json::str("out")}});
                    action = "out";
                }
            } else {
                high_since = -1;
            }
            if (jobs_pending == 0) {
                if (zero_since < 0) zero_since = now;
                if (now - zero_since >= kScaleInHoldSec && now - last_scale >= kScaleCooldownSec && worker_count > kWorkerMin) {
                    name = "worker-" + std::to_string(worker_count);
                    expected.erase(std::remove(expected.begin(), expected.end(), name), expected.end());
                    worker_count -= 1;
                    last_scale = now;
                    zero_since = -1;
                    log_event("scaled", "", {{"workers", Json::integer(worker_count)}, {"direction", Json::str("in")}});
                    action = "in";
                }
            } else {
                zero_since = -1;
            }
        }
        if (action == "out") spawn(name);
        else if (action == "in") stop_process(name);
    }

    Json status() {
        std::lock_guard<std::mutex> state(state_lock);
        Json ready_view = Json::obj({});
        for (const auto& name : expected) ready_view.set(name, Json::boolean(ready.count(name) && ready.at(name)));
        Json applied_view = Json::obj({});
        for (const auto& item : applied) applied_view.set(item.first, Json::integer(item.second));
        Json pid_view = Json::obj({});
        for (const auto& item : pids) pid_view.set(item.first, Json::integer(item.second));
        Json error_view = Json::obj({});
        for (const auto& item : last_error) error_view.set(item.first, Json::str(item.second));
        return Json::obj({
            {"leader", Json::str(leader)}, {"epoch", Json::integer(epoch)}, {"applied_index", applied_view},
            {"workers", Json::integer(worker_count)}, {"jobs_pending", Json::integer(jobs_pending)},
            {"gateway_paused", Json::boolean(gateway_paused)}, {"ready", ready_view},
            {"last_promotion_error", last_promotion_error}, {"pids", pid_view}, {"last_error", error_view},
        });
    }

    void loop() {
        while (!g_stop.load()) {
            try {
                ensure();
                probe_ready();
                maybe_promote();
                maybe_scale();
            } catch (const std::exception& ex) {
                log_event("supervisor_error", "", {{"message", Json::str(ex.what())}}, "error");
            }
            sleep_sec(kHeartbeatSec);
        }
        stop_all();
    }

    HttpResponse handle(const std::string& method, const std::string& path, const std::map<std::string, std::string>& headers, const std::string& raw) {
        std::string request_id = header_request_id(headers);
        auto [route, query] = split_path(path);
        (void)query;
        if (method == "GET" && route == "/health") return HttpResponse::empty(200);
        if (method == "GET" && route == "/v1/status") return HttpResponse::json(200, status());
        if (method == "GET" && route == "/internal/leader") {
            std::lock_guard<std::mutex> state(state_lock);
            return HttpResponse::json(200, Json::obj({{"leader", Json::str(leader)}, {"epoch", Json::integer(epoch)}}));
        }
        if (method == "GET" && route == "/internal/gateway-control") {
            std::lock_guard<std::mutex> state(state_lock);
            return HttpResponse::json(200, Json::obj({{"paused", Json::boolean(gateway_paused)}, {"jobs_pending", Json::integer(jobs_pending)}}));
        }
        if (method == "POST" && route == "/internal/heartbeat") {
            Json body = raw.empty() ? Json::obj({}) : Json::parse(raw);
            note_heartbeat(body);
            std::lock_guard<std::mutex> state(state_lock);
            return HttpResponse::json(200, Json::obj({{"leader", Json::str(leader)}, {"epoch", Json::integer(epoch)}}));
        }
        return HttpResponse::json(404, error_body("not_found", "没有这个路径", request_id));
    }
};
}  // namespace

int run_supervisor(int argc, char** argv) {
    std::string root = flag(argc, argv, "--root", ".");
    set_context("supervisor", root);
    mkdir_p(root + "/runtime/data");
    mkdir_p(root + "/runtime/logs");
    std::string faults = root + "/runtime/faults.json";
    if (!std::ifstream(faults)) {
        std::ofstream out(faults);
        out << "{\"delay_ms\":{\"store\":0,\"recommender\":0},\"pause_replication\":false,\"drop_heartbeat\":{\"store-a\":false,\"store-b\":false},\"exit_after_requests\":{}}\n";
    }
    start_fault_reader();
    auto supervisor = std::make_shared<Supervisor>();
    supervisor->root = root;
    char buffer[4096];
    ssize_t length = readlink("/proc/self/exe", buffer, sizeof(buffer) - 1);
    supervisor->executable = length > 0 ? std::string(buffer, buffer + length) : argv[0];
    std::thread([supervisor] {
        log_event("process_started", "", {{"port", Json::integer(8090)}});
        serve(8090, [supervisor](const std::string& method, const std::string& path, const std::map<std::string, std::string>& headers, const std::string& raw) {
            return supervisor->handle(method, path, headers, raw);
        });
    }).detach();
    sleep_sec(0.05);
    supervisor->loop();
    return 0;
}
