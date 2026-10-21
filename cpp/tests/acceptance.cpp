#include "common.hpp"
#include "http.hpp"

#include <spawn.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdio>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>

extern char** environ;

namespace {
const char* kGateway = "http://127.0.0.1:8080";
std::string g_binary;
int g_failed = 0;

struct Result {
    int status = 0;
    std::map<std::string, std::string> headers;
    Json body;
    std::string raw;
};

void check(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

std::string read_file(const std::filesystem::path& path) {
    std::ifstream in(path);
    if (!in) {
        return "";
    }
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

int count_text(const std::string& text, const std::string& needle) {
    int found = 0;
    std::size_t from = 0;
    while (true) {
        std::size_t at = text.find(needle, from);
        if (at == std::string::npos) {
            return found;
        }
        ++found;
        from = at + needle.size();
    }
}

const Json* child(const Json& node, const std::string& key) {
    const Json* value = node.find(key);
    return value == nullptr || value->is_null() ? nullptr : value;
}

std::string error_code(const Json& body) {
    const Json* error = child(body, "error");
    const Json* code = error == nullptr ? nullptr : error->find("code");
    return code == nullptr ? "" : code->text_or_empty();
}

std::string error_message(const Json& body) {
    const Json* error = child(body, "error");
    const Json* message = error == nullptr ? nullptr : error->find("message");
    return message == nullptr ? "" : message->text_or_empty();
}

Result http_quiet(const std::string& method, const std::string& url, const Json* body, const std::map<std::string, std::string>& headers, double timeout) {
    Result result;
    try {
        HttpResult response = http_request(method, url, body, headers, timeout);
        result.status = response.status;
        result.headers = response.headers;
        result.raw = response.body;
        if (!response.body.empty()) {
            result.body = Json::parse(response.body);
        }
    } catch (const std::exception& ex) {
        result.raw = ex.what();
    }
    return result;
}

Result post(const std::string& sku, std::int64_t qty, const std::string& key, const Json* extra) {
    Json body = Json::obj({{"sku", Json::str(sku)}, {"qty", Json::integer(qty)}});
    if (extra != nullptr) {
        for (const auto& item : extra->o) {
            body.set(item.first, item.second);
        }
    }
    std::map<std::string, std::string> headers = {
        {"X-Caller-Id", "gear-card"},
        {"Idempotency-Key", key},
        {"X-Request-Id", "req-" + std::to_string(now_ms())},
    };
    return http_quiet("POST", std::string(kGateway) + "/v1/orders", &body, headers, 3);
}

class Cluster {
public:
    std::filesystem::path root;
    pid_t pid = -1;

    static Cluster start() {
        char tmpl[] = "/tmp/orange-cpp-XXXXXX";
        if (mkdtemp(tmpl) == nullptr) {
            throw std::runtime_error("创建临时目录失败");
        }
        Cluster cluster;
        cluster.root = tmpl;
        std::filesystem::create_directories(cluster.root / "runtime");
        std::string out = (cluster.root / "runtime" / "supervisor.out").string();
        std::string root = cluster.root.string();
        std::string binary = g_binary;
        std::vector<char*> argv = {binary.data(), const_cast<char*>("supervisor"), const_cast<char*>("--root"), root.data(), nullptr};
        posix_spawn_file_actions_t actions;
        posix_spawn_file_actions_init(&actions);
        posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, out.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        posix_spawn_file_actions_adddup2(&actions, STDOUT_FILENO, STDERR_FILENO);
        posix_spawnattr_t attr;
        posix_spawnattr_init(&attr);
#ifdef POSIX_SPAWN_SETSID
        posix_spawnattr_setflags(&attr, POSIX_SPAWN_SETSID);
#endif
        int spawned = posix_spawn(&cluster.pid, binary.c_str(), &actions, &attr, argv.data(), environ);
        posix_spawn_file_actions_destroy(&actions);
        posix_spawnattr_destroy(&attr);
        if (spawned != 0) {
            throw std::runtime_error("拉起监督者失败");
        }
        double deadline = monotonic() + 20;
        while (monotonic() < deadline) {
            Result ready = http_quiet("GET", std::string(kGateway) + "/ready", nullptr, {}, 1);
            if (ready.status == 200) {
                return cluster;
            }
            int status = 0;
            if (waitpid(cluster.pid, &status, WNOHANG) == cluster.pid) {
                cluster.pid = -1;
                break;
            }
            sleep_sec(0.2);
        }
        std::string diagnostics = cluster.diagnostics();
        cluster.stop();
        throw std::runtime_error(diagnostics);
    }

    Json status() {
        Result result = http_quiet("GET", "http://127.0.0.1:8090/v1/status", nullptr, {}, 1);
        return result.body;
    }

    void faults(std::int64_t store_delay, std::int64_t recommend_delay, bool drop_a) {
        Json payload = Json::obj({
            {"delay_ms", Json::obj({{"store", Json::integer(store_delay)}, {"recommender", Json::integer(recommend_delay)}})},
            {"pause_replication", Json::boolean(false)},
            {"drop_heartbeat", Json::obj({{"store-a", Json::boolean(drop_a)}, {"store-b", Json::boolean(false)}})},
            {"exit_after_requests", Json::obj({})},
        });
        {
            std::ofstream out(root / "runtime" / "faults.json", std::ios::trunc);
            out << payload.dump() << '\n';
            out.flush();
        }
        sleep_sec(0.6);
    }

    std::string logs(const std::string& name) { return read_file(root / "runtime" / "logs" / (name + ".log")); }

    std::string diagnostics() {
        std::string out = read_file(root / "runtime" / "supervisor.out");
        std::filesystem::path directory = root / "runtime" / "logs";
        if (!std::filesystem::is_directory(directory)) {
            return out.empty() ? "监督者没有留下日志" : out;
        }
        for (const auto& entry : std::filesystem::directory_iterator(directory)) {
            out += "----- " + entry.path().filename().string() + " -----\n";
            out += read_file(entry.path());
            out += "\n";
        }
        return out.empty() ? "监督者没有留下日志" : out;
    }

    void stop() {
        if (pid <= 0) {
            return;
        }
        if (pid > 0) {
            if (kill(-pid, SIGTERM) != 0) {
                kill(pid, SIGTERM);
            }
            for (int n = 0; n < 80; ++n) {
                int status = 0;
                if (waitpid(pid, &status, WNOHANG) == pid) {
                    pid = -1;
                    break;
                }
                sleep_sec(0.1);
            }
            if (pid > 0) {
                if (kill(-pid, SIGKILL) != 0) {
                    kill(pid, SIGKILL);
                }
                waitpid(pid, nullptr, 0);
                pid = -1;
            }
        }
        sleep_sec(0.3);
        std::error_code ignored;
        std::filesystem::remove_all(root, ignored);
    }

    Cluster() = default;
    Cluster(const Cluster&) = delete;
    Cluster& operator=(const Cluster&) = delete;
    Cluster(Cluster&& other) noexcept : root(std::move(other.root)), pid(other.pid) { other.pid = -1; }
    Cluster& operator=(Cluster&&) = delete;
    ~Cluster() { stop(); }
};

class Fulfill {
public:
    int port = 0;
    std::vector<std::string> calls;
    std::vector<std::string> applied;
    std::atomic<bool> released{false};
    std::atomic<bool> stop_flag{false};
    int listen_fd = -1;
    std::thread thread;
    std::mutex lock;
    std::mutex workers_lock;
    std::vector<std::thread> workers;

    static std::unique_ptr<Fulfill> start(int status, double delay, bool hang_first) {
        auto fulfill = std::make_unique<Fulfill>();
        fulfill->listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
        int yes = 1;
        setsockopt(fulfill->listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        if (bind(fulfill->listen_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
            throw std::runtime_error("履约端口绑定失败");
        }
        listen(fulfill->listen_fd, 64);
        socklen_t length = sizeof(address);
        getsockname(fulfill->listen_fd, reinterpret_cast<sockaddr*>(&address), &length);
        fulfill->port = ntohs(address.sin_port);
        Fulfill* self = fulfill.get();
        int fd = self->listen_fd;
        self->thread = std::thread([self, fd, status, delay, hang_first] {
            while (!self->stop_flag.load()) {
                int client = accept(fd, nullptr, nullptr);
                if (client < 0) {
                    break;
                }
                std::lock_guard<std::mutex> guard(self->workers_lock);
                self->workers.emplace_back(&Fulfill::handle, self, client, status, delay, hang_first);
            }
        });
        return fulfill;
    }

    std::string url() const { return "http://127.0.0.1:" + std::to_string(port) + "/hooks/orange-fulfill"; }

    void release() { released.store(true); }

    void handle(int client, int status, double delay, bool hang_first) {
        std::string header;
        char byte = 0;
        while (header.size() < 4 || header.substr(header.size() - 4) != "\r\n\r\n") {
            ssize_t got = recv(client, &byte, 1, 0);
            if (got <= 0) {
                ::close(client);
                return;
            }
            header.push_back(byte);
        }
        int length = 0;
        std::string lower = header;
        for (char& ch : lower) {
            ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        }
        auto at = lower.find("content-length:");
        if (at != std::string::npos) {
            length = std::stoi(lower.substr(at + std::string("content-length:").size()));
        }
        std::string payload(static_cast<std::size_t>(length), '\0');
        std::size_t got_body = 0;
        while (got_body < payload.size()) {
            ssize_t got = recv(client, payload.data() + got_body, payload.size() - got_body, 0);
            if (got <= 0) {
                break;
            }
            got_body += static_cast<std::size_t>(got);
        }
        Json json = payload.empty() ? Json::obj({}) : Json::parse(payload);
        std::string order_id = json.find("order_id") ? json.find("order_id")->text_or_empty() : "";
        bool first = false;
        {
            std::lock_guard<std::mutex> guard(lock);
            first = calls.empty();
            calls.push_back(order_id);
        }
        if (hang_first && first) {
            double deadline = monotonic() + 30;
            while (!released.load() && monotonic() < deadline) {
                sleep_sec(0.02);
            }
        } else if (delay > 0) {
            sleep_sec(delay);
        }
        std::string response = "HTTP/1.1 " + std::to_string(status) + " OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}";
        send(client, response.data(), response.size(), MSG_NOSIGNAL);
        ::close(client);
        if ((status >= 200 && status < 300) || status == 409) {
            std::lock_guard<std::mutex> guard(lock);
            if (!order_id.empty() && std::find(applied.begin(), applied.end(), order_id) == applied.end()) {
                applied.push_back(order_id);
            }
        }
    }

    void close() {
        release();
        stop_flag.store(true);
        if (listen_fd >= 0) {
            ::shutdown(listen_fd, SHUT_RDWR);
            ::close(listen_fd);
            listen_fd = -1;
        }
        if (thread.joinable()) {
            thread.join();
        }
        std::vector<std::thread> pending;
        {
            std::lock_guard<std::mutex> guard(workers_lock);
            pending.swap(workers);
        }
        for (auto& worker : pending) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

    ~Fulfill() { close(); }
};

void unit() {
    check(canonical_body("gear-1", 1, Json::nul(), Json::nul()) == "{\"external_ref\":\"\",\"fulfill_url\":\"\",\"qty\":1,\"sku\":\"gear-1\"}", "canonical");
    check(canonical_body("gear-1", 1, Json::str("cart"), Json::str("")) == "{\"external_ref\":\"cart\",\"fulfill_url\":\"\",\"qty\":1,\"sku\":\"gear-1\"}", "canonical cart");
    check(validate_order_body(Json::obj({{"sku", Json::str("gear-1")}, {"qty", Json::str("1")}})).is_string(), "qty string");
    check(validate_order_body(Json::obj({{"sku", Json::str("gear-1")}, {"qty", Json::boolean(true)}})).is_string(), "qty bool");
    check(valid_fulfill_url(""), "empty fulfill");
    check(valid_fulfill_url("http://127.0.0.1:9000/hooks"), "loopback");
    check(!valid_fulfill_url("http://127.0.0.1/hooks"), "missing port");
    check(!valid_fulfill_url("https://127.0.0.1:9000/hooks"), "https");
    check(!valid_fulfill_url("http://user:pass@127.0.0.1:9000/hooks"), "userinfo");
    check(!valid_fulfill_url("http://example.com:9000/hooks"), "example.com");
    TokenBucket bucket(2, 0);
    check(bucket.take().first, "token");
    check(bucket.take().first, "token second");
    auto [allowed, retry_after] = bucket.take();
    check(!allowed && retry_after >= 1, "token empty");
    CircuitBreaker breaker;
    for (int n = 0; n < 5; ++n) {
        check(breaker.allow(), "breaker allow");
        breaker.record(false);
    }
    check(!breaker.allow(), "breaker open");
}

void ac1() {
    Cluster cluster = Cluster::start();
    Result first = post("gear-1", 1, "same-key-0001", nullptr);
    check(first.status == 201, "ac1 first " + first.raw + cluster.diagnostics());
    Result second = post("gear-1", 1, "same-key-0001", nullptr);
    check(second.status == 201, "ac1 second " + second.raw);
    check(child(first.body, "order_id") && child(second.body, "order_id") && child(first.body, "order_id")->text_or_empty() == child(second.body, "order_id")->text_or_empty(), "ac1 id");
}

void ac2() {
    Cluster cluster = Cluster::start();
    Result first = post("gear-1", 1, "same-key-0002", nullptr);
    check(first.status == 201, "ac2 " + first.raw + cluster.diagnostics());
    Result second = post("gear-2", 1, "same-key-0002", nullptr);
    check(second.status == 409, "ac2 status " + second.raw);
    check(error_code(second.body) == "idempotency_mismatch", "ac2 code");
    std::string order_id = child(first.body, "order_id")->text_or_empty();
    Result got = http_quiet("GET", std::string(kGateway) + "/v1/orders/" + order_id, nullptr, {{"X-Caller-Id", "gear-card"}}, 2);
    check(got.status == 200, "ac2 get");
    check(child(got.body, "sku") && child(got.body, "sku")->text_or_empty() == "gear-1", "ac2 sku");
}

void ac3() {
    Cluster cluster = Cluster::start();
    std::int64_t pid = child(cluster.status(), "pids")->find("order-2")->i;
    kill(static_cast<pid_t>(pid), SIGTERM);
    bool saw_down = false;
    bool restored = false;
    double deadline = monotonic() + 8;
    while (monotonic() < deadline) {
        Json view = cluster.status();
        bool ready = child(view, "ready") && child(view, "ready")->find("order-2") && child(view, "ready")->find("order-2")->truth();
        saw_down = saw_down || !ready;
        const Json* next = child(view, "pids") ? child(view, "pids")->find("order-2") : nullptr;
        if (ready && next != nullptr && next->i != pid) {
            restored = true;
            break;
        }
        sleep_sec(0.1);
    }
    Result body = post("gear-1", 1, "after-order-b", nullptr);
    check(body.status == 201, "ac3 post " + body.raw);
    check(saw_down, cluster.diagnostics());
    check(restored, cluster.diagnostics());
}

void ac4() {
    Cluster cluster = Cluster::start();
    std::int64_t pid = child(cluster.status(), "pids")->find("recommender")->i;
    kill(static_cast<pid_t>(pid), SIGTERM);
    double deadline = monotonic() + 3;
    Result body;
    while (monotonic() < deadline) {
        body = post("gear-1", 1, "degrade-" + std::to_string(now_ms()), nullptr);
        const Json* degraded = child(body.body, "degraded");
        if (body.status == 201 && degraded != nullptr && degraded->truth() && child(body.body, "recommendation") == nullptr) {
            break;
        }
        sleep_sec(0.05);
    }
    check(body.status == 201 && child(body.body, "degraded") && child(body.body, "degraded")->truth(), "ac4 " + body.raw + cluster.diagnostics());
}

void ac5() {
    Cluster cluster = Cluster::start();
    cluster.faults(0, 500, false);
    for (int n = 0; n < 12; ++n) {
        char key[32];
        std::snprintf(key, sizeof(key), "breaker-%02d-key", n);
        Result body = post("gear-1", 1, key, nullptr);
        check(body.status == 201, "ac5 " + body.raw + cluster.diagnostics());
    }
    sleep_sec(0.3);
    int before = count_text(cluster.logs("recommender"), "\"event\":\"recommend_served\"");
    Result after_order = post("gear-1", 1, "breaker-after-open", nullptr);
    check(after_order.status == 201, "ac5 after");
    sleep_sec(0.3);
    int after = count_text(cluster.logs("recommender"), "\"event\":\"recommend_served\"");
    check(before == after, cluster.logs("recommender"));
    check((cluster.logs("order-1") + cluster.logs("order-2")).find("\"event\":\"breaker_open\"") != std::string::npos, "breaker");
}

void ac6() {
    Cluster cluster = Cluster::start();
    cluster.faults(350, 0, false);
    std::vector<std::thread> threads;
    for (int n = 0; n < 8; ++n) {
        threads.emplace_back([n] {
            char key[32];
            std::snprintf(key, sizeof(key), "hold-%02d-xxxx", n);
            post("gear-1", 1, key, nullptr);
        });
    }
    double deadline = monotonic() + 2;
    while (monotonic() < deadline) {
        Result metrics = http_quiet("GET", std::string(kGateway) + "/metrics", nullptr, {}, 1);
        if (metrics.raw.find("orange_inflight 8") != std::string::npos) {
            break;
        }
        sleep_sec(0.02);
    }
    std::mutex results_lock;
    std::vector<Result> results;
    for (int n = 0; n < 40; ++n) {
        threads.emplace_back([n, &results, &results_lock] {
            char key[32];
            std::snprintf(key, sizeof(key), "bucket-%02d-xx", n);
            Result result = post("gear-1", 1, key, nullptr);
            std::lock_guard<std::mutex> guard(results_lock);
            results.push_back(std::move(result));
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    bool limited = false;
    bool retry = false;
    std::string summary;
    for (const Result& result : results) {
        summary += std::to_string(result.status) + " " + error_message(result.body) + "; ";
        if (result.status == 429 && error_message(result.body) == "令牌桶已空") {
            limited = true;
            retry = result.headers.count("retry-after") > 0;
        }
    }
    check(limited, "ac6 " + summary);
    check(retry, "ac6 retry");
}

void ac7() {
    Cluster cluster = Cluster::start();
    auto fulfill = Fulfill::start(200, 0, true);
    Json extra = Json::obj({{"fulfill_url", Json::str(fulfill->url())}});
    Result body = post("gear-1", 1, "visibility-timeout", &extra);
    check(body.status == 201, "ac7 " + body.raw + cluster.diagnostics());
    double deadline = monotonic() + 5;
    bool called = false;
    while (monotonic() < deadline) {
        {
            std::lock_guard<std::mutex> guard(fulfill->lock);
            called = !fulfill->calls.empty();
        }
        if (called) {
            break;
        }
        sleep_sec(0.05);
    }
    check(called, cluster.diagnostics());
    std::int64_t pid = child(cluster.status(), "pids")->find("worker-1")->i;
    kill(static_cast<pid_t>(pid), SIGTERM);
    fulfill->release();
    deadline = monotonic() + 25;
    std::string state;
    std::string order_id = child(body.body, "order_id")->text_or_empty();
    while (monotonic() < deadline) {
        Result got = http_quiet("GET", std::string(kGateway) + "/v1/orders/" + order_id, nullptr, {{"X-Caller-Id", "gear-card"}}, 2);
        check(got.status == 200, cluster.diagnostics());
        state = child(got.body, "state") ? child(got.body, "state")->text_or_empty() : "";
        if (state == "fulfilled") {
            break;
        }
        sleep_sec(0.5);
    }
    check(state == "fulfilled", cluster.diagnostics());
    std::lock_guard<std::mutex> guard(fulfill->lock);
    check(fulfill->applied.size() == 1, "ac7 applied");
}

void ac8() {
    Cluster cluster = Cluster::start();
    auto fulfill = Fulfill::start(500, 0, false);
    Json extra = Json::obj({{"fulfill_url", Json::str(fulfill->url())}});
    Result body = post("gear-1", 1, "dead-letter-0001", &extra);
    check(body.status == 201, "ac8 " + body.raw);
    double deadline = monotonic() + 20;
    std::string order_id = child(body.body, "order_id")->text_or_empty();
    while (monotonic() < deadline) {
        Result got = http_quiet("GET", std::string(kGateway) + "/v1/orders/" + order_id, nullptr, {{"X-Caller-Id", "gear-card"}}, 2);
        const Json* state = child(got.body, "state");
        if (state != nullptr && state->text_or_empty() == "fulfillment_failed") {
            const Json* fulfillment = child(got.body, "fulfillment");
            check(fulfillment && fulfillment->find("status") && fulfillment->find("status")->text_or_empty() == "dead", "ac8 dead");
            check(fulfillment->find("attempts") && fulfillment->find("attempts")->int_or(0) >= 5, "ac8 attempts");
            return;
        }
        sleep_sec(0.3);
    }
    check(false, cluster.diagnostics());
}

void ac9() {
    Cluster cluster = Cluster::start();
    std::string original = child(cluster.status(), "leader")->text_or_empty();
    cluster.faults(0, 0, true);
    double deadline = monotonic() + 8;
    bool promoted = false;
    while (monotonic() < deadline) {
        Json view = cluster.status();
        std::int64_t epoch = child(view, "epoch") ? child(view, "epoch")->int_or(0) : 0;
        std::string leader = child(view, "leader") ? child(view, "leader")->text_or_empty() : "";
        if (leader != original && epoch > 1) {
            promoted = true;
            break;
        }
        sleep_sec(0.2);
    }
    check(promoted, cluster.diagnostics());
    cluster.faults(0, 0, false);
    deadline = monotonic() + 5;
    bool fenced = false;
    Json payload = Json::obj({
        {"epoch", Json::integer(1)},
        {"caller_id", Json::str("gear-card")},
        {"idempotency_key", Json::str("fenced-write-01")},
        {"order", Json::obj({{"sku", Json::str("gear-1")}, {"qty", Json::integer(1)}, {"external_ref", Json::nul()}, {"fulfill_url", Json::nul()}})},
    });
    while (monotonic() < deadline) {
        Result result = http_quiet("POST", original + "/internal/append", &payload, {}, 1);
        if (result.status == 409 && error_code(result.body) == "epoch_fenced") {
            fenced = true;
            break;
        }
        sleep_sec(0.2);
    }
    check(fenced, cluster.diagnostics());
    Result created = post("gear-1", 1, "after-promotion", nullptr);
    check(created.status == 201, "ac9 " + created.raw + cluster.diagnostics());
    check(child(created.body, "epoch") && child(created.body, "epoch")->int_or(0) > 1, "ac9 epoch");
}

void ac10() {
    Cluster cluster = Cluster::start();
    auto fulfill = Fulfill::start(500, 0.9, false);
    Json extra = Json::obj({{"fulfill_url", Json::str(fulfill->url())}});
    bool limited = false;
    bool created = false;
    for (int n = 0; n < 70; ++n) {
        char key[32];
        std::snprintf(key, sizeof(key), "pile-%02d-order", n);
        Result body = post("gear-1", 1, key, &extra);
        if (body.status == 201) {
            created = true;
            continue;
        }
        std::string message = error_message(body.body);
        if (body.status == 429 && message == "待处理任务已达上限") {
            limited = true;
            break;
        }
        if (body.status == 429 && message == "令牌桶已空") {
            sleep_sec(0.2);
            continue;
        }
        check(false, "意外响应 " + std::to_string(body.status) + " " + body.raw + cluster.diagnostics());
    }
    check(limited, cluster.diagnostics());
    check(created, "ac10 created");
    int before = count_text(cluster.logs("store-a"), "\"event\":\"job_claimed\"");
    double deadline = monotonic() + 8;
    bool progressed = false;
    while (monotonic() < deadline) {
        if (count_text(cluster.logs("store-a"), "\"event\":\"job_claimed\"") > before) {
            progressed = true;
            break;
        }
        sleep_sec(0.3);
    }
    check(progressed, cluster.diagnostics());
}

void ac11() {
    Cluster cluster = Cluster::start();
    auto fulfill = Fulfill::start(500, 0.9, false);
    Json extra = Json::obj({{"fulfill_url", Json::str(fulfill->url())}});
    for (int n = 0; n < 40; ++n) {
        while (true) {
            char key[32];
            std::snprintf(key, sizeof(key), "scale-%02d-order", n);
            Result body = post("gear-1", 1, key, &extra);
            if (body.status == 429 && error_message(body.body) == "令牌桶已空") {
                sleep_sec(0.2);
                continue;
            }
            check(body.status == 201, "ac11 " + body.raw + cluster.diagnostics());
            break;
        }
    }
    double deadline = monotonic() + 40;
    while (monotonic() < deadline) {
        const Json* workers = child(cluster.status(), "workers");
        if (workers != nullptr && workers->int_or(0) >= 3) {
            break;
        }
        sleep_sec(0.5);
    }
    check(child(cluster.status(), "workers") && child(cluster.status(), "workers")->int_or(0) >= 3, cluster.diagnostics());
    sleep_sec(10);
    check(child(cluster.status(), "workers") && child(cluster.status(), "workers")->int_or(0) == 3, cluster.diagnostics());
}

void ac12() {
    Cluster cluster = Cluster::start();
    cluster.faults(200, 0, false);
    Json body = Json::obj({{"sku", Json::str("gear-1")}, {"qty", Json::integer(1)}});
    std::string request_id = "req-trace-ac12-0001";
    Result result = http_quiet("POST", std::string(kGateway) + "/v1/orders", &body, {
        {"X-Caller-Id", "gear-card"},
        {"Idempotency-Key", "trace-request-001"},
        {"X-Request-Id", request_id},
    }, 3);
    check(result.status == 201, result.raw);
    sleep_sec(0.3);
    check(cluster.logs("gateway").find(request_id) != std::string::npos, cluster.logs("gateway"));
    check((cluster.logs("order-1") + cluster.logs("order-2")).find(request_id) != std::string::npos, "orders");
    check((cluster.logs("store-a") + cluster.logs("store-b")).find(request_id) != std::string::npos, "stores");
}

void run(const std::string& name, void (*test)()) {
    try {
        test();
        std::cout << name << " 通过" << std::endl;
    } catch (const std::exception& ex) {
        ++g_failed;
        std::cerr << name << " 失败: " << ex.what() << std::endl;
    }
}
}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "用法: acceptance <orange 可执行文件>\n";
        return 2;
    }
    g_binary = argv[1];
    try {
        unit();
        std::cout << "单元检查通过" << std::endl;
    } catch (const std::exception& ex) {
        std::cerr << "单元检查失败: " << ex.what() << std::endl;
        return 1;
    }
    run("AC-1", ac1);
    run("AC-2", ac2);
    run("AC-3", ac3);
    run("AC-4", ac4);
    run("AC-5", ac5);
    run("AC-6", ac6);
    run("AC-7", ac7);
    run("AC-8", ac8);
    run("AC-9", ac9);
    run("AC-10", ac10);
    run("AC-11", ac11);
    run("AC-12", ac12);
    if (g_failed > 0) {
        std::cerr << g_failed << " 个验收失败" << std::endl;
        return 1;
    }
    std::cout << "C++ 验收通过" << std::endl;
    return 0;
}
