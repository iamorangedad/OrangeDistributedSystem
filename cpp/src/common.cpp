#include "common.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <ctime>
#include <fstream>
#include <iostream>
#include <iterator>
#include <random>
#include <sys/stat.h>
#include <thread>

std::atomic<bool> g_stop{false};

namespace {
std::mutex g_context_lock;
std::string g_process = "orange";
std::string g_root = ".";
std::mutex g_log_lock;
std::mutex g_fault_lock;
Json g_faults = Json::obj({});
bool g_fault_error_logged = false;
std::mutex g_count_lock;
int g_request_count = 0;

}  // namespace

void set_context(std::string process, std::string root) {
    std::lock_guard<std::mutex> guard(g_context_lock);
    g_process = std::move(process);
    g_root = std::move(root);
}

const std::string& process_name() { return g_process; }
const std::string& root_dir() { return g_root; }

void request_stop() { g_stop.store(true); }
void clear_stop() { g_stop.store(false); }

std::int64_t now_ms() {
    timespec ts{};
    clock_gettime(CLOCK_REALTIME, &ts);
    return static_cast<std::int64_t>(ts.tv_sec) * 1000 + ts.tv_nsec / 1000000;
}

double monotonic() {
    timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<double>(ts.tv_sec) + static_cast<double>(ts.tv_nsec) / 1e9;
}

std::string iso_now() {
    timespec ts{};
    clock_gettime(CLOCK_REALTIME, &ts);
    std::tm broken{};
    gmtime_r(&ts.tv_sec, &broken);
    char buffer[32];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%SZ", &broken);
    return buffer;
}

std::string new_id(const std::string& prefix) {
    unsigned char bytes[16];
    std::ifstream random("/dev/urandom", std::ios::binary);
    random.read(reinterpret_cast<char*>(bytes), sizeof(bytes));
    static const char* hex = "0123456789abcdef";
    std::string out = prefix;
    for (unsigned char byte : bytes) {
        out.push_back(hex[byte >> 4]);
        out.push_back(hex[byte & 0xf]);
    }
    return out;
}

void sleep_sec(double seconds) {
    if (seconds <= 0) {
        return;
    }
    auto delay = std::chrono::duration<double>(seconds);
    std::this_thread::sleep_for(delay);
}

void log_event(const std::string& event, const std::string& request_id, std::vector<std::pair<std::string, Json>> fields, const std::string& level) {
    Json record = Json::obj({
        {"ts", Json::str(iso_now())},
        {"level", Json::str(level)},
        {"process", Json::str(process_name())},
        {"event", Json::str(event)},
        {"request_id", Json::str(request_id)},
    });
    for (auto& field : fields) {
        record.set(field.first, std::move(field.second));
    }
    std::string line = record.dump();
    std::lock_guard<std::mutex> guard(g_log_lock);
    std::cout << line << std::endl;
    std::string directory = root_dir() + "/runtime/logs";
    mkdir_p(directory);
    std::ofstream out(directory + "/" + process_name() + ".log", std::ios::app);
    if (out) {
        out << line << '\n';
    }
}

void mkdir_p(const std::string& path) {
    std::string current;
    for (size_t n = 0; n < path.size(); ++n) {
        current.push_back(path[n]);
        if (path[n] == '/' || n + 1 == path.size()) {
            if (current.empty() || current == "/") {
                continue;
            }
            mkdir(current.c_str(), 0755);
        }
    }
}

namespace {
void load_faults_once() {
    std::string path = root_dir() + "/runtime/faults.json";
    std::ifstream in(path);
    if (!in) {
        std::lock_guard<std::mutex> guard(g_fault_lock);
        g_faults = Json::obj({});
        if (!g_fault_error_logged) {
            g_fault_error_logged = true;
            log_event("faults_invalid", "", {{"message", Json::str("故障文件不存在")}}, "error");
        }
        return;
    }
    std::string text((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    try {
        Json loaded = Json::parse(text);
        if (!loaded.is_object()) {
            throw std::runtime_error("faults 必须是对象");
        }
        std::lock_guard<std::mutex> guard(g_fault_lock);
        g_faults = std::move(loaded);
        g_fault_error_logged = false;
    } catch (const std::exception& ex) {
        std::lock_guard<std::mutex> guard(g_fault_lock);
        g_faults = Json::obj({});
        if (!g_fault_error_logged) {
            g_fault_error_logged = true;
            log_event("faults_invalid", "", {{"message", Json::str(ex.what())}}, "error");
        }
    }
}
}  // namespace

void start_fault_reader() {
    load_faults_once();
    std::thread([] {
        while (!g_stop.load()) {
            sleep_sec(kFaultPollSec);
            load_faults_once();
        }
    }).detach();
}

Json current_faults() {
    std::lock_guard<std::mutex> guard(g_fault_lock);
    return g_faults;
}

void fault_delay(const std::string& kind) {
    Json faults = current_faults();
    const Json* delay = faults.find("delay_ms");
    if (delay == nullptr || !delay->is_object()) {
        return;
    }
    const Json* value = delay->find(kind);
    std::int64_t ms = value == nullptr ? 0 : value->int_or(0);
    if (ms > 0) {
        sleep_sec(static_cast<double>(ms) / 1000.0);
    }
}

bool pause_replication() {
    const Json* value = current_faults().find("pause_replication");
    return value != nullptr && value->truth();
}

bool drop_heartbeat(const std::string& node_id) {
    Json faults = current_faults();
    const Json* dropped = faults.find("drop_heartbeat");
    if (dropped == nullptr || !dropped->is_object()) {
        return false;
    }
    const Json* value = dropped->find(node_id);
    return value != nullptr && value->truth();
}

void note_http_request() {
    Json faults = current_faults();
    const Json* limits = faults.find("exit_after_requests");
    if (limits == nullptr || !limits->is_object()) {
        return;
    }
    const Json* value = limits->find(process_name());
    std::int64_t limit = value == nullptr ? 0 : value->int_or(0);
    if (limit <= 0) {
        return;
    }
    bool hit = false;
    {
        std::lock_guard<std::mutex> guard(g_count_lock);
        g_request_count += 1;
        hit = g_request_count >= limit;
    }
    if (hit) {
        log_event("exit_after_requests", "", {{"limit", Json::integer(limit)}});
        std::_Exit(0);
    }
}

int backoff_ms(int attempt) {
    int shift = std::min(16, std::max(0, attempt - 1));
    int cap = std::min(kBackoffCapMs, kBackoffBaseMs << shift);
    if (cap <= 0) {
        return 0;
    }
    thread_local std::mt19937 rng{std::random_device{}()};
    std::uniform_int_distribution<int> dist(0, cap);
    return dist(rng);
}

std::string canonical_body(const std::string& sku, std::int64_t qty, const Json& external_ref, const Json& fulfill_url) {
    std::string external = external_ref.is_string() ? external_ref.s : "";
    std::string fulfill = fulfill_url.is_string() ? fulfill_url.s : "";
    Json body = Json::obj({
        {"external_ref", Json::str(external)},
        {"fulfill_url", Json::str(fulfill)},
        {"qty", Json::integer(qty)},
        {"sku", Json::str(sku)},
    });
    return body.dump();
}

bool valid_caller(const std::string& value) {
    if (value.empty() || value.size() > 32) {
        return false;
    }
    for (unsigned char ch : value) {
        if (!((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-')) {
            return false;
        }
    }
    return true;
}

bool valid_idempotency_key(const std::string& value) {
    if (value.size() < 8 || value.size() > 128) {
        return false;
    }
    for (unsigned char ch : value) {
        if (ch < 32 || ch > 126) {
            return false;
        }
    }
    return true;
}

bool valid_fulfill_url(const std::string& value) {
    if (value.empty()) {
        return true;
    }
    const std::string prefix = "http://";
    if (value.rfind(prefix, 0) != 0) {
        return false;
    }
    std::string rest = value.substr(prefix.size());
    auto slash = rest.find('/');
    std::string authority = slash == std::string::npos ? rest : rest.substr(0, slash);
    if (authority.find('@') != std::string::npos) {
        return false;
    }
    auto colon = authority.rfind(':');
    if (colon == std::string::npos) {
        return false;
    }
    std::string host = authority.substr(0, colon);
    std::string port = authority.substr(colon + 1);
    if (host != "127.0.0.1" || port.empty() || port == "0") {
        return false;
    }
    for (unsigned char ch : port) {
        if (!std::isdigit(ch)) {
            return false;
        }
    }
    return true;
}

Json validate_order_body(const Json& data) {
    if (!data.is_object()) {
        return Json::str("正文必须是 JSON 对象");
    }
    const Json* sku = data.find("sku");
    if (sku == nullptr || !sku->is_string() || sku->s.empty() || sku->s.size() > 64) {
        return Json::str("sku 必须是 1 到 64 个字符");
    }
    const Json* qty = data.find("qty");
    if (qty == nullptr || qty->is_bool() || !qty->is_int() || qty->i < 1 || qty->i > 100) {
        return Json::str("qty 必须是 1 到 100 的整数");
    }
    const Json* external = data.find("external_ref");
    if (external != nullptr && !external->is_null() && (!external->is_string() || external->s.size() > 128)) {
        return Json::str("external_ref 最长 128 个字符");
    }
    const Json* fulfill = data.find("fulfill_url");
    if (fulfill != nullptr && !fulfill->is_null() && !fulfill->is_string()) {
        return Json::str("fulfill_url 必须是字符串");
    }
    std::string fulfill_text = fulfill != nullptr && fulfill->is_string() ? fulfill->s : "";
    if (!valid_fulfill_url(fulfill_text)) {
        return Json::str("fulfill_url 只能是 http://127.0.0.1 上的地址");
    }
    Json external_value = external == nullptr ? Json::nul() : *external;
    Json fulfill_value = fulfill_text.empty() ? Json::nul() : Json::str(fulfill_text);
    return Json::obj({
        {"sku", Json::str(sku->s)},
        {"qty", Json::integer(qty->i)},
        {"external_ref", std::move(external_value)},
        {"fulfill_url", std::move(fulfill_value)},
    });
}

Json error_body(const std::string& code, const std::string& message, const std::string& request_id) {
    return Json::obj({{"error", Json::obj({
                                    {"code", Json::str(code)},
                                    {"message", Json::str(message)},
                                    {"request_id", Json::str(request_id)},
                                })}});
}

std::string flag(int argc, char** argv, const std::string& name, const std::string& fallback) {
    for (int n = 1; n + 1 < argc; ++n) {
        if (argv[n] == name) {
            return argv[n + 1];
        }
    }
    return fallback;
}

int flag_int(int argc, char** argv, const std::string& name, int fallback) {
    std::string value = flag(argc, argv, name, "");
    if (value.empty()) {
        return fallback;
    }
    return std::stoi(value);
}

TokenBucket::TokenBucket(double capacity_value, double rate_value)
    : capacity(capacity_value), rate(rate_value), tokens(capacity_value), updated(monotonic()) {}

std::pair<bool, int> TokenBucket::take() {
    std::lock_guard<std::mutex> guard(lock);
    double now = monotonic();
    tokens = std::min(capacity, tokens + (now - updated) * rate);
    updated = now;
    if (tokens >= 1) {
        tokens -= 1;
        return {true, 0};
    }
    if (rate <= 0) {
        return {false, 1};
    }
    int retry = static_cast<int>(std::max(1.0, std::ceil((1 - tokens) / rate)));
    return {false, retry};
}

bool CircuitBreaker::is_open() {
    std::lock_guard<std::mutex> guard(lock);
    return state == "open" && monotonic() < open_until;
}

bool CircuitBreaker::allow() {
    std::lock_guard<std::mutex> guard(lock);
    double now = monotonic();
    if (state == "open") {
        if (now < open_until) {
            return false;
        }
        state = "half";
        return true;
    }
    if (state == "half") {
        return false;
    }
    return true;
}

void CircuitBreaker::record(bool success) {
    std::lock_guard<std::mutex> guard(lock);
    double now = monotonic();
    if (state == "half") {
        if (success) {
            state = "closed";
            failures.clear();
        } else {
            state = "open";
            open_until = now + kBreakerOpenSec;
        }
        return;
    }
    if (success) {
        return;
    }
    failures.erase(std::remove_if(failures.begin(), failures.end(), [&](double item) { return now - item > kBreakerWindowSec; }), failures.end());
    failures.push_back(now);
    if (static_cast<int>(failures.size()) >= kBreakerFailures) {
        state = "open";
        open_until = now + kBreakerOpenSec;
        failures.clear();
    }
}

void Metrics::inc(const std::string& name, std::int64_t amount) {
    std::lock_guard<std::mutex> guard(lock);
    for (auto& item : values) {
        if (item.first == name) {
            item.second += amount;
            return;
        }
    }
    values.emplace_back(name, amount);
}

void Metrics::set(const std::string& name, std::int64_t value) {
    std::lock_guard<std::mutex> guard(lock);
    for (auto& item : values) {
        if (item.first == name) {
            item.second = value;
            return;
        }
    }
    values.emplace_back(name, value);
}

std::int64_t Metrics::get(const std::string& name) {
    std::lock_guard<std::mutex> guard(lock);
    for (const auto& item : values) {
        if (item.first == name) {
            return item.second;
        }
    }
    return 0;
}

std::string Metrics::render(const std::vector<std::string>& names) {
    std::lock_guard<std::mutex> guard(lock);
    std::string out;
    for (const auto& name : names) {
        std::int64_t value = 0;
        for (const auto& item : values) {
            if (item.first == name) {
                value = item.second;
            }
        }
        out += name + " " + std::to_string(value) + "\n";
    }
    return out;
}
