#pragma once

#include "json.hpp"

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

// 对照 python/src/orange/common.py。改行为先改这些数。
constexpr int kTokenCapacity = 30;
constexpr double kTokenRefillPerSec = 10;
constexpr int kGatewayInflightLimit = 8;
constexpr int kJobsPendingLimit = 50;
constexpr int kOrderDeadlineMs = 800;
constexpr int kStoreCallCapMs = 400;
constexpr int kRecommendBudgetMs = 80;
constexpr double kBreakerWindowSec = 10;
constexpr int kBreakerFailures = 5;
constexpr double kBreakerOpenSec = 5;
constexpr std::int64_t kVisibilityMs = 15000;
constexpr double kFulfillTimeoutSec = 1;
constexpr int kJobMaxAttempts = 5;
constexpr int kBackoffBaseMs = 100;
constexpr int kBackoffCapMs = 5000;
constexpr double kHeartbeatSec = 0.2;
constexpr double kLeaderLostSec = 1.0;
constexpr int kScaleOutPending = 10;
constexpr double kScaleOutHoldSec = 5;
constexpr double kScaleInHoldSec = 20;
constexpr double kScaleCooldownSec = 15;
constexpr int kWorkerMin = 1;
constexpr int kWorkerMax = 3;
constexpr double kPromotionPauseSec = 2;
constexpr double kFaultPollSec = 0.2;

extern std::atomic<bool> g_stop;

void set_context(std::string process, std::string root);
const std::string& process_name();
const std::string& root_dir();
void request_stop();
void clear_stop();

std::int64_t now_ms();
double monotonic();
std::string iso_now();
std::string new_id(const std::string& prefix);
void sleep_sec(double seconds);

void log_event(const std::string& event, const std::string& request_id, std::vector<std::pair<std::string, Json>> fields, const std::string& level = "info");

void start_fault_reader();
Json current_faults();
void fault_delay(const std::string& kind);
bool pause_replication();
bool drop_heartbeat(const std::string& node_id);
void note_http_request();

int backoff_ms(int attempt);
std::string canonical_body(const std::string& sku, std::int64_t qty, const Json& external_ref, const Json& fulfill_url);
bool valid_caller(const std::string& value);
bool valid_idempotency_key(const std::string& value);
bool valid_fulfill_url(const std::string& value);
// 成功时返回字段对象。失败时 kind 为 String，内容是错误说明。
Json validate_order_body(const Json& data);
Json error_body(const std::string& code, const std::string& message, const std::string& request_id);

std::string flag(int argc, char** argv, const std::string& name, const std::string& fallback = "");
int flag_int(int argc, char** argv, const std::string& name, int fallback);
void mkdir_p(const std::string& path);

struct TokenBucket {
    explicit TokenBucket(double capacity = kTokenCapacity, double rate = kTokenRefillPerSec);
    // first 是是否取到，second 是 Retry-After 秒。
    std::pair<bool, int> take();

    double capacity;
    double rate;
    double tokens;
    double updated;
    std::mutex lock;
};

struct CircuitBreaker {
    bool is_open();
    bool allow();
    void record(bool success);

    std::mutex lock;
    std::vector<double> failures;
    std::string state = "closed";
    double open_until = 0;
};

struct Metrics {
    void inc(const std::string& name, std::int64_t amount = 1);
    void set(const std::string& name, std::int64_t value);
    std::int64_t get(const std::string& name);
    std::string render(const std::vector<std::string>& names);

    std::mutex lock;
    std::vector<std::pair<std::string, std::int64_t>> values;
};
