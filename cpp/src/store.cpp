#include "common.hpp"
#include "http.hpp"

#include <sqlite3.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>

namespace {

const char* kSchema = R"SQL(
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY, caller_id TEXT NOT NULL, idempotency_key TEXT NOT NULL,
    external_ref TEXT, sku TEXT NOT NULL, qty INTEGER NOT NULL, fulfill_url TEXT,
    state TEXT NOT NULL, recommendation TEXT, degraded INTEGER NOT NULL, epoch INTEGER NOT NULL,
    log_index INTEGER NOT NULL, created_at TEXT NOT NULL, response_json TEXT NOT NULL,
    request_hash TEXT NOT NULL, UNIQUE (caller_id, idempotency_key));
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY, order_id TEXT NOT NULL, kind TEXT NOT NULL, state TEXT NOT NULL,
    attempts INTEGER NOT NULL, visible_after INTEGER NOT NULL, worker_id TEXT, last_error TEXT,
    log_index INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS replication_log (
    log_index INTEGER PRIMARY KEY, epoch INTEGER NOT NULL, kind TEXT NOT NULL,
    payload TEXT NOT NULL, committed_at TEXT NOT NULL);
)SQL";

struct Stmt {
    sqlite3_stmt* raw = nullptr;
    explicit Stmt(sqlite3* db, const char* sql) {
        if (sqlite3_prepare_v2(db, sql, -1, &raw, nullptr) != SQLITE_OK) {
            throw std::runtime_error(sqlite3_errmsg(db));
        }
    }
    ~Stmt() { sqlite3_finalize(raw); }
    void bind_text(int index, const std::string& value) { sqlite3_bind_text(raw, index, value.c_str(), -1, SQLITE_TRANSIENT); }
    void bind_null(int index) { sqlite3_bind_null(raw, index); }
    void bind_int(int index, std::int64_t value) { sqlite3_bind_int64(raw, index, value); }
    bool next() { return sqlite3_step(raw) == SQLITE_ROW; }
    int column(const char* name) const {
        int count = sqlite3_column_count(raw);
        for (int n = 0; n < count; ++n) {
            if (std::strcmp(sqlite3_column_name(raw, n), name) == 0) {
                return n;
            }
        }
        return -1;
    }
    bool is_null(const char* name) const { return sqlite3_column_type(raw, column(name)) == SQLITE_NULL; }
    std::string text(const char* name) const {
        auto value = sqlite3_column_text(raw, column(name));
        return value == nullptr ? "" : reinterpret_cast<const char*>(value);
    }
    std::int64_t integer(const char* name) const { return sqlite3_column_int64(raw, column(name)); }
};

void exec_sql(sqlite3* db, const std::string& sql) {
    char* error = nullptr;
    if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &error) != SQLITE_OK) {
        std::string message = error == nullptr ? "sqlite" : error;
        sqlite3_free(error);
        throw std::runtime_error(message);
    }
}

void bind_optional(Stmt& stmt, int index, const Json& value) {
    if (value.is_string()) {
        stmt.bind_text(index, value.s);
    } else {
        stmt.bind_null(index);
    }
}

Json optional_text(const Stmt& stmt, const char* name) {
    return stmt.is_null(name) ? Json::nul() : Json::str(stmt.text(name));
}

class Store {
public:
    Store(std::string node_id, std::string role, std::string peer, const std::string& path, std::string supervisor)
        : node_id_(std::move(node_id)), peer_(std::move(peer)), supervisor_(std::move(supervisor)) {
        mkdir_p(path.substr(0, path.find_last_of('/')));
        if (sqlite3_open_v2(path.c_str(), &db_, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nullptr) != SQLITE_OK) {
            throw std::runtime_error("open sqlite");
        }
        sqlite3_busy_timeout(db_, 5000);
        exec_sql(db_, "PRAGMA journal_mode=WAL");
        exec_sql(db_, "PRAGMA synchronous=FULL");
        std::string schema = kSchema;
        size_t start = 0;
        while (start < schema.size()) {
            auto end = schema.find(';', start);
            std::string piece = schema.substr(start, end == std::string::npos ? std::string::npos : end - start);
            start = end == std::string::npos ? schema.size() : end + 1;
            if (piece.find("CREATE") == std::string::npos) {
                continue;
            }
            exec_sql(db_, piece);
        }
        ensure_meta(role);
    }

    ~Store() { sqlite3_close(db_); }

    HttpResponse handle(const std::string& method, const std::string& path, const std::map<std::string, std::string>& headers, const std::string& raw) {
        std::string request_id = header_request_id(headers);
        auto [route, query] = split_path(path);
        if (method == "GET" && route == "/health") return HttpResponse::empty(200);
        if (method == "GET" && route == "/ready") return HttpResponse::empty(is_leader() || replication_started_.load() ? 200 : 503);
        if (method == "GET" && route == "/metrics") {
            Json stats = this->stats();
            bool leader = is_leader();
            std::string text = "orange_errors_total " + std::to_string(metrics_errors_) + "\n";
            text += "orange_jobs_pending " + std::to_string(leader ? stats.find("jobs_pending")->i : 0) + "\n";
            text += "orange_replicate_lag " + std::to_string(leader ? stats.find("replicate_lag")->i : 0) + "\n";
            return HttpResponse::text(200, text);
        }
        if (method == "GET" && route == "/internal/stats") return HttpResponse::json(200, stats());
        if (method == "GET" && route == "/internal/log") {
            std::int64_t after = query.count("after") ? std::stoll(query.at("after")) : 0;
            return HttpResponse::json(200, Json::obj({{"entries", entries_after(after)}, {"applied_index", Json::integer(applied())}}));
        }
        if (method == "POST" && route == "/internal/replicate") {
            Json body = parse_body(raw);
            auto [ok, reason] = apply_remote(body.find("entries") && body.find("entries")->is_array() ? body.find("entries")->a : std::vector<Json>{});
            return HttpResponse::json(ok ? 200 : 409, Json::obj({{"applied_index", Json::integer(applied())}, {"error", Json::str(reason)}}));
        }
        if (method == "POST" && route == "/internal/promote") {
            Json body = parse_body(raw);
            std::string next = body.find("peer") && body.find("peer")->is_string() ? body.find("peer")->s : peer_;
            return promote(body.find("new_epoch") ? body.find("new_epoch")->int_or(0) : 0, next, request_id);
        }
        if (method == "POST" && route == "/internal/append") return append(parse_body(raw), request_id);
        if (method == "POST" && route == "/internal/snapshot") return save_snapshot(parse_body(raw), request_id);
        if (method == "GET" && route == "/internal/orders/by-key") {
            return read_by_key(query.count("caller_id") ? query.at("caller_id") : "", query.count("idempotency_key") ? query.at("idempotency_key") : "", request_id);
        }
        if (method == "GET" && route.rfind("/internal/orders/", 0) == 0) {
            return read_order(route.substr(route.rfind('/') + 1), query.count("caller_id") ? query.at("caller_id") : "", request_id);
        }
        if (method == "POST" && route == "/internal/jobs/claim") {
            Json body = parse_body(raw);
            std::string worker = body.find("worker_id") && body.find("worker_id")->is_string() ? body.find("worker_id")->s : "worker";
            return claim(worker, request_id);
        }
        if (method == "POST" && route.rfind("/internal/jobs/", 0) == 0 && route.size() > 5 && route.substr(route.size() - 4) == "/ack") {
            return finish(route.substr(std::string("/internal/jobs/").size(), route.size() - std::string("/internal/jobs/").size() - 4), request_id, true, "");
        }
        if (method == "POST" && route.rfind("/internal/jobs/", 0) == 0 && route.size() > 5 && route.substr(route.size() - 5) == "/fail") {
            Json body = parse_body(raw);
            std::string error = body.find("error") && body.find("error")->is_string() ? body.find("error")->s : "履约失败";
            return finish(route.substr(std::string("/internal/jobs/").size(), route.size() - std::string("/internal/jobs/").size() - 5), request_id, false, error);
        }
        return HttpResponse::json(404, error_body("not_found", "没有这个路径", request_id));
    }

    void heartbeat_loop() {
        while (!g_stop.load()) {
            sleep_sec(kHeartbeatSec);
            if (drop_heartbeat(node_id_)) continue;
            Json view = stats();
            try {
                HttpResult result = http_request("POST", supervisor_ + "/internal/heartbeat", &view, {}, 0.5);
                if (result.status == 200 && !result.body.empty()) {
                    Json body = Json::parse(result.body);
                    std::string leader = body.find("leader") && body.find("leader")->is_string() ? body.find("leader")->s : "";
                    std::int64_t epoch = body.find("epoch") ? body.find("epoch")->int_or(0) : 0;
                    if (!leader.empty() && epoch > this->epoch() && is_leader()) step_down(epoch, leader);
                    else if (!leader.empty() && !is_leader()) {
                        std::lock_guard<std::mutex> guard(lock_);
                        peer_ = leader;
                    }
                }
            } catch (const std::exception&) {
            }
        }
    }

    void replicate_loop() {
        replication_started_.store(true);
        while (!g_stop.load()) {
            sleep_sec(kHeartbeatSec);
            if (is_leader()) push();
            else pull();
        }
    }

    void sweep_loop() {
        while (!g_stop.load()) {
            sleep_sec(kHeartbeatSec);
            if (!is_leader()) continue;
            std::lock_guard<std::mutex> guard(lock_);
            release_expired();
            refresh_metrics();
        }
    }

private:
    sqlite3* db_ = nullptr;
    std::string node_id_;
    std::string peer_;
    std::string supervisor_;
    std::mutex lock_;
    std::int64_t follower_applied_ = 0;
    std::atomic<bool> replication_started_{false};
    std::int64_t metrics_errors_ = 0;
    std::int64_t metrics_pending_ = 0;
    std::int64_t metrics_lag_ = 0;

    static Json parse_body(const std::string& raw) {
        if (raw.empty()) return Json::obj({});
        try {
            return Json::parse(raw);
        } catch (const std::exception&) {
            return Json::obj({});
        }
    }

    void ensure_meta(const std::string& role) {
        Stmt existing(db_, "SELECT key FROM meta");
        if (existing.next()) return;
        Stmt insert(db_, "INSERT INTO meta(key, value) VALUES (?, ?)");
        for (const auto& item : {std::pair<const char*, std::string>{"role", role}, {"epoch", "1"}, {"applied_index", "0"}, {"node_id", node_id_}}) {
            sqlite3_reset(insert.raw);
            sqlite3_clear_bindings(insert.raw);
            insert.bind_text(1, item.first);
            insert.bind_text(2, item.second);
            sqlite3_step(insert.raw);
        }
    }

    std::string meta(const char* key) {
        Stmt stmt(db_, "SELECT value FROM meta WHERE key = ?");
        stmt.bind_text(1, key);
        return stmt.next() ? stmt.text("value") : "";
    }

    void set_meta(const char* key, const std::string& value) {
        Stmt stmt(db_, "INSERT INTO meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value");
        stmt.bind_text(1, key);
        stmt.bind_text(2, value);
        sqlite3_step(stmt.raw);
    }

    std::string role() { return meta("role"); }
    std::int64_t epoch() { return meta("epoch").empty() ? 1 : std::stoll(meta("epoch")); }
    std::int64_t applied() { return meta("applied_index").empty() ? 0 : std::stoll(meta("applied_index")); }
    bool is_leader() {
        std::lock_guard<std::mutex> guard(lock_);
        return role() == "leader";
    }

    void refresh_metrics() {
        Stmt stmt(db_, "SELECT COUNT(*) AS n FROM jobs WHERE state = 'pending'");
        stmt.next();
        metrics_pending_ = stmt.integer("n");
        metrics_lag_ = role() == "leader" ? std::max<std::int64_t>(0, applied() - follower_applied_) : 0;
    }

    std::int64_t append_log(const std::string& kind, const Json& payload) {
        std::int64_t index = applied() + 1;
        Stmt stmt(db_, "INSERT INTO replication_log(log_index, epoch, kind, payload, committed_at) VALUES (?, ?, ?, ?, ?)");
        stmt.bind_int(1, index);
        stmt.bind_int(2, epoch());
        stmt.bind_text(3, kind);
        stmt.bind_text(4, payload.dump());
        stmt.bind_text(5, iso_now());
        sqlite3_step(stmt.raw);
        set_meta("applied_index", std::to_string(index));
        return index;
    }

    bool order_by_id(const std::string& order_id, Stmt& stmt) {
        sqlite3_reset(stmt.raw);
        sqlite3_clear_bindings(stmt.raw);
        stmt.bind_text(1, order_id);
        return stmt.next();
    }

    Json fulfillment_of(Stmt& job) {
        return Json::obj({{"status", Json::str(job.text("state"))}, {"attempts", Json::integer(job.integer("attempts"))}, {"last_error", optional_text(job, "last_error")}});
    }

    Json snapshot_of(Stmt& order, Stmt& job, const std::string& request_id) {
        return Json::obj({
            {"order_id", Json::str(order.text("order_id"))},
            {"caller_id", Json::str(order.text("caller_id"))},
            {"external_ref", optional_text(order, "external_ref")},
            {"sku", Json::str(order.text("sku"))},
            {"qty", Json::integer(order.integer("qty"))},
            {"state", Json::str(order.text("state"))},
            {"recommendation", optional_text(order, "recommendation")},
            {"degraded", Json::boolean(order.integer("degraded") != 0)},
            {"fulfillment", fulfillment_of(job)},
            {"epoch", Json::integer(order.integer("epoch"))},
            {"request_id", Json::str(request_id)},
        });
    }

    Json public_body(const std::string& response_json, const std::string& request_id) {
        Json saved = Json::parse(response_json);
        saved.set("request_id", Json::str(request_id));
        return saved;
    }

    HttpResponse fenced(const std::string& request_id, const std::string& message, std::int64_t expected) {
        metrics_errors_ += 1;
        log_event("write_fenced", request_id, {{"epoch", Json::integer(epoch())}, {"expected", Json::integer(expected)}});
        return HttpResponse::json(409, Json::obj({{"error", Json::obj({
            {"code", Json::str("epoch_fenced")},
            {"message", Json::str(message)},
            {"request_id", Json::str(request_id)},
            {"epoch", Json::integer(epoch())},
        })}}));
    }

    HttpResponse require_leader(const std::string& request_id) {
        if (role() == "leader") return HttpResponse::empty(0);
        return fenced(request_id, "当前节点不接受写入", epoch());
    }

    void write_snapshot_row(const std::string& order_id, const Json& body) {
        Stmt stmt(db_, "UPDATE orders SET response_json = ?, state = ?, recommendation = ?, degraded = ? WHERE order_id = ?");
        stmt.bind_text(1, body.dump());
        stmt.bind_text(2, body.find("state")->s);
        bind_optional(stmt, 3, *body.find("recommendation"));
        stmt.bind_int(4, body.find("degraded")->truth() ? 1 : 0);
        stmt.bind_text(5, order_id);
        sqlite3_step(stmt.raw);
    }

    HttpResponse append(const Json& payload, const std::string& request_id) {
        fault_delay("store");
        std::int64_t expected = payload.find("epoch") ? payload.find("epoch")->int_or(0) : 0;
        std::string caller_id = payload.find("caller_id") ? payload.find("caller_id")->text_or_empty() : "";
        std::string key = payload.find("idempotency_key") ? payload.find("idempotency_key")->text_or_empty() : "";
        const Json* fields = payload.find("order");
        const Json empty = Json::obj({});
        if (fields == nullptr) fields = &empty;
        std::string digest = canonical_body(fields->find("sku") ? fields->find("sku")->text_or_empty() : "", fields->find("qty") ? fields->find("qty")->int_or(0) : 0, fields->find("external_ref") ? *fields->find("external_ref") : Json::nul(), fields->find("fulfill_url") ? *fields->find("fulfill_url") : Json::nul());
        std::lock_guard<std::mutex> guard(lock_);
        HttpResponse refused = require_leader(request_id);
        if (refused.status != 0) return refused;
        if (expected != epoch()) return fenced(request_id, "写入代际已过期", expected);
        exec_sql(db_, "BEGIN IMMEDIATE");
        try {
            Stmt existing(db_, "SELECT * FROM orders WHERE caller_id = ? AND idempotency_key = ?");
            existing.bind_text(1, caller_id);
            existing.bind_text(2, key);
            if (existing.next()) {
                exec_sql(db_, "COMMIT");
                if (existing.text("request_hash") != digest) {
                    return HttpResponse::json(409, error_body("idempotency_mismatch", "幂等键与已有订单正文不一致", request_id));
                }
                Json body = public_body(existing.text("response_json"), request_id);
                return HttpResponse::json(200, Json::obj({{"created", Json::boolean(false)}, {"order", body}, {"log_index", Json::integer(existing.integer("log_index"))}, {"epoch", Json::integer(epoch())}}));
            }
            std::string order_id = new_id("ord_");
            std::string job_id = new_id("job_");
            std::string created_at = iso_now();
            Json job = Json::obj({{"job_id", Json::str(job_id)}, {"order_id", Json::str(order_id)}, {"kind", Json::str("fulfill")}, {"state", Json::str("pending")}, {"attempts", Json::integer(0)}, {"visible_after", Json::integer(0)}, {"worker_id", Json::nul()}, {"last_error", Json::nul()}});
            Json order_state = Json::obj({
                {"order_id", Json::str(order_id)}, {"caller_id", Json::str(caller_id)}, {"external_ref", fields->find("external_ref") ? *fields->find("external_ref") : Json::nul()},
                {"sku", Json::str(fields->find("sku")->s)}, {"qty", Json::integer(fields->find("qty")->i)}, {"state", Json::str("accepted")},
                {"recommendation", Json::nul()}, {"degraded", Json::boolean(true)},
                {"fulfillment", Json::obj({{"status", Json::str("pending")}, {"attempts", Json::integer(0)}, {"last_error", Json::nul()}})},
                {"epoch", Json::integer(epoch())}, {"request_id", Json::str(request_id)},
            });
            Stmt insert_order(db_, "INSERT INTO orders(order_id, caller_id, idempotency_key, external_ref, sku, qty, fulfill_url, state, recommendation, degraded, epoch, log_index, created_at, response_json, request_hash) VALUES (?, ?, ?, ?, ?, ?, ?, 'accepted', NULL, 1, ?, 0, ?, ?, ?)");
            insert_order.bind_text(1, order_id);
            insert_order.bind_text(2, caller_id);
            insert_order.bind_text(3, key);
            bind_optional(insert_order, 4, fields->find("external_ref") ? *fields->find("external_ref") : Json::nul());
            insert_order.bind_text(5, fields->find("sku")->s);
            insert_order.bind_int(6, fields->find("qty")->i);
            bind_optional(insert_order, 7, fields->find("fulfill_url") ? *fields->find("fulfill_url") : Json::nul());
            insert_order.bind_int(8, epoch());
            insert_order.bind_text(9, created_at);
            insert_order.bind_text(10, order_state.dump());
            insert_order.bind_text(11, digest);
            sqlite3_step(insert_order.raw);
            Stmt insert_job(db_, "INSERT INTO jobs(job_id, order_id, kind, state, attempts, visible_after, worker_id, last_error, log_index) VALUES (?, ?, 'fulfill', 'pending', 0, 0, NULL, NULL, 0)");
            insert_job.bind_text(1, job_id);
            insert_job.bind_text(2, order_id);
            sqlite3_step(insert_job.raw);
            Json log_payload = Json::obj({{"order", Json::obj({
                {"order_id", Json::str(order_id)}, {"caller_id", Json::str(caller_id)}, {"idempotency_key", Json::str(key)},
                {"external_ref", fields->find("external_ref") ? *fields->find("external_ref") : Json::nul()},
                {"sku", Json::str(fields->find("sku")->s)}, {"qty", Json::integer(fields->find("qty")->i)},
                {"fulfill_url", fields->find("fulfill_url") ? *fields->find("fulfill_url") : Json::nul()},
                {"created_at", Json::str(created_at)}, {"request_hash", Json::str(digest)}, {"response", order_state},
            })}, {"job", job}});
            std::int64_t index = append_log("order_created", log_payload);
            Stmt update_order(db_, "UPDATE orders SET log_index = ?, epoch = ? WHERE order_id = ?");
            update_order.bind_int(1, index);
            update_order.bind_int(2, epoch());
            update_order.bind_text(3, order_id);
            sqlite3_step(update_order.raw);
            Stmt update_job(db_, "UPDATE jobs SET log_index = ? WHERE job_id = ?");
            update_job.bind_int(1, index);
            update_job.bind_text(2, job_id);
            sqlite3_step(update_job.raw);
            order_state.set("epoch", Json::integer(epoch()));
            Stmt update_body(db_, "UPDATE orders SET response_json = ? WHERE order_id = ?");
            update_body.bind_text(1, order_state.dump());
            update_body.bind_text(2, order_id);
            sqlite3_step(update_body.raw);
            exec_sql(db_, "COMMIT");
            refresh_metrics();
            log_event("order_accepted", request_id, {{"order_id", Json::str(order_id)}, {"epoch", Json::integer(epoch())}, {"degraded", Json::boolean(true)}});
            return HttpResponse::json(200, Json::obj({{"created", Json::boolean(true)}, {"order", order_state}, {"log_index", Json::integer(index)}, {"epoch", Json::integer(epoch())}}));
        } catch (...) {
            exec_sql(db_, "ROLLBACK");
            throw;
        }
    }

    HttpResponse read_order(const std::string& order_id, const std::string& caller_id, const std::string& request_id) {
        std::lock_guard<std::mutex> guard(lock_);
        HttpResponse refused = require_leader(request_id);
        if (refused.status != 0) return refused;
        Stmt order(db_, "SELECT * FROM orders WHERE order_id = ?");
        order.bind_text(1, order_id);
        if (!order.next() || order.text("caller_id") != caller_id) return HttpResponse::json(404, error_body("not_found", "没有这笔订单", request_id));
        return HttpResponse::json(200, public_body(order.text("response_json"), request_id));
    }

    HttpResponse read_by_key(const std::string& caller_id, const std::string& key, const std::string& request_id) {
        std::lock_guard<std::mutex> guard(lock_);
        HttpResponse refused = require_leader(request_id);
        if (refused.status != 0) return refused;
        Stmt order(db_, "SELECT * FROM orders WHERE caller_id = ? AND idempotency_key = ?");
        order.bind_text(1, caller_id);
        order.bind_text(2, key);
        if (!order.next()) return HttpResponse::json(404, error_body("not_found", "没有这笔订单", request_id));
        Json body = public_body(order.text("response_json"), request_id);
        body.set("request_hash", Json::str(order.text("request_hash")));
        return HttpResponse::json(200, body);
    }

    HttpResponse save_snapshot(const Json& payload, const std::string& request_id) {
        std::string order_id = payload.find("order_id") ? payload.find("order_id")->text_or_empty() : "";
        std::lock_guard<std::mutex> guard(lock_);
        HttpResponse refused = require_leader(request_id);
        if (refused.status != 0) return refused;
        if ((payload.find("epoch") ? payload.find("epoch")->int_or(0) : 0) != epoch()) return fenced(request_id, "写入代际已过期", payload.find("epoch") ? payload.find("epoch")->int_or(0) : 0);
        Stmt order(db_, "SELECT * FROM orders WHERE order_id = ?");
        order.bind_text(1, order_id);
        if (!order.next()) return HttpResponse::json(404, error_body("not_found", "没有这笔订单", request_id));
        Stmt job(db_, "SELECT * FROM jobs WHERE order_id = ?");
        job.bind_text(1, order_id);
        if (!job.next()) return HttpResponse::json(404, error_body("not_found", "没有这笔订单", request_id));
        Json body = Json::parse(order.text("response_json"));
        body.set("recommendation", payload.find("recommendation") ? *payload.find("recommendation") : Json::nul());
        body.set("degraded", Json::boolean(payload.find("degraded") && payload.find("degraded")->truth()));
        body.set("request_id", Json::str(request_id));
        body.set("epoch", Json::integer(epoch()));
        exec_sql(db_, "BEGIN IMMEDIATE");
        try {
            write_snapshot_row(order_id, body);
            append_log("order_snapshot", Json::obj({{"order_id", Json::str(order_id)}, {"response", body}}));
            exec_sql(db_, "COMMIT");
        } catch (...) {
            exec_sql(db_, "ROLLBACK");
            throw;
        }
        return HttpResponse::json(200, body);
    }

    HttpResponse claim(const std::string& worker_id, const std::string& request_id) {
        std::lock_guard<std::mutex> guard(lock_);
        HttpResponse refused = require_leader(request_id);
        if (refused.status != 0) return refused;
        release_expired();
        Stmt row(db_, "SELECT * FROM jobs WHERE state = 'pending' AND visible_after <= ? ORDER BY job_id LIMIT 1");
        row.bind_int(1, now_ms());
        if (!row.next()) return HttpResponse::empty(204);
        std::string job_id = row.text("job_id");
        std::string order_id = row.text("order_id");
        std::int64_t attempts = row.integer("attempts") + 1;
        exec_sql(db_, "BEGIN IMMEDIATE");
        try {
            Stmt update(db_, "UPDATE jobs SET state = 'inflight', attempts = ?, visible_after = ?, worker_id = ? WHERE job_id = ?");
            update.bind_int(1, attempts);
            update.bind_int(2, now_ms() + kVisibilityMs);
            update.bind_text(3, worker_id);
            update.bind_text(4, job_id);
            sqlite3_step(update.raw);
            Stmt order(db_, "SELECT * FROM orders WHERE order_id = ?");
            order.bind_text(1, order_id);
            order.next();
            Stmt job(db_, "SELECT * FROM jobs WHERE order_id = ?");
            job.bind_text(1, order_id);
            job.next();
            Json body = snapshot_of(order, job, request_id);
            write_snapshot_row(order_id, body);
            append_log("job_claimed", Json::obj({{"job_id", Json::str(job_id)}, {"order_id", Json::str(order_id)}, {"response", body}, {"worker_id", Json::str(worker_id)}}));
            exec_sql(db_, "COMMIT");
            refresh_metrics();
            Stmt claimed(db_, "SELECT * FROM jobs WHERE order_id = ?");
            claimed.bind_text(1, order_id);
            claimed.next();
            Stmt fresh(db_, "SELECT * FROM orders WHERE order_id = ?");
            fresh.bind_text(1, order_id);
            fresh.next();
            log_event("job_claimed", request_id, {{"order_id", Json::str(order_id)}, {"worker_id", Json::str(worker_id)}});
            return HttpResponse::json(200, Json::obj({
                {"job_id", Json::str(claimed.text("job_id"))}, {"order_id", Json::str(fresh.text("order_id"))},
                {"caller_id", Json::str(fresh.text("caller_id"))}, {"external_ref", optional_text(fresh, "external_ref")},
                {"sku", Json::str(fresh.text("sku"))}, {"qty", Json::integer(fresh.integer("qty"))},
                {"fulfill_url", optional_text(fresh, "fulfill_url")}, {"attempts", Json::integer(claimed.integer("attempts"))},
            }));
        } catch (...) {
            exec_sql(db_, "ROLLBACK");
            throw;
        }
    }

    HttpResponse finish(const std::string& job_id, const std::string& request_id, bool success, const std::string& error) {
        std::lock_guard<std::mutex> guard(lock_);
        HttpResponse refused = require_leader(request_id);
        if (refused.status != 0) return refused;
        Stmt job(db_, "SELECT * FROM jobs WHERE job_id = ?");
        job.bind_text(1, job_id);
        if (!job.next()) return HttpResponse::json(404, error_body("not_found", "没有这个任务", request_id));
        std::string order_id = job.text("order_id");
        std::int64_t attempts = job.integer("attempts");
        exec_sql(db_, "BEGIN IMMEDIATE");
        std::string event;
        try {
            if (success) {
                Stmt update(db_, "UPDATE jobs SET state = 'done', last_error = NULL, worker_id = NULL WHERE job_id = ?");
                update.bind_text(1, job_id);
                sqlite3_step(update.raw);
                event = "job_done";
                append_after_finish(job_id, order_id, "job_done");
            } else if (attempts >= kJobMaxAttempts) {
                Stmt update(db_, "UPDATE jobs SET state = 'dead', last_error = ?, worker_id = NULL WHERE job_id = ?");
                update.bind_text(1, error);
                update.bind_text(2, job_id);
                sqlite3_step(update.raw);
                event = "job_dead";
                append_after_finish(job_id, order_id, "job_dead");
            } else {
                int wait = backoff_ms(static_cast<int>(attempts));
                Stmt update(db_, "UPDATE jobs SET state = 'pending', last_error = ?, worker_id = NULL, visible_after = ? WHERE job_id = ?");
                update.bind_text(1, error);
                update.bind_int(2, now_ms() + wait);
                update.bind_text(3, job_id);
                sqlite3_step(update.raw);
                event = "job_retry";
                append_after_finish(job_id, order_id, "job_reset");
            }
            exec_sql(db_, "COMMIT");
        } catch (...) {
            exec_sql(db_, "ROLLBACK");
            throw;
        }
        refresh_metrics();
        Stmt order(db_, "SELECT response_json FROM orders WHERE order_id = ?");
        order.bind_text(1, order_id);
        order.next();
        Json body = Json::parse(order.text("response_json"));
        log_event(event, request_id, {{"order_id", Json::str(order_id)}, {"job_id", Json::str(job_id)}});
        return HttpResponse::json(200, body);
    }

    void append_after_finish(const std::string& job_id, const std::string& order_id, const std::string& kind) {
        Stmt job(db_, "SELECT * FROM jobs WHERE job_id = ?");
        job.bind_text(1, job_id);
        job.next();
        Stmt order(db_, "SELECT * FROM orders WHERE order_id = ?");
        order.bind_text(1, order_id);
        order.next();
        std::string state = job.text("state") == "done" ? "fulfilled" : job.text("state") == "dead" ? "fulfillment_failed" : "accepted";
        Json body = Json::parse(order.text("response_json"));
        body.set("state", Json::str(state));
        body.set("fulfillment", fulfillment_of(job));
        body.set("epoch", Json::integer(epoch()));
        Stmt update_state(db_, "UPDATE orders SET state = ? WHERE order_id = ?");
        update_state.bind_text(1, state);
        update_state.bind_text(2, order_id);
        sqlite3_step(update_state.raw);
        write_snapshot_row(order_id, body);
        append_log(kind, Json::obj({{"job_id", Json::str(job_id)}, {"order_id", Json::str(order_id)}, {"response", body}}));
    }

    void release_expired() {
        Stmt rows(db_, "SELECT * FROM jobs WHERE state = 'inflight' AND visible_after <= ?");
        rows.bind_int(1, now_ms());
        std::vector<std::pair<std::string, std::string>> expired;
        while (rows.next()) expired.emplace_back(rows.text("job_id"), rows.text("order_id"));
        for (const auto& item : expired) {
            exec_sql(db_, "BEGIN IMMEDIATE");
            try {
                Stmt update(db_, "UPDATE jobs SET state = 'pending', worker_id = NULL WHERE job_id = ? AND state = 'inflight'");
                update.bind_text(1, item.first);
                sqlite3_step(update.raw);
                Stmt order(db_, "SELECT * FROM orders WHERE order_id = ?");
                order.bind_text(1, item.second);
                Stmt job(db_, "SELECT * FROM jobs WHERE order_id = ?");
                job.bind_text(1, item.second);
                if (order.next() && job.next()) {
                    Json body = Json::parse(order.text("response_json"));
                    body.set("state", Json::str("accepted"));
                    body.set("fulfillment", fulfillment_of(job));
                    write_snapshot_row(item.second, body);
                    append_log("job_reset", Json::obj({{"job_id", Json::str(item.first)}, {"order_id", Json::str(item.second)}, {"response", body}}));
                }
                exec_sql(db_, "COMMIT");
            } catch (...) {
                exec_sql(db_, "ROLLBACK");
                throw;
            }
            log_event("job_retry", "", {{"order_id", Json::str(item.second)}, {"job_id", Json::str(item.first)}, {"reason", Json::str("visibility_timeout")}});
        }
    }

    HttpResponse promote(std::int64_t new_epoch, const std::string& peer, const std::string& request_id) {
        std::lock_guard<std::mutex> guard(lock_);
        if (new_epoch <= epoch() && role() == "leader") {
            return HttpResponse::json(200, Json::obj({{"epoch", Json::integer(epoch())}, {"role", Json::str(role())}}));
        }
        exec_sql(db_, "BEGIN IMMEDIATE");
        try {
            set_meta("role", "leader");
            set_meta("epoch", std::to_string(new_epoch));
            append_log("epoch_promoted", Json::obj({{"epoch", Json::integer(new_epoch)}, {"node_id", Json::str(node_id_)}}));
            exec_sql(db_, "COMMIT");
        } catch (...) {
            exec_sql(db_, "ROLLBACK");
            throw;
        }
        peer_ = peer;
        follower_applied_ = 0;
        refresh_metrics();
        log_event("promoted", request_id, {{"epoch", Json::integer(new_epoch)}, {"node_id", Json::str(node_id_)}});
        return HttpResponse::json(200, Json::obj({{"epoch", Json::integer(new_epoch)}, {"role", Json::str("leader")}}));
    }

    void step_down(std::int64_t new_epoch, const std::string& leader) {
        std::lock_guard<std::mutex> guard(lock_);
        if (role() == "leader" && new_epoch <= epoch()) return;
        if (role() != "leader" && new_epoch == epoch() && peer_ == leader) return;
        exec_sql(db_, "BEGIN IMMEDIATE");
        try {
            exec_sql(db_, "DELETE FROM orders");
            exec_sql(db_, "DELETE FROM jobs");
            exec_sql(db_, "DELETE FROM replication_log");
            set_meta("role", "follower");
            set_meta("epoch", std::to_string(new_epoch));
            set_meta("applied_index", "0");
            exec_sql(db_, "COMMIT");
        } catch (...) {
            exec_sql(db_, "ROLLBACK");
            throw;
        }
        peer_ = leader;
        log_event("stepped_down", "", {{"epoch", Json::integer(new_epoch)}, {"leader", Json::str(leader)}});
    }

    Json entries_after(std::int64_t index) {
        std::lock_guard<std::mutex> guard(lock_);
        Stmt rows(db_, "SELECT * FROM replication_log WHERE log_index > ? ORDER BY log_index");
        rows.bind_int(1, index);
        Json entries = Json::arr({});
        while (rows.next()) {
            entries.a.push_back(Json::obj({
                {"log_index", Json::integer(rows.integer("log_index"))},
                {"epoch", Json::integer(rows.integer("epoch"))},
                {"kind", Json::str(rows.text("kind"))},
                {"payload", Json::parse(rows.text("payload"))},
                {"committed_at", Json::str(rows.text("committed_at"))},
            }));
        }
        return entries;
    }

    std::pair<bool, std::string> apply_remote(const std::vector<Json>& entries) {
        std::lock_guard<std::mutex> guard(lock_);
        exec_sql(db_, "BEGIN IMMEDIATE");
        try {
            for (const Json& entry : entries) {
                std::int64_t expected = applied() + 1;
                if (entry.find("log_index")->int_or(0) != expected) {
                    exec_sql(db_, "ROLLBACK");
                    return {false, "gap"};
                }
                apply_payload(entry.find("kind")->s, *entry.find("payload"), entry.find("epoch")->int_or(0));
                Stmt insert(db_, "INSERT INTO replication_log(log_index, epoch, kind, payload, committed_at) VALUES (?, ?, ?, ?, ?)");
                insert.bind_int(1, entry.find("log_index")->i);
                insert.bind_int(2, entry.find("epoch")->i);
                insert.bind_text(3, entry.find("kind")->s);
                insert.bind_text(4, entry.find("payload")->dump());
                insert.bind_text(5, entry.find("committed_at") && entry.find("committed_at")->is_string() ? entry.find("committed_at")->s : iso_now());
                sqlite3_step(insert.raw);
                set_meta("applied_index", std::to_string(entry.find("log_index")->i));
                if (entry.find("kind")->s == "epoch_promoted") {
                    set_meta("epoch", std::to_string(entry.find("payload")->find("epoch")->i));
                    set_meta("role", "follower");
                }
            }
            exec_sql(db_, "COMMIT");
        } catch (...) {
            exec_sql(db_, "ROLLBACK");
            throw;
        }
        refresh_metrics();
        return {true, ""};
    }

    void apply_payload(const std::string& kind, const Json& payload, std::int64_t epoch_value) {
        if (kind == "order_created") {
            const Json& order = *payload.find("order");
            const Json& job = *payload.find("job");
            const Json& response = *order.find("response");
            Stmt insert(db_, "INSERT OR REPLACE INTO orders(order_id, caller_id, idempotency_key, external_ref, sku, qty, fulfill_url, state, recommendation, degraded, epoch, log_index, created_at, response_json, request_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)");
            insert.bind_text(1, order.find("order_id")->s);
            insert.bind_text(2, order.find("caller_id")->s);
            insert.bind_text(3, order.find("idempotency_key")->s);
            bind_optional(insert, 4, order.find("external_ref") ? *order.find("external_ref") : Json::nul());
            insert.bind_text(5, order.find("sku")->s);
            insert.bind_int(6, order.find("qty")->i);
            bind_optional(insert, 7, order.find("fulfill_url") ? *order.find("fulfill_url") : Json::nul());
            insert.bind_text(8, response.find("state")->s);
            bind_optional(insert, 9, response.find("recommendation") ? *response.find("recommendation") : Json::nul());
            insert.bind_int(10, response.find("degraded") && response.find("degraded")->truth() ? 1 : 0);
            insert.bind_int(11, epoch_value);
            insert.bind_text(12, order.find("created_at")->s);
            insert.bind_text(13, response.dump());
            insert.bind_text(14, order.find("request_hash")->s);
            sqlite3_step(insert.raw);
            Stmt insert_job(db_, "INSERT OR REPLACE INTO jobs(job_id, order_id, kind, state, attempts, visible_after, worker_id, last_error, log_index) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)");
            insert_job.bind_text(1, job.find("job_id")->s);
            insert_job.bind_text(2, job.find("order_id")->s);
            insert_job.bind_text(3, job.find("kind")->s);
            insert_job.bind_text(4, job.find("state")->s);
            insert_job.bind_int(5, job.find("attempts")->i);
            insert_job.bind_int(6, job.find("visible_after")->i);
            bind_optional(insert_job, 7, job.find("worker_id") ? *job.find("worker_id") : Json::nul());
            bind_optional(insert_job, 8, job.find("last_error") ? *job.find("last_error") : Json::nul());
            sqlite3_step(insert_job.raw);
            return;
        }
        if (payload.find("response") && payload.find("order_id")) {
            const Json& body = *payload.find("response");
            write_snapshot_row(payload.find("order_id")->s, body);
            if (kind != "order_snapshot") {
                const Json* fulfillment = body.find("fulfillment");
                Stmt update(db_, "UPDATE jobs SET state = ?, attempts = ?, last_error = ?, worker_id = ? WHERE order_id = ?");
                update.bind_text(1, fulfillment->find("status")->s);
                update.bind_int(2, fulfillment->find("attempts")->i);
                bind_optional(update, 3, fulfillment->find("last_error") ? *fulfillment->find("last_error") : Json::nul());
                bind_optional(update, 4, payload.find("worker_id") ? *payload.find("worker_id") : Json::nul());
                update.bind_text(5, payload.find("order_id")->s);
                sqlite3_step(update.raw);
            }
        }
    }

    Json stats() {
        std::lock_guard<std::mutex> guard(lock_);
        refresh_metrics();
        return Json::obj({
            {"node_id", Json::str(node_id_)}, {"role", Json::str(role())}, {"epoch", Json::integer(epoch())},
            {"applied_index", Json::integer(applied())}, {"jobs_pending", Json::integer(metrics_pending_)},
            {"replicate_lag", Json::integer(metrics_lag_)},
        });
    }

    void push() {
        if (pause_replication() || peer_.empty()) return;
        std::int64_t from = 0;
        {
            std::lock_guard<std::mutex> guard(lock_);
            from = follower_applied_;
        }
        Json entries = entries_after(from);
        if (entries.a.empty()) {
            std::lock_guard<std::mutex> guard(lock_);
            refresh_metrics();
            return;
        }
        try {
            Json payload = Json::obj({{"entries", entries}});
            HttpResult result = http_request("POST", peer_ + "/internal/replicate", &payload, {}, 0.5);
            if (result.status == 200 && !result.body.empty()) {
                Json body = Json::parse(result.body);
                std::lock_guard<std::mutex> guard(lock_);
                follower_applied_ = body.find("applied_index") ? body.find("applied_index")->int_or(follower_applied_) : follower_applied_;
            } else if (result.status == 409 && !result.body.empty()) {
                Json body = Json::parse(result.body);
                std::lock_guard<std::mutex> guard(lock_);
                follower_applied_ = body.find("applied_index") ? body.find("applied_index")->int_or(0) : 0;
            }
        } catch (const std::exception& ex) {
            std::int64_t lag = 0;
            {
                std::lock_guard<std::mutex> guard(lock_);
                lag = std::max<std::int64_t>(0, applied() - follower_applied_);
            }
            log_event("replicate_lag", "", {{"message", Json::str(ex.what())}, {"lag", Json::integer(lag)}}, "error");
        }
        std::int64_t lag = 0;
        {
            std::lock_guard<std::mutex> guard(lock_);
            refresh_metrics();
            lag = metrics_lag_;
        }
        if (lag) log_event("replicate_lag", "", {{"lag", Json::integer(lag)}});
    }

    void pull() {
        if (peer_.empty()) return;
        try {
            std::int64_t index = 0;
            {
                std::lock_guard<std::mutex> guard(lock_);
                index = applied();
            }
            HttpResult result = http_request("GET", peer_ + "/internal/log?after=" + std::to_string(index), nullptr, {}, 0.5);
            if (result.status != 200 || result.body.empty()) return;
            Json body = Json::parse(result.body);
            if (body.find("entries") && body.find("entries")->is_array() && !body.find("entries")->a.empty()) {
                apply_remote(body.find("entries")->a);
            }
        } catch (const std::exception&) {
        }
    }
};

}  // namespace

int run_store(int argc, char** argv) {
    std::string node = flag(argc, argv, "--node-id");
    set_context(node, flag(argc, argv, "--root", "."));
    start_fault_reader();
    auto store = std::make_shared<Store>(node, flag(argc, argv, "--role"), flag(argc, argv, "--peer"), flag(argc, argv, "--data"), flag(argc, argv, "--supervisor"));
    std::thread([store] { store->heartbeat_loop(); }).detach();
    std::thread([store] { store->replicate_loop(); }).detach();
    std::thread([store] { store->sweep_loop(); }).detach();
    int port = flag_int(argc, argv, "--port", 0);
    log_event("process_started", "", {{"role", Json::str(flag(argc, argv, "--role"))}, {"port", Json::integer(port)}});
    serve(port, [store](const std::string& method, const std::string& path, const std::map<std::string, std::string>& headers, const std::string& body) {
        return store->handle(method, path, headers, body);
    });
    return 0;
}
