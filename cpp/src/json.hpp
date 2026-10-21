#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

// 紧凑 JSON。对象保留插入顺序，字符串按原样写出非 ASCII，方便和 Python 的日志对齐。
struct Json {
    enum class Kind { Null, Bool, Int, Real, String, Array, Object };

    Kind kind = Kind::Null;
    bool b = false;
    std::int64_t i = 0;
    double d = 0;
    std::string s;
    std::vector<Json> a;
    std::vector<std::pair<std::string, Json>> o;

    static Json nul();
    static Json boolean(bool value);
    static Json integer(std::int64_t value);
    static Json real(double value);
    static Json str(std::string value);
    static Json arr(std::vector<Json> value);
    static Json obj(std::vector<std::pair<std::string, Json>> value);

    bool is_null() const;
    bool is_bool() const;
    bool is_int() const;
    bool is_string() const;
    bool is_object() const;
    bool is_array() const;
    const Json* find(const std::string& key) const;
    Json* find(const std::string& key);
    void set(const std::string& key, Json value);
    std::string text_or_empty() const;
    std::int64_t int_or(std::int64_t fallback) const;
    bool truth() const;
    std::string dump() const;
    static Json parse(const std::string& text);
};

Json Jobj(std::vector<std::pair<std::string, Json>> fields);
