#include "json.hpp"

#include <cctype>
#include <cmath>
#include <sstream>
#include <stdexcept>

Json Json::nul() { return Json(); }

Json Json::boolean(bool value) {
    Json out;
    out.kind = Kind::Bool;
    out.b = value;
    return out;
}

Json Json::integer(std::int64_t value) {
    Json out;
    out.kind = Kind::Int;
    out.i = value;
    return out;
}

Json Json::real(double value) {
    Json out;
    out.kind = Kind::Real;
    out.d = value;
    return out;
}

Json Json::str(std::string value) {
    Json out;
    out.kind = Kind::String;
    out.s = std::move(value);
    return out;
}

Json Json::arr(std::vector<Json> value) {
    Json out;
    out.kind = Kind::Array;
    out.a = std::move(value);
    return out;
}

Json Json::obj(std::vector<std::pair<std::string, Json>> value) {
    Json out;
    out.kind = Kind::Object;
    out.o = std::move(value);
    return out;
}

Json Jobj(std::vector<std::pair<std::string, Json>> fields) { return Json::obj(std::move(fields)); }

bool Json::is_null() const { return kind == Kind::Null; }
bool Json::is_bool() const { return kind == Kind::Bool; }
bool Json::is_int() const { return kind == Kind::Int; }
bool Json::is_string() const { return kind == Kind::String; }
bool Json::is_object() const { return kind == Kind::Object; }
bool Json::is_array() const { return kind == Kind::Array; }

const Json* Json::find(const std::string& key) const {
    if (kind != Kind::Object) {
        return nullptr;
    }
    for (const auto& item : o) {
        if (item.first == key) {
            return &item.second;
        }
    }
    return nullptr;
}

Json* Json::find(const std::string& key) {
    if (kind != Kind::Object) {
        return nullptr;
    }
    for (auto& item : o) {
        if (item.first == key) {
            return &item.second;
        }
    }
    return nullptr;
}

void Json::set(const std::string& key, Json value) {
    if (kind != Kind::Object) {
        kind = Kind::Object;
        o.clear();
    }
    for (auto& item : o) {
        if (item.first == key) {
            item.second = std::move(value);
            return;
        }
    }
    o.emplace_back(key, std::move(value));
}

std::string Json::text_or_empty() const { return kind == Kind::String ? s : ""; }

std::int64_t Json::int_or(std::int64_t fallback) const { return kind == Kind::Int ? i : fallback; }

bool Json::truth() const { return kind == Kind::Bool && b; }

namespace {

void quote(std::ostream& out, const std::string& text) {
    out << '"';
    for (unsigned char ch : text) {
        switch (ch) {
            case '"':
                out << "\\\"";
                break;
            case '\\':
                out << "\\\\";
                break;
            case '\b':
                out << "\\b";
                break;
            case '\f':
                out << "\\f";
                break;
            case '\n':
                out << "\\n";
                break;
            case '\r':
                out << "\\r";
                break;
            case '\t':
                out << "\\t";
                break;
            default:
                if (ch < 0x20) {
                    const char* hex = "0123456789abcdef";
                    out << "\\u00" << hex[ch >> 4] << hex[ch & 0xf];
                } else {
                    out << static_cast<char>(ch);
                }
        }
    }
    out << '"';
}

void dump_into(std::ostream& out, const Json& value) {
    switch (value.kind) {
        case Json::Kind::Null:
            out << "null";
            break;
        case Json::Kind::Bool:
            out << (value.b ? "true" : "false");
            break;
        case Json::Kind::Int:
            out << value.i;
            break;
        case Json::Kind::Real:
            out << value.d;
            break;
        case Json::Kind::String:
            quote(out, value.s);
            break;
        case Json::Kind::Array: {
            out << '[';
            for (size_t n = 0; n < value.a.size(); ++n) {
                if (n) {
                    out << ',';
                }
                dump_into(out, value.a[n]);
            }
            out << ']';
            break;
        }
        case Json::Kind::Object: {
            out << '{';
            for (size_t n = 0; n < value.o.size(); ++n) {
                if (n) {
                    out << ',';
                }
                quote(out, value.o[n].first);
                out << ':';
                dump_into(out, value.o[n].second);
            }
            out << '}';
            break;
        }
    }
}

struct Parser {
    const std::string& text;
    size_t at = 0;

    explicit Parser(const std::string& input) : text(input) {}

    [[noreturn]] void fail() const { throw std::runtime_error("json"); }

    void skip() {
        while (at < text.size() && std::isspace(static_cast<unsigned char>(text[at]))) {
            ++at;
        }
    }

    char peek() {
        skip();
        if (at >= text.size()) {
            fail();
        }
        return text[at];
    }

    char get() {
        char ch = peek();
        ++at;
        return ch;
    }

    bool eat(char expected) {
        if (peek() != expected) {
            return false;
        }
        ++at;
        return true;
    }

    Json parse_value() {
        char ch = peek();
        if (ch == 'n') {
            literal("null");
            return Json::nul();
        }
        if (ch == 't') {
            literal("true");
            return Json::boolean(true);
        }
        if (ch == 'f') {
            literal("false");
            return Json::boolean(false);
        }
        if (ch == '"') {
            return Json::str(parse_string());
        }
        if (ch == '[') {
            return parse_array();
        }
        if (ch == '{') {
            return parse_object();
        }
        return parse_number();
    }

    void literal(const char* word) {
        for (const char* p = word; *p; ++p) {
            if (get() != *p) {
                fail();
            }
        }
    }

    std::string parse_string() {
        if (get() != '"') {
            fail();
        }
        std::string out;
        while (at < text.size()) {
            char ch = text[at++];
            if (ch == '"') {
                return out;
            }
            if (ch != '\\') {
                out.push_back(ch);
                continue;
            }
            if (at >= text.size()) {
                fail();
            }
            char esc = text[at++];
            switch (esc) {
                case '"':
                case '\\':
                case '/':
                    out.push_back(esc);
                    break;
                case 'b':
                    out.push_back('\b');
                    break;
                case 'f':
                    out.push_back('\f');
                    break;
                case 'n':
                    out.push_back('\n');
                    break;
                case 'r':
                    out.push_back('\r');
                    break;
                case 't':
                    out.push_back('\t');
                    break;
                case 'u': {
                    int code = 0;
                    for (int n = 0; n < 4; ++n) {
                        if (at >= text.size()) {
                            fail();
                        }
                        char hex = text[at++];
                        code <<= 4;
                        if (hex >= '0' && hex <= '9') {
                            code += hex - '0';
                        } else if (hex >= 'a' && hex <= 'f') {
                            code += hex - 'a' + 10;
                        } else if (hex >= 'A' && hex <= 'F') {
                            code += hex - 'A' + 10;
                        } else {
                            fail();
                        }
                    }
                    if (code < 0x80) {
                        out.push_back(static_cast<char>(code));
                    } else if (code < 0x800) {
                        out.push_back(static_cast<char>(0xc0 | (code >> 6)));
                        out.push_back(static_cast<char>(0x80 | (code & 0x3f)));
                    } else {
                        out.push_back(static_cast<char>(0xe0 | (code >> 12)));
                        out.push_back(static_cast<char>(0x80 | ((code >> 6) & 0x3f)));
                        out.push_back(static_cast<char>(0x80 | (code & 0x3f)));
                    }
                    break;
                }
                default:
                    fail();
            }
        }
        fail();
    }

    Json parse_number() {
        size_t start = at;
        if (peek() == '-') {
            ++at;
        }
        if (at >= text.size() || !std::isdigit(static_cast<unsigned char>(text[at]))) {
            fail();
        }
        if (text[at] == '0') {
            ++at;
        } else {
            while (at < text.size() && std::isdigit(static_cast<unsigned char>(text[at]))) {
                ++at;
            }
        }
        bool real = false;
        if (at < text.size() && text[at] == '.') {
            real = true;
            ++at;
            if (at >= text.size() || !std::isdigit(static_cast<unsigned char>(text[at]))) {
                fail();
            }
            while (at < text.size() && std::isdigit(static_cast<unsigned char>(text[at]))) {
                ++at;
            }
        }
        if (at < text.size() && (text[at] == 'e' || text[at] == 'E')) {
            real = true;
            ++at;
            if (at < text.size() && (text[at] == '+' || text[at] == '-')) {
                ++at;
            }
            if (at >= text.size() || !std::isdigit(static_cast<unsigned char>(text[at]))) {
                fail();
            }
            while (at < text.size() && std::isdigit(static_cast<unsigned char>(text[at]))) {
                ++at;
            }
        }
        std::string token = text.substr(start, at - start);
        if (!real) {
            return Json::integer(std::stoll(token));
        }
        return Json::real(std::stod(token));
    }

    Json parse_array() {
        if (get() != '[') {
            fail();
        }
        std::vector<Json> items;
        skip();
        if (eat(']')) {
            return Json::arr(std::move(items));
        }
        while (true) {
            items.push_back(parse_value());
            if (eat(']')) {
                return Json::arr(std::move(items));
            }
            if (!eat(',')) {
                fail();
            }
        }
    }

    Json parse_object() {
        if (get() != '{') {
            fail();
        }
        Json out = Json::obj({});
        skip();
        if (eat('}')) {
            return out;
        }
        while (true) {
            if (peek() != '"') {
                fail();
            }
            std::string key = parse_string();
            if (!eat(':')) {
                fail();
            }
            out.set(key, parse_value());
            if (eat('}')) {
                return out;
            }
            if (!eat(',')) {
                fail();
            }
        }
    }
};

}  // namespace

std::string Json::dump() const {
    std::ostringstream out;
    dump_into(out, *this);
    return out.str();
}

Json Json::parse(const std::string& text) {
    Parser parser(text);
    Json value = parser.parse_value();
    parser.skip();
    if (parser.at != text.size()) {
        parser.fail();
    }
    return value;
}
