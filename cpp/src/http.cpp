#include "http.hpp"

#include "common.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cctype>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <thread>

namespace {
std::atomic<int> g_listen{-1};

std::string reason(int status) {
    switch (status) {
        case 200: return "OK";
        case 201: return "Created";
        case 204: return "No Content";
        case 400: return "Bad Request";
        case 404: return "Not Found";
        case 409: return "Conflict";
        case 429: return "Too Many Requests";
        case 500: return "Internal Server Error";
        case 503: return "Service Unavailable";
        case 504: return "Gateway Timeout";
        default: return "OK";
    }
}

std::string lower_copy(std::string value) {
    for (char& ch : value) {
        if (ch >= 'A' && ch <= 'Z') {
            ch = static_cast<char>(ch - 'A' + 'a');
        }
    }
    return value;
}

int hex_nibble(char ch) {
    if (ch >= '0' && ch <= '9') return ch - '0';
    if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
    if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
    return -1;
}

bool wait_fd(int fd, short events, double timeout_sec) {
    pollfd poller{};
    poller.fd = fd;
    poller.events = events;
    int ms = static_cast<int>(timeout_sec * 1000);
    if (ms < 1) {
        ms = 1;
    }
    int ready = poll(&poller, 1, ms);
    return ready > 0 && (poller.revents & events);
}

std::string recv_some(int fd, double timeout_sec) {
    if (!wait_fd(fd, POLLIN, timeout_sec)) {
        throw std::runtime_error("timed out");
    }
    char buffer[8192];
    ssize_t n = ::recv(fd, buffer, sizeof(buffer), 0);
    if (n < 0) {
        throw std::runtime_error("recv");
    }
    return std::string(buffer, buffer + n);
}

void send_all(int fd, const std::string& data, double timeout_sec) {
    size_t sent = 0;
    while (sent < data.size()) {
        if (!wait_fd(fd, POLLOUT, timeout_sec)) {
            throw std::runtime_error("timed out");
        }
        ssize_t n = ::send(fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (n <= 0) {
            throw std::runtime_error("send");
        }
        sent += static_cast<size_t>(n);
    }
}

struct ParsedUrl {
    std::string host;
    int port = 80;
    std::string path;
};

ParsedUrl parse_url(const std::string& url) {
    const std::string prefix = "http://";
    if (url.rfind(prefix, 0) != 0) {
        throw std::runtime_error("url");
    }
    std::string rest = url.substr(prefix.size());
    auto slash = rest.find('/');
    std::string authority = slash == std::string::npos ? rest : rest.substr(0, slash);
    std::string path = slash == std::string::npos ? "/" : rest.substr(slash);
    auto colon = authority.rfind(':');
    ParsedUrl out;
    out.host = colon == std::string::npos ? authority : authority.substr(0, colon);
    out.port = colon == std::string::npos ? 80 : std::stoi(authority.substr(colon + 1));
    out.path = path.empty() ? "/" : path;
    return out;
}

void handle_client(int fd, Handler handler) {
    std::string data;
    try {
        while (data.find("\r\n\r\n") == std::string::npos) {
            std::string chunk = recv_some(fd, 5);
            if (chunk.empty()) {
                break;
            }
            data += chunk;
            if (data.size() > 1024 * 1024) {
                break;
            }
        }
        auto head_end = data.find("\r\n\r\n");
        if (head_end == std::string::npos) {
            ::close(fd);
            return;
        }
        std::string head = data.substr(0, head_end);
        std::string body = data.substr(head_end + 4);
        std::istringstream lines(head);
        std::string request_line;
        std::getline(lines, request_line);
        if (!request_line.empty() && request_line.back() == '\r') {
            request_line.pop_back();
        }
        std::istringstream request(request_line);
        std::string method;
        std::string target;
        request >> method >> target;
        std::map<std::string, std::string> headers;
        std::string header_line;
        while (std::getline(lines, header_line)) {
            if (!header_line.empty() && header_line.back() == '\r') {
                header_line.pop_back();
            }
            auto colon = header_line.find(':');
            if (colon == std::string::npos) {
                continue;
            }
            std::string key = lower_copy(header_line.substr(0, colon));
            std::string value = header_line.substr(colon + 1);
            if (!value.empty() && value[0] == ' ') {
                value.erase(value.begin());
            }
            headers[key] = value;
        }
        int length = 0;
        if (headers.count("content-length")) {
            length = std::stoi(headers["content-length"]);
        }
        while (static_cast<int>(body.size()) < length) {
            std::string chunk = recv_some(fd, 5);
            if (chunk.empty()) {
                break;
            }
            body += chunk;
        }
        body.resize(static_cast<size_t>(std::max(0, length)));
        HttpResponse response;
        try {
            response = handler(method, target, headers, body);
        } catch (const std::exception& ex) {
            log_event("handler_error", headers.count("x-request-id") ? headers["x-request-id"] : "", {{"message", Json::str(ex.what())}, {"path", Json::str(target)}}, "error");
            response = HttpResponse::json(500, error_body("unavailable", "进程内部错误", headers.count("x-request-id") ? headers["x-request-id"] : ""));
        }
        std::ostringstream out;
        out << "HTTP/1.1 " << response.status << ' ' << reason(response.status) << "\r\n";
        bool has_length = false;
        bool has_close = false;
        bool has_type = false;
        for (const auto& header : response.headers) {
            out << header.first << ": " << header.second << "\r\n";
            std::string key = lower_copy(header.first);
            has_length = has_length || key == "content-length";
            has_close = has_close || key == "connection";
            has_type = has_type || key == "content-type";
        }
        if (!has_type && !response.body.empty()) {
            out << "Content-Type: application/json; charset=utf-8\r\n";
        }
        if (!has_length) {
            out << "Content-Length: " << response.body.size() << "\r\n";
        }
        if (!has_close) {
            out << "Connection: close\r\n";
        }
        out << "\r\n";
        send_all(fd, out.str(), 5);
        if (!response.body.empty()) {
            send_all(fd, response.body, 5);
        }
        note_http_request();
    } catch (const std::exception&) {
        // 对端提前关掉连接时，这个请求结束即可。
    }
    ::close(fd);
}
}  // namespace

HttpResponse HttpResponse::json(int status, const Json& payload, std::vector<std::pair<std::string, std::string>> extra) {
    HttpResponse out;
    out.status = status;
    out.headers.push_back({"Content-Type", "application/json; charset=utf-8"});
    for (auto& header : extra) {
        out.headers.push_back(std::move(header));
    }
    out.body = payload.dump();
    return out;
}

HttpResponse HttpResponse::text(int status, const std::string& text) {
    HttpResponse out;
    out.status = status;
    out.headers.push_back({"Content-Type", "text/plain; charset=utf-8"});
    out.body = text;
    return out;
}

HttpResponse HttpResponse::raw(int status, std::string content_type, std::string payload, std::vector<std::pair<std::string, std::string>> extra) {
    HttpResponse out;
    out.status = status;
    out.headers.push_back({"Content-Type", std::move(content_type)});
    for (auto& header : extra) {
        out.headers.push_back(std::move(header));
    }
    out.body = std::move(payload);
    return out;
}

HttpResponse HttpResponse::empty(int status) {
    HttpResponse out;
    out.status = status;
    return out;
}

void shutdown_listen() {
    int fd = g_listen.exchange(-1);
    if (fd >= 0) {
        ::shutdown(fd, SHUT_RDWR);
        ::close(fd);
    }
}

void serve(int port, const Handler& handler) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::cerr << "bind " << port << " failed\n";
        std::_Exit(1);
    }
    int yes = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<uint16_t>(port));
    inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);
    if (bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0 || listen(fd, 128) < 0) {
        std::cerr << "bind " << port << " failed\n";
        ::close(fd);
        std::_Exit(1);
    }
    g_listen.store(fd);
    struct sigaction action {};
    action.sa_handler = [](int) {
        g_stop.store(true);
        shutdown_listen();
    };
    sigaction(SIGTERM, &action, nullptr);
    sigaction(SIGINT, &action, nullptr);
    while (!g_stop.load()) {
        int client = ::accept(fd, nullptr, nullptr);
        if (client < 0) {
            if (g_stop.load()) {
                break;
            }
            continue;
        }
        int nodelay = 1;
        setsockopt(client, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
        std::thread(handle_client, client, handler).detach();
    }
    shutdown_listen();
}

HttpResult http_request(const std::string& method, const std::string& url, const Json* body, const std::map<std::string, std::string>& headers, double timeout_sec) {
    ParsedUrl target = parse_url(url);
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        throw std::runtime_error("socket");
    }
    int nodelay = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<uint16_t>(target.port));
    if (inet_pton(AF_INET, target.host.c_str(), &address.sin_addr) != 1) {
        ::close(fd);
        throw std::runtime_error("host");
    }
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    int connected = ::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address));
    if (connected < 0 && errno != EINPROGRESS) {
        ::close(fd);
        throw std::runtime_error("connect");
    }
    if (connected < 0 && !wait_fd(fd, POLLOUT, timeout_sec)) {
        ::close(fd);
        throw std::runtime_error("timed out");
    }
    int so_error = 0;
    socklen_t len = sizeof(so_error);
    getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &len);
    if (so_error != 0) {
        ::close(fd);
        throw std::runtime_error("connect");
    }
    fcntl(fd, F_SETFL, flags);
    std::string payload = body == nullptr ? "" : body->dump();
    std::ostringstream request;
    request << method << ' ' << target.path << " HTTP/1.1\r\n";
    request << "Host: " << target.host << ':' << target.port << "\r\n";
    request << "Connection: close\r\n";
    bool has_type = false;
    for (const auto& header : headers) {
        request << header.first << ": " << header.second << "\r\n";
        if (lower_copy(header.first) == "content-type") {
            has_type = true;
        }
    }
    if (body != nullptr) {
        if (!has_type) {
            request << "Content-Type: application/json\r\n";
        }
        request << "Content-Length: " << payload.size() << "\r\n";
    }
    request << "\r\n";
    send_all(fd, request.str(), timeout_sec);
    if (!payload.empty()) {
        send_all(fd, payload, timeout_sec);
    }
    std::string data;
    double deadline = monotonic() + timeout_sec;
    while (data.find("\r\n\r\n") == std::string::npos) {
        double left = deadline - monotonic();
        if (left <= 0) {
            ::close(fd);
            throw std::runtime_error("timed out");
        }
        std::string chunk = recv_some(fd, left);
        if (chunk.empty()) {
            break;
        }
        data += chunk;
    }
    auto head_end = data.find("\r\n\r\n");
    if (head_end == std::string::npos) {
        ::close(fd);
        throw std::runtime_error("response");
    }
    std::string head = data.substr(0, head_end);
    std::string response_body = data.substr(head_end + 4);
    std::istringstream lines(head);
    std::string status_line;
    std::getline(lines, status_line);
    std::istringstream status_parser(status_line);
    std::string http_version;
    int status = 0;
    status_parser >> http_version >> status;
    HttpResult result;
    result.status = status;
    std::string header_line;
    while (std::getline(lines, header_line)) {
        if (!header_line.empty() && header_line.back() == '\r') {
            header_line.pop_back();
        }
        auto colon = header_line.find(':');
        if (colon == std::string::npos) {
            continue;
        }
        std::string key = lower_copy(header_line.substr(0, colon));
        std::string value = header_line.substr(colon + 1);
        if (!value.empty() && value[0] == ' ') {
            value.erase(value.begin());
        }
        result.headers[key] = value;
    }
    int length = result.headers.count("content-length") ? std::stoi(result.headers["content-length"]) : -1;
    if (length >= 0) {
        while (static_cast<int>(response_body.size()) < length) {
            double left = deadline - monotonic();
            if (left <= 0) {
                ::close(fd);
                throw std::runtime_error("timed out");
            }
            std::string chunk = recv_some(fd, left);
            if (chunk.empty()) {
                break;
            }
            response_body += chunk;
        }
        response_body.resize(static_cast<size_t>(std::max(0, length)));
    }
    result.body = std::move(response_body);
    ::close(fd);
    return result;
}

std::string url_encode(const std::string& value) {
    static const char* hex = "0123456789ABCDEF";
    std::string out;
    for (unsigned char ch : value) {
        if (std::isalnum(ch) || ch == '-' || ch == '_' || ch == '.' || ch == '~') {
            out.push_back(static_cast<char>(ch));
        } else {
            out.push_back('%');
            out.push_back(hex[ch >> 4]);
            out.push_back(hex[ch & 0xf]);
        }
    }
    return out;
}

std::string url_decode(const std::string& value) {
    std::string out;
    for (size_t n = 0; n < value.size(); ++n) {
        if (value[n] == '%' && n + 2 < value.size()) {
            int hi = hex_nibble(value[n + 1]);
            int lo = hex_nibble(value[n + 2]);
            if (hi >= 0 && lo >= 0) {
                out.push_back(static_cast<char>((hi << 4) | lo));
                n += 2;
                continue;
            }
        }
        if (value[n] == '+') {
            out.push_back(' ');
            continue;
        }
        out.push_back(value[n]);
    }
    return out;
}

std::pair<std::string, std::map<std::string, std::string>> split_path(const std::string& path) {
    auto query_at = path.find('?');
    std::string route = query_at == std::string::npos ? path : path.substr(0, query_at);
    std::map<std::string, std::string> query;
    if (query_at != std::string::npos) {
        std::string text = path.substr(query_at + 1);
        size_t start = 0;
        while (start < text.size()) {
            auto amp = text.find('&', start);
            std::string pair = text.substr(start, amp == std::string::npos ? std::string::npos : amp - start);
            auto eq = pair.find('=');
            std::string key = url_decode(eq == std::string::npos ? pair : pair.substr(0, eq));
            std::string value = url_decode(eq == std::string::npos ? "" : pair.substr(eq + 1));
            query[key] = value;
            if (amp == std::string::npos) {
                break;
            }
            start = amp + 1;
        }
    }
    return {route, query};
}

std::string header_request_id(const std::map<std::string, std::string>& headers) {
    auto found = headers.find("x-request-id");
    if (found != headers.end() && !found->second.empty()) {
        return found->second;
    }
    return new_id("req_");
}
