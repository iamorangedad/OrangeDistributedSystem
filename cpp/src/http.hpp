#pragma once

#include "json.hpp"

#include <functional>
#include <map>
#include <string>
#include <utility>
#include <vector>

struct HttpResponse {
    int status = 200;
    std::vector<std::pair<std::string, std::string>> headers;
    std::string body;

    static HttpResponse json(int status, const Json& payload, std::vector<std::pair<std::string, std::string>> extra = {});
    static HttpResponse text(int status, const std::string& text);
    static HttpResponse raw(int status, std::string content_type, std::string payload, std::vector<std::pair<std::string, std::string>> extra = {});
    static HttpResponse empty(int status);
};

struct HttpResult {
    int status = 0;
    std::map<std::string, std::string> headers;
    std::string body;
};

using Handler = std::function<HttpResponse(const std::string&, const std::string&, const std::map<std::string, std::string>&, const std::string&)>;

void serve(int port, const Handler& handler);
HttpResult http_request(const std::string& method, const std::string& url, const Json* body, const std::map<std::string, std::string>& headers, double timeout_sec);
std::pair<std::string, std::map<std::string, std::string>> split_path(const std::string& path);
std::string url_encode(const std::string& value);
std::string header_request_id(const std::map<std::string, std::string>& headers);
void shutdown_listen();
