package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type response struct {
	status  int
	headers map[string]string
	body    []byte
}

func jsonResponse(status int, payload any, extra map[string]string) response {
	headers := map[string]string{"Content-Type": "application/json; charset=utf-8"}
	for key, value := range extra {
		headers[key] = value
	}
	return response{status: status, headers: headers, body: marshal(payload)}
}

func textResponse(status int, text string) response {
	return response{status: status, headers: map[string]string{"Content-Type": "text/plain; charset=utf-8"}, body: []byte(text)}
}

func emptyResponse(status int) response {
	return response{status: status, headers: map[string]string{}, body: nil}
}

type handlerFunc func(method, path string, headers map[string]string, raw []byte) response

var (
	serverMu      sync.Mutex
	currentServer *http.Server
)

var httpClient = &http.Client{
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	},
	Transport: &http.Transport{DisableKeepAlives: true},
}

func httpRequest(method, rawURL string, body any, headers map[string]string, timeout time.Duration) (int, map[string]string, []byte, error) {
	var payload io.Reader
	if body != nil {
		payload = bytes.NewReader(marshal(body))
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, rawURL, payload)
	if err != nil {
		return 0, nil, nil, err
	}
	req.Close = true
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, nil, err
	}
	out := map[string]string{}
	for key, values := range resp.Header {
		if len(values) > 0 {
			out[strings.ToLower(key)] = values[0]
		}
	}
	return resp.StatusCode, out, raw, nil
}

func splitPath(raw string) (string, map[string]string) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw, map[string]string{}
	}
	query := map[string]string{}
	for key, values := range parsed.Query() {
		if len(values) > 0 {
			query[key] = values[len(values)-1]
		}
	}
	path := parsed.Path
	if path == "" {
		path = "/"
	}
	return path, query
}

func headerRequestID(headers map[string]string) string {
	if incoming := headers["x-request-id"]; incoming != "" {
		return incoming
	}
	return newID("req_")
}

func serve(port int, handler handlerFunc) error {
	listener, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port))
	if err != nil {
		return err
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wrote := false
		defer func() {
			if recovered := recover(); recovered != nil && !wrote {
				logEvent("handler_error", r.Header.Get("X-Request-Id"), "error", map[string]any{
					"message": fmt.Sprint(recovered),
					"path":    r.URL.RequestURI(),
				})
				payload := marshal(errorBody("unavailable", "进程内部错误", r.Header.Get("X-Request-Id")))
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Connection", "close")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write(payload)
			}
			noteHTTPRequest()
		}()
		raw, _ := io.ReadAll(r.Body)
		headers := map[string]string{}
		for key, values := range r.Header {
			if len(values) > 0 {
				headers[strings.ToLower(key)] = values[0]
			}
		}
		result := handler(r.Method, r.URL.RequestURI(), headers, raw)
		for key, value := range result.headers {
			w.Header().Set(key, value)
		}
		w.Header().Set("Connection", "close")
		if result.status == 0 {
			result.status = http.StatusInternalServerError
		}
		wrote = true
		w.WriteHeader(result.status)
		if len(result.body) > 0 {
			_, _ = w.Write(result.body)
		}
	})}
	serverMu.Lock()
	currentServer = server
	serverMu.Unlock()
	err = server.Serve(listener)
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

func shutdownServer() {
	serverMu.Lock()
	server := currentServer
	serverMu.Unlock()
	if server == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}
