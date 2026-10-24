import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

/** 对照 orange/common.py 里的本机 HTTP。不跟随重定向。 */
final class HttpUtil {
    private HttpUtil() {}

    record Response(int status, Map<String, String> headers, byte[] body) {
        static Response json(int status, Object payload, String... extra) {
            Map<String, String> headers = new LinkedHashMap<>();
            headers.put("Content-Type", "application/json; charset=utf-8");
            for (int i = 0; i < extra.length; i += 2) {
                headers.put(extra[i], extra[i + 1]);
            }
            return new Response(status, headers, Json.stringify(payload).getBytes(StandardCharsets.UTF_8));
        }

        static Response text(int status, String text) {
            return new Response(status, Map.of("Content-Type", "text/plain; charset=utf-8"), text.getBytes(StandardCharsets.UTF_8));
        }

        static Response empty(int status) {
            return new Response(status, Map.of(), new byte[0]);
        }
    }

    record Result(int status, Map<String, String> headers, byte[] body) {}

    interface Handler {
        Response handle(String method, String path, Map<String, String> headers, byte[] body);
    }

    static HttpServer start(int port, Handler handler) {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
            server.createContext("/", exchange -> dispatch(exchange, handler));
            server.setExecutor(Executors.newCachedThreadPool(runnable -> {
                Thread thread = new Thread(runnable, "http-" + port);
                thread.setDaemon(true);
                return thread;
            }));
            server.start();
            return server;
        } catch (IOException ex) {
            System.err.println("bind " + port + " failed: " + ex.getMessage());
            System.exit(1);
            throw new IllegalStateException(ex);
        }
    }

    static void serve(int port, Handler handler) {
        HttpServer server = start(port, handler);
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Common.stop = true;
            server.stop(0);
            latch.countDown();
        }));
        while (!Common.stop) {
            try {
                if (latch.await(200, java.util.concurrent.TimeUnit.MILLISECONDS)) {
                    break;
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private static void dispatch(HttpExchange exchange, Handler handler) {
        Map<String, String> headers = new LinkedHashMap<>();
        exchange.getRequestHeaders().forEach((key, values) -> {
            if (!values.isEmpty()) {
                headers.put(key.toLowerCase(), values.get(0));
            }
        });
        byte[] body = readAll(exchange.getRequestBody());
        String requestId = headers.getOrDefault("x-request-id", "");
        Response response;
        try {
            URI uri = exchange.getRequestURI();
            String path = uri.getRawPath() == null ? "/" : uri.getRawPath();
            if (uri.getRawQuery() != null) {
                path = path + "?" + uri.getRawQuery();
            }
            response = handler.handle(exchange.getRequestMethod(), path, headers, body);
        } catch (Exception ex) {
            Common.logEvent("handler_error", requestId, "error", Json.obj("message", String.valueOf(ex.getMessage()), "path", exchange.getRequestURI().getPath()));
            response = Response.json(500, Common.errorBody("unavailable", "进程内部错误", requestId));
        }
        try {
            response.headers.forEach((key, value) -> exchange.getResponseHeaders().set(key, value));
            byte[] payload = response.body == null ? new byte[0] : response.body;
            long length = response.status == 204 ? -1 : payload.length;
            exchange.sendResponseHeaders(response.status, length);
            if (length > 0) {
                exchange.getResponseBody().write(payload);
            }
            exchange.close();
        } catch (IOException ignored) {
            exchange.close();
        } finally {
            Common.noteHttpRequest();
        }
    }

    static Map<String, String> splitQuery(String path) {
        Map<String, String> query = new LinkedHashMap<>();
        int mark = path.indexOf('?');
        if (mark < 0) {
            return query;
        }
        for (String pair : path.substring(mark + 1).split("&")) {
            if (pair.isEmpty()) {
                continue;
            }
            int eq = pair.indexOf('=');
            String key = decode(eq < 0 ? pair : pair.substring(0, eq));
            String value = decode(eq < 0 ? "" : pair.substring(eq + 1));
            query.put(key, value);
        }
        return query;
    }

    static String routeOf(String path) {
        int mark = path.indexOf('?');
        return mark < 0 ? path : path.substring(0, mark);
    }

    static String encode(String value) {
        StringBuilder out = new StringBuilder();
        for (byte bite : value.getBytes(StandardCharsets.UTF_8)) {
            int ch = bite & 0xff;
            if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '.' || ch == '~') {
                out.append((char) ch);
            } else {
                out.append(String.format("%%%02X", ch));
            }
        }
        return out.toString();
    }

    static String decode(String value) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch == '%' && i + 2 < value.length()) {
                out.write(Integer.parseInt(value.substring(i + 1, i + 3), 16));
                i += 2;
            } else if (ch == '+') {
                out.write(' ');
            } else {
                out.write(ch);
            }
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    static Result request(String method, String url, Object body, Map<String, String> headers, double timeoutSec) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setInstanceFollowRedirects(false);
        connection.setRequestMethod(method);
        int timeout = Math.max(1, (int) (timeoutSec * 1000));
        connection.setConnectTimeout(timeout);
        connection.setReadTimeout(timeout);
        if (headers != null) {
            headers.forEach(connection::setRequestProperty);
        }
        if (body != null) {
            byte[] payload = Json.stringify(body).getBytes(StandardCharsets.UTF_8);
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.getOutputStream().write(payload);
        }
        int status = connection.getResponseCode();
        InputStream stream = status >= 400 ? connection.getErrorStream() : connection.getInputStream();
        byte[] raw = stream == null ? new byte[0] : readAll(stream);
        Map<String, String> responseHeaders = new LinkedHashMap<>();
        connection.getHeaderFields().forEach((key, values) -> {
            if (key != null && values != null && !values.isEmpty()) {
                responseHeaders.put(key.toLowerCase(), values.get(0));
            }
        });
        connection.disconnect();
        return new Result(status, responseHeaders, raw);
    }

    static Object decodeJson(byte[] raw) {
        if (raw == null || raw.length == 0) {
            return null;
        }
        Object value = Json.parse(new String(raw, StandardCharsets.UTF_8));
        return value;
    }

    private static byte[] readAll(InputStream stream) {
        try (stream) {
            return stream.readAllBytes();
        } catch (IOException ex) {
            return new byte[0];
        }
    }
}
