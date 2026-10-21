//! 对照 `orange/common.py` 里的本机 HTTP。不跟随重定向。

use crate::common::{self, note_http_request};
use serde_json::Value;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

#[derive(Clone)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

pub fn json_response(status: u16, payload: &Value, extra: &[(&str, String)]) -> Response {
    let body = payload.to_string().into_bytes();
    let mut headers = vec![("Content-Type".into(), "application/json; charset=utf-8".into())];
    for (key, value) in extra {
        headers.push(((*key).to_string(), value.clone()));
    }
    Response { status, headers, body }
}

pub fn text_response(status: u16, text: &str) -> Response {
    Response {
        status,
        headers: vec![("Content-Type".into(), "text/plain; charset=utf-8".into())],
        body: text.as_bytes().to_vec(),
    }
}

pub fn empty_response(status: u16) -> Response {
    Response { status, headers: Vec::new(), body: Vec::new() }
}

pub fn split_path(path: &str) -> (String, HashMap<String, String>) {
    let (path, query) = path.split_once('?').unwrap_or((path, ""));
    let mut map = HashMap::new();
    if !query.is_empty() {
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            map.insert(decode(key), decode(value));
        }
    }
    (path.to_string(), map)
}

pub fn encode(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let Ok(byte) = u8::from_str_radix(&value[index + 1..index + 3], 16) {
                out.push(byte);
                index += 3;
                continue;
            }
        }
        if bytes[index] == b'+' {
            out.push(b' ');
        } else {
            out.push(bytes[index]);
        }
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub fn http_request(
    method: &str,
    url: &str,
    body: Option<&Value>,
    headers: &[(&str, &str)],
    timeout: f64,
) -> std::io::Result<(u16, HashMap<String, String>, Vec<u8>)> {
    let parsed = parse_url(url).ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "地址不合法"))?;
    let timeout = Duration::from_secs_f64(timeout.max(0.05));
    let mut stream = TcpStream::connect_timeout(&parsed.addr, timeout)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    let payload = body.map(|value| value.to_string().into_bytes()).unwrap_or_default();
    let mut request = format!("{} {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n", method, parsed.path, parsed.host_header);
    if body.is_some() {
        request.push_str("Content-Type: application/json\r\n");
        request.push_str(&format!("Content-Length: {}\r\n", payload.len()));
    }
    for (key, value) in headers {
        request.push_str(key);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes())?;
    if body.is_some() {
        stream.write_all(&payload)?;
    }
    read_response(&mut stream)
}

struct ParsedUrl {
    addr: std::net::SocketAddr,
    path: String,
    host_header: String,
}

fn parse_url(url: &str) -> Option<ParsedUrl> {
    let rest = url.strip_prefix("http://")?;
    let (authority, path) = rest.split_once('/').map(|(a, p)| (a, format!("/{p}"))).unwrap_or((rest, "/".into()));
    let (host, port) = authority.rsplit_once(':')?;
    let port: u16 = port.parse().ok()?;
    let addr = format!("{host}:{port}").parse().ok()?;
    let path = if url.contains('?') && !path.contains('?') {
        let query = url.split_once('?').map(|(_, q)| q).unwrap_or("");
        format!("{path}?{query}")
    } else {
        path
    };
    Some(ParsedUrl { addr, path, host_header: authority.to_string() })
}

fn read_response(stream: &mut TcpStream) -> std::io::Result<(u16, HashMap<String, String>, Vec<u8>)> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            break None;
        }
        buffer.extend_from_slice(&chunk[..n]);
        if let Some(pos) = find_header_end(&buffer) {
            break Some(pos);
        }
        if buffer.len() > 1_048_576 {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "响应头过大"));
        }
    };
    let Some(header_end) = header_end else {
        return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "响应不完整"));
    };
    let head = String::from_utf8_lossy(&buffer[..header_end]).into_owned();
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or("");
    let status = status_line.split_whitespace().nth(1).and_then(|v| v.parse().ok()).unwrap_or(0);
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((key, value)) = line.split_once(':') {
            headers.insert(key.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }
    let mut body = buffer[header_end + 4..].to_vec();
    if let Some(length) = headers.get("content-length").and_then(|v| v.parse::<usize>().ok()) {
        while body.len() < length {
            let n = stream.read(&mut chunk)?;
            if n == 0 {
                break;
            }
            body.extend_from_slice(&chunk[..n]);
        }
        body.truncate(length);
    } else {
        loop {
            match stream.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(n) => body.extend_from_slice(&chunk[..n]),
            }
        }
    }
    Ok((status, headers, body))
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

fn bind_loopback(port: u16) -> std::io::Result<TcpListener> {
    use std::os::fd::FromRawFd;
    unsafe {
        let fd = libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0);
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let yes: libc::c_int = 1;
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEADDR,
            &yes as *const _ as *const libc::c_void,
            std::mem::size_of_val(&yes) as libc::socklen_t,
        );
        let addr = libc::sockaddr_in {
            sin_family: libc::AF_INET as libc::sa_family_t,
            sin_port: port.to_be(),
            sin_addr: libc::in_addr { s_addr: u32::from_be_bytes([127, 0, 0, 1]).to_be() },
            sin_zero: [0; 8],
        };
        if libc::bind(fd, &addr as *const _ as *const libc::sockaddr, std::mem::size_of::<libc::sockaddr_in>() as _) != 0
            || libc::listen(fd, 128) != 0
        {
            let err = std::io::Error::last_os_error();
            libc::close(fd);
            return Err(err);
        }
        let listener = TcpListener::from_raw_fd(fd);
        let timeout = libc::timeval { tv_sec: 0, tv_usec: 200_000 };
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            &timeout as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::timeval>() as libc::socklen_t,
        );
        Ok(listener)
    }
}

pub fn serve<F>(port: u16, handler: F)
where
    F: Fn(String, String, HashMap<String, String>, Vec<u8>) -> Response + Send + Sync + 'static,
{
    let listener = bind_loopback(port).unwrap_or_else(|err| {
        eprintln!("bind {port} failed: {err}");
        std::process::exit(1);
    });
    let handler = std::sync::Arc::new(handler);
    thread::spawn(move || {
        while !common::stop_requested() {
            match listener.accept() {
                Ok((stream, _)) => {
                    let handler = handler.clone();
                    thread::spawn(move || handle_connection(stream, handler.as_ref()));
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock || err.kind() == std::io::ErrorKind::TimedOut => {}
                Err(_) => thread::sleep(Duration::from_millis(20)),
            }
        }
    });
}

fn handle_connection<F>(mut stream: TcpStream, handler: &F)
where
    F: Fn(String, String, HashMap<String, String>, Vec<u8>) -> Response,
{
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return,
            Ok(n) => {
                buffer.extend_from_slice(&chunk[..n]);
                if let Some(pos) = find_header_end(&buffer) {
                    break pos;
                }
                if buffer.len() > 1_048_576 {
                    return;
                }
            }
        }
    };
    let head = String::from_utf8_lossy(&buffer[..header_end]).into_owned();
    let mut lines = head.split("\r\n");
    let request = lines.next().unwrap_or("");
    let mut parts = request.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("/").to_string();
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((key, value)) = line.split_once(':') {
            headers.insert(key.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }
    let length = headers.get("content-length").and_then(|v| v.parse::<usize>().ok()).unwrap_or(0);
    let mut body = buffer[header_end + 4..].to_vec();
    while body.len() < length {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(n) => body.extend_from_slice(&chunk[..n]),
        }
    }
    body.truncate(length);
    let response = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(method, path.clone(), headers.clone(), body))) {
        Ok(response) => response,
        Err(_) => json_response(500, &common::error_body("unavailable", "进程内部错误", headers.get("x-request-id").map(|s| s.as_str()).unwrap_or("")), &[]),
    };
    let reason = reason_phrase(response.status);
    let mut out = format!("HTTP/1.1 {} {reason}\r\n", response.status);
    let mut has_length = false;
    let mut has_close = false;
    for (key, value) in &response.headers {
        if key.eq_ignore_ascii_case("content-length") {
            has_length = true;
        }
        if key.eq_ignore_ascii_case("connection") {
            has_close = true;
        }
        out.push_str(key);
        out.push_str(": ");
        out.push_str(value);
        out.push_str("\r\n");
    }
    if !has_length {
        out.push_str(&format!("Content-Length: {}\r\n", response.body.len()));
    }
    if !has_close {
        out.push_str("Connection: close\r\n");
    }
    out.push_str("\r\n");
    let _ = stream.write_all(out.as_bytes());
    if !response.body.is_empty() {
        let _ = stream.write_all(&response.body);
    }
    note_http_request();
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        409 => "Conflict",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "OK",
    }
}

pub fn decode_json(raw: &[u8]) -> Result<Option<Value>, ()> {
    if raw.is_empty() {
        return Ok(None);
    }
    match serde_json::from_slice::<Value>(raw) {
        Ok(Value::Null) => Ok(None),
        Ok(value) => Ok(Some(value)),
        Err(_) => Err(()),
    }
}

pub fn block_until_stopped() {
    while !common::stop_requested() {
        thread::sleep(Duration::from_millis(100));
    }
}
