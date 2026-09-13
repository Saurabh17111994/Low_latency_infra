//! Production `BridgeClient` transport against the Go execution bridge.
//!
//! Implements the offline-runnable HTTP/WS client for
//! [`BridgeClient`](super::client::BridgeClient): `POST /v1/commands` carries a
//! [`CommandEnvelope`] and synchronously returns a [`ReportEnvelope`], and `/v1/events`
//! streams asynchronous reports over WebSocket. It speaks the exact wire contract of
//! `code/02_services/06_execution_bridge/go-bridge/server.go` (`Bearer` auth, loopback bridge)
//! so it interoperates with the Go bridge's `fake` profile — no real Arrow credentials ever
//! leave this crate.
//!
//! The HTTP hop is a deliberately small client: the bridge is loopback-only and the server is a
//! controlled peer, so a minimal HTTP/1.1 client (content-length / chunked / read-to-close
//! framing) avoids pulling a full `reqwest` dependency tree.
//!
//! The `/v1/events` intake is an equally small RFC 6455 client (no `tungstenite`): it performs
//! the upgrade handshake by hand, decodes text frames into [`ReportEnvelope`]s, answers server
//! pings with masked pongs, and hands envelopes to a caller-owned [`BridgeReportStream`]. The
//! intake task runs autonomously and reconnects with backoff when the bridge closes the socket
//! (the Go server's own ping/pong keepalive is honored, so a long-lived quiet market never
//! trips a read timeout at either side).

use std::io;
use std::time::Duration;

use anyhow::{Context as _, Result};
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

use super::client::{BridgeClient, BridgeReportStream, BRIDGE_REPORT_BUFFER};
use super::protocol::{CommandEnvelope, ReportEnvelope};

// --- RFC 6455 opcodes (subset we speak: text, ping, pong, close). ---
const OP_TEXT: u8 = 0x1;
const OP_CLOSE: u8 = 0x8;
const OP_PING: u8 = 0x9;
const OP_PONG: u8 = 0xA;
/// Frames larger than this are treated as corrupt and drop the connection (reconnect).
const MAX_WS_FRAME: usize = 1 << 20;

/// A parsed HTTP/1.1 response (status + headers + raw body).
pub(crate) struct HttpResponse {
    pub(crate) status: u16,
    pub(crate) body: Vec<u8>,
}

/// Minimal loopback HTTP/1.1 request (no redirects, no TLS, `Connection: close`).
async fn http_request(
    method: &str,
    url: &str,
    auth_token: &str,
    body_json: &[u8],
) -> Result<HttpResponse> {
    let (host, port, path) = parse_url(url)?;
    let addrs = tokio::net::lookup_host((host.as_str(), port))
        .await
        .with_context(|| format!("resolve bridge host {host}:{port}"))?;
    let addr = addrs
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("bridge host {host}:{port} resolved to nothing"))?;
    let mut stream = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connect to bridge {addr}"))?;
    stream.set_nodelay(true)?;

    let head = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: {host}:{port}\r\n\
         Authorization: Bearer {auth_token}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        body_json.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body_json).await?;
    stream.flush().await?;

    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut chunk = [0u8; 4096];
        loop {
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            // P3-189: stop as soon as the response is complete; `Connection: close` is a
            // request, and a keep-alive peer would otherwise hold this read until the timeout.
            if response_is_complete(&buf) {
                break;
            }
        }
        Ok::<_, io::Error>(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("bridge read timeout"))??;

    parse_http_response(&buf)
}

/// Minimal loopback HTTP/1.1 `POST`.
pub(crate) async fn http_post(
    url: &str,
    auth_token: &str,
    body_json: &[u8],
) -> Result<HttpResponse> {
    http_request("POST", url, auth_token, body_json).await
}

/// Minimal loopback HTTP/1.1 `GET`.
async fn http_get(url: &str, auth_token: &str) -> Result<HttpResponse> {
    http_request("GET", url, auth_token, b"").await
}

fn parse_url(url: &str) -> Result<(String, u16, String)> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| anyhow::anyhow!("bridge base url must be http:// (loopback only): {url}"))?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].to_string()),
        None => (rest, "/".to_string()),
    };
    // Reject userinfo and fragments/schemes we cannot support.
    anyhow::ensure!(
        !authority.contains('@'),
        "bridge url must not contain credentials: {url}"
    );
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse::<u16>()?),
        None => (authority.to_string(), 8787),
    };
    Ok((host, port, path))
}

/// Parses a raw HTTP/1.1 response into status + headers + body.
fn parse_http_response(raw: &[u8]) -> Result<HttpResponse> {
    let head_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| anyhow::anyhow!("malformed HTTP response (no header terminator)"))?;
    let head = std::str::from_utf8(&raw[..head_end])?;
    let mut lines = head.split("\r\n");

    let status_line = lines.next().unwrap_or_default();
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("malformed status line: {status_line:?}"))?
        .parse()?;

    let raw_body = &raw[head_end + 4..];
    let header = |name: &str| {
        lines.clone().find_map(|line| {
            let (k, v) = line.split_once(':')?;
            (k.trim().eq_ignore_ascii_case(name)).then(|| v.trim().to_string())
        })
    };
    let chunked = header("transfer-encoding")
        .unwrap_or_default()
        .to_lowercase()
        == "chunked";
    let body = if let Some(len) = header("content-length") {
        let len: usize = len.trim().parse()?;
        // P3-189: slicing to `len.min(raw_body.len())` reported a truncated body as a complete
        // shorter one, so callers acted on half a reply and failed later with a misleading
        // parse error. A body shorter than its own Content-Length is a framing error.
        anyhow::ensure!(
            raw_body.len() >= len,
            "truncated HTTP body: got {} of {len} bytes",
            raw_body.len()
        );
        raw_body[..len].to_vec()
    } else if chunked {
        parse_chunked_body(raw_body)?
    } else {
        raw_body.to_vec()
    };
    Ok(HttpResponse { status, body })
}

/// True when `buf` already holds a complete HTTP/1.1 response, so a reader need not wait for a
/// connection the peer keeps open.
///
/// P3-189: `Connection: close` is a request, not a promise — a keep-alive peer left the bridge
/// waiting for an EOF that never came, until its own timeout fired. Only Content-Length-framed
/// responses are detected here; a chunked body (which the Go bridge does not emit for its JSON
/// replies) still ends at EOF, keeping the previous behaviour for that case.
fn response_is_complete(buf: &[u8]) -> bool {
    let Some(head_end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
        return false;
    };
    let Ok(head) = std::str::from_utf8(&buf[..head_end]) else {
        return false;
    };
    let declared = head.split("\r\n").skip(1).find_map(|line| {
        let (k, v) = line.split_once(':')?;
        k.trim()
            .eq_ignore_ascii_case("content-length")
            .then(|| v.trim().parse::<usize>().ok())
            .flatten()
    });
    match declared {
        Some(len) => buf.len() - (head_end + 4) >= len,
        None => false,
    }
}

/// Decodes a chunked transfer body.
fn parse_chunked_body(data: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    loop {
        let line_end = match data[pos..].iter().position(|&b| b == b'\n') {
            Some(i) => pos + i,
            None => anyhow::bail!("malformed chunked body"),
        };
        // P3-430: the chunk header is `<hex size>[;ext=value]...` — the size ends at the first
        // `;`. `trim_end_matches(';')` only strips a *trailing* semicolon, so `5;ext=abc` was
        // handed to `from_str_radix` whole and every extended chunk failed to parse.
        let raw_size = std::str::from_utf8(&data[pos..line_end])?;
        let size_str = raw_size.split(';').next().unwrap_or_default().trim();
        let size = usize::from_str_radix(size_str, 16)
            .map_err(|e| anyhow::anyhow!("bad chunk size: {e}"))?;
        if size == 0 {
            break;
        }
        let chunk_start = line_end + 1;
        let chunk_end = chunk_start + size;
        if chunk_end > data.len() {
            anyhow::bail!("chunk overruns body");
        }
        out.extend_from_slice(&data[chunk_start..chunk_end]);
        pos = chunk_end;
        if data.get(pos..pos + 2) == Some(b"\r\n") {
            pos += 2;
        }
    }
    Ok(out)
}

// --- WebSocket report intake (minimal RFC 6455 client) ------------------------

struct WsFrame {
    opcode: u8,
    payload: Vec<u8>,
}

fn parse_ws_url(url: &str) -> Result<(String, u16, String)> {
    let rest = url
        .strip_prefix("ws://")
        .or_else(|| url.strip_prefix("http://"))
        .ok_or_else(|| {
            anyhow::anyhow!("bridge ws url must be ws:// or http:// (loopback only): {url}")
        })?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].to_string()),
        None => (rest, "/".to_string()),
    };
    anyhow::ensure!(
        !authority.contains('@'),
        "bridge ws url must not contain credentials: {url}"
    );
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse::<u16>()?),
        None => (authority.to_string(), 8787),
    };
    Ok((host, port, path))
}

const BASE64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(BASE64_ALPHABET[((n >> 18) & 63) as usize] as char);
        out.push(BASE64_ALPHABET[((n >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            BASE64_ALPHABET[((n >> 6) & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            BASE64_ALPHABET[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// CSPRNG-backed random bytes for the `Sec-WebSocket-Key` nonce and the frame masks.
///
/// P3-431: this used to be an xorshift seeded from the wall clock and the pid — a predictable
/// keystream for the one handshake value RFC 6455 requires to be unpredictable. `uuid` is
/// already a dependency and its v4 bytes come from the OS CSPRNG.
fn ws_random_bytes(n: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(n + 16);
    while out.len() < n {
        out.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    }
    out.truncate(n);
    out
}

/// Reads one complete WebSocket frame. Applies the mask when the peer masked the payload
/// (client→server frames are masked by RFC 6455; server→client frames are not).
async fn read_ws_frame(stream: &mut TcpStream) -> Result<WsFrame> {
    let mut hdr = [0u8; 2];
    stream.read_exact(&mut hdr).await?;
    let opcode = hdr[0] & 0x0F;
    let masked = hdr[1] & 0x80 != 0;
    let mut len = (hdr[1] & 0x7F) as u64;
    if len == 126 {
        let mut b = [0u8; 2];
        stream.read_exact(&mut b).await?;
        len = u16::from_be_bytes(b) as u64;
        // P3-432: RFC 6455 §5.2 requires the minimal length encoding — 126 means "at least 126".
        anyhow::ensure!(
            len >= 126,
            "websocket frame length {len} must not use the 16-bit form"
        );
    } else if len == 127 {
        let mut b = [0u8; 8];
        stream.read_exact(&mut b).await?;
        // P3-432: the 64-bit form's top bit is reserved and must be zero, and the form is legal
        // only for lengths the 16-bit form cannot express. (An absurd length is refused by the
        // size guard below either way; these checks make the verdict about the encoding rather
        // than about MAX_WS_FRAME.)
        anyhow::ensure!(
            b[0] & 0x80 == 0,
            "websocket frame length has the reserved bit set"
        );
        len = u64::from_be_bytes(b);
        anyhow::ensure!(
            len > 0xFFFF,
            "websocket frame length {len} must not use the 64-bit form"
        );
    }
    anyhow::ensure!(len as usize <= MAX_WS_FRAME, "websocket frame too large");
    let mask = if masked {
        let mut m = [0u8; 4];
        stream.read_exact(&mut m).await?;
        Some(m)
    } else {
        None
    };
    let mut payload = vec![0u8; len as usize];
    stream.read_exact(&mut payload).await?;
    if let Some(m) = mask {
        for (i, b) in payload.iter_mut().enumerate() {
            *b ^= m[i % 4];
        }
    }
    Ok(WsFrame { opcode, payload })
}

/// Writes one WebSocket frame from a client (always masked per RFC 6455).
async fn write_client_ws_frame(stream: &mut TcpStream, opcode: u8, payload: &[u8]) -> Result<()> {
    let mask = ws_random_bytes(4);
    let mut out = Vec::with_capacity(14 + payload.len());
    out.push(0x80 | opcode); // FIN + opcode
    match payload.len() {
        0..=125 => out.push(0x80 | payload.len() as u8),
        126..=0xFFFF => {
            out.push(0x80 | 126);
            out.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        }
        _ => {
            out.push(0x80 | 127);
            out.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        }
    }
    out.extend_from_slice(&mask);
    for (i, b) in payload.iter().enumerate() {
        out.push(b ^ mask[i % 4]);
    }
    stream.write_all(&out).await?;
    Ok(())
}

/// P3-433: the reply to the upgrade request must be a 101 (see the call site); the
/// header checks live here so they can be exercised without a socket.
fn check_upgrade_reply(reply: &[u8]) -> Result<()> {
    anyhow::ensure!(
        reply.starts_with(b"HTTP/1.1 101"),
        "websocket upgrade rejected: {}",
        String::from_utf8_lossy(&reply[..reply.len().min(120)])
    );
    let head = std::str::from_utf8(&reply[..reply.len().min(16_384)]).unwrap_or_default();
    let header = |name: &str| {
        head.split("\r\n").skip(1).find_map(|line| {
            let (k, v) = line.split_once(':')?;
            (k.trim().eq_ignore_ascii_case(name)).then(|| v.trim().to_string())
        })
    };
    // A bare 101 is not an upgrade: the peer must confirm the protocol switch itself.
    let upgrade = header("upgrade").unwrap_or_default();
    anyhow::ensure!(
        upgrade.eq_ignore_ascii_case("websocket"),
        "websocket upgrade rejected: Upgrade: {upgrade:?}"
    );
    let connection = header("connection").unwrap_or_default();
    anyhow::ensure!(
        connection
            .split(',')
            .any(|t| t.trim().eq_ignore_ascii_case("upgrade")),
        "websocket upgrade rejected: Connection: {connection:?}"
    );
    // RFC 6455's accept value is `base64(sha1(key + GUID))`. Verifying it needs a SHA-1
    // implementation this crate does not depend on, so only the shape is checked here: a
    // value-shaped forgery still passes. Recorded as a residual gap in the wave handoff.
    let accept = header("sec-websocket-accept").unwrap_or_default();
    anyhow::ensure!(
        is_base64_sha1_digest(&accept),
        "websocket upgrade rejected: Sec-WebSocket-Accept: {accept:?}"
    );
    Ok(())
}

/// A base64-encoded SHA-1 digest: 28 base64 characters with a single `=` pad.
fn is_base64_sha1_digest(value: &str) -> bool {
    value.len() == 28
        && value.ends_with('=')
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '/' | '='))
}

/// Performs the RFC 6455 upgrade on an established TCP stream.
///
/// Mirrors the Go bridge's expectations exactly: `Authorization: Bearer <token>` (the server
/// rejects otherwise) and **no** `Origin` header (the server's `CheckOrigin` only admits
/// browser-free clients).
async fn ws_handshake(
    stream: &mut TcpStream,
    host: &str,
    port: u16,
    path: &str,
    auth_token: &str,
) -> Result<()> {
    let key = base64_encode(&ws_random_bytes(16));
    let request = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: {host}:{port}\r\n\
         Authorization: Bearer {auth_token}\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Key: {key}\r\n\
         Sec-WebSocket-Version: 13\r\n\
         \r\n"
    );
    stream.write_all(request.as_bytes()).await?;
    stream.flush().await?;

    let mut reply = Vec::new();
    let mut byte = [0u8; 1];
    while !reply.windows(4).any(|w| w == b"\r\n\r\n") && reply.len() < 16_384 {
        if stream.read(&mut byte).await? == 0 {
            anyhow::bail!("connection closed during websocket upgrade");
        }
        reply.push(byte[0]);
    }
    check_upgrade_reply(&reply)
}

/// Runs one lifeline of the `/v1/events` stream, returning when the socket closes, times out,
/// or the caller's receiver is dropped. The caller decides whether (and how long) to back off.
async fn ws_stream_once(
    url: &str,
    auth_token: &str,
    tx: &Sender<ReportEnvelope>,
    read_timeout: Duration,
) -> Result<()> {
    let (host, port, path) = parse_ws_url(url)?;
    let addrs = tokio::net::lookup_host((host.as_str(), port))
        .await
        .with_context(|| format!("resolve bridge host {host}:{port}"))?;
    let addr = addrs
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("bridge ws host {host}:{port} resolved to nothing"))?;
    let mut stream = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connect to bridge ws {addr}"))?;
    stream.set_nodelay(true)?;
    ws_handshake(&mut stream, &host, port, &path, auth_token).await?;

    loop {
        let frame = match tokio::time::timeout(read_timeout, read_ws_frame(&mut stream)).await {
            Err(_) => return Ok(()), // silent for too long (bridge ping cadence missed)
            Ok(Err(_)) => return Ok(()), // bridge closed or frame corrupt
            Ok(Ok(frame)) => frame,
        };
        match frame.opcode {
            OP_TEXT => {
                if let Ok(envelope) = serde_json::from_slice::<ReportEnvelope>(&frame.payload) {
                    if tx.send(envelope).await.is_err() {
                        return Ok(()); // caller dropped the receiver (shutdown)
                    }
                }
                // A malformed frame is ignored, not fatal: the bridge never emits one in
                // normal operation, and dropping the connection would reorder nothing.
            }
            OP_PING => {
                if write_client_ws_frame(&mut stream, OP_PONG, &frame.payload)
                    .await
                    .is_err()
                {
                    return Ok(());
                }
            }
            OP_PONG => {} // we never send pings; ignore
            OP_CLOSE => {
                let _ = write_client_ws_frame(&mut stream, OP_CLOSE, &frame.payload).await;
                return Ok(());
            }
            _ => {} // continuation/binary frames are outside the bridge contract
        }
    }
}

/// Background intake task: connects to `/v1/events` and reconnects with exponential backoff
/// (capped at [`HttpBridgeClient::reconnect_max`]`-equivalent`) until the receiver is dropped.
async fn report_intake_loop(
    url: String,
    auth_token: String,
    tx: Sender<ReportEnvelope>,
    reconnect_min: Duration,
    reconnect_max: Duration,
    read_timeout: Duration,
) {
    let mut delay = reconnect_min;
    loop {
        if tx.is_closed() {
            return;
        }
        let clean = ws_stream_once(&url, &auth_token, &tx, read_timeout)
            .await
            .is_ok();
        tokio::time::sleep(delay).await;
        delay = if clean {
            reconnect_min
        } else {
            (delay * 2).min(reconnect_max)
        };
    }
}

/// Production HTTP/WS [`BridgeClient`] targeting the Go execution bridge.
#[derive(Debug)]
pub struct HttpBridgeClient {
    base_url: String,
    auth_token: String,
    connected: bool,
    reports_tx: Option<Sender<ReportEnvelope>>,
    intake: Option<JoinHandle<()>>,
    reconnect_min: Duration,
    reconnect_max: Duration,
    read_timeout: Duration,
}

impl HttpBridgeClient {
    pub fn new(base_url: String, auth_token: String) -> Self {
        Self {
            base_url,
            auth_token,
            connected: false,
            reports_tx: None,
            intake: None,
            reconnect_min: Duration::from_millis(500),
            reconnect_max: Duration::from_secs(10),
            // The Go bridge pings every 20 s and expects a pong within its 60 s read window;
            // honouring a wider window here means we only drop a truly dead lifeline.
            read_timeout: Duration::from_secs(65),
        }
    }

    /// Test-facing knobs: shrink the reconnect backoff so reconnect tests run in milliseconds.
    #[cfg(test)]
    fn with_reconnect(mut self, min: Duration, max: Duration) -> Self {
        self.reconnect_min = min;
        self.reconnect_max = max;
        self
    }
}

#[async_trait]
impl BridgeClient for HttpBridgeClient {
    fn is_connected(&self) -> bool {
        self.connected
    }

    async fn connect(&mut self) -> Result<()> {
        // Reachability probe only — `/healthz` answers without Arrow credentials of any kind.
        let url = format!("{}/healthz", self.base_url.trim_end_matches('/'));
        let resp = http_get(&url, &self.auth_token).await?;
        anyhow::ensure!(
            resp.status == 200,
            "bridge health probe failed with status {}",
            resp.status
        );
        self.connected = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.connected = false;
        // Stop the intake task; dropping the sender also unblocks its `tx.is_closed()` check.
        if let Some(tx) = self.reports_tx.take() {
            drop(tx);
        }
        if let Some(task) = self.intake.take() {
            task.abort();
        }
        Ok(())
    }

    async fn send_command(&mut self, envelope: CommandEnvelope) -> Result<ReportEnvelope> {
        let url = format!("{}/v1/commands", self.base_url.trim_end_matches('/'));
        let body = serde_json::to_vec(&envelope)?;
        let resp = http_post(&url, &self.auth_token, &body).await?;
        anyhow::ensure!(
            resp.status == 200,
            "bridge command rejected with status {}: {}",
            resp.status,
            String::from_utf8_lossy(&resp.body)
        );
        let report: ReportEnvelope = serde_json::from_slice(&resp.body)
            .with_context(|| "bridge returned an unparseable report envelope")?;
        Ok(report)
    }

    fn take_reports(&mut self) -> Option<BridgeReportStream> {
        if self.reports_tx.is_some() {
            return None; // the stream has already been taken
        }
        let (tx, rx) = tokio::sync::mpsc::channel(BRIDGE_REPORT_BUFFER);
        let url = format!("{}/v1/events", self.base_url.trim_end_matches('/'));
        let token = self.auth_token.clone();
        let min = self.reconnect_min;
        let max = self.reconnect_max;
        let read_timeout = self.read_timeout;
        let intake = tokio::spawn(report_intake_loop(
            url,
            token,
            tx.clone(),
            min,
            max,
            read_timeout,
        ));
        self.reports_tx = Some(tx);
        self.intake = Some(intake);
        Some(rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::protocol::{
        Command, CommandEnvelope, OrderCommand, OrderType, Product, ReportEnvelope, ReportOutcome,
        TransactionType, Validity, PROTOCOL_VERSION, RECORD_COMMAND, RECORD_REPORT,
    };
    use anyhow::Result;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    const WS_101: &[u8] = b"HTTP/1.1 101 Switching Protocols\r\n\
Upgrade: websocket\r\n\
Connection: Upgrade\r\n\
Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n";

    // P3-430: a chunk extension (`;name=value`) is part of the chunk header, not of the size.
    #[test]
    fn chunked_body_accepts_chunk_extensions() {
        let body =
            parse_chunked_body(b"5;ext=abc\r\nhello\r\n6;x=1\r\n world\r\n0\r\n\r\n").unwrap();
        assert_eq!(body, b"hello world");
    }

    // P3-189 (first half): a body shorter than Content-Length is a truncated response and
    // must surface as an error, not as a silently shorter body.
    #[test]
    fn truncated_http_body_is_an_error_not_a_shorter_body() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort";
        let err = match parse_http_response(raw) {
            Ok(_) => panic!("a body shorter than Content-Length must not be masked"),
            Err(e) => e,
        };
        assert!(
            format!("{err}").contains("truncated"),
            "unexpected error: {err}"
        );
    }

    // P3-189 (second half): `Connection: close` is a request, not a promise. A complete
    // response whose peer keeps the connection open must return, not wait for EOF.
    #[tokio::test]
    async fn http_post_returns_once_the_declared_body_is_complete() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = sock.read(&mut buf).await;
            sock.write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok",
            )
            .await
            .unwrap();
            sock.flush().await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await; // keep-alive: never closes
        });
        let resp = tokio::time::timeout(
            Duration::from_secs(3),
            http_post(&format!("http://{addr}/v1/commands"), "tok", b"{}"),
        )
        .await
        .expect("http_post must not wait for EOF once the declared body is complete")
        .unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"ok");
    }

    // P3-432: RFC 6455 requires the minimal length encoding. A frame that declares the
    // 8-byte form for a payload that fits in 125 bytes is malformed and must be refused.
    #[tokio::test]
    async fn ws_frame_length_must_use_the_minimal_encoding() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            // FIN + text, unmasked (server->client), 64-bit length for a 4-byte payload.
            let frame = [0x81u8, 127, 0, 0, 0, 0, 0, 0, 0, 4, b't', b'e', b's', b't'];
            sock.write_all(&frame).await.unwrap();
            sock.flush().await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(3), read_ws_frame(&mut stream)).await;
        assert!(
            result.is_ok() && result.unwrap().is_err(),
            "a non-minimally-encoded frame must be refused"
        );
    }

    // P3-433: a bare 101 is not an upgrade. (The first assertion is the finding's red; the
    // rest pin the header handling the fix adds. The accept-key value is checked for shape
    // only: verifying it needs SHA-1, which this crate does not depend on.)
    #[test]
    fn upgrade_reply_must_be_a_websocket_upgrade() {
        let bare = b"HTTP/1.1 101 Switching Protocols\r\n\r\n";
        assert!(
            check_upgrade_reply(bare).is_err(),
            "a bare 101 that is not an upgrade must be refused"
        );
        let wrong_upgrade =
            b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: h2c\r\nConnection: Upgrade\r\n\r\n";
        assert!(
            check_upgrade_reply(wrong_upgrade).is_err(),
            "Upgrade: h2c is not a websocket upgrade"
        );
        let bad_key = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: short\r\n\r\n";
        assert!(
            check_upgrade_reply(bad_key).is_err(),
            "a malformed accept key must be refused"
        );
        let good = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n";
        assert!(
            check_upgrade_reply(good).is_ok(),
            "a well-formed upgrade reply must be accepted"
        );
        let lower = b"HTTP/1.1 101 Switching Protocols\r\nupgrade: WebSocket\r\nconnection: keep-alive, Upgrade\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n";
        assert!(
            check_upgrade_reply(lower).is_ok(),
            "header names and tokens are case-insensitive: {:?}",
            check_upgrade_reply(lower)
        );
    }

    /// Reads an HTTP head (up to CRLFCRLF) from the raw socket — used by the in-test server.
    async fn read_http_head(stream: &mut TcpStream) -> String {
        let mut buf = Vec::new();
        let mut byte = [0u8; 1];
        while !buf.windows(4).any(|w| w == b"\r\n\r\n") && buf.len() < 16_384 {
            if stream.read(&mut byte).await.unwrap() == 0 {
                break;
            }
            buf.push(byte[0]);
        }
        String::from_utf8_lossy(&buf).into_owned()
    }

    /// Writes a server→client frame (never masked, per RFC 6455).
    async fn write_server_frame(stream: &mut TcpStream, opcode: u8, payload: &[u8]) -> Result<()> {
        let mut out = vec![0x80 | opcode];
        match payload.len() {
            0..=125 => out.push(payload.len() as u8),
            126..=0xFFFF => {
                out.push(126);
                out.extend_from_slice(&(payload.len() as u16).to_be_bytes());
            }
            _ => {
                out.push(127);
                out.extend_from_slice(&(payload.len() as u64).to_be_bytes());
            }
        }
        out.extend_from_slice(payload);
        stream.write_all(&out).await?;
        Ok(())
    }

    fn ws_report(ref_id: &str, report_type: &str, status: &str) -> String {
        serde_json::json!({
            "record_type": RECORD_REPORT,
            "contract_version": PROTOCOL_VERSION,
            "client_order_ref": ref_id,
            "broker_order_id": format!("BROKER-{ref_id}"),
            "order_status": status,
            "report_type": report_type,
            "fill_price": "120.5",
            "fill_quantity": "10",
            "instrument_token": "26000",
        })
        .to_string()
    }

    #[tokio::test]
    async fn intake_delivers_report_envelope_over_ws() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let head = read_http_head(&mut stream).await;
            assert!(
                head.starts_with("GET /v1/events HTTP/1.1"),
                "upgrade path: {head}"
            );
            assert!(head.contains("Upgrade: websocket"));
            assert!(head.contains("Connection: Upgrade"));
            assert!(head.contains("Sec-WebSocket-Version: 13"));
            assert!(
                head.contains("Authorization: Bearer tok_123"),
                "bearer auth on ws"
            );
            assert!(
                !head.contains("\r\nOrigin:"),
                "CheckOrigin forbids Origin header"
            );
            stream.write_all(WS_101).await.unwrap();
            write_server_frame(
                &mut stream,
                OP_TEXT,
                ws_report("REF-1", "order_filled", "FILLED").as_bytes(),
            )
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
        });

        let mut client =
            HttpBridgeClient::new(format!("http://127.0.0.1:{port}"), "tok_123".to_string());
        let mut rx = client.take_reports().expect("report stream");
        let envelope = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("timeout waiting for report")
            .expect("report stream closed unexpectedly");
        assert_eq!(envelope.client_order_ref, "REF-1");
        assert_eq!(envelope.report_type.as_deref(), Some("order_filled"));
        assert_eq!(envelope.order_status.as_deref(), Some("FILLED"));
        assert_eq!(envelope.fill_quantity.as_deref(), Some("10"));
        assert_eq!(envelope.broker_order_id, "BROKER-REF-1");
        let _ = client.disconnect().await;
        server.await.unwrap();
    }

    #[tokio::test]
    async fn intake_reconnects_after_server_close() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            // Lifeline 1: deliver one fill, then slam the door.
            let (mut s1, _) = listener.accept().await.unwrap();
            read_http_head(&mut s1).await;
            s1.write_all(WS_101).await.unwrap();
            write_server_frame(
                &mut s1,
                OP_TEXT,
                ws_report("REF-A", "order_filled", "FILLED").as_bytes(),
            )
            .await
            .unwrap();
            drop(s1);
            // Lifeline 2: the client must come back with a fresh handshake.
            let (mut s2, _) = listener.accept().await.unwrap();
            let head2 = read_http_head(&mut s2).await;
            assert!(
                head2.starts_with("GET /v1/events HTTP/1.1"),
                "reconnect handshake: {head2}"
            );
            s2.write_all(WS_101).await.unwrap();
            write_server_frame(
                &mut s2,
                OP_TEXT,
                ws_report("REF-B", "order_canceled", "CANCELED").as_bytes(),
            )
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
        });

        let mut client =
            HttpBridgeClient::new(format!("http://127.0.0.1:{port}"), "tok_123".to_string())
                .with_reconnect(Duration::from_millis(25), Duration::from_millis(100));
        let mut rx = client.take_reports().expect("report stream");
        let a = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("timeout waiting for REF-A")
            .expect("stream closed");
        assert_eq!(a.client_order_ref, "REF-A");
        assert_eq!(a.order_status.as_deref(), Some("FILLED"));
        let b = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("timeout waiting for REF-B after reconnect")
            .expect("stream closed");
        assert_eq!(b.client_order_ref, "REF-B");
        assert_eq!(b.order_status.as_deref(), Some("CANCELED"));
        // No duplicate deliveries after the reconnect.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            rx.try_recv().is_err(),
            "no duplicate envelopes after reconnect"
        );
        let _ = client.disconnect().await;
        server.await.unwrap();
    }

    #[tokio::test]
    async fn intake_answers_server_ping_with_pong() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            read_http_head(&mut stream).await;
            stream.write_all(WS_101).await.unwrap();
            // The Go bridge pings every 20 s; the client must answer with a masked pong.
            write_server_frame(&mut stream, OP_PING, b"hb")
                .await
                .unwrap();
            let pong = read_ws_frame(&mut stream).await.unwrap();
            assert_eq!(pong.opcode, OP_PONG, "client must answer ping with pong");
            assert_eq!(pong.payload, b"hb");
            write_server_frame(
                &mut stream,
                OP_TEXT,
                ws_report("REF-P", "order_filled", "FILLED").as_bytes(),
            )
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
        });

        let mut client =
            HttpBridgeClient::new(format!("http://127.0.0.1:{port}"), "tok_123".to_string());
        let mut rx = client.take_reports().expect("report stream");
        let envelope = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("timeout waiting for fill after ping/pong")
            .expect("stream closed");
        assert_eq!(envelope.client_order_ref, "REF-P");
        let _ = client.disconnect().await;
        server.await.unwrap();
    }

    fn command_env() -> CommandEnvelope {
        CommandEnvelope {
            record_type: RECORD_COMMAND.to_string(),
            contract_version: PROTOCOL_VERSION,
            request_id: uuid::Uuid::new_v4().to_string(),
            command: Command::Place.as_str().to_string(),
            instruction_id: "ins-1".into(),
            execution_attempt_id: "a-1".into(),
            client_order_ref: "COREF00000001".into(),
            broker_order_id: String::new(),
            order: Some(
                OrderCommand::new("NSE", "RELIANCE")
                    .with_quantity("10")
                    .with_side(TransactionType::Buy)
                    .with_order_type(OrderType::Mkt)
                    .with_product(Product::Cash)
                    .with_validity(Validity::Day),
            ),
        }
    }

    /// In-process Go-bridge-compatible command endpoint used by the round-trip tests.
    async fn serve_bridge(addr: &str, token: String) -> tokio::task::JoinHandle<()> {
        let listener = TcpListener::bind(addr).await.unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    continue;
                };
                tokio::spawn(handle_conn(stream, token.clone()));
            }
        })
    }

    async fn handle_conn(mut stream: TcpStream, token: String) {
        // Read request: headers + Content-Length body.
        let mut buf = Vec::new();
        let mut chunk = [0u8; 2048];
        loop {
            match stream.read(&mut chunk).await {
                Ok(0) => break,
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        let head_str = String::from_utf8_lossy(&buf);
        let authorized = head_str.contains(&format!("Bearer {token}"));

        let mut body = Vec::new();
        if let Some(idx) = head_str.find("\r\n\r\n") {
            let header_block = &head_str[..idx];
            let clen: usize = header_block
                .lines()
                .find_map(|l| {
                    let mut it = l.splitn(2, ':');
                    (it.next()?.trim().eq_ignore_ascii_case("content-length"))
                        .then(|| it.next()?.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            body = buf[idx + 4..]
                .get(..clen.min(buf.len() - idx - 4))
                .unwrap_or(&[])
                .to_vec();
        }

        let response: Vec<u8> = if !authorized {
            b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n".to_vec()
        } else {
            let cmd = match serde_json::from_slice::<CommandEnvelope>(&body) {
                Ok(cmd) => cmd,
                Err(_) => {
                    let bad = b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n".to_vec();
                    let _ = stream.write_all(&bad).await;
                    return;
                }
            };
            let report = ReportEnvelope {
                record_type: RECORD_REPORT.to_string(),
                contract_version: PROTOCOL_VERSION,
                request_id: cmd.request_id.clone(),
                command: cmd.command.clone(),
                outcome: ReportOutcome::Success.as_str().to_string(),
                reason: String::new(),
                instruction_id: cmd.instruction_id.clone(),
                execution_attempt_id: cmd.execution_attempt_id.clone(),
                client_order_ref: cmd.client_order_ref.clone(),
                broker_order_id: format!("FAKE-{}", cmd.client_order_ref),
                exchange_order_id: String::new(),
                postback_event_id: String::new(),
                order_status: Some("ACCEPTED".into()),
                report_type: None,
                fill_shares: String::new(),
                average_price: String::new(),
                fill_price: None,
                fill_quantity: None,
                fill_time: String::new(),
                instrument_token: String::new(),
                received_ts_ms: 0,
                response_fingerprint: String::new(),
                data: None,
            };
            let json = serde_json::to_vec(&report).unwrap();
            let mut out = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                json.len()
            )
            .into_bytes();
            out.extend_from_slice(&json);
            out
        };
        let _ = stream.write_all(&response).await;
        let _ = stream.flush().await;
    }

    #[tokio::test]
    async fn send_command_round_trip_accepted() {
        let _l = serve_bridge("127.0.0.1:18787", "s3cret".into()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut client = HttpBridgeClient::new("http://127.0.0.1:18787".into(), "s3cret".into());
        let report = client.send_command(command_env()).await.unwrap();
        assert_eq!(report.outcome, ReportOutcome::Success.as_str());
        assert_eq!(report.order_status.as_deref(), Some("ACCEPTED"));
        assert!(report.broker_order_id.starts_with("FAKE-"));
        assert_eq!(report.client_order_ref, "COREF00000001");
    }

    #[tokio::test]
    async fn send_command_rejects_unauthorized() {
        let _l = serve_bridge("127.0.0.1:18788", "s3cret".into()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut client = HttpBridgeClient::new("http://127.0.0.1:18788".into(), "wrong".into());
        let err = client.send_command(command_env()).await.unwrap_err();
        assert!(err.to_string().contains("401"), "got: {err}");
    }

    #[test]
    fn parses_url_without_port_defaults_8787() {
        let (host, port, path) = parse_url("http://127.0.0.1/v1/commands").unwrap();
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 8787);
        assert_eq!(path, "/v1/commands");
    }

    #[test]
    fn parses_url_with_port() {
        let (host, port, path) = parse_url("http://localhost:9999/healthz").unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(port, 9999);
        assert_eq!(path, "/healthz");
    }

    #[test]
    fn parses_chunked_body() {
        let data = b"7\r\nMozilla\r\n9\r\nDeveloper\r\n7\r\nNetwork\r\n0\r\n\r\n";
        let out = parse_chunked_body(data).unwrap();
        assert_eq!(out, b"MozillaDeveloperNetwork");
    }
}
