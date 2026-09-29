//! H2-5/D2 (CHG-334): durable gate-transition reports to the execution gateway.
//!
//! The executor owns the gate **decision** — its DEC-044 envelope is verified on `/v1/approve`
//! before anything here runs. The gateway owns the durable **record** (`Execution_Gate` in
//! Fluss, written through the production `FlussGateStateStore`). This module sends the signed
//! `GATE_REPORT` envelope to `POST {gateway}/v1/gate` and parses the persisted row the executor
//! adopts, so the durable row, the epoch the gateway's forward leg sends, and the executor's
//! control epoch cannot drift.
//!
//! Durable-first: a failed report **refuses** the transition (the gate stays HALTED); it is
//! never completed locally first. A safety halt is the one exception in shape, not in
//! substance: the local halt is fail-safe on its own, and the report is retried in the
//! background — if it never lands, the lease expires and the gateway defers.
//!
//! Wire shape: the same `gateway_protocol` envelope as `/v1/events` (HMAC over the canonical
//! fields), `message_type = GATE_REPORT`; the payload carries the transition-specific fields
//! (`owner_instance_id`, `principal`, `evidence_hash`, `reason`, `lease_ms`).

use crate::bridge::transport::http_post;
use crate::gateway_protocol::{encode_envelope, sha256_hex, Envelope};
use std::sync::atomic::{AtomicU64, Ordering};

/// The message type the gateway's `/v1/gate` endpoint accepts (and checks).
pub const GATE_REPORT_MESSAGE_TYPE: &str = "GATE_REPORT";

/// The persisted row the gateway returns; the executor adopts these values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateAck {
    pub outcome: String,
    pub state: String,
    pub epoch: u64,
    pub fence_token: u64,
    pub owner_instance_id: Option<String>,
    pub lease_expires_ts: Option<i64>,
}

impl GateAck {
    pub fn is_enabled(&self) -> bool {
        self.state == "ENABLED"
    }

    pub fn is_halted(&self) -> bool {
        self.state == "HALTED"
    }

    fn parse(body: &[u8]) -> Result<Self, String> {
        let v: serde_json::Value = serde_json::from_slice(body)
            .map_err(|e| format!("gate report response is not JSON: {e}"))?;
        let outcome = v
            .get("outcome")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let state = v
            .get("state")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        if outcome.is_empty() || state.is_empty() {
            return Err("gate report response missing outcome/state".to_string());
        }
        Ok(Self {
            outcome,
            state,
            epoch: v.get("epoch").and_then(|x| x.as_u64()).unwrap_or(0),
            fence_token: v.get("fence_token").and_then(|x| x.as_u64()).unwrap_or(0),
            owner_instance_id: v
                .get("owner_instance_id")
                .and_then(|x| x.as_str())
                .map(str::to_string),
            lease_expires_ts: v.get("lease_expires_ts").and_then(|x| x.as_i64()),
        })
    }
}

/// Signs and posts gate reports; one instance per process, carrying the instance id that owns
/// the fence.
pub struct GateReporter {
    gateway_endpoint: String,
    shared_secret: String,
    protocol_version: String,
    partition: String,
    account_scope: String,
    instance_id: String,
    lease_ttl_ms: u64,
    renew_ms: u64,
    seq: AtomicU64,
}

impl std::fmt::Debug for GateReporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print the shared secret (same rule as ServiceConfig's manual Debug).
        f.debug_struct("GateReporter")
            .field("gateway_endpoint", &self.gateway_endpoint)
            .field("shared_secret", &"<redacted>")
            .field("protocol_version", &self.protocol_version)
            .field("partition", &self.partition)
            .field("account_scope", &self.account_scope)
            .field("instance_id", &self.instance_id)
            .field("lease_ttl_ms", &self.lease_ttl_ms)
            .field("renew_ms", &self.renew_ms)
            .finish()
    }
}

/// Why a gate report failed. `ScopeMismatch` is terminal: the gateway's identity differs from
/// the envelope's, and no retry can fix a configuration mismatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateReportError {
    /// The gateway answered 409 with `SCOPE_MISMATCH`: this executor's `ACCOUNT_SCOPE_ID` /
    /// `EXECUTION_PARTITION_ID` do not match the gateway's own identity.
    ScopeMismatch {
        account_scope: String,
        partition: String,
        body: String,
    },
    /// Any other failure (transport, non-2xx, unparseable ack) — retryable as before.
    Other(String),
}

impl std::fmt::Display for GateReportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ScopeMismatch {
                account_scope,
                partition,
                body,
            } => write!(
                f,
                "gateway /v1/gate responded 409 SCOPE_MISMATCH: account_scope_id={account_scope} \
                 execution_partition_id={partition}: {body}"
            ),
            Self::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for GateReportError {}

impl GateReporter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        gateway_endpoint: String,
        shared_secret: String,
        protocol_version: String,
        partition: String,
        account_scope: String,
        instance_id: String,
        lease_ttl_ms: u64,
        renew_ms: u64,
    ) -> Self {
        Self {
            gateway_endpoint,
            shared_secret,
            protocol_version,
            partition,
            account_scope,
            instance_id,
            lease_ttl_ms,
            renew_ms,
            seq: AtomicU64::new(0),
        }
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub fn lease_ttl_ms(&self) -> u64 {
        self.lease_ttl_ms
    }

    pub fn renew_ms(&self) -> u64 {
        self.renew_ms
    }

    /// One signed report. Any non-2xx is an error carrying the gateway's reason — a transition
    /// must never be reported as applied when it was not.
    async fn report(
        &self,
        transition: &str,
        epoch: u64,
        fence_token: u64,
        payload: &serde_json::Value,
    ) -> Result<GateAck, GateReportError> {
        if self.gateway_endpoint.trim().is_empty() {
            return Err(GateReportError::Other(
                "gate report disabled (no GATEWAY_ENDPOINT)".to_string(),
            ));
        }
        let payload_json = serde_json::to_string(payload).map_err(|e| {
            GateReportError::Other(format!("gate report payload serialization failed: {e}"))
        })?;
        let envelope = Envelope {
            protocol_version: self.protocol_version.clone(),
            message_type: GATE_REPORT_MESSAGE_TYPE.to_string(),
            request_id: format!(
                "gate-{}-{}",
                self.instance_id,
                self.seq.fetch_add(1, Ordering::Relaxed)
            ),
            account_scope_id: self.account_scope.clone(),
            execution_partition_id: self.partition.clone(),
            payload_hash: sha256_hex(payload_json.as_bytes()),
            gate_epoch: epoch as i64,
            // The envelope requires a non-blank fence; "0" means "unfenced" (boot/approve/halt).
            fence_token: fence_token.to_string(),
            deadline_epoch_ms: now_ms() + 60_000,
            payload: payload.clone(),
            authentication: String::new(),
        };
        let body = encode_envelope(&self.shared_secret, &envelope)
            .map_err(|e| GateReportError::Other(e.to_string()))?;
        let url = format!("{}/v1/gate", self.gateway_endpoint.trim_end_matches('/'));
        let resp = http_post(&url, "", body.as_bytes())
            .await
            .map_err(|e| GateReportError::Other(e.to_string()))?;
        let text = String::from_utf8_lossy(&resp.body)
            .trim()
            .chars()
            .take(200)
            .collect::<String>();
        // H3-1: the gateway's refusal of a foreign identity is named and terminal — retrying
        // cannot fix a configuration mismatch, so it must not look like a locked gateway.
        if resp.status == 409 && text.contains("SCOPE_MISMATCH") {
            return Err(GateReportError::ScopeMismatch {
                account_scope: self.account_scope.clone(),
                partition: self.partition.clone(),
                body: text,
            });
        }
        if !(200..300).contains(&resp.status) {
            return Err(GateReportError::Other(format!(
                "gateway /v1/gate responded {}: {}",
                resp.status, text
            )));
        }
        let ack = GateAck::parse(&resp.body).map_err(GateReportError::Other)?;
        tracing::info!(
            transition,
            outcome = %ack.outcome,
            state = %ack.state,
            epoch = ack.epoch,
            fence_token = ack.fence_token,
            "durable gate report applied"
        );
        Ok(ack)
    }

    /// Boot: the row must exist and be HALTED; the ack's epoch is the one this process adopts.
    /// H3-1: the error stays typed so the keeper can tell a 409 SCOPE_MISMATCH (terminal) from
    /// a locked gateway (retryable).
    pub async fn boot_halt(&self, epoch: u64) -> Result<GateAck, GateReportError> {
        self.report(
            "BOOT_HALT",
            epoch,
            0,
            &serde_json::json!({
                "transition": "BOOT_HALT",
                "owner_instance_id": self.instance_id,
                "reason": "executor boot",
            }),
        )
        .await
    }

    /// The approved transition: fence acquire + the sanctioned path + single-operator promotion.
    pub async fn approve(
        &self,
        epoch: u64,
        principal: &str,
        evidence: &str,
    ) -> Result<GateAck, String> {
        self.report(
            "APPROVE",
            epoch,
            0,
            &serde_json::json!({
                "transition": "APPROVE",
                "owner_instance_id": self.instance_id,
                "principal": principal,
                "evidence_hash": evidence,
                "lease_ms": self.lease_ttl_ms,
            }),
        )
        .await
        .map_err(|e| e.to_string())
    }

    /// A safety halt: unconditional on the gateway side (a delayed halt still fences a live gate).
    pub async fn halt(
        &self,
        epoch: u64,
        fence_token: u64,
        reason: &str,
        evidence: &str,
    ) -> Result<GateAck, String> {
        self.report(
            "HALT",
            epoch,
            fence_token,
            &serde_json::json!({
                "transition": "HALT",
                "owner_instance_id": self.instance_id,
                "reason": reason,
                "evidence_hash": evidence,
            }),
        )
        .await
        .map_err(|e| e.to_string())
    }

    /// Lease renewal: keeps the fence live while ENABLED. A conflict means the executor must halt.
    pub async fn renew(&self, epoch: u64, fence_token: u64) -> Result<GateAck, String> {
        self.report(
            "RENEW",
            epoch,
            fence_token,
            &serde_json::json!({
                "transition": "RENEW",
                "owner_instance_id": self.instance_id,
                "lease_ms": self.lease_ttl_ms,
            }),
        )
        .await
        .map_err(|e| e.to_string())
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// One-shot capture server: records the request, replies with the given status line + body.
    async fn capture_once(
        status: &'static str,
        reply_body: &'static str,
    ) -> (
        std::net::SocketAddr,
        tokio::sync::mpsc::UnboundedReceiver<String>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let handle = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            let mut expected: Option<usize> = None;
            loop {
                let n = tokio::time::timeout(std::time::Duration::from_secs(5), s.read(&mut chunk))
                    .await
                    .expect("timed out reading the request")
                    .unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
                if expected.is_none() {
                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&buf[..pos]).to_ascii_lowercase();
                        let len = head
                            .split("content-length:")
                            .nth(1)
                            .and_then(|v| v.trim().split(' ').next())
                            .and_then(|d| d.parse::<usize>().ok())
                            .unwrap_or(0);
                        expected = Some(pos + 4 + len);
                    }
                }
                if let Some(end) = expected {
                    if buf.len() >= end {
                        break;
                    }
                }
            }
            tx.send(String::from_utf8_lossy(&buf).to_string()).unwrap();
            let resp = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply_body}",
                reply_body.len()
            );
            s.write_all(resp.as_bytes()).await.unwrap();
        });
        (addr, rx, handle)
    }

    fn reporter(addr: std::net::SocketAddr) -> GateReporter {
        GateReporter::new(
            format!("http://{addr}"),
            "secret".to_string(),
            "execution-gateway.v1".to_string(),
            "dev-partition".to_string(),
            "dev-scope".to_string(),
            "exec-1".to_string(),
            30_000,
            10_000,
        )
    }

    #[test]
    fn ack_parse_reads_the_row_and_fails_closed_on_a_missing_state() {
        let ack = GateAck::parse(
            br#"{"outcome":"ENABLED","state":"ENABLED","epoch":3,"fence_token":7,
                 "owner_instance_id":"exec-1","lease_expires_ts":1234567890}"#,
        )
        .unwrap();
        assert!(ack.is_enabled());
        assert_eq!(ack.epoch, 3);
        assert_eq!(ack.fence_token, 7);
        assert_eq!(ack.owner_instance_id.as_deref(), Some("exec-1"));
        assert_eq!(ack.lease_expires_ts, Some(1234567890));

        assert!(GateAck::parse(br#"{"outcome":"X"}"#).is_err());
        assert!(GateAck::parse(b"not json").is_err());
    }

    #[tokio::test]
    async fn approve_posts_a_signed_gate_report_and_returns_the_persisted_row() {
        let (addr, mut rx, server) = capture_once(
            "200 OK",
            r#"{"outcome":"ENABLED","state":"ENABLED","epoch":3,"fence_token":7,"owner_instance_id":"exec-1","lease_expires_ts":1234567890}"#,
        )
        .await;
        let ack = reporter(addr)
            .approve(1, "saurabh", "ev-1")
            .await
            .expect("report succeeds");

        assert_eq!(ack.state, "ENABLED");
        assert_eq!(ack.epoch, 3);
        assert_eq!(ack.fence_token, 7);

        let request = rx.recv().await.expect("server got the request");
        assert!(
            request.starts_with("POST /v1/gate HTTP/1.1"),
            "request: {request}"
        );
        assert!(
            request.contains("\"message_type\":\"GATE_REPORT\""),
            "request: {request}"
        );
        assert!(
            request.contains("\"transition\":\"APPROVE\""),
            "request: {request}"
        );
        assert!(
            request.contains("\"principal\":\"saurabh\""),
            "request: {request}"
        );
        assert!(
            request.contains("\"evidence_hash\":\"ev-1\""),
            "request: {request}"
        );
        assert!(request.contains("\"lease_ms\":30000"), "request: {request}");
        assert!(
            request.contains("\"authentication\":\""),
            "request: {request}"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn a_non_2xx_report_is_an_error_carrying_the_gateway_reason() {
        let (addr, _rx, server) = capture_once(
            "409 Conflict",
            r#"{"error":"stale report: epoch 2 != durable 3","outcome":"EPOCH_MISMATCH"}"#,
        )
        .await;
        let err = reporter(addr)
            .approve(2, "saurabh", "ev-1")
            .await
            .unwrap_err();
        assert!(err.contains("409"), "err: {err}");
        assert!(err.contains("EPOCH_MISMATCH"), "err: {err}");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn a_409_scope_mismatch_is_a_named_non_retryable_error() {
        // H3-1: the keeper's decision is this classification — a locked gateway is `Other`
        // (retry), a foreign identity is `ScopeMismatch` (stop and stay unhydrated).
        let (addr, _rx, server) = capture_once(
            "409 Conflict",
            r#"{"error":"partition/scope mismatch","outcome":"SCOPE_MISMATCH"}"#,
        )
        .await;
        let err = reporter(addr)
            .boot_halt(1)
            .await
            .expect_err("409 must fail");
        match &err {
            GateReportError::ScopeMismatch {
                account_scope,
                partition,
                ..
            } => {
                assert_eq!(account_scope.as_str(), "dev-scope");
                assert_eq!(partition.as_str(), "dev-partition");
            }
            other => panic!("expected ScopeMismatch, got {other:?}"),
        }
        assert!(err.to_string().contains("409 SCOPE_MISMATCH"), "{err}");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn boot_halt_and_renew_carry_their_transition_fields() {
        let (addr, mut rx, server) = capture_once(
            "200 OK",
            r#"{"outcome":"HALTED","state":"HALTED","epoch":1,"fence_token":0}"#,
        )
        .await;
        let ack = reporter(addr).boot_halt(1).await.unwrap();
        assert!(ack.is_halted());
        assert_eq!(ack.epoch, 1);
        let request = rx.recv().await.unwrap();
        assert!(
            request.contains("\"transition\":\"BOOT_HALT\""),
            "request: {request}"
        );
        server.await.unwrap();

        let (addr, mut rx, server) = capture_once(
            "200 OK",
            r#"{"outcome":"RENEWED","state":"ENABLED","epoch":3,"fence_token":7,"owner_instance_id":"exec-1","lease_expires_ts":1234567890}"#,
        )
        .await;
        let ack = reporter(addr).renew(3, 7).await.unwrap();
        assert!(ack.is_enabled());
        assert_eq!(ack.fence_token, 7);
        let request = rx.recv().await.unwrap();
        assert!(
            request.contains("\"transition\":\"RENEW\""),
            "request: {request}"
        );
        assert!(
            request.contains("\"fence_token\":\"7\""),
            "request: {request}"
        );
        server.await.unwrap();
    }

    #[test]
    fn debug_never_prints_the_shared_secret() {
        let r = GateReporter::new(
            "http://gateway:9180".to_string(),
            "super-secret-value".to_string(),
            "execution-gateway.v1".to_string(),
            "p".to_string(),
            "a".to_string(),
            "exec-1".to_string(),
            30_000,
            10_000,
        );
        let text = format!("{r:?}");
        assert!(
            !text.contains("super-secret-value"),
            "debug leaked the secret: {text}"
        );
        assert!(text.contains("<redacted>"));
    }
}
