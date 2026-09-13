//! Minimal HTTP health + private gateway intent endpoint (WP-1 + T4).
//!
//! Serves `GET /healthz` (process alive + gate state) and `GET /readyz` (ready to accept
//! inbound; 503 while draining). And `POST /v1/intents` (private gateway envelope,
//! verified via `gateway_protocol` HMAC + payload hash, fail-closed while gate HALTED).
//!
//! `POST /v1/approve` + `POST /v1/halt` are the DEC-044 control plane (P3-020) and are
//! authenticated with the same [`gateway_protocol`] HMAC envelope the intent route uses — the
//! body is NOT a bare operator name. The operator mints (with the gateway shared secret) an
//! envelope whose SIGNED payload carries the identity and the evidence:
//!
//! ```text
//! { "protocol_version": <GATEWAY_PROTOCOL_VERSION>, "message_type": "GATE_APPROVE"|"GATE_HALT",
//!   "request_id": <unique>, "account_scope_id": <scope>, "execution_partition_id": <partition>,
//!   "payload_hash": hex(sha256(compact payload JSON)), "gate_epoch": <current, from /healthz>,
//!   "fence_token": <token>, "deadline_epoch_ms": <future epoch ms>,
//!   "payload": { "operator": <T9_APPROVED_BY>, "evidence": <evidence hash>, "reason": <halt only> },
//!   "authentication": hex(hmacSha256(secret, canonical)) }
//! ```
//!
//! Like the bridge transport it is a deliberately small `tokio` HTTP/1.1 server — no web-framework
//! dependency — because the surface is five small routes: two health reads, the authenticated
//! intent POST above, and the two authenticated control POSTs. Safety invariant: health never
//! implies ENABLED; intents never execute while HALTED (503); an unsigned, wrong-key, wrong-type
//! or stale-epoch control request performs no control action; and nothing here ever logs the
//! shared secret.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::bridge::{BridgeClient, ReportOutcome};
use crate::events;
use crate::gate::ExecState;
use crate::gateway_protocol;
use crate::intent;

/// Control-plane message type for `POST /v1/approve` (P3-020). `message_type` is part of the
/// signed canonical string, so it is the domain separator: a signature minted for an
/// `EXECUTION_INTENT` — or for the sibling control endpoint — cannot be replayed on this route.
const APPROVE_MESSAGE_TYPE: &str = "GATE_APPROVE";

/// Control-plane message type for `POST /v1/halt` (P3-020).
const HALT_MESSAGE_TYPE: &str = "GATE_HALT";

/// DEC-044 single operator (P3-020): the authorized identity when `T9_APPROVED_BY` — the same
/// env key the T9 placement gate uses — is not configured. The env value, when present, wins.
const DEFAULT_OPERATOR: &str = "saurabh";

/// Shared bridge transport used by the ENABLED sync forward (T4a). `tokio::sync::Mutex` is
/// required because `BridgeClient::send_command` takes `&mut self` across an await point; the
/// `Send` bound is required by the spawned server task (the trait object is used only here,
/// never by the non-`Send` nautilus runtime).
pub type BridgeForwarder = Arc<tokio::sync::Mutex<Box<dyn BridgeClient + Send>>>;

/// Point-in-time health snapshot shared between the server and the shutdown path.
#[derive(Clone)]
pub struct ServerState {
    inner: Arc<Mutex<Snapshot>>,
    forwarder: Option<BridgeForwarder>,
}

impl std::fmt::Debug for ServerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerState")
            .field("inner", &self.inner)
            .field("forwarder", &self.forwarder.is_some())
            .finish()
    }
}

#[derive(Debug)]
struct Snapshot {
    gate: ExecState,
    process_alive: bool,
    draining: bool,
    shared_secret: String,
    protocol_version: String,
    gateway_endpoint: String,
    /// First unresolved UNKNOWN outcome (WP-2 §UNKNOWN): arms the 15 s escalation.
    unknown_first_seen: Option<std::time::Instant>,
    /// Escalation window; production pins 15 s (dossier), tests shrink it.
    unknown_escalation: std::time::Duration,
    /// Set once the escalation fires: an operator must review before any re-enable.
    operator_review_required: bool,
    /// DEC-044 single-operator approval: the sanctioned approver (T9_APPROVED_BY).
    approved_by: Option<String>,
    /// Evidence hash bound to the approval (fail-closed: re-enable always re-approves).
    enabled_evidence: Option<String>,
    /// P3-020: the authorized DEC-044 operator identity, resolved once at construction from
    /// `T9_APPROVED_BY` (default `saurabh`). A signed control request naming any other identity
    /// is refused before any control action happens.
    authorized_operator: String,
    /// P3-020: the current control-plane epoch, exposed as `gate_epoch` on `/healthz` and inside
    /// the signed control envelope. It starts at 1 (0 is never current) and every gate transition
    /// bumps it, so an envelope captured before the transition names a stale epoch and is refused.
    control_epoch: u64,
}

impl ServerState {
    /// A fresh server state: process alive, gate as given, not draining, no gateway auth.
    pub fn new(gate: ExecState) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Snapshot {
                gate,
                process_alive: true,
                draining: false,
                shared_secret: String::new(),
                protocol_version: "execution-gateway.v1".into(),
                gateway_endpoint: String::new(),
                unknown_first_seen: None,
                unknown_escalation: Self::UNKNOWN_ESCALATION,
                operator_review_required: false,
                approved_by: None,
                enabled_evidence: None,
                authorized_operator: Self::operator_from_env(),
                control_epoch: 1,
            })),
            forwarder: None,
        }
    }

    /// Production escalation window for an unresolved UNKNOWN outcome (dossier
    /// WP-2 §UNKNOWN: 15 s global halt timer + operator review hook).
    pub const UNKNOWN_ESCALATION: std::time::Duration = std::time::Duration::from_secs(15);

    /// P3-020: the authorized operator identity — `T9_APPROVED_BY` when it is set and non-blank,
    /// otherwise the DEC-044 default. Resolved once per `ServerState` so a request can never be
    /// judged against an identity that changed mid-flight.
    fn operator_from_env() -> String {
        std::env::var("T9_APPROVED_BY")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| DEFAULT_OPERATOR.to_string())
    }

    /// With private gateway auth (shared secret + expected protocol version) for POST /v1/intents.
    pub fn with_gateway_auth(
        gate: ExecState,
        shared_secret: String,
        protocol_version: String,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Snapshot {
                gate,
                process_alive: true,
                draining: false,
                shared_secret,
                protocol_version,
                gateway_endpoint: String::new(),
                unknown_first_seen: None,
                unknown_escalation: Self::UNKNOWN_ESCALATION,
                operator_review_required: false,
                approved_by: None,
                enabled_evidence: None,
                authorized_operator: Self::operator_from_env(),
                control_epoch: 1,
            })),
            forwarder: None,
        }
    }

    /// Pins the authorized operator (tests only — production resolves it from `T9_APPROVED_BY`
    /// at construction, so a test never depends on ambient process env).
    #[cfg(test)]
    fn with_authorized_operator(self, operator: &str) -> Self {
        if let Ok(mut s) = self.inner.lock() {
            s.authorized_operator = operator.to_string();
        }
        self
    }

    /// Shrinks the UNKNOWN escalation window (tests only — production keeps 15 s).
    #[cfg(test)]
    fn with_unknown_escalation(self, d: std::time::Duration) -> Self {
        if let Ok(mut s) = self.inner.lock() {
            s.unknown_escalation = d;
        }
        self
    }

    /// Attaches the bridge transport for the ENABLED sync forward (T4a). When absent the
    /// ENABLED path returns the paper 202 ack without executing (offline/test construction).
    #[must_use]
    pub fn with_forwarder(mut self, forwarder: BridgeForwarder) -> Self {
        self.forwarder = Some(forwarder);
        self
    }

    /// Sets the gateway base URL for normalized event emission (A2.4 leg). Empty (default)
    /// disables emission — the 202 then reports `event_emission: disabled`.
    #[must_use]
    pub fn with_gateway_endpoint(self, gateway_endpoint: String) -> Self {
        if let Ok(mut s) = self.inner.lock() {
            s.gateway_endpoint = gateway_endpoint;
        }
        self
    }

    /// Marks the service as draining (shutdown in progress); `/readyz` then returns 503.
    pub fn set_draining(&self, draining: bool) {
        if let Ok(mut s) = self.inner.lock() {
            s.draining = draining;
        }
    }

    /// The sanctioned DEC-044 approval: advances the gate to `ENABLED` only when the
    /// approver is the authorized single operator (`T9_APPROVED_BY`, default `saurabh`) and an
    /// evidence hash is supplied. Fail-closed: any other approver, a missing evidence hash, or an
    /// operator-review flag already raised → refused (gate stays HALTED).
    ///
    /// Mirrors `gate.rs::record_approval` + `enable` semantics for the runtime snapshot
    /// (the full `Gate` state machine is exercised in tests; the server surface is
    /// HALTED-until-sanctioned-approval). Callers on the HTTP surface pass an identity that
    /// `verify_control` already checked against the signature-covered payload.
    pub fn approve(&self, approver: &str, evidence_hash: &str) -> Result<(), String> {
        let mut s = match self.inner.lock() {
            Ok(s) => s,
            Err(_) => return Err("snapshot lock poisoned".to_string()),
        };
        if approver.is_empty() || approver != s.authorized_operator {
            return Err(format!(
                "approver {approver:?} is not the authorized operator"
            ));
        }
        if evidence_hash.is_empty() {
            return Err("evidence hash required".to_string());
        }
        if s.operator_review_required {
            return Err("operator review required — re-approval forbidden".to_string());
        }
        // Fail-closed: only HALTED can be advanced (a gate already ENABLED stays; a
        // RECONCILING/APPROVAL_PENDING snapshot is not part of the server surface).
        if s.gate == ExecState::Enabled {
            return Ok(()); // idempotent — already approved
        }
        if s.gate != ExecState::Halted {
            return Err(format!("cannot approve from {}", s.gate.as_str()));
        }
        s.gate = ExecState::Enabled;
        // P3-019: a new approval epoch re-arms the UNKNOWN watchdog — without this,
        // `unknown_first_seen` from the previous epoch makes `record_unknown_outcome`
        // early-return forever and the escalation silently stops working.
        s.unknown_first_seen = None;
        // P3-020: spend the epoch the envelope named. A copy of that approve envelope now
        // names a stale epoch and is refused instead of re-enabling the gate.
        s.bump_control_epoch();
        s.approved_by = Some(approver.to_string());
        s.enabled_evidence = Some(evidence_hash.to_string());
        tracing::info!(
            "DEC-044 approval recorded: approver={approver} evidence={evidence_hash} gate=ENABLED"
        );
        Ok(())
    }

    /// The sanctioned safety halt: returns the gate to `HALTED` from any state and
    /// invalidates the approval + bound evidence (fail-closed — re-enable re-approves).
    pub fn safety_halt(&self, reason: &str) {
        if let Ok(mut s) = self.inner.lock() {
            s.gate = ExecState::Halted;
            s.approved_by = None;
            s.enabled_evidence = None;
            s.operator_review_required = false;
            // P3-020: a halt is a transition — envelopes minted before it are spent.
            s.bump_control_epoch();
            tracing::warn!("gate safety-halted: {reason}");
        }
    }

    fn snapshot(&self) -> Snapshot {
        self.inner.lock().map(|s| s.clone()).unwrap_or(Snapshot {
            gate: ExecState::Halted,
            process_alive: false,
            draining: true,
            shared_secret: String::new(),
            protocol_version: "execution-gateway.v1".into(),
            gateway_endpoint: String::new(),
            unknown_first_seen: None,
            unknown_escalation: Self::UNKNOWN_ESCALATION,
            operator_review_required: true,
            approved_by: None,
            enabled_evidence: None,
            authorized_operator: Self::operator_from_env(),
            control_epoch: 1,
        })
    }

    /// Records the first unresolved UNKNOWN outcome and arms its one-shot escalation
    /// task (WP-2 §UNKNOWN: never retried; after 15 s force-HALT + operator review).
    fn record_unknown_outcome(&self) {
        let escalation = {
            let mut s = match self.inner.lock() {
                Ok(s) => s,
                Err(_) => return,
            };
            if s.unknown_first_seen.is_some() || s.operator_review_required {
                return; // already armed / already escalated
            }
            s.unknown_first_seen = Some(std::time::Instant::now());
            s.unknown_escalation
        };
        let st = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(escalation).await;
            st.escalate_unknown_review();
        });
    }

    /// Escalation hook: force the gate back to HALTED (safety halt from any state)
    /// and raise the operator-review flag. Idempotent; a no-op when the outcome was
    /// resolved (first-seen cleared) or already escalated.
    fn escalate_unknown_review(&self) {
        let mut s = match self.inner.lock() {
            Ok(s) => s,
            Err(_) => return,
        };
        let Some(t0) = s.unknown_first_seen else {
            return;
        };
        if t0.elapsed() < s.unknown_escalation || s.operator_review_required {
            return;
        }
        if s.gate != ExecState::Halted {
            // Forced safety halt from any state (gate invariant: uncertain → HALTED).
            s.gate = ExecState::Halted;
            // P3-020: a forced halt is a transition — spend the epoch too, so no envelope
            // minted while the gate was ENABLED can be presented afterwards.
            s.bump_control_epoch();
        }
        s.operator_review_required = true;
        tracing::error!(
            "UNKNOWN bridge outcome unresolved for >= {:?}: gate force-HALTED, operator review required before re-enable",
            s.unknown_escalation
        );
    }
}

impl Snapshot {
    /// P3-020: spends the current control-plane epoch. Called on every gate transition, so a
    /// signed control envelope (which carries the epoch it was minted for) is single-shot: after
    /// the transition it names a stale epoch and `verify_control` refuses it. Cheap and
    /// unbounded-in-theory (2^64 transitions) rather than a nonce store.
    fn bump_control_epoch(&mut self) {
        self.control_epoch += 1;
    }
}

impl Clone for Snapshot {
    fn clone(&self) -> Self {
        Self {
            gate: self.gate,
            process_alive: self.process_alive,
            draining: self.draining,
            shared_secret: self.shared_secret.clone(),
            protocol_version: self.protocol_version.clone(),
            gateway_endpoint: self.gateway_endpoint.clone(),
            unknown_first_seen: self.unknown_first_seen,
            unknown_escalation: self.unknown_escalation,
            operator_review_required: self.operator_review_required,
            approved_by: self.approved_by.clone(),
            enabled_evidence: self.enabled_evidence.clone(),
            authorized_operator: self.authorized_operator.clone(),
            control_epoch: self.control_epoch,
        }
    }
}

/// Health document for `/healthz`.
pub fn health_json(state: &ServerState) -> serde_json::Value {
    let s = state.snapshot();
    let trading_ready = s.gate == ExecState::Enabled;
    serde_json::json!({
        "service": "nautilus-execution-service",
        "process_alive": s.process_alive,
        "gate_state": s.gate.as_str(),
        "trading_ready": trading_ready,
        "draining": s.draining,
        "enabled": s.gate == ExecState::Enabled,
        "operator_review_required": s.operator_review_required,
        "approved_by": s.approved_by,
        "enabled_evidence": s.enabled_evidence,
        // P3-020: the epoch a signed approve/halt envelope must name (see the module doc).
        "gate_epoch": s.control_epoch,
    })
}

fn text(status: u16, body: &str) -> Vec<u8> {
    let mut out = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    out.extend_from_slice(body.as_bytes());
    out
}

fn json(status: u16, v: &serde_json::Value) -> Vec<u8> {
    let body = v.to_string();
    let mut out = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    out.extend_from_slice(body.as_bytes());
    out
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// P3-454: an accepted `gateway_protocol::verify` that carries no decoded envelope is a contract
/// violation on the sibling crate's side (a change there cannot be seen from here). It must not
/// panic the connection task — that would drop the response entirely — so the request boundary
/// turns it into a clean 500 the operator can see.
fn envelope_or_500(
    ver: gateway_protocol::Verification,
) -> Result<gateway_protocol::Envelope, Vec<u8>> {
    match ver.envelope {
        Some(envelope) => Ok(envelope),
        None => Err(json(
            500,
            &serde_json::json!({
                "accepted": false,
                "reason": "internal error: verified envelope missing",
            }),
        )),
    }
}

/// A verified DEC-044 control request (P3-020), decoded from the signed envelope's payload.
struct ControlRequest {
    operator: String,
    evidence: String,
    reason: String,
}

/// Reads a trimmed string field from a signed control payload; absent, wrong-typed or blank → "".
fn payload_str(payload: &serde_json::Value, field: &str) -> String {
    payload
        .get(field)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string()
}

/// P3-020: verify a control-plane request with the same `gateway_protocol` HMAC path
/// `/v1/intents` uses, and decode the identity/evidence the signature covers. Fail closed — every
/// rejection answers the caller and performs NO control action:
///
/// - secret, protocol version, required identity fields, deadline and MAC (inside `verify`);
/// - `message_type` must be this route's type — the type is a field of the signed canonical, so
///   an intent signature (or the sibling control endpoint's) cannot be replayed here;
/// - `gate_epoch` must equal the current control epoch — a captured envelope names a stale one;
/// - `payload.operator` must be the configured operator, and `payload.evidence` must be present.
///   Both are covered by the MAC: `payload_hash` and `payload_json` are canonical fields, so a
///   signature cannot be lifted onto a payload with different content.
fn verify_control(
    body: &str,
    snap: &Snapshot,
    expected_message_type: &str,
    now_ms: i64,
) -> Result<ControlRequest, Vec<u8>> {
    let ver = gateway_protocol::verify(body, &snap.shared_secret, &snap.protocol_version, now_ms);
    if !ver.accepted {
        // Same fail-closed shape as /v1/intents: 401 for auth/hash/version/deadline.
        return Err(json(
            401,
            &serde_json::json!({ "accepted": false, "reason": ver.reason }),
        ));
    }
    let envelope = envelope_or_500(ver)?;
    if envelope.message_type != expected_message_type {
        return Err(json(
            401,
            &serde_json::json!({
                "accepted": false,
                "reason": "control message type not accepted on this route",
            }),
        ));
    }
    if envelope.gate_epoch != snap.control_epoch as i64 {
        // Stale (or forged) epoch: the envelope was not minted for the current gate epoch.
        return Err(json(
            401,
            &serde_json::json!({
                "accepted": false,
                "reason": "stale gate epoch",
                "gate_epoch": snap.control_epoch,
            }),
        ));
    }
    let operator = payload_str(&envelope.payload, "operator");
    let evidence = payload_str(&envelope.payload, "evidence");
    if operator.is_empty() || evidence.is_empty() {
        return Err(json(
            401,
            &serde_json::json!({
                "accepted": false,
                "reason": "payload operator and evidence required",
            }),
        ));
    }
    if operator != snap.authorized_operator {
        return Err(json(
            403,
            &serde_json::json!({
                "accepted": false,
                "reason": format!("operator {operator:?} is not the authorized operator"),
            }),
        ));
    }
    Ok(ControlRequest {
        operator,
        evidence,
        reason: payload_str(&envelope.payload, "reason"),
    })
}

async fn route(state: &ServerState, method: &str, path: &str, body: &str) -> Vec<u8> {
    match (method, path) {
        ("GET", "/healthz") => json(200, &health_json(state)),
        ("GET", "/readyz") => {
            let s = state.snapshot();
            if s.draining {
                json(
                    503,
                    &serde_json::json!({ "ready": false, "draining": true }),
                )
            } else {
                json(
                    200,
                    &serde_json::json!({ "ready": true, "gate_state": s.gate.as_str() }),
                )
            }
        }
        ("POST", "/v1/intents") => {
            let snap = state.snapshot();
            // Never log the secret.
            let ver = gateway_protocol::verify(
                body,
                &snap.shared_secret,
                &snap.protocol_version,
                now_ms(),
            );
            if !ver.accepted {
                let doc = serde_json::json!({ "accepted": false, "reason": ver.reason });
                // 401 for auth/hash/version, as the gateway protocol defines.
                return json(401, &doc);
            }
            // Fail closed while HALTED — no execution, but prove the private route is wired.
            if snap.gate != ExecState::Enabled {
                let doc = serde_json::json!({
                    "accepted": false,
                    "reason": "gate HALTED",
                    "gate_state": snap.gate.as_str(),
                });
                return json(503, &doc);
            }
            // Gate ENABLED + envelope accepted — T4a sync leg: forward the order to the
            // bridge transport (Fake locally, Http in production) and map the synchronous
            // report. Fail-closed mapping: SUCCESS → 202, REJECTED → 409, UNKNOWN/transport
            // error → 503 (never an ack, never a retry — a lost ack is reconciled by query).
            let Some(forwarder) = &state.forwarder else {
                return json(
                    202,
                    &serde_json::json!({
                        "accepted": true,
                        "gate_state": snap.gate.as_str(),
                        "reason": "no bridge transport configured (paper ack)",
                    }),
                );
            };
            // P3-454: a guarded decode — no `expect` in the request path.
            let envelope = match envelope_or_500(ver) {
                Ok(envelope) => envelope,
                Err(resp) => return resp,
            };
            // Optional `action` field: absent → "place" (back-compatible with the
            // pinned schema-v1 payload bytes); "cancel" → cancel mapping (requires
            // broker_order_id); "amend" → modify mapping (requires broker_order_id
            // + the amended order block). Anything else fails closed with 422.
            let action = envelope
                .payload
                .get("action")
                .and_then(|v| v.as_str())
                .unwrap_or("place")
                .to_ascii_lowercase();
            let cmd_env = match action.as_str() {
                "place" => intent::place_envelope_from_payload(&envelope.payload),
                "cancel" => intent::cancel_envelope_from_payload(&envelope.payload),
                "amend" => intent::amend_envelope_from_payload(&envelope.payload),
                other => Err(format!("unsupported action: {other}")),
            };
            let cmd_env = match cmd_env {
                Ok(p) => p,
                Err(reason) => {
                    return json(
                        422,
                        &serde_json::json!({
                            "accepted": false,
                            "reason": reason,
                            "gate_state": snap.gate.as_str(),
                        }),
                    );
                }
            };
            // Scope the bridge lock to the send only: a concurrent /v1/intents must not
            // serialize behind the gateway emission round-trip (a hung gateway would
            // otherwise stall every subsequent order submit).
            let submit = {
                let mut guard = forwarder.lock().await;
                guard.send_command(cmd_env.clone()).await
            };
            match submit {
                Ok(report) if report.is_success() => {
                    // A2.4 leg: emit the normalized lifecycle+correlation image to the
                    // gateway /v1/events intake. The order is already accepted — a failed
                    // emission never rewrites the 202, it is surfaced as event_emission.
                    let trade_context_id = envelope
                        .payload
                        .get("trade_context_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let now = now_ms();
                    let event = events::lifecycle_event_value(
                        &report,
                        &cmd_env,
                        &envelope.account_scope_id,
                        &envelope.execution_partition_id,
                        envelope.gate_epoch,
                        &trade_context_id,
                        now,
                    );
                    // Empty endpoint = emission disabled (offline/paper mode) — distinct
                    // from a real emission failure, which the operator must see.
                    let delivery = if snap.gateway_endpoint.is_empty() {
                        "disabled".to_string()
                    } else {
                        match events::emit_event(
                            &snap.gateway_endpoint,
                            &snap.shared_secret,
                            &snap.protocol_version,
                            &event,
                            now,
                        )
                        .await
                        {
                            Ok(()) => "accepted".to_string(),
                            Err(e) => format!("failed: {e}"),
                        }
                    };
                    json(
                        202,
                        &serde_json::json!({
                            "accepted": true,
                            "gate_state": snap.gate.as_str(),
                            "action": action,
                            "instruction_id": cmd_env.instruction_id,
                            "execution_attempt_id": cmd_env.execution_attempt_id,
                            "client_order_ref": cmd_env.client_order_ref,
                            "broker_order_id": report.broker_order_id,
                            "order_status": report.order_status,
                            "event_emission": delivery,
                        }),
                    )
                }
                Ok(report) if report.outcome() == Some(ReportOutcome::Rejected) => json(
                    409,
                    &serde_json::json!({
                        "accepted": false,
                        "outcome": "REJECTED",
                        "reason": report.reason,
                        "gate_state": snap.gate.as_str(),
                    }),
                ),
                Ok(report) => {
                    state.record_unknown_outcome();
                    json(
                        503,
                        &serde_json::json!({
                            "accepted": false,
                            "outcome": "UNKNOWN",
                            "reason": if report.reason.is_empty() {
                                "bridge UNKNOWN outcome".to_string()
                            } else {
                                report.reason
                            },
                            "gate_state": snap.gate.as_str(),
                        }),
                    )
                }
                Err(e) => {
                    state.record_unknown_outcome();
                    json(
                        503,
                        &serde_json::json!({
                            "accepted": false,
                            "outcome": "UNKNOWN",
                            "reason": e.to_string(),
                            "gate_state": snap.gate.as_str(),
                        }),
                    )
                }
            }
        }
        (m, "/healthz") | (m, "/readyz") if m != "GET" => text(405, "method_not_allowed"),
        (_, "/v1/intents") if method != "POST" => text(405, "method_not_allowed"),
        // Sanctioned DEC-044 approval + safety halt (A2.2/T9 operator surface).
        // P3-020: both are authenticated with the same `gateway_protocol` HMAC envelope the
        // private `/v1/intents` route uses — the operator identity and the evidence value live
        // INSIDE the signed payload (see the module doc for the wire shape), so an unsigned,
        // wrong-key, wrong-type or stale-epoch request never reaches a control action.
        ("POST", "/v1/approve") => {
            let snap = state.snapshot();
            let req = match verify_control(body, &snap, APPROVE_MESSAGE_TYPE, now_ms()) {
                Ok(req) => req,
                Err(resp) => return resp,
            };
            match state.approve(&req.operator, &req.evidence) {
                Ok(()) => json(
                    200,
                    &serde_json::json!({
                        "approved": true,
                        "gate_state": state.snapshot().gate.as_str(),
                        "approved_by": req.operator,
                        "gate_epoch": state.snapshot().control_epoch,
                    }),
                ),
                Err(reason) => json(
                    403,
                    &serde_json::json!({
                        "approved": false,
                        "reason": reason,
                        "gate_state": state.snapshot().gate.as_str(),
                    }),
                ),
            }
        }
        ("POST", "/v1/halt") => {
            let snap = state.snapshot();
            let req = match verify_control(body, &snap, HALT_MESSAGE_TYPE, now_ms()) {
                Ok(req) => req,
                Err(resp) => return resp,
            };
            // The signed `reason` is the operator's halt note; fall back to the evidence hash
            // so the audit line always names something the signature covers.
            let reason = if req.reason.is_empty() {
                req.evidence
            } else {
                req.reason
            };
            state.safety_halt(&reason);
            json(
                200,
                &serde_json::json!({
                    "halted": true,
                    "gate_state": state.snapshot().gate.as_str(),
                    "halted_by": req.operator,
                    "gate_epoch": state.snapshot().control_epoch,
                }),
            )
        }
        (_, "/v1/approve") | (_, "/v1/halt") if method != "POST" => text(405, "method_not_allowed"),
        _ => text(404, "not_found"),
    }
}

/// Binds `addr` and serves health/readiness + private intents until the process exits.
pub async fn serve(addr: SocketAddr, state: ServerState) -> Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind health server {addr}"))?;
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(x) => x,
            Err(_) => continue,
        };
        let st = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream, st).await {
                tracing::debug!("health connection closed: {e}");
            }
        });
    }
}

async fn handle_conn(mut stream: TcpStream, state: ServerState) -> Result<()> {
    // Read headers + optional body. We support Content-Length only (no chunked).
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        match stream.read(&mut chunk).await {
            Ok(0) => return Ok(()),
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if let Some(idx) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    break idx + 4;
                }
                if buf.len() > 64 * 1024 {
                    return Ok(());
                }
            }
            Err(_) => return Ok(()),
        }
    };
    let h_end = header_end;
    let header_str = String::from_utf8_lossy(&buf[..h_end]).to_string();
    let mut lines = header_str.lines();
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let raw_path = parts.next().unwrap_or("").to_string();
    let path = raw_path.split('?').next().unwrap_or("").to_string();

    // Parse Content-Length (case-insensitive).
    let mut content_length: usize = 0;
    for line in lines {
        if line.is_empty() {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            if k.trim().eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse::<usize>().unwrap_or(0);
                // Cap to 1 MiB to avoid abuse.
                if content_length > 1024 * 1024 {
                    let resp = text(413, "payload_too_large");
                    let _ = stream.write_all(&resp).await;
                    let _ = stream.flush().await;
                    return Ok(());
                }
            }
        }
    }

    // Body may already be partially in buf beyond header_end; read remainder.
    let mut body_bytes = buf[h_end..].to_vec();
    while body_bytes.len() < content_length {
        let need = content_length - body_bytes.len();
        let to_read = need.min(chunk.len());
        match stream.read(&mut chunk[..to_read]).await {
            Ok(0) => break,
            Ok(n) => body_bytes.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
    }
    body_bytes.truncate(content_length);
    let body = String::from_utf8_lossy(&body_bytes).to_string();

    let resp = route(&state, &method, &path, &body).await;
    let _ = stream.write_all(&resp).await;
    let _ = stream.flush().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::{CommandScript, FakeBridge};
    use crate::gate::ExecState;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn raw_request(addr: SocketAddr, request: &str) -> (u16, String) {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        let mut resp = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            match stream.read(&mut chunk).await {
                Ok(0) => break,
                Ok(n) => resp.extend_from_slice(&chunk[..n]),
                Err(_) => break,
            }
        }
        let s = String::from_utf8_lossy(&resp);
        let status = s
            .split_whitespace()
            .nth(1)
            .and_then(|x| x.parse::<u16>().ok())
            .unwrap_or(0);
        let body = s.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
        (status, body)
    }

    async fn raw_get(addr: SocketAddr, request: &str) -> (u16, String) {
        raw_request(addr, request).await
    }

    async fn spawn_server(state: ServerState) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    continue;
                };
                let st = state.clone();
                tokio::spawn(async move {
                    let _ = handle_conn(stream, st).await;
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn healthz_reports_halted_and_not_trading() {
        let state = ServerState::new(ExecState::Halted);
        let addr = spawn_server(state.clone()).await;
        let (status, body) = raw_get(addr, "GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(status, 200);
        assert!(body.contains("\"gate_state\":\"HALTED\""), "body: {body}");
        assert!(body.contains("\"trading_ready\":false"), "body: {body}");
        assert!(body.contains("\"enabled\":false"), "body: {body}");
    }

    #[tokio::test]
    async fn readyz_ok_while_running_503_while_draining() {
        let state = ServerState::new(ExecState::Halted);
        let addr = spawn_server(state.clone()).await;
        let (s1, b1) = raw_get(addr, "GET /readyz HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(s1, 200);
        assert!(b1.contains("\"ready\":true"), "body: {b1}");

        state.set_draining(true);
        let (s2, _) = raw_get(addr, "GET /readyz HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(s2, 503);
    }

    #[tokio::test]
    async fn unknown_path_404_and_non_get_405() {
        let state = ServerState::new(ExecState::Halted);
        let addr = spawn_server(state.clone()).await;
        let (s1, _) = raw_get(addr, "GET /nope HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(s1, 404);
        let (s2, _) = raw_get(addr, "POST /healthz HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(s2, 405);
    }

    #[tokio::test]
    async fn intents_requires_post() {
        let state = ServerState::new(ExecState::Halted);
        let addr = spawn_server(state.clone()).await;
        let (s, _) = raw_get(addr, "GET /v1/intents HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(s, 405);
    }

    #[tokio::test]
    async fn intents_rejects_bad_auth_while_halted() {
        let state = ServerState::with_gateway_auth(
            ExecState::Halted,
            "s3cr3t".into(),
            "execution-gateway.v1".into(),
        );
        let addr = spawn_server(state.clone()).await;
        let body = "{\"garbage\":1}";
        let req = format!(
            "POST /v1/intents HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let (s, b) = raw_request(addr, &req).await;
        assert_eq!(s, 401, "body: {b}");
        assert!(b.contains("accepted"), "body: {b}");
    }

    #[tokio::test]
    async fn intents_halted_returns_503_even_with_valid_envelope() {
        use crate::gateway_protocol::{encode_envelope, sha256_hex, Envelope};
        let payload = serde_json::json!({"instruction_id":"i1"});
        let payload_json = serde_json::to_string(&payload).unwrap();
        let hash = sha256_hex(payload_json.as_bytes());
        let env = Envelope {
            protocol_version: "execution-gateway.v1".into(),
            message_type: "EXECUTION_INTENT".into(),
            request_id: "req-1".into(),
            account_scope_id: "acc-1".into(),
            execution_partition_id: "part-1".into(),
            payload_hash: hash,
            gate_epoch: 1,
            fence_token: "fence-abc".into(),
            deadline_epoch_ms: 9_999_999_999_999,
            payload: payload.clone(),
            authentication: String::new(),
        };
        let encoded = encode_envelope("s3cr3t", &env).unwrap();
        let state = ServerState::with_gateway_auth(
            ExecState::Halted,
            "s3cr3t".into(),
            "execution-gateway.v1".into(),
        );
        let addr = spawn_server(state.clone()).await;
        let req = format!(
            "POST /v1/intents HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            encoded.len(),
            encoded
        );
        let (s, b) = raw_request(addr, &req).await;
        assert_eq!(s, 503, "body: {b}");
        assert!(b.contains("HALTED"), "body: {b}");
        assert!(b.contains("\"accepted\":false"), "body: {b}");
    }

    fn bieq_payload_json() -> String {
        serde_json::to_string(&serde_json::json!({
            "instruction_id": "T9-SB-0001",
            "symbol": "BI-EQ",
            "exchange": "NSE",
            "side": "BUY",
            "quantity": 1,
            "order_type": "LIMIT",
            "limit_price_paise": 5050,
            "product_type": "CNC",
            "time_in_force": "DAY",
        }))
        .unwrap()
    }

    async fn encoded_request_with_payload(secret: &str, payload: serde_json::Value) -> String {
        use crate::gateway_protocol::{encode_envelope, sha256_hex, Envelope};
        let payload_json = serde_json::to_string(&payload).unwrap();
        let hash = sha256_hex(payload_json.as_bytes());
        let env = Envelope {
            protocol_version: "execution-gateway.v1".into(),
            message_type: "EXECUTION_INTENT".into(),
            request_id: "req-t4a".into(),
            account_scope_id: "acc-1".into(),
            execution_partition_id: "part-1".into(),
            payload_hash: hash,
            gate_epoch: 1,
            fence_token: "fence-t4a".into(),
            deadline_epoch_ms: 9_999_999_999_999,
            payload,
            authentication: String::new(),
        };
        let encoded = encode_envelope(secret, &env).unwrap();
        format!(
            "POST /v1/intents HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            encoded.len(),
            encoded
        )
    }

    async fn encoded_bieq_request(secret: &str) -> String {
        let payload = serde_json::from_str(&bieq_payload_json()).unwrap();
        encoded_request_with_payload(secret, payload).await
    }

    fn fake_forwarder(script: CommandScript) -> BridgeForwarder {
        let mut fake = FakeBridge::new();
        fake.script(script);
        Arc::new(tokio::sync::Mutex::new(
            Box::new(fake) as Box<dyn BridgeClient + Send>
        ))
    }

    fn enabled_state(forwarder: BridgeForwarder) -> ServerState {
        ServerState::with_gateway_auth(
            ExecState::Enabled,
            "s3cr3t".into(),
            "execution-gateway.v1".into(),
        )
        .with_forwarder(forwarder)
    }

    #[tokio::test]
    async fn intents_enabled_forwards_to_bridge_and_returns_start_report() {
        let state = enabled_state(fake_forwarder(CommandScript::Accept));
        let addr = spawn_server(state).await;
        let req = encoded_bieq_request("s3cr3t").await;
        let (s, b) = raw_request(addr, &req).await;
        assert_eq!(s, 202, "body: {b}");
        assert!(b.contains("\"accepted\":true"), "body: {b}");
        // The minted 14-hex deterministic ref is echoed with the bridge's broker id.
        assert!(b.contains("\"client_order_ref\":"), "body: {b}");
        assert!(b.contains("\"broker_order_id\":\"BRK-0001\""), "body: {b}");
        assert!(b.contains("\"execution_attempt_id\":"), "body: {b}");
        assert!(b.contains("\"gate_state\":\"ENABLED\""), "body: {b}");
        let v: serde_json::Value = serde_json::from_str(&b).unwrap();
        let ref_ = v["client_order_ref"].as_str().unwrap();
        assert_eq!(ref_.len(), 14, "deterministic 14-hex ref: {ref_}");
        assert!(ref_.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[tokio::test]
    async fn intents_enabled_bridge_unknown_returns_503_never_ack() {
        let state = enabled_state(fake_forwarder(CommandScript::Unknown(
            "synthetic-unknown".into(),
        )));
        let addr = spawn_server(state).await;
        let req = encoded_bieq_request("s3cr3t").await;
        let (s, b) = raw_request(addr, &req).await;
        assert_eq!(s, 503, "body: {b}");
        assert!(b.contains("\"accepted\":false"), "body: {b}");
        assert!(b.contains("\"outcome\":\"UNKNOWN\""), "body: {b}");
        assert!(
            !b.contains("broker_order_id"),
            "UNKNOWN must not leak a broker id: {b}"
        );
    }

    #[tokio::test]
    async fn unknown_outcome_escalates_to_halt_and_operator_review() {
        // Shrunk escalation window (production pins 15 s): UNKNOWN → 503, then the
        // watchdog force-HALTs the gate and raises operator_review_required.
        let state = enabled_state(fake_forwarder(CommandScript::Unknown(
            "synthetic-unknown".into(),
        )))
        .with_unknown_escalation(std::time::Duration::from_millis(80));
        let addr = spawn_server(state).await;
        let req = encoded_bieq_request("s3cr3t").await;
        let (s, b) = raw_request(addr, &req).await;
        assert_eq!(s, 503, "body: {b}");
        // Poll /healthz until the escalation fires.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let (_, hb) = raw_request(addr, "GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n").await;
            let review = hb.contains("\"operator_review_required\":true");
            if review || std::time::Instant::now() > deadline {
                assert!(review, "escalation never fired; last body: {hb}");
                assert!(
                    hb.contains("\"gate_state\":\"HALTED\""),
                    "watchdog must force HALT; last body: {hb}"
                );
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }
    /// Bounded wait for the UNKNOWN watchdog to raise the operator-review flag and force
    /// the gate HALTED. Asserts instead of falling through, so a watchdog that never
    /// escalates fails the test rather than hanging it.
    async fn wait_for_unknown_escalation(state: &ServerState) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let s = state.snapshot();
            if s.operator_review_required {
                assert_eq!(s.gate, ExecState::Halted, "watchdog must force HALT");
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "UNKNOWN watchdog never escalated (gate={})",
                s.gate.as_str()
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn p3_019_reapproval_rearms_the_unknown_watchdog() {
        // P3-019. After an UNKNOWN escalation forces HALT + operator review, the DEC-044
        // recovery path is operator halt → re-approve, which opens a new approval epoch.
        // The watchdog must re-arm for that epoch so the NEXT unresolved UNKNOWN still
        // escalates. Before the fix `unknown_first_seen` was never cleared, so
        // `record_unknown_outcome` early-returned for the rest of the process lifetime
        // and the force-HALT + operator-review property was silently dead.
        let state = ServerState::new(ExecState::Enabled)
            .with_authorized_operator("saurabh")
            .with_unknown_escalation(std::time::Duration::from_millis(40));

        // Epoch 1: an unresolved UNKNOWN escalates to force-HALT + operator review.
        state.record_unknown_outcome();
        wait_for_unknown_escalation(&state).await;

        // DEC-044 recovery: the operator halts, then re-approves (gate HALTED → ENABLED).
        state.safety_halt("operator halt after UNKNOWN");
        state
            .approve("saurabh", "evidence-epoch-2")
            .expect("re-approval in the new epoch");
        assert_eq!(
            state.snapshot().gate,
            ExecState::Enabled,
            "re-approval must enable the gate"
        );

        // Epoch 2: a fresh UNKNOWN must arm a fresh escalation.
        state.record_unknown_outcome();
        wait_for_unknown_escalation(&state).await;
    }

    #[tokio::test]
    async fn intents_enabled_bridge_rejected_returns_409() {
        let state = enabled_state(fake_forwarder(CommandScript::Reject(
            "synthetic-reject".into(),
        )));
        let addr = spawn_server(state).await;
        let req = encoded_bieq_request("s3cr3t").await;
        let (s, b) = raw_request(addr, &req).await;
        assert_eq!(s, 409, "body: {b}");
        assert!(b.contains("\"accepted\":false"), "body: {b}");
        assert!(b.contains("\"outcome\":\"REJECTED\""), "body: {b}");
        assert!(b.contains("synthetic-reject"), "body: {b}");
    }

    #[tokio::test]
    async fn intents_enabled_rejects_unsupported_payload_mapping() {
        let state = enabled_state(fake_forwarder(CommandScript::Accept));
        let addr = spawn_server(state).await;
        // Drop symbol from the payload: the mapper must fail closed (422), never forward.
        let mut payload: serde_json::Value = serde_json::from_str(&bieq_payload_json()).unwrap();
        payload.as_object_mut().unwrap().remove("symbol");
        let req = encoded_request_with_payload("s3cr3t", payload).await;
        let (s, b) = raw_request(addr, &req).await;
        assert_eq!(s, 422, "body: {b}");
        assert!(b.contains("\"accepted\":false"), "body: {b}");
        assert!(b.contains("symbol required"), "body: {b}");
    }

    #[tokio::test]
    async fn intents_enabled_cancel_action_cancels_placed_order() {
        // Two scripts: one per bridge round-trip (place, then cancel) — the fake
        // consumes scripts FIFO, one per send_command.
        let mut fake = FakeBridge::new();
        fake.script(CommandScript::Accept);
        fake.script(CommandScript::Accept);
        let forwarder = Arc::new(tokio::sync::Mutex::new(
            Box::new(fake) as Box<dyn BridgeClient + Send>
        ));
        let state = enabled_state(forwarder);
        let addr = spawn_server(state).await;
        // Place first: the fake mints BRK-0001 into its order book.
        let (sp, bp) = raw_request(addr, &encoded_bieq_request("s3cr3t").await).await;
        assert_eq!(sp, 202, "place body: {bp}");
        assert!(
            bp.contains("\"broker_order_id\":\"BRK-0001\""),
            "body: {bp}"
        );
        // Cancel it via the action discriminator: same forwarder state, 202 + CANCELED.
        let payload = serde_json::json!({
            "action": "cancel",
            "instruction_id": "T9-SB-0001",
            "broker_order_id": "BRK-0001",
        });
        let (sc, bc) =
            raw_request(addr, &encoded_request_with_payload("s3cr3t", payload).await).await;
        assert_eq!(sc, 202, "cancel body: {bc}");
        assert!(bc.contains("\"action\":\"cancel\""), "body: {bc}");
        assert!(
            bc.contains("\"broker_order_id\":\"BRK-0001\""),
            "body: {bc}"
        );
        // The fake carries the terminal CANCELED status on the async report; the
        // sync ack may omit order_status (null here) — the lifecycle event leg
        // normalizes that to CANCELED (see events.rs default_state).
    }

    #[tokio::test]
    async fn intents_enabled_amend_action_modifies_placed_order() {
        // place, then amend — the fake consumes scripts FIFO, one per send_command.
        let mut fake = FakeBridge::new();
        fake.script(CommandScript::Accept);
        fake.script(CommandScript::Accept);
        let forwarder = Arc::new(tokio::sync::Mutex::new(
            Box::new(fake) as Box<dyn BridgeClient + Send>
        ));
        let state = enabled_state(forwarder);
        let addr = spawn_server(state).await;
        // Place first: the fake mints BRK-0001 into its order book.
        let (sp, bp) = raw_request(addr, &encoded_bieq_request("s3cr3t").await).await;
        assert_eq!(sp, 202, "place body: {bp}");
        assert!(
            bp.contains("\"broker_order_id\":\"BRK-0001\""),
            "body: {bp}"
        );
        // Amend it via the action discriminator: needs broker_order_id + the
        // amended order block; the fake accepts the modify (202 + modify ack).
        let payload = serde_json::json!({
            "action": "amend",
            "instruction_id": "T9-SB-0001",
            "broker_order_id": "BRK-0001",
            "symbol": "BI-EQ",
            "exchange": "NSE",
            "side": "BUY",
            "order_type": "LIMIT",
            "quantity": 2,
            "limit_price_paise": 5090,
            "product_type": "CNC",
            "time_in_force": "DAY",
        });
        let (sc, bc) =
            raw_request(addr, &encoded_request_with_payload("s3cr3t", payload).await).await;
        assert_eq!(sc, 202, "amend body: {bc}");
        assert!(bc.contains("\"action\":\"amend\""), "body: {bc}");
        assert!(
            bc.contains("\"broker_order_id\":\"BRK-0001\""),
            "body: {bc}"
        );
    }

    #[tokio::test]
    async fn intents_enabled_amend_requires_broker_order_id_422() {
        let state = enabled_state(fake_forwarder(CommandScript::Accept));
        let addr = spawn_server(state).await;
        let mut payload: serde_json::Value = serde_json::from_str(&bieq_payload_json()).unwrap();
        payload["action"] = serde_json::json!("amend");
        // No broker_order_id: the amend mapper must fail closed (422), never forward.
        let req = encoded_request_with_payload("s3cr3t", payload).await;
        let (s, b) = raw_request(addr, &req).await;
        assert_eq!(s, 422, "body: {b}");
        assert!(b.contains("broker_order_id"), "body: {b}");
    }

    #[tokio::test]
    async fn intents_enabled_rejects_unknown_action_with_422() {
        let state = enabled_state(fake_forwarder(CommandScript::Accept));
        let addr = spawn_server(state).await;
        let mut payload: serde_json::Value = serde_json::from_str(&bieq_payload_json()).unwrap();
        payload["action"] = serde_json::json!("delete"); // genuinely unknown action
        let req = encoded_request_with_payload("s3cr3t", payload).await;
        let (s, b) = raw_request(addr, &req).await;
        assert_eq!(s, 422, "body: {b}");
        assert!(b.contains("unsupported action"), "body: {b}");
    }

    #[tokio::test]
    async fn intents_enabled_emits_event_and_reports_accepted() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        // Fake gateway /v1/events intake.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let gw_addr = listener.local_addr().unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let gateway = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            let n = s.read(&mut chunk).await.unwrap();
            buf.extend_from_slice(&chunk[..n]);
            tx.send(String::from_utf8_lossy(&buf).to_string()).unwrap();
            s.write_all(
                b"HTTP/1.1 202 Accepted\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
            )
            .await
            .unwrap();
            drop(tx);
        });

        let state = enabled_state(fake_forwarder(CommandScript::Accept))
            .with_gateway_endpoint(format!("http://{gw_addr}"));
        let addr = spawn_server(state).await;
        let req = encoded_bieq_request("s3cr3t").await;
        let (s, b) = raw_request(addr, &req).await;
        assert_eq!(s, 202, "body: {b}");
        assert!(b.contains("\"event_emission\":\"accepted\""), "body: {b}");
        let captured = rx.recv().await.expect("gateway received the event");
        assert!(
            captured.starts_with("POST /v1/events HTTP/1.1"),
            "captured: {captured}"
        );
        assert!(
            captured.contains("LIFECYCLE"),
            "captured contains event type"
        );
        assert!(
            captured.contains("\"brokerOrderId\":\"BRK-0001\""),
            "captured: {captured}"
        );
        gateway.await.unwrap();
    }

    #[tokio::test]
    async fn intents_enabled_without_gateway_endpoint_reports_disabled() {
        let state = enabled_state(fake_forwarder(CommandScript::Accept));
        let addr = spawn_server(state).await;
        let req = encoded_bieq_request("s3cr3t").await;
        let (s, b) = raw_request(addr, &req).await;
        assert_eq!(s, 202, "body: {b}");
        assert!(b.contains("\"event_emission\":\"disabled\""), "body: {b}");
    }

    // ---- DEC-044 approval / safety-halt surface (A2.2/T9) ----

    /// A control-plane state with known gateway auth and a pinned operator, so no test depends
    /// on ambient `T9_APPROVED_BY` (P3-020 resolves it from the process env at construction).
    fn control_state(gate: ExecState) -> ServerState {
        ServerState::with_gateway_auth(gate, "s3cr3t".into(), "execution-gateway.v1".into())
            .with_authorized_operator("saurabh")
    }

    /// The signed-payload convention for a control request (P3-020): identity + evidence.
    fn control_payload(operator: &str, evidence: &str) -> serde_json::Value {
        serde_json::json!({ "operator": operator, "evidence": evidence })
    }

    /// Mints a control envelope exactly as the operator's signer must: the payload is hashed
    /// (`payload_hash`) and carried inside the HMAC canonical (P3-020).
    fn signed_control(
        secret: &str,
        message_type: &str,
        payload: serde_json::Value,
        gate_epoch: i64,
    ) -> String {
        use crate::gateway_protocol::{encode_envelope, sha256_hex, Envelope};
        let payload_json = serde_json::to_string(&payload).unwrap();
        let env = Envelope {
            protocol_version: "execution-gateway.v1".into(),
            message_type: message_type.into(),
            request_id: format!("ctl-{message_type}-{gate_epoch}"),
            account_scope_id: "acc-1".into(),
            execution_partition_id: "part-1".into(),
            payload_hash: sha256_hex(payload_json.as_bytes()),
            gate_epoch,
            fence_token: "fence-ctl".into(),
            deadline_epoch_ms: 9_999_999_999_999,
            payload,
            authentication: String::new(),
        };
        encode_envelope(secret, &env).unwrap()
    }

    /// The HTTP framing for a control body.
    fn control_request(path: &str, body: &str) -> String {
        format!(
            "POST {path} HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        )
    }

    #[tokio::test]
    async fn approve_with_authorized_operator_enables_gate() {
        let state = control_state(ExecState::Halted);
        let addr = spawn_server(state.clone()).await;
        let epoch = state.snapshot().control_epoch as i64;
        let envelope = signed_control(
            "s3cr3t",
            APPROVE_MESSAGE_TYPE,
            control_payload("saurabh", "evidence-1"),
            epoch,
        );
        let (s, b) = raw_request(addr, &control_request("/v1/approve", &envelope)).await;
        assert_eq!(s, 200, "body: {b}");
        assert!(b.contains("\"approved\":true"), "body: {b}");
        assert!(b.contains("\"gate_state\":\"ENABLED\""), "body: {b}");
        let (hs, hb) = raw_get(addr, "GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(hs, 200);
        assert!(hb.contains("\"gate_state\":\"ENABLED\""), "body: {hb}");
        assert!(hb.contains("\"approved_by\":\"saurabh\""), "body: {hb}");
        assert!(
            hb.contains("\"enabled_evidence\":\"evidence-1\""),
            "the signed evidence must be the one bound to the approval: {hb}"
        );
        // The approval spent the epoch it named (P3-020): health advertises the next one.
        assert_eq!(
            health_json(&state)["gate_epoch"],
            serde_json::json!(epoch + 1),
            "approval must advance the control epoch"
        );
    }

    #[tokio::test]
    async fn approve_with_unauthorized_operator_refused() {
        let state = control_state(ExecState::Halted);
        let addr = spawn_server(state.clone()).await;
        let epoch = state.snapshot().control_epoch as i64;
        let envelope = signed_control(
            "s3cr3t",
            APPROVE_MESSAGE_TYPE,
            control_payload("mallory", "evidence-1"),
            epoch,
        );
        let (s, b) = raw_request(addr, &control_request("/v1/approve", &envelope)).await;
        assert_eq!(s, 403, "body: {b}");
        assert!(b.contains("not the authorized operator"), "body: {b}");
        assert_eq!(
            state.snapshot().gate,
            ExecState::Halted,
            "an unauthorized (but properly signed) approve must not enable the gate"
        );
    }

    #[tokio::test]
    async fn approve_refused_after_operator_review() {
        let state = control_state(ExecState::Halted);
        // Simulate the UNKNOWN escalation raising the review flag.
        state.inner.lock().unwrap().operator_review_required = true;
        let addr = spawn_server(state.clone()).await;
        let envelope = signed_control(
            "s3cr3t",
            APPROVE_MESSAGE_TYPE,
            control_payload("saurabh", "evidence-1"),
            state.snapshot().control_epoch as i64,
        );
        let (s, b) = raw_request(addr, &control_request("/v1/approve", &envelope)).await;
        assert_eq!(s, 403, "body: {b}");
        assert!(b.contains("operator review required"), "body: {b}");
    }

    #[tokio::test]
    async fn halt_returns_gate_to_halted_and_invalidates_approval() {
        let state = control_state(ExecState::Halted);
        let addr = spawn_server(state.clone()).await;
        let approve = signed_control(
            "s3cr3t",
            APPROVE_MESSAGE_TYPE,
            control_payload("saurabh", "evidence-1"),
            state.snapshot().control_epoch as i64,
        );
        let (s, _) = raw_request(addr, &control_request("/v1/approve", &approve)).await;
        assert_eq!(s, 200);

        let halt = signed_control(
            "s3cr3t",
            HALT_MESSAGE_TYPE,
            serde_json::json!({
                "operator": "saurabh",
                "evidence": "evidence-halt",
                "reason": "operator kill switch",
            }),
            state.snapshot().control_epoch as i64,
        );
        let (s, b) = raw_request(addr, &control_request("/v1/halt", &halt)).await;
        assert_eq!(s, 200, "body: {b}");
        assert!(b.contains("\"halted\":true"), "body: {b}");
        assert!(b.contains("\"halted_by\":\"saurabh\""), "body: {b}");
        let (hs, hb) = raw_get(addr, "GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(hs, 200);
        assert!(hb.contains("\"gate_state\":\"HALTED\""), "body: {hb}");
        assert!(hb.contains("\"approved_by\":null"), "body: {hb}");
    }

    #[tokio::test]
    async fn approve_method_not_allowed_for_get() {
        let state = ServerState::new(ExecState::Halted);
        let addr = spawn_server(state).await;
        let (s, _) = raw_get(addr, "GET /v1/approve HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(s, 405);
    }

    #[tokio::test]
    async fn p3_020_unsigned_control_requests_are_refused() {
        // The pre-P3-020 wire format (the bare operator name) is now just an unsigned request:
        // it must be refused — and never downgraded to "allowed but unauthenticated".
        let halted = control_state(ExecState::Halted);
        let addr = spawn_server(halted.clone()).await;
        let (s, b) = raw_request(addr, &control_request("/v1/approve", "saurabh")).await;
        assert_eq!(s, 401, "unsigned approve must be refused; body: {b}");
        assert!(b.contains("\"accepted\":false"), "body: {b}");
        assert_eq!(
            halted.snapshot().gate,
            ExecState::Halted,
            "an unsigned approve must not enable the gate"
        );

        // The kill switch is authenticated too: an attacker who can reach the port must not be
        // able to halt trading (nor restart the watchdog) with an unsigned request.
        let enabled = control_state(ExecState::Enabled);
        let addr = spawn_server(enabled.clone()).await;
        let (s, b) = raw_request(addr, &control_request("/v1/halt", "saurabh")).await;
        assert_eq!(s, 401, "unsigned halt must be refused; body: {b}");
        assert_eq!(
            enabled.snapshot().gate,
            ExecState::Enabled,
            "an unsigned halt must not stop the service"
        );
    }

    #[tokio::test]
    async fn p3_020_wrong_key_wrong_type_and_lifted_signature_are_refused() {
        let state = control_state(ExecState::Halted);
        let addr = spawn_server(state.clone()).await;
        let epoch = state.snapshot().control_epoch as i64;
        let payload = control_payload("saurabh", "evidence-1");

        // Wrong key: the MAC cannot be verified.
        let wrong_key = signed_control(
            "not-the-secret",
            APPROVE_MESSAGE_TYPE,
            payload.clone(),
            epoch,
        );
        let (s, b) = raw_request(addr, &control_request("/v1/approve", &wrong_key)).await;
        assert_eq!(s, 401, "wrong-key approve must be refused; body: {b}");
        assert!(b.contains("authentication failed"), "body: {b}");

        // A signature minted for a different message type: the type is inside the signed
        // canonical, so it cannot be replayed on the control plane.
        let wrong_type = signed_control("s3cr3t", "EXECUTION_INTENT", payload.clone(), epoch);
        let (s, b) = raw_request(addr, &control_request("/v1/approve", &wrong_type)).await;
        assert_eq!(
            s, 401,
            "intent signature replayed on the control plane; body: {b}"
        );
        assert!(b.contains("control message type"), "body: {b}");

        // A signature lifted onto different content, with `payload_hash` recomputed so only the
        // HMAC can catch it (payload_json is a canonical field): the evidence stays unforgeable.
        use crate::gateway_protocol::sha256_hex;
        let mut lifted: serde_json::Value = serde_json::from_str(&signed_control(
            "s3cr3t",
            APPROVE_MESSAGE_TYPE,
            payload,
            epoch,
        ))
        .unwrap();
        let forged = control_payload("saurabh", "forged-evidence");
        let forged_json = serde_json::to_string(&forged).unwrap();
        lifted["payload"] = forged;
        lifted["payload_hash"] = serde_json::json!(sha256_hex(forged_json.as_bytes()));
        let lifted = serde_json::to_string(&lifted).unwrap();
        let (s, b) = raw_request(addr, &control_request("/v1/approve", &lifted)).await;
        assert_eq!(s, 401, "lifted signature must be refused; body: {b}");
        assert!(b.contains("authentication failed"), "body: {b}");

        assert_eq!(
            state.snapshot().gate,
            ExecState::Halted,
            "no rejected control request may change the gate"
        );
    }

    #[tokio::test]
    async fn p3_020_replayed_approve_envelope_is_refused_after_a_transition() {
        let state = control_state(ExecState::Halted);
        let addr = spawn_server(state.clone()).await;
        let approve = signed_control(
            "s3cr3t",
            APPROVE_MESSAGE_TYPE,
            control_payload("saurabh", "evidence-1"),
            state.snapshot().control_epoch as i64,
        );
        let (s, b) = raw_request(addr, &control_request("/v1/approve", &approve)).await;
        assert_eq!(s, 200, "body: {b}");

        // The operator halts (signed) — a transition, so the approve epoch is spent.
        let halt = signed_control(
            "s3cr3t",
            HALT_MESSAGE_TYPE,
            control_payload("saurabh", "evidence-halt"),
            state.snapshot().control_epoch as i64,
        );
        let (s, b) = raw_request(addr, &control_request("/v1/halt", &halt)).await;
        assert_eq!(s, 200, "body: {b}");

        // Replay the captured approve bytes: a valid MAC for a stale epoch must not re-enable.
        let (s, b) = raw_request(addr, &control_request("/v1/approve", &approve)).await;
        assert_eq!(s, 401, "replayed approve must be refused; body: {b}");
        assert!(b.contains("stale gate epoch"), "body: {b}");
        assert_eq!(
            state.snapshot().gate,
            ExecState::Halted,
            "a replayed approval must not re-enable the gate"
        );
    }

    #[test]
    fn p3_454_accepted_verification_without_envelope_answers_500() {
        use crate::gateway_protocol::{sha256_hex, Envelope, Verification};
        // The panic this guards (accepted but no decoded envelope) is a sibling-crate contract
        // violation that cannot be produced through `gateway_protocol`, so the boundary guard is
        // pinned directly: it must answer 500 — never panic the connection task into no response.
        let missing = Verification {
            accepted: true,
            reason: "accepted".into(),
            envelope: None,
        };
        let resp = envelope_or_500(missing).expect_err("a missing envelope must not be Ok");
        let resp = String::from_utf8_lossy(&resp).to_string();
        assert!(resp.starts_with("HTTP/1.1 500"), "got: {resp}");
        assert!(resp.contains("verified envelope missing"), "got: {resp}");

        // The happy path still hands the decoded envelope through untouched.
        let payload = serde_json::json!({ "x": 1 });
        let payload_json = serde_json::to_string(&payload).unwrap();
        let envelope = Envelope {
            protocol_version: "execution-gateway.v1".into(),
            message_type: "EXECUTION_INTENT".into(),
            request_id: "req-454".into(),
            account_scope_id: "acc-1".into(),
            execution_partition_id: "part-1".into(),
            payload_hash: sha256_hex(payload_json.as_bytes()),
            gate_epoch: 1,
            fence_token: "fence-1".into(),
            deadline_epoch_ms: 9_999_999_999_999,
            payload,
            authentication: "00".into(),
        };
        let ok = envelope_or_500(Verification {
            accepted: true,
            reason: "accepted".into(),
            envelope: Some(envelope),
        })
        .expect("a present envelope must pass through");
        assert_eq!(ok.request_id, "req-454");
    }
}
