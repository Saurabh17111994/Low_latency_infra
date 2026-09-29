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

use crate::bridge::{BridgeClient, ReportOutcome, SendFailure};
use crate::durable::LiveAttemptStore;
use crate::events;
use crate::executiongate::{Attempt, AttemptPhase, Claim};
use crate::gate::ExecState;
use crate::gate_report::GateReporter;
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

/// P3-209: production idle deadline for the request READ phase of one connection. Generous for
/// a live operator (three tiny request shapes) and useless to a stalled client.
const CONNECTION_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// P3-209: cap on concurrently served connections. A connection that cannot get a permit is
/// answered 503 and dropped immediately — never queued, never spawned as another task.
const MAX_CONNECTIONS: usize = 64;

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
    /// M1-3: shared correlation registry. The sync forward leg registers a `Route` owner for
    /// every command it sends, so the session dispatcher can book that command's postbacks
    /// instead of halting on an unknown ref. `None` keeps the pre-session behaviour (offline
    /// construction / tests that never build a session).
    registry: Option<crate::bridge::Registry>,
    /// Durable attempt guard for the live forward leg (Workstream-D swap, flag-gated). `None` is the
    /// flag-OFF behaviour: no durable guard here at all, and the gateway's own durable
    /// `Execution_Intent_Processed` dedup remains the only one. `Some` means an order is not sent
    /// until its attempt is recorded on disk.
    attempts: Option<Arc<dyn LiveAttemptStore>>,
    /// H2-5/D2 (CHG-334): the durable gate reporter. `None` is the offline/flag-off behaviour —
    /// gate transitions stay local, exactly as before. `Some` makes approval durable-first: the
    /// gateway writes the `Execution_Gate` row and the executor adopts the persisted epoch/fence,
    /// so the row and this process cannot drift.
    reporter: Option<Arc<GateReporter>>,
}

impl std::fmt::Debug for ServerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerState")
            .field("inner", &self.inner)
            .field("forwarder", &self.forwarder.is_some())
            .field("registry", &self.registry.is_some())
            .field("attempts", &self.attempts.is_some())
            .field("gate_reporter", &self.reporter.is_some())
            .finish()
    }
}

/// M1-3: production route-postback emitter. Reads the live gateway snapshot and emits the
/// normalized lifecycle image on the session dispatcher's spawned task. An empty gateway
/// endpoint is the offline/paper posture: the postback is booked (dispatched) but not emitted.
pub struct ServerStateRouteEmitter {
    state: ServerState,
}

impl ServerStateRouteEmitter {
    #[must_use]
    pub fn new(state: ServerState) -> Self {
        Self { state }
    }
}

#[async_trait::async_trait]
impl crate::bridge::session::RoutePostbackEmitter for ServerStateRouteEmitter {
    async fn emit(
        &self,
        report: &crate::bridge::ReportEnvelope,
        ctx: &crate::bridge::RouteContext,
    ) {
        let snap = self.state.snapshot();
        if snap.gateway_endpoint.trim().is_empty() {
            tracing::debug!(
                ref_ = %report.client_order_ref,
                "route postback booked; event emission disabled (no GATEWAY_ENDPOINT)"
            );
            return;
        }
        let now = now_ms();
        let event = events::lifecycle_event_value(
            report,
            &ctx.place,
            &ctx.account_scope_id,
            &ctx.execution_partition_id,
            ctx.gate_epoch,
            &ctx.trade_context_id,
            now,
        );
        if let Err(e) = events::emit_event(
            &snap.gateway_endpoint,
            &snap.shared_secret,
            &snap.protocol_version,
            &event,
            now,
        )
        .await
        {
            tracing::warn!(
                ref_ = %report.client_order_ref,
                error = %e,
                "route postback event emission failed (the postback was booked; the gateway \
                 projection may lag until the next lifecycle image)"
            );
        }
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
    /// H2-5/D2 (CHG-334): the durable row's fence token this process adopted (0 = unfenced).
    /// Sent on halt/renew reports; the gateway validates it against the row.
    fence_token: u64,
    /// H2-5/D2: the adopted lease horizon (epoch ms) — the row's `lease_expires_ts`.
    lease_expires_ts: Option<i64>,
    /// H2-5/D2: true once the boot `BOOT_HALT` report landed and the durable epoch was adopted.
    /// Until then `/healthz` reports the local epoch, and an approval would be refused by the
    /// gateway's epoch check — surfaced so the operator waits for the durable generation.
    gate_hydrated: bool,
    /// M1-1: the partition this executor owns (`EXECUTION_PARTITION_ID`), checked against the
    /// forwarded envelope under the send lock. `None` = not configured (flag-off/paper): the
    /// envelope's partition cannot be checked and the gate-only path stands.
    execution_partition_id: Option<String>,
    /// P3-209: idle deadline for the request read phase; production pins 5 s, tests shrink it.
    connection_timeout: std::time::Duration,
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
                fence_token: 0,
                lease_expires_ts: None,
                gate_hydrated: false,
                execution_partition_id: None,
                connection_timeout: CONNECTION_READ_TIMEOUT,
            })),
            forwarder: None,
            registry: None,
            attempts: None,
            reporter: None,
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
                fence_token: 0,
                lease_expires_ts: None,
                gate_hydrated: false,
                execution_partition_id: None,
                connection_timeout: CONNECTION_READ_TIMEOUT,
            })),
            forwarder: None,
            registry: None,
            attempts: None,
            reporter: None,
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

    /// Shrinks the request-read idle deadline (tests only — production keeps 5 s).
    #[cfg(test)]
    fn with_connection_timeout(self, d: std::time::Duration) -> Self {
        if let Ok(mut s) = self.inner.lock() {
            s.connection_timeout = d;
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

    /// Attaches the shared correlation registry (M1-3). The sync forward leg registers every
    /// command's `client_order_ref` as a `Route` owner before it sends, so the session
    /// dispatcher books that command's postbacks. Absent (default) keeps the pre-session
    /// behaviour: no ownership is published.
    #[must_use]
    pub fn with_registry(mut self, registry: crate::bridge::Registry) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Attaches the durable attempt guard (Workstream-D swap). Absent (default) is the flag-OFF
    /// behaviour: the ENABLED forward leg sends without a durable record of the attempt.
    #[must_use]
    pub fn with_attempts(mut self, attempts: Arc<dyn LiveAttemptStore>) -> Self {
        self.attempts = Some(attempts);
        self
    }

    /// Attaches the durable gate reporter (H2-5/D2, CHG-334). Absent (default) keeps the local
    /// gate transitions (offline/flag-off); present makes approval durable-first against the
    /// gateway's `Execution_Gate` row and starts the boot + lease-renew keeper.
    #[must_use]
    pub fn with_gate_reporter(mut self, reporter: Arc<GateReporter>) -> Self {
        self.reporter = Some(reporter);
        self
    }

    /// M1-1: the partition this executor owns (`EXECUTION_PARTITION_ID`). Checked against the
    /// forwarded envelope under the send lock; `None`/blank keeps the gate-only path (no partition
    /// configured — flag-off/paper runs cannot check one).
    #[must_use]
    pub fn with_execution_partition(self, partition: Option<String>) -> Self {
        if let Ok(mut s) = self.inner.lock() {
            s.execution_partition_id = partition.filter(|p| !p.trim().is_empty());
        }
        self
    }

    /// M1-1 tests: adopt a durable term as if the boot report had hydrated it (the boot keeper in
    /// `spawn_gate_keeper` is the production path).
    #[cfg(test)]
    fn with_hydrated_term(self, epoch: u64, fence: u64, lease_expires_ts: Option<i64>) -> Self {
        if let Ok(mut s) = self.inner.lock() {
            s.control_epoch = epoch;
            s.fence_token = fence;
            s.lease_expires_ts = lease_expires_ts;
            s.gate_hydrated = true;
        }
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
    pub async fn approve(&self, approver: &str, evidence_hash: &str) -> Result<(), String> {
        // Local validation under the lock; the durable report then runs outside it (await).
        let (epoch, reporter) = {
            let s = match self.inner.lock() {
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
            (s.control_epoch, self.reporter.clone())
        };

        // H2-5/D2: durable-first. With a reporter attached, the gateway writes the
        // Execution_Gate row (sanctioned path + single-operator promotion) and we adopt its
        // epoch/fence/lease; a failed report refuses the approval — the gate stays HALTED.
        if let Some(reporter) = reporter {
            let ack = reporter.approve(epoch, approver, evidence_hash).await?;
            if !ack.is_enabled() {
                return Err(format!(
                    "durable gate did not enable (state {}, outcome {})",
                    ack.state, ack.outcome
                ));
            }
            let mut s = match self.inner.lock() {
                Ok(s) => s,
                Err(_) => return Err("snapshot lock poisoned".to_string()),
            };
            // A concurrent halt (or another approval) won while the report was in flight: the
            // durable row is authoritative, so the local snapshot must not claim ENABLED.
            if s.gate != ExecState::Halted || s.control_epoch != epoch {
                return Err("gate changed while the durable approval was in flight".to_string());
            }
            s.gate = ExecState::Enabled;
            s.control_epoch = ack.epoch;
            s.fence_token = ack.fence_token;
            s.lease_expires_ts = ack.lease_expires_ts;
            // P3-019: a new approval epoch re-arms the UNKNOWN watchdog.
            s.unknown_first_seen = None;
            s.approved_by = Some(approver.to_string());
            s.enabled_evidence = Some(evidence_hash.to_string());
            tracing::info!(
                approver,
                evidence = evidence_hash,
                epoch = ack.epoch,
                fence_token = ack.fence_token,
                "DEC-044 approval recorded durably: gate=ENABLED (gateway row adopted)"
            );
            return Ok(());
        }

        // No reporter (offline/flag-off): the local transition, unchanged.
        let mut s = match self.inner.lock() {
            Ok(s) => s,
            Err(_) => return Err("snapshot lock poisoned".to_string()),
        };
        if s.gate != ExecState::Halted || s.control_epoch != epoch {
            return Err("gate changed while the approval was in flight".to_string());
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
    ///
    /// H2-5/D2: the local halt is immediate (it is fail-safe on its own) and the durable `HALT`
    /// report is retried in the background — if it never lands, the row's lease expires (30 s
    /// TTL) and the gateway's forward leg defers. A halt is never blocked on the network.
    pub fn safety_halt(&self, reason: &str) {
        let epoch = {
            let Ok(mut s) = self.inner.lock() else {
                return;
            };
            s.gate = ExecState::Halted;
            s.approved_by = None;
            s.enabled_evidence = None;
            s.operator_review_required = false;
            s.lease_expires_ts = None;
            s.fence_token = 0;
            // P3-020: a halt is a transition — envelopes minted before it are spent.
            s.bump_control_epoch();
            s.control_epoch
        };
        tracing::warn!("gate safety-halted: {reason}");
        self.report_durable_halt(epoch, reason);
    }

    /// M1-2: pushes a durable `HALT` report for `epoch` in the background. Shared by the immediate
    /// safety halt and the UNKNOWN escalation — the escalation used to halt only locally and let
    /// the gateway keep serving from the row until its 30 s lease expired. A no-op without a
    /// reporter; a missing tokio runtime is loud, with the lease expiry as the backstop.
    fn report_durable_halt(&self, epoch: u64, reason: &str) {
        let Some(reporter) = self.reporter.clone() else {
            return;
        };
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::error!(
                "no tokio runtime to report the durable gate halt — the row's lease will expire \
                 and the gateway will defer until it does"
            );
            return;
        };
        let reason = reason.to_string();
        let state = self.clone();
        handle.spawn(async move {
            // 15 attempts x 2 s covers a gateway restart inside the 30 s lease TTL.
            for attempt in 1..=15u32 {
                match reporter.halt(epoch, 0, &reason, "").await {
                    Ok(ack) => {
                        if let Ok(mut s) = state.inner.lock() {
                            // Keep the local epoch in lockstep with the durable one (never
                            // regress — a stale local term must not roll the generation back).
                            if s.gate == ExecState::Halted && ack.epoch > s.control_epoch {
                                s.control_epoch = ack.epoch;
                            }
                        }
                        tracing::info!(state = %ack.state, epoch = ack.epoch,
                            "durable gate halt applied");
                        return;
                    }
                    Err(e) => {
                        tracing::warn!(attempt, error = %e,
                            "durable gate halt report failed; retrying");
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
            tracing::error!(
                "durable gate halt report did not land after 15 attempts — the row's lease will \
                 expire and the gateway will defer until it does"
            );
        });
    }

    /// H2-5/D2: spawns the durable gate keeper — the boot `BOOT_HALT` report (retried until the
    /// gateway answers, hydrating the durable epoch/fence) and the lease-renew loop (renew
    /// interval / TTL, halt-on-loss). A no-op when no reporter is attached (offline/flag-off).
    pub fn spawn_gate_keeper(&self) {
        let Some(reporter) = self.reporter.clone() else {
            return;
        };
        let state = self.clone();
        tokio::spawn(async move {
            // Boot: retry until the gateway answers. The gateway may be disabled for a long
            // time; the executor serves HALTED meanwhile, which is safe.
            //
            // CHG-337: the 24×7 default has the gateway locked, so this retry can legitimately
            // run for hours (measured: the first drill attempt logged a WARN every 5 s while
            // locked — ~17k lines/day for an expected posture). The FIRST failure is loud; the
            // rest stay at debug, and the eventual adoption line is the success signal.
            let mut backoff = std::time::Duration::from_millis(500);
            let mut attempts: u64 = 0;
            loop {
                let epoch = state.snapshot().control_epoch;
                match reporter.boot_halt(epoch).await {
                    Ok(ack) => {
                        if let Ok(mut s) = state.inner.lock() {
                            // BOOT_HALT always answers HALTED; adopt defensively from the ack.
                            s.gate = if ack.is_enabled() {
                                ExecState::Enabled
                            } else {
                                ExecState::Halted
                            };
                            s.control_epoch = ack.epoch;
                            s.fence_token = ack.fence_token;
                            s.lease_expires_ts = ack.lease_expires_ts;
                            s.gate_hydrated = true;
                        }
                        tracing::info!(
                            epoch = ack.epoch,
                            state = %ack.state,
                            attempts = attempts + 1,
                            "durable gate row adopted at boot (epoch hydrated)"
                        );
                        break;
                    }
                    Err(e) => {
                        attempts += 1;
                        if attempts == 1 {
                            tracing::warn!(
                                error = %e,
                                "boot gate report failed; retrying quietly (gateway may be locked)"
                            );
                        } else {
                            tracing::debug!(
                                attempt = attempts,
                                error = %e,
                                "boot gate report retry"
                            );
                        }
                        tokio::time::sleep(backoff).await;
                        // 30 s cap: a locked gateway is an expected posture, not an incident.
                        backoff = (backoff * 2).min(std::time::Duration::from_secs(30));
                    }
                }
            }

            // Lease renew loop: only while ENABLED. The first failed renewal safety-halts.
            let renew_every = std::time::Duration::from_millis(reporter.renew_ms().max(1_000));
            loop {
                tokio::time::sleep(renew_every).await;
                let (enabled, epoch, fence) = {
                    let s = state.snapshot();
                    (s.gate == ExecState::Enabled, s.control_epoch, s.fence_token)
                };
                if !enabled {
                    continue;
                }
                match reporter.renew(epoch, fence).await {
                    Ok(ack) => {
                        if let Ok(mut s) = state.inner.lock() {
                            s.lease_expires_ts = ack.lease_expires_ts;
                        }
                    }
                    Err(e) => {
                        state.safety_halt(&format!("lease renewal failed: {e}"));
                    }
                }
            }
        });
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
            fence_token: 0,
            lease_expires_ts: None,
            gate_hydrated: false,
            execution_partition_id: None,
            connection_timeout: CONNECTION_READ_TIMEOUT,
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
    ///
    /// M1-2: the durable `HALT` report is pushed here (not only by `safety_halt`), so the gateway
    /// stops forwarding as soon as this fires instead of waiting out the row's 30 s lease. It is
    /// reported even when the local gate was already HALTED: an earlier report may have been lost
    /// and the row could still read ENABLED.
    fn escalate_unknown_review(&self) {
        let epoch = {
            let Ok(mut s) = self.inner.lock() else {
                return;
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
            // M1-2: the escalation is an operator action item, so it is counted (not just logged).
            crate::telemetry::METRICS
                .unknown_escalated
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::error!(
                "UNKNOWN bridge outcome unresolved for >= {:?}: gate force-HALTED, operator review required before re-enable",
                s.unknown_escalation
            );
            s.control_epoch
        };
        self.report_durable_halt(
            epoch,
            "UNKNOWN bridge outcome unresolved: operator review required before re-enable",
        );
    }

    /// M1-1 (P0-3 residue): the last look before the claim and the send, taken under the forwarder
    /// lock the send uses. The route's pre-check ran before that lock; a halt (operator, lease
    /// loss, UNKNOWN escalation) can land in between, so the order is judged against the state it
    /// actually sends under. `None` = proceed; `Some((status, doc))` = answer with the refusal.
    ///
    /// Ordering is `recheck -> claim -> send`: a refused request must leave no durable attempt
    /// behind, because a claimed-but-never-sent SUBMITTING record would be a permanent ambiguity.
    ///
    /// The term checks (epoch/fence/lease) run only once the durable boot row has been adopted
    /// (`gate_hydrated`): before that the local epoch is not the durable generation, so comparing
    /// would refuse legitimate orders. A wrong partition never halts — it is a routing/config
    /// error, not evidence that our generation is stale — and it is counted.
    fn recheck_send(
        &self,
        envelope: &gateway_protocol::Envelope,
    ) -> Option<(u16, serde_json::Value)> {
        let s = self.snapshot();
        if s.gate != ExecState::Enabled {
            return Some((
                503,
                serde_json::json!({
                    "accepted": false,
                    "outcome": "GATE_HALTED",
                    "reason": "gate is not ENABLED at send time",
                    "gate_state": s.gate.as_str(),
                }),
            ));
        }
        if let Some(mine) = s.execution_partition_id.as_deref() {
            if envelope.execution_partition_id != mine {
                crate::telemetry::METRICS
                    .order_denied_wrong_partition
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Some((
                    403,
                    serde_json::json!({
                        "accepted": false,
                        "outcome": "WRONG_PARTITION",
                        "reason": format!(
                            "envelope names partition {:?}, this executor owns {mine:?}",
                            envelope.execution_partition_id
                        ),
                        "gate_state": s.gate.as_str(),
                    }),
                ));
            }
        }
        if s.gate_hydrated {
            let epoch_matches =
                envelope.gate_epoch >= 0 && envelope.gate_epoch as u64 == s.control_epoch;
            if !epoch_matches {
                self.safety_halt(&format!(
                    "stale gate epoch on /v1/intents: envelope {} != current {}",
                    envelope.gate_epoch, s.control_epoch
                ));
                return Some((
                    409,
                    serde_json::json!({
                        "accepted": false,
                        "outcome": "STALE_EPOCH",
                        "reason": "the envelope's gate epoch is not the durable generation's",
                        "gate_state": ExecState::Halted.as_str(),
                    }),
                ));
            }
            let fence = envelope.fence_token.parse::<u64>().ok();
            if fence != Some(s.fence_token) {
                self.safety_halt(&format!(
                    "fence mismatch on /v1/intents: envelope {:?} != current {}",
                    envelope.fence_token, s.fence_token
                ));
                return Some((
                    409,
                    serde_json::json!({
                        "accepted": false,
                        "outcome": "FENCE_MISMATCH",
                        "reason": "the envelope's fence token is not the one this generation adopted",
                        "gate_state": ExecState::Halted.as_str(),
                    }),
                ));
            }
            let now = now_ms();
            if !s.lease_expires_ts.is_some_and(|expires| now <= expires) {
                self.safety_halt(&format!(
                    "durable gate lease is not live at send time (lease_expires_ts={:?}, now={now})",
                    s.lease_expires_ts
                ));
                return Some((
                    409,
                    serde_json::json!({
                        "accepted": false,
                        "outcome": "STALE_EPOCH",
                        "reason": "the durable gate lease has expired",
                        "gate_state": ExecState::Halted.as_str(),
                    }),
                ));
            }
        }
        None
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
            fence_token: self.fence_token,
            lease_expires_ts: self.lease_expires_ts,
            gate_hydrated: self.gate_hydrated,
            execution_partition_id: self.execution_partition_id.clone(),
            connection_timeout: self.connection_timeout,
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
        // H2-5/D2: true once the boot report landed and the durable epoch was adopted; before
        // that an approval is refused by the gateway's epoch check, so wait for it.
        "durable_gate": s.gate_hydrated,
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

/// What the durable attempt guard decided about an order the forward leg is about to send.
enum GuardOutcome {
    /// The claim is ours and its SUBMITTING record is on disk: the order may be sent.
    Send(Attempt),
    /// Answer without sending — the durable record already decides this request.
    Refuse(u16, serde_json::Value),
}

/// Durable pre-call persistence and duplicate guard for the live forward leg (Workstream-D swap).
///
/// The identity is the gateway's own: the attempt id [`intent`] mints per attempt, the
/// `instruction_id` from the payload, the bridge `command` as the action (M1-5: a place, its
/// modify, and its cancel share the instruction but are distinct money movements), and the signed
/// `payload_hash` — the same action-scoped pair the Java `Execution_Intent_Processed` dedup keys
/// on, which `gateway_protocol::verify` has already checked against the payload it covers. Nothing
/// is sent until a SUBMITTING record for this attempt is on disk:
///
/// * `Claimed` — new identity: record SUBMITTING, then send.
/// * `Existing` — this attempt id is already recorded. `ACCEPTED`/`REJECTED` answer from it with no
///   second call; `PREPARED` proves no call was made (the record is written *before* the send and a
///   failed write refuses instead of sending), so the send resumes; `SUBMITTING`/`UNKNOWN` is
///   ambiguous — the bridge may have seen this order — so it halts the gate rather than retrying.
/// * `Duplicate` — same instruction *and action* *and* payload under another attempt id: the
///   gateway's ordinary retry after an unacknowledged response. Refuse, do not halt; the order is
///   in a known state.
/// * `ContractViolation` — same instruction and action, different payload: an operational fault,
///   so it halts. A different action is a fresh claim, never a violation.
fn claim_for_send(
    state: &ServerState,
    attempts: &Arc<dyn LiveAttemptStore>,
    cmd_env: &crate::bridge::CommandEnvelope,
    payload_hash: &str,
) -> GuardOutcome {
    let attempt = match attempts.try_claim(
        &cmd_env.execution_attempt_id,
        &cmd_env.instruction_id,
        &cmd_env.command,
        payload_hash,
        &cmd_env.client_order_ref,
    ) {
        Ok(Claim::Claimed(attempt)) => attempt,
        Ok(Claim::Existing(existing)) => match existing.phase {
            AttemptPhase::Prepared => existing,
            AttemptPhase::Accepted => {
                return GuardOutcome::Refuse(
                    202,
                    serde_json::json!({
                        "accepted": true,
                        "outcome": "ACCEPTED",
                        "reason": "durable attempt record answers this retry; no second bridge call",
                        "instruction_id": existing.instruction_id,
                        "execution_attempt_id": existing.attempt_id,
                        "broker_order_id": existing.broker_order_id,
                        "gate_state": state.snapshot().gate.as_str(),
                    }),
                )
            }
            AttemptPhase::Rejected => {
                return GuardOutcome::Refuse(
                    409,
                    serde_json::json!({
                        "accepted": false,
                        "outcome": "REJECTED",
                        "reason": existing.reason,
                        "instruction_id": existing.instruction_id,
                        "execution_attempt_id": existing.attempt_id,
                        "gate_state": state.snapshot().gate.as_str(),
                    }),
                )
            }
            AttemptPhase::Submitting | AttemptPhase::Unknown => {
                state.safety_halt(&format!(
                    "unresolved durable attempt {} for instruction {} is {}: the bridge may have \
                     seen this order, so no retry is sent",
                    existing.attempt_id,
                    existing.instruction_id,
                    existing.phase.as_str()
                ));
                return GuardOutcome::Refuse(
                    503,
                    serde_json::json!({
                        "accepted": false,
                        "outcome": "UNRESOLVED",
                        "phase": existing.phase.as_str(),
                        "reason": "attempt recorded before the bridge call has no terminal outcome: \
                                   halted for reconciliation, never re-sent",
                        "instruction_id": existing.instruction_id,
                        "execution_attempt_id": existing.attempt_id,
                        "gate_state": state.snapshot().gate.as_str(),
                    }),
                );
            }
        },
        Ok(Claim::Duplicate) => {
            return GuardOutcome::Refuse(
                409,
                serde_json::json!({
                    "accepted": false,
                    "outcome": "DUPLICATE",
                    "reason": "this instruction and payload hash were already attempted: \
                               no second bridge call",
                    "instruction_id": cmd_env.instruction_id,
                    "gate_state": state.snapshot().gate.as_str(),
                }),
            )
        }
        Ok(Claim::ContractViolation) => {
            state.safety_halt(&format!(
                "instruction {} was attempted again with a different payload hash",
                cmd_env.instruction_id
            ));
            return GuardOutcome::Refuse(
                409,
                serde_json::json!({
                    "accepted": false,
                    "outcome": "CONTRACT_VIOLATION",
                    "reason": "this instruction was already attempted with different content: \
                               halted instead of sending",
                    "instruction_id": cmd_env.instruction_id,
                    "gate_state": state.snapshot().gate.as_str(),
                }),
            );
        }
        // No durable record could be written, so nothing may be sent: the whole point of the guard
        // is that the bridge is never called for an attempt the store does not know about.
        Err(e) => {
            return GuardOutcome::Refuse(
                503,
                serde_json::json!({
                    "accepted": false,
                    "outcome": "STORE_UNAVAILABLE",
                    "reason": format!("durable attempt store refused the claim: {e}"),
                    "instruction_id": cmd_env.instruction_id,
                    "gate_state": state.snapshot().gate.as_str(),
                }),
            )
        }
    };
    // The claim is durable; SUBMITTING must be too before the bridge can see the order.
    let mut submitting = attempt;
    submitting.phase = AttemptPhase::Submitting;
    match attempts.put(&submitting) {
        Ok(()) => GuardOutcome::Send(submitting),
        Err(e) => GuardOutcome::Refuse(
            503,
            serde_json::json!({
                "accepted": false,
                "outcome": "STORE_UNAVAILABLE",
                "reason": format!("durable attempt store refused to record SUBMITTING: {e}"),
                "instruction_id": submitting.instruction_id,
                "execution_attempt_id": submitting.attempt_id,
                "gate_state": state.snapshot().gate.as_str(),
            }),
        ),
    }
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
            // M1-1: hold the bridge lock from the last re-check through the claim and the send.
            // The pre-check above ran before this lock and a halt can land in between, so the
            // order is judged against the state it actually sends under; and the claim sits
            // behind the re-check, so a refused request leaves no durable attempt behind.
            // (The gateway event emission after the send stays outside: it must not stall
            // concurrent order submits.)
            //
            // D-swap: with a durable guard attached, the attempt is claimed and recorded BEFORE
            // the send — a request the store cannot record is refused here and never reaches the
            // bridge.
            let (guarded, submit) = {
                let mut guard = forwarder.lock().await;
                if let Some((status, doc)) = state.recheck_send(&envelope) {
                    return json(status, &doc);
                }
                let guarded = match &state.attempts {
                    None => None,
                    Some(attempts) => {
                        match claim_for_send(state, attempts, &cmd_env, &envelope.payload_hash) {
                            GuardOutcome::Send(attempt) => Some((Arc::clone(attempts), attempt)),
                            GuardOutcome::Refuse(status, doc) => return json(status, &doc),
                        }
                    }
                };
                // M1-3: publish route ownership before the send so a postback that lands the
                // instant the bridge dispatches the command is correlated (the session
                // dispatcher would otherwise halt on an unknown ref).
                if let Some(registry) = &state.registry {
                    if let Ok(mut registry) = registry.lock() {
                        registry.insert(
                            cmd_env.client_order_ref.clone(),
                            crate::bridge::ReportOwner::Route(Box::new(
                                crate::bridge::RouteContext {
                                    place: cmd_env.clone(),
                                    account_scope_id: envelope.account_scope_id.clone(),
                                    execution_partition_id: envelope.execution_partition_id.clone(),
                                    gate_epoch: envelope.gate_epoch,
                                    trade_context_id: envelope
                                        .payload
                                        .get("trade_context_id")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("")
                                        .to_string(),
                                },
                            )),
                        );
                    }
                }
                let submit = guard.send_command(cmd_env.clone()).await;
                (guarded, submit)
            };
            // The terminal phase is recorded from the same report the response is built from, so a
            // later retry of this attempt is answered from durable truth instead of being re-sent.
            if let Some((attempts, mut settled)) = guarded {
                settled.phase = match &submit {
                    Ok(report) if report.is_success() => AttemptPhase::Accepted,
                    Ok(report) if report.outcome() == Some(ReportOutcome::Rejected) => {
                        AttemptPhase::Rejected
                    }
                    _ => AttemptPhase::Unknown,
                };
                settled.broker_order_id = submit
                    .as_ref()
                    .ok()
                    .map(|report| report.broker_order_id.clone())
                    .filter(|id| !id.is_empty());
                settled.reason = match &submit {
                    Ok(report) if report.is_success() => None,
                    Ok(report) => Some(report.reason.clone()).filter(|r| !r.is_empty()),
                    Err(e) => Some(e.to_string()),
                };
                if let Err(e) = attempts.put(&settled) {
                    // The bridge outcome is already known and outranks our bookkeeping, so the
                    // answer below stands. The attempt stays SUBMITTING on disk, which is the safe
                    // direction: a retry then halts instead of sending again. Nothing silent.
                    tracing::warn!(
                        error = %e,
                        attempt_id = %settled.attempt_id,
                        "terminal attempt phase could not be recorded durably"
                    );
                }
            }
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
                    // H1-2: an UNKNOWN send outcome on the forward leg means the broker may
                    // already hold the order — halt the served surface immediately (durable
                    // HALT via the reporter) instead of only arming the 15 s review timer. A
                    // provably-not-sent failure keeps today's 503 + escalation, no halt.
                    let unknown = matches!(e, SendFailure::Unknown(_));
                    state.record_unknown_outcome();
                    if unknown {
                        state.safety_halt("bridge send outcome UNKNOWN on /v1/intents");
                    }
                    json(
                        503,
                        &serde_json::json!({
                            "accepted": false,
                            "outcome": "UNKNOWN",
                            "reason": e.to_string(),
                            "gate_state": state.snapshot().gate.as_str(),
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
            match state.approve(&req.operator, &req.evidence).await {
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
    serve_on(listener, state).await
}

/// The accept loop over an already-bound listener (separated from [`serve`] so a test can drive
/// the production path — permit gate included — on an ephemeral port).
async fn serve_on(listener: TcpListener, state: ServerState) -> Result<()> {
    // P3-209: bound the number of connections in flight, so a flood cannot spawn unbounded
    // tasks. Over the cap the peer gets an immediate 503 and the connection is dropped — never
    // queued (nothing here is worth making a client wait for).
    let permits = Arc::new(tokio::sync::Semaphore::new(MAX_CONNECTIONS));
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(x) => x,
            Err(_) => continue,
        };
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            let mut stream = stream;
            let _ = stream.write_all(&text(503, "too_many_connections")).await;
            let _ = stream.flush().await;
            continue;
        };
        let st = state.clone();
        tokio::spawn(async move {
            let _permit = permit; // held for the connection's lifetime
            if let Err(e) = handle_conn(stream, st).await {
                tracing::debug!("health connection closed: {e}");
            }
        });
    }
}

/// The request line + body of one connection (P3-209 read phase output).
struct HttpRequest {
    method: String,
    path: String,
    body: String,
}

/// One parsed request. `Ok(None)` from [`read_request`] means there is nothing to answer: the
/// client closed first, or the request was refused there (413) and the refusal was written.
async fn handle_conn(mut stream: TcpStream, state: ServerState) -> Result<()> {
    // P3-209: bound only the READ phase. A slowloris client that sends a partial header — or
    // promises a `Content-Length` body it never sends — must not pin this task forever. The
    // deadline deliberately does NOT cover routing: the ENABLED `/v1/intents` path forwards
    // synchronously to the bridge (which has its own, longer timeouts), and killing a live order
    // submit mid-flight would turn a valid order into a lost ack.
    let deadline = state.snapshot().connection_timeout;
    let request = match tokio::time::timeout(deadline, read_request(&mut stream)).await {
        Ok(Ok(Some(request))) => request,
        Ok(Ok(None)) => return Ok(()),
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            return Err(anyhow::anyhow!(
                "request read idle timeout after {deadline:?}"
            ))
        }
    };

    let resp = route(&state, &request.method, &request.path, &request.body).await;
    let _ = stream.write_all(&resp).await;
    let _ = stream.flush().await;
    Ok(())
}

/// One request line + body (headers + Content-Length; we support Content-Length only — no chunked).
async fn read_request(stream: &mut TcpStream) -> Result<Option<HttpRequest>> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        match stream.read(&mut chunk).await {
            Ok(0) => return Ok(None),
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if let Some(idx) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    break idx + 4;
                }
                if buf.len() > 64 * 1024 {
                    return Ok(None);
                }
            }
            Err(_) => return Ok(None),
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
                    return Ok(None);
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

    Ok(Some(HttpRequest { method, path, body }))
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
            // Drive the production accept loop — P3-209 permit gate included — instead of a
            // copy of it, so these tests cover the code the service actually runs.
            let _ = serve_on(listener, state).await;
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

    /// M1-3: a successful route send publishes the command's ref as a `Route` owner before the
    /// bridge sees it, so the session dispatcher books its postbacks instead of halting on an
    /// unknown ref (and the context carries what the gateway emission needs).
    #[tokio::test]
    async fn m1_3_route_send_registers_the_ref_for_the_session_dispatcher() {
        let registry: crate::bridge::Registry =
            Arc::new(Mutex::new(std::collections::HashMap::new()));
        let state = enabled_state(fake_forwarder(CommandScript::Accept))
            .with_registry(Arc::clone(&registry));
        let addr = spawn_server(state).await;
        let req = encoded_bieq_request("s3cr3t").await;
        let (s, b) = raw_request(addr, &req).await;
        assert_eq!(s, 202, "body: {b}");
        let v: serde_json::Value = serde_json::from_str(&b).unwrap();
        let ref_ = v["client_order_ref"].as_str().unwrap().to_string();

        let registry = registry.lock().unwrap();
        match registry.get(&ref_) {
            Some(crate::bridge::ReportOwner::Route(ctx)) => {
                assert_eq!(
                    ctx.place.client_order_ref, ref_,
                    "the context carries the sent command"
                );
            }
            other => panic!("the sent ref must be registered as Route, got {other:?}"),
        }
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
            .await
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

    #[tokio::test]
    async fn p3_209_stalled_request_is_closed_by_the_read_deadline() {
        // Slowloris: a header that never terminates, with a shrunk deadline so the test is fast.
        let state = ServerState::new(ExecState::Halted)
            .with_connection_timeout(std::time::Duration::from_millis(150));
        let addr = spawn_server(state).await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: x\r\n")
            .await
            .unwrap();
        stream.flush().await.unwrap();
        let mut buf = [0u8; 64];
        let outcome =
            tokio::time::timeout(std::time::Duration::from_secs(2), stream.read(&mut buf)).await;
        assert!(
            matches!(outcome, Ok(Ok(0))),
            "stalled connection must be closed by the read deadline, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn p3_209_serve_bounds_concurrent_connections() {
        // The accept loop must cap connections in flight: `MAX_CONNECTIONS` idle sockets hold
        // every permit, so the next connection is answered 503 and dropped instead of being
        // accepted as yet another task. The held sockets are answered only by the read deadline.
        let state = ServerState::new(ExecState::Halted)
            .with_connection_timeout(std::time::Duration::from_millis(1000));
        let addr = spawn_server(state).await;
        let mut held = Vec::new();
        for _ in 0..MAX_CONNECTIONS {
            let stream = TcpStream::connect(addr).await.unwrap();
            held.push(stream);
        }
        // A permit frees only when a held connection hits its deadline; ask until the cap is
        // visibly reached (a 200 means the server had not yet accepted every held socket).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        loop {
            let (s, b) = raw_request(addr, "GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n").await;
            if s == 503 {
                assert!(b.contains("too_many_connections"), "body: {b}");
                break;
            }
            assert_eq!(s, 200, "unexpected status above the cap: {b}");
            assert!(
                std::time::Instant::now() < deadline,
                "connection cap never reached (MAX_CONNECTIONS={MAX_CONNECTIONS})"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        drop(held);
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

    // ── Workstream-D swap: the durable attempt guard on the live forward leg ──────────────────────

    use crate::bridge::{BridgeReportStream, CommandEnvelope, ReportEnvelope};
    use crate::durable_file::{FileAttemptStore, ATTEMPTS_LOG};
    use crate::executiongate::AttemptStore;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    /// A fake bridge that counts sends and, at send time, looks up what the durable store already
    /// knows about the attempt it is being handed. "The record is on disk before the bridge sees the
    /// order" is the whole contract of the swap, so the test observes it from inside the call instead
    /// of inferring it from the order of assertions afterwards. `store: None` counts sends only.
    struct WatchingBridge {
        inner: FakeBridge,
        store: Option<Arc<FileAttemptStore>>,
        sends: Arc<AtomicU64>,
        phase_at_send: Arc<Mutex<Vec<Option<AttemptPhase>>>>,
    }

    #[async_trait::async_trait]
    impl BridgeClient for WatchingBridge {
        fn is_connected(&self) -> bool {
            self.inner.is_connected()
        }

        async fn connect(&mut self) -> Result<()> {
            self.inner.connect().await
        }

        async fn disconnect(&mut self) -> Result<()> {
            self.inner.disconnect().await
        }

        async fn send_command(
            &mut self,
            envelope: CommandEnvelope,
        ) -> Result<ReportEnvelope, crate::bridge::SendFailure> {
            self.sends.fetch_add(1, Ordering::SeqCst);
            let phase = self
                .store
                .as_ref()
                .and_then(|store| store.get(&envelope.execution_attempt_id))
                .map(|attempt| attempt.phase);
            self.phase_at_send.lock().unwrap().push(phase);
            self.inner.send_command(envelope).await
        }

        fn take_reports(&mut self) -> Option<BridgeReportStream> {
            self.inner.take_reports()
        }
    }

    /// An attempt store that can never write — the guard must refuse *before* the bridge, not after.
    struct RefusingStore;

    impl AttemptStore for RefusingStore {
        fn get(&self, _attempt_id: &str) -> Option<Attempt> {
            None
        }
        fn put(&self, _attempt: &Attempt) -> Result<()> {
            anyhow::bail!("durable store unavailable")
        }
        fn has_duplicate(&self, _instruction_id: &str, _action: &str, _request_hash: &str) -> bool {
            true
        }
        fn has_instruction(&self, _instruction_id: &str, _action: &str) -> bool {
            true
        }
        fn try_claim(
            &self,
            _attempt_id: &str,
            _instruction_id: &str,
            _action: &str,
            _request_hash: &str,
            _client_order_ref: &str,
        ) -> Result<Claim> {
            anyhow::bail!("durable store unavailable")
        }
    }

    /// The same store in the shape the server shares it as (`LiveAttemptStore`), so the guard and
    /// the test's own handle are provably one log.
    fn live_guard(store: &Arc<FileAttemptStore>) -> Arc<dyn LiveAttemptStore> {
        let same: Arc<FileAttemptStore> = Arc::clone(store);
        same
    }

    fn watching_forwarder(
        script: CommandScript,
        store: Option<&Arc<FileAttemptStore>>,
        sends: &Arc<AtomicU64>,
        phase_at_send: &Arc<Mutex<Vec<Option<AttemptPhase>>>>,
    ) -> BridgeForwarder {
        let mut inner = FakeBridge::new();
        inner.script(script);
        Arc::new(tokio::sync::Mutex::new(Box::new(WatchingBridge {
            inner,
            store: store.cloned(),
            sends: Arc::clone(sends),
            phase_at_send: Arc::clone(phase_at_send),
        })
            as Box<dyn BridgeClient + Send>))
    }

    /// The durable store refuses to share a log, so every guard test gets its own directory.
    fn guard_scratch_dir() -> std::path::PathBuf {
        static N: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "nautilus-http-guard-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Drives the ENABLED intent route in-process: the TCP helpers above cover the accept loop, and
    /// these tests are about the durable guard.
    async fn post_intent(state: &ServerState, body: &str) -> (u16, String) {
        let resp = String::from_utf8(route(state, "POST", "/v1/intents", body).await).unwrap();
        let status = resp
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .expect("a status line");
        let body = resp
            .split("\r\n\r\n")
            .nth(1)
            .unwrap_or_default()
            .to_string();
        (status, body)
    }

    /// The signed envelope alone — no request line — for in-process route calls.
    async fn intent_body() -> String {
        encoded_bieq_request("s3cr3t")
            .await
            .split("\r\n\r\n")
            .nth(1)
            .unwrap()
            .to_string()
    }

    fn attempt_log(path: &std::path::Path) -> Vec<serde_json::Value> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    /// The guard's head line: the attempt is durable *before* the bridge is called, and the terminal
    /// phase is recorded from the report the response was built from.
    #[tokio::test]
    async fn durable_guard_records_the_attempt_before_the_bridge_sees_it() {
        let dir = guard_scratch_dir();
        let path = dir.join(ATTEMPTS_LOG);
        let store = Arc::new(FileAttemptStore::open(&path).unwrap());
        let sends = Arc::new(AtomicU64::new(0));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let state = enabled_state(watching_forwarder(
            CommandScript::Accept,
            Some(&store),
            &sends,
            &seen,
        ))
        .with_attempts(live_guard(&store));

        let (status, body) = post_intent(&state, &intent_body().await).await;
        assert_eq!(status, 202, "body: {body}");
        assert_eq!(sends.load(Ordering::SeqCst), 1, "exactly one bridge call");
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            [Some(AttemptPhase::Submitting)],
            "the bridge must be handed the order only after the attempt is durable as SUBMITTING"
        );

        let lines = attempt_log(&path);
        // The claim itself is a durable record, so the log shows the whole life of the attempt:
        // claimed PREPARED, then SUBMITTING (before the send), then the terminal phase.
        assert_eq!(lines.len(), 3, "claim + submitting + terminal: {lines:?}");
        assert_eq!(lines[0]["phase"], "PREPARED");
        assert_eq!(lines[1]["phase"], "SUBMITTING");
        assert_eq!(lines[2]["phase"], "ACCEPTED");
        assert_eq!(lines[2]["broker_order_id"], "BRK-0001");
        assert_eq!(lines[0]["instruction_id"], "T9-SB-0001");
        assert_eq!(
            lines[0]["attempt_id"], lines[2]["attempt_id"],
            "one attempt, three phases"
        );
        assert_eq!(
            lines[0]["request_hash"], lines[2]["request_hash"],
            "the attempt keeps the identity it was claimed under"
        );
        drop(state);
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A retry of the same signed envelope — what the gateway sends after a lost response — must be
    /// refused, not forwarded a second time, and must not halt: the order is in a known state.
    #[tokio::test]
    async fn durable_guard_refuses_a_replayed_intent_without_a_second_bridge_call() {
        let dir = guard_scratch_dir();
        let path = dir.join(ATTEMPTS_LOG);
        let store = Arc::new(FileAttemptStore::open(&path).unwrap());
        let sends = Arc::new(AtomicU64::new(0));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let state = enabled_state(watching_forwarder(
            CommandScript::Accept,
            Some(&store),
            &sends,
            &seen,
        ))
        .with_attempts(live_guard(&store));

        let body = intent_body().await;
        let (first, first_body) = post_intent(&state, &body).await;
        assert_eq!(first, 202, "body: {first_body}");
        let (second, second_body) = post_intent(&state, &body).await;
        assert_eq!(second, 409, "body: {second_body}");
        assert!(
            second_body.contains("\"outcome\":\"DUPLICATE\""),
            "body: {second_body}"
        );
        assert_eq!(
            sends.load(Ordering::SeqCst),
            1,
            "the duplicate never reaches the bridge"
        );
        assert_eq!(
            state.snapshot().gate,
            ExecState::Enabled,
            "a duplicate is not ambiguity: the gate stays ENABLED"
        );
        assert_eq!(
            attempt_log(&path).len(),
            3,
            "the duplicate adds no record: claim, SUBMITTING, terminal"
        );
        drop(state);
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The point of a *durable* guard: a process that has forgotten everything still refuses to send
    /// an order its predecessor already sent.
    #[tokio::test]
    async fn a_restarted_process_answers_from_the_durable_log() {
        let dir = guard_scratch_dir();
        let path = dir.join(ATTEMPTS_LOG);
        let body = intent_body().await;
        let sends_first = Arc::new(AtomicU64::new(0));
        {
            let store = Arc::new(FileAttemptStore::open(&path).unwrap());
            let seen = Arc::new(Mutex::new(Vec::new()));
            let state = enabled_state(watching_forwarder(
                CommandScript::Accept,
                Some(&store),
                &sends_first,
                &seen,
            ))
            .with_attempts(live_guard(&store));
            let (status, b) = post_intent(&state, &body).await;
            assert_eq!(status, 202, "body: {b}");
        } // the process ends: state and store drop, and the log's lock is released

        // A new process over the same directory: empty memory, empty bridge, same log.
        let store = Arc::new(FileAttemptStore::open(&path).unwrap());
        assert!(
            store.has_instruction("T9-SB-0001", "place"),
            "the attempt outlived the process that wrote it"
        );
        let sends_second = Arc::new(AtomicU64::new(0));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let state = enabled_state(watching_forwarder(
            CommandScript::Accept,
            Some(&store),
            &sends_second,
            &seen,
        ))
        .with_attempts(live_guard(&store));
        let (status, b) = post_intent(&state, &body).await;
        assert_eq!(status, 409, "body: {b}");
        assert!(b.contains("\"outcome\":\"DUPLICATE\""), "body: {b}");
        assert_eq!(
            sends_second.load(Ordering::SeqCst),
            0,
            "a restart must not re-send an order its predecessor already sent"
        );
        assert_eq!(sends_first.load(Ordering::SeqCst), 1);
        drop(state);
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Fail-closed: an attempt the store cannot record is refused, and the bridge is never called.
    #[tokio::test]
    async fn a_store_that_cannot_record_is_never_sent() {
        let sends = Arc::new(AtomicU64::new(0));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let state = enabled_state(watching_forwarder(
            CommandScript::Accept,
            None,
            &sends,
            &seen,
        ))
        .with_attempts(Arc::new(RefusingStore));

        let (status, body) = post_intent(&state, &intent_body().await).await;
        assert_eq!(status, 503, "body: {body}");
        assert!(
            body.contains("\"outcome\":\"STORE_UNAVAILABLE\""),
            "body: {body}"
        );
        assert_eq!(
            sends.load(Ordering::SeqCst),
            0,
            "no durable record, no send"
        );
        assert_eq!(
            state.snapshot().gate,
            ExecState::Enabled,
            "an unrecordable store is an outage, not ambiguity"
        );
    }

    /// The resume branch, tested where it lives. The live route mints a fresh attempt id per request
    /// (`intent.rs`), so an `Existing` record belongs to a caller that resumes *an attempt* — a
    /// recovery or a re-drive — not to one that re-issues an intent. What it must never do is send.
    #[tokio::test]
    async fn a_resumed_attempt_is_answered_from_its_record_or_halts() {
        let dir = guard_scratch_dir();
        let store = Arc::new(FileAttemptStore::open(&dir.join(ATTEMPTS_LOG)).unwrap());
        let guard = live_guard(&store);
        let state =
            enabled_state(fake_forwarder(CommandScript::Accept)).with_attempts(Arc::clone(&guard));
        let payload: serde_json::Value = serde_json::from_str(&bieq_payload_json()).unwrap();
        let envelope = intent::place_envelope_from_payload(&payload).unwrap();

        // First call claims the attempt and hands it to the bridge.
        let first = claim_for_send(&state, &guard, &envelope, "hash-1");
        let GuardOutcome::Send(mut settled) = first else {
            panic!("a fresh attempt must be sent");
        };
        settled.phase = AttemptPhase::Accepted;
        settled.broker_order_id = Some("BRK-0001".into());
        store.put(&settled).unwrap();

        // Resuming an accepted attempt answers with the recorded acceptance and sends nothing.
        match claim_for_send(&state, &guard, &envelope, "hash-1") {
            GuardOutcome::Refuse(202, doc) => {
                assert_eq!(doc["outcome"], "ACCEPTED");
                assert_eq!(doc["broker_order_id"], "BRK-0001");
            }
            GuardOutcome::Refuse(status, doc) => {
                panic!("expected the recorded acceptance, got {status}: {doc}")
            }
            GuardOutcome::Send(_) => panic!("a resumed attempt must never be sent again"),
        }

        // An ambiguous record is the case the whole guard exists for: halt, never retry.
        let mut ambiguous = settled;
        ambiguous.phase = AttemptPhase::Submitting;
        ambiguous.broker_order_id = None;
        store.put(&ambiguous).unwrap();
        match claim_for_send(&state, &guard, &envelope, "hash-1") {
            GuardOutcome::Refuse(503, doc) => {
                assert_eq!(doc["outcome"], "UNRESOLVED");
                assert_eq!(doc["phase"], "SUBMITTING");
            }
            GuardOutcome::Refuse(status, doc) => {
                panic!("an unresolved attempt must be refused, got {status}: {doc}")
            }
            GuardOutcome::Send(_) => panic!("an unresolved attempt must never be sent again"),
        }
        assert_eq!(
            state.snapshot().gate,
            ExecState::Halted,
            "an unresolved attempt halts the gate for reconciliation"
        );
        drop(state);
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// M1-5: the live guard is action-scoped. A place claims its instruction's place stream; an
    /// amend of the same instruction is a fresh money movement with its own hash stream — before
    /// this it was misread as a changed payload (`CONTRACT_VIOLATION` + halt) and never reached
    /// the bridge.
    #[tokio::test]
    async fn m1_5_an_amend_after_a_place_is_a_fresh_claim() {
        let dir = guard_scratch_dir();
        let store = Arc::new(FileAttemptStore::open(&dir.join(ATTEMPTS_LOG)).unwrap());
        let guard = live_guard(&store);
        let state =
            enabled_state(fake_forwarder(CommandScript::Accept)).with_attempts(Arc::clone(&guard));

        let place_payload: serde_json::Value = serde_json::from_str(&bieq_payload_json()).unwrap();
        let place = intent::place_envelope_from_payload(&place_payload).unwrap();
        let GuardOutcome::Send(mut settled) = claim_for_send(&state, &guard, &place, "hash-place")
        else {
            panic!("a fresh place must be sent");
        };
        settled.phase = AttemptPhase::Accepted;
        settled.broker_order_id = Some("BRK-0001".into());
        store.put(&settled).unwrap();

        let mut amend_payload = place_payload.clone();
        amend_payload["broker_order_id"] = serde_json::json!("BRK-0001");
        amend_payload["quantity"] = serde_json::json!(2);
        let amend = intent::amend_envelope_from_payload(&amend_payload).unwrap();
        assert_eq!(
            amend.instruction_id, place.instruction_id,
            "the amend must share the instruction to pin the identity question"
        );

        match claim_for_send(&state, &guard, &amend, "hash-amend") {
            GuardOutcome::Send(attempt) => assert_eq!(attempt.action, "modify"),
            GuardOutcome::Refuse(status, doc) => {
                panic!("an amend after a place must be a fresh claim, got {status}: {doc}")
            }
        }
        assert_eq!(
            state.snapshot().gate,
            ExecState::Enabled,
            "a legitimate amend is not a contract violation and must not halt the gate"
        );
        drop(state);
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── H2-5/D2 (CHG-334): the durable gate report path ─────────────────────────────────────────

    /// A signed `GATE_APPROVE` envelope naming `epoch` (the /v1/approve control request).
    async fn approve_request(secret: &str, epoch: u64, operator: &str, evidence: &str) -> String {
        use crate::gateway_protocol::{encode_envelope, sha256_hex, Envelope};
        let payload = serde_json::json!({ "operator": operator, "evidence": evidence });
        let payload_json = serde_json::to_string(&payload).unwrap();
        let env = Envelope {
            protocol_version: "execution-gateway.v1".into(),
            message_type: "GATE_APPROVE".into(),
            request_id: format!("approve-{epoch}-{operator}"),
            account_scope_id: "acc-1".into(),
            execution_partition_id: "part-1".into(),
            payload_hash: sha256_hex(payload_json.as_bytes()),
            gate_epoch: epoch as i64,
            fence_token: "0".into(),
            deadline_epoch_ms: 9_999_999_999_999,
            payload,
            authentication: String::new(),
        };
        let encoded = encode_envelope(secret, &env).unwrap();
        format!(
            "POST /v1/approve HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            encoded.len(),
            encoded
        )
    }

    fn test_reporter(addr: std::net::SocketAddr) -> crate::gate_report::GateReporter {
        crate::gate_report::GateReporter::new(
            format!("http://{addr}"),
            "secret".into(),
            "execution-gateway.v1".into(),
            "part-1".into(),
            "acc-1".into(),
            "exec-1".into(),
            30_000,
            10_000,
        )
    }

    /// One-shot gateway stub: captures the request and replies with the given status + body.
    async fn gateway_stub(
        status: &'static str,
        reply_body: &'static str,
    ) -> (
        std::net::SocketAddr,
        tokio::sync::mpsc::UnboundedReceiver<String>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
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
                    .expect("timed out reading the gate report")
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

    /// Two-shot gateway stub: refuses the first report (as a locked gateway does), then answers.
    async fn gateway_stub_two(
        replies: Vec<(&'static str, &'static str)>,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            for (status, reply_body) in replies {
                let (mut s, _) = listener.accept().await.unwrap();
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                let mut expected: Option<usize> = None;
                loop {
                    let n =
                        tokio::time::timeout(std::time::Duration::from_secs(5), s.read(&mut chunk))
                            .await
                            .expect("timed out reading the report")
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
                let resp = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply_body}",
                    reply_body.len()
                );
                s.write_all(resp.as_bytes()).await.unwrap();
            }
        });
        (addr, handle)
    }

    #[tokio::test]
    async fn gate_keeper_retries_a_refused_boot_report_then_hydrates() {
        // CHG-337: the live sequence — a locked gateway refuses the first boot report, the
        // keeper backs off and retries, and the durable epoch is adopted once it answers.
        let (gw_addr, gw) = gateway_stub_two(vec![
            (
                "503 Service Unavailable",
                r#"{"error":"execution disabled via EXECUTION_ENABLED"}"#,
            ),
            (
                "200 OK",
                r#"{"outcome":"HALTED","state":"HALTED","epoch":7,"fence_token":0}"#,
            ),
        ])
        .await;
        let state = ServerState::with_gateway_auth(
            ExecState::Halted,
            "secret".into(),
            "execution-gateway.v1".into(),
        )
        .with_gate_reporter(Arc::new(test_reporter(gw_addr)));
        state.spawn_gate_keeper();

        for _ in 0..250 {
            if state.snapshot().gate_hydrated {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let snap = state.snapshot();
        assert!(
            snap.gate_hydrated,
            "the retry must hydrate once the gateway answers"
        );
        assert_eq!(
            snap.control_epoch, 7,
            "the durable epoch is adopted after a retry"
        );
        assert_eq!(snap.gate, ExecState::Halted);
        gw.await.unwrap();
    }

    #[tokio::test]
    async fn durable_approve_adopts_the_gateway_row() {
        let (gw_addr, mut rx, gw) = gateway_stub(
            "200 OK",
            r#"{"outcome":"ENABLED","state":"ENABLED","epoch":3,"fence_token":7,"owner_instance_id":"exec-1","lease_expires_ts":9999999999999}"#,
        )
        .await;
        let state = ServerState::with_gateway_auth(
            ExecState::Halted,
            "secret".into(),
            "execution-gateway.v1".into(),
        )
        .with_authorized_operator("saurabh")
        .with_gate_reporter(Arc::new(test_reporter(gw_addr)));
        let addr = spawn_server(state.clone()).await;

        let (status, body) =
            raw_request(addr, &approve_request("secret", 1, "saurabh", "ev-1").await).await;
        assert_eq!(status, 200, "body: {body}");
        let snap = state.snapshot();
        assert_eq!(snap.gate, ExecState::Enabled);
        // The durable row is authoritative: its epoch/fence/lease are adopted, not the local ones.
        assert_eq!(snap.control_epoch, 3, "durable epoch adopted");
        assert_eq!(snap.fence_token, 7, "durable fence adopted");
        assert_eq!(snap.lease_expires_ts, Some(9_999_999_999_999));
        assert_eq!(snap.approved_by.as_deref(), Some("saurabh"));
        let req = rx.recv().await.expect("gateway got the report");
        assert!(req.starts_with("POST /v1/gate HTTP/1.1"), "req: {req}");
        assert!(req.contains("\"transition\":\"APPROVE\""), "req: {req}");
        assert!(req.contains("\"principal\":\"saurabh\""), "req: {req}");
        gw.await.unwrap();
    }

    #[tokio::test]
    async fn durable_approve_refuses_when_the_report_conflicts() {
        let (gw_addr, _rx, gw) = gateway_stub(
            "409 Conflict",
            r#"{"error":"stale report: epoch 1 != durable 3","outcome":"EPOCH_MISMATCH"}"#,
        )
        .await;
        let state = ServerState::with_gateway_auth(
            ExecState::Halted,
            "secret".into(),
            "execution-gateway.v1".into(),
        )
        .with_authorized_operator("saurabh")
        .with_gate_reporter(Arc::new(test_reporter(gw_addr)));
        let addr = spawn_server(state.clone()).await;

        let (status, body) =
            raw_request(addr, &approve_request("secret", 1, "saurabh", "ev-1").await).await;
        assert_eq!(status, 403, "body: {body}");
        // Durable-first: a failed report never completes locally.
        assert_eq!(state.snapshot().gate, ExecState::Halted);
        gw.await.unwrap();
    }

    #[tokio::test]
    async fn gate_keeper_hydrates_the_durable_epoch_at_boot() {
        let (gw_addr, mut rx, gw) = gateway_stub(
            "200 OK",
            r#"{"outcome":"HALTED","state":"HALTED","epoch":5,"fence_token":0}"#,
        )
        .await;
        let state = ServerState::with_gateway_auth(
            ExecState::Halted,
            "secret".into(),
            "execution-gateway.v1".into(),
        )
        .with_gate_reporter(Arc::new(test_reporter(gw_addr)));
        state.spawn_gate_keeper();

        for _ in 0..100 {
            if state.snapshot().gate_hydrated {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let snap = state.snapshot();
        assert!(
            snap.gate_hydrated,
            "the boot report must hydrate the durable epoch"
        );
        assert_eq!(
            snap.control_epoch, 5,
            "the durable epoch is adopted at boot"
        );
        assert_eq!(snap.gate, ExecState::Halted);
        let req = rx.recv().await.expect("gateway got the boot report");
        assert!(req.contains("\"transition\":\"BOOT_HALT\""), "req: {req}");
        gw.await.unwrap();
    }

    #[tokio::test]
    async fn safety_halt_reports_the_halt_in_the_background() {
        let (gw_addr, mut rx, gw) = gateway_stub(
            "200 OK",
            r#"{"outcome":"HALTED","state":"HALTED","epoch":5,"fence_token":3}"#,
        )
        .await;
        let state = ServerState::with_gateway_auth(
            ExecState::Enabled,
            "secret".into(),
            "execution-gateway.v1".into(),
        )
        .with_gate_reporter(Arc::new(test_reporter(gw_addr)));

        state.safety_halt("test halt");
        // The local halt is immediate; the durable report is retried in the background.
        assert_eq!(state.snapshot().gate, ExecState::Halted);
        let req = tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv())
            .await
            .expect("the halt report must be sent")
            .unwrap();
        assert!(req.contains("\"transition\":\"HALT\""), "req: {req}");
        // The durable halt's epoch is adopted, so the local term cannot drift from the row.
        for _ in 0..100 {
            if state.snapshot().control_epoch == 5 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(state.snapshot().control_epoch, 5);
        gw.await.unwrap();
    }

    // ── M1-2: the UNKNOWN escalation reports the durable halt ───────────────────────────────────

    #[tokio::test]
    async fn m1_2_unknown_escalation_reports_the_durable_halt() {
        let (gw_addr, mut rx, gw) = gateway_stub(
            "200 OK",
            r#"{"outcome":"HALTED","state":"HALTED","epoch":9,"fence_token":0}"#,
        )
        .await;
        let state = ServerState::with_gateway_auth(
            ExecState::Enabled,
            "secret".into(),
            "execution-gateway.v1".into(),
        )
        .with_gate_reporter(Arc::new(test_reporter(gw_addr)))
        .with_unknown_escalation(std::time::Duration::from_millis(40));

        state.record_unknown_outcome();
        wait_for_unknown_escalation(&state).await;
        // The local halt lands through the watchdog; the durable HALT must land with it, not when
        // the row's lease expires (that was the finding: the gateway kept serving up to 30 s).
        let req = tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv())
            .await
            .expect("the escalation must report the durable halt")
            .unwrap();
        assert!(req.contains("\"transition\":\"HALT\""), "req: {req}");
        assert!(state.snapshot().operator_review_required);
        gw.await.unwrap();
    }

    #[tokio::test]
    async fn m1_2_unknown_escalation_reports_even_when_already_halted() {
        let (gw_addr, mut rx, gw) = gateway_stub(
            "200 OK",
            r#"{"outcome":"HALTED","state":"HALTED","epoch":4,"fence_token":0}"#,
        )
        .await;
        let state = ServerState::with_gateway_auth(
            ExecState::Halted,
            "secret".into(),
            "execution-gateway.v1".into(),
        )
        .with_gate_reporter(Arc::new(test_reporter(gw_addr)))
        .with_unknown_escalation(std::time::Duration::from_millis(40));

        state.record_unknown_outcome();
        wait_for_unknown_escalation(&state).await;
        // The gate was already HALTED locally: an earlier report may have been lost and the row
        // could still read ENABLED, so the escalation still pushes the halt.
        let req = tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv())
            .await
            .expect("a lost earlier report must not keep the row serving")
            .unwrap();
        assert!(req.contains("\"transition\":\"HALT\""), "req: {req}");
        gw.await.unwrap();
    }

    /// M1-2: the escalation is counted. Run standalone the delta is exactly one; in a full suite
    /// the counter is process-wide and the sibling escalation tests may land concurrently, so the
    /// committed assertion is a lower bound (the mutation check runs this test in isolation).
    #[test]
    fn m1_2_escalation_counts_unknown_escalated() {
        let before = crate::telemetry::METRICS
            .unknown_escalated
            .load(Ordering::Relaxed);
        let state = ServerState::new(ExecState::Enabled);
        {
            let mut s = state.inner.lock().unwrap();
            s.unknown_first_seen =
                Some(std::time::Instant::now() - std::time::Duration::from_secs(20));
        }
        state.escalate_unknown_review();
        assert_eq!(state.snapshot().gate, ExecState::Halted);
        assert!(state.snapshot().operator_review_required);
        let after = crate::telemetry::METRICS
            .unknown_escalated
            .load(Ordering::Relaxed);
        assert!(after > before, "the escalation must be counted");
    }

    // ── M1-1: the send-time re-check (recheck -> claim -> send) ─────────────────────────────────

    fn recheck_envelope(
        epoch: i64,
        fence: &str,
        partition: &str,
    ) -> crate::gateway_protocol::Envelope {
        crate::gateway_protocol::Envelope {
            protocol_version: "execution-gateway.v1".into(),
            message_type: "EXECUTION_INTENT".into(),
            request_id: "req-recheck".into(),
            account_scope_id: "acc-1".into(),
            execution_partition_id: partition.into(),
            payload_hash: "h".into(),
            gate_epoch: epoch,
            fence_token: fence.into(),
            deadline_epoch_ms: 9_999_999_999_999,
            payload: serde_json::json!({}),
            authentication: String::new(),
        }
    }

    #[test]
    fn m1_1_recheck_refuses_when_the_gate_halts_before_send() {
        let state = ServerState::new(ExecState::Enabled);
        assert!(state
            .recheck_send(&recheck_envelope(1, "1", "part-1"))
            .is_none());
        state.safety_halt("operator halt between pre-check and send");
        let (status, doc) = state
            .recheck_send(&recheck_envelope(1, "1", "part-1"))
            .expect("a halt must refuse the send");
        assert_eq!(status, 503, "doc: {doc}");
        assert_eq!(doc["outcome"], "GATE_HALTED");
    }

    #[test]
    fn m1_1_recheck_refuses_a_stale_epoch_and_halts() {
        let state =
            ServerState::new(ExecState::Enabled).with_hydrated_term(7, 3, Some(now_ms() + 30_000));
        let (status, doc) = state
            .recheck_send(&recheck_envelope(6, "3", "part-1"))
            .expect("a stale epoch must refuse");
        assert_eq!(status, 409, "doc: {doc}");
        assert_eq!(doc["outcome"], "STALE_EPOCH");
        assert_eq!(state.snapshot().gate, ExecState::Halted);
    }

    #[test]
    fn m1_1_recheck_refuses_a_fence_mismatch_and_halts() {
        let state =
            ServerState::new(ExecState::Enabled).with_hydrated_term(7, 3, Some(now_ms() + 30_000));
        let (status, doc) = state
            .recheck_send(&recheck_envelope(7, "4", "part-1"))
            .expect("a fence mismatch must refuse");
        assert_eq!(status, 409, "doc: {doc}");
        assert_eq!(doc["outcome"], "FENCE_MISMATCH");
        assert_eq!(state.snapshot().gate, ExecState::Halted);

        // An unparseable fence is the same refusal, never a pass.
        let state =
            ServerState::new(ExecState::Enabled).with_hydrated_term(7, 3, Some(now_ms() + 30_000));
        let (_, doc) = state
            .recheck_send(&recheck_envelope(7, "not-a-number", "part-1"))
            .expect("an unparseable fence must refuse");
        assert_eq!(doc["outcome"], "FENCE_MISMATCH");
    }

    #[test]
    fn m1_1_recheck_refuses_an_expired_lease() {
        let state =
            ServerState::new(ExecState::Enabled).with_hydrated_term(7, 3, Some(now_ms() - 1));
        let (status, doc) = state
            .recheck_send(&recheck_envelope(7, "3", "part-1"))
            .expect("an expired lease must refuse");
        assert_eq!(status, 409, "doc: {doc}");
        assert_eq!(doc["outcome"], "STALE_EPOCH");
        assert_eq!(state.snapshot().gate, ExecState::Halted);

        // A missing lease is not live either.
        let state = ServerState::new(ExecState::Enabled).with_hydrated_term(7, 3, None);
        assert!(state
            .recheck_send(&recheck_envelope(7, "3", "part-1"))
            .is_some());
    }

    #[test]
    fn m1_1_recheck_accepts_a_matching_generation() {
        let state =
            ServerState::new(ExecState::Enabled).with_hydrated_term(7, 3, Some(now_ms() + 30_000));
        assert!(state
            .recheck_send(&recheck_envelope(7, "3", "part-1"))
            .is_none());
        assert_eq!(state.snapshot().gate, ExecState::Enabled);
    }

    #[test]
    fn m1_1_recheck_ignores_the_term_before_hydration() {
        // Flag-off/paper: no durable generation was adopted, so a stale-looking epoch is not
        // evidence that we are stale — the local gate check is the whole gate.
        let state = ServerState::new(ExecState::Enabled);
        assert!(state
            .recheck_send(&recheck_envelope(999, "junk", "part-1"))
            .is_none());
    }

    #[test]
    fn m1_1_wrong_partition_is_403_and_never_halts() {
        let before = crate::telemetry::METRICS
            .order_denied_wrong_partition
            .load(Ordering::Relaxed);
        let state = ServerState::new(ExecState::Enabled)
            .with_execution_partition(Some("dev-partition".into()))
            .with_hydrated_term(7, 3, Some(now_ms() + 30_000));
        let (status, doc) = state
            .recheck_send(&recheck_envelope(7, "3", "part-other"))
            .expect("a wrong partition must refuse");
        assert_eq!(status, 403, "doc: {doc}");
        assert_eq!(doc["outcome"], "WRONG_PARTITION");
        assert_eq!(
            state.snapshot().gate,
            ExecState::Enabled,
            "a routing error must not halt the gate"
        );
        let after = crate::telemetry::METRICS
            .order_denied_wrong_partition
            .load(Ordering::Relaxed);
        assert_eq!(after, before + 1, "the refusal is counted");
        // The matching partition passes on to the term checks and the send.
        assert!(state
            .recheck_send(&recheck_envelope(7, "3", "dev-partition"))
            .is_none());
    }

    #[tokio::test]
    async fn m1_1_a_refused_send_leaves_no_durable_attempt() {
        let dir = guard_scratch_dir();
        let store = Arc::new(FileAttemptStore::open(&dir.join(ATTEMPTS_LOG)).unwrap());
        let guard = live_guard(&store);
        let state = enabled_state(fake_forwarder(CommandScript::Accept))
            .with_attempts(Arc::clone(&guard))
            .with_hydrated_term(7, 3, Some(now_ms() + 30_000));
        let body = intent_body().await;
        let (status, resp) = post_intent(&state, &body).await;
        assert_eq!(status, 409, "body: {resp}");
        assert!(resp.contains("\"outcome\":\"STALE_EPOCH\""), "body: {resp}");
        assert_eq!(state.snapshot().gate, ExecState::Halted);
        assert!(
            !store.has_instruction("T9-SB-0001", "place"),
            "a refused send must leave no durable attempt behind (recheck -> claim -> send)"
        );
        drop(state);
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
