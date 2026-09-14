//! Service bootstrap and runtime handle (WP-1).
//!
//! [`Runtime::init`] constructs the fail-closed boot surface: a gate that always starts `HALTED`,
//! a health snapshot that never implies `ENABLED`, and the shared [`ServerState`] the health
//! server reads. `main` calls `init` and then serves health until a shutdown signal arrives;
//! shutdown halts the gate before draining (see [`Runtime::begin_shutdown`]).

use anyhow::{ensure, Result};

use crate::clockwatch::{DriftMonitor, DriftStatus};
use crate::config::ServiceConfig;
use crate::engine::EngineFactory;
use crate::gate::{ExecState, Gate};
use crate::health::HealthStatus;
use crate::http::{self, ServerState};

/// The alive, booted service handle.
#[derive(Debug)]
pub struct Runtime {
    pub config: ServiceConfig,
    gate: Gate,
    state: ServerState,
}

impl Runtime {
    /// Builds the boot surface and asserts the fail-closed invariants:
    /// gate `HALTED` and health does not imply `ENABLED`.
    pub fn init(config: ServiceConfig) -> Result<Self> {
        // Deliberate boot-time smoke test of the pinned `nautilus-live` wiring (P3-424). It runs
        // on every boot rather than only under `#[cfg(test)]` because `LiveNodeBuilder::from_config`
        // and `add_exec_client` report their failures at runtime (both return `Result`); a
        // compile-time/type check cannot see those, and a test-only check never runs against the
        // deployed binary. Failing here aborts the boot before the service advertises health, so a
        // broken constructor or exec-client registration surfaces as a failed start — not at the
        // first order.
        EngineFactory::verify_construction_path()?;

        let gate = Gate::new();
        ensure!(
            gate.state() == ExecState::Halted,
            "service must boot HALTED"
        );
        let health = HealthStatus::new(gate.state());
        ensure!(
            health.health_does_not_imply_enabled(),
            "health must not imply ENABLED at boot"
        );
        let state = if config.gateway_shared_secret.is_empty() {
            ServerState::new(gate.state())
        } else {
            ServerState::with_gateway_auth(
                gate.state(),
                config.gateway_shared_secret.clone(),
                config.protocol_version.clone(),
            )
        };
        Ok(Self {
            config,
            gate,
            state,
        })
    }

    /// Current gate state. `HALTED` at boot and again after any safety halt; **not** monotonic:
    /// the sanctioned enablement path advances it, and `Gate::safety_halt` (clock drift, operator
    /// halt, shutdown) restores `HALTED` from any state. Booting `HALTED` is the guarantee.
    pub fn gate_state(&self) -> ExecState {
        self.gate.state()
    }

    /// Shared health state for the HTTP server.
    pub fn server_state(&self) -> ServerState {
        self.state.clone()
    }

    /// Health document for `/healthz`.
    pub fn health_json(&self) -> serde_json::Value {
        http::health_json(&self.state)
    }

    /// Samples clock drift and enforces it on the gate (B8): `Beyond`/`Unmeasurable`
    /// trigger `safety_halt()`; recovery is only ever via the sanctioned human path.
    /// The live NTP source is wired in Workstream D behind `OffsetSource`.
    ///
    /// Halting `self.gate` alone is not enough (D4): `/v1/intents` and `/healthz` read the shared
    /// `ServerState` snapshot, so a drift halt must halt that surface too — otherwise the live
    /// intent route keeps forwarding after the gate has been halted (fail-open), while health
    /// still reports ENABLED. Same shape P3-185 fixed for shutdown.
    pub fn enforce_clock_drift(&mut self, monitor: &mut DriftMonitor) -> DriftStatus {
        let status = monitor.enforce(&mut self.gate);
        if matches!(
            status,
            DriftStatus::Beyond(_) | DriftStatus::Unmeasurable(_)
        ) {
            self.state.safety_halt("clock drift beyond limit");
        }
        status
    }

    /// Starts graceful shutdown: the gate is safety-halted **first** (clearing approvals and the
    /// bound evidence, so no new broker command can be emitted) and then `/readyz` returns 503.
    /// The order is deliberate (P3-185): a draining service must never still be armed to execute.
    pub fn begin_shutdown(&mut self) {
        self.gate.safety_halt();
        // The shared snapshot is the surface `/v1/intents` checks, so halting `self.gate` alone
        // would leave the live intent route forwarding while we drain.
        self.state.safety_halt("service shutdown");
        self.state.set_draining(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clockwatch::{FailingSource, FixedOffsetSource};
    use crate::config::ServiceConfig;

    fn halted_config() -> ServiceConfig {
        ServiceConfig {
            gateway_endpoint: String::new(),
            bridge_endpoint: String::new(),
            bridge_auth_token: String::new(),
            log_level: "info".into(),
            execution_enabled: false,
            listen_addr: "127.0.0.1:8787".into(),
            gateway_shared_secret: String::new(),
            protocol_version: "execution-gateway.v1".into(),
            clock_offset_limit_ms: 200,
            durable_gate_enabled: false,
            durable_attempts_enabled: false,
            durable_journal_enabled: false,
            durable_audit_enabled: false,
            durable_dir: "data/durable".into(),
            execution_partition_id: None,
        }
    }

    #[test]
    fn runtime_boots_halted_and_not_trading() {
        let rt = Runtime::init(halted_config()).unwrap();
        assert_eq!(rt.gate_state(), ExecState::Halted);
        let h = rt.health_json();
        assert_eq!(h["gate_state"], "HALTED");
        assert_eq!(h["trading_ready"], false);
        assert_eq!(h["enabled"], false);
        // Health being alive must not imply ENABLED trading.
        assert_ne!(h["gate_state"], "ENABLED");
    }

    #[test]
    fn begin_shutdown_marks_draining() {
        let mut rt = Runtime::init(halted_config()).unwrap();
        assert!(!rt.health_json()["draining"].as_bool().unwrap());
        rt.begin_shutdown();
        assert!(rt.health_json()["draining"].as_bool().unwrap());
    }

    /// Drives the runtime's authoritative `Gate` through the sanctioned enablement path:
    /// authorized operator + declared epoch + approval bound to the evidence hash.
    fn enable_runtime_gate(rt: &mut Runtime) {
        rt.gate.add_authorized("saurabh");
        rt.gate.transition(ExecState::Reconciling).unwrap();
        rt.gate.transition(ExecState::ApprovalPending).unwrap();
        rt.gate.set_epoch(1).unwrap();
        rt.gate.record_approval("saurabh", "ev-shutdown").unwrap();
        rt.gate.enable(1).unwrap();
    }

    #[test]
    fn begin_shutdown_halts_the_gate_before_draining() {
        // P3-185: a draining service must never still be armed to execute. Shutdown used to
        // only flip the draining flag, so an ENABLED gate kept accepting `/v1/intents` through
        // the shared snapshot while `/readyz` reported 503 — a fail-open drain.
        let mut rt = Runtime::init(halted_config()).unwrap();
        enable_runtime_gate(&mut rt);
        // The served snapshot is the surface the intent route checks; enable it too so the test
        // covers the live approval path, not just the in-process gate.
        rt.state.approve("saurabh", "ev-shutdown").unwrap();
        assert_eq!(rt.gate_state(), ExecState::Enabled);
        assert_eq!(rt.health_json()["gate_state"], "ENABLED");

        rt.begin_shutdown();

        let h = rt.health_json();
        assert!(
            !rt.gate.can_execute(),
            "shutdown must halt the gate, not only flag draining"
        );
        assert_eq!(rt.gate_state(), ExecState::Halted);
        assert_eq!(
            h["gate_state"], "HALTED",
            "the served snapshot must stop accepting intents on shutdown"
        );
        assert!(h["draining"].as_bool().unwrap());
    }

    #[test]
    fn clock_drift_enforcement_uses_configured_limit_and_stays_fail_closed() {
        // Ties the config value (CLOCK_OFFSET_LIMIT_MS=200) through the runtime's
        // enforce_clock_drift to the DriftMonitor classification: at the limit it is
        // WITHIN (no halt — the gate was already HALTED at boot and stays so), and an
        // unmeasurable probe fails closed to HALTED rather than ever opening the gate.
        let mut rt = Runtime::init(halted_config()).unwrap();
        let mut within = DriftMonitor::new(200, Box::new(FixedOffsetSource(200)));
        assert_eq!(
            rt.enforce_clock_drift(&mut within),
            DriftStatus::Within(200)
        );
        assert_eq!(rt.gate_state(), ExecState::Halted);

        // The offline slice boots HALTED, so a beyond-limit sample cannot "halt" further —
        // the fail-closed contract is that it must NEVER leave HALTED. An unmeasurable
        // probe exercises the same enforcement path and must keep the gate HALTED.
        let mut beyond = DriftMonitor::new(200, Box::new(FixedOffsetSource(500)));
        assert_eq!(
            rt.enforce_clock_drift(&mut beyond),
            DriftStatus::Beyond(500)
        );
        let mut failing = DriftMonitor::new(200, Box::new(FailingSource));
        assert!(matches!(
            rt.enforce_clock_drift(&mut failing),
            DriftStatus::Unmeasurable(_)
        ));
        assert_eq!(
            rt.gate_state(),
            ExecState::Halted,
            "fail-closed: never leaves HALTED"
        );
    }

    #[test]
    fn drift_halt_also_halts_the_served_snapshot() {
        // D4: `enforce_clock_drift` halted only the in-process gate, while `/v1/intents` checks
        // the shared snapshot. A drift halt therefore left the live intent route forwarding and
        // `/healthz` reporting ENABLED — fail-open, the same shape P3-185 fixed for shutdown.
        let mut rt = Runtime::init(halted_config()).unwrap();
        enable_runtime_gate(&mut rt);
        rt.state.approve("saurabh", "ev-drift").unwrap();
        assert_eq!(rt.gate_state(), ExecState::Enabled);
        assert_eq!(rt.health_json()["gate_state"], "ENABLED");

        let mut beyond = DriftMonitor::new(200, Box::new(FixedOffsetSource(500)));
        assert_eq!(
            rt.enforce_clock_drift(&mut beyond),
            DriftStatus::Beyond(500)
        );

        assert!(!rt.gate.can_execute(), "a drift halt must halt the gate");
        assert_eq!(rt.gate_state(), ExecState::Halted);
        assert_eq!(
            rt.health_json()["gate_state"],
            "HALTED",
            "the served snapshot must stop accepting intents on a drift halt"
        );
    }
}
