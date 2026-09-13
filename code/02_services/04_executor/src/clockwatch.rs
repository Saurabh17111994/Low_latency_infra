//! Clock-drift safety enforcement (plan Task B8 / CHG-064).
//!
//! `CLOCK_OFFSET_LIMIT_MS` was declared in compose for ingestion (enforced there by
//! `NtpClockChecker`) but nothing on the execution side read or enforced it. This module
//! closes that gap: a [`DriftMonitor`] samples the measured host-clock offset against UTC
//! from an injectable [`OffsetSource`] and enforces fail-closed semantics on the [`Gate`]:
//!
//! - `|offset| > limit`  -> `Beyond` -> `gate.safety_halt()` (approvals cleared, HALTED;
//!   re-enable ONLY through the sanctioned reconcile -> approval -> enable path).
//! - probe failure       -> `Unmeasurable` -> same fail-closed halt (never trust silence).
//! - `|offset| <= limit` -> `Within` (no action; halting is never automatic on recovery).
//!
//! The offline slice uses [`FixedOffsetSource`]; the live slice (Workstream D) swaps in a
//! real NTP/chrony source behind the same trait — identical to how the durable stores are
//! swapped behind `AttemptStore`/`GateStateStore`.

use anyhow::Result;

use crate::gate::Gate;

/// Where measured clock offsets come from (production: NTP/chrony; tests: fixed).
pub trait OffsetSource {
    /// Measured offset of the local clock vs the UTC reference, in milliseconds.
    /// Positive = local clock ahead. An `Err` means "cannot measure" (fail-closed).
    fn sample_offset_ms(&mut self) -> Result<i64>;
}

/// Deterministic source for offline proofs and unit tests.
#[derive(Debug, Clone)]
pub struct FixedOffsetSource(pub i64);

impl OffsetSource for FixedOffsetSource {
    fn sample_offset_ms(&mut self) -> Result<i64> {
        Ok(self.0)
    }
}

/// A probe that always fails — models NTP outage / unreachable reference.
#[derive(Debug, Default, Clone)]
pub struct FailingSource;

impl OffsetSource for FailingSource {
    fn sample_offset_ms(&mut self) -> Result<i64> {
        Err(anyhow::anyhow!("time reference unreachable"))
    }
}

/// Terminal classification of one drift sample.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriftStatus {
    Within(i64),
    Beyond(i64),
    Unmeasurable(String),
}

/// Fail-closed drift monitor bound to a configurable offset limit.
pub struct DriftMonitor {
    limit_ms: i64,
    source: Box<dyn OffsetSource>,
}

impl DriftMonitor {
    pub fn new(limit_ms: i64, source: Box<dyn OffsetSource>) -> Self {
        Self { limit_ms, source }
    }

    pub fn limit_ms(&self) -> i64 {
        self.limit_ms
    }

    /// Samples once and classifies against the limit (symmetric in sign).
    pub fn check(&mut self) -> DriftStatus {
        match self.source.sample_offset_ms() {
            Ok(offset) if offset.abs() > self.limit_ms => DriftStatus::Beyond(offset),
            Ok(offset) => DriftStatus::Within(offset),
            Err(e) => DriftStatus::Unmeasurable(e.to_string()),
        }
    }

    /// Samples and ENFORCES on the gate: `Beyond`/`Unmeasurable` trigger
    /// `safety_halt()` (idempotent — repeated breaches never inflate the count).
    /// Recovery from a drift halt is NEVER automatic: only the sanctioned human path.
    pub fn enforce(&mut self, gate: &mut Gate) -> DriftStatus {
        let status = self.check();
        match &status {
            DriftStatus::Within(offset) => {
                tracing::debug!(
                    offset_ms = offset,
                    limit_ms = self.limit_ms,
                    "clock drift within limit"
                );
            }
            DriftStatus::Beyond(offset) => {
                tracing::error!(
                    offset_ms = offset,
                    limit_ms = self.limit_ms,
                    "CLOCK DRIFT BEYOND LIMIT — safety halt; orders refused until re-enabled via sanctioned path"
                );
                gate.safety_halt();
            }
            DriftStatus::Unmeasurable(err) => {
                tracing::error!(
                    error = %err,
                    limit_ms = self.limit_ms,
                    "CLOCK DRIFT UNMEASURABLE — failing closed via safety halt"
                );
                gate.safety_halt();
            }
        }
        status
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gate::ExecState;

    fn monitor(offset: i64) -> DriftMonitor {
        DriftMonitor::new(200, Box::new(FixedOffsetSource(offset)))
    }

    #[test]
    fn boundary_just_inside_passes_just_outside_halts() {
        // Exactly at the limit is WITHIN (strict > comparison).
        assert_eq!(monitor(200).check(), DriftStatus::Within(200));
        assert_eq!(monitor(-200).check(), DriftStatus::Within(-200));
        // One millisecond beyond, either direction, is BEYOND.
        assert_eq!(monitor(201).check(), DriftStatus::Beyond(201));
        assert_eq!(monitor(-201).check(), DriftStatus::Beyond(-201));
    }

    #[test]
    fn unmeasurable_probe_fails_closed() {
        let mut m = DriftMonitor::new(200, Box::new(FailingSource));
        assert!(matches!(m.check(), DriftStatus::Unmeasurable(_)));
    }

    #[test]
    fn beyond_limit_halts_gate_and_clears_approvals() {
        let mut g = Gate::new();
        g.add_authorized("saurabh");
        g.set_epoch(5).unwrap();
        // Drive the sanctioned path to ENABLED.
        g.transition(ExecState::Reconciling).unwrap();
        g.transition(ExecState::ApprovalPending).unwrap();
        g.record_approval("saurabh", "evidence-1").unwrap();
        g.record_approval("saurabh", "evidence-2").unwrap();
        g.enable(g.epoch()).unwrap();
        assert_eq!(g.state(), ExecState::Enabled);

        let mut m = monitor(500);
        let status = m.enforce(&mut g);
        assert_eq!(status, DriftStatus::Beyond(500));
        assert_eq!(g.state(), ExecState::Halted);
        assert!(!g.can_execute());
    }

    #[test]
    fn repeated_breaches_are_idempotent_no_halt_count_inflation() {
        let mut g = Gate::new();
        let mut m = monitor(999);
        m.enforce(&mut g);
        let after_first = g.safety_halt_count();
        m.enforce(&mut g);
        m.enforce(&mut g);
        assert_eq!(
            g.safety_halt_count(),
            after_first,
            "already halted stays halted"
        );
    }

    #[test]
    fn within_limit_never_touches_the_gate() {
        let mut g = Gate::new();
        g.transition(ExecState::Reconciling).unwrap();
        let mut m = monitor(42);
        assert_eq!(m.enforce(&mut g), DriftStatus::Within(42));
        assert_eq!(
            g.state(),
            ExecState::Reconciling,
            "healthy drift must not halt"
        );
        assert_eq!(g.safety_halt_count(), 0);
    }

    #[test]
    fn drift_halt_recovers_only_via_sanctioned_path() {
        let mut g = Gate::new();
        g.add_authorized("saurabh");
        g.set_epoch(5).unwrap();
        let mut m = monitor(-1000); // large negative drift
        m.enforce(&mut g);
        assert_eq!(g.state(), ExecState::Halted);

        // Direct transition back to ENABLED is forbidden (INVARIANT-003).
        assert!(g.transition(ExecState::Enabled).is_err());
        // Even drift returning to normal must NOT auto-recover.
        let mut healthy = monitor(1);
        healthy.enforce(&mut g);
        assert_eq!(g.state(), ExecState::Halted, "no automatic recovery");
        // Only the sanctioned human path recovers.
        g.transition(ExecState::Reconciling).unwrap();
        g.transition(ExecState::ApprovalPending).unwrap();
        g.record_approval("saurabh", "drift-resolved-evidence")
            .unwrap();
        g.record_approval("saurabh", "drift-resolved-evidence-2")
            .unwrap();
        g.enable(g.epoch()).unwrap();
        assert_eq!(g.state(), ExecState::Enabled);
    }

    #[test]
    fn unmeasurable_enforcement_halts_too() {
        let mut g = Gate::new();
        let mut m = DriftMonitor::new(200, Box::new(FailingSource));
        let status = m.enforce(&mut g);
        assert!(matches!(status, DriftStatus::Unmeasurable(_)));
        assert_eq!(g.state(), ExecState::Halted);
    }
}
