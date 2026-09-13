use crate::gate::ExecState;

/// Health dimensions — process health never implies ENABLED trading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthStatus {
    pub process_alive: bool,
    pub readiness: bool,
    pub gate_state: ExecState,
    pub trading_ready: bool,
}

impl HealthStatus {
    pub fn new(gate_state: ExecState) -> Self {
        Self {
            process_alive: true,
            readiness: true,
            gate_state,
            trading_ready: gate_state == ExecState::Enabled,
        }
    }
    pub fn trading_implies_enabled(&self) -> bool {
        !self.trading_ready || self.gate_state == ExecState::Enabled
    }

    /// Fail-closed direction: liveness and readiness never *amount to* trading enablement.
    /// Asserted at boot (`Runtime::init`), by the T9 paper harness, and in client tests.
    ///
    /// `readiness` is deliberately **not** a term. A process that is not ready is not
    /// trading-enabled either, so folding readiness in would make the invariant vacuous in
    /// exactly the degraded case it exists to survive — and would silently turn a
    /// fail-closed boot assertion into a no-op. (`readiness` is `true` by construction in
    /// [`Self::new`]; no code path sets it `false` today.)
    pub fn health_does_not_imply_enabled(&self) -> bool {
        self.process_alive && self.gate_state != ExecState::Enabled && !self.trading_ready
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gate::ExecState;
    #[test]
    fn halted_not_trading() {
        let h = HealthStatus::new(ExecState::Halted);
        assert!(!h.trading_ready);
        assert!(h.health_does_not_imply_enabled());
    }
    #[test]
    fn enabled_trading() {
        let h = HealthStatus::new(ExecState::Enabled);
        assert!(h.trading_ready);
        assert!(h.trading_implies_enabled());
    }

    #[test]
    fn not_ready_health_still_does_not_imply_enabled() {
        // Guards the fail-closed direction: `readiness` must never become a term of the
        // invariant, or a degraded dependency would switch the boot/T9 assertion off.
        let mut h = HealthStatus::new(ExecState::Halted);
        h.readiness = false;
        assert!(h.health_does_not_imply_enabled());
    }

    #[test]
    fn enabled_gate_breaks_the_invariant() {
        // Other direction, so the predicate cannot be a constant `true` that proves nothing.
        let h = HealthStatus::new(ExecState::Enabled);
        assert!(!h.health_does_not_imply_enabled());
    }
}
