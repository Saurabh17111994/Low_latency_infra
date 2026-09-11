//! Babysitter MVP no-op observer — dossier §State machines Babysitter (WP-6 / B5).
//!
//! MVP is observation-only: consumes position snapshots, emits no order
//! actions, counts positions by state and no-op decisions by reason.
//! Respects `POSITION_ACTIONS_ENABLED=false` fail-closed invariant — when
//! true the observer construction fails (no Position_Actions writes).

use std::collections::HashMap;

use crate::projection::{PositionSnapshot, PositionState};

/// No-op babysitter observer — observation only, never emits trade actions.
#[derive(Debug, Default)]
pub struct NoOpPositionObserver {
    positions_by_state: HashMap<PositionState, usize>,
    no_op_decisions: HashMap<String, usize>,
}

impl NoOpPositionObserver {
    /// Create observer. Fails closed if `position_actions_enabled` is true — MVP must
    /// never write Position_Actions; production path blocked until proof.
    pub fn new(position_actions_enabled: bool) -> Result<Self, &'static str> {
        if position_actions_enabled {
            return Err("POSITION_ACTIONS_ENABLED must be false for MVP no-op babysitter");
        }
        // P3-419: no enabled field — it could only ever hold false (new()
        // errs on true), so the fail-closed invariant lives in this check.
        Ok(Self::default())
    }

    /// Observe a position snapshot — counts by state, never emits an action.
    pub fn observe(&mut self, snapshot: &PositionSnapshot) {
        let c = self.positions_by_state.entry(snapshot.state).or_insert(0);
        *c += 1;
        // No trade action emitted — record no-op by reason for audit.
        // P3-181 won't-fix: the format! alloc is noise on this cold
        // observation-only path, and &'static str keys would break the
        // no_op_count(substring) audit API the tests rely on.
        let reason = format!("no-op: state={:?}", snapshot.state);
        *self.no_op_decisions.entry(reason).or_insert(0) += 1;
    }

    /// Observe without a snapshot (e.g. FLAT with no open position) — still no-op.
    pub fn observe_empty(&mut self) {
        *self
            .no_op_decisions
            .entry("no-op: flat".to_string())
            .or_insert(0) += 1;
    }

    pub fn positions_by_state(&self, state: PositionState) -> usize {
        *self.positions_by_state.get(&state).unwrap_or(&0)
    }

    pub fn no_op_count(&self, reason_substr: &str) -> usize {
        self.no_op_decisions
            .iter()
            .filter(|(k, _)| k.contains(reason_substr))
            .map(|(_, v)| v)
            .sum()
    }

    pub fn total_observations(&self) -> usize {
        self.positions_by_state.values().sum()
    }

    pub fn total_no_ops(&self) -> usize {
        self.no_op_decisions.values().sum()
    }

    /// Invariant: never emits trade actions (always 0 actions).
    pub fn emitted_actions(&self) -> usize {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projection::{PositionSnapshot, PositionState, Side};

    fn snapshot(state: PositionState) -> PositionSnapshot {
        PositionSnapshot {
            position_id: "pos-1".into(),
            trade_context_id: "ctx".into(),
            account_scope_id: "acc".into(),
            instrument_token: 101,
            exchange: "NSE".into(),
            symbol: "INFY".into(),
            side: Side::Buy,
            state,
            open_quantity: if state == PositionState::Flat { 0 } else { 10 },
            closed_quantity: if state == PositionState::Closed {
                10
            } else {
                0
            },
            average_entry_paise: 100,
            average_exit_paise: 0,
            source_event_id: "evt".into(),
            source_version: 1,
            created_ts: 0,
            last_update_ts: 0,
            schema_version: "v2".into(),
        }
    }

    #[test]
    fn babysitter_mvp_is_observation_only_never_emits_actions() {
        let mut obs = NoOpPositionObserver::new(false).unwrap();
        obs.observe(&snapshot(PositionState::Open));
        obs.observe(&snapshot(PositionState::Reducing));
        obs.observe(&snapshot(PositionState::Closed));
        obs.observe_empty();

        assert_eq!(obs.total_observations(), 3);
        assert_eq!(obs.positions_by_state(PositionState::Open), 1);
        assert_eq!(obs.positions_by_state(PositionState::Reducing), 1);
        assert_eq!(obs.positions_by_state(PositionState::Closed), 1);
        assert_eq!(
            obs.emitted_actions(),
            0,
            "MVP must never emit trade actions"
        );
        assert!(obs.total_no_ops() >= 4);
    }

    #[test]
    fn babysitter_fail_closed_when_position_actions_enabled() {
        let err = NoOpPositionObserver::new(true).unwrap_err();
        assert!(
            err.contains("POSITION_ACTIONS_ENABLED"),
            "must fail-closed when enabled"
        );
    }

    #[test]
    fn babysitter_counts_positions_by_state_and_no_op_by_reason() {
        let mut obs = NoOpPositionObserver::new(false).unwrap();
        obs.observe(&snapshot(PositionState::Open));
        obs.observe(&snapshot(PositionState::Open));
        obs.observe(&snapshot(PositionState::Flat));
        assert_eq!(obs.positions_by_state(PositionState::Open), 2);
        assert_eq!(obs.total_no_ops(), 3);
        assert_eq!(obs.emitted_actions(), 0);
    }
}
