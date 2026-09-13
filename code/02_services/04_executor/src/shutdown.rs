//! Clean shutdown and restart-recovery (T4.11 / T4.13).
//!
//! The service is **fail-closed** on shutdown: stop accepting new inbound commands, abandon any
//! queued (in-flight) bridge jobs as unresolved attempts, flush remaining event evidence, and
//! release the execution fence back to `HALTED`. A restarted process must never auto-retry an
//! abandoned attempt — it boots `HALTED` and [`verify_restart_safe`] asserts that invariant.

use crate::{execution::client::BridgeExecutionClient, gate::ExecState};
use anyhow::Result;

/// The clean-shutdown phase the coordinator is currently in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownPhase {
    /// Not shutting down; accepting inbound commands.
    Running,
    /// Stopped accepting new commands; gate safety-halted.
    Draining,
    /// Queued bridge jobs abandoned as unresolved attempts; reports flushed.
    Flushed,
    /// Execution fence released (gate HALTED).
    FenceReleased,
    /// Shutdown sequence complete; service no longer trading-ready.
    Complete,
}

/// Outcome of a clean shutdown, for callers and evidence bundles.
#[derive(Debug, Clone)]
pub struct ShutdownReport {
    /// Terminal phase (always [`ShutdownPhase::Complete`] on success).
    pub phase: ShutdownPhase,
    /// Bridge jobs abandoned without a broker round-trip (must never be auto-retried).
    pub unresolved_attempts: usize,
    /// Gate state after shutdown (always [`ExecState::Halted`] for fail-closed).
    pub gate_state: ExecState,
    /// Trading-ready must be false after shutdown.
    pub trading_ready: bool,
}

/// Drives the fail-closed shutdown sequence against a bridge client.
#[derive(Debug)]
pub struct ShutdownCoordinator {
    phase: ShutdownPhase,
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl ShutdownCoordinator {
    /// A fresh coordinator starts `Running`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            phase: ShutdownPhase::Running,
        }
    }

    /// Current shutdown phase.
    #[must_use]
    pub fn phase(&self) -> ShutdownPhase {
        self.phase
    }

    /// Whether the shutdown sequence has completed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.phase == ShutdownPhase::Complete
    }

    /// Executes the shutdown sequence against a bridge client:
    ///
    /// 1. **Stop ingress** — safety-halt the gate (`HALTED`), so no new broker command can be
    ///    emitted and the execution fence is released (fail-closed).
    /// 2. **Drain** — abandon queued bridge jobs without executing them, recording each as an
    ///    unresolved attempt (an in-flight attempt that never reached the broker).
    /// 3. **Flush** — drain remaining asynchronous reports so event evidence is emitted.
    /// 4. **Complete** — report the terminal state; the service is no longer trading-ready.
    pub async fn shutdown(&mut self, client: &mut BridgeExecutionClient) -> Result<ShutdownReport> {
        // 1. Stop ingress + release the fence: the gate returns to HALTED.
        client.safety_halt();
        self.phase = ShutdownPhase::Draining;

        // 2. Abandon queued in-flight jobs as unresolved attempts (never sent / never retried).
        let unresolved_attempts = client.abort_pending_as_unresolved();
        self.phase = ShutdownPhase::Flushed;

        // 3. Flush remaining asynchronous event evidence.
        client.flush_reports();
        self.phase = ShutdownPhase::FenceReleased;

        let gate_state = client.gate_state();
        self.phase = ShutdownPhase::Complete;
        Ok(ShutdownReport {
            phase: self.phase,
            unresolved_attempts,
            gate_state,
            trading_ready: false,
        })
    }
}

/// Restart-recovery invariant: a service that shut down with (or without) unresolved attempts
/// must boot back into `HALTED`, so no abandoned attempt is auto-retried by the restarted
/// process. The gate is the single source of truth for this.
#[must_use]
pub fn verify_restart_safe(gate_state: ExecState) -> bool {
    gate_state == ExecState::Halted
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_common::{
        cache::Cache, clients::ExecutionClient, messages::execution::SubmitOrder,
    };
    use nautilus_core::{UnixNanos, UUID4};
    use nautilus_live::ExecutionClientCore;
    use nautilus_model::{
        enums::{AccountType, OmsType, OrderSide, OrderType},
        identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, Symbol, TraderId, Venue},
        orders::{Order, OrderTestBuilder},
        types::{Price, Quantity},
    };
    use std::{cell::RefCell, rc::Rc};

    /// A client booted `HALTED` with one cached order.
    fn base_client() -> (BridgeExecutionClient, nautilus_model::orders::OrderAny) {
        let instrument_id = InstrumentId::new(Symbol::from("NIFTY"), Venue::from("NFO"));
        let client_order_id = ClientOrderId::from("RND-0001");
        let order = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(instrument_id)
            .side(OrderSide::Buy)
            .quantity(Quantity::from(10))
            .price(Price::from("100"))
            .client_order_id(client_order_id)
            .build();
        let cache = Rc::new(RefCell::new(Cache::default()));
        cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .expect("order cached");
        let core = ExecutionClientCore::new(
            TraderId::from("TRADER-001"),
            ClientId::from("EXEC-001"),
            Venue::from("NFO"),
            OmsType::Netting,
            AccountId::from("EXEC-001"),
            AccountType::Cash,
            None,
            cache,
        );
        let client = BridgeExecutionClient::new(core, Box::new(crate::bridge::FakeBridge::new()));
        (client, order)
    }

    fn enable(client: &mut BridgeExecutionClient) {
        client
            .advance_gate(crate::gate::ExecState::Reconciling)
            .expect("sanctioned transition");
        client
            .advance_gate(crate::gate::ExecState::ApprovalPending)
            .expect("sanctioned transition");
        // ENABLED is reachable only through the single-operator (saurabh, DEC-044)
        // approval gate (INVARIANT-003).
        let mut g = client.gate().borrow_mut();
        g.add_authorized("saurabh");
        g.set_epoch(1).expect("epoch declared");
        g.record_approval("saurabh", "h1")
            .expect("saurabh approval");
        g.enable(1).expect("single-operator enable");
    }

    fn submit_place(client: &BridgeExecutionClient, order: &nautilus_model::orders::OrderAny) {
        let submit = SubmitOrder::from_order(
            order,
            TraderId::from("TRADER-001"),
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
        );
        client.submit_order(submit).expect("enqueued");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fresh_shutdown_halts_gate_and_is_not_trading_ready() {
        let (mut client, _order) = base_client();
        let mut coord = ShutdownCoordinator::new();
        let report = coord.shutdown(&mut client).await.unwrap();

        assert!(coord.is_closed());
        assert_eq!(report.phase, ShutdownPhase::Complete);
        assert_eq!(report.gate_state, ExecState::Halted);
        assert!(!report.trading_ready);
        assert_eq!(report.unresolved_attempts, 0);
        assert_eq!(client.gate_state(), ExecState::Halted);
        assert_eq!(client.pending_count(), 0);
        assert!(verify_restart_safe(client.gate_state()));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_with_inflight_job_reports_unresolved() {
        let (mut client, order) = base_client();
        enable(&mut client);
        submit_place(&client, &order); // queued but not pumped -> in-flight, un-sent
        assert_eq!(client.pending_count(), 1);

        let mut coord = ShutdownCoordinator::new();
        let report = coord.shutdown(&mut client).await.unwrap();

        assert_eq!(report.unresolved_attempts, 1);
        assert_eq!(report.gate_state, ExecState::Halted);
        assert_eq!(client.pending_count(), 0);
        assert!(verify_restart_safe(client.gate_state()));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn restart_boots_halted_and_refuses_the_queued_order() {
        // Shut down a client that had one unresolved in-flight attempt.
        let (mut c1, order1) = base_client();
        enable(&mut c1);
        submit_place(&c1, &order1);
        let mut coord = ShutdownCoordinator::new();
        let report = coord.shutdown(&mut c1).await.unwrap();
        assert_eq!(report.unresolved_attempts, 1);
        assert!(verify_restart_safe(c1.gate_state()));

        // What follows is a *fresh* client (`base_client()`), not a restore of `c1`:
        // no order/job identity crosses over. All this test proves is that a fresh boot
        // comes up HALTED and refuses the queued order, which never reaches the bridge.
        // The durable half (a restarted process must not re-issue the attempt abandoned by
        // `c1`) lives at the gate level, exercised by
        // `crash_after_submitting_halts_with_zero_duplicate_calls` in `src/executiongate.rs`,
        // because the live client does not yet consume the durable ExecutionGate stores —
        // the persisted gate is where a cross-process restore can actually be replayed.
        let (mut fresh, order2) = base_client();
        assert_eq!(fresh.gate_state(), ExecState::Halted);
        assert!(verify_restart_safe(fresh.gate_state()));
        assert!(!fresh.gate().borrow().can_execute());

        // A submit while HALTED is denied and never reaches the bridge.
        submit_place(&fresh, &order2);
        fresh.process_pending().await.expect("no bridge job ran");
        assert_eq!(fresh.gate_state(), ExecState::Halted);
        assert_eq!(fresh.pending_count(), 0);
        assert_eq!(
            fresh.position(&order2.instrument_id()),
            rust_decimal::Decimal::from(0)
        );
    }
}
