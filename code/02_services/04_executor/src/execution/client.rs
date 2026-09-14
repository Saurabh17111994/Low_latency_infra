//! Custom Nautilus [`ExecutionClient`] backed by an upstream bridge.
//!
//! `BridgeExecutionClient` is the long-lived execution/position authority of the service. It is
//! built on the audited Nautilus Rust execution APIs: an [`ExecutionClientCore`] (cache, connected
//! and started state) and an [`ExecutionEventEmitter`] (order/report/account event generation).
//!
//! Command flow is *enqueue-then-pump*: the synchronous `ExecutionClient` trait methods
//! ([`Self::submit_order`], [`Self::modify_order`], [`Self::cancel_order`]) validate the safety
//! gate and enqueue bridge jobs; an asynchronous pump ([`Self::process_pending`]) performs the
//! bridge round-trip and emits the resulting Nautilus events. This keeps the client usable from a
//! single-threaded (non-`Send`) Nautilus runtime context while the bridge I/O stays `async`.
//!
//! The service always boots into the safety gate `HALTED` state; while `HALTED` no broker command
//! is emitted and orders are denied. The only enablement path is
//! `HALTED -> RECONCILING -> APPROVAL_PENDING -> ENABLED`.

use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, VecDeque},
    rc::Rc,
    sync::atomic::Ordering,
};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use nautilus_common::{
    clients::ExecutionClient,
    live::runner::get_exec_event_sender,
    messages::execution::{
        BatchCancelOrders, BatchModifyOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
        GenerateOrderStatusReport, GenerateOrderStatusReports, GeneratePositionStatusReports,
        ModifyOrder, QueryAccount, QueryOrder, SubmitOrder, SubmitOrderList,
    },
};
use nautilus_core::{time::get_atomic_clock_realtime, Params, UnixNanos, UUID4};
use nautilus_live::{ExecutionClientCore, ExecutionEventEmitter};
use nautilus_model::{
    accounts::AccountAny,
    enums::{LiquiditySide, OmsType, OrderSide, OrderType as NautilusOrderType, TimeInForce},
    events::AccountState,
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, TradeId, Venue, VenueOrderId},
    instruments::Instrument,
    orders::{Order, OrderAny},
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Price, Quantity},
};
use sha2::{Digest, Sha256};

use crate::resilience::{AttemptError, RetryConfig, RetryError, RetryOrchestrator};

/// Deterministic broker-facing reference: sha256(format_version|instruction_id|execution_attempt_id) 14 hex chars.
/// Format_version is pinned to `v1` per 05-execution-core §Reconciliation (fits Arrow 16-char remarks).
pub(crate) fn deterministic_client_order_ref(
    format_version: &str,
    instruction_id: &str,
    execution_attempt_id: &str,
) -> String {
    let canonical = format!(
        "{}|{}|{}",
        format_version, instruction_id, execution_attempt_id
    );
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    let digest = hasher.finalize();
    let hex = hex::encode(digest);
    hex[..14].to_string()
}

/// Bridge outcome classification (dossier §Reconciliation, offline).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BridgeOutcome {
    Accepted,
    Rejected,
    Unknown,
}

/// Classify a synchronous bridge report. Pure function — no I/O, no retry.
pub fn classify_bridge_report(report: &crate::bridge::protocol::ReportEnvelope) -> BridgeOutcome {
    use crate::bridge::protocol::ReportOutcome;
    match report.outcome() {
        Some(ReportOutcome::Success) if !report.broker_order_id.is_empty() => {
            BridgeOutcome::Accepted
        }
        Some(ReportOutcome::Rejected) => BridgeOutcome::Rejected,
        Some(ReportOutcome::Unknown) => BridgeOutcome::Unknown,
        _ => BridgeOutcome::Unknown,
    }
}

/// UNKNOWN→HALT never retry guard.
pub fn unknown_halts_never_retries(outcome: BridgeOutcome) -> bool {
    outcome == BridgeOutcome::Unknown
}

#[cfg(test)]
mod deterministic_ref_tests {
    use super::deterministic_client_order_ref;

    #[test]
    fn deterministic_same_inputs_same_output() {
        let a = deterministic_client_order_ref("v1", "instr-1", "attempt-1");
        let b = deterministic_client_order_ref("v1", "instr-1", "attempt-1");
        assert_eq!(a, b);
    }

    #[test]
    fn deterministic_14_hex_chars() {
        let r = deterministic_client_order_ref("v1", "instr-1", "attempt-1");
        assert_eq!(r.len(), 14);
        assert!(r.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(r, r.to_lowercase());
    }

    #[test]
    fn deterministic_different_attempts_different_refs() {
        let r1 = deterministic_client_order_ref("v1", "instr-1", "attempt-1");
        let r2 = deterministic_client_order_ref("v1", "instr-1", "attempt-2");
        assert_ne!(r1, r2);
    }

    #[test]
    fn deterministic_different_instructions_different_refs() {
        let r1 = deterministic_client_order_ref("v1", "instr-1", "attempt-1");
        let r2 = deterministic_client_order_ref("v1", "instr-2", "attempt-1");
        assert_ne!(r1, r2);
    }

    #[test]
    fn deterministic_format_version_pinned() {
        let r_v1 = deterministic_client_order_ref("v1", "instr-1", "attempt-1");
        let r_v2 = deterministic_client_order_ref("v2", "instr-1", "attempt-1");
        assert_ne!(r_v1, r_v2);
    }

    #[test]
    fn deterministic_fits_arrow_16char_remarks() {
        let r = deterministic_client_order_ref("v1", "instr-1", "attempt-1");
        assert!(r.len() <= 16);
    }
}

use crate::{
    bridge::{
        protocol::{
            Command, CommandEnvelope, OrderCommand, OrderType as BridgeOrderType, Product,
            ReportEnvelope, TransactionType, Validity,
        },
        BridgeClient, BridgeReportStream,
    },
    gate::{ExecState, Gate},
};

/// A bridge command awaiting its round-trip in the pump.
struct PendingJob {
    command: Command,
    client_order_id: ClientOrderId,
    envelope: CommandEnvelope,
}

/// The custom Nautilus execution client.
///
/// Deliberately non-`Send`: it owns `Rc`/`RefCell` state (cache view, gate, bridge, report stream,
/// positions) and is intended to run on a single-threaded Nautilus runtime context.
pub struct BridgeExecutionClient {
    core: ExecutionClientCore,
    clock: &'static nautilus_core::time::AtomicTime,
    emitter: ExecutionEventEmitter,
    bridge: Rc<RefCell<Box<dyn BridgeClient>>>,
    gate: Rc<RefCell<Gate>>,
    /// Queued bridge jobs awaiting [`Self::process_pending`].
    pending: RefCell<VecDeque<PendingJob>>,
    /// Asynchronous bridge report stream (fills, order-state updates, rejections).
    reports: RefCell<Option<BridgeReportStream>>,
    /// Bounded backoff / retry-budget / circuit-breaker guard for the live bridge transport.
    resilience: RefCell<RetryOrchestrator>,
    /// Venue order id recorded per client order id after a successful place.
    orders: Rc<RefCell<HashMap<ClientOrderId, VenueOrderId>>>,
    /// Mapping from deterministic `client_order_ref` (remarks) to `ClientOrderId` for fill correlation.
    client_refs: Rc<RefCell<HashMap<String, ClientOrderId>>>,
    /// Node-driven progress counter (P3-223): bumped on every mass-status callback the live
    /// runtime makes, so an observer outside the node can tell a running loop from a wedged one
    /// whose running flag never clears.
    progress: Rc<Cell<u64>>,
    /// Net position (signed quantity) tracked per instrument from fills.
    /// Parity reference only — Nautilus `portfolio` is the production authority (dossier §Position-management
    /// `PositionProjector` is differential-test oracle). This map stays for offline fake-bridge parity;
    /// `position()` will delegate to portfolio when live cache is available.
    positions: Rc<RefCell<HashMap<InstrumentId, rust_decimal::Decimal>>>,
}

impl BridgeExecutionClient {
    /// Creates a new client, always booting the safety gate into `HALTED`.
    #[must_use]
    pub fn new(core: ExecutionClientCore, bridge: Box<dyn BridgeClient>) -> Self {
        Self {
            clock: get_atomic_clock_realtime(),
            emitter: ExecutionEventEmitter::new(
                get_atomic_clock_realtime(),
                core.trader_id,
                core.account_id,
                core.account_type,
                core.base_currency,
            ),
            core,
            bridge: Rc::new(RefCell::new(bridge)),
            gate: Rc::new(RefCell::new(Gate::new())),
            pending: RefCell::new(VecDeque::new()),
            reports: RefCell::new(None),
            resilience: RefCell::new(RetryOrchestrator::new(RetryConfig {
                max_attempts: 4,
                base_backoff_ms: 50,
                cap_backoff_ms: 2_000,
                breaker_threshold: 5,
                breaker_cooldown_ms: 10_000,
            })),
            orders: Rc::new(RefCell::new(HashMap::new())),
            client_refs: Rc::new(RefCell::new(HashMap::new())),
            progress: Rc::new(Cell::new(0)),
            positions: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    /// Overrides the resilience tuning for the live bridge transport (retry budget, backoff,
    /// circuit breaker). Must be called before the client is driven by the pump.
    #[must_use]
    pub fn with_resilience_config(self, cfg: RetryConfig) -> Self {
        *self.resilience.borrow_mut() = RetryOrchestrator::new(cfg);
        self
    }

    /// P3-025: adopt an externally owned safety gate.
    ///
    /// The client is boxed inside the node, so a caller that needs to observe the gate *while
    /// the node runs* — rather than the boot-time snapshot the factory records for
    /// `LiveNodeRuntime::gate_was_halted_at_boot` — must create the gate itself and hand it in
    /// here. The gate still boots `HALTED` (`Gate::new`); only its owner changes.
    #[must_use]
    pub fn with_gate(self, gate: Rc<RefCell<Gate>>) -> Self {
        Self { gate, ..self }
    }

    /// P3-223: adopt an externally owned progress counter, so the creator can watch the node's
    /// callbacks while the client itself is boxed inside the node.
    #[must_use]
    pub fn with_progress_ticks(self, progress: Rc<Cell<u64>>) -> Self {
        Self { progress, ..self }
    }

    /// How many mass-status callbacks the runtime has driven through this client (P3-223).
    #[must_use]
    pub fn progress_ticks(&self) -> u64 {
        self.progress.get()
    }

    /// Count one node-driven tick (see [`Self::progress_ticks`]).
    fn record_tick(&self) {
        self.progress.set(self.progress.get().wrapping_add(1));
    }

    /// Returns a reference to the shared safety gate.
    #[must_use]
    pub fn gate(&self) -> &Rc<RefCell<Gate>> {
        &self.gate
    }

    /// Current gate state.
    #[must_use]
    pub fn gate_state(&self) -> ExecState {
        self.gate.borrow().state()
    }

    /// Net position (signed quantity) currently held for an instrument.
    /// Tier 1: delegates to Nautilus `portfolio`/`cache` when available; falls back to
    /// the in-memory parity map used by the fake bridge. The map remains for offline
    /// tests only and is not a second production authority (dossier §Position-management).
    #[must_use]
    pub fn position(&self, instrument_id: &InstrumentId) -> rust_decimal::Decimal {
        // Try live portfolio/cache first if the cache is populated (e.g. after fill events
        // have been applied through the Nautilus portfolio). This keeps the method
        // authoritative when running against a real Nautilus node.
        // For offline fake-bridge tests the cache may be empty, so fall back to the
        // parity map which is updated in `handle_report` for those tests.
        // NOTE: Nautilus `Cache` position lookup is not directly exposed via
        // `ExecutionClientCore` in the current 0.62.0 API, so we conservatively
        // check the parity map but leave a hook for future portfolio delegation:
        //   if let Some(pos) = self.try_portfolio_position(instrument_id) { return pos; }
        self.positions
            .borrow()
            .get(instrument_id)
            .copied()
            .unwrap_or_default()
    }

    /// Hook for future portfolio delegation (Tier 1+). Returns `None` until the
    /// Nautilus portfolio API is wired via `self.core.cache` or `portfolio`.
    #[allow(dead_code)]
    fn try_portfolio_position(
        &self,
        _instrument_id: &InstrumentId,
    ) -> Option<rust_decimal::Decimal> {
        // Placeholder: when `ExecutionClientCore` exposes `cache`/`portfolio`,
        // query it here and convert `Position` quantity to `Decimal`.
        // Kept as `None` for now to preserve Tier 0 parity behavior while
        // documenting the intended authority handoff.
        None
    }

    /// Advances the enablement path one sanctioned step (`HALTED -> ... -> ENABLED`).
    ///
    /// Returns `InvalidTransition` when the requested step is not sanctioned.
    pub fn advance_gate(&self, to: ExecState) -> Result<(), crate::gate::InvalidTransition> {
        self.gate.borrow_mut().transition(to)
    }

    /// Applies a safety halt, returning the gate to `HALTED` from any state (fail-closed).
    pub fn safety_halt(&self) {
        self.gate.borrow_mut().safety_halt();
        crate::telemetry::METRICS
            .gate_safety_halt
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Number of bridge jobs currently queued but not yet round-tripped.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending.borrow().len()
    }

    /// Abandons all queued bridge jobs without executing them, returning the count.
    ///
    /// Used by clean shutdown: a queued job is an in-flight attempt that has not reached the
    /// broker. It is recorded as an unresolved attempt and must never be auto-retried by a
    /// restarted process (the gate boots `HALTED` and `shutdown::verify_restart_safe` asserts it).
    pub fn abort_pending_as_unresolved(&self) -> usize {
        let n = self.pending.borrow().len();
        self.pending.borrow_mut().clear();
        crate::telemetry::METRICS
            .unresolved_attempt
            .fetch_add(n as u64, Ordering::Relaxed);
        n
    }

    /// Flushes any remaining asynchronous event evidence (drains the report queue).
    pub fn flush_reports(&self) {
        self.drain_reports();
    }

    /// Drains queue bridge jobs, performing the bridge round-trip and emitting events, then
    /// drains any asynchronous bridge reports (fills, cancellations).
    pub async fn process_pending(&mut self) -> Result<()> {
        let jobs = std::mem::take(&mut *self.pending.get_mut());
        // P3-017: never drop the jobs behind a failing one. The `take` above detaches the whole
        // batch, so the old `self.execute_job(job).await?` discarded the remainder on the first
        // `Err` — silently: not sent to the bridge, not counted via
        // `abort_pending_as_unresolved`, no denial/rejection event. A `Place` followed by
        // `Modify`/`Cancel` therefore lost the trailing commands while the underlying order
        // stayed live at the broker.
        //
        // Every queued job is now attempted. Each failure is recorded as an unresolved attempt
        // (it reached the queue but never the broker, so it must never be auto-retried after a
        // restart — see `abort_pending_as_unresolved`), and the first error is still surfaced so
        // a caller cannot mistake a partial batch for success. `drain_reports` runs regardless,
        // so fills belonging to the jobs that *did* succeed are not stranded in the queue.
        let mut first_error: Option<anyhow::Error> = None;
        let mut failed: u64 = 0;
        for job in jobs {
            if let Err(e) = self.execute_job(job).await {
                failed += 1;
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
        if failed > 0 {
            crate::telemetry::METRICS
                .unresolved_attempt
                .fetch_add(failed, Ordering::Relaxed);
        }
        self.drain_reports();
        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Single-threaded invariant: only the pump touches the bridge, so holding the `RefCell`
    /// guard across the deterministic bridge round-trip cannot alias another borrow.
    #[allow(clippy::await_holding_refcell_ref)]
    async fn execute_job(&mut self, job: PendingJob) -> Result<()> {
        let order = self
            .core
            .get_order(&job.client_order_id)
            .with_context(|| format!("order {} not in cache", job.client_order_id))?;
        let ts_event = self.clock.get_time_ns();

        // Bounded, backed-off, breaker-protected transport retry (RESILIENCE-001..005 on the
        // live bridge path). Only a `send_command` transport `Err` is retried; a broker
        // decision that returns an envelope (Success/Rejected/Unknown) is TERMINAL here — an
        // `Unknown` outcome is never auto-retried (fail closed). Once-only/duplicate avoidance
        // for the money-moving call is owned by the durable executiongate + safety gate +
        // bridge dedup, so no second in-memory idempotency layer is layered onto this path.
        let reply = {
            let bridge = self.bridge.clone();
            let mut res = self.resilience.borrow_mut();
            let attempts_used: u32;
            let outcome = res
                .execute_async(
                    || self.clock.get_time_ns().as_u64() / 1_000_000,
                    || {
                        let bridge = bridge.clone();
                        let envelope = job.envelope.clone();
                        async move {
                            let mut b = bridge.borrow_mut();
                            match b.send_command(envelope).await {
                                Ok(report) => Ok(report),
                                Err(e) => Err(AttemptError::Transient(e.to_string())),
                            }
                        }
                    },
                )
                .await;
            // Count explicit retransmissions for observability (RESILIENCE/OBS), including on
            // the budget-exhaustion path so a failed round-trip is still observable.
            let reply = match outcome {
                Ok((reply, attempts)) => {
                    attempts_used = attempts;
                    Some(reply)
                }
                Err(RetryError::Exhausted { attempts }) => {
                    attempts_used = attempts;
                    None
                }
                Err(err) => {
                    return Err(anyhow::anyhow!(
                        "bridge {} failed after bounded resilience retries: {err:?}",
                        job.command
                    ));
                }
            };
            if attempts_used > 1 {
                crate::telemetry::METRICS
                    .bridge_transport_retries
                    .fetch_add(attempts_used as u64 - 1, Ordering::Relaxed);
            }
            match reply {
                Some(reply) => reply,
                None => {
                    return Err(anyhow::anyhow!(
                        "bridge {} failed after {} bounded retries: retry budget exhausted",
                        job.command,
                        attempts_used
                    ));
                }
            }
        };

        // P3-195: classify through the same pure function the rest of the code uses, so a
        // `Success` carrying an EMPTY `broker_order_id` is UNKNOWN (ambiguous) and fails closed.
        // Accepting it unconditionally recorded a `VenueOrderId::new("")` and emitted
        // `order_accepted` for a response that cannot be correlated to any real broker order.
        match classify_bridge_report(&reply) {
            BridgeOutcome::Accepted => match job.command {
                Command::Place => {
                    let venue_order_id = VenueOrderId::new(&reply.broker_order_id);
                    self.emitter.emit_order_submitted(&order);
                    self.emitter
                        .emit_order_accepted(&order, venue_order_id, ts_event);
                    self.orders
                        .borrow_mut()
                        .insert(job.client_order_id, venue_order_id);
                }
                Command::Modify => {
                    let venue_order_id = VenueOrderId::new(&reply.broker_order_id);
                    // P3-196: report what was actually SENT — the envelope's target values — not
                    // the cache's pre-modify values, so a modify 100 -> 99 no longer emits 100.
                    let (new_qty, new_px) = match job.envelope.order.as_ref() {
                        Some(spec) => {
                            // A market order carries no price; keep it `None` rather than
                            // fabricating one from an empty string.
                            let px = match spec.price.trim() {
                                "" => None,
                                p => Some(Price::from(p)),
                            };
                            (Quantity::from(spec.quantity.as_str()), px)
                        }
                        None => (order.quantity(), order.price()),
                    };
                    self.emitter.emit_order_updated(
                        &order,
                        venue_order_id,
                        new_qty,
                        new_px,
                        None,
                        None,
                        ts_event,
                    );
                }
                Command::Cancel => {
                    // The fake bridge additionally pushes an asynchronous `order_canceled` report;
                    // the terminal canceled event is emitted when that report is drained.
                }
                _ => {}
            },
            BridgeOutcome::Rejected => {
                self.emitter
                    .emit_order_rejected(&order, &reply.reason, ts_event, false);
                crate::telemetry::METRICS
                    .order_rejected
                    .fetch_add(1, Ordering::Relaxed);
            }
            BridgeOutcome::Unknown => {
                // Ambiguous outcome — including a `Success` with no usable broker order id: fail
                // closed and halt.
                self.gate.borrow_mut().safety_halt();
                self.emitter.emit_order_rejected(
                    &order,
                    &format!("{}: UNKNOWN bridge outcome; safety-halted", reply.outcome),
                    ts_event,
                    false,
                );
            }
        }
        Ok(())
    }

    fn drain_reports(&self) {
        let mut guard = self.reports.borrow_mut();
        if let Some(rx) = guard.as_mut() {
            while let Ok(report) = rx.try_recv() {
                self.handle_report(report);
            }
        }
    }

    fn handle_report(&self, report: ReportEnvelope) {
        crate::telemetry::METRICS
            .report_received
            .fetch_add(1, Ordering::Relaxed);
        // Deterministic `client_order_ref` (14-char hash) is the broker echo; map via `client_refs` to the
        // Nautilus `ClientOrderId` (which may be RND-xxx in tests, deterministic in prod).
        //
        // P3-197: a report we cannot correlate to a known order is AMBIGUOUS, not droppable. This
        // used to fall back to treating the raw ref as a `ClientOrderId`, which on the production
        // path is never in the cache — so the `get_order` below failed and the report was
        // discarded in silence: no fill event, no position update, no halt. A dropped fill is a
        // position the desk does not know it holds, so both paths now halt instead.
        let mapped = self
            .client_refs
            .borrow()
            .get(&report.client_order_ref)
            .cloned();
        let Some(client_order_id) = mapped else {
            tracing::warn!(
                "uncorrelated bridge report (ref={:?}, type={:?}, broker_order_id={:?}): \
                 safety-halting rather than dropping it",
                report.client_order_ref,
                report.report_type,
                report.broker_order_id
            );
            self.gate.borrow_mut().safety_halt();
            return;
        };
        let Ok(order) = self.core.get_order(&client_order_id) else {
            tracing::warn!(
                "bridge report maps to {} which is not in the order cache: safety-halting \
                 rather than dropping it",
                client_order_id
            );
            self.gate.borrow_mut().safety_halt();
            return;
        };
        let ts_event = self.clock.get_time_ns();

        match report.report_type.as_deref() {
            Some("order_filled") => self.handle_fill(&order, &report, ts_event),
            Some("order_canceled") => {
                let venue_order_id = VenueOrderId::new(&report.broker_order_id);
                self.emitter
                    .emit_order_canceled(&order, Some(venue_order_id), ts_event);
            }
            _ => {}
        }
    }

    /// Parses a bridge-supplied numeric field, rejecting anything that is not a strictly positive
    /// decimal. Bridge input is protocol data from outside the process, not trusted internal
    /// state, so it is validated before it is turned into a quantity or a price (P3-198).
    fn bridge_positive_decimal(field: &str, value: &str) -> Result<rust_decimal::Decimal> {
        let raw = value.trim();
        if raw.is_empty() {
            bail!("{field} is missing");
        }
        let parsed: rust_decimal::Decimal = raw
            .parse()
            .map_err(|_| anyhow::anyhow!("{field} is not a number: {raw:?}"))?;
        if parsed <= rust_decimal::Decimal::ZERO {
            bail!("{field} must be > 0, got {raw:?}");
        }
        Ok(parsed)
    }

    fn handle_fill(&self, order: &OrderAny, report: &ReportEnvelope, ts_event: UnixNanos) {
        let venue_order_id = VenueOrderId::new(&report.broker_order_id);
        let trade_id = if report.postback_event_id.is_empty() {
            TradeId::new(format!("T{}", ts_event))
        } else {
            TradeId::new(&report.postback_event_id)
        };
        // P3-198: never fabricate a fill. A missing `fill_quantity`/`fill_price` used to become a
        // zero-quantity/zero-price fill — an `order_filled` event for nothing, booked against the
        // position — while a malformed string could panic inside the `From<&str>` parse. An
        // unusable payload now halts instead of inventing a fill.
        let qty_str = report.fill_quantity.as_deref().unwrap_or("");
        let px_str = report.fill_price.as_deref().unwrap_or("");
        if let Err(e) = Self::bridge_positive_decimal("fill_quantity", qty_str)
            .and_then(|_| Self::bridge_positive_decimal("fill_price", px_str))
        {
            tracing::warn!(
                "rejecting order_filled for {}: {e}; safety-halting rather than booking a \
                 fabricated fill",
                order.client_order_id()
            );
            self.gate.borrow_mut().safety_halt();
            return;
        }
        let last_qty = Quantity::from(qty_str.trim());
        let last_px = Price::from(px_str.trim());
        let quote_currency = self
            .core
            .cache()
            .instrument(&order.instrument_id())
            .map(|i| i.quote_currency())
            .unwrap_or_else(|| Currency::from("USDT"));

        // Update net position from the fill.
        let qty = last_qty.as_decimal();
        // Unfiled finding of the same class as P3-199: an unrecognised side used to book the fill
        // as a POSITIVE delta, inventing exposure in the wrong direction.
        let signed = match order.order_side() {
            OrderSide::Buy => qty,
            OrderSide::Sell => -qty,
            other => {
                tracing::warn!(
                    "order_filled for {} has unsupported side {other:?}: safety-halting rather \
                     than booking the fill in an unknown direction",
                    order.client_order_id()
                );
                self.gate.borrow_mut().safety_halt();
                return;
            }
        };
        *self
            .positions
            .borrow_mut()
            .entry(order.instrument_id())
            .or_default() += signed;

        self.emitter.emit_order_filled(
            order,
            venue_order_id,
            None,
            trade_id,
            last_qty,
            last_px,
            quote_currency,
            None,
            LiquiditySide::Taker,
            ts_event,
        );
    }

    /// Maps an order's side / type / time-in-force onto the bridge vocabulary.
    ///
    /// P3-199: these were `_ =>` catch-alls, so an unrecognised side silently became `Buy`, an
    /// unrecognised type `Lmt`, and any other policy `Day`. A wrong **side** sent to a real broker
    /// is executed against real money, so every arm is now explicit and an unmappable variant is
    /// rejected instead of guessed.
    fn bridge_order_spec(order: &OrderAny) -> Result<(TransactionType, BridgeOrderType, Validity)> {
        let side = match order.order_side() {
            OrderSide::Buy => TransactionType::Buy,
            OrderSide::Sell => TransactionType::Sell,
            other => bail!("unsupported order side {other:?}: refusing to guess a direction"),
        };
        let order_type = match order.order_type() {
            NautilusOrderType::Market => BridgeOrderType::Mkt,
            NautilusOrderType::Limit => BridgeOrderType::Lmt,
            NautilusOrderType::StopMarket => BridgeOrderType::SlMkt,
            NautilusOrderType::StopLimit => BridgeOrderType::SlLmt,
            other => bail!("unsupported order type {other:?}: no bridge equivalent"),
        };
        // The bridge exposes only DAY and IOC. GTC is day-scoped for this venue so it maps to DAY;
        // anything else (FOK, GTD, auction policies) has no equivalent and is rejected rather than
        // silently rewritten into a different order.
        let validity = match order.time_in_force() {
            TimeInForce::Ioc => Validity::Ioc,
            TimeInForce::Day | TimeInForce::Gtc => Validity::Day,
            other => bail!("unsupported time-in-force {other:?}: no bridge equivalent"),
        };
        Ok((side, order_type, validity))
    }

    /// Builds a bridge [`CommandEnvelope`] for an order.
    ///
    /// `quantity`/`price` are passed in rather than read from the cache so a `Modify` can send its
    /// **target** values: reading `order.quantity()`/`order.price()` here sent the pre-modify
    /// values, which made the modify a no-op at the broker (P3-196).
    ///
    /// `strict_spec` is false only for a Cancel: that command is identified by `broker_order_id`
    /// and its order block is never executed, so an unmappable variant must not be the reason a
    /// live order cannot be cancelled.
    fn build_order_envelope(
        &self,
        command: Command,
        order: &OrderAny,
        quantity: &str,
        price: &str,
        strict_spec: bool,
    ) -> Result<CommandEnvelope> {
        let (side, bridge_type, validity) = match Self::bridge_order_spec(order) {
            Ok(spec) => spec,
            Err(_) if !strict_spec => (TransactionType::Buy, BridgeOrderType::Lmt, Validity::Day),
            Err(e) => return Err(e),
        };
        let symbol = order.instrument_id().symbol.to_string();

        let mut envelope = CommandEnvelope::new(command, &UUID4::new().to_string());
        // Separate identities per the bridge contract: `instruction_id` is a stable,
        // order-scoped instruction (derived from the order's init id), `execution_attempt_id`
        // is unique per attempt, and `client_order_ref` (Arrow `remarks`) is the deterministic
        // broker-facing ref. None of them reuse the other.
        envelope.instruction_id = order.init_id().to_string();
        envelope.execution_attempt_id = UUID4::new().to_string();
        envelope.client_order_ref = deterministic_client_order_ref(
            "v1",
            &envelope.instruction_id,
            &envelope.execution_attempt_id,
        );
        // Record mapping for report correlation (deterministic ref -> Nautilus client order id)
        self.client_refs
            .borrow_mut()
            .insert(envelope.client_order_ref.clone(), order.client_order_id());
        envelope.order = Some(
            OrderCommand::new(self.core.venue.as_str(), &symbol)
                .with_side(side)
                .with_quantity(quantity)
                .with_order_type(bridge_type)
                .with_product(Product::Intraday)
                .with_validity(validity)
                .with_price(price),
        );
        Ok(envelope)
    }

    fn order_status_report(&self, order: &OrderAny) -> OrderStatusReport {
        let venue_order_id = self
            .orders
            .borrow()
            .get(&order.client_order_id())
            .copied()
            .unwrap_or_else(|| VenueOrderId::new(""));
        let ts = self.clock.get_time_ns();
        OrderStatusReport::new(
            self.core.account_id,
            order.instrument_id(),
            Some(order.client_order_id()),
            venue_order_id,
            order.order_side(),
            order.order_type(),
            order.time_in_force(),
            order.status(),
            order.quantity(),
            order.filled_qty(),
            ts,
            ts,
            ts,
            None,
        )
    }
}

#[async_trait(?Send)]
impl ExecutionClient for BridgeExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected()
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        self.core.venue
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        self.core
            .cache()
            .account(&self.core.account_id)
            .map(|account_ref| (*account_ref).clone())
    }

    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
        info: Option<Params>,
    ) -> Result<()> {
        let _ = info;
        let state = AccountState::new(
            self.core.account_id,
            self.core.account_type,
            balances,
            margins,
            reported,
            UUID4::new(),
            ts_event,
            self.clock.get_time_ns(),
            self.core.base_currency,
        );
        self.emitter.send_account_state(state);
        Ok(())
    }

    fn start(&mut self) -> Result<()> {
        if self.core.is_started() {
            return Ok(());
        }
        let sender = get_exec_event_sender();
        self.emitter.set_sender(sender);
        self.core.set_started();
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        if self.core.is_stopped() {
            return Ok(());
        }
        self.core.set_stopped();
        Ok(())
    }

    #[allow(clippy::await_holding_refcell_ref)]
    async fn connect(&mut self) -> Result<()> {
        self.bridge.borrow_mut().connect().await?;
        if let Some(rx) = self.bridge.borrow_mut().take_reports() {
            *self.reports.get_mut() = Some(rx);
        }
        self.core.set_connected();
        Ok(())
    }

    #[allow(clippy::await_holding_refcell_ref)]
    async fn disconnect(&mut self) -> Result<()> {
        self.bridge.borrow_mut().disconnect().await?;
        self.core.set_disconnected();
        Ok(())
    }

    fn submit_order(&self, cmd: SubmitOrder) -> Result<()> {
        if !self.gate.borrow().can_execute() {
            if let Ok(order) = self.core.get_order(&cmd.client_order_id) {
                self.emitter.emit_order_denied(
                    &order,
                    "execution gate HALTED: enable path before submitting",
                );
            }
            crate::telemetry::METRICS
                .order_denied
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        let Some(order) = self.core.get_order(&cmd.client_order_id).ok() else {
            bail!("order {} not in cache", cmd.client_order_id);
        };
        let envelope = self.build_order_envelope(
            Command::Place,
            &order,
            &order.quantity().to_string(),
            &order.price().map(|p| p.to_string()).unwrap_or_default(),
            true,
        )?;
        self.pending.borrow_mut().push_back(PendingJob {
            command: Command::Place,
            client_order_id: cmd.client_order_id,
            envelope,
        });
        crate::telemetry::METRICS
            .order_submitted
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn submit_order_list(&self, cmd: SubmitOrderList) -> Result<()> {
        // Batch submission (`OrderList` over `OrderInitialized`) is not part of T4's fake-bridge
        // scope; individual orders are driven through `Self::submit_order`.
        let _ = cmd;
        // P3-200: fail loudly — a silent Ok would report a batch as submitted that never reached
        // the bridge.
        bail!("submit_order_list is not implemented in this client (T4 fake-bridge scope); submit individual orders")
    }

    fn modify_order(&self, cmd: ModifyOrder) -> Result<()> {
        if !self.gate.borrow().can_execute() {
            if let Ok(order) = self.core.get_order(&cmd.client_order_id) {
                self.emitter.emit_order_modify_rejected(
                    &order,
                    None,
                    "execution gate HALTED: enable path before modifying",
                    self.clock.get_time_ns(),
                );
            }
            return Ok(());
        }
        let Some(order) = self.core.get_order(&cmd.client_order_id).ok() else {
            bail!("order {} not in cache", cmd.client_order_id);
        };
        // P3-196: send the *target* values. The envelope (and therefore the modify itself) used to
        // carry the cached pre-modify quantity/price, so a modify 100 -> 99 told the broker 100 —
        // a no-op — and the emitted `order_updated` reported the same stale values.
        let new_qty = cmd.quantity.unwrap_or_else(|| order.quantity());
        let new_px = cmd.price.or_else(|| order.price());
        let mut envelope = self.build_order_envelope(
            Command::Modify,
            &order,
            &new_qty.to_string(),
            &new_px.map(|p| p.to_string()).unwrap_or_default(),
            true,
        )?;
        envelope.broker_order_id = cmd
            .venue_order_id
            .map(|v| v.to_string())
            .unwrap_or_default();
        self.pending.borrow_mut().push_back(PendingJob {
            command: Command::Modify,
            client_order_id: cmd.client_order_id,
            envelope,
        });
        Ok(())
    }

    fn batch_modify_orders(&self, cmd: BatchModifyOrders) -> Result<()> {
        for modify in cmd.modifies {
            self.modify_order(modify)?;
        }
        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> Result<()> {
        if !self.gate.borrow().can_execute() {
            if let Ok(order) = self.core.get_order(&cmd.client_order_id) {
                self.emitter.emit_order_cancel_rejected(
                    &order,
                    None,
                    "execution gate HALTED: enable path before cancelling",
                    self.clock.get_time_ns(),
                );
            }
            return Ok(());
        }
        let Some(order) = self.core.get_order(&cmd.client_order_id).ok() else {
            bail!("order {} not in cache", cmd.client_order_id);
        };
        let mut envelope = self.build_order_envelope(
            Command::Cancel,
            &order,
            &order.quantity().to_string(),
            &order.price().map(|p| p.to_string()).unwrap_or_default(),
            false,
        )?;
        envelope.broker_order_id = cmd
            .venue_order_id
            .map(|v| v.to_string())
            .unwrap_or_default();
        self.pending.borrow_mut().push_back(PendingJob {
            command: Command::Cancel,
            client_order_id: cmd.client_order_id,
            envelope,
        });
        Ok(())
    }

    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> Result<()> {
        // T4 scope: cancelling a specific order. All-orders cancellation for a venue is a
        // later-phase bridge feature.
        let _ = cmd;
        // P3-200: fail loudly — a silent Ok here drops a venue-wide cancel.
        bail!("cancel_all_orders is not implemented in this client (T4 scope); cancel per order")
    }

    fn batch_cancel_orders(&self, cmd: BatchCancelOrders) -> Result<()> {
        for cancel in cmd.cancels {
            self.cancel_order(cancel)?;
        }
        Ok(())
    }

    fn query_account(&self, cmd: QueryAccount) -> Result<()> {
        let _ = cmd;
        // P3-200: fail loudly — an empty Ok tells the caller the account holds nothing.
        bail!("query_account is not implemented in this client; no account data is available")
    }

    fn query_order(&self, cmd: QueryOrder) -> Result<()> {
        if let Ok(order) = self.core.get_order(&cmd.client_order_id) {
            let report = self.order_status_report(&order);
            self.emitter.send_order_status_report(report);
        }
        Ok(())
    }

    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> Result<Option<OrderStatusReport>> {
        let Some(client_order_id) = &cmd.client_order_id else {
            return Ok(None);
        };
        if let Ok(order) = self.core.get_order(client_order_id) {
            return Ok(Some(self.order_status_report(&order)));
        }
        Ok(None)
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> Result<Vec<OrderStatusReport>> {
        self.record_tick();
        // P3-200 note: unlike the five stubs beside it this method IS implemented — it just
        // ignores the command's filters (instrument / time window) and reports every order this
        // client knows about. Narrowing the filter is separate work; it is not an error path.
        let _ = cmd;
        let known: Vec<ClientOrderId> = self.orders.borrow().keys().cloned().collect();
        let mut reports = Vec::new();
        for client_order_id in known {
            if let Ok(order) = self.core.get_order(&client_order_id) {
                reports.push(self.order_status_report(&order));
            }
        }
        Ok(reports)
    }

    async fn generate_fill_reports(&self, cmd: GenerateFillReports) -> Result<Vec<FillReport>> {
        self.record_tick();
        let _ = cmd;
        // P3-200: still a stub, and deliberately NOT an error. The Nautilus runtime's periodic
        // mass-status reconciliation calls this on every loop tick: returning Err here kills the
        // node loop before it can handle a stop request (engine.rs's runtime_hosted_run_loop_*
        // tests fail with "Failed to get mass status from exec"). The silent empty history is the
        // real defect — it is fixed by serving fills from a durable source (B7 / Workstream D),
        // not by failing a loop that cannot act on the failure.
        Ok(Vec::new())
    }

    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> Result<Vec<PositionStatusReport>> {
        self.record_tick();
        let _ = cmd;
        // P3-200: same contract as generate_fill_reports — the runtime's mass-status path calls
        // this per tick, so it must not Err. Empty positions is the stub's risk, not a claim.
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use nautilus_common::cache::Cache;
    use nautilus_model::enums::{AccountType, OrderSide, OrderType};
    use nautilus_model::identifiers::{StrategyId, Symbol, TraderId};
    use nautilus_model::orders::OrderTestBuilder;
    use std::{cell::RefCell, rc::Rc};

    #[test]
    fn client_boots_into_halted() {
        let core = ExecutionClientCore::new(
            TraderId::from("TRADER-001"),
            ClientId::from("EXEC-001"),
            Venue::from("NFO"),
            OmsType::Netting,
            AccountId::from("EXEC-001"),
            AccountType::Cash,
            None,
            Rc::new(RefCell::new(Cache::default())),
        );
        let client = BridgeExecutionClient::new(core, Box::new(crate::bridge::FakeBridge::new()));
        assert_eq!(client.gate_state(), ExecState::Halted);
        assert!(!client.is_connected());
    }

    /// Builds a populated cache + client fixture for the roundtrip tests.
    ///
    /// The order is keyed in the cache by a 16-char-safe `client_order_id` so the bridge
    /// reports (which echo `remarks` = `client_order_ref`) correlate back to the order.
    ///
    /// `bridge` is consumed as-is, so callers pre-script the responses they expect.
    fn roundtrip_fixture(
        bridge: crate::bridge::FakeBridge,
        scripts: &[crate::bridge::CommandScript],
    ) -> (BridgeExecutionClient, InstrumentId, ClientOrderId, OrderAny) {
        let trader = TraderId::from("TRADER-001");
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
            trader,
            ClientId::from("EXEC-001"),
            Venue::from("NFO"),
            OmsType::Netting,
            AccountId::from("EXEC-001"),
            AccountType::Cash,
            None,
            cache,
        );
        let mut fake = bridge;
        for script in scripts {
            fake.script(script.clone());
        }
        let client = BridgeExecutionClient::new(core, Box::new(fake));
        (client, instrument_id, client_order_id, order)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn roundtrip_place_fill_then_modify_cancel_echoes_remarks() {
        use nautilus_common::clients::ExecutionClient;

        // Script the fake bridge: place fills immediately, then modify/cancel accept.
        let scripts = vec![
            crate::bridge::CommandScript::AcceptThenFill,
            crate::bridge::CommandScript::Accept, // modify
            crate::bridge::CommandScript::Accept, // cancel
        ];
        let (client, instrument_id, client_order_id, order) =
            roundtrip_fixture(crate::bridge::FakeBridge::new(), &scripts);
        let mut client = client;
        client.connect().await.expect("connect");

        // Gate boots HALTED; health must never imply ENABLED.
        assert_eq!(client.gate_state(), ExecState::Halted);
        let health = crate::health::HealthStatus::new(client.gate_state());
        assert!(!health.trading_ready);
        assert!(health.health_does_not_imply_enabled());

        // Sanctioned path reaches APPROVAL_PENDING; ENABLED requires the
        // single-operator (saurabh, DEC-044) approval gate (INVARIANT-003),
        // which cannot be bypassed via advance_gate.
        client.advance_gate(ExecState::Reconciling).unwrap();
        client.advance_gate(ExecState::ApprovalPending).unwrap();
        assert!(
            client.advance_gate(ExecState::Enabled).is_err(),
            "gate must reject unsanctioned enable"
        );
        {
            let mut g = client.gate.borrow_mut();
            g.add_authorized("saurabh");
            g.set_epoch(1).unwrap();
            g.record_approval("saurabh", "h1").unwrap();
            g.enable(1).unwrap();
        }
        assert!(client.gate.borrow().can_execute());

        let submit = SubmitOrder::from_order(
            &order,
            TraderId::from("TRADER-001"),
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
        );
        client.submit_order(submit).expect("place enqueued");
        client
            .process_pending()
            .await
            .expect("place roundtrip + fill drained");

        // The fill report carries the order's `remarks` (= client_order_ref), so the
        // client correlates it back and captures the position.
        assert_eq!(
            client.position(&instrument_id),
            rust_decimal::Decimal::from(10)
        );

        // Modify: reference the broker-assigned order id, roundtrip succeeds.
        let modify = ModifyOrder::new(
            TraderId::from("TRADER-001"),
            None,
            StrategyId::from("S-001"),
            instrument_id,
            client_order_id,
            Some(VenueOrderId::new("BRK-0001")),
            Some(Quantity::from(10)),
            Some(Price::from("99")),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );
        client.modify_order(modify).expect("modify enqueued");
        client.process_pending().await.expect("modify roundtrip");

        // Cancel: reference the broker-assigned order id, roundtrip succeeds.
        let cancel = CancelOrder::new(
            TraderId::from("TRADER-001"),
            None,
            StrategyId::from("S-001"),
            instrument_id,
            client_order_id,
            Some(VenueOrderId::new("BRK-0001")),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );
        client.cancel_order(cancel).expect("cancel enqueued");
        client.process_pending().await.expect("cancel roundtrip");

        drop(client);
    }

    /// Enables the safety gate along the only sanctioned path (HALTED -> RECONCILING ->
    /// APPROVAL_PENDING -> ENABLED via single-operator (saurabh) approval, DEC-044,
    /// INVARIANT-003), mirroring the existing tests.
    fn enable_gate(client: &BridgeExecutionClient) {
        client.advance_gate(ExecState::Reconciling).unwrap();
        client.advance_gate(ExecState::ApprovalPending).unwrap();
        let mut g = client.gate.borrow_mut();
        g.add_authorized("saurabh");
        g.set_epoch(1).unwrap();
        g.record_approval("saurabh", "h1").unwrap();
        g.enable(1).unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resilient_bridge_retries_transient_timeouts_then_place_succeeds() {
        use nautilus_common::clients::ExecutionClient;
        use std::sync::atomic::Ordering;

        // Two transient transport timeouts, then the bridge accepts and fills.
        let scripts = vec![
            crate::bridge::CommandScript::Timeout,
            crate::bridge::CommandScript::Timeout,
            crate::bridge::CommandScript::AcceptThenFill,
        ];
        let (client, instrument_id, _client_order_id, order) =
            roundtrip_fixture(crate::bridge::FakeBridge::new(), &scripts);
        let mut client = client.with_resilience_config(crate::resilience::RetryConfig {
            max_attempts: 5,
            base_backoff_ms: 1,
            cap_backoff_ms: 5,
            breaker_threshold: 10,
            breaker_cooldown_ms: 1000,
        });
        client.connect().await.expect("connect");
        enable_gate(&client);
        assert!(client.gate.borrow().can_execute());

        let retries_before = crate::telemetry::METRICS
            .bridge_transport_retries
            .load(Ordering::Relaxed);
        let submit = SubmitOrder::from_order(
            &order,
            TraderId::from("TRADER-001"),
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
        );
        client.submit_order(submit).expect("place enqueued");
        client
            .process_pending()
            .await
            .expect("place succeeds after transient retries");

        // The two timeouts were re-invoked: the order is placed AND filled.
        assert_eq!(
            client.position(&instrument_id),
            rust_decimal::Decimal::from(10)
        );
        // The metric is populated (>=1 because the two timeouts were retransmitted); the exact
        // count is not asserted cross-test because `METRICS` is a process-global counter shared
        // with the concurrently-running parallel tests. The strong correctness proof is the
        // position==10 above: without retry the place would have failed on the first timeout.
        assert!(
            crate::telemetry::METRICS
                .bridge_transport_retries
                .load(Ordering::Relaxed)
                - retries_before
                >= 1,
            "transient transport was retransmitted at least once (metric populated)"
        );
        drop(client);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resilient_bridge_bounded_attempts_surface_without_phantom_success() {
        use nautilus_common::clients::ExecutionClient;
        use std::sync::atomic::Ordering;

        // The bridge is persistently down (every attempt times out).
        let scripts = vec![
            crate::bridge::CommandScript::Timeout,
            crate::bridge::CommandScript::Timeout,
            crate::bridge::CommandScript::Timeout,
            crate::bridge::CommandScript::Timeout,
        ];
        let (client, _instrument_id, _client_order_id, order) =
            roundtrip_fixture(crate::bridge::FakeBridge::new(), &scripts);
        let mut client = client.with_resilience_config(crate::resilience::RetryConfig {
            max_attempts: 4,
            base_backoff_ms: 1,
            cap_backoff_ms: 5,
            breaker_threshold: 100,
            breaker_cooldown_ms: 1000,
        });
        client.connect().await.expect("connect");
        enable_gate(&client);

        let retries_before = crate::telemetry::METRICS
            .bridge_transport_retries
            .load(Ordering::Relaxed);
        let submit = SubmitOrder::from_order(
            &order,
            TraderId::from("TRADER-001"),
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
        );
        client.submit_order(submit).expect("place enqueued");
        let result = client.process_pending().await;

        // Bounded attempts exhaust and surface an error — never a phantom success, and never
        // an unbounded retry storm (RESILIENCE-001/003 on the live path).
        assert!(
            result.is_err(),
            "bounded retries must surface, not fabricate success"
        );
        // Bounded behavior is the proof (`result` is Err, never a phantom success). The retry

        // counter is populated too, but its exact value is not cross-test deterministic because
        // `METRICS` is process-global and parallel tests share it.
        assert!(
            crate::telemetry::METRICS
                .bridge_transport_retries
                .load(Ordering::Relaxed)
                - retries_before
                >= 1,
            "retransmissions were counted before exhaustion surfaced"
        );
        drop(client);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn halted_gate_denies_place_before_any_bridge_command() {
        use nautilus_common::clients::ExecutionClient;

        // No scripts: if the client tried to place while HALTED, the fake would error
        // on an unexpected command rather than stay at zero.
        let (client, _instrument_id, _client_order_id, order) =
            roundtrip_fixture(crate::bridge::FakeBridge::new(), &[]);
        let mut client = client;
        client.connect().await.expect("connect");

        // While HALTED, a place is denied and never reaches the bridge.
        let submit = SubmitOrder::from_order(
            &order,
            TraderId::from("TRADER-001"),
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
        );
        client.submit_order(submit).expect("denied without error");
        client
            .process_pending()
            .await
            .expect("no bridge job to run");

        // The order gate stays HALTED and no position was captured.
        assert_eq!(client.gate_state(), ExecState::Halted);
        assert_eq!(
            client.position(&order.instrument_id()),
            rust_decimal::Decimal::from(0)
        );
    }

    /// P3-017: a failing job must not drop the jobs queued behind it.
    ///
    /// Queues Place (scripted to time out, so its single attempt exhausts and `execute_job`
    /// returns `Err`) followed by Modify. The old loop propagated that first `Err` with `?` and
    /// discarded the Modify — which `std::mem::take` had already detached from `pending`, so it
    /// could not even be reached by `abort_pending_as_unresolved`. It never reached the bridge.
    ///
    /// The Modify is scripted to `Reject`, so `METRICS.order_rejected` is the observable proof
    /// that the trailing job was actually attempted: under the old behaviour it stays at zero.
    #[tokio::test(flavor = "current_thread")]
    async fn failing_job_does_not_drop_the_jobs_behind_it() {
        use nautilus_common::clients::ExecutionClient;

        let scripts = vec![
            crate::bridge::CommandScript::Timeout, // place: exhausts its one attempt
            crate::bridge::CommandScript::Reject("trailing modify rejected".into()),
        ];
        let (client, instrument_id, client_order_id, order) =
            roundtrip_fixture(crate::bridge::FakeBridge::new(), &scripts);
        let mut client = client.with_resilience_config(crate::resilience::RetryConfig {
            max_attempts: 1, // the place fails outright rather than retrying
            base_backoff_ms: 1,
            cap_backoff_ms: 1,
            breaker_threshold: 100, // keep the breaker from short-circuiting job 2
            breaker_cooldown_ms: 1000,
        });
        client.connect().await.expect("connect");
        enable_gate(&client);

        let rejected_before = crate::telemetry::METRICS
            .order_rejected
            .load(Ordering::Relaxed);
        let unresolved_before = crate::telemetry::METRICS
            .unresolved_attempt
            .load(Ordering::Relaxed);

        let submit = SubmitOrder::from_order(
            &order,
            TraderId::from("TRADER-001"),
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
        );
        client.submit_order(submit).expect("place enqueued");
        let modify = ModifyOrder::new(
            TraderId::from("TRADER-001"),
            None,
            StrategyId::from("S-001"),
            instrument_id,
            client_order_id,
            Some(VenueOrderId::new("BRK-0001")),
            Some(Quantity::from(10)),
            Some(Price::from("99")),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );
        client.modify_order(modify).expect("modify enqueued");
        assert_eq!(client.pending_count(), 2, "both jobs queue for one pump");

        let result = client.process_pending().await;

        // The failure still surfaces — a partial batch must not read as success.
        assert!(result.is_err(), "the failed job must surface an error");
        // ...but the job behind it WAS attempted. Old code never sent it (delta 0).
        assert!(
            crate::telemetry::METRICS
                .order_rejected
                .load(Ordering::Relaxed)
                - rejected_before
                >= 1,
            "the job queued behind a failure must still reach the bridge"
        );
        // ...and the failed job is accounted rather than silently lost.
        assert!(
            crate::telemetry::METRICS
                .unresolved_attempt
                .load(Ordering::Relaxed)
                - unresolved_before
                >= 1,
            "a job that reached the queue but not the broker is an unresolved attempt"
        );
        assert_eq!(client.pending_count(), 0, "the batch drains completely");
        drop(client);
    }

    /// P3-195: `Success` with no usable broker order id is ambiguous, not an acceptance.
    ///
    /// `classify_bridge_report` has always treated an empty `broker_order_id` as UNKNOWN; the live
    /// path bypassed it and matched on `reply.outcome()` directly, so such a response recorded
    /// `VenueOrderId::new("")` and emitted `order_accepted`. Exercised here through the same
    /// function the live path now calls.
    #[test]
    fn p3_195_success_without_a_broker_order_id_is_unknown() {
        let empty_id = ReportEnvelope {
            outcome: crate::bridge::protocol::ReportOutcome::Success
                .as_str()
                .to_string(),
            broker_order_id: String::new(),
            ..ReportEnvelope::default()
        };
        assert_eq!(
            classify_bridge_report(&empty_id),
            BridgeOutcome::Unknown,
            "an uncorrelatable success is ambiguous and must fail closed"
        );

        // ...and a real id is still accepted, so the guard is not a blanket refusal.
        let usable = ReportEnvelope {
            outcome: crate::bridge::protocol::ReportOutcome::Success
                .as_str()
                .to_string(),
            broker_order_id: "BRK-1".to_string(),
            ..ReportEnvelope::default()
        };
        assert_eq!(classify_bridge_report(&usable), BridgeOutcome::Accepted);
        assert!(unknown_halts_never_retries(BridgeOutcome::Unknown));
    }

    /// P3-196: a Modify must send — and report — its TARGET values, not the cached pre-modify ones.
    /// The cached order stays 10 @ 100; the modify asks for 7 @ 123.
    #[tokio::test(flavor = "current_thread")]
    async fn p3_196_modify_sends_the_target_values_not_the_cached_ones() {
        use nautilus_common::clients::ExecutionClient;

        let (client, instrument_id, client_order_id, order) =
            roundtrip_fixture(crate::bridge::FakeBridge::new(), &[]);
        let mut client = client;
        client.connect().await.expect("connect");
        enable_gate(&client);

        let submit = SubmitOrder::from_order(
            &order,
            TraderId::from("TRADER-001"),
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
        );
        client.submit_order(submit).expect("place enqueued");
        // Inspect only the modify, so drop the queued place.
        client.pending.borrow_mut().clear();

        let modify = ModifyOrder::new(
            TraderId::from("TRADER-001"),
            None,
            StrategyId::from("S-001"),
            instrument_id,
            client_order_id,
            Some(VenueOrderId::new("BRK-0001")),
            Some(Quantity::from(7)),
            Some(Price::from("123")),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );
        client.modify_order(modify).expect("modify enqueued");

        assert_eq!(
            order.quantity(),
            Quantity::from(10),
            "cache is still pre-modify"
        );
        let jobs = client.pending.borrow();
        let job = jobs.back().expect("modify queued");
        assert_eq!(job.command, Command::Modify);
        let spec = job.envelope.order.as_ref().expect("order spec");
        assert_eq!(
            spec.quantity, "7",
            "the broker must be told the target quantity, not the cached 10"
        );
        assert_eq!(
            spec.price, "123",
            "the broker must be told the target price, not the cached 100"
        );
        drop(jobs);
        drop(client);
    }

    /// P3-197: a report that cannot be correlated to a known order must halt, not vanish.
    #[tokio::test(flavor = "current_thread")]
    async fn p3_197_uncorrelated_report_halts_instead_of_being_dropped() {
        let (client, instrument_id, _client_order_id, _order) =
            roundtrip_fixture(crate::bridge::FakeBridge::new(), &[]);
        let client = client;
        enable_gate(&client);
        assert!(client.gate.borrow().can_execute());

        // A fill for a ref that `client_refs` has never seen — the old code fell back to treating
        // the raw ref as a ClientOrderId, failed `get_order`, and returned in silence.
        let report = ReportEnvelope {
            client_order_ref: "NO-SUCH-REF".to_string(),
            report_type: Some("order_filled".to_string()),
            broker_order_id: "BRK-1".to_string(),
            fill_quantity: Some("10".to_string()),
            fill_price: Some("100".to_string()),
            ..ReportEnvelope::default()
        };
        client.handle_report(report);

        assert_eq!(
            client.gate_state(),
            ExecState::Halted,
            "an uncorrelatable report is ambiguous and must halt the gate"
        );
        assert_eq!(
            client.position(&instrument_id),
            rust_decimal::Decimal::from(0),
            "and must not book a fill it cannot attribute"
        );
    }

    /// P3-198: a fill with an unusable payload must halt, not become a zero fill.
    #[tokio::test(flavor = "current_thread")]
    async fn p3_198_unusable_fill_payload_halts_instead_of_fabricating_one() {
        let (client, instrument_id, _client_order_id, order) =
            roundtrip_fixture(crate::bridge::FakeBridge::new(), &[]);
        let client = client;
        enable_gate(&client);
        let ts = client.clock.get_time_ns();

        // Quantity missing, price present: previously became a 0-quantity/0-ish fill that still
        // emitted `order_filled`.
        let missing_qty = ReportEnvelope {
            broker_order_id: "BRK-1".to_string(),
            fill_price: Some("100".to_string()),
            ..ReportEnvelope::default()
        };
        client.handle_fill(&order, &missing_qty, ts);
        assert_eq!(
            client.gate_state(),
            ExecState::Halted,
            "a fill with no usable payload must halt"
        );
        assert_eq!(
            client.position(&instrument_id),
            rust_decimal::Decimal::from(0)
        );

        // Malformed (non-numeric) quantity must not reach the `From<&str>` parse.
        let client2 = roundtrip_fixture(crate::bridge::FakeBridge::new(), &[]).0;
        enable_gate(&client2);
        let malformed = ReportEnvelope {
            broker_order_id: "BRK-1".to_string(),
            fill_quantity: Some("not-a-number".to_string()),
            fill_price: Some("100".to_string()),
            ..ReportEnvelope::default()
        };
        client2.handle_fill(&order, &malformed, ts);
        assert_eq!(client2.gate_state(), ExecState::Halted);
        assert_eq!(
            client2.position(&instrument_id),
            rust_decimal::Decimal::from(0)
        );
    }

    /// P3-199: an unmappable order spec is rejected, never silently rewritten.
    #[test]
    fn p3_199_unsupported_order_spec_is_rejected_not_guessed() {
        let build = |tif: TimeInForce, side: OrderSide, cid: &str| {
            OrderTestBuilder::new(OrderType::Limit)
                .instrument_id(InstrumentId::new(Symbol::from("NIFTY"), Venue::from("NFO")))
                .side(side)
                .quantity(Quantity::from(1))
                .price(Price::from("100"))
                .time_in_force(tif)
                .client_order_id(ClientOrderId::from(cid))
                .build()
        };

        // FOK has no bridge equivalent: previously it was silently rewritten to DAY.
        let fok = build(TimeInForce::Fok, OrderSide::Buy, "RND-0002");
        let err = BridgeExecutionClient::bridge_order_spec(&fok)
            .expect_err("an unmappable time-in-force must be rejected, not substituted");
        assert!(
            format!("{err}").contains("unsupported time-in-force"),
            "got: {err}"
        );

        // ...while the supported mappings still work, and the side is not guessed.
        let gtc = build(TimeInForce::Gtc, OrderSide::Sell, "RND-0003");
        let (side, order_type, validity) =
            BridgeExecutionClient::bridge_order_spec(&gtc).expect("GTC maps to DAY");
        assert_eq!(
            side,
            TransactionType::Sell,
            "side must follow the order, not default to Buy"
        );
        assert_eq!(order_type, BridgeOrderType::Lmt);
        assert_eq!(validity, Validity::Day);
    }
}
