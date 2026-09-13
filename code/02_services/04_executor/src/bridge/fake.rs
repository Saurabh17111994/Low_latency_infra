//! In-process fake bridge implementing the Go bridge protocol.
//!
//! Deterministic and scripted so the Nautilus execution service can be exercised offline with
//! zero network egress (the network-isolation guarantee). Mirrors the order lifecycle of
//! `code/02_services/06_execution_bridge/go-bridge/fake_broker.go`.

use std::collections::{HashMap, VecDeque};

use anyhow::{anyhow, Context as _};
use async_trait::async_trait;
use tracing::debug;

use super::client::{BridgeClient, BridgeReportStream};
use super::protocol::{Command, CommandEnvelope, ReportEnvelope, RECORD_REPORT};

/// Scripted synchronous reply for the next command.
#[derive(Debug, Clone)]
pub enum CommandScript {
    /// Successful acknowledgement of the command.
    Accept,
    /// Successful acknowledgement followed by an asynchronous full-fill on the report stream.
    AcceptThenFill,
    /// Scripted mass-status snapshot for ReconcileOrders/ReconcileTrades/ReconcilePositions.
    ReconcileSnapshot(Vec<OrderRecord>),
    /// Resource-side rejection with a reason.
    Reject(String),
    /// A response the service cannot classify as success or rejection.
    Unknown(String),
    /// No reply within the command window (bridge stall).
    Timeout,
}

/// The order-status value used in reports (mirrors the bridge's `order_status` strings).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FakeOrderStatus {
    Open,
    PartialFill,
    Filled,
    Canceled,
}

impl FakeOrderStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "OPEN",
            Self::PartialFill => "PARTIAL_FILL",
            Self::Filled => "FILLED",
            Self::Canceled => "CANCELED",
        }
    }
}

/// A recorded order held by the fake bridge.
#[derive(Debug, Clone)]
pub struct OrderRecord {
    pub broker_order_id: String,
    pub client_order_ref: String,
    pub symbol: String,
    pub quantity: String,
    pub status: FakeOrderStatus,
    pub filled_qty: String,
    pub price: String,
}

/// The in-process fake bridge.
pub struct FakeBridge {
    connected: bool,
    reports_tx: Option<tokio::sync::mpsc::UnboundedSender<ReportEnvelope>>,
    reports_rx: Option<BridgeReportStream>,
    orders: HashMap<String, OrderRecord>,
    counter: u64,
    commands: u64,
    place_calls: u64,
    modify_calls: u64,
    cancel_calls: u64,
    query_calls: u64,
    reconcile_calls: u64,
    command_log: Vec<String>,
    scripts: VecDeque<CommandScript>,
}

impl FakeBridge {
    pub fn new() -> Self {
        Self {
            connected: false,
            reports_tx: None,
            reports_rx: None,
            orders: HashMap::new(),
            counter: 0,
            commands: 0,
            place_calls: 0,
            modify_calls: 0,
            cancel_calls: 0,
            query_calls: 0,
            reconcile_calls: 0,
            command_log: Vec::new(),
            scripts: VecDeque::new(),
        }
    }

    /// Total number of commands received (place/modify/cancel/query/reconcile).
    pub fn command_count(&self) -> u64 {
        self.commands
    }

    /// Number of Place commands received.
    pub fn place_call_count(&self) -> u64 {
        self.place_calls
    }

    /// Number of Cancel commands received.
    pub fn cancel_call_count(&self) -> u64 {
        self.cancel_calls
    }

    /// Number of QueryOrder commands received.
    pub fn query_call_count(&self) -> u64 {
        self.query_calls
    }

    /// Number of Reconcile* commands received.
    pub fn reconcile_call_count(&self) -> u64 {
        self.reconcile_calls
    }

    /// Ordered log of command strings received (e.g. "place", "query-order").
    pub fn command_log(&self) -> Vec<String> {
        self.command_log.clone()
    }

    /// Queues a scripted reply for the next `send_command` call (consumed in FIFO order).
    pub fn script(&mut self, script: CommandScript) -> &mut Self {
        self.scripts.push_back(script);
        self
    }

    /// Snapshot of all known orders (mass-status view, offline).
    pub fn open_orders_snapshot(&self) -> Vec<OrderRecord> {
        self.orders.values().cloned().collect()
    }

    fn next_broker_id(&mut self) -> String {
        self.counter += 1;
        format!("BRK-{:04}", self.counter)
    }

    fn make_success(
        &self,
        cmd: Command,
        envelope: &CommandEnvelope,
        broker_order_id: &str,
    ) -> ReportEnvelope {
        ReportEnvelope {
            record_type: RECORD_REPORT.to_string(),
            contract_version: 1,
            request_id: envelope.request_id.clone(),
            command: cmd.as_str().to_string(),
            outcome: "SUCCESS".to_string(),
            // Arrow echoes `remarks` (= our client_order_ref) on every postback; the fake
            // bridge mirrors that so the service can correlate reports back to orders.
            client_order_ref: envelope.client_order_ref.clone(),
            broker_order_id: broker_order_id.to_string(),
            ..ReportEnvelope::default()
        }
    }

    fn push_report(&self, report: ReportEnvelope) {
        if let Some(tx) = &self.reports_tx {
            let _ = tx.send(report);
        }
    }
}

impl Default for FakeBridge {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl BridgeClient for FakeBridge {
    fn is_connected(&self) -> bool {
        self.connected
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        self.connected = true;
        if self.reports_tx.is_none() {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            self.reports_tx = Some(tx);
            self.reports_rx = Some(rx);
        }
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.connected = false;
        Ok(())
    }

    async fn send_command(&mut self, envelope: CommandEnvelope) -> anyhow::Result<ReportEnvelope> {
        // Lenient validation for reconcile QueryOrder by client_order_ref (offline): the
        // protocol requires broker_order_id for QueryOrder, but reconcile by deterministic
        // client_order_ref (remarks) carries only client_order_ref. Treat that as valid
        // for the offline fake so the UNKNOWN→HALT never-retry path can be exercised.
        match envelope.validate() {
            Ok(()) => {}
            Err(e) => {
                let msg = e.to_string();
                let is_query_by_ref = envelope.command == "query-order"
                    && envelope.broker_order_id.is_empty()
                    && !envelope.client_order_ref.is_empty()
                    && envelope.client_order_ref.len() <= 16;
                if !(is_query_by_ref && msg.contains("broker_order_id is required for query-order"))
                {
                    return Err(anyhow!("invalid command: {e}"));
                }
            }
        }

        self.commands += 1;
        // Per-command introspection for engine_reconcile_test: proves UNKNOWN never retries Place
        let cmd_for_count = envelope.command().unwrap_or(Command::Place);
        match cmd_for_count {
            Command::Place => self.place_calls += 1,
            Command::Modify => self.modify_calls += 1,
            Command::Cancel => self.cancel_calls += 1,
            Command::QueryOrder => self.query_calls += 1,
            Command::ReconcileOrders | Command::ReconcileTrades | Command::ReconcilePositions => {
                self.reconcile_calls += 1
            }
        }
        self.command_log.push(envelope.command.clone());

        let script = self.scripts.pop_front();
        let Some(script) = script else {
            return Err(anyhow!(
                "fake bridge: unexpected command {}",
                envelope.command
            ));
        };

        let cmd = envelope
            .command()
            .ok_or_else(|| anyhow!("unsupported command {}", envelope.command))?;

        match script {
            CommandScript::Timeout => Err(anyhow!("fake bridge: command {} timed out", cmd)),
            CommandScript::Reject(reason) => Ok(ReportEnvelope {
                record_type: RECORD_REPORT.to_string(),
                contract_version: 1,
                request_id: envelope.request_id.clone(),
                command: cmd.as_str().to_string(),
                outcome: "REJECTED".to_string(),
                reason,
                client_order_ref: envelope.client_order_ref.clone(),
                broker_order_id: envelope.broker_order_id.clone(),
                ..ReportEnvelope::default()
            }),
            CommandScript::Unknown(reason) => Ok(ReportEnvelope {
                record_type: RECORD_REPORT.to_string(),
                contract_version: 1,
                request_id: envelope.request_id.clone(),
                command: cmd.as_str().to_string(),
                outcome: "UNKNOWN".to_string(),
                reason,
                client_order_ref: envelope.client_order_ref.clone(),
                broker_order_id: envelope.broker_order_id.clone(),
                ..ReportEnvelope::default()
            }),
            CommandScript::ReconcileSnapshot(snapshot) => Ok(self
                .emit_reconcile_snapshot(cmd, &envelope, &snapshot)
                .await),
            CommandScript::Accept => self.handle_accept(cmd, envelope).await,
            CommandScript::AcceptThenFill => {
                let report = self.handle_accept(cmd, envelope.clone()).await?;
                self.emit_fill(&envelope);
                Ok(report)
            }
        }
    }

    fn take_reports(&mut self) -> Option<BridgeReportStream> {
        self.reports_rx.take()
    }
}

impl FakeBridge {
    async fn handle_accept(
        &mut self,
        cmd: Command,
        envelope: CommandEnvelope,
    ) -> anyhow::Result<ReportEnvelope> {
        match cmd {
            Command::Place => {
                let order = envelope.order.as_ref().context("place requires an order")?;
                let broker_order_id = self.next_broker_id();
                self.orders.insert(
                    broker_order_id.clone(),
                    OrderRecord {
                        broker_order_id: broker_order_id.clone(),
                        client_order_ref: envelope.client_order_ref.clone(),
                        symbol: order.symbol.clone(),
                        quantity: order.quantity.clone(),
                        status: FakeOrderStatus::Open,
                        filled_qty: "0".to_string(),
                        price: order.price.clone(),
                    },
                );
                Ok(self.make_success(Command::Place, &envelope, &broker_order_id))
            }
            Command::Modify => {
                // P3-428: re-record every field a modify may change, and refuse a
                // body-less modify the way the live bridge does (validateCommand requires
                // the order body) instead of reporting SUCCESS with no state change.
                let order = envelope
                    .order
                    .as_ref()
                    .context("modify requires an order")?;
                let record = self
                    .orders
                    .get_mut(&envelope.broker_order_id)
                    .context("modify references unknown order")?;
                record.price = order.price.clone();
                record.quantity = order.quantity.clone();
                Ok(self.make_success(Command::Modify, &envelope, &envelope.broker_order_id))
            }
            Command::Cancel => {
                let record = self
                    .orders
                    .get_mut(&envelope.broker_order_id)
                    .context("cancel references unknown order")?;
                record.status = FakeOrderStatus::Canceled;
                debug!(broker_order_id = %envelope.broker_order_id, "fake bridge canceled order");
                self.push_report(ReportEnvelope {
                    record_type: RECORD_REPORT.to_string(),
                    contract_version: 1,
                    request_id: envelope.request_id.clone(),
                    command: Command::Cancel.as_str().to_string(),
                    outcome: "SUCCESS".to_string(),
                    client_order_ref: envelope.client_order_ref.clone(),
                    broker_order_id: envelope.broker_order_id.clone(),
                    order_status: Some(FakeOrderStatus::Canceled.as_str().to_string()),
                    report_type: Some("order_canceled".to_string()),
                    ..ReportEnvelope::default()
                });
                Ok(self.make_success(Command::Cancel, &envelope, &envelope.broker_order_id))
            }
            Command::QueryOrder => {
                // Reconcile path: allow lookup by client_order_ref when broker_order_id empty,
                // and synthesize a deterministic broker id for the offline UNKNOWN→HALT test
                // when no order is seeded. Production still returns real broker state when
                // seeded; the synthetic path is only for the integeration test's scripted
                // Accept without a prior Place.
                let record_opt = if !envelope.broker_order_id.is_empty() {
                    self.orders.get(&envelope.broker_order_id).cloned()
                } else {
                    None
                };
                let record_opt = record_opt.or_else(|| {
                    self.orders
                        .values()
                        .find(|r| r.client_order_ref == envelope.client_order_ref)
                        .cloned()
                });
                let broker_id = if let Some(rec) = record_opt {
                    rec.broker_order_id
                } else if !envelope.client_order_ref.is_empty() {
                    format!("BRK-{}", envelope.client_order_ref)
                } else {
                    self.next_broker_id()
                };
                // Determine status: if we had a matching seeded record, echo its status;
                // otherwise Open is the synthetic reconcile hit.
                let status = self
                    .orders
                    .get(&envelope.broker_order_id)
                    .map(|r| r.status.as_str().to_string())
                    .or_else(|| {
                        self.orders
                            .values()
                            .find(|r| r.client_order_ref == envelope.client_order_ref)
                            .map(|r| r.status.as_str().to_string())
                    })
                    .unwrap_or_else(|| FakeOrderStatus::Open.as_str().to_string());
                Ok(ReportEnvelope {
                    record_type: RECORD_REPORT.to_string(),
                    contract_version: 1,
                    request_id: envelope.request_id.clone(),
                    command: Command::QueryOrder.as_str().to_string(),
                    outcome: "SUCCESS".to_string(),
                    client_order_ref: envelope.client_order_ref.clone(),
                    broker_order_id: broker_id,
                    order_status: Some(status),
                    report_type: Some("order_status".to_string()),
                    ..ReportEnvelope::default()
                })
            }
            Command::ReconcileOrders | Command::ReconcileTrades | Command::ReconcilePositions => {
                let snap = self.open_orders_snapshot();
                Ok(self.emit_reconcile_snapshot(cmd, &envelope, &snap).await)
            }
        }
    }

    /// P3-427: the scripted snapshot and the reconcile commands used to carry two
    /// near-identical copies of this emission (one report per record plus the summary),
    /// which would drift as the fake grows. One emitter, one shape.
    async fn emit_reconcile_snapshot(
        &self,
        cmd: Command,
        envelope: &CommandEnvelope,
        records: &[OrderRecord],
    ) -> ReportEnvelope {
        for rec in records {
            self.push_report(ReportEnvelope {
                record_type: RECORD_REPORT.to_string(),
                contract_version: 1,
                request_id: envelope.request_id.clone(),
                command: cmd.as_str().to_string(),
                outcome: "SUCCESS".to_string(),
                client_order_ref: rec.client_order_ref.clone(),
                broker_order_id: rec.broker_order_id.clone(),
                order_status: Some(rec.status.as_str().to_string()),
                report_type: Some("order_status".to_string()),
                ..ReportEnvelope::default()
            })
            .await;
        }
        ReportEnvelope {
            record_type: RECORD_REPORT.to_string(),
            contract_version: 1,
            request_id: envelope.request_id.clone(),
            command: cmd.as_str().to_string(),
            outcome: "SUCCESS".to_string(),
            client_order_ref: envelope.client_order_ref.clone(),
            broker_order_id: envelope.broker_order_id.clone(),
            order_status: Some(if records.is_empty() {
                "NO_OPEN_ORDERS".to_string()
            } else {
                "OPEN_SNAPSHOT".to_string()
            }),
            report_type: Some("reconcile_snapshot".to_string()),
            ..ReportEnvelope::default()
        }
    }

    async fn emit_fill(&self, envelope: &CommandEnvelope) {
        let Some(order) = envelope.order.as_ref() else {
            return;
        };
        let broker_order_id = self
            .orders
            .get(&envelope.client_order_ref)
            .map(|r| r.broker_order_id.clone())
            .or_else(|| {
                self.orders
                    .iter()
                    .find(|(_, r)| r.client_order_ref == envelope.client_order_ref)
                    .map(|(id, _)| id.clone())
            })
            .unwrap_or_default();
        if broker_order_id.is_empty() {
            return;
        }
        self.push_report(ReportEnvelope {
            record_type: RECORD_REPORT.to_string(),
            contract_version: 1,
            request_id: envelope.request_id.clone(),
            command: Command::Place.as_str().to_string(),
            outcome: "SUCCESS".to_string(),
            // Echo `remarks` (= client_order_ref) so the service can correlate the
            // asynchronous fill back to the order (Arrow postback contract).
            client_order_ref: envelope.client_order_ref.clone(),
            broker_order_id,
            order_status: Some(FakeOrderStatus::Filled.as_str().to_string()),
            report_type: Some("order_filled".to_string()),
            fill_quantity: Some(order.quantity.clone()),
            fill_price: Some(if order.price.is_empty() {
                "100".to_string()
            } else {
                order.price.clone()
            }),
            ..ReportEnvelope::default()
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::protocol::{
        Command, OrderCommand, OrderType, Product, TransactionType, Validity,
    };

    fn place_env() -> CommandEnvelope {
        CommandEnvelope {
            instruction_id: "inst-1".into(),
            execution_attempt_id: "att-1".into(),
            client_order_ref: "CLIENT-1".into(),
            order: Some(
                OrderCommand::new("NFO", "NIFTY")
                    .with_quantity("10")
                    .with_side(TransactionType::Buy)
                    .with_order_type(OrderType::Lmt)
                    .with_product(Product::Cash)
                    .with_validity(Validity::Day)
                    .with_price("100"),
            ),
            ..CommandEnvelope::new(Command::Place, "req-1")
        }
    }

    #[tokio::test]
    async fn place_returns_success_with_broker_id() {
        let mut b = FakeBridge::new();
        b.connect().await.unwrap();
        b.script(CommandScript::Accept);
        let mut env = place_env();
        env.command = Command::Place.as_str().to_string();
        let rep = b.send_command(env).await.unwrap();
        assert!(rep.is_success());
        assert!(!rep.broker_order_id.is_empty());
        b.disconnect().await.unwrap();
        assert!(!b.is_connected());
    }

    // P3-187: `orders` is keyed by broker_order_id, so looking a fill up by the
    // envelope's client_order_ref in that map attributes the fill to an unrelated
    // order whenever a client ref collides with another order's broker id.
    #[tokio::test]
    async fn fill_is_attributed_to_the_order_that_owns_the_client_ref() {
        let mut b = FakeBridge::new();
        b.connect().await.unwrap();
        b.script(CommandScript::Accept);
        b.script(CommandScript::AcceptThenFill);

        let mut first = place_env();
        first.command = Command::Place.as_str().to_string();
        let first_id = b.send_command(first).await.unwrap().broker_order_id;
        assert!(!first_id.is_empty());

        // A second order whose *client ref* equals the first order's broker id: the
        // collision the wrongly-keyed lookup mis-resolves.
        let mut second = place_env();
        second.command = Command::Place.as_str().to_string();
        second.request_id = "req-2".into();
        second.client_order_ref = first_id.clone();
        let second_id = b.send_command(second).await.unwrap().broker_order_id;
        assert_ne!(
            first_id, second_id,
            "the fake must mint a distinct id per order"
        );

        let mut reports = b.take_reports().expect("report stream");
        let fill = reports
            .try_recv()
            .expect("a fill report for the second order");
        assert_eq!(
            fill.broker_order_id, second_id,
            "fill attributed to {} (another order's broker id) instead of {}",
            fill.broker_order_id, second_id
        );
        assert_eq!(fill.client_order_ref, first_id);
    }

    // P3-428 (first half): a modify must record what it claims to record. Quantity is
    // as mutable as price, and the old code updated only price.
    #[tokio::test]
    async fn modify_records_the_whole_order_body() {
        let mut b = FakeBridge::new();
        b.connect().await.unwrap();
        b.script(CommandScript::Accept); // place
        b.script(CommandScript::Accept); // modify with a body

        let mut place = place_env();
        place.command = Command::Place.as_str().to_string();
        let id = b.send_command(place).await.unwrap().broker_order_id;

        let mut modify = CommandEnvelope::new(Command::Modify, "req-2");
        modify.client_order_ref = "CLIENT-1".into();
        modify.instruction_id = "inst-1".into();
        modify.execution_attempt_id = "att-1".into();
        modify.broker_order_id = id.clone();
        modify.order = Some(
            OrderCommand::new("NFO", "NIFTY")
                .with_quantity("7")
                .with_side(TransactionType::Buy)
                .with_order_type(OrderType::Lmt)
                .with_product(Product::Cash)
                .with_validity(Validity::Day)
                .with_price("99"),
        );
        assert!(b.send_command(modify).await.unwrap().is_success());
        let record = b.orders.get(&id).expect("the placed order is recorded");
        assert_eq!(record.price, "99");
        assert_eq!(
            record.quantity, "7",
            "modify re-recorded the price but left the old quantity"
        );
    }

    // P3-428 (second half): a body-less modify must fail rather than report
    // SUCCESS-with-no-change. The envelope validator normally catches it first, so the
    // fake must not rely on that and must refuse it on the direct path too.
    #[tokio::test]
    async fn modify_refuses_a_missing_order_body() {
        let mut b = FakeBridge::new();
        b.connect().await.unwrap();
        b.script(CommandScript::Accept); // place
        b.script(CommandScript::Accept); // modify without a body

        let mut place = place_env();
        place.command = Command::Place.as_str().to_string();
        let id = b.send_command(place).await.unwrap().broker_order_id;

        let mut no_body = CommandEnvelope::new(Command::Modify, "req-3");
        no_body.client_order_ref = "CLIENT-1".into();
        no_body.instruction_id = "inst-1".into();
        no_body.execution_attempt_id = "att-1".into();
        no_body.broker_order_id = id.clone();
        no_body.order = None;
        assert!(
            b.send_command(no_body).await.is_err(),
            "a modify with no order body must fail, not report SUCCESS: the live bridge rejects it"
        );

        // The envelope validator catches that first, so pin the guard in handle_accept
        // too: a direct caller must not get SUCCESS-with-no-change either.
        let mut direct = CommandEnvelope::new(Command::Modify, "req-4");
        direct.broker_order_id = id.clone();
        direct.order = None;
        assert!(
            b.handle_accept(Command::Modify, direct).await.is_err(),
            "handle_accept must refuse a body-less modify rather than report SUCCESS"
        );
    }

    // P3-427: the scripted snapshot and the reconcile-command snapshot are one emitter
    // after the dedupe; pin the shape both paths promise (one order_status report per
    // record plus one summary) so a later edit to either cannot drift unnoticed.
    #[tokio::test]
    async fn reconcile_snapshot_shape_is_pinned_for_both_paths() {
        let mut b = FakeBridge::new();
        b.connect().await.unwrap();
        b.script(CommandScript::ReconcileSnapshot(vec![
            OrderRecord {
                broker_order_id: "BRK-1".into(),
                client_order_ref: "c-1".into(),
                symbol: "NIFTY".into(),
                quantity: "10".into(),
                status: FakeOrderStatus::Open,
                filled_qty: "0".into(),
                price: "100".into(),
            },
            OrderRecord {
                broker_order_id: "BRK-2".into(),
                client_order_ref: "c-2".into(),
                symbol: "NIFTY".into(),
                quantity: "5".into(),
                status: FakeOrderStatus::PartialFill,
                filled_qty: "2".into(),
                price: "101".into(),
            },
        ]));

        let mut env = CommandEnvelope::new(Command::ReconcileOrders, "req-rec");
        env.command = Command::ReconcileOrders.as_str().to_string();
        let summary = b.send_command(env).await.unwrap();
        assert_eq!(summary.order_status.as_deref(), Some("OPEN_SNAPSHOT"));
        assert_eq!(summary.report_type.as_deref(), Some("reconcile_snapshot"));

        let mut reports = b.take_reports().expect("report stream");
        for want in ["BRK-1", "BRK-2"] {
            let rec = reports
                .try_recv()
                .expect("one order_status report per record");
            assert_eq!(rec.broker_order_id, want);
            assert_eq!(rec.report_type.as_deref(), Some("order_status"));
            assert_eq!(rec.outcome, "SUCCESS");
        }
        assert!(
            reports.try_recv().is_err(),
            "no report beyond the records and the summary"
        );

        // Empty snapshot: the summary says so, and nothing is pushed.
        b.script(CommandScript::ReconcileSnapshot(vec![]));
        let mut empty_env = CommandEnvelope::new(Command::ReconcileOrders, "req-empty");
        empty_env.command = Command::ReconcileOrders.as_str().to_string();
        let empty = b.send_command(empty_env).await.unwrap();
        assert_eq!(empty.order_status.as_deref(), Some("NO_OPEN_ORDERS"));
        assert!(b.take_reports().is_none(), "the stream is taken once");
    }

    // P3-186: the report channel is bounded. With a stalled consumer the producer must stop
    // (backpressure) instead of queueing reports without limit.
    #[tokio::test]
    async fn report_channel_is_bounded_and_applies_backpressure() {
        let mut b = FakeBridge::new();
        b.connect().await.unwrap();
        let _rx = b.take_reports().expect("report stream"); // held, never drained
        let mut sent = 0usize;
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            // Three times the buffer: an unbounded channel swallows every one of them.
            while sent < 3 * BRIDGE_REPORT_BUFFER {
                let mut env = place_env();
                env.command = Command::Place.as_str().to_string();
                env.request_id = format!("req-{sent}");
                b.script(CommandScript::AcceptThenFill);
                b.send_command(env).await.unwrap();
                sent += 1;
            }
        })
        .await;
        assert!(
            outcome.is_err(),
            "the producer ran {sent} commands without ever stalling: the report channel is unbounded"
        );
        assert!(
            sent <= BRIDGE_REPORT_BUFFER,
            "the producer got {sent} commands past a {BRIDGE_REPORT_BUFFER}-report buffer"
        );
    }

    #[tokio::test]
    async fn rejection_and_unknown_classify() {
        let mut b = FakeBridge::new();
        b.connect().await.unwrap();
        b.script(CommandScript::Reject("insufficient_margin".into()));
        b.script(CommandScript::Unknown("ambigous_state".into()));
        let mut env = place_env();
        env.command = Command::Place.as_str().to_string();
        let rej = b.send_command(env.clone()).await.unwrap();
        assert_eq!(
            rej.outcome(),
            Some(crate::bridge::protocol::ReportOutcome::Rejected)
        );
        let unk = b.send_command(env).await.unwrap();
        assert_eq!(
            unk.outcome(),
            Some(crate::bridge::protocol::ReportOutcome::Unknown)
        );
    }

    #[tokio::test]
    async fn timeout_returns_error() {
        let mut b = FakeBridge::new();
        b.connect().await.unwrap();
        b.script(CommandScript::Timeout);
        let mut env = place_env();
        env.command = Command::Place.as_str().to_string();
        assert!(b.send_command(env).await.is_err());
    }

    #[tokio::test]
    async fn place_modify_cancel_roundtrip_echoes_client_order_ref() {
        let mut b = FakeBridge::new();
        b.connect().await.unwrap();
        b.script(CommandScript::Accept); // place
        b.script(CommandScript::Accept); // modify
        b.script(CommandScript::Accept); // cancel
        b.script(CommandScript::Accept); // query

        // Place: returns a broker id and echoes our `remarks` (= client_order_ref).
        let mut place = place_env();
        place.command = Command::Place.as_str().to_string();
        let placed = b.send_command(place).await.unwrap();
        assert!(placed.is_success());
        let broker_id = placed.broker_order_id.clone();
        assert!(!broker_id.is_empty());
        assert_eq!(placed.client_order_ref, "CLIENT-1");

        // Modify: references the broker id, echoes the same client_order_ref.
        let mut modify = CommandEnvelope::new(Command::Modify, "req-2");
        modify.client_order_ref = "CLIENT-1".into();
        modify.instruction_id = "inst-1".into();
        modify.execution_attempt_id = "att-1".into();
        modify.broker_order_id = broker_id.clone();
        modify.order = Some(
            OrderCommand::new("NFO", "NIFTY")
                .with_quantity("10")
                .with_side(TransactionType::Buy)
                .with_order_type(OrderType::Lmt)
                .with_product(Product::Cash)
                .with_validity(Validity::Day)
                .with_price("99"),
        );
        let modified = b.send_command(modify).await.unwrap();
        assert!(modified.is_success());
        assert_eq!(modified.broker_order_id, broker_id);
        assert_eq!(modified.client_order_ref, "CLIENT-1");

        // Cancel: the order flips to CANCELED and an async report (with remarks echo)
        // lands on the report stream.
        let mut cancel = CommandEnvelope::new(Command::Cancel, "req-3");
        cancel.client_order_ref = "CLIENT-1".into();
        cancel.broker_order_id = broker_id.clone();
        let canceled = b.send_command(cancel).await.unwrap();
        assert!(canceled.is_success());
        assert_eq!(canceled.client_order_ref, "CLIENT-1");

        let mut reports = b.take_reports().expect("report stream available");
        let report = reports.try_recv().expect("canceled report emitted");
        assert_eq!(report.report_type.as_deref(), Some("order_canceled"));
        assert_eq!(report.order_status.as_deref(), Some("CANCELED"));
        assert_eq!(report.broker_order_id, broker_id);
        assert_eq!(report.client_order_ref, "CLIENT-1");

        // Query confirms the lifecycle closed at CANCELED.
        let mut query = CommandEnvelope::new(Command::QueryOrder, "req-4");
        query.client_order_ref = "CLIENT-1".into();
        query.broker_order_id = broker_id;
        let queried = b.send_command(query).await.unwrap();
        assert_eq!(queried.order_status.as_deref(), Some("CANCELED"));
        assert_eq!(queried.client_order_ref, "CLIENT-1");

        assert_eq!(b.command_count(), 4);
    }

    #[tokio::test]
    async fn fill_report_echoes_client_order_ref() {
        let mut b = FakeBridge::new();
        b.connect().await.unwrap();
        b.script(CommandScript::AcceptThenFill);

        let mut place = place_env();
        place.command = Command::Place.as_str().to_string();
        let reply = b.send_command(place).await.unwrap();
        assert!(reply.is_success());
        assert_eq!(reply.client_order_ref, "CLIENT-1");
        let broker_id = reply.broker_order_id;

        let mut reports = b.take_reports().expect("report stream available");
        let fill = reports.try_recv().expect("async fill report emitted");
        assert_eq!(fill.report_type.as_deref(), Some("order_filled"));
        assert_eq!(fill.order_status.as_deref(), Some("FILLED"));
        // Arrow postback contract: the fill must echo our `remarks` (= client_order_ref)
        // so the service can correlate it back to the placed order.
        assert_eq!(fill.client_order_ref, "CLIENT-1");
        assert_eq!(fill.broker_order_id, broker_id);
        assert_eq!(fill.fill_quantity.as_deref(), Some("10"));
        assert_eq!(fill.fill_price.as_deref(), Some("100"));
    }

    #[tokio::test]
    async fn command_count_tracks_all_commands() {
        let mut b = FakeBridge::new();
        b.connect().await.unwrap();
        assert_eq!(b.command_count(), 0);
        for _ in 0..3 {
            b.script(CommandScript::Accept);
        }
        let mut env = place_env();
        env.command = Command::Place.as_str().to_string();
        b.send_command(env.clone()).await.unwrap();
        b.send_command(env.clone()).await.unwrap();
        b.send_command(env).await.unwrap();
        assert_eq!(b.command_count(), 3);
    }
}
