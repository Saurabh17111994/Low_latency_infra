//! M1-3: one bridge session — one dial, one dispatcher, one correlation registry.
//!
//! Before M1-3 the route (gateway forward) leg built its own send-only `HttpBridgeClient`
//! and the node built a second one; the route client never called `take_reports`, so a
//! postback for a route order (fill/cancel/reject) had no consumer at all — the audit's
//! "second dial / lost route fill". This module owns the single transport: the route and
//! the node share one [`SessionHandle`], one connection, and one dispatcher that routes
//! every report by `client_order_ref` through the shared [`Registry`]:
//!
//! * `Route` → the dispatcher books the postback itself and emits the normalized gateway
//!   event on a spawned task (never blocking the report stream on a gateway round-trip);
//! * `Node` → the report is handed to the node's consumer channel (bounded; a full or
//!   closed channel is ambiguity, so it halts instead of dropping);
//! * unknown refs, and unrecognized postback `event_type`s for route refs, halt in ONE
//!   place — the same fail-closed rule the node already applies to its own reports.
//!
//! The node's booking state stays where it was: `BridgeExecutionClient` remains the only
//! owner of its `Rc`-based state and consumes the same `BridgeReportStream` contract; the
//! session only feeds it. `ensure_open` spawns the dispatcher before the route can send,
//! so a postback can never race an unregistered ref.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::{mpsc, Mutex as AsyncMutex};

use super::client::{BridgeClient, BridgeReportStream, SendFailure, BRIDGE_REPORT_BUFFER};
use super::protocol::{CommandEnvelope, ReportEnvelope};

/// Registry: `client_order_ref` → owning leg. Shared by the session (dispatcher), the
/// route forward leg (writes `Route`) and the node client (writes `Node`).
pub type Registry = Arc<Mutex<HashMap<String, ReportOwner>>>;

/// Which leg owns a `client_order_ref`.
///
/// `Route` carries the context boxed: the registry holds many entries and the context is
/// ~450 bytes, so the unboxed variant would make every `Node` entry pay for it.
#[derive(Debug, Clone)]
pub enum ReportOwner {
    /// Route (gateway forward) leg: the dispatcher books the postback itself.
    Route(Box<RouteContext>),
    /// Node (Nautilus) leg: the dispatcher forwards the report to the node's consumer.
    Node,
}

/// Everything the dispatcher needs to emit a route postback to the gateway.
#[derive(Debug, Clone)]
pub struct RouteContext {
    /// The command envelope the route leg sent (identity + order block for the event image).
    pub place: CommandEnvelope,
    pub account_scope_id: String,
    pub execution_partition_id: String,
    pub gate_epoch: i64,
    pub trade_context_id: String,
}

/// Route-leg postback emission seam. The production implementation reads the live gateway
/// snapshot and calls `events::`; tests inject a recorder. `None` on the session = the
/// offline/paper posture: a route postback is booked but not emitted (there is no gateway).
#[async_trait]
pub trait RoutePostbackEmitter: Send + Sync {
    async fn emit(&self, report: &ReportEnvelope, ctx: &RouteContext);
}

/// Process-wide halt seam for the dispatcher. Send+Sync because the dispatcher runs on a
/// tokio task, unlike the node's `Rc`-based `HaltNotifier` — the service binds both to the
/// same `ServerState::safety_halt`.
pub type SessionHalt = Arc<dyn Fn(&str) + Send + Sync>;

/// The single bridge session (M1-3).
pub struct BridgeSession {
    inner: AsyncMutex<SessionInner>,
    connected: AtomicBool,
    registry: Registry,
    node_tx: mpsc::Sender<ReportEnvelope>,
    node_rx: Mutex<Option<BridgeReportStream>>,
    halt: Option<SessionHalt>,
    emitter: Option<Arc<dyn RoutePostbackEmitter>>,
}

struct SessionInner {
    client: Box<dyn BridgeClient + Send>,
    opened: bool,
    dispatcher: Option<tokio::task::JoinHandle<()>>,
}

/// Cheap cloneable handle to the session; the route forwarder and the node client each hold
/// one, so both legs share the single transport.
#[derive(Clone)]
pub struct SessionHandle(Arc<BridgeSession>);

impl SessionHandle {
    /// Builds the session over `client` and returns the handle. `registry` is shared with
    /// the writers (route: `ServerState`; node: the execution client).
    #[must_use]
    pub fn new(
        client: Box<dyn BridgeClient + Send>,
        registry: Registry,
        halt: Option<SessionHalt>,
        emitter: Option<Arc<dyn RoutePostbackEmitter>>,
    ) -> Self {
        let (node_tx, node_rx) = mpsc::channel(BRIDGE_REPORT_BUFFER);
        Self(Arc::new(BridgeSession {
            inner: AsyncMutex::new(SessionInner {
                client,
                opened: false,
                dispatcher: None,
            }),
            connected: AtomicBool::new(false),
            registry,
            node_tx,
            node_rx: Mutex::new(Some(node_rx)),
            halt,
            emitter,
        }))
    }
}

impl BridgeSession {
    /// Connects the single transport once and spawns the dispatcher (idempotent).
    async fn ensure_open(self: &Arc<Self>) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if inner.opened {
            return Ok(());
        }
        inner.client.connect().await?;
        let reports = inner.client.take_reports();
        self.connected.store(true, Ordering::Relaxed);
        inner.opened = true;
        if let Some(reports) = reports {
            let session = Arc::clone(self);
            inner.dispatcher = Some(tokio::spawn(async move { session.dispatch(reports).await }));
        }
        Ok(())
    }

    /// The dispatcher: one place decides where every report goes (M1-3).
    async fn dispatch(self: Arc<Self>, mut reports: BridgeReportStream) {
        while let Some(report) = reports.recv().await {
            let owner = self
                .registry
                .lock()
                .ok()
                .and_then(|registry| registry.get(&report.client_order_ref).cloned());
            match owner {
                Some(ReportOwner::Node) => {
                    // Bounded hand-off; a full or closed channel means the node cannot
                    // receive a report it must book — ambiguity, never a silent drop.
                    if self.node_tx.try_send(report).is_err() {
                        self.halt(
                            "bridge report for the node leg could not be delivered; safety-halted",
                        );
                        return;
                    }
                }
                Some(ReportOwner::Route(ctx)) => {
                    // C1-4 dispatch: the canonical `event_type` vocabulary decides. A
                    // non-postback (e.g. reconcile echo) stays ignored; a postback with a
                    // missing/unrecognized type is ambiguous -> halt in one place.
                    match report.event_type.as_deref() {
                        Some("order_filled")
                        | Some("order_canceled")
                        | Some("order_rejected")
                        | Some("order_accepted") => {
                            if let Some(emitter) = &self.emitter {
                                // Spawned: a gateway round-trip must never stall the report
                                // stream (the node leg shares it).
                                let emitter = Arc::clone(emitter);
                                let report = report.clone();
                                let ctx = ctx.clone();
                                tokio::spawn(async move { emitter.emit(&report, &ctx).await });
                            }
                        }
                        _ if report.command == "postback" => {
                            self.halt(
                                "bridge postback with a missing or unrecognized event_type; \
                                 safety-halted",
                            );
                            return;
                        }
                        _ => {
                            tracing::debug!(
                                "ignoring non-postback bridge report for route ref {} (command={:?}, \
                                 report_type={:?})",
                                report.client_order_ref,
                                report.command,
                                report.report_type
                            );
                        }
                    }
                }
                None => {
                    self.halt("uncorrelated bridge report; safety-halted");
                    return;
                }
            }
        }
    }

    /// Fans a dispatcher halt out to the bound service surface (no-op when unbound).
    fn halt(&self, reason: &str) {
        if let Some(halt) = &self.halt {
            halt(reason);
        }
    }
}

#[async_trait]
impl BridgeClient for SessionHandle {
    fn is_connected(&self) -> bool {
        self.0.connected.load(Ordering::Relaxed)
    }

    async fn connect(&mut self) -> Result<()> {
        self.0.ensure_open().await
    }

    async fn disconnect(&mut self) -> Result<()> {
        let mut inner = self.0.inner.lock().await;
        self.0.connected.store(false, Ordering::Relaxed);
        if let Some(task) = inner.dispatcher.take() {
            task.abort();
        }
        inner.opened = false;
        inner.client.disconnect().await
    }

    async fn send_command(
        &mut self,
        envelope: CommandEnvelope,
    ) -> Result<ReportEnvelope, SendFailure> {
        // A transport that cannot be opened provably did not carry the command: NotSent.
        self.0.ensure_open().await.map_err(SendFailure::not_sent)?;
        let mut inner = self.0.inner.lock().await;
        inner.client.send_command(envelope).await
    }

    fn take_reports(&mut self) -> Option<BridgeReportStream> {
        self.0
            .node_rx
            .lock()
            .ok()
            .and_then(|mut guard| guard.take())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use super::*;
    use crate::bridge::protocol::Command;

    /// Test transport: counts dials, records sends, and lets the test push reports into
    /// the stream the dispatcher consumes.
    struct TestTransport {
        connects: Arc<AtomicU64>,
        take_calls: Arc<AtomicU64>,
        report_tx: mpsc::Sender<ReportEnvelope>,
        report_rx: Option<BridgeReportStream>,
        sent: Arc<Mutex<Vec<CommandEnvelope>>>,
    }

    impl TestTransport {
        fn new() -> Self {
            let (report_tx, report_rx) = mpsc::channel(BRIDGE_REPORT_BUFFER);
            Self {
                connects: Arc::new(AtomicU64::new(0)),
                take_calls: Arc::new(AtomicU64::new(0)),
                report_tx,
                report_rx: Some(report_rx),
                sent: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl BridgeClient for TestTransport {
        fn is_connected(&self) -> bool {
            true
        }

        async fn connect(&mut self) -> Result<()> {
            self.connects.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn disconnect(&mut self) -> Result<()> {
            Ok(())
        }

        async fn send_command(
            &mut self,
            envelope: CommandEnvelope,
        ) -> Result<ReportEnvelope, SendFailure> {
            self.sent.lock().unwrap().push(envelope);
            Ok(ReportEnvelope::default())
        }

        fn take_reports(&mut self) -> Option<BridgeReportStream> {
            self.take_calls.fetch_add(1, Ordering::Relaxed);
            self.report_rx.take()
        }
    }

    /// Recording emitter: sends every emitted report to the test.
    struct RecordingEmitter {
        emitted: mpsc::UnboundedSender<(ReportEnvelope, RouteContext)>,
    }

    #[async_trait]
    impl RoutePostbackEmitter for RecordingEmitter {
        async fn emit(&self, report: &ReportEnvelope, ctx: &RouteContext) {
            let _ = self.emitted.send((report.clone(), ctx.clone()));
        }
    }

    /// Captured halt surface.
    #[derive(Default)]
    struct HaltRecorder {
        halted: AtomicBool,
        reason: Mutex<String>,
    }

    fn report(ref_: &str, event_type: Option<&str>) -> ReportEnvelope {
        ReportEnvelope {
            client_order_ref: ref_.to_string(),
            command: "postback".to_string(),
            event_type: event_type.map(str::to_string),
            ..Default::default()
        }
    }

    fn route_ctx() -> RouteContext {
        RouteContext {
            place: CommandEnvelope::new(Command::Place, "inst-1"),
            account_scope_id: "acct".to_string(),
            execution_partition_id: "part".to_string(),
            gate_epoch: 1,
            trade_context_id: "tc-1".to_string(),
        }
    }

    struct Harness {
        connects: Arc<AtomicU64>,
        take_calls: Arc<AtomicU64>,
        report_tx: mpsc::Sender<ReportEnvelope>,
        registry: Registry,
        halt: Arc<HaltRecorder>,
        emitted: mpsc::UnboundedReceiver<(ReportEnvelope, RouteContext)>,
    }

    fn harness() -> (Harness, SessionHandle) {
        let transport = TestTransport::new();
        let connects = Arc::clone(&transport.connects);
        let take_calls = Arc::clone(&transport.take_calls);
        let report_tx = transport.report_tx.clone();
        let registry: Registry = Arc::new(Mutex::new(HashMap::new()));
        let halt = Arc::new(HaltRecorder::default());
        let halt_seam: SessionHalt = {
            let halt = Arc::clone(&halt);
            Arc::new(move |reason: &str| {
                halt.halted.store(true, Ordering::Relaxed);
                *halt.reason.lock().unwrap() = reason.to_string();
            })
        };
        let (emitted_tx, emitted_rx) = mpsc::unbounded_channel();
        let emitter: Arc<dyn RoutePostbackEmitter> = Arc::new(RecordingEmitter {
            emitted: emitted_tx,
        });
        let session = SessionHandle::new(
            Box::new(transport),
            Arc::clone(&registry),
            Some(halt_seam),
            Some(emitter),
        );
        (
            Harness {
                connects,
                take_calls,
                report_tx,
                registry,
                halt,
                emitted: emitted_rx,
            },
            session,
        )
    }

    #[tokio::test]
    async fn one_dial_for_route_and_node() {
        let (h, session) = harness();
        // The node connects; the route sends through a second handle of the same session.
        session.clone().connect().await.unwrap();
        let mut route = session.clone();
        let _ = route
            .send_command(CommandEnvelope::new(Command::Place, "i-1"))
            .await;
        assert_eq!(h.connects.load(Ordering::Relaxed), 1, "exactly one dial");
        assert_eq!(
            h.take_calls.load(Ordering::Relaxed),
            1,
            "exactly one report-stream take"
        );
    }

    #[tokio::test]
    async fn route_fill_is_booked_not_halted() {
        let (mut h, session) = harness();
        session.clone().connect().await.unwrap();
        h.registry.lock().unwrap().insert(
            "route-ref-1".to_string(),
            ReportOwner::Route(Box::new(route_ctx())),
        );

        h.report_tx
            .send(report("route-ref-1", Some("order_filled")))
            .await
            .unwrap();

        let (emitted, ctx) =
            tokio::time::timeout(std::time::Duration::from_secs(2), h.emitted.recv())
                .await
                .expect("route fill must be emitted")
                .expect("emitter channel open");
        assert_eq!(emitted.client_order_ref, "route-ref-1");
        assert_eq!(ctx.account_scope_id, "acct");
        assert!(
            !h.halt.halted.load(Ordering::Relaxed),
            "a route fill must not halt"
        );
    }

    #[tokio::test]
    async fn node_ref_routes_to_the_node_stream() {
        let (h, session) = harness();
        let mut node = session.clone();
        node.connect().await.unwrap();
        let mut node_rx = node.take_reports().expect("node stream");
        h.registry
            .lock()
            .unwrap()
            .insert("node-ref-1".to_string(), ReportOwner::Node);

        h.report_tx
            .send(report("node-ref-1", Some("order_accepted")))
            .await
            .unwrap();

        let got = tokio::time::timeout(std::time::Duration::from_secs(2), node_rx.recv())
            .await
            .expect("node report must arrive")
            .expect("channel open");
        assert_eq!(got.client_order_ref, "node-ref-1");
        assert!(!h.halt.halted.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn unknown_ref_halts_in_one_place() {
        let (mut h, session) = harness();
        session.clone().connect().await.unwrap();

        h.report_tx
            .send(report("never-registered", Some("order_filled")))
            .await
            .unwrap();

        // Wait for the dispatcher to consume the report.
        for _ in 0..200 {
            if h.halt.halted.load(Ordering::Relaxed) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(
            h.halt.halted.load(Ordering::Relaxed),
            "unknown ref must halt"
        );
        assert!(h.halt.reason.lock().unwrap().contains("uncorrelated"));
        assert!(
            h.emitted.try_recv().is_err(),
            "nothing may be emitted for an unknown ref"
        );
    }

    #[tokio::test]
    async fn route_postback_with_unknown_event_type_halts() {
        let (mut h, session) = harness();
        session.clone().connect().await.unwrap();
        h.registry.lock().unwrap().insert(
            "route-ref-2".to_string(),
            ReportOwner::Route(Box::new(route_ctx())),
        );

        h.report_tx
            .send(report("route-ref-2", Some("arrow_vocabulary_drift")))
            .await
            .unwrap();

        for _ in 0..200 {
            if h.halt.halted.load(Ordering::Relaxed) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(
            h.halt.halted.load(Ordering::Relaxed),
            "a route postback with an unrecognized event_type must halt"
        );
        assert!(h.emitted.try_recv().is_err());
    }

    #[tokio::test]
    async fn non_postback_traffic_is_ignored() {
        let (mut h, session) = harness();
        session.clone().connect().await.unwrap();
        h.registry.lock().unwrap().insert(
            "route-ref-3".to_string(),
            ReportOwner::Route(Box::new(route_ctx())),
        );

        let mut echo = report("route-ref-3", None);
        echo.command = "reconcile".to_string();
        h.report_tx.send(echo).await.unwrap();
        // A following canonical fill proves the stream kept flowing past the echo.
        h.report_tx
            .send(report("route-ref-3", Some("order_canceled")))
            .await
            .unwrap();

        let got = tokio::time::timeout(std::time::Duration::from_secs(2), h.emitted.recv())
            .await
            .expect("the canonical postback must still be emitted")
            .expect("emitter channel open");
        assert_eq!(got.0.event_type.as_deref(), Some("order_canceled"));
        assert!(!h.halt.halted.load(Ordering::Relaxed));
    }
}
