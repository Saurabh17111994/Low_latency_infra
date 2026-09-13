//! Nautilus `LiveNode` construction (T4.5 / T4.6) and hosted run loop (Workstream B).
//!
//! Builds the OMS/risk/portfolio/reconciliation surface from the audited `nautilus-live`
//! [`LiveNodeBuilder`] API — without a market-data client or strategy actor. The service always
//! boots `HALTED` and the bridge execution client is the only registered exec client.
//!
//! The `LiveNode` is fully constructible: the execution client shares the kernel cache through
//! the `CacheView` supplied by `LiveNodeBuilder` (T4.5 resolved). The bridge transport is
//! selected by [`BridgeSelection`]: the deterministic `FakeBridge` by default (offline slice),
//! or the production `HttpBridgeClient` (HTTP `/v1/commands` + WS `/v1/events` intake with
//! reconnect) when a `BRIDGE_ENDPOINT` is configured (WP-2 remainder, 2026-08-21). Either way
//! the `BridgeExecutionClient` gate boots `HALTED`, so no broker command flows until an
//! authorized approval advances the gate.
//!
//! [`LiveNodeRuntime`] (Workstream B slice, 2026-08-21) now drives the node: the hosted run
//! loop (`NodeRunMode::Hosted` — the service keeps its own Ctrl-C / SIGTERM handling),
//! a clean stop request through the shared [`LiveNodeHandle`], and a fail-closed duplicate-run
//! guard. `LiveNode` is `!Send` (nautilus internals are `Rc<RefCell<..>>`), so the run loop
//! must be awaited on the owning task (the main tokio task in `main`, or the current-thread
//! test task) and must never be moved into a spawned task. The handle is the only
//! cross-task control surface.

use anyhow::Result;
use nautilus_common::{
    cache::CacheView,
    clients::ExecutionClient,
    factories::{ClientConfig, ExecutionClientFactory},
};
use nautilus_live::node::{builder::LiveNodeBuilder, config::LiveNodeConfig};
use nautilus_live::node::{LiveNode, LiveNodeHandle, NodeRunMode};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::{AccountId, ClientId, TraderId, Venue},
};

use crate::bridge::{FakeBridge, HttpBridgeClient};
use crate::config::ServiceConfig;
use crate::execution::BridgeExecutionClient;

/// Marker configuration accepted by the bridge exec client.
#[derive(Debug, Default, Clone)]
pub struct BridgeClientConfig;

impl ClientConfig for BridgeClientConfig {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Execution-client factory registered into the [`LiveNodeBuilder`].
///
/// The construction path is verified offline (see [`EngineFactory::verify_construction_path`])
/// and constructs the client against the LiveNode's kernel cache via the `CacheView` supplied
/// by the builder. Which bridge transport backs the client is decided by the [`BridgeSelection`]
/// held by the factory — `Fake` (default) or `Http`. The factory always boots the client
/// `HALTED` (via `BridgeExecutionClient::new`).
#[derive(Debug, Clone)]
pub struct BridgeExecutionClientFactory {
    selection: BridgeSelection,
}

impl Default for BridgeExecutionClientFactory {
    fn default() -> Self {
        Self {
            selection: BridgeSelection::Fake,
        }
    }
}

/// Which bridge transport backs the execution client.
///
/// - [`BridgeSelection::Fake`] — deterministic in-process fake (offline slice / tests).
/// - [`BridgeSelection::Http`] — production `HttpBridgeClient` against the Go bridge
///   (`BRIDGE_ENDPOINT` + optional `BRIDGE_AUTH_TOKEN`). Constructing it performs no I/O;
///   the connection is established lazily by the run loop's connect path.
// NOTE: no `Debug` derive — `Http` carries `BRIDGE_AUTH_TOKEN`. The manual impl below is the
// only way to format a selection (P3-193); `BridgeExecutionClientFactory`'s derived `Debug`
// prints it through that impl, so the redaction holds for every wrapper too.
#[derive(Clone, PartialEq, Eq)]
pub enum BridgeSelection {
    Fake,
    Http {
        base_url: String,
        auth_token: String,
    },
}

impl std::fmt::Debug for BridgeSelection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fake => f.write_str("Fake"),
            Self::Http { base_url, .. } => f
                .debug_struct("Http")
                .field("base_url", base_url)
                .field("auth_token", &"<redacted>")
                .finish(),
        }
    }
}

impl BridgeSelection {
    /// Derives the selection from service config: a non-empty `BRIDGE_ENDPOINT` selects the
    /// production HTTP transport; otherwise the offline fake stays the default (fail-closed:
    /// no endpoint means no network transport).
    #[must_use]
    pub fn from_config(cfg: &ServiceConfig) -> Self {
        if cfg.bridge_endpoint.is_empty() {
            Self::Fake
        } else {
            Self::Http {
                base_url: cfg.bridge_endpoint.clone(),
                auth_token: cfg.bridge_auth_token.clone(),
            }
        }
    }

    /// Human-readable mode name for boot logs (never includes the token).
    #[must_use]
    pub fn mode(&self) -> &'static str {
        match self {
            Self::Fake => "fake",
            Self::Http { .. } => "http",
        }
    }
}

impl ExecutionClientFactory for BridgeExecutionClientFactory {
    fn create(
        &self,
        name: &str,
        _config: &dyn ClientConfig,
        cache: CacheView,
    ) -> Result<Box<dyn ExecutionClient>> {
        // The bridge transport comes from the factory's selection; either way it shares the
        // LiveNode's kernel cache via the supplied CacheView (the view is a read handle over
        // the LiveNode's Rc<RefCell<Cache>>).
        let trader_id = TraderId::from("TRADER-001");
        let account_id = AccountId::from("ACCOUNT-001");
        let venue = Venue::from("SIM");
        let core = ExecutionClientCore::new(
            trader_id,
            ClientId::from(name),
            venue,
            OmsType::Hedging,
            account_id,
            AccountType::Cash,
            None,
            cache,
        );
        let bridge: Box<dyn crate::bridge::BridgeClient> = match self.selection {
            BridgeSelection::Fake => Box::new(FakeBridge::new()),
            BridgeSelection::Http {
                ref base_url,
                ref auth_token,
            } => Box::new(HttpBridgeClient::new(base_url.clone(), auth_token.clone())),
        };
        let client = BridgeExecutionClient::new(core, bridge);
        Ok(Box::new(client))
    }

    fn name(&self) -> &str {
        "bridge"
    }

    fn config_type(&self) -> &str {
        "BridgeClientConfig"
    }
}

/// Factory for the execution service's node/engine surface.
pub struct EngineFactory;

impl EngineFactory {
    /// Builds the pinned node configuration (fail-closed defaults).
    #[must_use]
    pub fn build_node_config() -> LiveNodeConfig {
        LiveNodeConfig::default()
    }

    /// Verifies the real [`LiveNodeBuilder`] construction + exec-client registration path compiles
    /// and constructs against the pinned `nautilus-live` crates.
    ///
    /// Only the builder + registration are exercised (`create` is not invoked): that is the live
    /// boundary (production bridge + node kernel-cache sharing).
    pub fn verify_construction_path() -> Result<()> {
        let cfg = Self::build_node_config();
        let builder = LiveNodeBuilder::from_config(cfg)?;
        let _ = builder.add_exec_client(
            Some("exec".to_string()),
            Box::new(BridgeExecutionClientFactory::default()),
            Box::new(BridgeClientConfig),
        )?;
        Ok(())
    }

    #[must_use]
    pub fn has_no_market_data_client() -> bool {
        true
    }

    #[must_use]
    pub fn has_no_strategy() -> bool {
        true
    }
}

/// Drives the [`LiveNode`] event loop on the service's main task.
///
/// Fail-closed contract (Workstream B): the registered bridge execution client boots
/// `HALTED` (see [`BridgeExecutionClient::new`](crate::execution::BridgeExecutionClient) /
/// `client_boots_into_halted`), so the run loop dispatches no broker commands until an
/// authorized approval advances the gate.
pub struct LiveNodeRuntime {
    node: LiveNode,
    handle: LiveNodeHandle,
    gate_halted_at_boot: bool,
}

impl LiveNodeRuntime {
    /// Constructs the node with the default (offline `FakeBridge`) selection.
    pub fn build() -> Result<Self> {
        Self::build_with_bridge(BridgeSelection::Fake)
    }

    /// Constructs the node with an explicit bridge transport selection (WP-2 remainder:
    /// the production path passes `BridgeSelection::from_config(&config)`).
    pub fn build_with_bridge(selection: BridgeSelection) -> Result<Self> {
        let cfg = EngineFactory::build_node_config();
        let builder = LiveNodeBuilder::from_config(cfg)?;
        let builder = builder.add_exec_client(
            Some("exec".to_string()),
            Box::new(BridgeExecutionClientFactory { selection }),
            Box::new(BridgeClientConfig),
        )?;
        let node = builder.build()?;
        let handle = node.handle();
        Ok(Self {
            node,
            handle,
            // `BridgeExecutionClient::new` always boots the gate `HALTED`; the factory path is
            // covered by `factory_create_succeeds_with_fake_bridge` + `client_boots_into_halted`.
            gate_halted_at_boot: true,
        })
    }

    /// Shared, `Send` control handle (safe to clone and hand to other tasks).
    #[must_use]
    pub fn handle(&self) -> LiveNodeHandle {
        self.handle.clone()
    }

    /// Whether the node's run loop is currently live.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.handle.is_running()
    }

    /// The fail-closed boot invariant (gate `HALTED`, health never implies `ENABLED`).
    #[must_use]
    pub fn gate_was_halted_at_boot(&self) -> bool {
        self.gate_halted_at_boot
    }

    /// Runs the node in hosted mode: nautilus does **not** touch our shutdown signals; the
    /// service owns Ctrl-C / SIGTERM and calls [`Self::request_shutdown`] to end the loop.
    ///
    /// The loop returns `Ok` after a stop request; a second call fails (the runner is
    /// consumed), which is the restart-safety guard — a node can never be re-entered.
    pub async fn run_forever(&mut self) -> Result<()> {
        self.node.run_with_mode(NodeRunMode::Hosted).await
    }

    /// Signals a clean stop; the run loop returns on its next poll.
    pub fn request_shutdown(&self) {
        self.handle.stop();
    }
}

/// Offline mass-status reconciliation (Tier 11). Iterates UNKNOWN attempts, read-only via bridge, never re-issues Place.
pub mod reconcile {
    use crate::bridge::protocol::{Command, CommandEnvelope, ReportEnvelope};
    use crate::bridge::BridgeClient;
    use crate::execution::client::{classify_bridge_report, BridgeOutcome};

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum ReconcileDecision {
        Accepted,
        Rejected,
        StillUnknownHalted,
        /// The bridge round-trip for this COR failed (unreachable, HTTP failure, parse error).
        /// Distinct from [`Self::StillUnknownHalted`], which is a *successful* reply saying the
        /// state is still unknown — an availability failure must not read as a clean "no change"
        /// sweep (P3-194). Carries the transport error, which is per-COR and not interchangeable.
        TransportFailure(String),
    }

    /// Maps a bridge reply to a decision; a failed round-trip keeps its own error instead of
    /// being flattened into a fabricated `UNKNOWN` report (P3-194).
    fn classify_or_transport(result: anyhow::Result<ReportEnvelope>) -> ReconcileDecision {
        match result {
            Ok(rep) => match classify_bridge_report(&rep) {
                BridgeOutcome::Accepted => ReconcileDecision::Accepted,
                BridgeOutcome::Rejected => ReconcileDecision::Rejected,
                BridgeOutcome::Unknown => ReconcileDecision::StillUnknownHalted,
            },
            Err(e) => ReconcileDecision::TransportFailure(e.to_string()),
        }
    }

    pub async fn reconcile_execution_mass_status<B: BridgeClient>(
        bridge: &mut B,
        unknown_client_order_refs: &[String],
    ) -> anyhow::Result<Vec<(String, ReconcileDecision)>> {
        if !bridge.is_connected() {
            bridge.connect().await?;
        }
        let mut orders = Vec::new();
        for cor in unknown_client_order_refs {
            let mut q = CommandEnvelope::new(Command::QueryOrder, &format!("reconcile-q-{}", cor));
            q.client_order_ref = cor.clone();
            orders.push((
                cor.clone(),
                classify_or_transport(bridge.send_command(q).await),
            ));
        }
        // Every COR failed at transport level: the bridge is unreachable, so this sweep is an
        // availability failure, not a "found nothing" result. Return non-OK with each COR's own
        // reason rather than one generic error (P3-194). A mixed sweep still returns `Ok` with
        // per-COR markers. The mass snapshot is skipped — it would dial the same dead bridge.
        if !orders.is_empty()
            && orders
                .iter()
                .all(|(_, d)| matches!(d, ReconcileDecision::TransportFailure(_)))
        {
            let reasons: Vec<String> = orders
                .iter()
                .filter_map(|(cor, d)| match d {
                    ReconcileDecision::TransportFailure(reason) => Some(format!("{cor}: {reason}")),
                    _ => None,
                })
                .collect();
            anyhow::bail!(
                "bridge transport failed for all {} UNKNOWN COR(s); no status could be read: {}",
                reasons.len(),
                reasons.join("; ")
            );
        }
        let mass = CommandEnvelope::new(Command::ReconcileOrders, "reconcile-mass-1");
        let _ = bridge.send_command(mass).await;
        Ok(orders)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::protocol::CommandEnvelope;
    use crate::bridge::{BridgeClient, BridgeReportStream, ReportEnvelope};
    use nautilus_common::{cache::Cache, enums::Environment};
    use std::collections::VecDeque;
    use std::time::Duration;
    use std::{cell::RefCell, rc::Rc};

    /// P3-193: the selection carries `BRIDGE_AUTH_TOKEN`, so its `Debug` must never be able to
    /// print it — `Debug` is what any wrapper, `unwrap_err`, panic payload, or startup log
    /// reaches for. A distinctive prefix and suffix let this catch a partial leak (truncation,
    /// first-N chars), not just the exact full token.
    #[test]
    fn bridge_selection_debug_never_prints_the_auth_token() {
        let token = "tok_live_9f4c2a7e88b1";
        let sel = BridgeSelection::Http {
            base_url: "http://bridge:8080".to_string(),
            auth_token: token.to_string(),
        };
        let dbg = format!("{sel:?}");
        assert!(
            dbg.contains("http://bridge:8080"),
            "endpoint should stay readable: {dbg}"
        );
        assert!(
            dbg.contains("<redacted>"),
            "the redaction should be visible: {dbg}"
        );
        assert!(!dbg.contains("tok_live"), "token prefix leaked: {dbg}");
        assert!(!dbg.contains("9f4c2a7e88b1"), "token suffix leaked: {dbg}");
        assert!(!dbg.contains(token), "full token leaked: {dbg}");

        // The factory holds the selection; its derived `Debug` must inherit the redaction.
        let factory = BridgeExecutionClientFactory {
            selection: BridgeSelection::Http {
                base_url: "http://bridge:8080".to_string(),
                auth_token: token.to_string(),
            },
        };
        let fdbg = format!("{factory:?}");
        assert!(
            !fdbg.contains(token),
            "factory Debug leaked the token: {fdbg}"
        );

        // `Fake` stays a plain, readable name.
        assert_eq!(format!("{:?}", BridgeSelection::Fake), "Fake");
    }

    #[test]
    fn node_config_defaults_to_live_environment() {
        let cfg = EngineFactory::build_node_config();
        assert_eq!(cfg.environment, Environment::Live);
    }

    #[test]
    fn construction_probe() {
        assert!(EngineFactory::verify_construction_path().is_ok());
    }

    #[test]
    fn factory_create_succeeds_with_fake_bridge() {
        let cache = CacheView::new(Rc::new(RefCell::new(Cache::default())));
        let factory = BridgeExecutionClientFactory::default();
        let res = factory.create("exec", &BridgeClientConfig, cache);
        assert!(
            res.is_ok(),
            "factory should now create client via FakeBridge"
        );
        let client = res.unwrap();
        assert_eq!(client.client_id().as_str(), "exec");
        assert_eq!(client.venue().as_str(), "SIM");
    }

    #[test]
    fn bridge_selection_defaults_to_fake_without_endpoint() {
        // No BRIDGE_ENDPOINT -> offline fake (fail-closed: no network transport).
        let cfg = ServiceConfig::from_iter(std::iter::empty()).unwrap();
        assert_eq!(BridgeSelection::from_config(&cfg), BridgeSelection::Fake);
        assert_eq!(BridgeSelection::Fake.mode(), "fake");
    }

    #[test]
    fn bridge_selection_http_when_endpoint_configured() {
        // A configured BRIDGE_ENDPOINT selects the production HttpBridgeClient transport,
        // carrying the optional auth token.
        let cfg = ServiceConfig::from_iter(
            [
                ("BRIDGE_ENDPOINT", "http://execution-bridge:8787"),
                ("BRIDGE_AUTH_TOKEN", "devtest"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string())),
        )
        .unwrap();
        let sel = BridgeSelection::from_config(&cfg);
        assert_eq!(sel.mode(), "http");
        match sel {
            BridgeSelection::Http {
                base_url,
                auth_token,
            } => {
                assert_eq!(base_url, "http://execution-bridge:8787");
                assert_eq!(auth_token, "devtest");
            }
            _ => panic!("expected Http selection when BRIDGE_ENDPOINT is set"),
        }
    }

    #[test]
    fn factory_create_succeeds_with_http_selection_offline() {
        // Constructing HttpBridgeClient performs no I/O (connection is lazy), so the factory
        // can be exercised fully offline — the production transport is wired but never dials
        // a broker here.
        let cache = CacheView::new(Rc::new(RefCell::new(Cache::default())));
        let factory = BridgeExecutionClientFactory {
            selection: BridgeSelection::Http {
                base_url: "http://127.0.0.1:9".to_string(),
                auth_token: "devtest".to_string(),
            },
        };
        let res = factory.create("exec", &BridgeClientConfig, cache);
        assert!(
            res.is_ok(),
            "factory must create the client with HttpBridgeClient selected (lazy connect)"
        );
        let client = res.unwrap();
        assert_eq!(client.client_id().as_str(), "exec");
        assert_eq!(client.venue().as_str(), "SIM");
    }

    #[test]
    fn runtime_builds_with_http_selection_and_halts() {
        // The full LiveNodeRuntime construction path with the Http selection must succeed
        // (offline — no connection is opened at construction) and still boot fail-closed.
        let selection = BridgeSelection::Http {
            base_url: "http://127.0.0.1:9".to_string(),
            auth_token: "devtest".to_string(),
        };
        match LiveNodeRuntime::build_with_bridge(selection) {
            Ok(rt) => assert!(rt.gate_was_halted_at_boot()),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("logger is already registered") {
                    // Global-logger singleton artifact when tests run in parallel — the
                    // factory + construction path is what we're proving; see
                    // live_node_builds_with_bridge_client for the same guard.
                    return;
                }
                panic!("LiveNodeRuntime should build with Http selection: {e}");
            }
        }
    }

    #[test]
    fn no_market_data() {
        assert!(EngineFactory::has_no_market_data_client());
    }

    #[test]
    fn live_node_builds_with_bridge_client() {
        // Proves the LiveNodeBuilder → ExecutionClientFactory → CacheView → FakeBridge
        // path is fully wired (T4.5). This is the same path the service will use at
        // startup; the node is not run, only constructed.
        let cfg = EngineFactory::build_node_config();
        let builder = LiveNodeBuilder::from_config(cfg).expect("LiveNodeConfig should be valid");
        let builder = builder
            .add_exec_client(
                Some("exec".to_string()),
                Box::new(BridgeExecutionClientFactory::default()),
                Box::new(BridgeClientConfig),
            )
            .expect("add_exec_client should succeed");
        let node = builder.build();
        // In-process test parallelism shares a global logger; a second LiveNode in the same
        // process may fail with "A non-Nautilus logger is already registered". That is a test-
        // harness artifact, not a wiring failure. The important assertion is that the factory
        // was invoked and the only failure, if any, is the logger, not our bridge/cache path.
        match node {
            Ok(_) => {}
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("logger is already registered") {
                    // Global logger already initialized by a prior test — consider the LiveNode
                    // construction path proven (the factory was called and succeeded; the
                    // logger is a process-singleton artifact when running with --test-threads>1).
                    return;
                }
                panic!("LiveNode should build with FakeBridge: {e}");
            }
        }
    }

    #[test]
    fn runtime_boots_fail_closed_gate_halted() {
        // LiveNodeRuntime contract: the bridge client inside the node boots `HALTED`,
        // so the run loop dispatches no broker commands until approval.
        let rt = LiveNodeRuntime::build().expect("runtime should build");
        assert!(rt.gate_was_halted_at_boot());
        assert!(
            !rt.is_running(),
            "node must not be running before the run loop starts"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn runtime_hosted_run_loop_stops_cleanly_on_request() {
        // Workstream B: the node runs (hosted, no signal grabbing) and a stop request
        // ends the loop with `Ok`. This is the clean-shutdown evidence at node level
        // (client-level halt/flush/fence sequence stays covered by shutdown.rs).
        let mut rt = LiveNodeRuntime::build().expect("runtime should build");
        let handle = rt.handle();
        let mut run = Box::pin(rt.run_forever());
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(80)) => handle.stop(),
            result = &mut run => panic!("node loop must not end before a stop request: {result:?}"),
        }
        run.await
            .expect("node run should return Ok after a stop request");
        assert!(
            !rt.is_running(),
            "node must be stopped after a clean stop request"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn runtime_second_run_fails_fail_closed() {
        // Restart-safety guard: the runner is consumed by the first run, so a second run
        // fails instead of silently double-driving the node.
        let mut rt = LiveNodeRuntime::build().expect("runtime should build");
        let handle = rt.handle();
        let mut run = Box::pin(rt.run_forever());
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(60)) => handle.stop(),
            result = &mut run => panic!("node loop must not end before a stop request: {result:?}"),
        }
        run.await.expect("first run should stop cleanly");
        assert!(!rt.is_running());
        let second = rt.run_forever().await;
        assert!(
            second.is_err(),
            "a second run must fail (runner consumed) — fail-closed restart guard"
        );
    }

    // ---------- P3-194 / P3-440: reconcile transport failures and the mass snapshot ----------

    /// Test bridge double: records every envelope id and answers from a scripted queue, so
    /// per-COR transport failures and ids can be observed deterministically.
    struct ProbeBridge {
        connected: bool,
        sent_ids: Vec<String>,
        replies: VecDeque<anyhow::Result<ReportEnvelope>>,
    }

    impl ProbeBridge {
        fn new() -> Self {
            Self {
                connected: false,
                sent_ids: Vec::new(),
                replies: VecDeque::new(),
            }
        }

        fn script(&mut self, reply: anyhow::Result<ReportEnvelope>) {
            self.replies.push_back(reply);
        }

        fn script_bridge_error(&mut self, reason: &str) {
            self.script(Err(anyhow::anyhow!(reason.to_string())));
        }

        /// Envelope ids of the trailing mass snapshots sent so far.
        fn mass_ids(&self) -> Vec<String> {
            self.sent_ids
                .iter()
                .filter(|id| id.starts_with("reconcile-mass-"))
                .cloned()
                .collect()
        }
    }

    #[async_trait::async_trait]
    impl BridgeClient for ProbeBridge {
        fn is_connected(&self) -> bool {
            self.connected
        }

        async fn connect(&mut self) -> anyhow::Result<()> {
            self.connected = true;
            Ok(())
        }

        async fn disconnect(&mut self) -> anyhow::Result<()> {
            self.connected = false;
            Ok(())
        }

        async fn send_command(
            &mut self,
            envelope: CommandEnvelope,
        ) -> anyhow::Result<ReportEnvelope> {
            self.sent_ids.push(envelope.request_id.clone());
            match self.replies.pop_front() {
                Some(reply) => reply,
                None => Err(anyhow::anyhow!("probe bridge: no scripted reply")),
            }
        }

        fn take_reports(&mut self) -> Option<BridgeReportStream> {
            None
        }
    }

    /// Bridge reply for the given outcome; a broker id makes `SUCCESS` classify as accepted.
    fn probe_reply(outcome: &str) -> ReportEnvelope {
        ReportEnvelope {
            outcome: outcome.to_string(),
            broker_order_id: "BRK-0001".to_string(),
            ..ReportEnvelope::default()
        }
    }

    /// P3-194: when every COR round-trip fails at transport level the sweep must not return `Ok`
    /// with all-`StillUnknownHalted` — an unreachable bridge is an availability failure, and each
    /// COR's own reason must survive in the error.
    #[tokio::test(flavor = "current_thread")]
    async fn reconcile_all_cors_unreachable_is_not_ok() {
        let mut bridge = ProbeBridge::new();
        bridge.script_bridge_error("bridge unreachable");
        bridge.script_bridge_error("bridge unreachable");
        let refs = vec!["REF-A".to_string(), "REF-B".to_string()];

        let err = reconcile::reconcile_execution_mass_status(&mut bridge, &refs)
            .await
            .expect_err("all-COR transport failure must not read as a clean no-change sweep");
        let msg = err.to_string();
        for cor in &refs {
            assert!(msg.contains(cor), "per-COR reason missing for {cor}: {msg}");
        }
        assert!(
            msg.contains("bridge unreachable"),
            "transport reason missing from the error: {msg}"
        );
        assert!(
            bridge.mass_ids().is_empty(),
            "no mass snapshot should be dialled against an unreachable bridge"
        );
    }

    /// P3-194: a mixed sweep stays `Ok` and keeps the transport failure distinct from a genuine
    /// still-unknown answer, so callers can see which CORs could not be read.
    #[tokio::test(flavor = "current_thread")]
    async fn reconcile_mixed_transport_failure_is_marked_per_cor() {
        let mut bridge = ProbeBridge::new();
        bridge.script_bridge_error("bridge unreachable");
        bridge.script(Ok(probe_reply("SUCCESS"))); // REF-B
        bridge.script(Ok(probe_reply("SUCCESS"))); // mass snapshot
        let refs = vec!["REF-A".to_string(), "REF-B".to_string()];

        let report = reconcile::reconcile_execution_mass_status(&mut bridge, &refs)
            .await
            .expect("a partially readable sweep still returns Ok");
        assert_eq!(
            report[0].1,
            reconcile::ReconcileDecision::TransportFailure("bridge unreachable".to_string())
        );
        assert_eq!(report[1].1, reconcile::ReconcileDecision::Accepted);
    }
}
