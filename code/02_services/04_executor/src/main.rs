//! Nautilus execution service — real bootstrap (WP-1) + hosted `LiveNode` run loop (Workstream B).
//!
//! Parses strict config, builds the fail-closed [`Runtime`] (gate boots `HALTED`, health never
//! implies `ENABLED`), starts the [`LiveNodeRuntime`] hosted run loop (bridge execution client
//! boots `HALTED`, no broker commands), serves `/healthz` + `/readyz`, and shuts down cleanly
//! on Ctrl-C / SIGTERM: stop request → run loop returns → draining (`/readyz` flips to 503).
//! No broker credentials are read here.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use nautilus_execution_service::{
    bootstrap::Runtime,
    bridge::{BridgeClient, CommandScript, FakeBridge, HttpBridgeClient},
    clockwatch::{DriftMonitor, FixedOffsetSource},
    config::ServiceConfig,
    engine::{BridgeSelection, LiveNodeRuntime},
    http, telemetry,
};

/// Builds the route's bridge transport (T4a sync forward) from the same selection the node's
/// exec client uses: the deterministic fake (offline slice, seeded with an Accept script) or
/// the production `HttpBridgeClient`. The forwarder is fail-closed by construction — it is
/// only ever reached when the route's gate is ENABLED.
fn build_route_forwarder(selection: &BridgeSelection) -> Box<dyn BridgeClient + Send> {
    match selection {
        BridgeSelection::Fake => {
            let mut fake = FakeBridge::new();
            fake.script(CommandScript::Accept);
            Box::new(fake)
        }
        BridgeSelection::Http {
            base_url,
            auth_token,
        } => Box::new(HttpBridgeClient::new(base_url.clone(), auth_token.clone())),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = ServiceConfig::from_env()?;
    let addr: SocketAddr = config.listen_addr()?;
    // WP-2 remainder (2026-08-21): a configured BRIDGE_ENDPOINT selects the production
    // HttpBridgeClient transport; no endpoint keeps the offline FakeBridge default. Either
    // way the execution client boots HALTED — no broker command flows until an authorized
    // approval advances the gate.
    let selection = BridgeSelection::from_config(&config);
    let bridge_mode = selection.mode();
    let route_forwarder: http::BridgeForwarder =
        Arc::new(tokio::sync::Mutex::new(build_route_forwarder(&selection)));
    let gateway_endpoint = config.gateway_endpoint.clone();
    let mut runtime = Runtime::init(config)?;
    // Nautilus's kernel registers the process-wide `log` logger (LoggerConfig owns
    // set_boxed_logger); our own tracing subscriber registers a `log` bridge
    // (tracing-subscriber's tracing-log feature) and must therefore come SECOND —
    // otherwise the kernel errors "A non-Nautilus logger is already registered" and the
    // service aborts at boot. Tracing still works: set_global_default is a separate
    // system; only the `log` bridge is skipped. `init_logging` is infallible by design
    // (no `Result`): "already installed" is the ordinary case on this boot path.
    let mut node = LiveNodeRuntime::build_with_bridge(selection)?;

    telemetry::init_logging("info");
    telemetry::METRICS.record_restart();

    tracing::info!(
        "nautilus-execution-service boot: gate HALTED, bridge mode {bridge_mode}, LiveNode hosted run loop armed, health on {addr} (execution enabled: false)"
    );

    let state = runtime
        .server_state()
        .with_forwarder(route_forwarder)
        .with_gateway_endpoint(gateway_endpoint);
    let server = tokio::spawn(http::serve(addr, state));

    // B8 clock-drift safety: the offline slice samples a fixed zero offset (no NTP on the
    // laptop dev box — a real NTP/chrony source is a Workstream-D/prod concern behind the
    // same OffsetSource trait, see clockwatch.rs). The monitor enforces CLOCK_OFFSET_LIMIT_MS
    // on the gate: |offset| beyond the limit (or an unmeasurable probe) fails closed to
    // HALTED; recovery is only ever the sanctioned reconcile -> approval -> enable path.
    let drift_interval = Duration::from_secs(
        std::env::var("CLOCK_DRIFT_CHECK_INTERVAL_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30),
    );
    let mut drift_monitor = DriftMonitor::new(
        runtime.config.clock_offset_limit_ms,
        Box::new(FixedOffsetSource(0)),
    );
    let mut drift_tick = tokio::time::interval(drift_interval);
    drift_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tracing::info!(
        interval_s = drift_interval.as_secs(),
        limit_ms = runtime.config.clock_offset_limit_ms,
        "clock-drift monitor armed (fixed zero-offset source; NTP in Workstream D)"
    );

    // Run until a shutdown signal or the node loop ends; the periodic drift check is a
    // non-terminal branch (a drift halt is enforced on the gate, the process keeps serving).
    loop {
        tokio::select! {
            _ = wait_for_shutdown_signal() => {
                tracing::info!("shutdown signal received; stopping LiveNode, draining (readyz -> 503)");
                node.request_shutdown();
                break;
            }
            result = node.run_forever() => {
                // The loop must not end on its own in normal operation (node stays HALTED).
                result?;
                break;
            }
            _ = drift_tick.tick() => {
                let status = runtime.enforce_clock_drift(&mut drift_monitor);
                tracing::debug!(status = ?status, "periodic clock-drift enforcement");
            }
        }
    }
    runtime.begin_shutdown();
    server.abort();
    let _ = server.await;
    Ok(())
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> anyhow::Result<()> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
    Ok(())
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> anyhow::Result<()> {
    tokio::signal::ctrl_c().await?;
    Ok(())
}
