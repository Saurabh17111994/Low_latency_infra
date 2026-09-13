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

/// Resolves the clock-drift check interval from `CLOCK_DRIFT_CHECK_INTERVAL_S` (P3-456).
/// Zero must never reach `tokio::time::interval` (it panics on a zero period), so a bad or
/// missing value falls back to the 30 s default. Pure + env-free so the parse is testable.
fn parse_drift_interval(raw: Option<&str>) -> Duration {
    Duration::from_secs(
        raw.and_then(|v| v.parse::<u64>().ok())
            .filter(|secs| *secs > 0)
            .unwrap_or(30),
    )
}

#[cfg(test)]
mod p3_456_drift_interval_tests {
    use super::*;

    #[test]
    fn zero_clock_drift_check_interval_is_rejected_before_tokio_interval() {
        // P3-456: `tokio::time::interval(Duration::ZERO)` panics; a 0-second config must
        // fall back to the default instead of aborting the service at boot.
        assert_eq!(
            parse_drift_interval(Some("0")),
            Duration::from_secs(30),
            "a 0-second interval must not reach tokio::time::interval"
        );
        assert_eq!(parse_drift_interval(Some("45")), Duration::from_secs(45));
        assert_eq!(parse_drift_interval(None), Duration::from_secs(30));
        assert_eq!(
            parse_drift_interval(Some("nonsense")),
            Duration::from_secs(30)
        );
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
    let mut server = tokio::spawn(http::serve(addr, state));

    // B8 clock-drift safety: the offline slice samples a fixed zero offset (no NTP on the
    // laptop dev box — a real NTP/chrony source is a Workstream-D/prod concern behind the
    // same OffsetSource trait, see clockwatch.rs). The monitor enforces CLOCK_OFFSET_LIMIT_MS
    // on the gate: |offset| beyond the limit (or an unmeasurable probe) fails closed to
    // HALTED; recovery is only ever the sanctioned reconcile -> approval -> enable path.
    let drift_interval = parse_drift_interval(
        std::env::var("CLOCK_DRIFT_CHECK_INTERVAL_S")
            .ok()
            .as_deref(),
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
            result = &mut server => {
                // P3-212: an HTTP task exit leaves the service with no health/readiness
                // surface; surface it as fatal instead of swallowing it.
                return Err(http_server_exit_error(result));
            }
        }
    }
    runtime.begin_shutdown();
    server.abort();
    let _ = server.await;
    Ok(())
}

/// Classifies an HTTP server task exit as the fatal error it is (P3-212). The old shutdown
/// path dropped this result with `let _ = server.await`, so a failed bind left the service
/// running with no health/readiness endpoints and no trace of the cause.
fn http_server_exit_error(
    result: Result<anyhow::Result<()>, tokio::task::JoinError>,
) -> anyhow::Error {
    match result {
        Ok(Ok(())) => anyhow::anyhow!("http server exited unexpectedly"),
        Ok(Err(e)) => anyhow::anyhow!("http server failed: {e:#}"),
        Err(join) => anyhow::anyhow!("http server task failed: {join:?}"),
    }
}

#[cfg(test)]
mod p3_212_http_server_exit_tests {
    use super::*;

    #[tokio::test]
    async fn failed_http_server_handle_is_surfaced_as_fatal_not_discarded() {
        // P3-212: a bind failure must reach the operator with its cause; the old
        // `let _ = server.await` dropped both the join outcome and the serve error.
        let failed = tokio::spawn(async {
            Err::<(), anyhow::Error>(anyhow::anyhow!(
                "bind health server 127.0.0.1:9: address in use"
            ))
        });
        let err = http_server_exit_error(failed.await);
        assert!(
            format!("{err:#}").contains("address in use"),
            "the joined server error must be surfaced with its cause, got: {err:#}"
        );

        let panicked = tokio::spawn(async { panic!("bind panicked") });
        let err = http_server_exit_error(panicked.await);
        assert!(
            format!("{err}").contains("http server task failed"),
            "a panicked server task must still be surfaced as fatal, got: {err}"
        );
    }
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
