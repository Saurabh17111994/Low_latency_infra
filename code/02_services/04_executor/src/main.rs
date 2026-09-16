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

/// How long the HTTP task is given to finish in-flight responses after the shutdown
/// signal, before the task is aborted so the process can exit on the drain path.
///
/// This is one term of the container's shutdown budget: the vendored kernel waits up to
/// `KERNEL_RESIDUAL_EVENT_WAIT_SECS` for residual events to drain first, then this window
/// elapses. The budget can be 15 s; compose's `stop_grace_period` on the `nautilus` service
/// must exceed it or Docker SIGKILLs the process before the D7 clean-shutdown sequence
/// finishes (measured live 2026-09-14: 10 s grace → exit 137, report lost; 90 s grace →
/// 16 s, exit 0). `shutdown_budget_grace_tests` pins that sum against the compose file.
const HTTP_DRAIN_GRACE_SECS: u64 = 5;

use nautilus_execution_service::{
    bootstrap::Runtime,
    bridge::{BridgeClient, CommandScript, FakeBridge, HttpBridgeClient},
    clockwatch::{DriftMonitor, FixedOffsetSource},
    config::ServiceConfig,
    durable::{DurableClients, DurableFlags},
    engine::{BridgeSelection, LiveNodeRuntime},
    gate::ExecState,
    http, shutdown, telemetry,
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

    // D1/D2: the durable flags select real stores. All OFF — the default — keeps the in-memory
    // clients, so this changes nothing unless an operator enables one. Captured here because
    // `config` moves into `Runtime::init`; the stores are opened once the logger exists (below) so
    // the outcome is reported rather than silently dropped.
    let durable_flags = DurableFlags {
        gate: config.durable_gate_enabled,
        attempts: config.durable_attempts_enabled,
        journal: config.durable_journal_enabled,
        audit: config.durable_audit_enabled,
    };
    let durable_dir = config.durable_dir.clone();
    let execution_partition_id = config.execution_partition_id.clone();

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

    // D1/D2: open the flag-selected durable stores before the health server starts, so a start that
    // cannot reach its durable store fails instead of serving without it — an enabled flag is never
    // a silent no-op. With the gate flag on, the partition's row is created HALTED and unbound when
    // absent and left untouched when it exists (D2: a restart must not clear a fenced gate).
    let durable_clients = if durable_flags.any_on() {
        let clients = DurableClients::open_for_service(
            std::path::Path::new(&durable_dir),
            durable_flags,
            execution_partition_id.as_deref(),
        )?;
        let gate_row = clients
            .gate_store
            .read(execution_partition_id.as_deref().unwrap_or_default());
        tracing::info!(
            dir = %durable_dir,
            gate = durable_flags.gate,
            attempts = durable_flags.attempts,
            gate_row = ?gate_row,
            "durable clients opened (file-backed stores selected by flag)"
        );
        Some(clients)
    } else {
        None
    };
    // The clients are kept for the life of the process: the handle the forward leg holds is a clone
    // of the same store, and its exclusive lock on the log lives inside it.
    let live_attempts = durable_clients
        .as_ref()
        .and_then(|clients| clients.live_attempts.clone());

    tracing::info!(
        "nautilus-execution-service boot: gate HALTED, bridge mode {bridge_mode}, LiveNode hosted run loop armed, health on {addr} (execution enabled: false)"
    );

    let state = runtime
        .server_state()
        .with_forwarder(route_forwarder)
        .with_gateway_endpoint(gateway_endpoint);
    // Workstream-D swap: with the attempts flag on, the live forward leg claims and records the
    // attempt before the bridge sees the order, and answers retries from the record. With the flag
    // off there is no guard here (the gateway's own dedup index is the only one) — unchanged.
    let state = match live_attempts {
        Some(attempts) => {
            tracing::info!("durable attempt guard armed on the /v1/intents forward leg");
            state.with_attempts(attempts)
        }
        None => state,
    };
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

    // The hosted run future must be pinned once and polled by `&mut`: `run_forever` consumes
    // the runner, so re-creating it on every drift tick would abort the service and cancel
    // in-flight node work each pass (P3-021). The stop handle is taken first because the
    // pinned future holds the mutable borrow of `node`.
    let node_handle = node.handle();
    let mut node_run = Box::pin(node.run_forever());

    // D3: the shutdown signals are registered **once**, before the loop, and the streams live for
    // the whole run. The pre-D3 shape called `signal(...)`/`ctrl_c()` inside the loop, so every
    // drift tick dropped the listeners and re-created them: a SIGTERM delivered in that window
    // reached no live listener and — because tokio owns the disposition once it has registered a
    // handler — was dropped rather than stopping the process. `recv()` is cancel-safe, so one
    // registration can serve every iteration.
    let mut shutdown_signals = install_shutdown_signals()?;
    let mut shutdown_fired = false;

    // Run until a shutdown signal or the node loop ends; the periodic drift check is a
    // non-terminal branch (a drift halt is enforced on the gate, the process keeps serving).
    loop {
        tokio::select! {
            // A second signal is deliberately not acted on: the process is already draining, and
            // the guard keeps this branch from re-entering (or re-polling a fired handler).
            () = await_shutdown_signal(&mut shutdown_signals), if !shutdown_fired => {
                shutdown_fired = true;
                tracing::info!("shutdown signal received; stopping LiveNode, draining (readyz -> 503)");
                // Keep the loop alive so the pinned run future is polled to its clean return
                // instead of being dropped mid-shutdown (P3-211).
                node_handle.stop();
            }
            result = &mut node_run => {
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
    // The pinned hosted-run future holds the mutable borrow of `node` and has returned above;
    // release it before reading the shutdown evidence that same run produced.
    drop(node_run);
    runtime.begin_shutdown();
    drain_http_server(&mut server, Duration::from_secs(HTTP_DRAIN_GRACE_SECS)).await;
    verify_clean_shutdown(&node)
}

/// D7: reads the shutdown evidence the client recorded on the node's stop path and asserts the
/// restart invariant here, on the production path.
///
/// The audited sequence (safety halt → abandon queued jobs as unresolved attempts → flush
/// reports) runs inside the node's `finalize_stop`, where the client is still reachable; this
/// function is what makes its absence or failure visible. `verify_restart_safe` used to be
/// test-only, so a production stop that left the execution fence armed — or that never ran the
/// sequence at all — exited 0 with no trace.
fn verify_clean_shutdown(node: &LiveNodeRuntime) -> anyhow::Result<()> {
    match node.shutdown_report() {
        Some(report) => tracing::info!(
            unresolved_attempts = report.unresolved_attempts,
            gate_state = ?report.gate_state,
            "clean shutdown complete: queued bridge jobs abandoned as unresolved attempts"
        ),
        None => tracing::error!(
            "no clean-shutdown report: the node stop path did not run the client sequence, so \
             queued jobs may have been dropped uncounted"
        ),
    }
    assert_restart_safe(node.gate_watch().state())
}

/// The restart invariant (`shutdown::verify_restart_safe`) enforced as a production stop check:
/// an exit that leaves the execution fence armed must fail loudly, because a restarted process
/// could then auto-retry an attempt this one abandoned.
fn assert_restart_safe(gate_state: ExecState) -> anyhow::Result<()> {
    if shutdown::verify_restart_safe(gate_state) {
        return Ok(());
    }
    anyhow::bail!(
        "shutdown left the execution fence armed ({gate_state:?}): a restarted process could \
         auto-retry an abandoned attempt"
    )
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
/// The process's shutdown-signal streams (D3), registered once by [`install_shutdown_signals`].
#[cfg(unix)]
struct ShutdownSignals {
    sigint: tokio::signal::unix::Signal,
    sigterm: tokio::signal::unix::Signal,
}

/// Non-unix has a single Ctrl-C handler; boxing it once gives the same "one registration for the
/// whole run" shape as the unix path.
#[cfg(not(unix))]
struct ShutdownSignals {
    ctrl_c: std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<()>> + Send>>,
}

/// Registers the shutdown signals **now**, not on first poll (D3).
///
/// `tokio::signal::unix::signal` installs the handler as it constructs the stream, so calling
/// this before the loop closes the window in which a signal would be swallowed. Registration
/// failures surface here, at startup, instead of being indistinguishable from a signal later.
#[cfg(unix)]
fn install_shutdown_signals() -> anyhow::Result<ShutdownSignals> {
    use tokio::signal::unix::{signal, SignalKind};
    Ok(ShutdownSignals {
        sigint: signal(SignalKind::interrupt())?,
        sigterm: signal(SignalKind::terminate())?,
    })
}

#[cfg(not(unix))]
fn install_shutdown_signals() -> anyhow::Result<ShutdownSignals> {
    Ok(ShutdownSignals {
        ctrl_c: Box::pin(tokio::signal::ctrl_c()),
    })
}

/// Resolves when SIGINT or SIGTERM arrives. Cancel-safe: the streams persist across loop
/// iterations, only this `recv()` is re-created, so no signal can fall between ticks (D3).
#[cfg(unix)]
async fn await_shutdown_signal(signals: &mut ShutdownSignals) {
    tokio::select! {
        _ = signals.sigint.recv() => {}
        _ = signals.sigterm.recv() => {}
    }
}

#[cfg(not(unix))]
async fn await_shutdown_signal(signals: &mut ShutdownSignals) {
    if let Err(e) = signals.ctrl_c.as_mut().await {
        // Cannot be signalled any more: stopping is the fail-closed reading of that.
        tracing::error!(error = ?e, "Ctrl-C handler failed; stopping the service");
    }
}

/// Bounded drain of the HTTP server task after `/readyz` flips to 503 (P3-211): in-flight
/// requests get up to `grace` to finish before the task is aborted and reaped. The old
/// shutdown aborted immediately, cancelling every in-flight request instead of draining.
async fn drain_http_server(
    server: &mut tokio::task::JoinHandle<anyhow::Result<()>>,
    grace: Duration,
) {
    match tokio::time::timeout(grace, &mut *server).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(e))) => {
            tracing::error!(error = ?e, "http server exited with an error while draining")
        }
        Ok(Err(join)) => {
            tracing::error!(error = ?join, "http server task failed while draining")
        }
        Err(_) => {
            tracing::warn!(
                grace_s = grace.as_secs(),
                "http server drain window elapsed; aborting the server task"
            );
            server.abort();
            if let Err(join) = server.await {
                tracing::debug!(error = ?join, "http server task reaped after abort");
            }
        }
    }
}

#[cfg(test)]
mod p3_211_http_server_drain_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn drain_waits_out_the_grace_window_then_aborts_a_hung_server() {
        // P3-211: the old shutdown aborted the server immediately, so in-flight work was
        // cancelled instead of being allowed to finish against the draining surface.
        let finished = Arc::new(AtomicBool::new(false));
        let flag = finished.clone();
        let mut completes_soon = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            flag.store(true, Ordering::SeqCst);
            Ok::<(), anyhow::Error>(())
        });
        drain_http_server(&mut completes_soon, Duration::from_secs(1)).await;
        assert!(
            finished.load(Ordering::SeqCst),
            "in-flight work must get the grace window instead of an immediate abort"
        );

        let mut hangs = tokio::spawn(async { std::future::pending::<anyhow::Result<()>>().await });
        let started = std::time::Instant::now();
        drain_http_server(&mut hangs, Duration::from_millis(40)).await;
        assert!(
            hangs.is_finished(),
            "a hung server must still be aborted and reaped"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the drain must be bounded by the grace window, elapsed {:?}",
            started.elapsed()
        );
    }
}

#[cfg(test)]
mod d7_restart_invariant_tests {
    use super::*;

    #[test]
    fn a_stop_that_left_the_fence_armed_fails_the_production_check() {
        // D7: `verify_restart_safe` was test-only, so a production exit with an armed execution
        // fence was silent. The stop path now refuses it (non-zero exit) instead.
        assert!(assert_restart_safe(ExecState::Halted).is_ok());
        let err = assert_restart_safe(ExecState::Enabled)
            .expect_err("an armed fence must fail the stop, not be reported as clean");
        assert!(
            err.to_string().contains("fence armed"),
            "the failure must name the invariant: {err}"
        );
    }
}

#[cfg(all(test, unix))]
mod d3_shutdown_signal_tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn a_signal_delivered_while_the_loop_was_busy_is_not_lost() {
        // D3: the streams are registered before the run loop and live for the whole run. This
        // delivers a real SIGTERM to this process while nothing is awaiting it (standing in for
        // the drift branch), then requires the already-registered stream to observe it. The
        // pre-D3 shape re-created the listeners every iteration, so at delivery time none
        // existed and — because tokio owns the disposition once it has registered a handler — the
        // signal was dropped instead of stopping the process. The test also could not have been
        // written against the old shape: with lazy registration the handler is installed on first
        // poll, so this `kill` would have terminated the test binary.
        let mut signals = install_shutdown_signals().expect("signal handlers install");
        let delivered = std::process::Command::new("kill")
            .arg("-TERM")
            .arg(std::process::id().to_string())
            .status()
            .expect("kill(1) runs");
        assert!(delivered.success(), "kill -TERM delivery failed");

        tokio::time::timeout(Duration::from_secs(5), await_shutdown_signal(&mut signals))
            .await
            .expect("a signal registered before the gap must still be observed");
    }
}

/// The container's stop grace period must outlast the executor's worst-case drain, or Docker
/// SIGKILLs the process mid-shutdown: the D7 clean-shutdown sequence never finishes, queued
/// attempts are abandoned uncounted, and the exit code is 137 instead of 0. That is not
/// hypothetical — the live test of 2026-09-14 measured both ends of it:
///
/// * `docker stop` with the compose default (10 s) → exit 137, no "clean shutdown complete"
///   line, `unresolved_attempts` never reported;
/// * `docker stop -t 90` → 15-16 s, exit 0, `unresolved_attempts=0 gate_state=Halted`.
///
/// So the grace period is load-bearing, exactly as it is for ingestion
/// (`ShutdownBudgetGraceTest`). This mirrors that test for the Rust service: it reads the
/// real compose file and our real constant, so raising a term without raising the grace
/// fails here instead of in production.
#[cfg(test)]
mod shutdown_budget_grace_tests {
    use super::HTTP_DRAIN_GRACE_SECS;

    /// The vendored nautilus kernel's residual-event wait, in seconds. Not a constant we own —
    /// it is fixed in the kernel and only observable in the log line "Awaiting residual events
    /// (10s)..." — but it is spent inside the same drain, so the grace period must cover it.
    /// It lives here rather than at module scope because the running binary never reads it.
    const KERNEL_RESIDUAL_EVENT_WAIT_SECS: u64 = 10;

    /// The compose file the running stack is created from.
    const COMPOSE: &str = include_str!("../../../01_platform/01_docker/docker-compose.yml");

    /// The service whose stop path runs the clean-shutdown sequence.
    const SERVICE: &str = "nautilus:";

    /// Headroom over the summed budget. The two terms describe the *longest* each phase may
    /// run, so a grace equal to their sum would still let a phase that overruns by a
    /// millisecond lose the report; 10 s also absorbs process start/exit overhead.
    const REQUIRED_HEADROOM_SECS: u64 = 10;

    /// The `stop_grace_period` a service declares, in seconds. Hand-parsed on purpose: the
    /// block is two levels of a YAML file we own, and pulling a YAML dependency into the
    /// executor's test build to read one key would cost more than it proves. Returns `None`
    /// when the service or the key is absent — both are failures the caller names.
    fn stop_grace_period_secs(service: &str) -> Option<u64> {
        let head = format!("\n  {service}\n");
        let after = COMPOSE.split_once(&head)?.1;
        // The block ends at the next two-space-indented key (the service's siblings).
        let block = after
            .lines()
            .take_while(|l| {
                !(l.starts_with("  ") && !l.starts_with("   ") && l.trim_end().ends_with(':'))
            })
            .collect::<Vec<_>>()
            .join("\n");
        block
            .lines()
            .find_map(|l| l.trim().strip_prefix("stop_grace_period:"))
            .and_then(|v| v.trim().strip_suffix('s'))
            .and_then(|v| v.trim().parse::<u64>().ok())
    }

    #[test]
    fn the_executor_declares_a_stop_grace_period() {
        assert!(
            stop_grace_period_secs(SERVICE).is_some(),
            "{SERVICE} declares no stop_grace_period, so Docker uses its 10 s default and \
             SIGKILLs the executor mid-drain (measured 2026-09-14: exit 137, no clean-shutdown \
             report). Declare it explicitly."
        );
    }

    #[test]
    fn the_stop_grace_period_covers_the_worst_case_drain() {
        let grace = stop_grace_period_secs(SERVICE)
            .expect("test the_executor_declares_a_stop_grace_period first");
        let budget = HTTP_DRAIN_GRACE_SECS + KERNEL_RESIDUAL_EVENT_WAIT_SECS;
        assert!(
            grace >= budget + REQUIRED_HEADROOM_SECS,
            "stop_grace_period {grace}s does not cover the {budget}s drain budget plus \
             {REQUIRED_HEADROOM_SECS}s headroom (HTTP_DRAIN_GRACE_SECS={HTTP_DRAIN_GRACE_SECS} + \
             KERNEL_RESIDUAL_EVENT_WAIT_SECS={KERNEL_RESIDUAL_EVENT_WAIT_SECS}). Raise the grace \
             period in the compose file or lower a budget — a stop that outruns the grace is a \
             SIGKILL, and the queued-attempt accounting is what gets lost."
        );
    }
}
