//! LiveNodeRuntime long-run soak — plan Task B1
//! (docs/plans/2026-08-25-live-readiness-unified-plan.md, Phase B).
//!
//! Drives the hosted run loop against the deterministic FakeBridge for a sustained
//! interval and asserts the fail-closed contract holds for the whole soak:
//!
//! 1. The gate boots `HALTED` and stays so (no broker commands dispatched).
//! 2. The loop stays live and responsive for the full duration (sampled).
//! 3. A clean stop request through the shared [`LiveNodeHandle`] ends the loop `Ok`.
//! 4. The runner is consumed: a second run FAILS (fail-closed duplicate-run guard).
//! 5. No resource leak across the soak: process fd count returns to ~baseline and
//!    RSS growth stays bounded (runaway-allocation guard).
//!
//! Duration is env-tunable: `SOAK_SECS` (default 30 for dev validation; the evidence
//! run uses `SOAK_SECS=1800` per the task DoD ">= 30 min").
//!
//! Run:
//!
//! ```text
//! cargo test --offline --test live_node_soak                 # 30 s validation leg
//! SOAK_SECS=1800 cargo test --offline --test live_node_soak  # 30 min evidence leg
//! ```
//!
//! Single test function by design: nautilus registers a process-global logger, so this
//! file must build exactly one LiveNode (see engine.rs unit-test note).

use std::time::{Duration, Instant};

use nautilus_execution_service::gate::ExecState;

#[test]
fn live_node_runtime_sustained_soak() {
    // P3-024: a typo (`SOAK_SECS=abc`) used to be silently parsed away to the 30s default,
    // and 0 was accepted — a 0-second soak samples nothing and then passes the liveness
    // legs vacuously. Parse loudly and require a floor that can actually prove liveness.
    let soak_secs: u64 = match std::env::var("SOAK_SECS") {
        Ok(v) => v.parse().unwrap_or_else(|_| {
            panic!("SOAK_SECS must be a positive integer number of seconds, got {v:?}")
        }),
        Err(_) => 30,
    };
    assert!(
        soak_secs >= 5,
        "SOAK_SECS={soak_secs} is too short to prove liveness; use >= 5 (the default is 30)"
    );
    let soak = Duration::from_secs(soak_secs);

    // Resource baselines BEFORE building the node (Linux-only laptop/dev environment;
    // on non-Linux these probes degrade to skip, the functional assertions still hold).
    let fds_before = probe_fd_count();
    let rss_before_kb = probe_rss_kb();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime should build");

    let result = rt.block_on(async move {
        let mut node_rt = nautilus_execution_service::engine::LiveNodeRuntime::build()
            .expect("runtime should build");
        assert!(
            node_rt.gate_was_halted_at_boot(),
            "B1.1 fail-closed: gate must boot HALTED"
        );
        assert!(!node_rt.is_running(), "node must not run before start");

        let handle = node_rt.handle();
        // Owned boxed future (not pin!) so the &mut borrow can be released with an
        // explicit drop before the duplicate-run guard leg below.
        // P3-025: the boot flag is a construction-time snapshot, so a "stays HALTED" claim needs
    // the live gate. Taken as a clone before `run` borrows the runtime mutably.
    let gate_watch = node_rt.gate_watch();

    let mut run = Box::pin(node_rt.run_forever());

        // Sustained leg: keep the loop alive for the full soak, sampling liveness.
        let started = Instant::now();
        let mut samples = 0u64;
        // P3-224: `is_running()` is only meaningful once the hosted loop has marked itself
        // running. Asserting it from the very first 500ms tick made a slow or loaded runner
        // fail the soak spuriously, so ticks are skipped until the loop is first observed
        // running — with a bounded grace, because a loop that never starts must still fail.
        let mut started_running = false;
        let stop_after = tokio::time::sleep(soak);
        tokio::pin!(stop_after);
        loop {
            tokio::select! {
                biased;
                _ = &mut stop_after => break,
                _ = tokio::time::sleep(Duration::from_millis(500)) => {
                    if !started_running {
                        if handle.is_running() {
                            started_running = true;
                        } else if started.elapsed() > Duration::from_secs(30) {
                            panic!(
                                "loop never became running within the 30s startup grace (soak requested {:?})",
                                soak
                            );
                        }
                        continue;
                    }
                    assert!(
                        handle.is_running(),
                        "loop went down before the stop request (after {:?})",
                        started.elapsed()
                    );
                    assert_eq!(
                        gate_watch.state(),
                        ExecState::Halted,
                        "gate left HALTED mid-soak (after {:?})",
                        started.elapsed()
                    );
                    samples += 1;
                }
                result = &mut run => panic!(
                    "loop ended on its own after {:?}: {result:?}",
                    started.elapsed()
                ),
            }
        }
        assert!(
            started_running,
            "loop never became running within {:?}; no liveness sample was taken for the {soak_secs}s soak",
            started.elapsed()
        );
        // P3-466: the floor was `soak_secs / 2`, which integer division turns into 0 for a
        // 1s soak, and it said nothing about the sampler's 500ms cadence. Half of the ideal
        // cadence (2 samples/s) is the honest floor for a starved-sampler verdict.
        let min_samples = soak_secs.saturating_mul(1000) / 500 / 2;
        assert!(
            samples >= min_samples,
            "liveness sampler starved (samples={samples} for {soak_secs}s, min={min_samples}) — event loop not responsive"
        );

        // Clean-stop leg. P3-026: the await is bounded — a loop that ignores `stop()` (or
        // wedges holding the runtime thread) must fail the soak, not hang CI for the whole
        // job, which matters most on the 30-minute evidence leg.
        handle.stop();
        let outcome = tokio::time::timeout(Duration::from_secs(30), &mut run)
            .await
            .expect("clean stop timed out after 30s — the run loop ignored the stop request");
        assert!(outcome.is_ok(), "clean stop must return Ok, got {outcome:?}");
        assert_eq!(
            gate_watch.state(),
            ExecState::Halted,
            "gate must stay HALTED for the whole soak (checked live, not from the boot snapshot)"
        );
        assert!(
            !handle.is_running(),
            "node must be stopped after the stop request"
        );
        drop(run); // release the &mut node_rt borrow held by the consumed runner

        // Duplicate-run guard leg: the runner was consumed; re-entry must fail closed.
        // P3-221: bounded too — a duplicate-run guard that blocks instead of returning an
        // error would hang the soak, and `.is_err()` below would then be satisfied by the
        // timeout rather than by the guard. Only the guard's own error may reach the assert.
        let second = tokio::time::timeout(Duration::from_secs(30), node_rt.run_forever())
            .await
            .expect("second run must return an error promptly, not hang");
        assert!(
            second.is_err(),
            "second run must fail (fail-closed duplicate-run guard)"
        );

        (fds_before, rss_before_kb)
    });

    // Leak legs (post-run, same process).
    let (fds_before, rss_before_kb) = result;
    if let (Some(before), Some(after)) = (fds_before, probe_fd_count()) {
        assert!(
            after <= before + 8,
            "fd leak suspected: {before} -> {after} open fds across the soak"
        );
    }
    if let (Some(before), Some(after)) = (rss_before_kb, probe_rss_kb()) {
        let delta_mb = after.saturating_sub(before) / 1024;
        assert!(
            delta_mb <= 128,
            "RSS growth {delta_mb} MB across a {soak_secs}s soak exceeds the runaway guard (before={before}kB after={after}kB)"
        );
        println!(
            "live_node_soak[SOAK_SECS={soak_secs}]: ok — samples_ok, clean_stop, dup_guard, fds_stable, rss_delta_mb={delta_mb}"
        );
    } else {
        println!("live_node_soak[SOAK_SECS={soak_secs}]: ok — functional legs green (resource probes unavailable)");
    }
}

/// Open-fd count for this process (Linux /proc); `None` when unavailable.
fn probe_fd_count() -> Option<u64> {
    std::fs::read_dir("/proc/self/fd")
        .ok()
        .map(|entries| entries.count() as u64)
}

/// Resident set size in kB from /proc/self/status (Linux); `None` when unavailable.
fn probe_rss_kb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|rest| rest.split_whitespace().next()?.parse().ok())
}
