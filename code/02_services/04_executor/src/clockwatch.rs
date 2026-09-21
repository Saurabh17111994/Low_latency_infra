//! Clock-drift safety enforcement (plan Task B8 / CHG-064).
//!
//! `CLOCK_OFFSET_LIMIT_MS` was declared in compose for ingestion (enforced there by
//! `NtpClockChecker`) but nothing on the execution side read or enforced it. This module
//! closes that gap: a [`DriftMonitor`] samples the measured host-clock offset against UTC
//! from an injectable [`OffsetSource`] and enforces fail-closed semantics on the [`Gate`]:
//!
//! - `|offset| > limit`  -> `Beyond` -> `gate.safety_halt()` (approvals cleared, HALTED;
//!   re-enable ONLY through the sanctioned reconcile -> approval -> enable path).
//! - probe failure       -> `Unmeasurable` -> same fail-closed halt (never trust silence).
//! - `|offset| <= limit` -> `Within` (no action; halting is never automatic on recovery).
//!
//! The offline slice uses [`FixedOffsetSource`]; [`ChronycOffsetSource`] reads the live host
//! clock through `chronyc tracking` behind the same trait — identical to how the durable stores
//! are swapped behind `AttemptStore`/`GateStateStore`. `main.rs` selects it with
//! `CLOCK_OFFSET_SOURCE=chronyc`; the default stays the fixed source so a dev box without a
//! disciplined clock keeps working, and a *selected but unreadable* chrony fails closed
//! (`Unmeasurable` -> halt) rather than silently reporting zero (CHG-272).
//!
//! The container cannot run `chronyc` against the host's socket: the Debian package owns
//! `/run/chrony` as `0700 _chrony:_chrony` (measured 2026-09-21 — a `nobody` process is denied even
//! with the socket's group added), and the socket is unauthenticated, so whoever can read it can
//! also move the clock the gate is protecting. Production therefore uses
//! [`ClockFileOffsetSource`]: the node publishes the same number to a file (CHG-288), the deck
//! mounts that directory read-only, and `CLOCK_OFFSET_SOURCE=clockfile` selects it.

use anyhow::Result;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::gate::Gate;

/// Where measured clock offsets come from (production: NTP/chrony; tests: fixed).
pub trait OffsetSource {
    /// Measured offset of the local clock vs the UTC reference, in milliseconds.
    /// Positive = local clock ahead. An `Err` means "cannot measure" (fail-closed).
    fn sample_offset_ms(&mut self) -> Result<i64>;
}

/// Deterministic source for offline proofs and unit tests.
#[derive(Debug, Clone)]
pub struct FixedOffsetSource(pub i64);

impl OffsetSource for FixedOffsetSource {
    fn sample_offset_ms(&mut self) -> Result<i64> {
        Ok(self.0)
    }
}

/// Production offset source: the host's own chrony estimate, read from `chronyc tracking`.
///
/// The offset is field 4 of the `System time` line — the same field `vm-bootstrap.sh` (S4) and
/// `prod_node_check.py` (D1.2) read, so the three checks cannot disagree about what the clock
/// says. One process per sample is the cost; the monitor samples on a period, not per order.
///
/// Every failure — chronyc absent, non-zero exit, unparseable line — is an `Err`, so the monitor
/// classifies it `Unmeasurable` and halts. Silence is never read as a disciplined clock.
#[derive(Debug, Clone)]
pub struct ChronycOffsetSource {
    program: String,
}

impl Default for ChronycOffsetSource {
    fn default() -> Self {
        Self::new()
    }
}

impl ChronycOffsetSource {
    pub fn new() -> Self {
        Self {
            program: "chronyc".to_string(),
        }
    }

    /// Only for tests: point at a stub instead of a `chronyc` on PATH.
    #[cfg(test)]
    fn with_program(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
        }
    }

    /// `chronyc tracking` prints e.g.
    /// `System time     : 0.000123456 seconds fast of NTP time`; field 4 is the seconds offset,
    /// positive meaning the local clock is ahead. Milliseconds, rounded.
    fn parse_tracking(stdout: &str) -> Option<i64> {
        let line = stdout.lines().find(|l| l.starts_with("System time"))?;
        let seconds: f64 = line.split_whitespace().nth(3)?.parse().ok()?;
        Some((seconds * 1000.0).round() as i64)
    }
}

impl OffsetSource for ChronycOffsetSource {
    fn sample_offset_ms(&mut self) -> Result<i64> {
        let out = std::process::Command::new(&self.program)
            .arg("tracking")
            .output()?;
        if !out.status.success() {
            anyhow::bail!("`{} tracking` exited with {}", self.program, out.status);
        }
        let stdout = String::from_utf8_lossy(&out.stdout);
        Self::parse_tracking(&stdout).ok_or_else(|| {
            anyhow::anyhow!(
                "no readable 'System time' offset in `{} tracking` output",
                self.program
            )
        })
    }
}

/// Production offset source: the offset the node publishes, read from a file (CHG-288).
///
/// The producer is `04_scripts/clock_offset_fact.sh`, run every 10 s by
/// `arrow-clock-offset.timer`: it reads field 4 of `chronyc tracking`'s `System time` line — the
/// same field `vm-bootstrap.sh --check` and `prod_node_check.py` read — and publishes
/// `offset_ms`, `measured_epoch_s` and the source line. The deck mounts the *directory* read-only
/// (a file bind-mount would pin the inode, so the reader would keep seeing the first sample).
///
/// Every failure is an `Err`, so the monitor classifies it `Unmeasurable` and halts: an absent
/// file, a line that will not parse, and a sample older than `max_age_s`. A stale sample describes
/// a clock nobody is watching, and that is indistinguishable from a clock nobody is watching
/// correctly. Silence is never read as a disciplined clock.
#[derive(Debug, Clone)]
pub struct ClockFileOffsetSource {
    path: PathBuf,
    max_age_s: i64,
}

impl ClockFileOffsetSource {
    /// Where `arrow-clock-offset.timer` publishes on every node.
    pub const DEFAULT_PATH: &'static str = "/run/arrow-clock/offset";

    /// Three producer intervals: a sample survives one missed timer tick, not ten.
    pub const DEFAULT_MAX_AGE_S: i64 = 30;

    pub fn new(path: impl Into<PathBuf>, max_age_s: i64) -> Self {
        Self {
            path: path.into(),
            max_age_s,
        }
    }

    /// From `CLOCK_OFFSET_FILE` / `CLOCK_OFFSET_MAX_AGE_S`, with the documented defaults.
    pub fn from_env() -> Self {
        let path =
            std::env::var("CLOCK_OFFSET_FILE").unwrap_or_else(|_| Self::DEFAULT_PATH.to_string());
        let max_age_s = std::env::var("CLOCK_OFFSET_MAX_AGE_S")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(Self::DEFAULT_MAX_AGE_S);
        Self::new(path, max_age_s)
    }

    /// Pure form: parse the published fact and judge its age against `now_epoch_s`.
    ///
    /// A small negative age is an NTP step-back across a sample (`now` earlier than the sample by
    /// less than the limit). A large one is not rounding: it means the two timestamps disagree
    /// about the time, which is exactly what this gate exists to notice.
    fn parse_fact(text: &str, now_epoch_s: i64, max_age_s: i64) -> Option<i64> {
        let mut offset_ms = None;
        let mut measured_at = None;
        for line in text.lines() {
            match line.split_once('=') {
                Some(("offset_ms", value)) => offset_ms = value.trim().parse::<i64>().ok(),
                Some(("measured_epoch_s", value)) => measured_at = value.trim().parse::<i64>().ok(),
                _ => {}
            }
        }
        let age_s = now_epoch_s - measured_at?;
        if age_s > max_age_s || age_s < -max_age_s {
            return None;
        }
        offset_ms
    }
}

impl OffsetSource for ClockFileOffsetSource {
    fn sample_offset_ms(&mut self) -> Result<i64> {
        let text = std::fs::read_to_string(&self.path)
            .map_err(|e| anyhow::anyhow!("cannot read {}: {e}", self.path.display()))?;
        let now_epoch_s = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| anyhow::anyhow!("host clock is before the epoch: {e}"))?
            .as_secs() as i64;
        Self::parse_fact(&text, now_epoch_s, self.max_age_s).ok_or_else(|| {
            anyhow::anyhow!(
                "no fresh offset_ms in {} (max age {} s)",
                self.path.display(),
                self.max_age_s
            )
        })
    }
}

/// A probe that always fails — models NTP outage / unreachable reference.
#[derive(Debug, Default, Clone)]
pub struct FailingSource;

impl OffsetSource for FailingSource {
    fn sample_offset_ms(&mut self) -> Result<i64> {
        Err(anyhow::anyhow!("time reference unreachable"))
    }
}

/// Terminal classification of one drift sample.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriftStatus {
    Within(i64),
    Beyond(i64),
    Unmeasurable(String),
}

/// Fail-closed drift monitor bound to a configurable offset limit.
pub struct DriftMonitor {
    limit_ms: i64,
    source: Box<dyn OffsetSource>,
}

impl DriftMonitor {
    pub fn new(limit_ms: i64, source: Box<dyn OffsetSource>) -> Self {
        Self { limit_ms, source }
    }

    pub fn limit_ms(&self) -> i64 {
        self.limit_ms
    }

    /// Samples once and classifies against the limit (symmetric in sign).
    pub fn check(&mut self) -> DriftStatus {
        match self.source.sample_offset_ms() {
            Ok(offset) if offset.unsigned_abs() > self.limit_ms as u64 => {
                DriftStatus::Beyond(offset)
            }
            Ok(offset) => DriftStatus::Within(offset),
            Err(e) => DriftStatus::Unmeasurable(e.to_string()),
        }
    }

    /// Samples and ENFORCES on the gate: `Beyond`/`Unmeasurable` trigger
    /// `safety_halt()` (idempotent — repeated breaches never inflate the count).
    /// Recovery from a drift halt is NEVER automatic: only the sanctioned human path.
    pub fn enforce(&mut self, gate: &mut Gate) -> DriftStatus {
        let status = self.check();
        match &status {
            DriftStatus::Within(offset) => {
                tracing::debug!(
                    offset_ms = offset,
                    limit_ms = self.limit_ms,
                    "clock drift within limit"
                );
            }
            DriftStatus::Beyond(offset) => {
                tracing::error!(
                    offset_ms = offset,
                    limit_ms = self.limit_ms,
                    "CLOCK DRIFT BEYOND LIMIT — safety halt; orders refused until re-enabled via sanctioned path"
                );
                gate.safety_halt();
            }
            DriftStatus::Unmeasurable(err) => {
                tracing::error!(
                    error = %err,
                    limit_ms = self.limit_ms,
                    "CLOCK DRIFT UNMEASURABLE — failing closed via safety halt"
                );
                gate.safety_halt();
            }
        }
        status
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gate::ExecState;

    fn monitor(offset: i64) -> DriftMonitor {
        DriftMonitor::new(200, Box::new(FixedOffsetSource(offset)))
    }

    #[test]
    fn boundary_just_inside_passes_just_outside_halts() {
        // Exactly at the limit is WITHIN (strict > comparison).
        assert_eq!(monitor(200).check(), DriftStatus::Within(200));
        assert_eq!(monitor(-200).check(), DriftStatus::Within(-200));
        // One millisecond beyond, either direction, is BEYOND.
        assert_eq!(monitor(201).check(), DriftStatus::Beyond(201));
        assert_eq!(monitor(-201).check(), DriftStatus::Beyond(-201));
    }

    #[test]
    fn unmeasurable_probe_fails_closed() {
        let mut m = DriftMonitor::new(200, Box::new(FailingSource));
        assert!(matches!(m.check(), DriftStatus::Unmeasurable(_)));
    }

    #[test]
    fn p3_190_i64_min_offset_is_beyond_not_panic() {
        // P3-190: `offset.abs()` overflows on `i64::MIN` (panics in debug,
        // wraps fail-open to Within in release). `i64::MIN` is so far beyond
        // any sane limit that it must classify as Beyond - fail closed.
        let mut m = DriftMonitor::new(200, Box::new(FixedOffsetSource(i64::MIN)));
        assert_eq!(m.check(), DriftStatus::Beyond(i64::MIN));
    }

    #[test]
    fn beyond_limit_halts_gate_and_clears_approvals() {
        let mut g = Gate::new();
        g.add_authorized("saurabh");
        g.set_epoch(5).unwrap();
        // Drive the sanctioned path to ENABLED.
        g.transition(ExecState::Reconciling).unwrap();
        g.transition(ExecState::ApprovalPending).unwrap();
        g.record_approval("saurabh", "evidence-1").unwrap();
        g.record_approval("saurabh", "evidence-2").unwrap();
        g.enable(g.epoch()).unwrap();
        assert_eq!(g.state(), ExecState::Enabled);

        let mut m = monitor(500);
        let status = m.enforce(&mut g);
        assert_eq!(status, DriftStatus::Beyond(500));
        assert_eq!(g.state(), ExecState::Halted);
        assert!(!g.can_execute());
    }

    #[test]
    fn repeated_breaches_are_idempotent_no_halt_count_inflation() {
        let mut g = Gate::new();
        let mut m = monitor(999);
        m.enforce(&mut g);
        let after_first = g.safety_halt_count();
        m.enforce(&mut g);
        m.enforce(&mut g);
        assert_eq!(
            g.safety_halt_count(),
            after_first,
            "already halted stays halted"
        );
    }

    #[test]
    fn within_limit_never_touches_the_gate() {
        let mut g = Gate::new();
        g.transition(ExecState::Reconciling).unwrap();
        let mut m = monitor(42);
        assert_eq!(m.enforce(&mut g), DriftStatus::Within(42));
        assert_eq!(
            g.state(),
            ExecState::Reconciling,
            "healthy drift must not halt"
        );
        assert_eq!(g.safety_halt_count(), 0);
    }

    #[test]
    fn drift_halt_recovers_only_via_sanctioned_path() {
        let mut g = Gate::new();
        g.add_authorized("saurabh");
        g.set_epoch(5).unwrap();
        let mut m = monitor(-1000); // large negative drift
        m.enforce(&mut g);
        assert_eq!(g.state(), ExecState::Halted);

        // Direct transition back to ENABLED is forbidden (INVARIANT-003).
        assert!(g.transition(ExecState::Enabled).is_err());
        // Even drift returning to normal must NOT auto-recover.
        let mut healthy = monitor(1);
        healthy.enforce(&mut g);
        assert_eq!(g.state(), ExecState::Halted, "no automatic recovery");
        // Only the sanctioned human path recovers: the drift halt cleared the declared term
        // (P3-450), so the term must be re-declared for the new session before enable.
        g.transition(ExecState::Reconciling).unwrap();
        g.transition(ExecState::ApprovalPending).unwrap();
        g.set_epoch(5).unwrap();
        g.record_approval("saurabh", "drift-resolved-evidence")
            .unwrap();
        g.record_approval("saurabh", "drift-resolved-evidence-2")
            .unwrap();
        g.enable(5).unwrap();
        assert_eq!(g.state(), ExecState::Enabled);
    }

    #[test]
    fn unmeasurable_enforcement_halts_too() {
        let mut g = Gate::new();
        let mut m = DriftMonitor::new(200, Box::new(FailingSource));
        let status = m.enforce(&mut g);
        assert!(matches!(status, DriftStatus::Unmeasurable(_)));
        assert_eq!(g.state(), ExecState::Halted);
    }

    // ------------------------------------------------ CHG-272: the live chrony source

    /// A stub `chronyc` on disk: the spawn-and-parse path is exercised for real, without a
    /// chrony daemon (there is none on a dev box, and a container cannot read the host's socket
    /// without the packaging work recorded in CHG-272).
    fn stub_chronyc(name: &str, body: &str) -> String {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("clockwatch-{}-{}", std::process::id(), name));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("chronyc");
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn parses_the_field_the_host_scripts_read() {
        let out = "Reference ID    : 1B2C3D4E (ntp.example)\n\
                   System time     : 0.350000000 seconds fast of NTP time\n\
                   Leap status     : Normal\n";
        assert_eq!(ChronycOffsetSource::parse_tracking(out), Some(350));
        // negative = local clock behind, and the sign must survive
        assert_eq!(
            ChronycOffsetSource::parse_tracking(
                "System time     : -0.045678901 seconds slow of NTP time\n"
            ),
            Some(-46)
        );
        // anything else is "cannot measure", never a zero
        assert_eq!(
            ChronycOffsetSource::parse_tracking("Leap status     : Normal\n"),
            None
        );
        assert_eq!(
            ChronycOffsetSource::parse_tracking("System time     : unreadable\n"),
            None
        );
    }

    #[test]
    fn the_live_source_measures_a_real_process() {
        let program = stub_chronyc(
            "ok",
            "echo 'System time     : 0.350000000 seconds fast of NTP time'",
        );
        let mut src = ChronycOffsetSource::with_program(program);
        assert_eq!(src.sample_offset_ms().unwrap(), 350);
    }

    #[test]
    fn a_failing_chronyc_is_unmeasurable_and_halts() {
        let program = stub_chronyc("fail", "exit 1");
        let mut src = ChronycOffsetSource::with_program(program);
        assert!(src.sample_offset_ms().is_err());
        let mut g = Gate::new();
        let mut m = DriftMonitor::new(200, Box::new(src));
        assert!(matches!(m.enforce(&mut g), DriftStatus::Unmeasurable(_)));
        assert_eq!(g.state(), ExecState::Halted, "silence must fail closed");
    }

    #[test]
    fn an_unreadable_output_is_unmeasurable_not_zero() {
        let program = stub_chronyc("garbage", "echo 'Leap status     : Normal'");
        let mut src = ChronycOffsetSource::with_program(program);
        assert!(src.sample_offset_ms().is_err());
    }

    #[test]
    fn the_live_source_drives_the_existing_enforcement() {
        let program = stub_chronyc(
            "drift",
            "echo 'System time     : 1.500000000 seconds fast of NTP time'",
        );
        let mut g = Gate::new();
        let mut m = DriftMonitor::new(200, Box::new(ChronycOffsetSource::with_program(program)));
        assert_eq!(m.enforce(&mut g), DriftStatus::Beyond(1500));
        assert_eq!(g.state(), ExecState::Halted);
    }

    // ------------------------------------------------ CHG-288: the node-published fact

    /// Write a fact file the way `clock_offset_fact.sh` does.
    fn fact_file(name: &str, body: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("clockfact-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("offset");
        std::fs::write(&path, body).unwrap();
        path
    }

    fn now_epoch_s() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    #[test]
    fn parses_the_published_fact_and_rejects_stale_or_broken_ones() {
        let now = 1_700_000_000;
        let published =
            "offset_ms=-12\nmeasured_epoch_s=1700000000\nsource=chronyc-tracking-field4\n";
        assert_eq!(
            ClockFileOffsetSource::parse_fact(published, now, 30),
            Some(-12)
        );

        // at the limit is still readable; one second past it is not
        let at_limit = format!("offset_ms=250\nmeasured_epoch_s={}\n", now - 30);
        assert_eq!(
            ClockFileOffsetSource::parse_fact(&at_limit, now, 30),
            Some(250)
        );
        let stale = format!("offset_ms=250\nmeasured_epoch_s={}\n", now - 31);
        assert_eq!(ClockFileOffsetSource::parse_fact(&stale, now, 30), None);

        // a small step-back is a step-back; a large one is two clocks disagreeing
        let stepped = format!("offset_ms=1\nmeasured_epoch_s={}\n", now + 5);
        assert_eq!(
            ClockFileOffsetSource::parse_fact(&stepped, now, 30),
            Some(1)
        );
        let backwards = format!("offset_ms=1\nmeasured_epoch_s={}\n", now + 31);
        assert_eq!(ClockFileOffsetSource::parse_fact(&backwards, now, 30), None);

        // a zero with no sample time, or an unreadable number, is never "clock is fine"
        assert_eq!(
            ClockFileOffsetSource::parse_fact("offset_ms=0\n", now, 30),
            None
        );
        assert_eq!(
            ClockFileOffsetSource::parse_fact(
                "offset_ms=soon\nmeasured_epoch_s=1700000000\n",
                now,
                30
            ),
            None
        );
        assert_eq!(
            ClockFileOffsetSource::parse_fact("measured_epoch_s=1700000000\nsource=x\n", now, 30),
            None
        );
    }

    #[test]
    fn the_fact_source_reads_a_file_and_fails_closed_without_one() {
        let fresh = fact_file(
            "ok",
            &format!(
                "offset_ms=-46\nmeasured_epoch_s={}\nsource=chronyc-tracking-field4\n",
                now_epoch_s()
            ),
        );
        let mut source = ClockFileOffsetSource::new(&fresh, 30);
        assert_eq!(source.sample_offset_ms().unwrap(), -46);

        // an absent file is "cannot measure" — never a zero, and never a pass
        let mut missing = ClockFileOffsetSource::new(fresh.with_file_name("absent"), 30);
        assert!(missing.sample_offset_ms().is_err());
        let mut gate = Gate::new();
        let mut monitor = DriftMonitor::new(200, Box::new(missing));
        assert!(matches!(
            monitor.enforce(&mut gate),
            DriftStatus::Unmeasurable(_)
        ));
        assert_eq!(
            gate.state(),
            ExecState::Halted,
            "an absent fact must fail closed"
        );

        // a file nobody refreshed is stale, and staleness is also a halt
        let stale = fact_file(
            "stale",
            &format!("offset_ms=5\nmeasured_epoch_s={}\n", now_epoch_s() - 3600),
        );
        let mut stale_source = ClockFileOffsetSource::new(stale, 30);
        assert!(stale_source.sample_offset_ms().is_err());
        let mut gate = Gate::new();
        let mut monitor = DriftMonitor::new(200, Box::new(stale_source));
        assert!(matches!(
            monitor.enforce(&mut gate),
            DriftStatus::Unmeasurable(_)
        ));
        assert_eq!(
            gate.state(),
            ExecState::Halted,
            "a stale fact must fail closed"
        );
    }

    #[test]
    fn the_fact_source_drives_the_existing_enforcement() {
        let drifting = fact_file(
            "drift",
            &format!(
                "offset_ms=1500\nmeasured_epoch_s={}\nsource=chronyc-tracking-field4\n",
                now_epoch_s()
            ),
        );
        let mut gate = Gate::new();
        let mut monitor =
            DriftMonitor::new(200, Box::new(ClockFileOffsetSource::new(drifting, 30)));
        assert_eq!(monitor.enforce(&mut gate), DriftStatus::Beyond(1500));
        assert_eq!(gate.state(), ExecState::Halted);
    }
}
