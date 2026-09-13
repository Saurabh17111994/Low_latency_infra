//! Lightweight telemetry for the execution service (T4.2).
//!
//! Provides a process-local metrics registry (atomic counters) and a `tracing` logging
//! initializer built on the already-pinned `tracing` + `tracing-subscriber` dependencies.
//!
//! The production reporting target is OpenObserve (`EXECUTOR` service) via OTLP. A full
//! `opentelemetry` exporter requires additional pinned crates and a running OpenObserve
//! OTLP endpoint, both outside the offline slice. The export seam is the [`TelemetrySink`]
//! trait with a [`NullSink`] default, so the metrics path is exercised without a network
//! dependency and a real OTLP sink can be attached later without touching call sites.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Once;

/// Process-local atomic metric counters.
#[derive(Debug, Default)]
pub struct Metrics {
    /// Successful order submissions enqueued toward the bridge.
    pub order_submitted: AtomicU64,
    /// Order submissions/mods/cancels denied by the safety gate.
    pub order_denied: AtomicU64,
    /// Orders rejected by the bridge (or failed closed on an unknown outcome).
    pub order_rejected: AtomicU64,
    /// Number of times the gate was safety-halted.
    pub gate_safety_halt: AtomicU64,
    /// Asynchronous bridge reports drained (fills, cancels).
    pub report_received: AtomicU64,
    /// Bridge jobs abandoned at shutdown without reaching the broker.
    pub unresolved_attempt: AtomicU64,
    /// Explicit retransmissions of a bridge command beyond its first attempt (transport retries).
    pub bridge_transport_retries: AtomicU64,
    /// Process restarts observed (incremented on boot).
    pub restart: AtomicU64,
}

impl Metrics {
    /// Raised on a clean boot.
    pub fn record_restart(&self) {
        self.restart.fetch_add(1, Ordering::Relaxed);
    }

    /// An approximate point-in-time snapshot of every counter: each counter is read with a
    /// single `Relaxed` load (individually consistent), but the eight loads happen in sequence,
    /// so a concurrent `fetch_add` can land between two of them and the returned values need not
    /// correspond to any single instant. Benign for monotonic OTLP counters, which only ever
    /// grow and are never compared against each other.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            order_submitted: self.order_submitted.load(Ordering::Relaxed),
            order_denied: self.order_denied.load(Ordering::Relaxed),
            order_rejected: self.order_rejected.load(Ordering::Relaxed),
            gate_safety_halt: self.gate_safety_halt.load(Ordering::Relaxed),
            report_received: self.report_received.load(Ordering::Relaxed),
            unresolved_attempt: self.unresolved_attempt.load(Ordering::Relaxed),
            bridge_transport_retries: self.bridge_transport_retries.load(Ordering::Relaxed),
            restart: self.restart.load(Ordering::Relaxed),
        }
    }
}

/// Copy of the counters at a point in time, safe for export.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub order_submitted: u64,
    pub order_denied: u64,
    pub order_rejected: u64,
    pub gate_safety_halt: u64,
    pub report_received: u64,
    pub unresolved_attempt: u64,
    pub bridge_transport_retries: u64,
    pub restart: u64,
}

/// The process-global telemetry registry.
pub static METRICS: Metrics = Metrics {
    order_submitted: AtomicU64::new(0),
    order_denied: AtomicU64::new(0),
    order_rejected: AtomicU64::new(0),
    gate_safety_halt: AtomicU64::new(0),
    report_received: AtomicU64::new(0),
    unresolved_attempt: AtomicU64::new(0),
    bridge_transport_retries: AtomicU64::new(0),
    restart: AtomicU64::new(0),
};

/// Export seam for a telemetry backend. The default is a no-op; a real OTLP/OpenObserve sink
/// can be attached to record each snapshot.
pub trait TelemetrySink: std::fmt::Debug {
    /// Exports one snapshot to the backend.
    fn export(&self, snapshot: &MetricsSnapshot);
}

/// No-op sink used until a network-backed exporter is configured.
#[derive(Debug, Default, Clone, Copy)]
pub struct NullSink;

impl TelemetrySink for NullSink {
    fn export(&self, _snapshot: &MetricsSnapshot) {}
}

static TRACING_INIT: Once = Once::new();

/// Initializes the `tracing` subscriber from `RUST_LOG` (falling back to `filter`, defaulting
/// to `info`). Idempotent, safe to call multiple times, and deliberately infallible.
///
/// Not installing a subscriber is a normal outcome, not an error: another component may
/// already own the process-global logger. In this service Nautilus's kernel registers it
/// first and our subscriber must therefore come second (see the boot comment in `main`),
/// so `try_init` reports "already set" on the ordinary boot path. That outcome is neither
/// actionable nor distinguishable here, which is why there is no `Result` — callers must
/// not treat logging bootstrap as fallible.
pub fn init_logging(filter: &str) {
    TRACING_INIT.call_once(|| {
        let fallback = if filter.trim().is_empty() {
            "info".to_string()
        } else {
            filter.trim().to_string()
        };
        let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(fallback));
        // `try_init` (not `init`): if a global subscriber/logger is already installed in this
        // process (a concurrent or earlier initializer), `init()` would panic. A logging
        // initializer must be a harmless no-op in that case, not a panic — this keeps the
        // documented "idempotent and safe to call multiple times" contract under concurrency.
        let _ = tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .with_target(false)
            .try_init();
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_start_at_zero_and_increment() {
        let m = Metrics::default();
        assert_eq!(m.snapshot(), MetricsSnapshot::default());
        m.order_submitted.fetch_add(3, Ordering::Relaxed);
        m.gate_safety_halt.fetch_add(1, Ordering::Relaxed);
        let s = m.snapshot();
        assert_eq!(s.order_submitted, 3);
        assert_eq!(s.gate_safety_halt, 1);
    }

    #[test]
    fn null_sink_is_a_noop() {
        let sink = NullSink;
        let m = Metrics::default();
        sink.export(&m.snapshot());
    }

    #[test]
    fn init_logging_is_idempotent() {
        // No assertion to make: the contract is "must not panic", also when called twice or
        // when another component already owns the global logger (`try_init`, not `init`).
        init_logging("info");
        init_logging("debug");
    }
    #[test]
    fn obs_monotonic_counters_never_decrease() {
        // OBS correctness: counters are cumulative and monotone across snapshots —
        // an exported snapshot can never show a count that later decreases.
        let m = Metrics::default();
        let s0 = m.snapshot();
        m.order_submitted.fetch_add(5, Ordering::Relaxed);
        m.order_rejected.fetch_add(2, Ordering::Relaxed);
        m.gate_safety_halt.fetch_add(1, Ordering::Relaxed);
        let s1 = m.snapshot();
        m.order_submitted.fetch_add(1, Ordering::Relaxed);
        let s2 = m.snapshot();
        // Monotonicity per counter.
        assert!(s1.order_submitted >= s0.order_submitted);
        assert!(s1.order_rejected >= s0.order_rejected);
        assert!(s2.order_submitted >= s1.order_submitted);
        // And the cumulative total only grows (no counter ever decreases).
        let total = |s: &MetricsSnapshot| {
            s.order_submitted
                + s.order_denied
                + s.order_rejected
                + s.gate_safety_halt
                + s.report_received
                + s.unresolved_attempt
                + s.restart
        };
        assert!(total(&s1) >= total(&s0));
        assert!(total(&s2) >= total(&s1));
    }
}
