//! Dependency-call resilience: bounded retry + exponential backoff + circuit breaker,
//! plus an idempotency guard so a retry never re-issues an already-terminal call.
//!
//! Closes the spec's gap ("no retry/backoff/circuit code exists") for the executor's
//! outbound dependency calls (Flink/Fluss/observability/bridge). A **standalone component**,
//! unit-tested offline (RESILIENCE-002/003/004/005/007, RETRY-STORM containment 001), and
//! wired onto the live bridge transport via [`RetryOrchestrator::execute_async`]
//! (see `execution::BridgeExecutionClient::execute_job`).
//!
//! **Wiring boundary (evidence-based):** the live binary's only outbound dependency call is the
//! bridge (`send_command`). Fluss, S3, the durable Broker-KV (`AttemptStore`) and the
//! observability sink have **no in-process call site** in this component — they are separate
//! cluster components exercised at the M3 rig. Any future Rust client for them MUST be wrapped
//! with [`RetryOrchestrator::execute_async`] (async) or [`RetryOrchestrator::execute`] (sync):
//! classify only clearly-transient transport `Err`s as [`AttemptError::Transient`]; a broker /
//! store decision (including UNKNOWN / ambiguous) is terminal and must fail closed, never
//! auto-retry.
//!
//! Wall-clock reads are injected (`now` closures), so breaker cooldowns stay deterministically
//! testable, and [`Backoff`] remains a pure function verified on its own (RESILIENCE-002).
//!
//! **Backoff pacing is applied for real** (P3-023). The retry loop sleeps `next_delay_ms()`
//! between transient attempts: `std::thread::sleep` in [`RetryOrchestrator::execute`],
//! `tokio::time::sleep` in [`RetryOrchestrator::execute_async`]. Tests stay cheap because
//! they configure millisecond-scale `base_backoff_ms`/`cap_backoff_ms`; before this the loop
//! computed the delay and discarded it, so a "backed-off" retry was actually immediate.
//!
//! **The retry budget is per logical call** (P3-022), not per orchestrator lifetime. One
//! orchestrator is built per client and lives for the process, so a shared budget meant a
//! single exhausted call bricked every later call. Cross-call storm containment is the
//! circuit breaker's role — it is the stateful, shared guard.

use std::collections::{HashMap, VecDeque};
use std::future::Future;

/// Exponential backoff: `base_ms << attempt` per step, capped at `cap_ms`.
#[derive(Debug, Clone)]
pub struct Backoff {
    base_ms: u64,
    cap_ms: u64,
    attempt: u32,
}

impl Backoff {
    pub fn new(base_ms: u64, cap_ms: u64) -> Self {
        Self {
            base_ms,
            cap_ms: cap_ms.max(base_ms),
            attempt: 0,
        }
    }

    /// Delay (ms) for the current attempt, then advances. Attempt 0 == `base_ms`; the
    /// delay doubles each step and saturates at `cap_ms`.
    pub fn next_delay_ms(&mut self) -> u64 {
        let shift = self.attempt.min(16);
        let d = self.base_ms.saturating_mul(1u64 << shift);
        self.attempt = self.attempt.wrapping_add(1);
        d.min(self.cap_ms)
    }
}

/// Bounded retry budget: at most `max_attempts` total calls (RETRY-STORM containment).
#[derive(Debug, Clone)]
pub struct RetryBudget {
    max_attempts: u32,
    used: u32,
}

impl RetryBudget {
    pub fn new(max_attempts: u32) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
            used: 0,
        }
    }

    /// Records one attempt and returns whether it was within budget. `false` = exhausted.
    pub fn try_allow(&mut self) -> bool {
        if self.used >= self.max_attempts {
            false
        } else {
            self.used += 1;
            true
        }
    }

    pub fn exhausted(&self) -> bool {
        self.used >= self.max_attempts
    }

    pub fn used(&self) -> u32 {
        self.used
    }
}

/// Circuit breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    Closed,
    Open,
    HalfOpen,
}

/// Circuit breaker: closes on success; opens after `failure_threshold` consecutive
/// failures; after `cooldown_ms` an `allow_call` probe half-opens; one success closes
/// again (recovery after the dependency returns).
#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    failure_threshold: u32,
    failures: u32,
    state: BreakerState,
    cooldown_ms: u64,
    opened_at_ms: u64,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, cooldown_ms: u64) -> Self {
        Self {
            failure_threshold: failure_threshold.max(1),
            failures: 0,
            state: BreakerState::Closed,
            cooldown_ms,
            opened_at_ms: 0,
        }
    }

    pub fn state(&self) -> BreakerState {
        self.state
    }

    pub fn is_open(&self) -> bool {
        self.state == BreakerState::Open
    }

    /// May a call proceed at `now_ms`? An Open breaker allows exactly one probe once the
    /// cooldown has elapsed (transitioning to HalfOpen); otherwise it short-circuits.
    pub fn allow_call(&mut self, now_ms: u64) -> bool {
        match self.state {
            BreakerState::Closed | BreakerState::HalfOpen => true,
            BreakerState::Open => {
                if now_ms.saturating_sub(self.opened_at_ms) >= self.cooldown_ms {
                    self.state = BreakerState::HalfOpen; // probe allowed
                    true
                } else {
                    false
                }
            }
        }
    }

    pub fn record_success(&mut self) {
        self.failures = 0;
        if self.state == BreakerState::HalfOpen {
            self.state = BreakerState::Closed;
        }
    }

    pub fn record_failure(&mut self, now_ms: u64) {
        self.failures = self.failures.saturating_add(1);
        if self.state == BreakerState::HalfOpen || self.failures >= self.failure_threshold {
            self.state = BreakerState::Open;
            self.opened_at_ms = now_ms;
            self.failures = 0;
        }
    }
}

/// Outcome of a single dependency attempt: transient (retryable) vs terminal (never retry).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptError {
    Transient(String),
    Terminal(String),
}

/// Result returned by the orchestrator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryError {
    /// The key was already completed; the call was NOT re-invoked (RESILIENCE-007).
    Duplicate,
    /// Retry budget exhausted (RESILIENCE-003) — possibly after a breaker short-circuit.
    Exhausted { attempts: u32 },
    /// The breaker is open and cooldown wasn't elapsed (RESILIENCE-004).
    BreakerOpen,
    /// A terminal failure (no retry).
    Terminal,
}

/// Idempotency guard (RESILIENCE-007): once a key reaches a terminal/done outcome it is
/// memoized, and a duplicate logical call returns without re-invoking the dependency.
///
/// P3-216: the memo is bounded (FIFO). A long-lived executor sees arbitrarily many distinct
/// logical keys, so without a bound this map grows for the lifetime of the process. Eviction
/// is the weaker of the two outcomes — a re-admitted key re-invokes the dependency — which is
/// why [`IdempotencyGuard::MEMO_CAPACITY`] is sized to never evict anything that could still
/// be retried.
#[derive(Debug, Default)]
pub struct IdempotencyGuard {
    memo: HashMap<String, ()>,
    /// Insertion order, used only to know which memoized key is the oldest.
    order: VecDeque<String>,
}

impl IdempotencyGuard {
    /// Retained keys. Any key still inside the retry/ack window must survive here — evicting
    /// one early re-admits a duplicate and re-invokes the dependency, which is the failure
    /// this guard exists to prevent. The bound is therefore generous; it exists to stop
    /// unbounded growth over a long-lived process, not to trim aggressively.
    /// Tripwire: revisit if distinct logical keys per gate epoch can approach this.
    pub const MEMO_CAPACITY: usize = 4096;

    pub fn new() -> Self {
        Self::default()
    }

    pub fn done(&self, key: &str) -> bool {
        self.memo.contains_key(key)
    }

    /// Marks the key done, evicting the oldest keys beyond `MEMO_CAPACITY` (P3-216).
    pub fn mark(&mut self, key: &str) {
        if self.memo.insert(key.to_string(), ()).is_some() {
            return; // already known: keep its original place in the eviction order
        }
        self.order.push_back(key.to_string());
        while self.order.len() > Self::MEMO_CAPACITY {
            if let Some(oldest) = self.order.pop_front() {
                self.memo.remove(&oldest);
            }
        }
    }
}

/// Orchestrator tuning.
#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub base_backoff_ms: u64,
    pub cap_backoff_ms: u64,
    pub breaker_threshold: u32,
    pub breaker_cooldown_ms: u64,
}

/// Composes budget + backoff + breaker + idempotency guard for one logical call.
pub struct RetryOrchestrator {
    /// P3-022: a per-call bound, not a lifetime one — see the module doc.
    max_attempts: u32,
    breaker: CircuitBreaker,
    backoff: Backoff,
    guard: IdempotencyGuard,
}

impl RetryOrchestrator {
    pub fn new(cfg: RetryConfig) -> Self {
        Self {
            max_attempts: cfg.max_attempts,
            breaker: CircuitBreaker::new(cfg.breaker_threshold, cfg.breaker_cooldown_ms),
            backoff: Backoff::new(cfg.base_backoff_ms, cfg.cap_backoff_ms),
            guard: IdempotencyGuard::new(),
        }
    }

    pub fn breaker_state(&self) -> BreakerState {
        self.breaker.state()
    }

    /// Executes `attempt` for `key`, bounded by the retry budget and breaker. Transient
    /// failures are retried (backed off) until the budget is spent or the breaker opens;
    /// terminal failures return immediately; a completed key is never re-invoked.
    ///
    /// Returns `(value, attempts_used)` on success, else a [`RetryError`].
    pub fn execute<T, F>(
        &mut self,
        key: &str,
        mut now: impl FnMut() -> u64,
        mut attempt: F,
    ) -> Result<(T, u32), RetryError>
    where
        F: FnMut() -> Result<T, AttemptError>,
    {
        // Duplicate retry prevention: an already-handled logical call is not re-issued.
        if self.guard.done(key) {
            return Err(RetryError::Duplicate);
        }

        // P3-022: bound THIS call, not the orchestrator's lifetime.
        let mut budget = RetryBudget::new(self.max_attempts);
        let mut attempts: u32 = 0;
        loop {
            // Breaker before budget: a short-circuited call never reaches the dependency, so
            // it must not consume a budget unit either.
            if !self.breaker.allow_call(now()) {
                return Err(RetryError::BreakerOpen);
            }
            if !budget.try_allow() {
                return Err(RetryError::Exhausted { attempts });
            }
            attempts += 1;
            match attempt() {
                Ok(value) => {
                    self.breaker.record_success();
                    self.guard.mark(key);
                    return Ok((value, attempts));
                }
                Err(AttemptError::Terminal(_)) => {
                    self.guard.mark(key);
                    return Err(RetryError::Terminal);
                }
                Err(AttemptError::Transient(_)) => {
                    self.breaker.record_failure(now());
                    if budget.exhausted() || self.breaker.is_open() {
                        return Err(RetryError::Exhausted { attempts });
                    }
                    // P3-023: apply the backoff, don't just compute it.
                    let delay_ms = self.backoff.next_delay_ms();
                    std::thread::sleep(std::time::Duration::from_millis(delay_ms));
                }
            }
        }
    }

    /// Asynchronous variant for the live bridge/transport path. Semantics mirror
    /// [`Self::execute`] (bounded by the retry budget and circuit breaker, transient
    /// failures retried with backoff) but the attempt is `async`.
    ///
    /// Deliberately does **not** consult the in-memory idempotency guard: once-only /
    /// duplicate avoidance for the money-moving bridge call is owned by the durable
    /// executiongate + safety gate + bridge dedup. Only clearly-transient transport `Err`s
    /// must be mapped to [`AttemptError::Transient`] by the caller; a broker decision that
    /// yields an envelope (Success/Rejected/Unknown) is returned as `Ok` and is **never**
    /// re-issued — in particular an `Unknown` outcome must fail closed, not retry.
    ///
    /// Returns `(value, attempts_used)` on success, else a [`RetryError`].
    pub async fn execute_async<T, F, Fut>(
        &mut self,
        mut now: impl FnMut() -> u64,
        mut attempt: F,
    ) -> Result<(T, u32), RetryError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, AttemptError>>,
    {
        // P3-022: bound THIS call, not the orchestrator's lifetime.
        let mut budget = RetryBudget::new(self.max_attempts);
        let mut attempts: u32 = 0;
        loop {
            // Breaker before budget: a short-circuited call never reaches the dependency, so
            // it must not consume a budget unit either.
            if !self.breaker.allow_call(now()) {
                return Err(RetryError::BreakerOpen);
            }
            if !budget.try_allow() {
                return Err(RetryError::Exhausted { attempts });
            }
            attempts += 1;
            match attempt().await {
                Ok(value) => {
                    self.breaker.record_success();
                    return Ok((value, attempts));
                }
                Err(AttemptError::Terminal(_)) => return Err(RetryError::Terminal),
                Err(AttemptError::Transient(_)) => {
                    self.breaker.record_failure(now());
                    if budget.exhausted() || self.breaker.is_open() {
                        return Err(RetryError::Exhausted { attempts });
                    }
                    // P3-023: apply the backoff. This is the live bridge path — a discarded
                    // delay meant a retry storm against a struggling transport.
                    let delay_ms = self.backoff.next_delay_ms();
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- RESILIENCE-002: exponential backoff correctness ---
    #[test]
    fn resilience002_exponential_backoff_doubles_then_caps() {
        let mut b = Backoff::new(100, 1000);
        assert_eq!(b.next_delay_ms(), 100); // base
        assert_eq!(b.next_delay_ms(), 200); // x2
        assert_eq!(b.next_delay_ms(), 400); // x4
        assert_eq!(b.next_delay_ms(), 800); // x8
        assert_eq!(b.next_delay_ms(), 1000); // capped
        assert_eq!(b.next_delay_ms(), 1000); // stays capped
    }

    // --- RESILIENCE-003: retry budget exhaustion ---
    #[test]
    fn resilience003_retry_budget_exhaustion_stops() {
        let mut b = RetryBudget::new(3);
        assert!(b.try_allow());
        assert!(b.try_allow());
        assert!(b.try_allow());
        assert!(!b.try_allow()); // exhausted
        assert!(b.exhausted());
        assert_eq!(b.used(), 3);
    }

    // --- RESILIENCE-004: circuit breaker opens after threshold ---
    #[test]
    fn resilience004_circuit_breaker_opens_after_threshold() {
        let mut c = CircuitBreaker::new(3, 10_000);
        assert_eq!(c.state(), BreakerState::Closed);
        c.record_failure(0);
        c.record_failure(0);
        c.record_failure(0); // 3rd -> open
        assert_eq!(c.state(), BreakerState::Open);
        assert!(!c.allow_call(100)); // cooldown (10s) not elapsed -> short-circuit
        assert_eq!(c.state(), BreakerState::Open);
    }

    // --- RESILIENCE-005: recovery after dependency returns (half-open probe closes) ---
    #[test]
    fn resilience005_breaker_recovers_after_dependency_returns() {
        let mut c = CircuitBreaker::new(2, 50);
        c.record_failure(0);
        c.record_failure(0);
        assert_eq!(c.state(), BreakerState::Open);
        assert!(!c.allow_call(0)); // still within cooldown
                                   // After cooldown, exactly one probe is allowed -> HalfOpen.
        assert!(c.allow_call(100));
        assert_eq!(c.state(), BreakerState::HalfOpen);
        // A successful probe closes the breaker.
        c.record_success();
        assert_eq!(c.state(), BreakerState::Closed);
        assert!(c.allow_call(101));
        // A failed probe re-opens.
        let mut d = CircuitBreaker::new(1, 50);
        d.record_failure(0);
        assert!(d.allow_call(100)); // probe
        d.record_failure(100); // probe fails -> re-open
        assert_eq!(d.state(), BreakerState::Open);
    }

    // --- RESILIENCE-007: duplicate retry prevention (never re-invoke a done key) ---
    #[test]
    fn resilience007_duplicate_retry_never_re_invokes() {
        use std::cell::Cell;
        let calls = Cell::new(0u32);
        let mut o = RetryOrchestrator::new(RetryConfig {
            max_attempts: 3,
            base_backoff_ms: 10,
            cap_backoff_ms: 80,
            breaker_threshold: 3,
            breaker_cooldown_ms: 100,
        });
        let mut attempt = || {
            calls.set(calls.get() + 1);
            Ok::<i64, AttemptError>(42)
        };
        let (v, n) = o.execute("k1", || 0, &mut attempt).unwrap();
        assert_eq!(v, 42);
        assert_eq!(n, 1);
        assert_eq!(calls.get(), 1);
        // Re-executing the SAME logical call returns Duplicate and does NOT re-invoke.
        let r = o.execute("k1", || 0, &mut attempt);
        assert_eq!(r, Err(RetryError::Duplicate));
        assert_eq!(
            calls.get(),
            1,
            "a done key is never re-invoked (RESILIENCE-007)"
        );
    }

    // --- RESILIENCE-003 (orchestrator): transient failures retried until success ---
    #[test]
    fn resilience003_orchestrator_retries_transient_then_succeeds() {
        let mut attempts = 0u32;
        let mut o = RetryOrchestrator::new(RetryConfig {
            max_attempts: 5,
            base_backoff_ms: 10,
            cap_backoff_ms: 40,
            breaker_threshold: 5,
            breaker_cooldown_ms: 100,
        });
        let mut attempt = || {
            attempts += 1;
            if attempts < 3 {
                Err(AttemptError::Transient("flaky".into()))
            } else {
                Ok("done")
            }
        };
        let (v, n) = o.execute("k2", || 0, &mut attempt).unwrap();
        assert_eq!(v, "done");
        assert_eq!(n, 3, "retried exactly until third success");
        assert_eq!(attempts, 3);
        assert_eq!(o.breaker_state(), BreakerState::Closed);
    }

    // --- RESILIENCE-001/003: storm containment — bounded attempts, then exhausted ---
    #[test]
    fn resilience001_retry_storm_is_bounded_and_exhausts() {
        let mut attempts = 0u32;
        let mut o = RetryOrchestrator::new(RetryConfig {
            max_attempts: 4,
            base_backoff_ms: 10,
            cap_backoff_ms: 40,
            breaker_threshold: 100, // no breaker interference
            breaker_cooldown_ms: 100,
        });
        let mut attempt = || {
            attempts += 1;
            Err::<(), AttemptError>(AttemptError::Transient("permanent outage".into()))
        };
        let r = o.execute("k3", || 0, &mut attempt);
        assert_eq!(r, Err(RetryError::Exhausted { attempts: 4 }));
        assert_eq!(
            attempts, 4,
            "retry storm is hard-bounded (RESILIENCE-001/003)"
        );
    }

    // --- RESILIENCE-004 (orchestrator): breaker short-circuits further calls ---
    #[test]
    fn resilience004_orchestrator_short_circuits_when_open() {
        use std::cell::Cell;
        let calls = Cell::new(0u32);
        let mut o = RetryOrchestrator::new(RetryConfig {
            max_attempts: 50,
            base_backoff_ms: 1,
            cap_backoff_ms: 1,
            breaker_threshold: 2, // opens fast
            breaker_cooldown_ms: 1000,
        });
        let mut attempt = || {
            calls.set(calls.get() + 1);
            Err::<(), AttemptError>(AttemptError::Transient("down".into()))
        };
        let _ = o.execute("k4", || 0, &mut attempt);
        let calls_before = calls.get();
        // A NEW key while the breaker is open is not even attempted (short-circuit).
        let r = o.execute("k5", || 0, &mut attempt);
        assert_eq!(r, Err(RetryError::BreakerOpen));
        assert_eq!(
            calls.get(),
            calls_before,
            "open breaker short-circuits without touching the dependency"
        );
        assert_eq!(o.breaker_state(), BreakerState::Open);
    }

    // --- async live-path retry: transient err -> retry until success, bounded ---
    #[tokio::test(flavor = "current_thread")]
    async fn execute_async_retries_transient_err_then_succeeds() {
        use std::cell::Cell;
        let calls = Cell::new(0u32);
        let mut o = RetryOrchestrator::new(RetryConfig {
            max_attempts: 5,
            base_backoff_ms: 1,
            cap_backoff_ms: 2,
            breaker_threshold: 10,
            breaker_cooldown_ms: 1000,
        });
        let now = 0u64;
        // Two transient transport failures, then a broker decision (Ok per live contract).
        let mut attempt = || {
            let n = calls.get() + 1;
            calls.set(n);
            async move {
                match n {
                    1 | 2 => Err::<i64, AttemptError>(AttemptError::Transient("timeout".into())),
                    _ => Ok::<i64, AttemptError>(42),
                }
            }
        };
        let (v, attempts) = o.execute_async(|| now, &mut attempt).await.unwrap();
        assert_eq!(v, 42);
        assert_eq!(attempts, 3);
        assert_eq!(
            calls.get(),
            3,
            "transient errs are re-invoked, then success"
        );
    }

    // --- async live-path retry: budget exhaustion surfaces, never retries forever ---
    #[tokio::test(flavor = "current_thread")]
    async fn execute_async_budget_exhaustion_bounds_transient_retries() {
        use std::cell::Cell;
        let mut o = RetryOrchestrator::new(RetryConfig {
            max_attempts: 4,
            base_backoff_ms: 1,
            cap_backoff_ms: 2,
            breaker_threshold: 100,
            breaker_cooldown_ms: 1000,
        });
        let attempts = Cell::new(0u32);
        let mut attempt = || {
            attempts.set(attempts.get() + 1);
            async move { Err::<i64, AttemptError>(AttemptError::Transient("down".into())) }
        };
        let r = o.execute_async(|| 0, &mut attempt).await;
        assert_eq!(r, Err(RetryError::Exhausted { attempts: 4 }));
        assert_eq!(attempts.get(), 4, "bounded: never retries past the budget");
    }

    // --- P3-022: the budget bounds one call, not the orchestrator's lifetime ---
    #[test]
    fn p3_022_exhausted_call_does_not_brick_later_calls() {
        let mut o = RetryOrchestrator::new(RetryConfig {
            max_attempts: 2,
            base_backoff_ms: 1,
            cap_backoff_ms: 1,
            breaker_threshold: 100, // keep the breaker out of the way
            breaker_cooldown_ms: 1000,
        });
        // The first logical call burns its entire budget on a permanent outage.
        let mut always_fail = || Err::<(), AttemptError>(AttemptError::Transient("down".into()));
        assert_eq!(
            o.execute("k1", || 0, &mut always_fail),
            Err(RetryError::Exhausted { attempts: 2 })
        );
        // A later, unrelated call must still be attempted. Against the old shared budget this
        // returned Exhausted { attempts: 0 } WITHOUT touching the dependency, for the rest of
        // the process's life (one orchestrator is built per client).
        let mut ok = || Ok::<i64, AttemptError>(7);
        let (v, n) = o.execute("k2", || 0, &mut ok).unwrap();
        assert_eq!(v, 7);
        assert_eq!(n, 1, "a fresh key gets a fresh budget, not the drained one");
    }

    // --- P3-022: a breaker short-circuit must not consume budget ---
    #[test]
    fn p3_022_breaker_short_circuit_does_not_consume_budget() {
        let mut o = RetryOrchestrator::new(RetryConfig {
            max_attempts: 2,
            base_backoff_ms: 1,
            cap_backoff_ms: 1,
            breaker_threshold: 1, // opens on the first failure
            breaker_cooldown_ms: 10_000,
        });
        let mut always_fail = || Err::<(), AttemptError>(AttemptError::Transient("down".into()));
        let _ = o.execute("k1", || 0, &mut always_fail); // opens the breaker
        assert_eq!(o.breaker_state(), BreakerState::Open);
        // Short-circuited: reported as BreakerOpen, not Exhausted, because no budget unit
        // was spent on a call that never reached the dependency.
        let mut ok = || Ok::<i64, AttemptError>(1);
        assert_eq!(o.execute("k2", || 0, &mut ok), Err(RetryError::BreakerOpen));
    }

    // --- P3-023: the backoff delay is actually applied between attempts ---
    #[test]
    fn p3_023_backoff_is_applied_between_attempts() {
        use std::time::{Duration, Instant};
        let mut o = RetryOrchestrator::new(RetryConfig {
            max_attempts: 3,
            base_backoff_ms: 40,
            cap_backoff_ms: 40,
            breaker_threshold: 100,
            breaker_cooldown_ms: 1000,
        });
        let started = Instant::now();
        let mut fail = || Err::<(), AttemptError>(AttemptError::Transient("down".into()));
        assert_eq!(
            o.execute("k1", || 0, &mut fail),
            Err(RetryError::Exhausted { attempts: 3 })
        );
        // Two gaps of 40ms. Before the fix the loop computed and discarded each delay, so
        // this returned in microseconds — the "backoff" was a hot retry storm.
        assert!(
            started.elapsed() >= Duration::from_millis(80),
            "backoff must actually be applied, elapsed={:?}",
            started.elapsed()
        );
    }

    // --- P3-023: ...on the LIVE async bridge path too ---
    #[tokio::test(flavor = "current_thread")]
    async fn p3_023_async_backoff_is_applied_between_attempts() {
        use std::time::{Duration, Instant};
        let mut o = RetryOrchestrator::new(RetryConfig {
            max_attempts: 3,
            base_backoff_ms: 30,
            cap_backoff_ms: 30,
            breaker_threshold: 100,
            breaker_cooldown_ms: 1000,
        });
        let started = Instant::now();
        let mut fail =
            || async { Err::<i64, AttemptError>(AttemptError::Transient("down".into())) };
        assert_eq!(
            o.execute_async(|| 0, &mut fail).await,
            Err(RetryError::Exhausted { attempts: 3 })
        );
        assert!(
            started.elapsed() >= Duration::from_millis(60),
            "async backoff must actually be applied, elapsed={:?}",
            started.elapsed()
        );
    }

    // --- P3-216: the idempotency memo is bounded, so a long-lived executor cannot grow it
    // forever with distinct logical keys ---
    #[test]
    fn memo_is_bounded_and_evicts_the_oldest_key() {
        let mut guard = IdempotencyGuard::new();
        let total = IdempotencyGuard::MEMO_CAPACITY + 1;
        for i in 0..total {
            guard.mark(&format!("key-{i}"));
        }
        assert!(!guard.done("key-0"), "the oldest key must be evicted");
        assert!(
            guard.done(&format!("key-{}", total - 1)),
            "the newest key stays"
        );
        let retained = (0..total).filter(|i| guard.done(&format!("key-{i}"))).count();
        assert_eq!(
            retained,
            IdempotencyGuard::MEMO_CAPACITY,
            "the memo is capped"
        );
    }

    #[test]
    fn a_key_inside_the_window_is_never_re_admitted() {
        let mut guard = IdempotencyGuard::new();
        guard.mark("first");
        for i in 0..(IdempotencyGuard::MEMO_CAPACITY - 1) {
            guard.mark(&format!("filler-{i}"));
        }
        assert!(guard.done("first"), "a key still inside the window survives");
    }
}
