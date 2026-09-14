//! Durable, crash-window-safe command gate — Rust port of the T5 CHG-044 protocol.
//!
//! Ports the `ExecutionCommandGate` + `ExecutionCommandGateCrashWindowTest` semantics
//! (Java, `code/common/.../schema/execution/`) into the offline Rust slice. The
//! protocol composes an [`AttemptStore`], a [`GateStateStore`] and a [`BridgeCaller`]
//! to enforce the order-safety invariant: the bridge is called **exactly once per
//! attempt**, only between a durable `SUBMITTING` and a durable terminal outcome.
//!
//! Crash-window safety: a restart that observes `PREPARED` resumes (no bridge call was
//! issued yet); a restart that observes `SUBMITTING`/`UNKNOWN` halts the gate and demands
//! reconciliation rather than issuing a second place — zero duplicate commands for every
//! injected crash window. [`CrashHooks`] are production no-ops; `InMemory{Attempt,GateState}`
//! mirror the Java `InMemory*` stores, and multiple [`ExecutionGate`]s bound to the same
//! `Rc<dyn …>` handles simulate process restarts sharing one durable memory. The live slice
//! swaps the Fluss-backed stores (Workstream D) behind the same traits.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use anyhow::Result;

/// Durable phase of a single money-moving attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptPhase {
    /// Durable but the bridge has not been called.
    Prepared,
    /// Durable and the bridge call is in flight (or was).
    Submitting,
    /// Bridge positively accepted.
    Accepted,
    /// Bridge positively rejected.
    Rejected,
    /// Ambiguous outcome; reconciliation required.
    Unknown,
}

impl AttemptPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "PREPARED",
            Self::Submitting => "SUBMITTING",
            Self::Accepted => "ACCEPTED",
            Self::Rejected => "REJECTED",
            Self::Unknown => "UNKNOWN",
        }
    }
}

/// A durable attempt record keyed by attempt id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attempt {
    pub attempt_id: String,
    pub instruction_id: String,
    pub request_hash: String,
    pub client_order_ref: String,
    pub phase: AttemptPhase,
    pub broker_order_id: Option<String>,
    pub reason: Option<String>,
}

impl Attempt {
    pub fn new(
        attempt_id: &str,
        instruction_id: &str,
        request_hash: &str,
        client_order_ref: &str,
        phase: AttemptPhase,
    ) -> Self {
        Self {
            attempt_id: attempt_id.to_string(),
            instruction_id: instruction_id.to_string(),
            request_hash: request_hash.to_string(),
            client_order_ref: client_order_ref.to_string(),
            phase,
            broker_order_id: None,
            reason: None,
        }
    }
}

/// Durable per-attempt store (interior mutability; identically consistent across gate restarts).
///
/// **Atomicity contract (P3-201).** The claim path is `get` →
/// `has_duplicate`/`has_instruction` → `put` as it stood before D1. That sequence is safe only
/// while this store has a single actor: two actors sharing one store can both observe "nothing
/// durable for this `(instruction_id, request_hash)`" and both call the bridge, breaking
/// exactly-once. Any store reachable by more than one actor — a durable/remote store shared
/// between executors, or a restarted process running alongside its predecessor — MUST make
/// classify-and-claim atomic for one `(instruction_id, request_hash)`.
///
/// The entry point is now [`AttemptStore::try_claim`]: classify + insert in one call returning
/// claimed / existing / duplicate / contract-violation, called by [`ExecutionGate::execute`] in
/// place of the check-then-act pair. A contract stated only in a comment does not bind a remote
/// implementation. The key layout it fixes — resume by `attempt_id`, duplicate by
/// `instruction_id` + `request_hash`, existence by `instruction_id` — is the decision the durable
/// implementation has to carry, and the load-bearing part is that a durable store performs the
/// classification and the insert as ONE durable operation.
/// Result of an atomic classify-and-claim ([`AttemptStore::try_claim`], P3-201).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Claim {
    /// The identity was new: exactly one PREPARED attempt was inserted, and this is it.
    Claimed(Attempt),
    /// This `attempt_id` is already durable — resume from its phase, never a second bridge call.
    Existing(Attempt),
    /// Another attempt already carries this `(instruction_id, request_hash)` — no new call.
    Duplicate,
    /// This `instruction_id` is durable with a different `request_hash` — contract violation.
    ContractViolation,
}

pub trait AttemptStore {
    fn get(&self, attempt_id: &str) -> Option<Attempt>;
    /// Persists create/update and returns `Ok` only after the durable acknowledgement.
    fn put(&self, attempt: &Attempt) -> Result<()>;
    /// True when an attempt with this `(instruction_id, request_hash)` already exists.
    fn has_duplicate(&self, instruction_id: &str, request_hash: &str) -> bool;
    /// True when any attempt with this `instruction_id` exists (regardless of hash).
    fn has_instruction(&self, instruction_id: &str) -> bool;
    /// Classifies the identity and, when it is new, claims it — in one operation (P3-201).
    ///
    /// This is the whole of `get` → `has_duplicate`/`has_instruction` → `put` from
    /// [`ExecutionGate::execute`] as a single call, because that check-then-act sequence is safe
    /// only while the store has one actor: two actors sharing one store can both observe "nothing
    /// durable for this `(instruction_id, request_hash)`" and both call the bridge.
    ///
    /// **Key layout** (the decision a durable implementation must make deliberately): `attempt_id`
    /// is the primary key; `(instruction_id, request_hash)` must be unique; a different
    /// `request_hash` under an existing `instruction_id` is a contract violation. A store reachable
    /// by more than one actor MUST implement this as one durable conditional insert against those
    /// keys — not as a read followed by a write. The claim always mints `PREPARED`: a caller cannot
    /// claim straight into a later phase.
    fn try_claim(
        &self,
        attempt_id: &str,
        instruction_id: &str,
        request_hash: &str,
        client_order_ref: &str,
    ) -> Result<Claim>;
}

/// Durable gate row for a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateState {
    Enabled,
    Halted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateRow {
    pub partition: String,
    pub owner: String,
    pub state: GateState,
    pub epoch: u64,
    pub fence_token: u64,
}

/// Owner recorded on a gate row that this service created at boot, before any worker bound it.
/// It is a label, not an actor: gate decisions read state/epoch/fence, and a bound owner is never
/// replaced by a boot-time create (see [`GateStateStore::init`]).
pub const BOOT_HALTED_OWNER: &str = "unbound";

/// Durable gate-state store (interior mutability).
pub trait GateStateStore {
    fn read(&self, partition: &str) -> Option<GateRow>;
    fn write(&self, row: &GateRow) -> Result<()>;
    /// Creates the row if the partition has none, else returns the existing row untouched (D2).
    ///
    /// The row is durable state about a partition, so a deliberate halt must be able to *create* it:
    /// writing nothing and reporting success on a missing row is the fail-open shape — the gate
    /// would be HALTED for nobody, and the error propagation of P3-202 cannot fire when there is no
    /// row to write. "The row exists" is a property of the durable store, so the create belongs to
    /// the store, not to each caller as a read-then-write habit (mirrors Java `GateStateStore.init`,
    /// which is why a restarted executor cannot clear a fenced gate — P3-151).
    ///
    /// The incidental halts inside [`ExecutionGate`] must **not** come through here: a halt raised
    /// *because* no row vouches for the partition (missing/corrupt/unreadable gate state) must not
    /// then invent one, or a partition state that nothing established would acquire an epoch and a
    /// fence token.
    fn init(&self, row: &GateRow) -> Result<GateRow>;
}

/// Terminal bridge classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeKind {
    Accepted,
    Rejected,
    /// Ambiguous (timeout, disconnect, unparseable) — must never auto-retry.
    Unknown,
}

#[derive(Debug, Clone)]
pub struct BridgeOutcome {
    pub kind: OutcomeKind,
    pub broker_order_id: String,
    pub reason: String,
}

/// Exactly-once bridge call site.
pub trait BridgeCaller {
    /// Must not be retried by the caller on any outcome, including an error (`Result::Err`
    /// simulates a mid-flight bridge crash).
    fn call(&self, cmd: &Command) -> Result<BridgeOutcome>;
}

/// A single money-moving command; identity is deterministic and caller-supplied.
#[derive(Debug, Clone)]
pub struct Command {
    pub execution_attempt_id: String,
    pub account_scope_id: String,
    pub instruction_id: String,
    pub execution_partition_id: String,
    pub request_hash: String,
    pub client_order_ref: String,
    pub gate_epoch: u64,
    pub gate_fence_token: u64,
}

/// Terminal classification of the protocol after a run/restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Accepted,
    Rejected,
    /// Same `(instruction_id, request_hash)` on another attempt — no new call.
    Duplicate,
    /// Same `instruction_id` but different `request_hash` — contract violation, halted, no call.
    ContractViolation,
    /// Gate not ENABLED or epoch/fence mismatch — no call.
    Blocked,
    /// Restart on `SUBMITTING`/`UNKNOWN` or a bridge `UNKNOWN` — gate halted, reconcile.
    UnknownHalted,
}

/// Injectable crash points for the crash-window suite (no-op in production).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CrashHooks {
    pub after_prepared: bool,
    pub after_submitting: bool,
    pub before_bridge: bool,
    pub after_bridge: bool,
}

/// The durable command gate. Stores/bridge are injected as `Rc<dyn …>` handles so several
/// gate instances can share one durable memory (simulating process restarts).
pub struct ExecutionGate {
    attempts: Rc<dyn AttemptStore>,
    gates: Rc<dyn GateStateStore>,
    bridge: Rc<dyn BridgeCaller>,
}

impl ExecutionGate {
    pub fn new(
        attempts: Rc<dyn AttemptStore>,
        gates: Rc<dyn GateStateStore>,
        bridge: Rc<dyn BridgeCaller>,
    ) -> Self {
        Self {
            attempts,
            gates,
            bridge,
        }
    }

    pub fn shared_attempts(&self) -> Rc<dyn AttemptStore> {
        self.attempts.clone()
    }
    pub fn shared_gates(&self) -> Rc<dyn GateStateStore> {
        self.gates.clone()
    }
    pub fn shared_bridge(&self) -> Rc<dyn BridgeCaller> {
        self.bridge.clone()
    }

    /// Runs one command through the durable protocol and returns the terminal outcome.
    ///
    /// On a simulated crash the method returns `Err` (process death); the durable stores are
    /// the only memory that survives into the restarted gate.
    pub fn execute(&mut self, cmd: &Command, hooks: CrashHooks) -> Result<Outcome> {
        // D1/P3-201: classify and claim in ONE store operation. Resume-by-attempt-id stays
        // authoritative — a durable attempt for THIS id means this money movement already began (or
        // finished), so there is never a second bridge call — but the duplicate and
        // contract-violation classifications belong to the store now: it owns the keys
        // (`attempt_id`, `(instruction_id, request_hash)`, `instruction_id`), so only the store can
        // make "nothing durable for this identity" and "claim it" one indivisible step.
        let resume: Option<Attempt> = match self.attempts.try_claim(
            &cmd.execution_attempt_id,
            &cmd.instruction_id,
            &cmd.request_hash,
            &cmd.client_order_ref,
        )? {
            // Another attempt already carries the same (instruction_id, request_hash): a different
            // attempt id for the same logical order => duplicate, no new call.
            Claim::Duplicate => return Ok(Outcome::Duplicate),
            // Same instruction but a different request hash → contract violation: halt, no call.
            Claim::ContractViolation => {
                self.halt(cmd)?;
                return Ok(Outcome::ContractViolation);
            }
            // Fresh attempt: PREPARED is already durable before any bridge call.
            Claim::Claimed(_) => {
                if hooks.after_prepared {
                    anyhow::bail!("crash at AFTER_PREPARED");
                }
                None
            }
            Claim::Existing(existing) => Some(existing),
        };

        if let Some(existing) = resume {
            match existing.phase {
                // Resume from a durable, bridge-not-yet-called state: proceed to SUBMITTING.
                AttemptPhase::Prepared => {}
                // The bridge may or may not have been reached: halt, never a second call.
                AttemptPhase::Submitting | AttemptPhase::Unknown => {
                    self.halt(cmd)?;
                    return Ok(Outcome::UnknownHalted);
                }
                AttemptPhase::Accepted => return Ok(Outcome::Accepted),
                AttemptPhase::Rejected => return Ok(Outcome::Rejected),
            }
        }

        // Transition to SUBMITTING durably, then (optionally) crash.
        self.attempts.put(&Attempt::new(
            &cmd.execution_attempt_id,
            &cmd.instruction_id,
            &cmd.request_hash,
            &cmd.client_order_ref,
            AttemptPhase::Submitting,
        ))?;
        if hooks.after_submitting {
            anyhow::bail!("crash at AFTER_SUBMITTING");
        }

        // Validate the gate immediately before the call (epoch + fence + ENABLED).
        let gate_ok = matches!(
            self.gates.read(&cmd.execution_partition_id),
            Some(g)
                if g.state == GateState::Enabled
                    && g.epoch == cmd.gate_epoch
                    && g.fence_token == cmd.gate_fence_token
        );
        if !gate_ok {
            self.halt(cmd)?;
            return Ok(Outcome::Blocked);
        }

        if hooks.before_bridge {
            anyhow::bail!("crash at BEFORE_BRIDGE");
        }

        // Exactly-once bridge call; a bridge error simulates a mid-flight crash (attempt stays
        // SUBMITTING so a restart halts rather than re-issuing).
        let outcome = self.bridge.call(cmd)?;

        // Persist the terminal outcome durably.
        let terminal = match outcome.kind {
            OutcomeKind::Accepted => AttemptPhase::Accepted,
            OutcomeKind::Rejected => AttemptPhase::Rejected,
            OutcomeKind::Unknown => AttemptPhase::Unknown,
        };
        self.attempts.put(&Attempt {
            attempt_id: cmd.execution_attempt_id.clone(),
            instruction_id: cmd.instruction_id.clone(),
            request_hash: cmd.request_hash.clone(),
            client_order_ref: cmd.client_order_ref.clone(),
            phase: terminal,
            broker_order_id: if outcome.broker_order_id.is_empty() {
                None
            } else {
                Some(outcome.broker_order_id.clone())
            },
            reason: (!outcome.reason.is_empty()).then(|| outcome.reason.clone()),
        })?;

        if hooks.after_bridge {
            anyhow::bail!("crash at AFTER_BRIDGE");
        }

        // A bridge UNKNOWN leaves the order's fate ambiguous: persist the durable halt
        // before reporting UnknownHalted, so no other instruction on this partition can
        // reach the bridge while the attempt awaits reconciliation (P3-018).
        if terminal == AttemptPhase::Unknown {
            self.halt(cmd)?;
        }

        Ok(match terminal {
            AttemptPhase::Accepted => Outcome::Accepted,
            AttemptPhase::Rejected => Outcome::Rejected,
            _ => Outcome::UnknownHalted,
        })
    }

    fn halt(&self, cmd: &Command) -> Result<()> {
        if let Some(g) = self.gates.read(&cmd.execution_partition_id) {
            self.gates.write(&GateRow {
                partition: g.partition,
                owner: g.owner,
                state: GateState::Halted,
                epoch: g.epoch,
                fence_token: g.fence_token,
            })?;
        }
        Ok(())
    }
}

/// In-memory [`AttemptStore`] (mirrors Java `InMemoryAttemptStore`).
#[derive(Debug, Default, Clone)]
pub struct InMemoryAttemptStore {
    by_id: RefCell<HashMap<String, Attempt>>,
    // instruction_id -> request hashes. Nested so both lookups borrow (`&str`), keeping
    // the hot `has_duplicate` path allocation-free (P3-446).
    dup: RefCell<HashMap<String, HashSet<String>>>,
    by_instruction: RefCell<HashMap<String, ()>>,
}

impl InMemoryAttemptStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl AttemptStore for InMemoryAttemptStore {
    fn get(&self, attempt_id: &str) -> Option<Attempt> {
        self.by_id.borrow().get(attempt_id).cloned()
    }
    fn put(&self, attempt: &Attempt) -> Result<()> {
        self.by_id
            .borrow_mut()
            .insert(attempt.attempt_id.clone(), attempt.clone());
        self.dup
            .borrow_mut()
            .entry(attempt.instruction_id.clone())
            .or_default()
            .insert(attempt.request_hash.clone());
        self.by_instruction
            .borrow_mut()
            .insert(attempt.instruction_id.clone(), ());
        Ok(())
    }
    fn has_duplicate(&self, instruction_id: &str, request_hash: &str) -> bool {
        self.dup
            .borrow()
            .get(instruction_id)
            .is_some_and(|hashes| hashes.contains(request_hash))
    }
    fn has_instruction(&self, instruction_id: &str) -> bool {
        self.by_instruction.borrow().contains_key(instruction_id)
    }
    fn try_claim(
        &self,
        attempt_id: &str,
        instruction_id: &str,
        request_hash: &str,
        client_order_ref: &str,
    ) -> Result<Claim> {
        // Classification borrows the three indexes read-only and drops every borrow before the
        // insert, so nothing can observe a half-classified state; with one actor this is the same
        // sequence as the get → has_duplicate/has_instruction → put it replaces.
        {
            if let Some(existing) = self.by_id.borrow().get(attempt_id) {
                return Ok(Claim::Existing(existing.clone()));
            }
            if self
                .dup
                .borrow()
                .get(instruction_id)
                .is_some_and(|hashes| hashes.contains(request_hash))
            {
                return Ok(Claim::Duplicate);
            }
            if self.by_instruction.borrow().contains_key(instruction_id) {
                return Ok(Claim::ContractViolation);
            }
        }
        let attempt = Attempt::new(
            attempt_id,
            instruction_id,
            request_hash,
            client_order_ref,
            AttemptPhase::Prepared,
        );
        self.put(&attempt)?;
        Ok(Claim::Claimed(attempt))
    }
}

/// In-memory [`GateStateStore`] (mirrors Java `InMemoryGateStateStore`).
#[derive(Debug, Default, Clone)]
pub struct InMemoryGateStateStore {
    rows: RefCell<HashMap<String, GateRow>>,
}

impl InMemoryGateStateStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl GateStateStore for InMemoryGateStateStore {
    fn read(&self, partition: &str) -> Option<GateRow> {
        self.rows.borrow().get(partition).cloned()
    }
    fn write(&self, row: &GateRow) -> Result<()> {
        self.rows
            .borrow_mut()
            .insert(row.partition.clone(), row.clone());
        Ok(())
    }
    fn init(&self, row: &GateRow) -> Result<GateRow> {
        // Insert-if-absent under one borrow, so a concurrent caller cannot create it twice; an
        // existing row is returned as it stands.
        let mut rows = self.rows.borrow_mut();
        Ok(rows
            .entry(row.partition.clone())
            .or_insert_with(|| row.clone())
            .clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    /// P3-446: thread-local allocation counter, so the `has_duplicate` test can prove the
    /// duplicate lookup allocates nothing without interference from parallel tests. Calling
    /// threads count their own `alloc` calls; everything delegates to the system allocator.
    struct CountingAlloc;

    thread_local! {
        static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    }

    fn allocations() -> usize {
        ALLOCATIONS.with(Cell::get)
    }

    // SAFETY: a delegating wrapper around the system allocator; it only counts calls.
    unsafe impl std::alloc::GlobalAlloc for CountingAlloc {
        unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
            ALLOCATIONS.with(|n| n.set(n.get() + 1));
            unsafe { std::alloc::System.alloc(layout) }
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
            unsafe { std::alloc::System.dealloc(ptr, layout) }
        }
        unsafe fn realloc(
            &self,
            ptr: *mut u8,
            layout: std::alloc::Layout,
            new_size: usize,
        ) -> *mut u8 {
            unsafe { std::alloc::System.realloc(ptr, layout, new_size) }
        }
        unsafe fn alloc_zeroed(&self, layout: std::alloc::Layout) -> *mut u8 {
            unsafe { std::alloc::System.alloc_zeroed(layout) }
        }
    }

    #[global_allocator]
    static COUNTING_ALLOC: CountingAlloc = CountingAlloc;

    const PARTITION: &str = "p-1";

    /// Counting bridge shared across restarts — cumulative, must never see a duplicate place.
    struct CountingBridge {
        total: Rc<Cell<usize>>,
        crash: Cell<bool>,
    }

    impl CountingBridge {
        fn new(handle: Rc<Cell<usize>>, crash: bool) -> Self {
            Self {
                total: handle,
                crash: Cell::new(crash),
            }
        }
        fn set_crash(&self, crash: bool) {
            self.crash.set(crash);
        }
    }

    impl BridgeCaller for CountingBridge {
        fn call(&self, cmd: &Command) -> Result<BridgeOutcome> {
            self.total.set(self.total.get() + 1); // count even when it crashes mid-flight
            if self.crash.get() {
                anyhow::bail!("simulated bridge crash");
            }
            Ok(BridgeOutcome {
                kind: OutcomeKind::Accepted,
                broker_order_id: format!("B-{}", cmd.execution_attempt_id),
                reason: "ok".into(),
            })
        }
    }

    fn enabled_row() -> GateRow {
        GateRow {
            partition: PARTITION.into(),
            owner: "worker-1".into(),
            state: GateState::Enabled,
            epoch: 5,
            fence_token: 7,
        }
    }

    fn cmd(attempt_id: &str) -> Command {
        Command {
            execution_attempt_id: attempt_id.into(),
            account_scope_id: "acc".into(),
            instruction_id: "ins-1".into(),
            execution_partition_id: PARTITION.into(),
            request_hash: "h-1".into(),
            client_order_ref: format!("E-{attempt_id}"),
            gate_epoch: 5,
            gate_fence_token: 7,
        }
    }

    fn shared_gates() -> Rc<dyn GateStateStore> {
        let gates = InMemoryGateStateStore::new();
        gates.write(&enabled_row()).unwrap();
        Rc::new(gates)
    }

    /// Runs one command against a crashing engine, then a fresh engine over the SAME durable
    /// stores and the SAME cumulative counting bridge.
    fn crash_rerun(
        hooks: CrashHooks,
        bridge_crash: bool,
    ) -> (Outcome, usize, Option<AttemptPhase>, GateState) {
        let total_handle = Rc::new(Cell::new(0usize));
        let counter = Rc::new(CountingBridge::new(Rc::clone(&total_handle), bridge_crash));
        let attempts: Rc<dyn AttemptStore> = Rc::new(InMemoryAttemptStore::new());
        let gates = shared_gates();
        let bridge: Rc<dyn BridgeCaller> = counter.clone();

        // First engine: may die at the injected hook (or mid-bridge).
        let mut g0 = ExecutionGate::new(attempts.clone(), gates.clone(), bridge.clone());
        let _ = g0.execute(&cmd("a-1"), hooks).err();

        // Restart: durable state (and the cumulative bridge counter) are the only memory kept.
        counter.set_crash(false);
        let mut g1 = ExecutionGate::new(attempts.clone(), gates.clone(), bridge.clone());
        let result = g1.execute(&cmd("a-1"), CrashHooks::default()).unwrap();

        let total = total_handle.get();
        let phase = attempts.get("a-1").map(|a| a.phase);
        let state = gates.read(PARTITION).map(|g| g.state).unwrap();
        (result, total, phase, state)
    }

    #[test]
    fn crash_before_durable_prepare_issues_exactly_one_call() {
        let total_handle = Rc::new(Cell::new(0usize));
        let counter = Rc::new(CountingBridge::new(Rc::clone(&total_handle), false));
        let attempts: Rc<dyn AttemptStore> = Rc::new(InMemoryAttemptStore::new());
        let gates = shared_gates();
        let bridge: Rc<dyn BridgeCaller> = counter.clone();

        let mut g1 = ExecutionGate::new(attempts.clone(), gates.clone(), bridge.clone());
        assert_eq!(
            g1.execute(&cmd("a-1"), CrashHooks::default()).unwrap(),
            Outcome::Accepted
        );
        assert_eq!(total_handle.get(), 1);
        // Restart sees ACCEPTED -> idempotent, no second call.
        let mut g2 =
            ExecutionGate::new(g1.shared_attempts(), g1.shared_gates(), g1.shared_bridge());
        assert_eq!(
            g2.execute(&cmd("a-1"), CrashHooks::default()).unwrap(),
            Outcome::Accepted
        );
        assert_eq!(total_handle.get(), 1);
    }

    #[test]
    fn crash_after_prepared_resumes_with_exactly_one_call() {
        let (result, total, phase, state) = crash_rerun(
            CrashHooks {
                after_prepared: true,
                ..Default::default()
            },
            false,
        );
        assert_eq!(result, Outcome::Accepted);
        // One call total (none before the crash, one on resume) — never a duplicate.
        assert!(total <= 1);
        assert_eq!(total, 1);
        assert_eq!(phase, Some(AttemptPhase::Accepted));
        assert_eq!(state, GateState::Enabled);
    }

    #[test]
    fn crash_after_submitting_halts_with_zero_duplicate_calls() {
        let (result, total, phase, state) = crash_rerun(
            CrashHooks {
                after_submitting: true,
                ..Default::default()
            },
            false,
        );
        assert_eq!(total, 0);
        assert!(total <= 1);
        assert_eq!(result, Outcome::UnknownHalted);
        assert_eq!(phase, Some(AttemptPhase::Submitting));
        assert_eq!(state, GateState::Halted);
    }

    #[test]
    fn crash_before_bridge_halts_with_zero_duplicate_calls() {
        let (result, total, phase, _) = crash_rerun(
            CrashHooks {
                before_bridge: true,
                ..Default::default()
            },
            false,
        );
        assert_eq!(total, 0);
        assert!(total <= 1);
        assert_eq!(result, Outcome::UnknownHalted);
        assert_eq!(phase, Some(AttemptPhase::Submitting));
    }

    #[test]
    fn crash_during_bridge_halts_with_no_second_call() {
        // Mid-flight bridge crash counts the attempt once; restart halts (no re-issue).
        let (result, total, phase, state) =
            crash_rerun(CrashHooks::default(), /*bridge_crash=*/ true);
        assert_eq!(total, 1);
        assert!(total <= 1);
        assert_eq!(result, Outcome::UnknownHalted);
        assert_eq!(phase, Some(AttemptPhase::Submitting));
        assert_eq!(state, GateState::Halted);
    }

    #[test]
    fn crash_after_bridge_is_idempotent_accepted() {
        // Outcome persisted ACCEPTED, then crash after -> restart is idempotent, no re-call.
        let (result, total, phase, _) = crash_rerun(
            CrashHooks {
                after_bridge: true,
                ..Default::default()
            },
            false,
        );
        assert_eq!(total, 1);
        assert!(total <= 1);
        assert_eq!(result, Outcome::Accepted);
        assert_eq!(phase, Some(AttemptPhase::Accepted));
    }

    #[test]
    fn duplicate_instruction_request_hash_does_not_issue_a_new_call() {
        let total_handle = Rc::new(Cell::new(0usize));
        let counter = Rc::new(CountingBridge::new(Rc::clone(&total_handle), false));
        let attempts: Rc<dyn AttemptStore> = Rc::new(InMemoryAttemptStore::new());
        let gates = shared_gates();
        let bridge: Rc<dyn BridgeCaller> = counter.clone();

        let mut g = ExecutionGate::new(attempts.clone(), gates, bridge.clone());
        // First attempt accepted.
        assert_eq!(
            g.execute(&cmd("a-1"), CrashHooks::default()).unwrap(),
            Outcome::Accepted
        );
        assert_eq!(total_handle.get(), 1);
        // A different attempt id, same instruction+hash -> duplicate, no new call.
        let dup = Command {
            execution_attempt_id: "a-2".into(),
            ..cmd("a-2")
        };
        assert_eq!(
            g.execute(&dup, CrashHooks::default()).unwrap(),
            Outcome::Duplicate
        );
        assert_eq!(total_handle.get(), 1);
    }

    // D1/P3-201: the classify-and-claim primitive itself, independent of `execute`, so the four
    // classifications are pinned where a durable implementation will have to reproduce them.
    #[test]
    fn try_claim_classifies_and_claims_in_one_call() {
        let store = InMemoryAttemptStore::new();

        // Fresh identity: exactly one PREPARED attempt, returned to the caller.
        let Claim::Claimed(claimed) = store.try_claim("a-1", "ins-1", "h-1", "c-1").unwrap() else {
            panic!("a fresh identity must claim");
        };
        assert_eq!(claimed.phase, AttemptPhase::Prepared);
        assert_eq!(claimed.attempt_id, "a-1");

        // Same attempt id again: resume material, not a second claim — and the phase the store
        // holds at that moment is what comes back (it is the durable state that wins).
        assert_eq!(
            store.try_claim("a-1", "ins-1", "h-1", "c-1").unwrap(),
            Claim::Existing(claimed.clone())
        );

        // Different attempt id, same (instruction_id, request_hash): duplicate.
        assert_eq!(
            store.try_claim("a-2", "ins-1", "h-1", "c-2").unwrap(),
            Claim::Duplicate
        );

        // Same instruction, different request hash: contract violation. The duplicate check must
        // win over this one — a repeated (instruction, hash) is the duplicate case even though the
        // instruction also exists.
        assert_eq!(
            store.try_claim("a-3", "ins-1", "h-9", "c-3").unwrap(),
            Claim::ContractViolation
        );

        // A rejected claim leaves nothing behind: the store still holds exactly the one attempt.
        assert_eq!(store.get("a-2"), None);
        assert_eq!(store.get("a-3"), None);
    }

    // P3-446: the duplicate index is consulted on every fresh `execute` that reaches the
    // claim path; has_duplicate must borrow the command strings rather than build owned
    // tuple keys per call.
    #[test]
    fn duplicate_lookup_performs_no_heap_allocation() {
        let store = InMemoryAttemptStore::new();
        store
            .put(&Attempt::new(
                "a-1",
                "ins-1",
                "h-1",
                "E-a-1",
                AttemptPhase::Prepared,
            ))
            .unwrap();

        let before = allocations();
        let duplicate = store.has_duplicate("ins-1", "h-1");
        let after = allocations();
        assert!(duplicate, "the stored pair must still be detected");
        assert_eq!(
            after - before,
            0,
            "has_duplicate must not allocate per call (P3-446)"
        );

        let before = allocations();
        let other = store.has_duplicate("ins-2", "h-1");
        let after = allocations();
        assert!(!other, "an unseen instruction must not be a duplicate");
        assert_eq!(
            after - before,
            0,
            "has_duplicate must not allocate per call (P3-446)"
        );
    }

    #[test]
    fn changed_request_hash_for_same_instruction_halts_and_never_calls_bridge() {
        let total_handle = Rc::new(Cell::new(0usize));
        let counter = Rc::new(CountingBridge::new(Rc::clone(&total_handle), false));
        let attempts: Rc<dyn AttemptStore> = Rc::new(InMemoryAttemptStore::new());
        let gates = shared_gates();
        let bridge: Rc<dyn BridgeCaller> = counter.clone();

        let mut g = ExecutionGate::new(attempts.clone(), gates.clone(), bridge);
        assert_eq!(
            g.execute(&cmd("a-1"), CrashHooks::default()).unwrap(),
            Outcome::Accepted
        );
        assert_eq!(total_handle.get(), 1);
        // Same instruction but different request_hash → contract violation, halted, no call.
        let mutated = Command {
            execution_attempt_id: "a-2".into(),
            instruction_id: "ins-1".into(),
            request_hash: "h-2-changed".into(),
            ..cmd("a-2")
        };
        assert_eq!(
            g.execute(&mutated, CrashHooks::default()).unwrap(),
            Outcome::ContractViolation
        );
        assert_eq!(
            total_handle.get(),
            1,
            "contract violation must never invoke bridge"
        );
        assert_eq!(
            gates.read(PARTITION).unwrap().state,
            GateState::Halted,
            "gate must be halted for reconciliation on contract violation"
        );
    }

    /// Gate store whose durable write always fails — models the store being unreachable
    /// exactly when the gate must be halted.
    struct FailingGateWriteStore {
        row: GateRow,
    }

    impl GateStateStore for FailingGateWriteStore {
        fn read(&self, partition: &str) -> Option<GateRow> {
            (partition == self.row.partition).then(|| self.row.clone())
        }
        fn write(&self, _row: &GateRow) -> Result<()> {
            anyhow::bail!("durable gate write unavailable")
        }
        fn init(&self, _row: &GateRow) -> Result<GateRow> {
            // The fail-closed double refuses the create too: a store that cannot write must not be
            // able to satisfy "the row exists".
            anyhow::bail!("durable gate write unavailable")
        }
    }

    // P3-202: `halt` must not swallow a failed durable write. If the HALT cannot be
    // persisted, the caller must see the error instead of a clean Blocked/ContractViolation/
    // UnknownHalted that implies the partition is fenced when it is still ENABLED.
    #[test]
    fn durable_halt_write_failure_is_propagated_not_swallowed() {
        let total_handle = Rc::new(Cell::new(0usize));
        let counter = Rc::new(CountingBridge::new(Rc::clone(&total_handle), false));
        let attempts: Rc<dyn AttemptStore> = Rc::new(InMemoryAttemptStore::new());
        let gates: Rc<dyn GateStateStore> = Rc::new(FailingGateWriteStore { row: enabled_row() });
        let bridge: Rc<dyn BridgeCaller> = counter.clone();

        let mut g = ExecutionGate::new(attempts.clone(), gates, bridge);
        assert_eq!(
            g.execute(&cmd("a-1"), CrashHooks::default()).unwrap(),
            Outcome::Accepted
        );
        assert_eq!(total_handle.get(), 1);

        // Same instruction, changed hash: the contract violation must halt the gate, but
        // the durable write fails. The failure must surface, not be silently dropped.
        let mutated = Command {
            execution_attempt_id: "a-2".into(),
            request_hash: "h-2-changed".into(),
            ..cmd("a-2")
        };
        let err = g
            .execute(&mutated, CrashHooks::default())
            .expect_err("a failed durable halt must surface, not be reported as a clean outcome");
        assert!(
            err.to_string().contains("durable gate write unavailable"),
            "the durable-halt failure must be the propagated error, got: {err}"
        );
        assert_eq!(
            total_handle.get(),
            1,
            "the contract-violation path must still never invoke the bridge"
        );
    }
    // --------------------------------------------------------------------------
    // FENCE-* / INVARIANT-002 — at most one active owner per partition.
    // A command carrying a stale epoch or fence token (a split-brain survivor, or
    // an owner that was fenced out) must NOT reach the broker. Proof of fencing is
    // a zero invocation of BridgeCaller. This also secures CORR-008 (no phantom
    // state: a fenced/disabled gate cannot produce a successful order/position).
    // --------------------------------------------------------------------------

    #[test]
    fn stale_fence_token_never_issues_a_bridge_call() {
        let total_handle = Rc::new(Cell::new(0usize));
        let counter = Rc::new(CountingBridge::new(Rc::clone(&total_handle), false));
        let attempts: Rc<dyn AttemptStore> = Rc::new(InMemoryAttemptStore::new());
        let gates = shared_gates(); // durable row: epoch=5, fence_token=7, Enabled
        let bridge: Rc<dyn BridgeCaller> = counter.clone();

        let mut g = ExecutionGate::new(attempts.clone(), gates, bridge);
        // Same epoch, but a fence token that is no longer current -> fenced out.
        let fenced = Command {
            gate_fence_token: 8, // stale vs durable row fence_token 7
            ..cmd("a-fence")
        };
        assert_eq!(
            g.execute(&fenced, CrashHooks::default()).unwrap(),
            Outcome::Blocked
        );
        assert_eq!(
            total_handle.get(),
            0,
            "a fenced-out owner must never invoke the bridge"
        );
    }

    #[test]
    fn stale_gate_epoch_never_issues_a_bridge_call() {
        let total_handle = Rc::new(Cell::new(0usize));
        let counter = Rc::new(CountingBridge::new(Rc::clone(&total_handle), false));
        let attempts: Rc<dyn AttemptStore> = Rc::new(InMemoryAttemptStore::new());
        let gates = shared_gates(); // durable row epoch=5
        let bridge: Rc<dyn BridgeCaller> = counter.clone();

        let mut g = ExecutionGate::new(attempts.clone(), gates, bridge);
        // Fence token matches, but the epoch is one generation behind -> fenced out.
        let stale = Command {
            gate_epoch: 4, // < durable epoch 5
            ..cmd("a-epoch")
        };
        assert_eq!(
            g.execute(&stale, CrashHooks::default()).unwrap(),
            Outcome::Blocked
        );
        assert_eq!(
            total_handle.get(),
            0,
            "a stale-epoch owner must never invoke the bridge"
        );
    }

    #[test]
    fn disabled_gate_blocks_with_zero_calls() {
        let total_handle = Rc::new(Cell::new(0usize));
        let counter = Rc::new(CountingBridge::new(Rc::clone(&total_handle), false));
        let attempts: Rc<dyn AttemptStore> = Rc::new(InMemoryAttemptStore::new());
        // Durable row is HALTED even with a matching epoch + fence token.
        let gates_store = InMemoryGateStateStore::new();
        gates_store
            .write(&GateRow {
                partition: PARTITION.into(),
                owner: "worker-1".into(),
                state: GateState::Halted,
                epoch: 5,
                fence_token: 7,
            })
            .unwrap();
        let gates: Rc<dyn GateStateStore> = Rc::new(gates_store);
        let bridge: Rc<dyn BridgeCaller> = counter.clone();

        let mut g = ExecutionGate::new(attempts.clone(), gates, bridge);
        // Matching identity, but the gate is not ENABLED -> no execution.
        assert_eq!(
            g.execute(&cmd("a-halted"), CrashHooks::default()).unwrap(),
            Outcome::Blocked
        );
        assert_eq!(
            total_handle.get(),
            0,
            "a disabled gate must never invoke the bridge (CORR-008 no phantom state)"
        );
    }

    #[test]
    fn matching_fence_and_epoch_emits_exactly_one_call() {
        // Positive control: the identical gate the fence tests use, but with a
        // current epoch + fence, must emit exactly one broker call.
        let total_handle = Rc::new(Cell::new(0usize));
        let counter = Rc::new(CountingBridge::new(Rc::clone(&total_handle), false));
        let attempts: Rc<dyn AttemptStore> = Rc::new(InMemoryAttemptStore::new());
        let gates = shared_gates();
        let bridge: Rc<dyn BridgeCaller> = counter.clone();

        let mut g = ExecutionGate::new(attempts.clone(), gates, bridge);
        assert_eq!(
            g.execute(&cmd("a-ok"), CrashHooks::default()).unwrap(),
            Outcome::Accepted
        );
        assert_eq!(total_handle.get(), 1);
    }
    // --------------------------------------------------------------------------
    // CRASH-ORDER-009 / CRASH-ORDER-005 — broker returns UNKNOWN after acceptance
    // (timeout / delayed ACK / ambiguous outcome). The attempt becomes durable
    // UNKNOWN and must NEVER be auto-retried: a restart returns UnknownHalted
    // with zero additional broker calls.
    // --------------------------------------------------------------------------
    struct UnknownBridgeCount {
        total: Rc<Cell<usize>>,
    }
    impl UnknownBridgeCount {
        fn new(handle: Rc<Cell<usize>>) -> Self {
            Self { total: handle }
        }
    }
    impl BridgeCaller for UnknownBridgeCount {
        fn call(&self, _cmd: &Command) -> Result<BridgeOutcome> {
            self.total.set(self.total.get() + 1);
            Ok(BridgeOutcome {
                kind: OutcomeKind::Unknown,
                broker_order_id: String::new(),
                reason: "broker timeout / ambiguous".into(),
            })
        }
    }

    #[test]
    fn broker_unknown_halts_reconcile_and_never_retries() {
        let total_handle = Rc::new(Cell::new(0usize));
        let counter = Rc::new(UnknownBridgeCount::new(Rc::clone(&total_handle)));
        let attempts: Rc<dyn AttemptStore> = Rc::new(InMemoryAttemptStore::new());
        let gates = shared_gates();
        let bridge: Rc<dyn BridgeCaller> = counter.clone();

        let mut g0 = ExecutionGate::new(attempts.clone(), gates.clone(), bridge.clone());
        assert_eq!(
            g0.execute(&cmd("a-unk"), CrashHooks::default()).unwrap(),
            Outcome::UnknownHalted
        );
        assert_eq!(
            total_handle.get(),
            1,
            "broker UNKNOWN is one call, not a retry"
        );

        // Restart: attempt is durable UNKNOWN -> reconcile, never a second broker call.
        let mut g1 = ExecutionGate::new(attempts.clone(), gates.clone(), bridge.clone());
        assert_eq!(
            g1.execute(&cmd("a-unk"), CrashHooks::default()).unwrap(),
            Outcome::UnknownHalted
        );
        assert_eq!(
            total_handle.get(),
            1,
            "broker UNKNOWN must never be auto-retried (CRASH-ORDER-009/005)"
        );
    }

    // P3-018: a bridge UNKNOWN is ambiguous, so `execute` must persist the durable HALT
    // before it returns `UnknownHalted` — otherwise the partition stays ENABLED and any
    // other instruction can still reach the bridge while reconciliation is pending.
    #[test]
    fn unknown_outcome_persists_the_durable_halt_before_returning() {
        let total_handle = Rc::new(Cell::new(0usize));
        let counter = Rc::new(UnknownBridgeCount::new(Rc::clone(&total_handle)));
        let attempts: Rc<dyn AttemptStore> = Rc::new(InMemoryAttemptStore::new());
        let gates = shared_gates();
        let bridge: Rc<dyn BridgeCaller> = counter.clone();

        let mut g = ExecutionGate::new(attempts.clone(), gates.clone(), bridge.clone());
        assert_eq!(
            g.execute(&cmd("a-unk-halt"), CrashHooks::default())
                .unwrap(),
            Outcome::UnknownHalted
        );
        assert_eq!(total_handle.get(), 1);
        assert_eq!(
            gates.read(PARTITION).unwrap().state,
            GateState::Halted,
            "a bridge UNKNOWN must persist the durable halt before UnknownHalted is returned"
        );

        // The ambiguous attempt fences its partition for every other instruction until reconcile.
        assert_eq!(
            g.execute(&cmd_uid("a-unk-other", "other"), CrashHooks::default())
                .unwrap(),
            Outcome::Blocked
        );
        assert_eq!(
            total_handle.get(),
            1,
            "no other instruction may reach the bridge while the ambiguous attempt awaits reconciliation"
        );
    }

    // --------------------------------------------------------------------------
    // CORR-015 — repeated restart equivalence. Interrupted after the terminal
    // Accepted is durable, then restarted over the same durable stores, yields the
    // SAME authoritative result as an uninterrupted run (same outcome, same phase,
    // exactly one broker call each). Two INDEPENDENT runs compared.
    // --------------------------------------------------------------------------
    fn run_uninterrupted() -> (Outcome, usize, Option<AttemptPhase>) {
        let total_handle = Rc::new(Cell::new(0usize));
        let counter = Rc::new(CountingBridge::new(Rc::clone(&total_handle), false));
        let attempts: Rc<dyn AttemptStore> = Rc::new(InMemoryAttemptStore::new());
        let gates = shared_gates();
        let bridge: Rc<dyn BridgeCaller> = counter.clone();
        let mut e = ExecutionGate::new(attempts.clone(), gates, bridge);
        let res = e.execute(&cmd("a-eq"), CrashHooks::default()).unwrap();
        (
            res,
            total_handle.get(),
            attempts.get("a-eq").map(|a| a.phase),
        )
    }

    #[test]
    fn corr015_restart_after_durable_accepted_matches_uninterrupted() {
        let (outcome_u, total_u, phase_u) = run_uninterrupted();
        let (outcome_r, total_r, phase_r, _state_r) = crash_rerun(
            CrashHooks {
                after_bridge: true,
                ..Default::default()
            },
            false,
        );
        assert_eq!(
            outcome_r, outcome_u,
            "restarted outcome must equal uninterrupted"
        );
        assert_eq!(phase_r, phase_u, "restarted phase must equal uninterrupted");
        assert_eq!(total_r, total_u, "same broker-call count as uninterrupted");
    }

    // --------------------------------------------------------------------------
    // FENCE-009 — epoch monotonicity. After ownership is promoted to a NEW epoch, a
    // command stamped with the OLD epoch is fenced out even when its fence token
    // matches — the old epoch can never supersede the current one.
    // --------------------------------------------------------------------------
    /// A command with caller-unique (instruction_id, request_hash) so distinct logical
    /// orders do not collide in the duplicate index.
    fn cmd_uid(attempt: &str, k: &str) -> Command {
        Command {
            instruction_id: format!("ins-{k}"),
            request_hash: format!("h-{k}"),
            ..cmd(attempt)
        }
    }

    #[test]
    fn fence009_epoch_monotonicity_new_supersedes_old_never() {
        let total_handle = Rc::new(Cell::new(0usize));
        let counter = Rc::new(CountingBridge::new(Rc::clone(&total_handle), false));
        let attempts: Rc<dyn AttemptStore> = Rc::new(InMemoryAttemptStore::new());
        // Durable ownership promoted to epoch 6 (new owner generation).
        let gates_store = InMemoryGateStateStore::new();
        gates_store
            .write(&GateRow {
                partition: PARTITION.into(),
                owner: "worker-2".into(),
                state: GateState::Enabled,
                epoch: 6,
                fence_token: 7,
            })
            .unwrap();
        let gates: Rc<dyn GateStateStore> = Rc::new(gates_store);
        let bridge: Rc<dyn BridgeCaller> = counter.clone();

        let mut g = ExecutionGate::new(attempts.clone(), gates, bridge);
        // New epoch (6) executes as the current owner.
        let fresh = Command {
            gate_epoch: 6,
            ..cmd_uid("a-mon-1", "m1")
        };
        assert_eq!(
            g.execute(&fresh, CrashHooks::default()).unwrap(),
            Outcome::Accepted
        );
        assert_eq!(total_handle.get(), 1);
        // Old epoch (5) is fenced out even though the durable fence token matches.
        let old = cmd_uid("a-mon-2", "m2"); // cmd() default gate_epoch = 5
        assert_eq!(
            g.execute(&old, CrashHooks::default()).unwrap(),
            Outcome::Blocked
        );
        assert_eq!(
            total_handle.get(),
            1,
            "old epoch must never supersede the current epoch (FENCE-009)"
        );
    }

    // --------------------------------------------------------------------------
    // FENCE-012 — executor restart during active ownership. A fresh engine over the
    // SAME durable gate store reconstructs the fence token / owner and can keep
    // executing: ownership is reconstructed from durable state, not re-granted blank.
    // --------------------------------------------------------------------------
    #[test]
    fn fence012_ownership_and_fence_token_survive_restart() {
        let total_handle = Rc::new(Cell::new(0usize));
        let counter = Rc::new(CountingBridge::new(Rc::clone(&total_handle), false));
        let attempts: Rc<dyn AttemptStore> = Rc::new(InMemoryAttemptStore::new());
        let gates_store = InMemoryGateStateStore::new();
        gates_store.write(&enabled_row()).unwrap(); // owner worker-1, epoch 5, fence token 7
        let gates: Rc<dyn GateStateStore> = Rc::new(gates_store);
        let bridge: Rc<dyn BridgeCaller> = counter.clone();

        // First engine exercises ownership.
        let mut g0 = ExecutionGate::new(attempts.clone(), gates.clone(), bridge.clone());
        assert_eq!(
            g0.execute(&cmd_uid("a-f12", "f1"), CrashHooks::default())
                .unwrap(),
            Outcome::Accepted
        );
        assert_eq!(total_handle.get(), 1);

        // Restart over the same durable gate store: ownership reconstructed, token intact.
        let mut g1 = ExecutionGate::new(attempts.clone(), gates.clone(), bridge.clone());
        assert_eq!(
            g1.execute(&cmd_uid("a-f12-2", "f2"), CrashHooks::default())
                .unwrap(),
            Outcome::Accepted
        );
        assert_eq!(total_handle.get(), 2);
        let row = gates.read(PARTITION).unwrap();
        assert_eq!(row.fence_token, 7, "fence token must survive restart");
        assert_eq!(row.owner, "worker-1", "owner must survive restart");
    }
    // --------------------------------------------------------------------------
    // CRASH-ORDER-010 — reconciliation discovers an already-accepted order. A durable
    // Accepted attempt must resolve to Accepted with ZERO new bridge calls on replay.
    // --------------------------------------------------------------------------
    #[test]
    fn crash_order010_reconcile_discovers_accepted_no_new_call() {
        let total_handle = Rc::new(Cell::new(0usize));
        let counter = Rc::new(CountingBridge::new(Rc::clone(&total_handle), false));
        let attempts: Rc<dyn AttemptStore> = Rc::new(InMemoryAttemptStore::new());
        let gates = shared_gates();
        let bridge: Rc<dyn BridgeCaller> = counter.clone();

        // Reconciliation state: the order was already accepted prior to restart.
        attempts
            .put(&Attempt::new(
                "a-c10",
                "ins-1",
                "h-1",
                "E-a-c10",
                AttemptPhase::Accepted,
            ))
            .unwrap();

        let mut g = ExecutionGate::new(attempts.clone(), gates, bridge);
        assert_eq!(
            g.execute(&cmd("a-c10"), CrashHooks::default()).unwrap(),
            Outcome::Accepted
        );
        assert_eq!(
            total_handle.get(),
            0,
            "reconcile must never re-issue a bridge call for an already-accepted order"
        );
    }

    // --------------------------------------------------------------------------
    // FENCE-010 — broker call during ownership loss is blocked/fenced. After ownership
    // is promoted to a new epoch, a command the old owner still holds (stale epoch) is
    // fenced out: zero broker calls.
    // --------------------------------------------------------------------------
    #[test]
    fn fence010_broker_call_during_ownership_loss_is_fenced() {
        let total_handle = Rc::new(Cell::new(0usize));
        let counter = Rc::new(CountingBridge::new(Rc::clone(&total_handle), false));
        let attempts: Rc<dyn AttemptStore> = Rc::new(InMemoryAttemptStore::new());
        // Ownership already moved to a NEW epoch/fence before this command was issued.
        let gates_store = InMemoryGateStateStore::new();
        gates_store
            .write(&GateRow {
                partition: PARTITION.into(),
                owner: "worker-2".into(),
                state: GateState::Enabled,
                epoch: 6,
                fence_token: 8,
            })
            .unwrap();
        let gates: Rc<dyn GateStateStore> = Rc::new(gates_store);
        let bridge: Rc<dyn BridgeCaller> = counter.clone();

        let mut g = ExecutionGate::new(attempts.clone(), gates, bridge);
        // cmd() stamps the OLD epoch (5) / fence (7) — a stale owner's in-flight call.
        assert_eq!(
            g.execute(&cmd("f10-1"), CrashHooks::default()).unwrap(),
            Outcome::Blocked
        );
        assert_eq!(
            total_handle.get(),
            0,
            "broker call during ownership loss must be fenced, never emitted (FENCE-010)"
        );
    }

    // --------------------------------------------------------------------------
    // CORR-012 / INVARIANT-010 — correlation completeness. Each money-moving action
    // carries its own instruction_id as the correlation key: two actions with IDENTICAL
    // content hash but DIFFERENT instructions are NOT collapsed — both emit, each
    // correlated to its own instruction.
    // --------------------------------------------------------------------------
    #[test]
    fn corr012_identical_request_hash_distinct_instructions_stay_correlated() {
        let total_handle = Rc::new(Cell::new(0usize));
        let counter = Rc::new(CountingBridge::new(Rc::clone(&total_handle), false));
        let attempts: Rc<dyn AttemptStore> = Rc::new(InMemoryAttemptStore::new());
        let gates = shared_gates();
        let bridge: Rc<dyn BridgeCaller> = counter.clone();
        let mut g = ExecutionGate::new(attempts.clone(), gates, bridge);

        let mut c1 = cmd("c12-1");
        c1.instruction_id = "ins-A".into();
        c1.request_hash = "HASH".into();
        let mut c2 = cmd("c12-2");
        c2.instruction_id = "ins-B".into();
        c2.request_hash = "HASH".into(); // identical content hash

        assert_eq!(
            g.execute(&c1, CrashHooks::default()).unwrap(),
            Outcome::Accepted
        );
        assert_eq!(
            g.execute(&c2, CrashHooks::default()).unwrap(),
            Outcome::Accepted
        );
        assert_eq!(
            total_handle.get(),
            2,
            "same request hash on different instructions = two correlated money-moving actions"
        );
    }

    // --------------------------------------------------------------------------
    // NET-PART (offline semantic evidence) — dependency outage (Unknown) halts with
    // exactly one money-moving call; recovery must NEVER auto-retry the uncertain
    // instruction. Proves the once-only + halt semantics an in-process bridge outage
    // must exhibit (what a real network partition must also guarantee).
    // --------------------------------------------------------------------------
    struct OutageThenRecoverBridge {
        total: Rc<Cell<usize>>,
        down: Cell<bool>,
    }
    impl BridgeCaller for OutageThenRecoverBridge {
        fn call(&self, _cmd: &Command) -> Result<BridgeOutcome> {
            self.total.set(self.total.get() + 1);
            if self.down.get() {
                Ok(BridgeOutcome {
                    kind: OutcomeKind::Unknown,
                    broker_order_id: String::new(),
                    reason: "dependency unreachable (partition)".into(),
                })
            } else {
                Ok(BridgeOutcome {
                    kind: OutcomeKind::Accepted,
                    broker_order_id: "B-np".into(),
                    reason: "ok".into(),
                })
            }
        }
    }

    #[test]
    fn netpart_dependency_outage_halts_and_never_duplicates() {
        let total_handle = Rc::new(Cell::new(0usize));
        let bridge = Rc::new(OutageThenRecoverBridge {
            total: Rc::clone(&total_handle),
            down: Cell::new(true),
        });
        let attempts: Rc<dyn AttemptStore> = Rc::new(InMemoryAttemptStore::new());
        let gates = shared_gates();
        let bridge_rc: Rc<dyn BridgeCaller> = bridge.clone();

        // Outage window: dependency unreachable -> Unknown -> UnknownHalted, one call.
        let mut g = ExecutionGate::new(attempts.clone(), gates.clone(), bridge_rc.clone());
        assert_eq!(
            g.execute(&cmd("np-1"), CrashHooks::default()).unwrap(),
            Outcome::UnknownHalted
        );
        assert_eq!(total_handle.get(), 1);
        // Re-submitted while still down: still UnknownHalted, no retry call.
        assert_eq!(
            g.execute(&cmd("np-1"), CrashHooks::default()).unwrap(),
            Outcome::UnknownHalted
        );
        assert_eq!(total_handle.get(), 1);

        // Dependency heals: the uncertain instruction is reconciled, never auto-retried.
        bridge.down.set(false);
        assert_eq!(
            g.execute(&cmd("np-1"), CrashHooks::default()).unwrap(),
            Outcome::UnknownHalted
        );
        assert_eq!(
            total_handle.get(),
            1,
            "exactly one money-moving call across outage + recovery (NET-PART once-only)"
        );
    }
}
