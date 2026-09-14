//! Crash-exactly-once composite proof — plan Task B2
//! (docs/plans/2026-08-25-live-readiness-unified-plan.md, Phase B).
//!
//! The unit suite in `src/executiongate.rs` covers each crash hook individually. This
//! integration test walks the FULL plan checklist in one scenario against shared durable
//! stores and a cumulative bridge counter — the properties that must hold across a real
//! `kill -9` between "intent accepted durably" and "bridge ack recorded":
//!
//! 1. The bridge sees the order EXACTLY ONCE across crash + restart (mid-flight crash:
//!    broker got it, our process died before recording).
//! 2. Exactly ONE durable attempt record exists for the instruction after recovery.
//! 3. Recovery from the ambiguous window HALTS the partition (`UnknownHalted`).
//! 4. The PRE-CRASH command is quarantined, never re-issued: replaying it during recovery
//!    returns `UnknownHalted` (not `Blocked` — the ambiguous-window short-circuit runs
//!    before any gate or fence check) with no new bridge call, and the partition re-halts.
//! 5. A fresh command under the new epoch/fence succeeds exactly once with its own
//!    broker id; the durable store converges to one record per attempt.
//!
//! Run: `cargo test --offline --test crash_exactly_once`

use nautilus_execution_service::executiongate::{
    AttemptPhase, AttemptStore, BridgeCaller, BridgeOutcome, Command, CrashHooks, ExecutionGate,
    GateRow, GateState, GateStateStore, InMemoryAttemptStore, InMemoryGateStateStore, Outcome,
};
use std::cell::{Cell, RefCell};
use std::rc::Rc;

const PARTITION: &str = "p-b2";

/// Cumulative counting bridge: counts EVERY call including mid-flight crashes.
struct CountingBridge {
    total: Rc<Cell<usize>>,
    crash: Cell<bool>,
}

impl CountingBridge {
    fn new(total: Rc<Cell<usize>>) -> Self {
        Self {
            total,
            crash: Cell::new(false),
        }
    }
}

impl BridgeCaller for CountingBridge {
    fn call(&self, cmd: &Command) -> anyhow::Result<BridgeOutcome> {
        self.total.set(self.total.get() + 1); // count even when it crashes mid-flight
        if self.crash.get() {
            anyhow::bail!("simulated kill -9 inside the bridge call");
        }
        Ok(BridgeOutcome {
            kind: nautilus_execution_service::executiongate::OutcomeKind::Accepted,
            broker_order_id: format!("B-{}", cmd.execution_attempt_id),
            reason: "ok".into(),
        })
    }
}

/// Recording decorator over the durable attempt store: tracks unique inserted ids so the
/// test can assert "exactly one record per instruction" without touching prod code.
struct RecordingAttempts {
    inner: Rc<InMemoryAttemptStore>,
    ids: RefCell<Vec<String>>,
}

impl RecordingAttempts {
    fn new(inner: Rc<InMemoryAttemptStore>) -> Rc<Self> {
        Rc::new(Self {
            inner,
            ids: RefCell::new(Vec::new()),
        })
    }
    fn unique_ids(&self) -> usize {
        self.ids.borrow().len()
    }
}

impl AttemptStore for RecordingAttempts {
    fn get(&self, attempt_id: &str) -> Option<nautilus_execution_service::executiongate::Attempt> {
        self.inner.get(attempt_id)
    }
    fn put(
        &self,
        attempt: &nautilus_execution_service::executiongate::Attempt,
    ) -> anyhow::Result<()> {
        let mut ids = self.ids.borrow_mut();
        if !ids.contains(&attempt.attempt_id) {
            ids.push(attempt.attempt_id.clone());
        }
        drop(ids);
        self.inner.put(attempt)
    }
    fn has_duplicate(&self, instruction_id: &str, request_hash: &str) -> bool {
        self.inner.has_duplicate(instruction_id, request_hash)
    }
    fn has_instruction(&self, instruction_id: &str) -> bool {
        self.inner.has_instruction(instruction_id)
    }
}

fn command(attempt: &str, instruction: &str, hash: &str, epoch: u64, fence: u64) -> Command {
    Command {
        execution_attempt_id: attempt.into(),
        account_scope_id: "acc".into(),
        instruction_id: instruction.into(),
        execution_partition_id: PARTITION.into(),
        request_hash: hash.into(),
        client_order_ref: format!("E-{attempt}"),
        gate_epoch: epoch,
        gate_fence_token: fence,
    }
}

#[test]
fn crash_exactly_once_composite() {
    let calls = Rc::new(Cell::new(0usize));
    let bridge = Rc::new(CountingBridge::new(Rc::clone(&calls)));
    let attempts_rec = RecordingAttempts::new(Rc::new(InMemoryAttemptStore::new()));
    let gates = Rc::new(InMemoryGateStateStore::new());

    // Operator enable: epoch 5 / fence 7.
    gates
        .write(&GateRow {
            partition: PARTITION.into(),
            owner: "worker-1".into(),
            state: GateState::Enabled,
            epoch: 5,
            fence_token: 7,
        })
        .unwrap();

    let attempts_dyn: Rc<dyn AttemptStore> = Rc::clone(&attempts_rec) as Rc<dyn AttemptStore>;
    let gates_dyn: Rc<dyn GateStateStore> = Rc::clone(&gates) as Rc<dyn GateStateStore>;

    // ---- Leg A: kill -9 INSIDE the bridge call (intent durable, ack lost). ----
    let mut g0 = ExecutionGate::new(
        Rc::clone(&attempts_dyn),
        Rc::clone(&gates_dyn),
        Rc::clone(&bridge) as Rc<dyn BridgeCaller>,
    );
    bridge.crash.set(true);
    let crashed = g0.execute(&command("a-1", "ins-1", "h-1", 5, 7), CrashHooks::default());
    assert!(
        crashed.is_err(),
        "mid-bridge kill must surface as process death"
    );
    assert_eq!(
        calls.get(),
        1,
        "bridge saw the order exactly once (broker has it)"
    );

    // ---- Leg B: restart on the SAME durable memory; replay the same attempt. ----
    bridge.crash.set(false);
    let mut g1 = ExecutionGate::new(
        Rc::clone(&attempts_dyn),
        Rc::clone(&gates_dyn),
        Rc::clone(&bridge) as Rc<dyn BridgeCaller>,
    );
    let resumed = g1
        .execute(&command("a-1", "ins-1", "h-1", 5, 7), CrashHooks::default())
        .expect("replay itself must not crash");
    assert_eq!(
        resumed,
        Outcome::UnknownHalted,
        "ambiguous-window restart must halt, never re-issue"
    );
    assert_eq!(calls.get(), 1, "NO second bridge call after restart");
    assert_eq!(
        attempts_rec.unique_ids(),
        1,
        "exactly ONE durable attempt record for this order"
    );
    let stored = attempts_dyn.get("a-1").expect("attempt must be durable");
    assert_eq!(stored.phase, AttemptPhase::Submitting);
    assert_eq!(
        stored.broker_order_id, None,
        "no recorded broker id yet — reconcile, never guess"
    );
    assert_eq!(
        gates.read(PARTITION).unwrap().state,
        GateState::Halted,
        "partition halted by the ambiguous recovery"
    );

    // ---- Leg C: operator replays post-bump; quarantine holds, partition re-halts. ----
    // Protocol discovery (B2): the ambiguous-window attempt short-circuits to
    // UnknownHalted BEFORE any gate/fence check — an unresolved broker outcome can never
    // be fenced away by an epoch bump; it stays quarantined for reconciliation. This is
    // STRONGER than plain Blocked: even a matching new epoch cannot resurrect it.
    //
    // For that claim to be discriminative the gate must actually BE enabled at the epoch
    // and fence the replay carries — otherwise a gate-first implementation would compare
    // against a stale halted row and the assertion would hold vacuously. Enable 6/8 first.
    gates
        .write(&GateRow {
            partition: PARTITION.into(),
            owner: "worker-1".into(),
            state: GateState::Enabled,
            epoch: 6,
            fence_token: 8,
        })
        .unwrap();
    assert_eq!(
        gates.read(PARTITION).unwrap().state,
        GateState::Enabled,
        "precondition: the replay runs against a gate that WOULD accept this epoch/fence"
    );
    let stale = g1
        .execute(&command("a-1", "ins-1", "h-1", 6, 8), CrashHooks::default())
        .expect("stale replay must terminate, not crash");
    assert_eq!(
        stale,
        Outcome::UnknownHalted,
        "unresolved attempt stays quarantined even under a fresh epoch"
    );
    assert_eq!(calls.get(), 1, "still exactly one bridge call ever");
    assert_eq!(
        gates.read(PARTITION).unwrap().state,
        GateState::Halted,
        "replay of the unresolved attempt re-halts the partition"
    );

    // ---- Leg C2: human reconciliation resolves the unknown, then re-enables. ----
    // Mirror of crash_order010: the reconcile path queries the broker (REST reads) and
    // discovers the order was accepted — the durable record is corrected by EVIDENCE,
    // never guessed. The re-enable then bumps the epoch once more (6 -> 7, fence 8 -> 9).
    let mut reconciled = attempts_dyn.get("a-1").unwrap();
    reconciled.phase = AttemptPhase::Accepted;
    reconciled.broker_order_id = Some("B-a-1".into());
    attempts_dyn.put(&reconciled).unwrap();
    assert_eq!(calls.get(), 1, "reconciliation is read-only vs the broker");
    gates
        .write(&GateRow {
            partition: PARTITION.into(),
            owner: "worker-1".into(),
            state: GateState::Enabled,
            epoch: 7,
            fence_token: 9,
        })
        .unwrap();

    // ---- Leg D: fresh order under the new epoch/fence succeeds exactly once. ----
    let fresh = g1
        .execute(&command("a-2", "ins-2", "h-2", 7, 9), CrashHooks::default())
        .expect("fresh post-recovery order must not crash");
    assert_eq!(fresh, Outcome::Accepted);
    assert_eq!(calls.get(), 2, "second DISTINCT order = second call");
    let fresh_attempt = attempts_dyn.get("a-2").expect("fresh attempt durable");
    assert_eq!(
        fresh_attempt.broker_order_id.as_deref(),
        Some("B-a-2"),
        "accepted fresh order carries its own broker id"
    );
    assert_eq!(
        attempts_rec.unique_ids(),
        2,
        "durable store converged to one record per attempt"
    );
}
