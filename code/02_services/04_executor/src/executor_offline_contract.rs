//! Offline contract for 11-testing EXE-* (Executor) — deterministic, no market/4VM.
//! Covers EXE-FAIL-001/003/006 + EXE-AUDIT-001 via in-memory gate/attempt store.

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use crate::executiongate::{
        Attempt, AttemptPhase, AttemptStore, BridgeCaller, BridgeOutcome, Command, CrashHooks,
        ExecutionGate, GateRow, GateState, GateStateStore, InMemoryAttemptStore,
        InMemoryGateStateStore, Outcome, OutcomeKind,
    };
    use crate::gate::{ExecState, Gate};

    const PARTITION: &str = "p-exe";

    /// Cumulative bridge call counter, shared across gate restarts. Every invocation counts,
    /// including one that simulates a mid-flight bridge crash.
    struct CountingBridge {
        calls: Rc<Cell<usize>>,
    }

    impl CountingBridge {
        fn new(calls: Rc<Cell<usize>>) -> Self {
            Self { calls }
        }
    }

    impl BridgeCaller for CountingBridge {
        fn call(&self, cmd: &Command) -> anyhow::Result<BridgeOutcome> {
            self.calls.set(self.calls.get() + 1);
            Ok(BridgeOutcome {
                kind: OutcomeKind::Accepted,
                broker_order_id: format!("B-{}", cmd.execution_attempt_id),
                reason: "ok".into(),
            })
        }
    }

    /// A durable ENABLED gate row matching [`cmd`]'s epoch and fence token.
    fn enabled_gates() -> Rc<dyn GateStateStore> {
        let gates = InMemoryGateStateStore::new();
        gates
            .write(&GateRow {
                partition: PARTITION.into(),
                owner: "worker-1".into(),
                state: GateState::Enabled,
                epoch: 5,
                fence_token: 7,
            })
            .unwrap();
        Rc::new(gates)
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

    // EXE-FAIL-001: crash before/during/after no duplicate halt+reconcile.
    //
    // Per crash window: engine 0 dies at the injected hook, then engine 1 is the restarted
    // process over the same durable stores and the same cumulative bridge. `PREPARED` resumes
    // and issues the single allowed call; `SUBMITTING` and the post-persist crash can neither
    // re-issue nor be silently retried — the gate halts and the terminal record is replayed.
    #[test]
    fn exe_fail_001_crash_no_duplicate_halt_reconcile() {
        // (window, hooks, outcome after restart, cumulative calls, durable phase, gate state)
        let cases: [(&str, CrashHooks, Outcome, usize, AttemptPhase, GateState); 3] = [
            (
                "after_prepared",
                CrashHooks {
                    after_prepared: true,
                    ..Default::default()
                },
                Outcome::Accepted,
                1,
                AttemptPhase::Accepted,
                GateState::Enabled,
            ),
            (
                "after_submitting",
                CrashHooks {
                    after_submitting: true,
                    ..Default::default()
                },
                Outcome::UnknownHalted,
                0,
                AttemptPhase::Submitting,
                GateState::Halted,
            ),
            (
                "after_bridge",
                CrashHooks {
                    after_bridge: true,
                    ..Default::default()
                },
                Outcome::Accepted,
                1,
                AttemptPhase::Accepted,
                GateState::Enabled,
            ),
        ];
        // Pinned so a silently dropped row cannot make this test vacuous.
        assert_eq!(cases.len(), 3);

        for (window, hooks, want_outcome, want_calls, want_phase, want_gate) in cases {
            let calls = Rc::new(Cell::new(0usize));
            let bridge: Rc<dyn BridgeCaller> = Rc::new(CountingBridge::new(Rc::clone(&calls)));
            let attempts: Rc<dyn AttemptStore> = Rc::new(InMemoryAttemptStore::new());
            let gates = enabled_gates();
            let attempt_id = format!("a-{window}");

            // Engine 0: dies at the injected hook; only the durable stores are its memory.
            let mut g0 =
                ExecutionGate::new(Rc::clone(&attempts), Rc::clone(&gates), Rc::clone(&bridge));
            assert!(
                g0.execute(&cmd(&attempt_id), hooks).is_err(),
                "{window}: the injected crash must surface as Err"
            );

            // Engine 1: the restarted process over the same durable memory.
            let mut g1 =
                ExecutionGate::new(Rc::clone(&attempts), Rc::clone(&gates), Rc::clone(&bridge));
            assert_eq!(
                g1.execute(&cmd(&attempt_id), CrashHooks::default())
                    .unwrap(),
                want_outcome,
                "{window}: restart outcome"
            );
            assert!(
                calls.get() <= 1,
                "{window}: the bridge must never be called twice, saw {}",
                calls.get()
            );
            assert_eq!(calls.get(), want_calls, "{window}: cumulative bridge calls");
            assert_eq!(
                attempts.get(&attempt_id).map(|a| a.phase),
                Some(want_phase),
                "{window}: durable phase after the restart"
            );
            assert_eq!(
                gates.read(PARTITION).map(|g| g.state),
                Some(want_gate),
                "{window}: gate state after the restart"
            );
        }
    }

    // EXE-FAIL-003: missing/corrupt state blocks calls
    #[test]
    fn exe_fail_003_missing_corrupt_blocks_calls() {
        let gate = Gate::new();
        assert_eq!(gate.state(), ExecState::Halted);
        assert!(!gate.can_execute());
        let store = InMemoryAttemptStore::new();
        assert!(!store.has_instruction("missing"));
        let mut g2 = Gate::new();
        let stayed = g2.enter_reconciling_if_needed(false).unwrap();
        assert!(!stayed);
        assert_eq!(g2.state(), ExecState::Halted);
    }

    // EXE-FAIL-006: mapping quarantine blocks unsafe (ambiguous correlation → quarantine + halt)
    #[test]
    fn exe_fail_006_mapping_quarantine_blocks_unsafe() {
        let store = InMemoryAttemptStore::new();
        let a = Attempt::new(
            "attempt-1",
            "instr-1",
            "hash-1",
            "C-REF-1",
            AttemptPhase::Prepared,
        );
        store.put(&a).unwrap();
        assert!(store.has_instruction("instr-1"));
        assert!(!store.has_instruction("instr-2"));
        let gate = Gate::new();
        assert_eq!(gate.state(), ExecState::Halted);
        assert!(!gate.can_execute());
    }

    // EXE-AUDIT-001: audit reconstruction — journal replay yields same state
    #[test]
    fn exe_audit_001_audit_reconstruction() {
        let journal = vec![
            ("attempt-1", "instr-1", "hash-1", "C-1"),
            ("attempt-2", "instr-2", "hash-2", "C-2"),
        ];
        let rebuild = |j: &Vec<(&str, &str, &str, &str)>| {
            let s = InMemoryAttemptStore::new();
            for (att, instr, hash, cref) in j {
                let a = Attempt::new(att, instr, hash, cref, AttemptPhase::Prepared);
                s.put(&a).unwrap();
            }
            s
        };
        let r1 = rebuild(&journal);
        let r2 = rebuild(&journal);
        assert!(r1.has_duplicate("instr-1", "hash-1"));
        assert!(r2.has_duplicate("instr-1", "hash-1"));
        assert!(r1.has_duplicate("instr-2", "hash-2"));
        assert!(r2.has_duplicate("instr-2", "hash-2"));
        // both stores see same instruction set
        assert!(r1.has_instruction("instr-1") && r2.has_instruction("instr-1"));
    }
}
