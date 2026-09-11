package com.trading.common.schema.execution;

import static org.assertj.core.api.Assertions.assertThat;

import com.trading.common.model.GateState;
import com.trading.common.schema.execution.BridgeCaller.OutcomeKind;
import com.trading.common.schema.execution.ExecutionCommandGate.Command;
import com.trading.common.schema.execution.ExecutionCommandGate.CrashHooks;
import com.trading.common.schema.execution.ExecutionCommandGate.Outcome;
import com.trading.common.schema.execution.GateStateStore.FenceResult;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * The T5 crash-window suite (plan step 7 + checklist): inject a crash at every
 * window of the durable command protocol and assert restart never issues a
 * second fake-broker place order and begins with reconciliation required for
 * any attempt left in SUBMITTING or UNKNOWN. Also covers stale-owner
 * interleaving (concurrent fence acquisition) so no interleaving duplicates a
 * command.
 */
class ExecutionCommandGateCrashWindowTest {

    private static final long NOW = 1_700_000_000_000L;
    private static final String PARTITION = "p-1";

    /** Counting bridge shared across the (re)started engines — cumulative call count. */
    private static final class CountingBridge implements BridgeCaller {
        int total;
        boolean crash;

        @Override
        public BridgeOutcome call(Command c) {
            total++; // count the call attempt even when it crashes mid-flight
            if (crash) {
                throw new RuntimeException("simulated bridge crash");
            }
            return new BridgeOutcome(OutcomeKind.ACCEPTED, "B-" + c.executionAttemptId(), "ok");
        }
    }

    /** Throws on a selected crash point to simulate process death. */
    private static final class BoomHooks implements CrashHooks {
        final String point;
        BoomHooks(String point) {
            this.point = point;
        }
        private void boom(String p) {
            if (p.equals(point)) {
                throw new RuntimeException("crash at " + p);
            }
        }
        @Override public void afterPrepared() { boom("AFTER_PREPARED"); }
        @Override public void afterSubmitting() { boom("AFTER_SUBMITTING"); }
        @Override public void beforeBridge() { boom("BEFORE_BRIDGE"); }
        @Override public void afterBridge() { boom("AFTER_BRIDGE"); }
    }

    private static GateRow enabledRow() {
        return new GateRow(PARTITION, "acc", GateState.ENABLED, 5, "enabled", "ev-1",
                "saurabh", null, "ev-1", "worker-1", 7, NOW - 1000, NOW + 100_000, null);
    }

    private static Command cmd(String attemptId) {
        return new Command(attemptId, "acc", "ins-1", "act-" + attemptId, PARTITION, "h-1",
                "E-" + attemptId, 5, 7, "ev-1");
    }

    /** Runs one command against a crashing engine, then a fresh engine over the same durable store. */
    private static Restart rerun(CrashHooks hooks, CountingBridge bridge,
                                 InMemoryAttemptStore attempts, InMemoryGateStateStore gates,
                                 OutcomeKind bridgeResult) {
        bridge.crash = bridgeResult == OutcomeKind.UNKNOWN;
        ExecutionCommandGate g0 = new ExecutionCommandGate(attempts, gates, bridge, "worker-1",
                () -> NOW, null, hooks);
        try {
            g0.execute(cmd("a-1"));
        } catch (RuntimeException expected) {
            // process death at the injected point
        }
        // restart: durable state is the only memory
        bridge.crash = false;
        ExecutionCommandGate g1 = new ExecutionCommandGate(attempts, gates, bridge, "worker-1",
                () -> NOW, null);
        ExecutionCommandGate.Result r = g1.execute(cmd("a-1"));
        return new Restart(r, attempts, gates, bridge);
    }

    private record Restart(ExecutionCommandGate.Result result, InMemoryAttemptStore attempts,
                           InMemoryGateStateStore gates, CountingBridge bridge) {}

    private static InMemoryAttemptStore attempts() {
        return new InMemoryAttemptStore(new AtomicInteger()::incrementAndGet);
    }
    private static InMemoryGateStateStore gates() {
        InMemoryGateStateStore g = new InMemoryGateStateStore(Set.of("saurabh"));
        g.install(enabledRow());
        return g;
    }

    private static void assertMaxOneCall(Restart rz) {
        assertThat(rz.bridge.total)
                .as("fake broker must never see a duplicate place order")
                .isLessThanOrEqualTo(1);
    }

    @Test
    void crashBeforeDurablePrepareIssuesExactlyOneCall() {
        InMemoryAttemptStore a = attempts();
        InMemoryGateStateStore g = gates();
        CountingBridge b = new CountingBridge();
        // Nothing durable happened before the crash (equivalent to a no-op first run).
        ExecutionCommandGate g1 = new ExecutionCommandGate(a, g, b, "worker-1", () -> NOW, null);
        assertThat(g1.execute(cmd("a-1")).outcome()).isEqualTo(Outcome.ACCEPTED);
        assertThat(b.total).isEqualTo(1);
        assertMaxOneCall(new Restart(null, a, g, b));
    }

    @Test
    void crashAfterPreparedResumesWithExactlyOneCall() {
        InMemoryAttemptStore a = attempts();
        InMemoryGateStateStore g = gates();
        CountingBridge b = new CountingBridge();
        Restart rz = rerun(new BoomHooks("AFTER_PREPARED"), b, a, g, OutcomeKind.ACCEPTED);
        // PREPARED is durable but no bridge call happened -> resume completes the call once.
        assertThat(rz.bridge.total).isEqualTo(1);
        assertMaxOneCall(rz);
        assertThat(rz.result.outcome()).isEqualTo(Outcome.ACCEPTED);
    }

    @Test
    void crashAfterSubmittingHaltsWithZeroDuplicateCalls() {
        InMemoryAttemptStore a = attempts();
        InMemoryGateStateStore g = gates();
        CountingBridge b = new CountingBridge();
        Restart rz = rerun(new BoomHooks("AFTER_SUBMITTING"), b, a, g, OutcomeKind.ACCEPTED);
        // SUBMITTING is durable and the bridge may or may not have been reached:
        // restart must halt and require reconciliation, never issue a second call.
        assertThat(rz.bridge.total).isZero();
        assertMaxOneCall(rz);
        assertThat(rz.result.outcome()).isEqualTo(Outcome.UNKNOWN_HALTED);
        assertThat(rz.attempts.attemptById("a-1").phase()).isEqualTo(AttemptRecord.PHASE_SUBMITTING);
        assertThat(rz.gates.read(PARTITION).state()).isEqualTo(GateState.HALTED);
    }

    @Test
    void crashBeforeBridgeHaltsWithZeroDuplicateCalls() {
        InMemoryAttemptStore a = attempts();
        InMemoryGateStateStore g = gates();
        CountingBridge b = new CountingBridge();
        Restart rz = rerun(new BoomHooks("BEFORE_BRIDGE"), b, a, g, OutcomeKind.ACCEPTED);
        assertThat(rz.bridge.total).isZero();
        assertMaxOneCall(rz);
        assertThat(rz.result.outcome()).isEqualTo(Outcome.UNKNOWN_HALTED);
    }

    @Test
    void crashDuringBridgeHaltsWithNoSecondCall() {
        InMemoryAttemptStore a = attempts();
        InMemoryGateStateStore g = gates();
        CountingBridge b = new CountingBridge();
        // bridge crash = UNKNOWN: exactly one place attempt, then halt + reconcile.
        Restart rz = rerun(new BoomHooks("no-crash"), b, a, g, OutcomeKind.UNKNOWN);
        assertThat(rz.bridge.total).isEqualTo(1);
        assertMaxOneCall(rz);
        // The first engine already persisted UNKNOWN and halted the scope, so on
        // restart the command is BLOCKED by the halted gate — no second call ever.
        assertThat(rz.result.outcome()).isEqualTo(Outcome.BLOCKED);
        assertThat(rz.attempts.attemptById("a-1").phase()).isEqualTo(AttemptRecord.PHASE_UNKNOWN);
        assertThat(rz.gates.read(PARTITION).state()).isEqualTo(GateState.HALTED);
    }

    @Test
    void crashAfterBridgeButBeforeEvidenceHaltsWithNoSecondCall() {
        InMemoryAttemptStore a = attempts();
        InMemoryGateStateStore g = gates();
        CountingBridge b = new CountingBridge();
        // bridge already called once; evidence not durable -> restart sees SUBMITTING
        // and must not place again.
        Restart rz = rerun(new BoomHooks("AFTER_BRIDGE"), b, a, g, OutcomeKind.ACCEPTED);
        assertThat(rz.bridge.total).isEqualTo(1);
        assertMaxOneCall(rz);
        assertThat(rz.result.outcome()).isEqualTo(Outcome.UNKNOWN_HALTED);
        assertThat(rz.gates.read(PARTITION).state()).isEqualTo(GateState.HALTED);
    }

    @Test
    void crashAfterEvidenceIsIdempotentDuplicate() {
        InMemoryAttemptStore a = attempts();
        InMemoryGateStateStore g = gates();
        CountingBridge b = new CountingBridge();
        // Full happy path first -> terminal ACCEPTED is durable.
        ExecutionCommandGate gate = new ExecutionCommandGate(a, g, b, "worker-1", () -> NOW, null);
        assertThat(gate.execute(cmd("a-1")).outcome()).isEqualTo(Outcome.ACCEPTED);
        assertThat(b.total).isEqualTo(1);
        // "Restart" with a fresh engine: terminal attempt -> DUPLICATE, no new call.
        ExecutionCommandGate gate2 = new ExecutionCommandGate(a, g, b, "worker-1", () -> NOW, null);
        assertThat(gate2.execute(cmd("a-1")).outcome()).isEqualTo(Outcome.DUPLICATE);
        assertThat(b.total).isEqualTo(1);
    }

    // ---- stale-owner interleaving ----

    @Test
    void concurrentOwnerCannotAcquireWhileLeaseHeld() {
        InMemoryGateStateStore gates = new InMemoryGateStateStore(Set.of("saurabh"));
        gates.install(new GateRow(PARTITION, "acc", GateState.HALTED, 0, "boot", null,
                null, null, null, null, 0, null, null, null));
        // worker-1 acquires a live lease
        FenceResult first = gates.acquire(PARTITION, "worker-1", 60_000, NOW);
        assertThat(first.conflict()).isFalse();
        // worker-2 must fail closed (concurrent-owner), even though Fluss KV would let it write.
        FenceResult second = gates.acquire(PARTITION, "worker-2", 60_000, NOW + 1000);
        assertThat(second.conflict()).isTrue();
    }

    @Test
    void staleOwnerIsRejectedAfterFenceLossAndReAcquisition() {
        InMemoryGateStateStore gates = new InMemoryGateStateStore(Set.of("saurabh"));
        gates.install(new GateRow(PARTITION, "acc", GateState.HALTED, 0, "boot", null,
                null, null, null, null, 0, null, null, null));
        FenceResult r1 = gates.acquire(PARTITION, "worker-1", 60_000, NOW); // token 1
        // lease expires; worker-2 re-acquires (a fence loss for worker-1)
        FenceResult r2 = gates.acquire(PARTITION, "worker-2", 60_000, NOW + 120_000); // token 2
        assertThat(r2.conflict()).isFalse();
        // worker-1's OLD fence token must now be invalid — its authorization is stale.
        GateRow row = r2.row();
        assertThat(row.fenceValidFor("worker-1", r1.row().fenceToken(), NOW + 120_001)).isFalse();
    }
}
