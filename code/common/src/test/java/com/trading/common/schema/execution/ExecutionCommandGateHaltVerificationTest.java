package com.trading.common.schema.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.trading.common.model.GateState;
import com.trading.common.schema.execution.BridgeCaller.OutcomeKind;
import com.trading.common.schema.execution.ExecutionCommandGate.Command;
import com.trading.common.schema.execution.ExecutionCommandGate.Outcome;
import com.trading.common.schema.execution.GateStateStore.ApprovalResult;
import com.trading.common.schema.execution.GateStateStore.AuditRecord;
import com.trading.common.schema.execution.GateStateStore.FenceResult;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * P3-134: the safety halt must be durably verified before the gate reports
 * {@code UNKNOWN_HALTED}. Reporting it while the gate could still be ENABLED
 * would let the next {@code execute()} pass the ENABLED check and place a
 * second order. These tests drive a store whose halt can be made to "not land"
 * and pin both the escalation and the fail-closed outcome.
 */
class ExecutionCommandGateHaltVerificationTest {

    private static final long NOW = 1_700_000_000_000L;
    private static final String PARTITION = "p-1";

    /** Delegating store whose halt can be forced to fail (simulates a lost CAS / refusing store). */
    private static final class HaltProbe implements GateStateStore {
        private final InMemoryGateStateStore delegate = new InMemoryGateStateStore(Set.of("saurabh"));
        int haltCalls;
        boolean rejectConditional;
        boolean rejectUnconditional;

        HaltProbe() {
            delegate.install(new GateRow(PARTITION, "acc", GateState.ENABLED, 5, "enabled", "ev-1",
                    "saurabh", null, "ev-1", "worker-1", 7, NOW - 1000, NOW + 100_000, null));
        }

        @Override public GateRow read(String p) { return delegate.read(p); }
        @Override public GateRow init(GateRow r) { return delegate.init(r); }
        @Override public FenceResult acquire(String p, String o, long l, long t) {
            return delegate.acquire(p, o, l, t);
        }
        @Override public FenceResult renew(String p, String o, long tok, long l, long t) {
            return delegate.renew(p, o, tok, l, t);
        }
        @Override public GateRow revoke(String p, String o, long t) { return delegate.revoke(p, o, t); }
        @Override public ApprovalResult approve(String p, String pr, long e, String h, long t) {
            return delegate.approve(p, pr, e, h, t);
        }
        @Override public GateRow halt(String p, GateRow expected, String reason, String ev, long now,
                Long detectedTs) {
            haltCalls++;
            // "Did not land": return the current row unchanged, exactly as a
            // CAS-rejected or refusing store does (never HALTED).
            if (expected == null ? rejectUnconditional : rejectConditional) return delegate.read(p);
            return delegate.halt(p, expected, reason, ev, now);
        }
        @Override public void audit(AuditRecord r) { delegate.audit(r); }
        @Override public List<AuditRecord> auditLog() { return delegate.auditLog(); }
    }

    private static Command cmd() {
        return new Command("a-1", "acc", "ins-1", "act-a-1", PARTITION, "h-1", "E-a-1", 5, 7, "ev-1");
    }

    private static ExecutionCommandGate gate(HaltProbe store) {
        InMemoryAttemptStore attempts = new InMemoryAttemptStore(new AtomicInteger()::incrementAndGet);
        BridgeCaller ambiguous = c -> new BridgeCaller.BridgeOutcome(
                OutcomeKind.UNKNOWN, null, "ambiguous transport");
        return new ExecutionCommandGate(attempts, store, ambiguous, "worker-1", () -> NOW, null);
    }

    @Test
    void staleHaltWitnessEscalatesToUnconditionalHaltAndReportsHalted() {
        HaltProbe store = new HaltProbe();
        store.rejectConditional = true; // the named-generation halt loses its CAS
        ExecutionCommandGate.Result r = gate(store).execute(cmd());

        assertThat(store.haltCalls).isEqualTo(2);          // conditional then unconditional
        assertThat(r.outcome()).isEqualTo(Outcome.UNKNOWN_HALTED);
        assertThat(r.gate().state()).isEqualTo(GateState.HALTED);
        assertThat(store.read(PARTITION).state()).isEqualTo(GateState.HALTED);
    }

    @Test
    void unverifiedHaltFailsClosedInsteadOfClaimingUnknownHalted() {
        HaltProbe store = new HaltProbe();
        store.rejectConditional = true;
        store.rejectUnconditional = true; // halt can never land
        ExecutionCommandGate gate = gate(store);

        assertThatThrownBy(() -> gate.execute(cmd()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("halt not durably HALTED");
        assertThat(store.haltCalls).isEqualTo(2);
        // the gate was never reported halted while it is still ENABLED
        assertThat(store.read(PARTITION).state()).isEqualTo(GateState.ENABLED);
    }
}
