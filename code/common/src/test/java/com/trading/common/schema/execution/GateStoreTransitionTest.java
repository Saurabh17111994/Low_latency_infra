package com.trading.common.schema.execution;

import static org.assertj.core.api.Assertions.assertThat;

import com.trading.common.model.GateState;
import com.trading.common.schema.execution.GateStateStore.AuditRecord;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * H2-5 (CHG-334): the store's sanctioned forward step. The enablement path is
 * {@code HALTED → RECONCILING → APPROVAL_PENDING → ENABLED}; before this method the store had no
 * mutator for the first two steps, so no production path could ever produce the
 * {@code APPROVAL_PENDING} row that {@code approveAndEnableIfComplete} promotes from (the live
 * wiring would have had to jump {@code HALTED → ENABLED}, which {@link GateState#legalTargets()}
 * declares illegal).
 *
 * <p>These tests pin the primitive: legal steps only, epoch +1 per step, CAS witness honored,
 * fence columns preserved, and the illegal/no-op answers distinguishable from success by the
 * returned row's state.
 */
class GateStoreTransitionTest {

    private static final long NOW = 1_700_000_000_000L;
    private static final String P = "p-1";

    private static InMemoryGateStateStore store(GateState state) {
        InMemoryGateStateStore s = new InMemoryGateStateStore(Set.of("saurabh"));
        s.install(new GateRow(P, "acc", state, 3, "boot", "ev-1", null, null, null,
                "worker-1", 7, NOW - 1000, NOW + 100_000, null));
        return s;
    }

    @Test
    @DisplayName("the sanctioned path reaches APPROVAL_PENDING and the existing approve promotes it")
    void sanctionedPathReachesEnabled() {
        InMemoryGateStateStore s = store(GateState.HALTED);

        GateRow reconciling = s.transition(P, s.read(P), GateState.RECONCILING, "reconcile", null, NOW);
        assertThat(reconciling.state()).isEqualTo(GateState.RECONCILING);
        assertThat(reconciling.epoch()).isEqualTo(4);

        GateRow pending =
                s.transition(P, s.read(P), GateState.APPROVAL_PENDING, "reconciled", null, NOW);
        assertThat(pending.state()).isEqualTo(GateState.APPROVAL_PENDING);
        assertThat(pending.epoch()).isEqualTo(5);

        // the promotion leg runs against the current epoch, as the live wiring does
        GateStateStore.ApprovalResult res = s.approveAndEnableIfComplete(P, "saurabh", 5, "ev-1", NOW);
        assertThat(res.outcome()).isEqualTo(GateStateStore.ApprovalOutcome.APPLIED);
        assertThat(res.row().state()).isEqualTo(GateState.ENABLED);
        assertThat(res.row().approval1()).isEqualTo("saurabh");
        assertThat(res.row().approvedEvidenceHash()).isEqualTo("ev-1");
    }

    @Test
    @DisplayName("illegal targets are no-ops: no state change, no epoch bump, current row returned")
    void illegalTargetsAreNoOps() {
        // HALTED -> ENABLED skips the only enablement path.
        InMemoryGateStateStore halted = store(GateState.HALTED);
        GateRow out = halted.transition(P, halted.read(P), GateState.ENABLED, "jump", null, NOW);
        assertThat(out.state()).isEqualTo(GateState.HALTED);
        assertThat(out.epoch()).isEqualTo(3);
        assertThat(halted.read(P).state()).isEqualTo(GateState.HALTED);

        // HALTED -> APPROVAL_PENDING skips reconciliation.
        out = halted.transition(P, halted.read(P), GateState.APPROVAL_PENDING, "skip", null, NOW);
        assertThat(out.state()).isEqualTo(GateState.HALTED);
        assertThat(out.epoch()).isEqualTo(3);

        // APPROVAL_PENDING -> RECONCILING silently re-reconciles instead of halting.
        InMemoryGateStateStore pending = store(GateState.APPROVAL_PENDING);
        out = pending.transition(P, pending.read(P), GateState.RECONCILING, "back", null, NOW);
        assertThat(out.state()).isEqualTo(GateState.APPROVAL_PENDING);
        assertThat(out.epoch()).isEqualTo(3);
    }

    @Test
    @DisplayName("a stale CAS witness is a no-op — a newer generation is never advanced")
    void staleWitnessIsNoOp() {
        InMemoryGateStateStore s = store(GateState.HALTED);
        GateRow stale = new GateRow(P, "acc", GateState.HALTED, 2, "old", null, null, null, null,
                null, 0L, null, null, null);

        GateRow out = s.transition(P, stale, GateState.RECONCILING, "reconcile", null, NOW);

        assertThat(out.state()).isEqualTo(GateState.HALTED);
        assertThat(out.epoch()).isEqualTo(3);
    }

    @Test
    @DisplayName("no row for the partition returns null, never a synthetic row")
    void missingRowReturnsNull() {
        InMemoryGateStateStore s = store(GateState.HALTED);
        assertThat(s.transition("other", null, GateState.RECONCILING, "x", null, NOW)).isNull();
    }

    @Test
    @DisplayName("a transition preserves the fence columns and records the step in the audit log")
    void preservesFenceAndAudits() {
        InMemoryGateStateStore s = store(GateState.HALTED);
        s.acquire(P, "worker-1", 30_000, NOW);
        long token = s.read(P).fenceToken();

        GateRow out = s.transition(P, s.read(P), GateState.RECONCILING, "reconcile", "ev-2", NOW);

        assertThat(out.ownerInstanceId()).isEqualTo("worker-1");
        assertThat(out.fenceToken()).isEqualTo(token);
        assertThat(out.leaseExpiresTs()).isEqualTo(NOW + 30_000);
        assertThat(out.evidenceHash()).isEqualTo("ev-2");
        assertThat(s.auditLog())
                .extracting(AuditRecord::eventType)
                .contains("TRANSITION");
    }
}
