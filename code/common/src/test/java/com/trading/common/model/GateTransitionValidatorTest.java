package com.trading.common.model;

import static org.assertj.core.api.Assertions.assertThat;

import com.trading.common.schema.execution.AttemptRecord;
import com.trading.common.schema.execution.AttemptStore;
import com.trading.common.schema.execution.InMemoryAttemptStore;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * T5 reconciliation (CHG-044): the gate/attempt transition matrix is frozen in
 * one canonical table ({@link AttemptPhase#legalTargets} / {@link GateState#legalTargets})
 * and this test pins the exact contracts — including the pre-T5 conflict that
 * made ACCEPTED → REJECTED legal in the validator while the attempt-store's
 * TERMINAL_PHASES treated ACCEPTED as terminal. It also pins that the validator
 * and the store agree on every corrected transition.
 */
class GateTransitionValidatorTest {

    // ---- attempt matrix (single source of truth) ----

    @Test
    void canonicalAttemptMatrixExact() {
        assertThat(AttemptPhase.PREPARED.legalTargets()).containsExactly(AttemptPhase.SUBMITTING);
        assertThat(AttemptPhase.SUBMITTING.legalTargets())
                .containsExactlyInAnyOrder(AttemptPhase.ACCEPTED, AttemptPhase.REJECTED,
                        AttemptPhase.CANCELLED, AttemptPhase.UNKNOWN);
        assertThat(AttemptPhase.UNKNOWN.legalTargets())
                .containsExactlyInAnyOrder(AttemptPhase.ACCEPTED, AttemptPhase.REJECTED,
                        AttemptPhase.CANCELLED);
        assertThat(AttemptPhase.ACCEPTED.legalTargets()).isEmpty();
        assertThat(AttemptPhase.REJECTED.legalTargets()).isEmpty();
        assertThat(AttemptPhase.CANCELLED.legalTargets()).isEmpty();

        assertThat(AttemptPhase.ACCEPTED.isTerminal()).isTrue();
        assertThat(AttemptPhase.REJECTED.isTerminal()).isTrue();
        assertThat(AttemptPhase.CANCELLED.isTerminal()).isTrue();
        assertThat(AttemptPhase.UNKNOWN.isTerminal()).isFalse();
        assertThat(AttemptPhase.UNKNOWN.isReconciliationSource()).isTrue();
        assertThat(AttemptPhase.SUBMITTING.isReconciliationSource()).isFalse();
    }

    @Test
    void namedConflictResolvedAcceptedCannotBecomeRejected() {
        // Pre-T5 this was allowed by the validator and rejected by the store;
        // T5 freezes one meaning — ACCEPTED is terminal.
        assertThat(GateTransitionValidator.validateAttemptTransition(
                AttemptPhase.ACCEPTED, AttemptPhase.REJECTED, "review").allowed()).isFalse();
        assertThat(AttemptPhase.isLegal(AttemptPhase.ACCEPTED, AttemptPhase.REJECTED)).isFalse();
    }

    @Test
    void terminalPhasesNeverBecomeUnknown() {
        // The old validator used allOf for UNKNOWN, so a terminal outcome could
        // "become uncertain again". A terminal outcome is final.
        for (AttemptPhase terminal : new AttemptPhase[]{
                AttemptPhase.ACCEPTED, AttemptPhase.REJECTED, AttemptPhase.CANCELLED}) {
            assertThat(GateTransitionValidator.validateAttemptTransition(
                    terminal, AttemptPhase.UNKNOWN, "x").allowed()).as("%s->UNKNOWN", terminal)
                    .isFalse();
            assertThat(AttemptPhase.isLegal(terminal, AttemptPhase.UNKNOWN)).isFalse();
        }
    }

    @Test
    void preparedOnlyExitsToSubmitting() {
        for (AttemptPhase to : AttemptPhase.values()) {
            if (to == AttemptPhase.SUBMITTING) {
                continue;
            }
            assertThat(AttemptPhase.isLegal(AttemptPhase.PREPARED, to))
                    .as("PREPARED->%s", to).isFalse();
        }
        // ... including the previously-allowed PREPARED->CANCELLED and PREPARED->UNKNOWN.
        assertThat(AttemptPhase.isLegal(AttemptPhase.PREPARED, AttemptPhase.CANCELLED)).isFalse();
        assertThat(AttemptPhase.isLegal(AttemptPhase.PREPARED, AttemptPhase.UNKNOWN)).isFalse();
    }

    @Test
    void unknownExitsOnlyViaReconciliation() {
        assertThat(GateTransitionValidator.isReconciliationOnly(
                AttemptPhase.UNKNOWN, AttemptPhase.ACCEPTED)).isTrue();
        assertThat(GateTransitionValidator.isReconciliationOnly(
                AttemptPhase.UNKNOWN, AttemptPhase.REJECTED)).isTrue();
        assertThat(GateTransitionValidator.isReconciliationOnly(
                AttemptPhase.UNKNOWN, AttemptPhase.CANCELLED)).isTrue();
        // UNKNOWN can never go back to SUBMITTING (no auto-retry) and never to PREPARED.
        assertThat(AttemptPhase.isLegal(AttemptPhase.UNKNOWN, AttemptPhase.SUBMITTING)).isFalse();
        assertThat(AttemptPhase.isLegal(AttemptPhase.UNKNOWN, AttemptPhase.PREPARED)).isFalse();
    }

    @Test
    void reconciliationTargetsAreTerminalAndExcludeSubmission() {
        // P3-378 / P3-492 / P3-494: the reconciliation allowlist is pinned to
        // terminal exits — UNKNOWN -> SUBMITTING can never be routed through
        // resolveUnknown, even if the canonical matrix ever admitted that edge.
        for (AttemptPhase to : AttemptPhase.UNKNOWN.legalTargets()) {
            assertThat(to.isTerminal()).as("UNKNOWN->%s terminal", to).isTrue();
            assertThat(GateTransitionValidator.isReconciliationOnly(AttemptPhase.UNKNOWN, to))
                    .as("UNKNOWN->%s is reconciliation-only", to).isTrue();
            assertThat(GateTransitionValidator.isSubmissionLegal(AttemptPhase.UNKNOWN, to))
                    .as("UNKNOWN->%s is not a submission route", to).isFalse();
        }
        assertThat(GateTransitionValidator.isReconciliationOnly(
                AttemptPhase.UNKNOWN, AttemptPhase.SUBMITTING)).isFalse();
        assertThat(GateTransitionValidator.isReconciliationOnly(
                AttemptPhase.UNKNOWN, AttemptPhase.PREPARED)).isFalse();
        assertThat(GateTransitionValidator.isReconciliationOnly(
                AttemptPhase.UNKNOWN, null)).isFalse();
        assertThat(GateTransitionValidator.isReconciliationOnly(null, null)).isFalse();
        assertThat(GateTransitionValidator.isReconciliationOnly(
                AttemptPhase.PREPARED, AttemptPhase.SUBMITTING)).isFalse();
    }

    // ---- gate matrix (single source of truth) ----

    @Test
    void onlyEnablementPathIsSanctioned() {
        // HALTED -> RECONCILING -> APPROVAL_PENDING -> ENABLED, plus halt -> HALTED.
        assertThat(GateState.HALTED.legalTargets()).containsExactly(GateState.RECONCILING);
        assertThat(GateState.RECONCILING.legalTargets())
                .containsExactlyInAnyOrder(GateState.APPROVAL_PENDING, GateState.HALTED);
        assertThat(GateState.APPROVAL_PENDING.legalTargets())
                .containsExactlyInAnyOrder(GateState.ENABLED, GateState.HALTED);
        assertThat(GateState.ENABLED.legalTargets()).containsExactly(GateState.HALTED);
    }

    @Test
    void gateCannotSkipStepsOrSilentlyRegress() {
        // HALTED -> APPROVAL_PENDING skips RECONCILING; APPROVAL_PENDING ->
        // RECONCILING silently regresses instead of halting. Both were pre-T5
        // legal in the Java validator and illegal in the Rust gate; reconcile.
        assertThat(GateTransitionValidator.validateGateTransition(
                GateState.HALTED, GateState.APPROVAL_PENDING, 0, 1).allowed()).isFalse();
        assertThat(GateTransitionValidator.validateGateTransition(
                GateState.APPROVAL_PENDING, GateState.RECONCILING, 2, 3).allowed()).isFalse();
        // Safety halt from any state is always legal.
        assertThat(GateTransitionValidator.validateGateTransition(
                GateState.ENABLED, GateState.HALTED, 3, 4).allowed()).isTrue();
        assertThat(GateTransitionValidator.validateGateTransition(
                GateState.APPROVAL_PENDING, GateState.HALTED, 2, 3).allowed()).isTrue();
    }

    @Test
    void forwardEpochGapIsRejectedAndSingleStepAdvanceIsLegal() {
        // P3-118/P3-119: every accepted transition advances the epoch by exactly
        // one, so a request claiming a further generation is out of sync with the
        // gate (lost lease / stale owner / replayed message) and must be rejected.
        assertThat(GateTransitionValidator.validateGateTransition(
                GateState.RECONCILING, GateState.APPROVAL_PENDING, 1, 100).allowed()).isFalse();
        assertThat(GateTransitionValidator.validateGateTransition(
                GateState.RECONCILING, GateState.APPROVAL_PENDING, 1, 100).detail())
                .contains("forward epoch gap");

        // Exactly +1 is the sanctioned advance; same-generation is the no-gap case.
        assertThat(GateTransitionValidator.validateGateTransition(
                GateState.RECONCILING, GateState.APPROVAL_PENDING, 1, 2).allowed()).isTrue();
        assertThat(GateTransitionValidator.validateGateTransition(
                GateState.RECONCILING, GateState.APPROVAL_PENDING, 1, 1).allowed()).isTrue();

        // The gap rule is total — it is not bypassable via the idempotent branch.
        assertThat(GateTransitionValidator.validateGateTransition(
                GateState.ENABLED, GateState.ENABLED, 1, 50).allowed()).isFalse();

        // ...but a safety halt still lands on any epoch (P3-120).
        assertThat(GateTransitionValidator.validateGateTransition(
                GateState.ENABLED, GateState.HALTED, 1, 50).allowed()).isTrue();

        // Stale epochs keep their own rejection.
        assertThat(GateTransitionValidator.validateGateTransition(
                GateState.RECONCILING, GateState.APPROVAL_PENDING, 5, 4).allowed()).isFalse();
    }

    // ---- validator/store agreement on the corrected transitions ----

    @Test
    void storeAgreesWithReconciledValidator() {
        AtomicInteger halts = new AtomicInteger();
        InMemoryAttemptStore store = new InMemoryAttemptStore(halts::incrementAndGet);
        AttemptStore.PrepareRequest req = new AttemptStore.PrepareRequest(
                "a-1", "acc", "ins-1", null, "p-1", "h-1", "E-1", 1L, 7L, 1_700_000_000_000L);
        store.prepare(req);

        // ACCEPTED -> REJECTED: rejected by the store as TERMINAL (was the conflict).
        store.transition("a-1", 0, AttemptRecord.PHASE_SUBMITTING);
        store.transition("a-1", 1, AttemptRecord.PHASE_ACCEPTED);
        assertThat(store.transition("a-1", 2, AttemptRecord.PHASE_REJECTED).outcome())
                .isEqualTo(AttemptStore.TransitionOutcome.TERMINAL);
        // Terminal -> UNKNOWN: rejected as TERMINAL.
        assertThat(store.transition("a-1", 2, AttemptRecord.PHASE_UNKNOWN).outcome())
                .isEqualTo(AttemptStore.TransitionOutcome.TERMINAL);

        // A fresh chain: PREPARED -> SUBMITTING -> UNKNOWN is legal; UNKNOWN cannot
        // return to SUBMITTING (no auto-retry) but resolves only via reconciliation.
        InMemoryAttemptStore s2 = new InMemoryAttemptStore(halts::incrementAndGet);
        s2.prepare(req);
        s2.transition("a-1", 0, AttemptRecord.PHASE_SUBMITTING);
        assertThat(s2.transition("a-1", 1, AttemptRecord.PHASE_UNKNOWN).outcome())
                .isEqualTo(AttemptStore.TransitionOutcome.APPLIED);
        assertThat(s2.transition("a-1", 2, AttemptRecord.PHASE_SUBMITTING).outcome())
                .isEqualTo(AttemptStore.TransitionOutcome.ILLEGAL_TRANSITION);
        // P3-378: the reconciliation path rejects a submission target too — the
        // no-auto-retry invariant is pinned on both routes.
        assertThat(s2.resolveUnknown("a-1", 2, AttemptRecord.PHASE_SUBMITTING).outcome())
                .isEqualTo(AttemptStore.TransitionOutcome.ILLEGAL_TRANSITION);
        assertThat(s2.resolveUnknown("a-1", 2, AttemptRecord.PHASE_ACCEPTED).outcome())
                .isEqualTo(AttemptStore.TransitionOutcome.APPLIED);
    }
}
