package com.trading.common.schema.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.trading.common.schema.ownership.ColumnOwnership;
import com.trading.common.schema.ownership.ExecutionAttemptsColumnOwnership;
import com.trading.common.schema.ownership.ExecutionAttemptsColumns;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * SCH-15 first consumer: {@link InMemoryAttemptStore} runs the
 * column-ownership guard ({@code ColumnOwnership.checkWrite} via the
 * attempt-store matrix) on every mutation, and implements the Task 5 attempt
 * rules — prepare replay never rewrites the PREPARED attempt's identity;
 * legal transitions with monotonic {@code phase_epoch}; stale-epoch
 * rejection; terminal protection; and UNKNOWN resolution only through an
 * explicit reconciliation result.
 */
class InMemoryAttemptStoreTest {

    private static final String ACCOUNT = "acc-1";
    private static final String PARTITION = "p-1";
    private static final long GATE_EPOCH = 7L;
    private static final long GATE_FENCE = 42L;
    private static final long NOW_TS = 1_700_000_000_000L;

    private final AtomicInteger halts = new AtomicInteger();
    private InMemoryAttemptStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryAttemptStore(halts::incrementAndGet);
    }

    private static AttemptStore.PrepareRequest req(String attemptId, String instructionId,
                                                   String requestHash) {
        return new AttemptStore.PrepareRequest(attemptId, ACCOUNT, instructionId, null,
                PARTITION, requestHash, "E" + attemptId, GATE_FENCE, GATE_EPOCH, NOW_TS);
    }

    private static AttemptRecord preparedRecord(String attemptId, String instructionId,
                                                String requestHash) {
        return AttemptRecord.prepared(attemptId, ACCOUNT, instructionId, null, PARTITION,
                requestHash, "E" + attemptId, GATE_FENCE, GATE_EPOCH, NOW_TS);
    }

    // ── prepare / replay / identity guard ──────────────────────────────────

    @Test
    void prepareMintsPreparedAttemptWithOwnedDefaults() {
        AttemptStore.PrepareResult r = store.prepare(req("a-1", "ins-1", "h-1"));
        assertThat(r.status()).isEqualTo(AttemptStore.Status.CREATED);
        AttemptRecord a = r.record();
        assertThat(a.executionAttemptId()).isEqualTo("a-1");
        assertThat(a.instructionId()).isEqualTo("ins-1");
        assertThat(a.requestHash()).isEqualTo("h-1");
        assertThat(a.accountScopeId()).isEqualTo(ACCOUNT);
        assertThat(a.executionPartitionId()).isEqualTo(PARTITION);
        assertThat(a.gateEpoch()).isEqualTo(GATE_EPOCH);
        assertThat(a.gateFenceToken()).isEqualTo(GATE_FENCE);
        assertThat(a.phase()).isEqualTo(AttemptRecord.PHASE_PREPARED);
        assertThat(a.phaseEpoch()).isZero();
        assertThat(a.retryAttempt()).isZero();
        assertThat(a.preparedTs()).isEqualTo(NOW_TS);
        assertThat(a.schemaVersion()).isEqualTo(ExecutionAttemptsColumns.SCHEMA_VERSION_V3);
        // call-evidence group is null until the broker adapter reports
        assertThat(a.brokerOrderId()).isNull();
        assertThat(a.outcome()).isNull();
        assertThat(a.submittedTs()).isNull();
        assertThat(a.terminalTs()).isNull();
        assertThat(halts.get()).isZero();
        assertThat(store.size()).isEqualTo(1);
    }

    @Test
    void duplicatePrepareReturnsExistingWithoutMutation() {
        store.prepare(req("a-1", "ins-1", "h-1"));
        AttemptStore.PrepareResult r = store.prepare(req("a-1", "ins-1", "h-1"));
        assertThat(r.status()).isEqualTo(AttemptStore.Status.DUPLICATE);
        assertThat(r.record().executionAttemptId()).isEqualTo("a-1");
        assertThat(r.record().phase()).isEqualTo(AttemptRecord.PHASE_PREPARED);
        assertThat(r.record().phaseEpoch()).isZero();
        // replay returns the SAME record — identity never rewritten, no second row
        assertThat(r.record()).isEqualTo(store.attemptById("a-1"));
        assertThat(store.size()).isEqualTo(1);
        assertThat(halts.get()).isZero();
    }

    @Test
    void conflictingRequestHashRaisesContractViolationAndHalts() {
        store.prepare(req("a-1", "ins-1", "h-1"));
        AttemptStore.PrepareResult r = store.prepare(req("a-1", "ins-1", "h-2"));
        assertThat(r.status()).isEqualTo(AttemptStore.Status.CONTRACT_VIOLATION);
        assertThat(r.reason()).contains("modified decision");
        // halt requested, nothing created or mutated
        assertThat(halts.get()).isEqualTo(1);
        assertThat(store.size()).isEqualTo(1);
        assertThat(store.attemptById("a-1").requestHash()).isEqualTo("h-1");
        assertThat(store.attemptById("a-1").phase()).isEqualTo(AttemptRecord.PHASE_PREPARED);
    }

    @Test
    void prepareMintsUniqueAttemptsForDistinctInstructions() {
        store.prepare(req("a-1", "ins-1", "h-1"));
        store.prepare(req("a-2", "ins-2", "h-2"));
        assertThat(store.size()).isEqualTo(2);
        assertThat(halts.get()).isZero();
    }

    // ── legal transitions + monotonic phase_epoch ─────────────────────────

    @Test
    void transitionAppliesLegalMoveAndIncrementsPhaseEpoch() {
        store.prepare(req("a-1", "ins-1", "h-1"));
        AttemptStore.TransitionResult r = store.transition("a-1", 0, AttemptRecord.PHASE_SUBMITTING);
        assertThat(r.outcome()).isEqualTo(AttemptStore.TransitionOutcome.APPLIED);
        assertThat(r.record().phase()).isEqualTo(AttemptRecord.PHASE_SUBMITTING);
        assertThat(r.record().phaseEpoch()).isEqualTo(1);
        // identity + call evidence untouched by the transition
        assertThat(r.record().requestHash()).isEqualTo("h-1");
        assertThat(r.record().clientOrderRef()).isEqualTo("Ea-1");
        assertThat(r.record().brokerOrderId()).isNull();
        assertThat(store.attemptById("a-1")).isEqualTo(r.record());
        assertThat(halts.get()).isZero();
    }

    @Test
    void phaseEpochIsStrictlyMonotonicAcrossChain() {
        // PREPARED(0) -> SUBMITTING(1) -> UNKNOWN(2) -> ACCEPTED(3): every
        // applied transition bumps phase_epoch by exactly 1.
        store.prepare(req("a-1", "ins-1", "h-1"));
        AttemptStore.TransitionResult sub = store.transition("a-1", 0, AttemptRecord.PHASE_SUBMITTING);
        assertThat(sub.outcome()).isEqualTo(AttemptStore.TransitionOutcome.APPLIED);
        assertThat(sub.record().phaseEpoch()).isEqualTo(1);
        AttemptStore.TransitionResult unk = store.transition("a-1", 1, AttemptRecord.PHASE_UNKNOWN);
        assertThat(unk.outcome()).isEqualTo(AttemptStore.TransitionOutcome.APPLIED);
        assertThat(unk.record().phaseEpoch()).isEqualTo(2);
        AttemptStore.TransitionResult acc = store.resolveUnknown("a-1", 2, AttemptRecord.PHASE_ACCEPTED);
        assertThat(acc.outcome()).isEqualTo(AttemptStore.TransitionOutcome.APPLIED);
        assertThat(acc.record().phaseEpoch()).isEqualTo(3);
        assertThat(store.attemptById("a-1").phase()).isEqualTo(AttemptRecord.PHASE_ACCEPTED);
    }

    @Test
    void gateFenceTokenAndEpochAreImmutableIdentity() {
        // The authorization pair (gate_epoch, gate_fence_token) is captured at
        // PREPARED and never rewritten (dossier "PREPARED (... gate epoch +
        // fence)"); transitions must preserve it byte-for-byte.
        store.prepare(req("a-1", "ins-1", "h-1"));
        store.transition("a-1", 0, AttemptRecord.PHASE_SUBMITTING);
        store.transition("a-1", 1, AttemptRecord.PHASE_ACCEPTED);
        AttemptRecord a = store.attemptById("a-1");
        assertThat(a.gateEpoch()).isEqualTo(GATE_EPOCH);
        assertThat(a.gateFenceToken()).isEqualTo(GATE_FENCE);
    }

    @Test
    void staleEpochRejectedWithoutMutation() {
        store.prepare(req("a-1", "ins-1", "h-1"));
        AttemptStore.TransitionResult r = store.transition("a-1", 1, AttemptRecord.PHASE_SUBMITTING);
        assertThat(r.outcome()).isEqualTo(AttemptStore.TransitionOutcome.STALE_EPOCH);
        assertThat(r.reason()).contains("phase_epoch");
        // no mutation
        assertThat(store.attemptById("a-1").phase()).isEqualTo(AttemptRecord.PHASE_PREPARED);
        assertThat(store.attemptById("a-1").phaseEpoch()).isZero();
    }

    @Test
    void illegalTransitionRejectedWithoutMutation() {
        store.prepare(req("a-1", "ins-1", "h-1"));
        // PREPARED -> ACCEPTED skips SUBMITTING
        AttemptStore.TransitionResult skip = store.transition("a-1", 0, AttemptRecord.PHASE_ACCEPTED);
        assertThat(skip.outcome()).isEqualTo(AttemptStore.TransitionOutcome.ILLEGAL_TRANSITION);
        // PREPARED -> UNKNOWN is not a submission-path move
        AttemptStore.TransitionResult jump = store.transition("a-1", 0, AttemptRecord.PHASE_UNKNOWN);
        assertThat(jump.outcome()).isEqualTo(AttemptStore.TransitionOutcome.ILLEGAL_TRANSITION);
        assertThat(store.attemptById("a-1").phase()).isEqualTo(AttemptRecord.PHASE_PREPARED);
        assertThat(store.attemptById("a-1").phaseEpoch()).isZero();
    }

    @Test
    void terminalProtectionBlocksFurtherTransitions() {
        store.prepare(req("a-1", "ins-1", "h-1"));
        assertThat(store.transition("a-1", 0, AttemptRecord.PHASE_SUBMITTING).outcome())
                .isEqualTo(AttemptStore.TransitionOutcome.APPLIED);
        assertThat(store.transition("a-1", 1, AttemptRecord.PHASE_ACCEPTED).outcome())
                .isEqualTo(AttemptStore.TransitionOutcome.APPLIED);
        AttemptStore.TransitionResult r = store.transition("a-1", 2, AttemptRecord.PHASE_SUBMITTING);
        assertThat(r.outcome()).isEqualTo(AttemptStore.TransitionOutcome.TERMINAL);
        assertThat(store.attemptById("a-1").phase()).isEqualTo(AttemptRecord.PHASE_ACCEPTED);
        assertThat(store.attemptById("a-1").phaseEpoch()).isEqualTo(2);
    }

    @Test
    void unknownExitsOnlyThroughExplicitReconcile() {
        store.prepare(req("a-1", "ins-1", "h-1"));
        assertThat(store.transition("a-1", 0, AttemptRecord.PHASE_SUBMITTING).outcome())
                .isEqualTo(AttemptStore.TransitionOutcome.APPLIED);
        assertThat(store.transition("a-1", 1, AttemptRecord.PHASE_UNKNOWN).outcome())
                .isEqualTo(AttemptStore.TransitionOutcome.APPLIED);

        // UNKNOWN cannot go back to SUBMITTING (no automatic retry)…
        AttemptStore.TransitionResult retry = store.transition("a-1", 2, AttemptRecord.PHASE_SUBMITTING);
        assertThat(retry.outcome()).isEqualTo(AttemptStore.TransitionOutcome.ILLEGAL_TRANSITION);
        // …and cannot exit through the submission path at all
        AttemptStore.TransitionResult direct = store.transition("a-1", 2, AttemptRecord.PHASE_ACCEPTED);
        assertThat(direct.outcome()).isEqualTo(AttemptStore.TransitionOutcome.ILLEGAL_TRANSITION);

        // only an explicit reconciliation result resolves it
        AttemptStore.TransitionResult resolved =
                store.resolveUnknown("a-1", 2, AttemptRecord.PHASE_ACCEPTED);
        assertThat(resolved.outcome()).isEqualTo(AttemptStore.TransitionOutcome.APPLIED);
        assertThat(resolved.record().phase()).isEqualTo(AttemptRecord.PHASE_ACCEPTED);
        assertThat(resolved.record().phaseEpoch()).isEqualTo(3);
    }

    @Test
    void resolveUnknownRejectsNonUnknownAttempt() {
        store.prepare(req("a-1", "ins-1", "h-1"));
        assertThat(store.transition("a-1", 0, AttemptRecord.PHASE_SUBMITTING).outcome())
                .isEqualTo(AttemptStore.TransitionOutcome.APPLIED);
        AttemptStore.TransitionResult r =
                store.resolveUnknown("a-1", 1, AttemptRecord.PHASE_ACCEPTED);
        assertThat(r.outcome()).isEqualTo(AttemptStore.TransitionOutcome.ILLEGAL_TRANSITION);
        assertThat(store.attemptById("a-1").phase()).isEqualTo(AttemptRecord.PHASE_SUBMITTING);
        assertThat(store.attemptById("a-1").phaseEpoch()).isEqualTo(1);
    }

    @Test
    void resolveUnknownRejectsNonTerminalResolution() {
        store.prepare(req("a-1", "ins-1", "h-1"));
        assertThat(store.transition("a-1", 0, AttemptRecord.PHASE_SUBMITTING).outcome())
                .isEqualTo(AttemptStore.TransitionOutcome.APPLIED);
        assertThat(store.transition("a-1", 1, AttemptRecord.PHASE_UNKNOWN).outcome())
                .isEqualTo(AttemptStore.TransitionOutcome.APPLIED);
        AttemptStore.TransitionResult r =
                store.resolveUnknown("a-1", 2, AttemptRecord.PHASE_SUBMITTING);
        assertThat(r.outcome()).isEqualTo(AttemptStore.TransitionOutcome.ILLEGAL_TRANSITION);
        assertThat(store.attemptById("a-1").phase()).isEqualTo(AttemptRecord.PHASE_UNKNOWN);
    }

    @Test
    void resolveUnknownAppliesRejectedAndCancelled() {
        store.prepare(req("a-1", "ins-1", "h-1"));
        store.prepare(req("a-2", "ins-2", "h-2"));
        store.transition("a-1", 0, AttemptRecord.PHASE_SUBMITTING);
        store.transition("a-2", 0, AttemptRecord.PHASE_SUBMITTING);
        store.transition("a-1", 1, AttemptRecord.PHASE_UNKNOWN);
        store.transition("a-2", 1, AttemptRecord.PHASE_UNKNOWN);
        assertThat(store.resolveUnknown("a-1", 2, AttemptRecord.PHASE_REJECTED).outcome())
                .isEqualTo(AttemptStore.TransitionOutcome.APPLIED);
        assertThat(store.resolveUnknown("a-2", 2, AttemptRecord.PHASE_CANCELLED).outcome())
                .isEqualTo(AttemptStore.TransitionOutcome.APPLIED);
        assertThat(store.attemptById("a-1").phase()).isEqualTo(AttemptRecord.PHASE_REJECTED);
        assertThat(store.attemptById("a-2").phase()).isEqualTo(AttemptRecord.PHASE_CANCELLED);
    }

    @Test
    void transitionOfUnknownAttemptReturnsNotFound() {
        AttemptStore.TransitionResult r = store.transition("nope", 0, AttemptRecord.PHASE_SUBMITTING);
        assertThat(r.outcome()).isEqualTo(AttemptStore.TransitionOutcome.NOT_FOUND);
        assertThat(store.size()).isZero();
    }

    @Test
    void blankPhaseIsIllegal() {
        store.prepare(req("a-1", "ins-1", "h-1"));
        AttemptStore.TransitionResult r = store.transition("a-1", 0, " ");
        assertThat(r.outcome()).isEqualTo(AttemptStore.TransitionOutcome.ILLEGAL_TRANSITION);
    }

    // ── guard on every mutation ────────────────────────────────────────────

    @Test
    void guardRejectsPreparedIdentityColumns() {
        assertThatThrownBy(() -> store.assertWritableColumns(
                ExecutionAttemptsColumns.EXECUTION_ATTEMPT_ID,
                ExecutionAttemptsColumns.ACCOUNT_SCOPE_ID,
                ExecutionAttemptsColumns.INSTRUCTION_ID,
                ExecutionAttemptsColumns.ACTION_ID,
                ExecutionAttemptsColumns.EXECUTION_PARTITION_ID,
                ExecutionAttemptsColumns.REQUEST_HASH,
                ExecutionAttemptsColumns.CLIENT_ORDER_REF,
                ExecutionAttemptsColumns.GATE_EPOCH,
                ExecutionAttemptsColumns.GATE_FENCE_TOKEN,
                ExecutionAttemptsColumns.SCHEMA_VERSION))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("identity");
    }

    @Test
    void guardRejectsForeignBrokerAdapterGroup() {
        assertThatThrownBy(() -> store.assertWritableColumns(
                ExecutionAttemptsColumns.BROKER_ORDER_ID,
                ExecutionAttemptsColumns.OUTCOME,
                ExecutionAttemptsColumns.OUTCOME_DETAIL,
                ExecutionAttemptsColumns.SUBMITTED_TS,
                ExecutionAttemptsColumns.TERMINAL_TS,
                ExecutionAttemptsColumns.BROKER_RESPONSE_SUMMARY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("broker-adapter");
    }

    @Test
    void guardAllowsAttemptStoreOwnedGroup() {
        // the exact mutable group prepare()/transition() write — must always pass
        store.assertWritableColumns(ExecutionAttemptsColumns.PHASE,
                ExecutionAttemptsColumns.PHASE_EPOCH,
                ExecutionAttemptsColumns.PREPARED_TS,
                ExecutionAttemptsColumns.RETRY_ATTEMPT);
        // and prepare() itself runs this assertion on every mint
        AttemptStore.PrepareResult r = store.prepare(req("a-1", "ins-1", "h-1"));
        assertThat(r.status()).isEqualTo(AttemptStore.Status.CREATED);
    }

    @Test
    void guardFailsTransitionClosedOnMatrixDrift() {
        // Drift: PHASE becomes identity (creation-only). The transition path
        // must fail closed at checkWrite BEFORE mutating — proving the guard is
        // load-bearing on every mutation, not decorative.
        ColumnOwnership drifted = new ColumnOwnership(
                ExecutionAttemptsColumnOwnership.TABLE_NAME, "2", "executor",
                ExecutionAttemptsColumns.NAMES.toArray(new String[0]),
                new int[] {ExecutionAttemptsColumns.EXECUTION_ATTEMPT_ID,
                        ExecutionAttemptsColumns.ACCOUNT_SCOPE_ID,
                        ExecutionAttemptsColumns.INSTRUCTION_ID,
                        ExecutionAttemptsColumns.ACTION_ID,
                        ExecutionAttemptsColumns.EXECUTION_PARTITION_ID,
                        ExecutionAttemptsColumns.REQUEST_HASH,
                        ExecutionAttemptsColumns.CLIENT_ORDER_REF,
                        ExecutionAttemptsColumns.GATE_EPOCH,
                        ExecutionAttemptsColumns.GATE_FENCE_TOKEN,
                        ExecutionAttemptsColumns.PHASE,          // drifted to identity
                        ExecutionAttemptsColumns.SCHEMA_VERSION},
                new ColumnOwnership.Writer(ExecutionAttemptsColumnOwnership.WRITER_ATTEMPT_STORE,
                        ExecutionAttemptsColumns.BROKER_ORDER_ID,
                        ExecutionAttemptsColumns.PHASE_EPOCH,
                        ExecutionAttemptsColumns.OUTCOME,
                        ExecutionAttemptsColumns.OUTCOME_DETAIL,
                        ExecutionAttemptsColumns.PREPARED_TS,
                        ExecutionAttemptsColumns.SUBMITTED_TS,
                        ExecutionAttemptsColumns.TERMINAL_TS,
                        ExecutionAttemptsColumns.BROKER_RESPONSE_SUMMARY,
                        ExecutionAttemptsColumns.RETRY_ATTEMPT));

        InMemoryAttemptStore driftedStore =
                new InMemoryAttemptStore(halts::incrementAndGet, drifted);
        driftedStore.seedForTest(preparedRecord("a-1", "ins-1", "h-1"));

        assertThatThrownBy(() -> driftedStore.transition("a-1", 0, AttemptRecord.PHASE_SUBMITTING))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("identity");
        // fail-closed: the record is untouched
        assertThat(driftedStore.attemptById("a-1").phase()).isEqualTo(AttemptRecord.PHASE_PREPARED);
        assertThat(driftedStore.attemptById("a-1").phaseEpoch()).isZero();
    }

    @Test
    void prepareRejectsNullRequest() {
        assertThatThrownBy(() -> store.prepare(null))
                .isInstanceOf(NullPointerException.class);
    }
}
