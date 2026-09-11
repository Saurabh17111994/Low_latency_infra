package com.trading.common.schema.execution;

import com.trading.common.model.AttemptPhase;
import com.trading.common.model.GateTransitionValidator;
import com.trading.common.schema.ownership.ColumnOwnership;
import com.trading.common.schema.ownership.ExecutionAttemptsColumnOwnership;
import com.trading.common.schema.ownership.ExecutionAttemptsColumns;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * In-memory {@link AttemptStore} — the pure-JVM core of the executor attempt
 * lifecycle (docs/08_implementation/05-execution-core.md &rarr; attempt protocol), and the
 * first production consumer of the SCH-15 column-ownership guard
 * ({@link ColumnOwnership#checkWrite}).
 *
 * <p>The guard runs on every mutation:
 * <ul>
 *   <li><b>Store-level identity guard ({@link #prepare}):</b> a duplicate
 *       (instruction_id, request_hash) returns the existing PREPARED attempt
 *       untouched; a modified decision under an existing instruction_id (same
 *       id, different hash) is a contract violation — the halt callback fires
 *       and no row is created or mutated. The identity columns of the PREPARED
 *       attempt are never rewritten after minting.</li>
 *   <li><b>Matrix self-assertion (every mutation):</b> before accepting a minted
 *       row or an applied transition, {@code checkWrite(WRITER_ATTEMPT_STORE, …)}
 *       runs over the exact columns the mutation writes (prepare: phase,
 *       phase_epoch, prepared_ts, retry_attempt; transitions: phase,
 *       phase_epoch). If the ownership matrix ever drifts — those columns
 *       reassigned to identity or to the broker-adapter group — the store fails
 *       closed at the first write instead of silently writing columns it no
 *       longer owns.</li>
 * </ul>
 *
 * <p>Transition rules (dossier "Attempt rules"):
 * <ul>
 *   <li>legal moves are PREPARED &rarr; SUBMITTING; SUBMITTING &rarr; ACCEPTED /
 *       REJECTED / CANCELLED / UNKNOWN (via {@link #transition}); UNKNOWN &rarr;
 *       ACCEPTED / REJECTED / CANCELLED only via {@link #resolveUnknown} (explicit
 *       reconciliation) — UNKNOWN can never go back to SUBMITTING, so there is no
 *       automatic retry;</li>
 *   <li>every accepted transition increments {@code phase_epoch} by exactly 1;</li>
 *   <li>a stale expected {@code phase_epoch} rejects the update without mutation;</li>
 *   <li>terminal phases (ACCEPTED / REJECTED / CANCELLED) cannot transition again.</li>
 * </ul>
 *
 * <p>Not thread-safe; single-writer executor. The Fluss-backed store lands with
 * the {@code 04_executor} module; transitions only ever touch phase/phase_epoch —
 * call-evidence columns (outcome, submitted/terminal timestamps, broker order id)
 * belong to the broker-adapter's group and are never written here.
 *
 * <p>Since T5 (CHG-044) the legal matrices are no longer hand-maintained maps:
 * they are derived from the canonical {@link AttemptPhase#legalTargets} matrix
 * via {@link GateTransitionValidator} (submission vs reconciliation routing)
 * so the validator, this store, and the Rust gate can never disagree again.
 * The submission path keeps UNKNOWN's exits out (they are reconciliation-only).
 */
public final class InMemoryAttemptStore implements AttemptStore {

    private static final ColumnOwnership MATRIX = ExecutionAttemptsColumnOwnership.MATRIX;
    private static final String WRITER = ExecutionAttemptsColumnOwnership.WRITER_ATTEMPT_STORE;

    /** Submission-path legal transitions (dossier). Derived from the canonical
     * matrix via the validator, minus the reconciliation-only UNKNOWN exits. */
    private static final Map<String, Set<String>> SUBMIT_TRANSITIONS = deriveTransitions(false);

    /** Reconciliation-path legal transitions (explicit result only). */
    private static final Map<String, Set<String>> RESOLVE_TRANSITIONS = deriveTransitions(true);

    private static Map<String, Set<String>> deriveTransitions(boolean reconciliation) {
        Map<String, Set<String>> out = new HashMap<>();
        for (AttemptPhase from : AttemptPhase.values()) {
            Set<String> targets = new HashSet<>();
            for (AttemptPhase to : from.legalTargets()) {
                boolean submitLegal = GateTransitionValidator.isSubmissionLegal(from, to);
                boolean reconcileOnly = GateTransitionValidator.isReconciliationOnly(from, to);
                if (reconciliation ? reconcileOnly : submitLegal) {
                    targets.add(to.name());
                }
            }
            if (!targets.isEmpty()) {
                out.put(from.name(), Set.copyOf(targets));
            }
        }
        return Map.copyOf(out);
    }

    private final ColumnOwnership matrix;
    private final Map<String, AttemptRecord> byAttemptId = new LinkedHashMap<>();
    private final Map<String, String> instructionToAttemptId = new HashMap<>();
    private final Map<String, String> instructionHashToAttemptId = new HashMap<>();
    private final Runnable haltCallback;

    public InMemoryAttemptStore(Runnable haltCallback) {
        this(haltCallback, MATRIX);
    }

    /**
     * Test seam: inject a different ownership matrix so a drift (e.g. phase
     * moved to identity or to another writer) can be proven to fail the store
     * closed at the first mutation.
     */
    InMemoryAttemptStore(Runnable haltCallback, ColumnOwnership matrix) {
        this.haltCallback = Objects.requireNonNull(haltCallback, "haltCallback");
        this.matrix = Objects.requireNonNull(matrix, "matrix");
    }

    private static String replayKey(String instructionId, String requestHash) {
        return instructionId + "\u0000" + requestHash;
    }

    @Override
    public PrepareResult prepare(PrepareRequest request) {
        Objects.requireNonNull(request, "request");
        String replayKey = replayKey(request.instructionId(), request.requestHash());
        String replayAttemptId = instructionHashToAttemptId.get(replayKey);
        if (replayAttemptId != null) {
            // Duplicate (instruction_id, request_hash): return the existing
            // PREPARED attempt untouched — identity is never rewritten.
            return PrepareResult.duplicate(byAttemptId.get(replayAttemptId));
        }
        String existingAttemptId = instructionToAttemptId.get(request.instructionId());
        if (existingAttemptId != null) {
            // Same instruction_id, different request_hash: a modified decision
            // under an existing instruction identity. Contract violation — halt
            // and do not mutate anything.
            haltCallback.run();
            return PrepareResult.contractViolation(byAttemptId.get(existingAttemptId),
                    "modified decision under existing instruction_id: " + request.instructionId());
        }

        AttemptRecord created = AttemptRecord.prepared(
                request.executionAttemptId(), request.accountScopeId(),
                request.instructionId(), request.actionId(),
                request.executionPartitionId(), request.requestHash(),
                request.clientOrderRef(), request.gateFenceToken(),
                request.gateEpoch(), request.nowTs());

        // Guard on every mutation: the mutable group this store writes must
        // still belong to the attempt-store (and none of it be identity).
        assertWritableColumns(ExecutionAttemptsColumns.PHASE,
                ExecutionAttemptsColumns.PHASE_EPOCH,
                ExecutionAttemptsColumns.PREPARED_TS,
                ExecutionAttemptsColumns.RETRY_ATTEMPT);

        byAttemptId.put(created.executionAttemptId(), created);
        instructionToAttemptId.put(request.instructionId(), created.executionAttemptId());
        instructionHashToAttemptId.put(replayKey, created.executionAttemptId());
        return PrepareResult.created(created);
    }

    /**
     * Restart-refresh: install an attempt recovered from the durable Execution_Attempts row so a
     * restarted process rebuilds its duplicate/contract-violation index. First-writer wins for
     * the secondary indexes (a replayed identity must not overwrite a newer durable mapping);
     * the by-id map always reflects the latest durable row for that id.
     */
    public synchronized void hydrate(AttemptRecord rec) {
        Objects.requireNonNull(rec, "rec");
        byAttemptId.put(rec.executionAttemptId(), rec);
        instructionToAttemptId.putIfAbsent(rec.instructionId(), rec.executionAttemptId());
        instructionHashToAttemptId.putIfAbsent(
                replayKey(rec.instructionId(), rec.requestHash()), rec.executionAttemptId());
    }

    @Override
    public TransitionResult transition(String executionAttemptId, long expectedPhaseEpoch,
                                       String newPhase) {
        return applyTransition(executionAttemptId, expectedPhaseEpoch, newPhase,
                SUBMIT_TRANSITIONS, "illegal transition (submission path)");
    }

    @Override
    public TransitionResult resolveUnknown(String executionAttemptId, long expectedPhaseEpoch,
                                           String resolvedPhase) {
        return applyTransition(executionAttemptId, expectedPhaseEpoch, resolvedPhase,
                RESOLVE_TRANSITIONS, "resolveUnknown requires an UNKNOWN attempt and a terminal phase");
    }

    private TransitionResult applyTransition(String executionAttemptId, long expectedPhaseEpoch,
                                             String newPhase, Map<String, Set<String>> legal,
                                             String illegalReason) {
        if (newPhase == null || newPhase.isBlank()) {
            return TransitionResult.rejected(TransitionOutcome.ILLEGAL_TRANSITION, null,
                    "phase must be non-blank");
        }
        AttemptRecord current = byAttemptId.get(executionAttemptId);
        if (current == null) {
            return TransitionResult.rejected(TransitionOutcome.NOT_FOUND, null,
                    "no attempt " + executionAttemptId);
        }
        if (AttemptRecord.TERMINAL_PHASES.contains(current.phase())) {
            return TransitionResult.rejected(TransitionOutcome.TERMINAL, current,
                    "terminal phase " + current.phase() + " cannot transition");
        }
        Set<String> allowed = legal.get(current.phase());
        if (allowed == null || !allowed.contains(newPhase)) {
            return TransitionResult.rejected(TransitionOutcome.ILLEGAL_TRANSITION, current,
                    illegalReason + ": " + current.phase() + " -> " + newPhase);
        }
        if (current.phaseEpoch() != expectedPhaseEpoch) {
            return TransitionResult.rejected(TransitionOutcome.STALE_EPOCH, current,
                    "expected phase_epoch " + expectedPhaseEpoch + " but current is "
                            + current.phaseEpoch());
        }

        // Guard on every mutation: transitions only ever touch phase/phase_epoch
        // (the attempt-store group) — fail closed on matrix drift.
        assertWritableColumns(ExecutionAttemptsColumns.PHASE,
                ExecutionAttemptsColumns.PHASE_EPOCH);

        AttemptRecord next = current.withPhase(newPhase);
        byAttemptId.put(executionAttemptId, next);
        return TransitionResult.applied(next);
    }

    /**
     * The column-ownership guard surface for the attempt-store writer: any
     * column outside the attempt-store group (identity or broker-adapter's)
     * throws, naming the real owner. Called by every mutation path and
     * exercised directly by tests.
     */
    void assertWritableColumns(int... columns) {
        matrix.checkWrite(WRITER, columns);
    }

    /**
     * Test-only seam: seeds an attempt directly, bypassing prepare's minting
     * guards, so the transition/resolveUnknown paths can be exercised against
     * a deliberately drifted ownership matrix (fail-closed proof).
     */
    void seedForTest(AttemptRecord record) {
        byAttemptId.put(Objects.requireNonNull(record, "record").executionAttemptId(), record);
    }

    public AttemptRecord attemptById(String executionAttemptId) {
        return byAttemptId.get(executionAttemptId);
    }

    public int size() {
        return byAttemptId.size();
    }
}
