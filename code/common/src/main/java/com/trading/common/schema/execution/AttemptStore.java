package com.trading.common.schema.execution;

import java.util.Objects;

/**
 * Attempt lifecycle store (docs/08_implementation/05-execution-core.md &rarr;
 * "Attempt rules"): mints exactly one PREPARED attempt per
 * (instruction_id, request_hash), refuses to rewrite an attempt's identity,
 * and applies only legal phase transitions with a monotonic {@code phase_epoch}.
 *
 * <p>The {@code prepare} contract, per the dossier:
 * <ul>
 *   <li>a new (instruction_id, request_hash) mints one PREPARED attempt
 *       ({@link AttemptRecord#prepared});</li>
 *   <li>a duplicate (instruction_id, request_hash) returns the existing attempt
 *       without creating a second record and without touching its identity;</li>
 *   <li>a modified decision under an existing instruction_id (same id, different
 *       request_hash) is a contract violation: the store requests a halt through
 *       its callback and does not mutate anything.</li>
 * </ul>
 *
 * <p>The transition contract, per the dossier:
 * <ul>
 *   <li>legal transitions are PREPARED &rarr; SUBMITTING; SUBMITTING &rarr;
 *       ACCEPTED / REJECTED / CANCELLED / UNKNOWN; UNKNOWN &rarr;
 *       ACCEPTED / REJECTED / CANCELLED — the UNKNOWN exits only through
 *       {@link #resolveUnknown} (explicit reconciliation result), never via
 *       {@link #transition} (blocks new submissions and auto-retry);</li>
 *   <li>every accepted transition increments {@code phase_epoch} by exactly 1;</li>
 *   <li>a stale {@code phase_epoch} update (expected != current) is rejected
 *       without mutation;</li>
 *   <li>terminal phases (ACCEPTED / REJECTED / CANCELLED) cannot transition
 *       again.</li>
 * </ul>
 */
public interface AttemptStore {

    enum Status {
        /** A new PREPARED attempt was minted. */
        CREATED,
        /**
         * (instruction_id, request_hash) already exists — the existing attempt is
         * returned. Caller-supplied {@code executionAttemptId}/{@code clientOrderRef}
         * in the duplicate request are ignored and MUST NOT overwrite the stored
         * identity (P3-357).
         */
        DUPLICATE,
        /**
         * Same instruction_id, different request_hash — identity cannot be rebound.
         * The returned {@code record} is the existing attempt bound to that
         * instruction_id (or {@code null} if the store's index has diverged); the
         * store requests a halt and mutates nothing (P3-357).
         */
        CONTRACT_VIOLATION
    }

    /**
     * All identities and creation inputs are caller-supplied (deterministic).
     * The three deterministic identities are mandatory and non-blank: a null or
     * blank value must fail fast at construction rather than mint a corrupt
     * {@code "null"}-keyed attempt (P3-358, mirrors {@code ExecutionCommandGate.Command}).
     */
    record PrepareRequest(
            String executionAttemptId,
            String accountScopeId,
            String instructionId,
            String actionId,
            String executionPartitionId,
            String requestHash,
            String clientOrderRef,
            long gateFenceToken,
            long gateEpoch,
            long nowTs) {
        public PrepareRequest {
            requireIdentity(executionAttemptId, "executionAttemptId");
            requireIdentity(instructionId, "instructionId");
            requireIdentity(requestHash, "requestHash");
        }
    }

    private static String requireIdentity(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must be non-blank");
        }
        return value;
    }

    /**
     * Outcome of {@link #prepare}. {@code record} is non-null for CREATED and
     * DUPLICATE (the minted / existing attempt); on CONTRACT_VIOLATION it is the
     * conflicting attempt or null when the store's index has diverged (P3-360).
     */
    record PrepareResult(Status status, AttemptRecord record, String reason) {
        public PrepareResult {
            Objects.requireNonNull(status, "status");
            if ((status == Status.CREATED || status == Status.DUPLICATE) && record == null) {
                throw new IllegalArgumentException("record must be non-null on " + status);
            }
        }

        public static PrepareResult created(AttemptRecord record) {
            return new PrepareResult(Status.CREATED, record, null);
        }

        public static PrepareResult duplicate(AttemptRecord record) {
            return new PrepareResult(Status.DUPLICATE, record, "duplicate (instruction_id, request_hash)");
        }

        public static PrepareResult contractViolation(AttemptRecord record, String reason) {
            return new PrepareResult(Status.CONTRACT_VIOLATION, record, reason);
        }
    }

    enum TransitionOutcome {
        /** Phase/phase_epoch updated (phase_epoch + 1). */
        APPLIED,
        /** Expected phase_epoch != current — rejected without mutation. */
        STALE_EPOCH,
        /** The from&rarr;to move is not in the legal map — rejected without mutation. */
        ILLEGAL_TRANSITION,
        /** The attempt is already in a terminal phase — cannot transition again. */
        TERMINAL,
        /** No attempt with that execution_attempt_id. */
        NOT_FOUND
    }

    /**
     * Outcome of {@link #transition}/{@link #resolveUnknown}. {@code record} is
     * non-null on APPLIED and on rejections that name the current row; it is null
     * only for NOT_FOUND and for a blank-phase ILLEGAL_TRANSITION, where no attempt
     * was resolved (P3-360).
     */
    record TransitionResult(TransitionOutcome outcome, AttemptRecord record, String reason) {
        public TransitionResult {
            Objects.requireNonNull(outcome, "outcome");
            if (outcome == TransitionOutcome.APPLIED && record == null) {
                throw new IllegalArgumentException("record must be non-null on APPLIED");
            }
        }

        public static TransitionResult applied(AttemptRecord record) {
            return new TransitionResult(TransitionOutcome.APPLIED, record, null);
        }

        public static TransitionResult rejected(TransitionOutcome outcome, AttemptRecord record,
                                                String reason) {
            return new TransitionResult(outcome, record, reason);
        }
    }

    /**
     * Mints, or returns, the single attempt for {@code (instruction_id, request_hash)}.
     *
     * <p>Atomicity (P3-131): implementations MUST linearize concurrent {@code prepare}
     * calls on the same replay key — exactly one CREATED, the rest DUPLICATE — and MUST
     * apply {@link #transition}/{@link #resolveUnknown} as a compare-and-set on
     * {@code (execution_attempt_id, phase_epoch)}. An implementation MAY be single-writer
     * (documented as such, like {@link InMemoryAttemptStore}) or thread-safe; it MUST NOT
     * be silently neither.
     *
     * <p>Contract violation (P3-130): a modified decision under an existing
     * {@code instruction_id} (same id, different {@code request_hash}) returns
     * {@link Status#CONTRACT_VIOLATION}; the implementation MUST request a halt (e.g. via
     * a constructor-injected callback) and MUST NOT mutate state. Callers MUST treat this
     * result as must-halt even if the store already halted (the halt must be idempotent).
     *
     * @throws NullPointerException/IllegalArgumentException if the request identities are
     *         null or blank (see {@link PrepareRequest})
     */
    PrepareResult prepare(PrepareRequest request);

    /**
     * Applies a submission-path phase transition. The UNKNOWN phase cannot exit
     * through this method — use {@link #resolveUnknown} (explicit reconciliation
     * result). Returns the updated record on {@code APPLIED}; on any rejection
     * the store is unchanged.
     *
     * @param executionAttemptId  target attempt
     * @param expectedPhaseEpoch  caller's view of the current phase_epoch; a
     *                            mismatch rejects the update as stale without
     *                            mutation
     * @param newPhase            the phase to move to; one of the
     *                            {@link AttemptRecord#PHASE_PREPARED} /
     *                            {@code PHASE_*} constants (equivalently
     *                            {@link com.trading.common.model.AttemptPhase#name()}).
     *                            Kept as {@code String} for durable/Fluss and
     *                            cross-language interop (P3-361). A null, blank, or
     *                            unknown value is rejected without mutation, and
     *                            {@code UNKNOWN} must never be passed here — use
     *                            {@link #resolveUnknown}.
     */
    TransitionResult transition(String executionAttemptId, long expectedPhaseEpoch,
                                String newPhase);

    /**
     * Resolves an UNKNOWN attempt to a terminal phase — the ONLY way out of
     * UNKNOWN, and only through an explicit reconciliation result (the caller
     * supplies the resolved phase after verified broker/order evidence). Any
     * non-UNKNOWN attempt, or a resolved phase outside ACCEPTED / REJECTED /
     * CANCELLED, is rejected without mutation.
     */
    TransitionResult resolveUnknown(String executionAttemptId, long expectedPhaseEpoch,
                                    String resolvedPhase);
}
