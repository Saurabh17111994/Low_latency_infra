package com.trading.common.schema.execution;

import com.trading.common.model.AttemptPhase;
import com.trading.common.schema.ownership.ExecutionAttemptsColumns;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable Execution_Attempts row
 * ({@code code/01_platform/02_sql/ddl/12_execution_attempts.sql} v3) — the
 * executor attempt protocol's durable record
 * (docs/08_implementation/05-execution-core.md &rarr; "Attempt protocol").
 *
 * <p>P3-355: the record component order below is <b>not</b> the DDL column
 * order. Persistence MUST map by {@link ExecutionAttemptsColumns} index (as
 * {@link FlussAttemptStore} does); positional serialization of the components
 * would swap {@code gateFenceToken}/{@code brokerOrderId}/{@code gateEpoch}.
 * For reference, the DDL places {@code client_order_ref} at 6,
 * {@code broker_order_id} at 7, {@code gate_epoch} at 8, {@code phase} at 9,
 * …, {@code gate_fence_token} at 18, {@code schema_version} at 19.
 *
 * <p>Identity columns are fixed at minting and never rewritten: an attempt's
 * {@code execution_attempt_id}, scope, {@code request_hash}, and
 * {@code client_order_ref}, plus the authorization captured at creation —
 * {@code gate_epoch} and {@code gate_fence_token} (the exact fence token that
 * was validated before the bridge call, T5/CHG-044) — identify it for its
 * whole life. The mutable group ({@code phase}/{@code phase_epoch}/
 * {@code retry_attempt}) is owned by the
 * attempt-store; the call-evidence group (broker order id, outcome,
 * submitted/terminal timestamps, response summary) is owned by the
 * broker-adapter (see {@code ExecutionAttemptsColumnOwnership}).
 */
public record AttemptRecord(
        String executionAttemptId,
        String accountScopeId,
        String instructionId,
        String actionId,
        String executionPartitionId,
        String requestHash,
        String clientOrderRef,
        long gateFenceToken,
        String brokerOrderId,
        long gateEpoch,
        String phase,
        long phaseEpoch,
        String outcome,
        String outcomeDetail,
        long preparedTs,
        Long submittedTs,
        Long terminalTs,
        String brokerResponseSummary,
        int retryAttempt,
        String schemaVersion) {

    /**
     * P3-128: a compact constructor exists only to validate the phase — every
     * other column is mapped by index and validated by its owning store.
     */
    public AttemptRecord {
        requireCanonicalPhase(phase);
    }

    // P3-128: derived from the canonical matrix so the two can never drift
    // (the strings are identical to the former literals).
    public static final String PHASE_PREPARED = AttemptPhase.PREPARED.name();
    public static final String PHASE_SUBMITTING = AttemptPhase.SUBMITTING.name();
    public static final String PHASE_ACCEPTED = AttemptPhase.ACCEPTED.name();
    public static final String PHASE_REJECTED = AttemptPhase.REJECTED.name();
    public static final String PHASE_CANCELLED = AttemptPhase.CANCELLED.name();
    public static final String PHASE_UNKNOWN = AttemptPhase.UNKNOWN.name();

    /** Terminal phases cannot transition again (dossier "Attempt rules"). */
    public static final Set<String> TERMINAL_PHASES =
            Set.of(PHASE_ACCEPTED, PHASE_REJECTED, PHASE_CANCELLED);

    /**
     * Mints the PREPARED attempt (phase_epoch = 0, retry_attempt = 0,
     * prepared_ts = nowTs, call-evidence columns null). Caller supplies the
     * deterministic identities — including {@code execution_attempt_id} and
     * {@code client_order_ref} — per the dossier (no UUID / wall-clock inside
     * the store). The {@code gateFenceToken} is the durable fence token that
     * authorized this attempt (persisted at PREPARED, exactly as the dossier's
     * "PREPARED (request hash + client ref + gate epoch + fence)").
     */
    public static AttemptRecord prepared(String executionAttemptId, String accountScopeId,
                                         String instructionId, String actionId,
                                         String executionPartitionId, String requestHash,
                                         String clientOrderRef, long gateFenceToken,
                                         long gateEpoch, long nowTs) {
        // P3-356: deterministic identities are mandatory and non-blank — a null
        // must fail fast here, not flow into a store's replayKey ("null") or NPE
        // later in FlussAttemptStore.persist at BinaryString.fromString(null).
        requireIdentity(executionAttemptId, "executionAttemptId");
        requireIdentity(accountScopeId, "accountScopeId");
        requireIdentity(instructionId, "instructionId");
        requireIdentity(executionPartitionId, "executionPartitionId");
        requireIdentity(requestHash, "requestHash");
        requireIdentity(clientOrderRef, "clientOrderRef");
        return new AttemptRecord(executionAttemptId, accountScopeId, instructionId, actionId,
                executionPartitionId, requestHash, clientOrderRef, gateFenceToken, null, gateEpoch,
                PHASE_PREPARED, 0L, null, null, nowTs, null, null, null, 0,
                ExecutionAttemptsColumns.SCHEMA_VERSION_V3);
    }

    private static void requireIdentity(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must be non-blank");
        }
    }

    /**
     * Returns a copy with the given phase and {@code phaseEpoch + 1} — the only
     * columns the attempt-store transitions touch (phase/phase_epoch). Every
     * other column (identity, call evidence) is preserved untouched.
     *
     * <p>P3-128/P3-129: {@code newPhase} must be a canonical
     * {@link AttemptPhase} name (an arbitrary string is rejected), and a terminal
     * attempt (ACCEPTED / REJECTED / CANCELLED) can never be resurrected — the
     * terminal matrix is enforced here, not only at the store's call site.
     */
    public AttemptRecord withPhase(String newPhase) {
        requireCanonicalPhase(newPhase);
        if (isTerminalPhase(phase)) {
            throw new IllegalStateException("terminal phase " + phase + " cannot transition");
        }
        return new AttemptRecord(executionAttemptId, accountScopeId, instructionId, actionId,
                executionPartitionId, requestHash, clientOrderRef, gateFenceToken, brokerOrderId, gateEpoch,
                newPhase, phaseEpoch + 1, outcome, outcomeDetail, preparedTs, submittedTs,
                terminalTs, brokerResponseSummary, retryAttempt, schemaVersion);
    }

    /**
     * P3-128: only canonical {@link AttemptPhase} names may enter an attempt row.
     * Rejecting here means the public canonical constructor and
     * {@code FlussAttemptStore.fromRow} cannot carry an arbitrary string (e.g.
     * "BOGUS" or lowercase "accepted") past the canonical matrix and into the
     * durable store, where it would fail constraints or violate state-machine
     * assumptions downstream. A corrupt durable row now fails loud on decode,
     * matching the P3-366/fromRow precedent.
     */
    private static void requireCanonicalPhase(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("phase must be non-blank");
        }
        try {
            AttemptPhase.valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unknown phase: " + value);
        }
    }

    /** Whether {@code value} names a canonical terminal phase; unknown/null ⇒ false. */
    private static boolean isTerminalPhase(String value) {
        if (value == null) {
            return false;
        }
        try {
            return AttemptPhase.valueOf(value).isTerminal();
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
