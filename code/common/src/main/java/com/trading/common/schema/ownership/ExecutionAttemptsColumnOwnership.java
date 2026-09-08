package com.trading.common.schema.ownership;

/**
 * Writer/column ownership matrix for Execution_Attempts
 * ({@code code/01_platform/02_sql/ddl/12_execution_attempts.sql} v3, SCH-15).
 *
 * <p>Executor is the sole table owner (contract docs/04_contracts/07-executor.md;
 * {@link com.trading.common.ownership.OwnershipMatrix} row "order
 * gate/attempt/mapping/audit"), but two sub-modules update disjoint column
 * groups of the same row, which is exactly where {@code partial_update}
 * column ownership is load-bearing:
 * <ul>
 *   <li>{@code attempt-store} owns the transition state — {@code phase} /
 *       {@code phase_epoch} (monotonic, R-234), {@code prepared_ts},
 *       {@code retry_attempt}. It mints the PREPARED attempt and applies every
 *       legal phase transition (incrementing {@code phase_epoch} by exactly 1).</li>
 *   <li>{@code broker-adapter} owns the call-evidence group —
 *       {@code broker_order_id}, {@code outcome}/{@code outcome_detail},
 *       {@code submitted_ts}/{@code terminal_ts},
 *       {@code broker_response_summary}. Reconciliation (UNKNOWN resolution)
 *       writes through this same group under the stale-version guard, never
 *       through the transition-state group.</li>
 * </ul>
 *
 * <p>Identity columns — the PK {@code execution_attempt_id}, account /
 * instruction / partition scope, {@code request_hash} + {@code client_order_ref}
 * (deterministic attempt identity), the authorization pair {@code gate_epoch} +
 * {@code gate_fence_token} (the exact fence token that authorized the attempt,
 * stored at creation, CHG-044), and {@code schema_version} — are written once at
 * PREPARED and never partial-updated.
 */
public final class ExecutionAttemptsColumnOwnership {

    private ExecutionAttemptsColumnOwnership() {}

    public static final String TABLE_NAME = "Execution_Attempts";
    public static final String OWNER = "executor";
    public static final String WRITER_ATTEMPT_STORE = "executor:attempt-store";
    public static final String WRITER_BROKER_ADAPTER = "executor:broker-adapter";

    /** Identity: 0-6 (PK + scope + request identity) + 8 (gate_epoch) + 18-19
     * (schema_version, gate_fence_token — authorization captured at creation). */
    private static final int[] IDENTITY = {
            ExecutionAttemptsColumns.EXECUTION_ATTEMPT_ID,
            ExecutionAttemptsColumns.ACCOUNT_SCOPE_ID,
            ExecutionAttemptsColumns.INSTRUCTION_ID,
            ExecutionAttemptsColumns.ACTION_ID,
            ExecutionAttemptsColumns.EXECUTION_PARTITION_ID,
            ExecutionAttemptsColumns.REQUEST_HASH,
            ExecutionAttemptsColumns.CLIENT_ORDER_REF,
            ExecutionAttemptsColumns.GATE_EPOCH,
            ExecutionAttemptsColumns.SCHEMA_VERSION,
            ExecutionAttemptsColumns.GATE_FENCE_TOKEN
    };

    public static final ColumnOwnership MATRIX = new ColumnOwnership(
            TABLE_NAME,
            ExecutionAttemptsColumns.SCHEMA_VERSION_V3,
            OWNER,
            ExecutionAttemptsColumns.NAMES.toArray(new String[0]),
            IDENTITY,
            new ColumnOwnership.Writer(WRITER_ATTEMPT_STORE,
                    ExecutionAttemptsColumns.PHASE,
                    ExecutionAttemptsColumns.PHASE_EPOCH,
                    ExecutionAttemptsColumns.PREPARED_TS,
                    ExecutionAttemptsColumns.RETRY_ATTEMPT),
            new ColumnOwnership.Writer(WRITER_BROKER_ADAPTER,
                    ExecutionAttemptsColumns.BROKER_ORDER_ID,
                    ExecutionAttemptsColumns.OUTCOME,
                    ExecutionAttemptsColumns.OUTCOME_DETAIL,
                    ExecutionAttemptsColumns.SUBMITTED_TS,
                    ExecutionAttemptsColumns.TERMINAL_TS,
                    ExecutionAttemptsColumns.BROKER_RESPONSE_SUMMARY)
    );
}
