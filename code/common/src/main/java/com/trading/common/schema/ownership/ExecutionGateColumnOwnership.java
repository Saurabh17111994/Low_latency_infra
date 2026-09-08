package com.trading.common.schema.ownership;

/**
 * Writer/column ownership matrix for Execution_Gate
 * ({@code code/01_platform/02_sql/ddl/11_execution_gate.sql} v3, SCH-15).
 *
 * <p>Executor is the sole table owner (contract docs/04_contracts/07-executor.md),
 * but three sub-modules update disjoint column groups of the same row
 * ({@code partial_update} column ownership is load-bearing here):
 * <ul>
 *   <li>{@code gate-transition} owns the gate lifecycle — {@code state},
 *       {@code epoch} (incremented by exactly 1 on every accepted transition),
 *       {@code reason} / {@code detection_time} / {@code evidence_hash} /
 *       {@code transition_ts} recorded with each state change.</li>
 *   <li>{@code gate-fence} owns the partition lease — {@code owner_instance_id},
 *       {@code fence_token} (monotonic sequence, never reused),
 *       {@code fence_acquired_ts}, {@code lease_expires_ts},
 *       {@code fence_lost_ts}. The fence group is written only by the fenced
 *       leader under the deployment leadership mechanism (ASM-EXE-005 /
 *       REQ-EXE-012); a stale owner/sequence is rejected before every command.</li>
 *   <li>{@code gate-approvals} owns the approval group — {@code approval_1}
 *       (authenticated, authorized principal — {@code saurabh}, DEC-044), the
 *       optional {@code approval_2} (a second approval is not required and not
 *       checked, DEC-044) and {@code approved_evidence_hash} (the exact evidence
 *       hash the approval covered, so an epoch change invalidates the
 *       approval).</li>
 * </ul>
 *
 * <p>Identity columns — the PK {@code execution_partition_id},
 * {@code account_scope_id} (account-safe routing, R-233) and
 * {@code schema_version} — are written at row creation and never
 * partial-updated.
 */
public final class ExecutionGateColumnOwnership {

    private ExecutionGateColumnOwnership() {}

    public static final String TABLE_NAME = "Execution_Gate";
    public static final String OWNER = "executor";
    public static final String WRITER_GATE_TRANSITION = "executor:gate-transition";
    public static final String WRITER_GATE_FENCE = "executor:gate-fence";
    public static final String WRITER_GATE_APPROVALS = "executor:gate-approvals";

    /** Identity: PK (0) + account scope (1) + schema_version (16, last column). */
    private static final int[] IDENTITY = {
            ExecutionGateColumns.EXECUTION_PARTITION_ID,
            ExecutionGateColumns.ACCOUNT_SCOPE_ID,
            ExecutionGateColumns.SCHEMA_VERSION
    };

    public static final ColumnOwnership MATRIX = new ColumnOwnership(
            TABLE_NAME,
            ExecutionGateColumns.SCHEMA_VERSION_V3,
            OWNER,
            ExecutionGateColumns.NAMES.toArray(new String[0]),
            IDENTITY,
            new ColumnOwnership.Writer(WRITER_GATE_TRANSITION,
                    ExecutionGateColumns.STATE,
                    ExecutionGateColumns.EPOCH,
                    ExecutionGateColumns.REASON,
                    ExecutionGateColumns.DETECTION_TIME,
                    ExecutionGateColumns.EVIDENCE_HASH,
                    ExecutionGateColumns.TRANSITION_TS),
            new ColumnOwnership.Writer(WRITER_GATE_FENCE,
                    ExecutionGateColumns.OWNER_INSTANCE_ID,
                    ExecutionGateColumns.FENCE_TOKEN,
                    ExecutionGateColumns.FENCE_ACQUIRED_TS,
                    ExecutionGateColumns.LEASE_EXPIRES_TS,
                    ExecutionGateColumns.FENCE_LOST_TS),
            new ColumnOwnership.Writer(WRITER_GATE_APPROVALS,
                    ExecutionGateColumns.APPROVAL_1,
                    ExecutionGateColumns.APPROVAL_2,
                    ExecutionGateColumns.APPROVED_EVIDENCE_HASH)
    );
}
