package com.trading.common.schema.ownership;

import java.util.List;

/**
 * Physical column layout of the Execution_Gate KV state table
 * ({@code code/01_platform/02_sql/ddl/11_execution_gate.sql} v3). Pinned by
 * {@code ExecutionGateColumnsAgreementTest} against the DDL file. Key
 * {@code execution_partition_id}.
 *
 * <p>v3 (CHG-044, T5) added the fencing representation the epoch-only DDL
 * lacked: {@code owner_instance_id} (fenced executor instance), {@code fence_token}
 * (monotonic per-partition owner sequence, never reused), {@code fence_acquired_ts}
 * / {@code fence_lost_ts} (acquisition/loss evidence), {@code lease_expires_ts},
 * and {@code approved_evidence_hash} (the exact evidence hash the single-operator
 * approval covered, DEC-044; {@code approval_2} is optional — a second approval
 * is not required and not checked). {@code epoch} is the gate-generation value and is not a substitute
 * for the fence token.
 */
public final class ExecutionGateColumns {

    private ExecutionGateColumns() {}

    public static final int EXECUTION_PARTITION_ID = 0;
    public static final int ACCOUNT_SCOPE_ID = 1;
    public static final int STATE = 2;
    public static final int EPOCH = 3;
    public static final int REASON = 4;
    public static final int DETECTION_TIME = 5;
    public static final int EVIDENCE_HASH = 6;
    public static final int APPROVAL_1 = 7;
    public static final int APPROVAL_2 = 8;
    public static final int TRANSITION_TS = 9;
    public static final int OWNER_INSTANCE_ID = 10;
    public static final int FENCE_TOKEN = 11;
    public static final int FENCE_ACQUIRED_TS = 12;
    public static final int LEASE_EXPIRES_TS = 13;
    public static final int FENCE_LOST_TS = 14;
    public static final int APPROVED_EVIDENCE_HASH = 15;
    public static final int SCHEMA_VERSION = 16;

    public static final int FIELD_COUNT = 17;

    public static final String SCHEMA_VERSION_V3 = "3";

    /** Fluss {@code DataTypeRoot} name per column, DDL index order. */
    public static final List<String> TYPE_ROOTS = List.of(
            "STRING", "STRING", "STRING", "BIGINT", "STRING", "BIGINT", "STRING",
            "STRING", "STRING", "BIGINT", "STRING", "BIGINT", "BIGINT", "BIGINT",
            "BIGINT", "STRING", "STRING");

    /** DDL nullability per column (11 DDL v3). */
    public static final List<Boolean> COLUMN_NULLABLE_IN_DDL = List.of(
            false, false, false, false, true, true, true, true, true, false,
            true, true, true, true, true, true, false);

    /** DDL column names in index order (diagnostics + agreement pin). Immutable. */
    public static final List<String> NAMES = List.of(
            "execution_partition_id", "account_scope_id", "state", "epoch",
            "reason", "detection_time", "evidence_hash", "approval_1",
            "approval_2", "transition_ts", "owner_instance_id", "fence_token",
            "fence_acquired_ts", "lease_expires_ts", "fence_lost_ts",
            "approved_evidence_hash", "schema_version");

    static {
        if (NAMES.size() != FIELD_COUNT
                || TYPE_ROOTS.size() != FIELD_COUNT
                || COLUMN_NULLABLE_IN_DDL.size() != FIELD_COUNT) {
            throw new ExceptionInInitializerError("ExecutionGateColumns drift: FIELD_COUNT="
                    + FIELD_COUNT + " NAMES=" + NAMES.size()
                    + " TYPE_ROOTS=" + TYPE_ROOTS.size()
                    + " NULLABLE=" + COLUMN_NULLABLE_IN_DDL.size());
        }
    }
}
