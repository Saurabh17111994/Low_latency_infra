package com.trading.common.schema.ownership;

import java.util.List;

/**
 * Physical column layout of the Execution_Attempts KV state table
 * ({@code code/01_platform/02_sql/ddl/12_execution_attempts.sql} v3). Pinned
 * by {@code ExecutionAttemptsColumnsAgreementTest} against the DDL file. Key
 * {@code execution_attempt_id}; v2 added {@code terminal_ts} (set when the
 * attempt reaches a terminal phase) and {@code phase_epoch} (monotonic phase
 * version so stale phase writes from a crashed executor are detectable,
 * R-234). v3 (CHG-044) added {@code gate_fence_token}: the exact fencing token
 * that authorized the attempt, persisted at PREPARED with {@code gate_epoch}
 * (dossier "PREPARED (request hash + client ref + gate epoch + fence)"). The
 * column-ownership matrix ({@link ExecutionAttemptsColumnOwnership}) consumes
 * this layout so ownership indexes can never drift from the DDL.
 */
public final class ExecutionAttemptsColumns {

    private ExecutionAttemptsColumns() {}

    public static final int EXECUTION_ATTEMPT_ID = 0;
    public static final int ACCOUNT_SCOPE_ID = 1;
    public static final int INSTRUCTION_ID = 2;
    public static final int ACTION_ID = 3;
    public static final int EXECUTION_PARTITION_ID = 4;
    public static final int REQUEST_HASH = 5;
    public static final int CLIENT_ORDER_REF = 6;
    public static final int BROKER_ORDER_ID = 7;
    public static final int GATE_EPOCH = 8;
    public static final int PHASE = 9;
    public static final int PHASE_EPOCH = 10;
    public static final int OUTCOME = 11;
    public static final int OUTCOME_DETAIL = 12;
    public static final int PREPARED_TS = 13;
    public static final int SUBMITTED_TS = 14;
    public static final int TERMINAL_TS = 15;
    public static final int BROKER_RESPONSE_SUMMARY = 16;
    public static final int RETRY_ATTEMPT = 17;
    public static final int GATE_FENCE_TOKEN = 18;
    public static final int SCHEMA_VERSION = 19;

    public static final int FIELD_COUNT = 20;

    public static final String SCHEMA_VERSION_V3 = "3";

    /** Fluss {@code DataTypeRoot} name per column, DDL index order. */
    public static final List<String> TYPE_ROOTS = List.of(
            "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING",
            "STRING", "BIGINT", "STRING", "BIGINT", "STRING", "STRING", "BIGINT",
            "BIGINT", "BIGINT", "STRING", "INTEGER", "BIGINT", "STRING");

    /** DDL nullability per column (12 DDL v3). */
    public static final List<Boolean> COLUMN_NULLABLE_IN_DDL = List.of(
            false, false, false, true, false, false, false, true, false, false,
            false, true, true, false, true, true, true, false, false, false);

    /** DDL column names in index order (diagnostics + agreement pin). Immutable. */
    public static final List<String> NAMES = List.of(
            "execution_attempt_id", "account_scope_id", "instruction_id",
            "action_id", "execution_partition_id", "request_hash",
            "client_order_ref", "broker_order_id", "gate_epoch", "phase",
            "phase_epoch", "outcome", "outcome_detail", "prepared_ts",
            "submitted_ts", "terminal_ts", "broker_response_summary",
            "retry_attempt", "gate_fence_token", "schema_version");

    static {
        if (NAMES.size() != FIELD_COUNT
                || TYPE_ROOTS.size() != FIELD_COUNT
                || COLUMN_NULLABLE_IN_DDL.size() != FIELD_COUNT) {
            throw new ExceptionInInitializerError(
                    "ExecutionAttemptsColumns drift: NAMES/TYPE_ROOTS/NULLABLE size != FIELD_COUNT");
        }
        if (SCHEMA_VERSION != FIELD_COUNT - 1
                || GATE_FENCE_TOKEN != SCHEMA_VERSION - 1) {
            throw new ExceptionInInitializerError(
                    "ExecutionAttemptsColumns drift: gate_fence_token must sit immediately before trailing schema_version");
        }
    }
}
