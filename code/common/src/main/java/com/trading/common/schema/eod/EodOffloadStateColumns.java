package com.trading.common.schema.eod;

import java.util.List;

/**
 * Physical column layout of the durable EOD offload-state table
 * {@code eod_offload_state} (SCH-23; docs/08_implementation/01-foundation.md
 * "EOD controller and offload gate"). The controller's state must be durable
 * with restart/resume (docs/04_contracts/02-storage.md "EOD controller").
 *
 * <p>Must mirror the frozen 17-column layout of
 * {@code code/01_platform/02_sql/ddl/26_eod_offload_state.sql} v1 (KV, PK
 * {@code record_id}): the 15 {@link EodOffloadRecord} fields plus the record
 * identity ({@code record_id} = {@code trading_date|table_name}) and the
 * state table's own version pin. Pinned by
 * {@code EodOffloadStateColumnsAgreementTest} against the DDL file itself
 * (the repo's cross-boundary pin habit).
 *
 * <p><b>Single-field PK by design:</b> the EOD controller is a plain-JVM
 * runner driven by the Fluss raw client, which cannot upsert composite-PK KV
 * tables in Fluss 0.9.1 (iceberg key encoder — the COMPAT-FLUSS-005 matrix);
 * {@code trading_date} / {@code table_name} remain queryable columns. The
 * single-writer lease row uses the same columns with a reserved identity:
 * {@link #LEASE_RECORD_ID} {@code "lease|controller"}, token in
 * {@code source_hash}, lease expiry in {@code source_offset_start}, acquired
 * time in {@code updated_at_ms}.
 */
public final class EodOffloadStateColumns {

    private EodOffloadStateColumns() {}

    public static final int RECORD_ID = 0;
    public static final int TRADING_DATE = 1;
    public static final int TABLE_NAME = 2;
    public static final int SCHEMA_VERSION = 3;
    public static final int SOURCE_OFFSET_START = 4;
    public static final int SOURCE_OFFSET_END = 5;
    public static final int ROW_COUNT = 6;
    public static final int BYTE_COUNT = 7;
    public static final int SOURCE_HASH = 8;
    public static final int TARGET_HASH = 9;
    public static final int ICEBERG_SNAPSHOT_ID = 10;
    public static final int STATE = 11;
    public static final int RETRY_COUNT = 12;
    public static final int NEXT_RETRY_AT_MS = 13;
    public static final int EARLIEST_ALLOWED_SOURCE_EXPIRY_MS = 14;
    public static final int UPDATED_AT_MS = 15;
    public static final int STATE_SCHEMA_VERSION = 16;

    public static final int FIELD_COUNT = 17;

    /** State-table version pin written on every row. */
    public static final String STATE_SCHEMA_VERSION_V1 = "1";

    /** Reserved single-writer lease identity (see class javadoc for the field mapping). */
    public static final String LEASE_RECORD_ID = "lease|controller";
    public static final String LEASE_TRADING_DATE = "LEASE";
    public static final String LEASE_TABLE_NAME = "controller_lease";

    /** Lease-row predicate (P4-347): every data-plane read MUST exclude the
     *  lease row — filter by this, never by decomposing record_id. */
    public static boolean isLeaseRecordId(String recordId) {
        return LEASE_RECORD_ID.equals(recordId);
    }

    /** Fluss {@code DataTypeRoot} name per column, DDL index order. */
    public static final List<String> TYPE_ROOTS = List.of(
            "STRING", "STRING", "STRING", "STRING", "BIGINT", "BIGINT", "BIGINT", "BIGINT",
            "STRING", "STRING", "STRING", "STRING", "INTEGER", "BIGINT", "BIGINT", "BIGINT",
            "STRING");

    /** DDL nullability per column (26 DDL v1): all NOT NULL (sentinels, never null). */
    public static final List<Boolean> COLUMN_NULLABLE_IN_DDL = List.of(
            false, false, false, false, false, false, false, false, false, false,
            false, false, false, false, false, false, false);

    /** DDL column names in index order (diagnostics + agreement pin). */
    public static final List<String> NAMES = List.of(
        "record_id", "trading_date", "table_name", "schema_version",
        "source_offset_start", "source_offset_end", "row_count", "byte_count",
        "source_hash", "target_hash", "iceberg_snapshot_id", "state",
        "retry_count", "next_retry_at_ms", "earliest_allowed_source_expiry_ms",
        "updated_at_ms", "state_schema_version"
    );

    /** Deterministic record identity: {@code trading_date|table_name}.
     *  Writer-side guard (P4-233/P4-291): rejects null, embedded '|'
     *  (parse ambiguity), and collision with the reserved lease identity. */
    public static String recordId(String tradingDate, String tableName) {
        java.util.Objects.requireNonNull(tradingDate, "tradingDate");
        java.util.Objects.requireNonNull(tableName, "tableName");
        if (tradingDate.contains("|") || tableName.contains("|")) {
            throw new IllegalArgumentException(
                    "tradingDate/tableName must not contain '|': " + tradingDate + "|" + tableName);
        }
        String id = tradingDate + "|" + tableName;
        if (LEASE_RECORD_ID.equals(id)) {
            throw new IllegalArgumentException(
                    "recordId collides with reserved lease identity: " + id);
        }
        return id;
    }
}
