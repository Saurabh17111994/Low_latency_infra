package com.trading.common.schema.position;

import java.util.List;

/**
 * Physical column layout of the Positions KV projection
 * ({@code code/01_platform/02_sql/ddl/10_positions.sql} v2). Pinned by
 * {@code PositionsColumnsAgreementTest} against the DDL file. The projector
 * writes this exact shape: key {@code position_id}; value carries the derived
 * quantities (never a persisted {@code current_quantity} — v2 removed it so
 * no write path can corrupt state by updating one quantity side only).
 */
public final class PositionsColumns {

    private PositionsColumns() {}

    public static final int POSITION_ID = 0;
    public static final int TRADE_CONTEXT_ID = 1;
    public static final int ACCOUNT_SCOPE_ID = 2;
    public static final int INSTRUMENT_TOKEN = 3;
    public static final int EXCHANGE = 4;
    public static final int SYMBOL = 5;
    public static final int SIDE = 6;
    public static final int STATE = 7;
    public static final int OPEN_QUANTITY = 8;
    public static final int CLOSED_QUANTITY = 9;
    public static final int AVERAGE_ENTRY_PAISE = 10;
    public static final int AVERAGE_EXIT_PAISE = 11;
    public static final int SOURCE_EVENT_ID = 12;
    public static final int SOURCE_VERSION = 13;
    public static final int CREATED_TS = 14;
    public static final int LAST_UPDATE_TS = 15;
    public static final int SCHEMA_VERSION = 16;

    public static final int FIELD_COUNT = 17;

    public static final String SCHEMA_VERSION_V2 = "2";

    /** Fluss {@code DataTypeRoot} name per column, DDL index order. */
    public static final List<String> TYPE_ROOTS = List.of(
            "STRING", "STRING", "STRING", "BIGINT", "STRING", "STRING", "STRING",
            "STRING", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "STRING", "BIGINT",
            "BIGINT", "BIGINT", "STRING");

    /** DDL nullability per column (10 DDL v2). */
    public static final List<Boolean> COLUMN_NULLABLE_IN_DDL = List.of(
            false, false, false, false, false, false, false, false, false,
            false, true, true, false, false, false, false, false);

    /** DDL column names in index order (diagnostics + agreement pin). */
    public static final List<String> NAMES = List.of(
            "position_id", "trade_context_id", "account_scope_id",
            "instrument_token", "exchange", "symbol", "side", "state",
            "open_quantity", "closed_quantity", "average_entry_paise",
            "average_exit_paise", "source_event_id", "source_version",
            "created_ts", "last_update_ts", "schema_version");

    // P4-313: parallel structures (17 int ordinals + NAMES + TYPE_ROOTS +
    // COLUMN_NULLABLE_IN_DDL + FIELD_COUNT) must agree — fail fast at
    // class-load so a swapped ordinal or unbumped count breaks loudly
    // instead of silently misplacing projector reads/writes.
    static {
        if (NAMES.size() != FIELD_COUNT
                || TYPE_ROOTS.size() != FIELD_COUNT
                || COLUMN_NULLABLE_IN_DDL.size() != FIELD_COUNT) {
            throw new IllegalStateException("PositionsColumns drift: FIELD_COUNT=" + FIELD_COUNT
                    + " NAMES=" + NAMES.size() + " TYPE_ROOTS=" + TYPE_ROOTS.size()
                    + " NULLABLE=" + COLUMN_NULLABLE_IN_DDL.size());
        }
        if (!NAMES.get(POSITION_ID).equals("position_id")
                || !NAMES.get(INSTRUMENT_TOKEN).equals("instrument_token")
                || !NAMES.get(OPEN_QUANTITY).equals("open_quantity")
                || !NAMES.get(SCHEMA_VERSION).equals("schema_version")) {
            throw new IllegalStateException(
                    "PositionsColumns drift: ordinal does not match NAMES index");
        }
    }
}
