package com.trading.common.schema.position;

import java.util.List;

/**
 * Physical column layout of the immutable Fills LOG
 * ({@code code/01_platform/02_sql/ddl/08_fills.sql} v2). Pinned by
 * {@code FillsColumnsAgreementTest} against the DDL file (cross-boundary pin
 * habit). The projector consumes a caller-resolved subset; the full layout is
 * pinned here so the caller-side mapping and the DDL can never drift.
 */
public final class FillsColumns {

    private FillsColumns() {}

    public static final int POSTBACK_EVENT_ID = 0;
    public static final int POSTBACK_FINGERPRINT = 1;
    public static final int FINGERPRINT_VERSION = 2;
    public static final int ACCOUNT_SCOPE_ID = 3;
    public static final int BROKER_ORDER_ID = 4;
    public static final int INSTRUCTION_ID = 5;
    public static final int EXECUTION_ATTEMPT_ID = 6;
    public static final int TRADE_CONTEXT_ID = 7;
    public static final int ORDER_STATUS = 8;
    public static final int CUMULATIVE_QTY = 9;
    public static final int PENDING_QTY = 10;
    public static final int FILL_QTY = 11;
    public static final int FILL_PRICE_PAISE = 12;
    public static final int FILL_ID = 13;
    public static final int BROKER_EVENT_TIME = 14;
    public static final int RECEIVE_TIME = 15;
    public static final int INGEST_TS = 16;
    public static final int ORIGINAL_PAYLOAD = 17;
    public static final int PAYLOAD_HASH = 18;
    public static final int CORRELATION_STATE = 19;
    public static final int CORRELATION_REASON = 20;
    public static final int DECODER_VERSION = 21;
    public static final int SCHEMA_VERSION = 22;

    public static final int FIELD_COUNT = 23;

    public static final String SCHEMA_VERSION_V2 = "2";

    /** Fluss {@code DataTypeRoot} name per column, DDL index order. */
    public static final List<String> TYPE_ROOTS = List.of(
            "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING",
            "STRING", "STRING", "BIGINT", "BIGINT", "BIGINT", "BIGINT", "STRING",
            "BIGINT", "BIGINT", "BIGINT", "BYTES", "STRING", "STRING", "STRING",
            "STRING", "STRING");

    /** DDL nullability per column (08 DDL v2). */
    public static final List<Boolean> COLUMN_NULLABLE_IN_DDL = List.of(
            false, false, false, false, true, true, true, true, false, false,
            false, true, true, true, true, false, false, false, false, false,
            true, false, false);

    /** DDL column names in index order (diagnostics + agreement pin). */
    public static final List<String> NAMES = List.of(
            "postback_event_id", "postback_fingerprint", "fingerprint_version",
            "account_scope_id", "broker_order_id", "instruction_id",
            "execution_attempt_id", "trade_context_id", "order_status",
            "cumulative_qty", "pending_qty", "fill_qty", "fill_price_paise",
            "fill_id", "broker_event_time", "receive_time", "ingest_ts",
            "original_payload", "payload_hash", "correlation_state",
            "correlation_reason", "decoder_version", "schema_version");

    // P3-163 note: NAMES was a `public static final String[]`, where `final` binds only the
    // reference — any caller could write through it (`FillsColumns.NAMES[0] = "x"`,
    // `Arrays.sort(NAMES)`) and silently corrupt this shared agreement pin JVM-wide, or race a
    // concurrent reader. It is now an immutable List, matching TYPE_ROOTS and
    // COLUMN_NULLABLE_IN_DDL.
    //
    // P3-391: the parallel structures (23 int ordinals + NAMES + TYPE_ROOTS +
    // COLUMN_NULLABLE_IN_DDL + FIELD_COUNT + SCHEMA_VERSION) must stay in lockstep, but drift was
    // only caught if FillsColumnsAgreementTest happened to run — a one-sided edit (a renamed or
    // added column with a missed list entry) would silently misalign projector indexing in
    // production. This fails at class-load instead. Mirrors the guard PositionsColumns already
    // carries (P4-313).
    static {
        if (NAMES.size() != FIELD_COUNT
                || TYPE_ROOTS.size() != FIELD_COUNT
                || COLUMN_NULLABLE_IN_DDL.size() != FIELD_COUNT) {
            throw new IllegalStateException("FillsColumns drift: FIELD_COUNT=" + FIELD_COUNT
                    + " NAMES=" + NAMES.size() + " TYPE_ROOTS=" + TYPE_ROOTS.size()
                    + " NULLABLE=" + COLUMN_NULLABLE_IN_DDL.size());
        }
        if (SCHEMA_VERSION != FIELD_COUNT - 1) {
            throw new IllegalStateException("FillsColumns drift: SCHEMA_VERSION=" + SCHEMA_VERSION
                    + " must be FIELD_COUNT-1=" + (FIELD_COUNT - 1));
        }
        if (!NAMES.get(POSTBACK_EVENT_ID).equals("postback_event_id")
                || !NAMES.get(ACCOUNT_SCOPE_ID).equals("account_scope_id")
                || !NAMES.get(FILL_QTY).equals("fill_qty")
                || !NAMES.get(SCHEMA_VERSION).equals("schema_version")) {
            throw new IllegalStateException(
                    "FillsColumns drift: ordinal does not match NAMES index");
        }
    }
}
