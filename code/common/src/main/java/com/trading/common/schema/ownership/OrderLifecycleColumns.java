package com.trading.common.schema.ownership;

import java.util.List;

/**
 * Physical column layout of the Order_Lifecycle KV projection
 * ({@code code/01_platform/02_sql/ddl/09_order_lifecycle.sql} v2). Pinned by
 * {@code OrderLifecycleColumnsAgreementTest} against the DDL file
 * (cross-boundary pin habit). Key {@code (account_scope_id, broker_order_id)}
 * — broker-assigned order IDs are unique only within one brokerage account
 * (R-013), so the composite key makes the projection account-safe. The
 * column-ownership matrix ({@link OrderLifecycleColumnOwnership}) consumes
 * this layout so ownership indexes can never drift from the DDL.
 */
public final class OrderLifecycleColumns {

    private OrderLifecycleColumns() {}

    public static final int ACCOUNT_SCOPE_ID = 0;
    public static final int BROKER_ORDER_ID = 1;
    public static final int INSTRUCTION_ID = 2;
    public static final int EXECUTION_ATTEMPT_ID = 3;
    public static final int TRADE_CONTEXT_ID = 4;
    public static final int NORMALIZED_STATE = 5;
    public static final int CUMULATIVE_QTY = 6;
    public static final int PENDING_QTY = 7;
    public static final int AVERAGE_FILL_PRICE_PAISE = 8;
    public static final int SOURCE_EVENT_ID = 9;
    public static final int SOURCE_VERSION = 10;
    public static final int SOURCE_EVENT_TIME = 11;
    public static final int LAST_RECEIVE_TIME = 12;
    public static final int CORRELATION_STATE = 13;
    public static final int SCHEMA_VERSION = 14;

    public static final int FIELD_COUNT = 15;

    public static final String SCHEMA_VERSION_V2 = "2";

    /** Fluss {@code DataTypeRoot} name per column, DDL index order. */
    public static final List<String> TYPE_ROOTS = List.of(
            "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "BIGINT",
            "BIGINT", "BIGINT", "STRING", "BIGINT", "BIGINT", "BIGINT", "STRING",
            "STRING");

    /** DDL nullability per column (09 DDL v2). */
    public static final List<Boolean> COLUMN_NULLABLE_IN_DDL = List.of(
            false, false, true, true, true, false, false, false, true, false,
            false, true, false, false, false);

    /** DDL column names in index order (diagnostics + agreement pin). Immutable. */
    public static final List<String> NAMES = List.of(
            "account_scope_id", "broker_order_id", "instruction_id",
            "execution_attempt_id", "trade_context_id", "normalized_state",
            "cumulative_qty", "pending_qty", "average_fill_price_paise",
            "source_event_id", "source_version", "source_event_time",
            "last_receive_time", "correlation_state", "schema_version");

    static {
        if (TYPE_ROOTS.size() != FIELD_COUNT
                || COLUMN_NULLABLE_IN_DDL.size() != FIELD_COUNT
                || NAMES.size() != FIELD_COUNT) {
            throw new ExceptionInInitializerError("OrderLifecycleColumns metadata out of sync: FIELD_COUNT=" + FIELD_COUNT
                    + " TYPE_ROOTS=" + TYPE_ROOTS.size()
                    + " NULLABLE=" + COLUMN_NULLABLE_IN_DDL.size()
                    + " NAMES=" + NAMES.size());
        }
    }
}
