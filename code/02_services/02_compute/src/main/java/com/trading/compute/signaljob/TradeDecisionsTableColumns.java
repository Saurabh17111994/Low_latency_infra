package com.trading.compute.signaljob;

import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Physical column layout of the immutable instruction feed as written by the
 * Signal job.
 *
 * <p>Must mirror the frozen 25-column layout of
 * {@code code/01_platform/02_sql/ddl/07_trade_decisions.sql} v2 (LOG, DEC-035
 * dual-sink era — SCH-19: immutable instruction feed; REQ-FLS-008). The Fluss
 * LOG sink's {@code RowDataSerializationSchema} projects by column name, so
 * emitted {@code GenericRowData} fields must be positioned in DDL column
 * order. The layout is pinned by
 * {@link TradeDecisionsTableColumnsAgreementTest} against the DDL file itself
 * (the repo's cross-boundary pin habit: DDL → Java row layout).
 *
 * <p>Executable request only: this table SHALL NOT carry {@code client_order_ref},
 * {@code broker_order_id}, mutable execution status, or any Executor-assigned
 * field (REQ-FLS-008, {@code docs/04_contracts/07-executor.md}) — execution
 * state lives in {@code Execution_Attempts} / {@code Order_Correlation}.
 */
public final class TradeDecisionsTableColumns {

    private TradeDecisionsTableColumns() {}

    public static final int INSTRUCTION_ID = 0;
    public static final int CANDIDATE_ID = 1;
    public static final int TRADE_CONTEXT_ID = 2;
    public static final int INSTRUMENT_TOKEN = 3;
    public static final int EXCHANGE = 4;
    public static final int SYMBOL = 5;
    public static final int SIDE = 6;
    public static final int QUANTITY = 7;
    public static final int ORDER_TYPE = 8;
    public static final int PRODUCT_TYPE = 9;
    public static final int LIMIT_PRICE_PAISE = 10;
    public static final int PORTFOLIO_ID = 11;
    public static final int ACCOUNT_SCOPE_ID = 12;
    public static final int STRATEGY_ID = 13;
    public static final int STRATEGY_VERSION = 14;
    public static final int CONFIGURATION_VERSION = 15;
    public static final int EVALUATION_ID = 16;
    public static final int COMPOSITE_SCORE = 17;
    public static final int RESERVATION_ID = 18;
    public static final int RESERVATION_VERSION = 19;
    public static final int CREATED_TS = 20;
    public static final int EXPIRY_TS = 21;
    public static final int SUPERSEDES_INSTRUCTION_ID = 22;
    public static final int SUPERSEDED_BY_INSTRUCTION_ID = 23;
    public static final int SCHEMA_VERSION = 24;

    public static final int FIELD_COUNT = 25;

    /** Fixed values written by the Signal-job decision builder (DEC-034 MVP subset). */
    public static final String SCHEMA_VERSION_V2 = "2";
    public static final String SIDE_BUY = "BUY";
    public static final String SIDE_SELL = "SELL";
    public static final String ORDER_TYPE_MARKET = "MARKET";
    public static final String ORDER_TYPE_LIMIT = "LIMIT";

    /**
     * Fluss {@code DataTypeRoot} name per column, DDL index order (frozen
     * 25-column v2 layout). The table-contract validator (future
     * {@code validateTradeDecisionsLogTable}) compares live metadata roots
     * against this list column by column.
     */
    public static final List<String> TYPE_ROOTS = List.of(
            "STRING", "STRING", "STRING", "BIGINT", "STRING", "STRING", "STRING",
            "BIGINT", "STRING", "STRING", "BIGINT", "STRING", "STRING", "STRING",
            "STRING", "STRING", "STRING", "DOUBLE", "STRING", "STRING", "BIGINT",
            "BIGINT", "STRING", "STRING", "STRING");

    /**
     * DDL nullability per column (07 DDL v2): {@code true} = nullable,
     * {@code false} = NOT NULL. Every executable-request field is NOT NULL;
     * only price/score/expiry/supersession linkage is nullable. Same semantic
     * as {@link SignalCandidatesTableColumns#COLUMN_NULLABLE_IN_DDL} — used
     * for the schema-report divergence marker (live LOG metadata does not
     * carry NOT NULL).
     */
    public static final List<Boolean> COLUMN_NULLABLE_IN_DDL = List.of(
            false, false, false, false, false, false, false, false, false, false,
            true, false, false, false, false, false, false, true, false, false,
            false, true, true, true, false);

    /** DDL column names in index order (diagnostics + agreement pin). */
    private static final String[] NAMES_INTERNAL = {
        "instruction_id", "candidate_id", "trade_context_id", "instrument_token",
        "exchange", "symbol", "side", "quantity", "order_type", "product_type",
        "limit_price_paise", "portfolio_id", "account_scope_id", "strategy_id",
        "strategy_version", "configuration_version", "evaluation_id",
        "composite_score", "reservation_id", "reservation_version", "created_ts",
        "expiry_ts", "supersedes_instruction_id", "superseded_by_instruction_id",
        "schema_version"
    };

    /**
     * Immutable DDL column names in index order (P2-066: the array above is
     * private — this is the only public view, so no caller can mutate the
     * shared projection).
     */
    public static final List<String> COLUMN_NAMES = List.of(
        "instruction_id", "candidate_id", "trade_context_id", "instrument_token",
        "exchange", "symbol", "side", "quantity", "order_type", "product_type",
        "limit_price_paise", "portfolio_id", "account_scope_id", "strategy_id",
        "strategy_version", "configuration_version", "evaluation_id",
        "composite_score", "reservation_id", "reservation_version", "created_ts",
        "expiry_ts", "supersedes_instruction_id", "superseded_by_instruction_id",
        "schema_version");

    /**
     * @deprecated Use {@link #COLUMN_NAMES} — the public array is retained
     * only for source compatibility and is a fresh copy per access, so
     * mutation cannot corrupt the shared projection.
     */
    @Deprecated
    public static String[] NAMES() {
        return NAMES_INTERNAL.clone();
    }

    /**
     * Stream type info for emitted decision rows, derived from the frozen v2
     * DDL column order. Declared explicitly because TypeExtractor cannot
     * resolve a bare {@code RowData} to a schema — it would fall back to
     * GenericTypeInfo and route RowData through Kryo at operator boundaries.
     */
    public static final TypeInformation<RowData> ROW_TYPE_INFO = InternalTypeInfo.ofFields(
            new LogicalType[] {
                new VarCharType(VarCharType.MAX_LENGTH), // instruction_id
                new VarCharType(VarCharType.MAX_LENGTH), // candidate_id
                new VarCharType(VarCharType.MAX_LENGTH), // trade_context_id
                new BigIntType(),                        // instrument_token
                new VarCharType(VarCharType.MAX_LENGTH), // exchange
                new VarCharType(VarCharType.MAX_LENGTH), // symbol
                new VarCharType(VarCharType.MAX_LENGTH), // side
                new BigIntType(),                        // quantity
                new VarCharType(VarCharType.MAX_LENGTH), // order_type
                new VarCharType(VarCharType.MAX_LENGTH), // product_type
                new BigIntType(),                        // limit_price_paise
                new VarCharType(VarCharType.MAX_LENGTH), // portfolio_id
                new VarCharType(VarCharType.MAX_LENGTH), // account_scope_id
                new VarCharType(VarCharType.MAX_LENGTH), // strategy_id
                new VarCharType(VarCharType.MAX_LENGTH), // strategy_version
                new VarCharType(VarCharType.MAX_LENGTH), // configuration_version
                new VarCharType(VarCharType.MAX_LENGTH), // evaluation_id
                new DoubleType(),                        // composite_score
                new VarCharType(VarCharType.MAX_LENGTH), // reservation_id
                new VarCharType(VarCharType.MAX_LENGTH), // reservation_version
                new BigIntType(),                        // created_ts
                new BigIntType(),                        // expiry_ts
                new VarCharType(VarCharType.MAX_LENGTH), // supersedes_instruction_id
                new VarCharType(VarCharType.MAX_LENGTH), // superseded_by_instruction_id
                new VarCharType(VarCharType.MAX_LENGTH)  // schema_version
            },
            NAMES_INTERNAL.clone());

    static {
        // P2-066 tripwire (CandleClosedColumns P2-127 pattern): every layout
        // constant must agree on FIELD_COUNT, or class-load fails loud.
        if (COLUMN_NAMES.size() != FIELD_COUNT
                || TYPE_ROOTS.size() != FIELD_COUNT
                || COLUMN_NULLABLE_IN_DDL.size() != FIELD_COUNT
                || ((org.apache.flink.table.runtime.typeutils.InternalTypeInfo<RowData>)
                                ROW_TYPE_INFO)
                        .toRowSize()
                        != FIELD_COUNT) {
            throw new IllegalStateException(
                    "TradeDecisionsTableColumns drift: COLUMN_NAMES/TYPE_ROOTS/"
                            + "COLUMN_NULLABLE_IN_DDL/ROW_TYPE_INFO must all have FIELD_COUNT="
                            + FIELD_COUNT);
        }
        checkIndex(INSTRUCTION_ID, "instruction_id");
        checkIndex(CANDIDATE_ID, "candidate_id");
        checkIndex(TRADE_CONTEXT_ID, "trade_context_id");
        checkIndex(INSTRUMENT_TOKEN, "instrument_token");
        checkIndex(EXCHANGE, "exchange");
        checkIndex(SYMBOL, "symbol");
        checkIndex(SIDE, "side");
        checkIndex(QUANTITY, "quantity");
        checkIndex(ORDER_TYPE, "order_type");
        checkIndex(PRODUCT_TYPE, "product_type");
        checkIndex(LIMIT_PRICE_PAISE, "limit_price_paise");
        checkIndex(PORTFOLIO_ID, "portfolio_id");
        checkIndex(ACCOUNT_SCOPE_ID, "account_scope_id");
        checkIndex(STRATEGY_ID, "strategy_id");
        checkIndex(STRATEGY_VERSION, "strategy_version");
        checkIndex(CONFIGURATION_VERSION, "configuration_version");
        checkIndex(EVALUATION_ID, "evaluation_id");
        checkIndex(COMPOSITE_SCORE, "composite_score");
        checkIndex(RESERVATION_ID, "reservation_id");
        checkIndex(RESERVATION_VERSION, "reservation_version");
        checkIndex(CREATED_TS, "created_ts");
        checkIndex(EXPIRY_TS, "expiry_ts");
        checkIndex(SUPERSEDES_INSTRUCTION_ID, "supersedes_instruction_id");
        checkIndex(SUPERSEDED_BY_INSTRUCTION_ID, "superseded_by_instruction_id");
        checkIndex(SCHEMA_VERSION, "schema_version");
    }

    private static void checkIndex(int index, String expected) {
        if (!NAMES_INTERNAL[index].equals(expected)) {
            throw new IllegalStateException(
                    "TradeDecisionsTableColumns drift: index " + index + " must be '"
                            + expected + "', got '" + NAMES_INTERNAL[index] + "'");
        }
    }

    /** Defensive copy of the DDL column names (callers must not retain the array). */
    public static String[] namesCopy() {
        return NAMES_INTERNAL.clone();
    }
}
