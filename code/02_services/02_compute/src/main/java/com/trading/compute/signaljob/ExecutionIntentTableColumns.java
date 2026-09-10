package com.trading.compute.signaljob;

import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Physical column layout of the immutable {@code Execution_Intent} LOG.
 *
 * <p>This class is the Java side of
 * {@code code/01_platform/02_sql/ddl/27_execution_intent.sql} v1. It is kept
 * separate from the retired {@code Trade_Decisions} ranking-era layout so a
 * future execution-intent change cannot silently reintroduce ranking or
 * reservation fields.
 */
public final class ExecutionIntentTableColumns {

    private ExecutionIntentTableColumns() {}

    public static final int INSTRUCTION_ID = 0;
    public static final int CANDIDATE_ID = 1;
    public static final int TRADE_CONTEXT_ID = 2;
    public static final int ACCOUNT_SCOPE_ID = 3;
    public static final int EXECUTION_PARTITION_ID = 4;
    public static final int INSTRUMENT_TOKEN = 5;
    public static final int EXCHANGE = 6;
    public static final int SYMBOL = 7;
    public static final int SIDE = 8;
    public static final int QUANTITY = 9;
    public static final int ORDER_TYPE = 10;
    public static final int LIMIT_PRICE_PAISE = 11;
    public static final int PRODUCT_TYPE = 12;
    public static final int TIME_IN_FORCE = 13;
    public static final int STRATEGY_ID = 14;
    public static final int STRATEGY_VERSION = 15;
    public static final int CONFIGURATION_VERSION = 16;
    public static final int CREATED_TS = 17;
    public static final int EXPIRY_TS = 18;
    public static final int REQUEST_HASH = 19;
    public static final int SUPERSEDES_INSTRUCTION_ID = 20;
    public static final int SCHEMA_VERSION = 21;

    public static final int FIELD_COUNT = 22;
    public static final String SCHEMA_VERSION_V1 = "1";
    public static final String SIDE_BUY = "BUY";
    public static final String SIDE_SELL = "SELL";
    public static final String ORDER_TYPE_MARKET = "MARKET";
    public static final String ORDER_TYPE_LIMIT = "LIMIT";

    public static final List<String> TYPE_ROOTS = List.of(
            "STRING", "STRING", "STRING", "STRING", "STRING", "BIGINT", "STRING", "STRING",
            "STRING", "BIGINT", "STRING", "BIGINT", "STRING", "STRING", "STRING", "STRING",
            "STRING", "BIGINT", "BIGINT", "STRING", "STRING", "STRING");

    public static final List<Boolean> COLUMN_NULLABLE_IN_DDL = List.of(
            false, false, false, false, false, false, false, false, false, false, false, true,
            false, false, false, false, false, false, true, false, true, false);

    /** DDL column names in index order (diagnostics + agreement pin). */
    private static final String[] NAMES_INTERNAL = {
        "instruction_id", "candidate_id", "trade_context_id", "account_scope_id",
        "execution_partition_id", "instrument_token", "exchange", "symbol", "side", "quantity",
        "order_type", "limit_price_paise", "product_type", "time_in_force", "strategy_id",
        "strategy_version", "configuration_version", "created_ts", "expiry_ts", "request_hash",
        "supersedes_instruction_id", "schema_version"
    };

    /**
     * Immutable DDL column names in index order (P2-035: the array above is
     * private — this is the only public view, so no caller can mutate the
     * shared projection).
     */
    public static final List<String> COLUMN_NAMES = List.of(
        "instruction_id", "candidate_id", "trade_context_id", "account_scope_id",
        "execution_partition_id", "instrument_token", "exchange", "symbol", "side", "quantity",
        "order_type", "limit_price_paise", "product_type", "time_in_force", "strategy_id",
        "strategy_version", "configuration_version", "created_ts", "expiry_ts", "request_hash",
        "supersedes_instruction_id", "schema_version");

    /**
     * @deprecated Use {@link #COLUMN_NAMES} — retained only for source
     * compatibility; returns a fresh copy per call so mutation cannot corrupt
     * the shared projection.
     */
    @Deprecated
    public static String[] NAMES() {
        return NAMES_INTERNAL.clone();
    }

    public static final TypeInformation<RowData> ROW_TYPE_INFO = InternalTypeInfo.ofFields(
            new LogicalType[] {
                new VarCharType(VarCharType.MAX_LENGTH), // instruction_id
                new VarCharType(VarCharType.MAX_LENGTH), // candidate_id
                new VarCharType(VarCharType.MAX_LENGTH), // trade_context_id
                new VarCharType(VarCharType.MAX_LENGTH), // account_scope_id
                new VarCharType(VarCharType.MAX_LENGTH), // execution_partition_id
                new BigIntType(),                        // instrument_token
                new VarCharType(VarCharType.MAX_LENGTH), // exchange
                new VarCharType(VarCharType.MAX_LENGTH), // symbol
                new VarCharType(VarCharType.MAX_LENGTH), // side
                new BigIntType(),                        // quantity
                new VarCharType(VarCharType.MAX_LENGTH), // order_type
                new BigIntType(),                        // limit_price_paise
                new VarCharType(VarCharType.MAX_LENGTH), // product_type
                new VarCharType(VarCharType.MAX_LENGTH), // time_in_force
                new VarCharType(VarCharType.MAX_LENGTH), // strategy_id
                new VarCharType(VarCharType.MAX_LENGTH), // strategy_version
                new VarCharType(VarCharType.MAX_LENGTH), // configuration_version
                new BigIntType(),                        // created_ts
                new BigIntType(),                        // expiry_ts
                new VarCharType(VarCharType.MAX_LENGTH), // request_hash
                new VarCharType(VarCharType.MAX_LENGTH), // supersedes_instruction_id
                new VarCharType(VarCharType.MAX_LENGTH)  // schema_version
            },
            NAMES_INTERNAL.clone());

    static {
        // P2-137 drift gate (same class as P2-127/P2-243): every layout
        // constant must agree on FIELD_COUNT, or class-load fails loud even
        // when tests are skipped. Last column must be schema_version.
        if (COLUMN_NAMES.size() != FIELD_COUNT
                || TYPE_ROOTS.size() != FIELD_COUNT
                || COLUMN_NULLABLE_IN_DDL.size() != FIELD_COUNT
                || SCHEMA_VERSION != FIELD_COUNT - 1
                || ((org.apache.flink.table.runtime.typeutils.InternalTypeInfo<RowData>)
                                ROW_TYPE_INFO)
                        .toRowSize()
                        != FIELD_COUNT) {
            throw new IllegalStateException(
                    "Execution_Intent layout drift: FIELD_COUNT/NAMES/TYPE_ROOTS/"
                            + "nullability/ROW_TYPE_INFO out of sync");
        }
        checkIndex(INSTRUCTION_ID, "instruction_id");
        checkIndex(CANDIDATE_ID, "candidate_id");
        checkIndex(TRADE_CONTEXT_ID, "trade_context_id");
        checkIndex(ACCOUNT_SCOPE_ID, "account_scope_id");
        checkIndex(EXECUTION_PARTITION_ID, "execution_partition_id");
        checkIndex(INSTRUMENT_TOKEN, "instrument_token");
        checkIndex(EXCHANGE, "exchange");
        checkIndex(SYMBOL, "symbol");
        checkIndex(SIDE, "side");
        checkIndex(QUANTITY, "quantity");
        checkIndex(ORDER_TYPE, "order_type");
        checkIndex(LIMIT_PRICE_PAISE, "limit_price_paise");
        checkIndex(PRODUCT_TYPE, "product_type");
        checkIndex(TIME_IN_FORCE, "time_in_force");
        checkIndex(STRATEGY_ID, "strategy_id");
        checkIndex(STRATEGY_VERSION, "strategy_version");
        checkIndex(CONFIGURATION_VERSION, "configuration_version");
        checkIndex(CREATED_TS, "created_ts");
        checkIndex(EXPIRY_TS, "expiry_ts");
        checkIndex(REQUEST_HASH, "request_hash");
        checkIndex(SUPERSEDES_INSTRUCTION_ID, "supersedes_instruction_id");
        checkIndex(SCHEMA_VERSION, "schema_version");
    }

    private static void checkIndex(int index, String expected) {
        if (!NAMES_INTERNAL[index].equals(expected)) {
            throw new IllegalStateException(
                    "Execution_Intent layout drift: index " + index + " must be '"
                            + expected + "', got '" + NAMES_INTERNAL[index] + "'");
        }
    }

    /** Defensive copy of the DDL column names (callers must not retain the array). */
    public static String[] namesCopy() {
        return NAMES_INTERNAL.clone();
    }
}
