package com.trading.compute.signaljob;

import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Physical column layout of the live-candle KV projection
 * {@code candle_live} ({@code code/01_platform/02_sql/ddl/32_candle_live.sql}
 * v1, PK {@code instrument_token, tf, window_start}, 16 buckets). Pinned by
 * {@code CandleLiveColumnsAgreementTest} against the DDL file.
 *
 * <p>Phase 0 multi-TF aggregator contract: tf discriminator values are
 * {@code FIFTEEN_S, THIRTY_S, ONE_M, THREE_M, FIVE_M, FIFTEEN_M}; columns
 * mirror {@code candle_closed} exactly — the sink's
 * {@code RowDataSerializationSchema} projects by column name, so emitted
 * {@code GenericRowData} fields must be positioned in DDL column order.
 */
public final class CandleLiveColumns {

    private CandleLiveColumns() {}

    public static final int INSTRUMENT_TOKEN = 0;
    public static final int EXCHANGE = 1;
    public static final int SYMBOL = 2;
    public static final int TF = 3;
    public static final int WINDOW_START = 4;
    public static final int WINDOW_END = 5;
    public static final int OPEN_PAISE = 6;
    public static final int HIGH_PAISE = 7;
    public static final int LOW_PAISE = 8;
    public static final int CLOSE_PAISE = 9;
    public static final int VOLUME = 10;
    public static final int TICK_COUNT = 11;
    public static final int LAST_EVENT_TIME = 12;
    public static final int LAST_EVENT_FINGERPRINT = 13;
    public static final int SCHEMA_VERSION = 14;

    public static final int FIELD_COUNT = 15;

    public static final String SCHEMA_VERSION_V1 = "1";

    /** Fluss {@code DataTypeRoot} name per column, DDL index order. */
    public static final List<String> TYPE_ROOTS = List.of(
            "BIGINT", "STRING", "STRING", "STRING",
            "BIGINT", "BIGINT",
            "BIGINT", "BIGINT", "BIGINT", "BIGINT",
            "BIGINT", "INTEGER", "BIGINT", "STRING", "STRING");

    /** DDL nullability per column (32 DDL v1): exchange, symbol, last_event_fingerprint nullable. */
    public static final List<Boolean> COLUMN_NULLABLE_IN_DDL = List.of(
            false, true, true, false,
            false, false,
            false, false, false, false,
            false, false, false, true, false);

    /** DDL column names in index order (diagnostics + agreement pin). */
    private static final String[] NAMES = {
            "instrument_token", "exchange", "symbol", "tf",
            "window_start", "window_end",
            "open_paise", "high_paise", "low_paise", "close_paise",
            "volume", "tick_count",
            "last_event_time", "last_event_fingerprint", "schema_version"
    };

    /** Immutable DDL column names in index order (diagnostics + agreement pin). */
    public static final List<String> COLUMN_NAMES = List.of(
            "instrument_token", "exchange", "symbol", "tf",
            "window_start", "window_end",
            "open_paise", "high_paise", "low_paise", "close_paise",
            "volume", "tick_count",
            "last_event_time", "last_event_fingerprint", "schema_version");

    /** Stream type info for emitted candle_live rows (v1 DDL order). */
    public static final InternalTypeInfo<RowData> ROW_TYPE_INFO = InternalTypeInfo.ofFields(
            new LogicalType[] {
                new BigIntType(false),                       // instrument_token (NOT NULL)
                new VarCharType(VarCharType.MAX_LENGTH),     // exchange (nullable)
                new VarCharType(VarCharType.MAX_LENGTH),     // symbol (nullable)
                new VarCharType(false, VarCharType.MAX_LENGTH), // tf (NOT NULL)
                new BigIntType(false),                       // window_start (NOT NULL)
                new BigIntType(false),                       // window_end (NOT NULL)
                new BigIntType(false),                       // open_paise (NOT NULL)
                new BigIntType(false),                       // high_paise (NOT NULL)
                new BigIntType(false),                       // low_paise (NOT NULL)
                new BigIntType(false),                       // close_paise (NOT NULL)
                new BigIntType(false),                       // volume (NOT NULL)
                new IntType(false),                          // tick_count (NOT NULL)
                new BigIntType(false),                       // last_event_time (NOT NULL)
                new VarCharType(VarCharType.MAX_LENGTH),     // last_event_fingerprint (nullable)
                new VarCharType(false, VarCharType.MAX_LENGTH) // schema_version (NOT NULL)
            },
            NAMES.clone());

    static {
        if (COLUMN_NAMES.size() != FIELD_COUNT
                || TYPE_ROOTS.size() != FIELD_COUNT
                || COLUMN_NULLABLE_IN_DDL.size() != FIELD_COUNT
                || ROW_TYPE_INFO.toRowSize() != FIELD_COUNT) {
            throw new IllegalStateException(
                    "CandleLiveColumns drift: COLUMN_NAMES/TYPE_ROOTS/COLUMN_NULLABLE_IN_DDL/ROW_TYPE_INFO"
                            + " must all have FIELD_COUNT=" + FIELD_COUNT);
        }
        checkIndex(INSTRUMENT_TOKEN, "instrument_token");
        checkIndex(EXCHANGE, "exchange");
        checkIndex(SYMBOL, "symbol");
        checkIndex(TF, "tf");
        checkIndex(WINDOW_START, "window_start");
        checkIndex(WINDOW_END, "window_end");
        checkIndex(OPEN_PAISE, "open_paise");
        checkIndex(HIGH_PAISE, "high_paise");
        checkIndex(LOW_PAISE, "low_paise");
        checkIndex(CLOSE_PAISE, "close_paise");
        checkIndex(VOLUME, "volume");
        checkIndex(TICK_COUNT, "tick_count");
        checkIndex(LAST_EVENT_TIME, "last_event_time");
        checkIndex(LAST_EVENT_FINGERPRINT, "last_event_fingerprint");
        checkIndex(SCHEMA_VERSION, "schema_version");
    }

    private static void checkIndex(int index, String expected) {
        if (!NAMES[index].equals(expected)) {
            throw new IllegalStateException(
                    "CandleLiveColumns drift: index " + index + " must be '"
                            + expected + "', got '" + NAMES[index] + "'");
        }
    }

    /** Defensive copy of the DDL column names (callers must not retain the array). */
    public static String[] namesCopy() {
        return NAMES.clone();
    }
}
