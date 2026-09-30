package com.trading.compute.signaljob;

import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * DDL 35 (proposal) column contract for the merged candle+feature table
 * (Wave B/DEC-059): {@code candle_closed}'s 15 columns in DDL order plus
 * {@code features MAP<INT, DOUBLE>} (DEC-057: append-only registry ids) and
 * {@code sealed BOOLEAN}.
 *
 * <p>Mirrors {@link CandleClosedColumns} and {@link FeatureValuesColumns} in
 * shape: index constants, name/type/nullability lists for the contract
 * validator and the DDL-agreement pin, plus the {@link InternalTypeInfo} the
 * writer emits.
 */
public final class MergedCandleFeaturesColumns {

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
    public static final int FEATURES = 15;
    public static final int SEALED = 16;

    /** DDL 35 column count. */
    public static final int FIELD_COUNT = 17;

    /** DDL column names in index order (diagnostics + agreement pin). */
    public static final List<String> COLUMN_NAMES = List.of(
            "instrument_token", "exchange", "symbol", "tf",
            "window_start", "window_end",
            "open_paise", "high_paise", "low_paise", "close_paise",
            "volume", "tick_count",
            "last_event_time", "last_event_fingerprint", "schema_version",
            "features", "sealed");

    /** Fluss type roots in index order (contract validator). */
    public static final List<String> TYPE_ROOTS = List.of(
            "BIGINT", "STRING", "STRING", "STRING",
            "BIGINT", "BIGINT",
            "BIGINT", "BIGINT", "BIGINT", "BIGINT",
            "BIGINT", "INTEGER",
            "BIGINT", "STRING", "STRING",
            "MAP", "BOOLEAN");

    /** DDL nullability per column (35): exchange, symbol, fingerprint, features nullable. */
    public static final List<Boolean> COLUMN_NULLABLE_IN_DDL = List.of(
            false, true, true, false,
            false, false,
            false, false, false, false,
            false, false,
            false, true, false,
            true, false);

    private static final String[] NAMES = COLUMN_NAMES.toArray(new String[0]);

    /** Stream type info for emitted merged rows (DDL 35 order). */
    public static final InternalTypeInfo<RowData> ROW_TYPE_INFO = InternalTypeInfo.ofFields(
            new LogicalType[] {
                new BigIntType(false),                          // instrument_token (NOT NULL)
                new VarCharType(VarCharType.MAX_LENGTH),        // exchange (nullable)
                new VarCharType(VarCharType.MAX_LENGTH),        // symbol (nullable)
                new VarCharType(false, VarCharType.MAX_LENGTH), // tf (NOT NULL)
                new BigIntType(false),                          // window_start (NOT NULL)
                new BigIntType(false),                          // window_end (NOT NULL)
                new BigIntType(false),                          // open_paise (NOT NULL)
                new BigIntType(false),                          // high_paise (NOT NULL)
                new BigIntType(false),                          // low_paise (NOT NULL)
                new BigIntType(false),                          // close_paise (NOT NULL)
                new BigIntType(false),                          // volume (NOT NULL)
                new IntType(false),                             // tick_count (NOT NULL)
                new BigIntType(false),                          // last_event_time (NOT NULL)
                new VarCharType(VarCharType.MAX_LENGTH),        // last_event_fingerprint (nullable)
                new VarCharType(false, VarCharType.MAX_LENGTH), // schema_version (NOT NULL)
                new MapType(new IntType(false), new DoubleType()), // features (nullable)
                new BooleanType(false)                          // sealed (NOT NULL)
            },
            NAMES.clone());

    static {
        // Class-load tripwire — every layout constant must agree on FIELD_COUNT.
        if (COLUMN_NAMES.size() != FIELD_COUNT
                || TYPE_ROOTS.size() != FIELD_COUNT
                || COLUMN_NULLABLE_IN_DDL.size() != FIELD_COUNT
                || ROW_TYPE_INFO.toRowSize() != FIELD_COUNT) {
            throw new IllegalStateException(
                    "MergedCandleFeaturesColumns drift: COLUMN_NAMES/TYPE_ROOTS/"
                            + "COLUMN_NULLABLE_IN_DDL/ROW_TYPE_INFO must all have FIELD_COUNT="
                            + FIELD_COUNT);
        }
    }

    private MergedCandleFeaturesColumns() {}
}
