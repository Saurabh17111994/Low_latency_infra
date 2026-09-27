package com.trading.compute.signaljob;

import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * DDL 34 (proposal) column contract for the stored feature layer
 * (DEC-056/DEC-057): one row per {@code (instrument_token, tf, window_start)}
 * with a single {@code features MAP<INT, DOUBLE>} column keyed by the
 * registry's append-only integer feature id.
 *
 * <p>Mirrors {@code CandleClosedColumns} in shape: index constants, name and
 * type lists for the contract validator and the DDL-agreement pin, plus the
 * {@link InternalTypeInfo} the writer emits.
 */
public final class FeatureValuesColumns {

    public static final int INSTRUMENT_TOKEN = 0;
    public static final int TF = 1;
    public static final int WINDOW_START = 2;
    public static final int FEATURES = 3;

    /** DDL 34 column count. */
    public static final int FIELD_COUNT = 4;

    /** DDL column names in index order (diagnostics + agreement pin). */
    public static final List<String> COLUMN_NAMES =
            List.of("instrument_token", "tf", "window_start", "features");

    /** Fluss type roots in index order (contract validator). */
    public static final List<String> TYPE_ROOTS = List.of("BIGINT", "STRING", "BIGINT", "MAP");

    /** DDL nullability in index order (true = nullable in the DDL). */
    public static final List<Boolean> COLUMN_NULLABLE_IN_DDL =
            List.of(false, false, false, true);

    /** Stream type info for emitted feature rows (DDL 34 order). */
    public static final InternalTypeInfo<RowData> ROW_TYPE_INFO =
            InternalTypeInfo.ofFields(
                    new LogicalType[] {
                        new BigIntType(false), // instrument_token (NOT NULL)
                        new VarCharType(false, VarCharType.MAX_LENGTH), // tf (NOT NULL)
                        new BigIntType(false), // window_start (NOT NULL)
                        new MapType(new IntType(false), new DoubleType()) // features (nullable)
                    },
                    COLUMN_NAMES.toArray(new String[0]));

    static {
        if (COLUMN_NAMES.size() != FIELD_COUNT
                || TYPE_ROOTS.size() != FIELD_COUNT
                || COLUMN_NULLABLE_IN_DDL.size() != FIELD_COUNT
                || ROW_TYPE_INFO.toRowSize() != FIELD_COUNT) {
            throw new IllegalStateException(
                    "FeatureValuesColumns drift: COLUMN_NAMES/TYPE_ROOTS/COLUMN_NULLABLE_IN_DDL/"
                            + "ROW_TYPE_INFO must all have FIELD_COUNT=" + FIELD_COUNT);
        }
    }

    private FeatureValuesColumns() {}
}
