package com.trading.compute.signaljob;

import com.trading.common.schema.CandlePreviewTableSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Physical column layout of the candle-preview rows written by the Signal job
 * to the KV upsert sink ({@code feature_candles_15s_preview}) — the
 * low-latency candles Phase 1 visibility surface (2026-08-29).
 *
 * <p>Must mirror {@link CandlePreviewTableSchema} (the shared 14-column v1
 * contract) — {@link #FIELD_COUNT} and {@link #NAMES} derive from it, and
 * {@code code/01_platform/02_sql/ddl/30_feature_candles_15s_preview.sql}
 * exactly. The Fluss sink's {@code RowDataSerializationSchema} writes rows in
 * table column order, so emitted {@code GenericRowData} fields must be
 * positioned accordingly.
 */
public final class CandlePreviewColumns {

    private CandlePreviewColumns() {}

    public static final int INSTRUMENT_TOKEN = 0;
    public static final int EXCHANGE = 1;
    public static final int SYMBOL = 2;
    public static final int WINDOW_START = 3;
    public static final int WINDOW_END = 4;
    public static final int OPEN_PAISE = 5;
    public static final int HIGH_PAISE = 6;
    public static final int LOW_PAISE = 7;
    public static final int CLOSE_PAISE = 8;
    public static final int VOLUME = 9;
    public static final int TICK_COUNT = 10;
    public static final int IS_PREVIEW = 11;
    public static final int OUTPUT_TS = 12;
    public static final int SCHEMA_VERSION = 13;

    /** Field count from the shared contract (must stay 14). */
    public static final int FIELD_COUNT = CandlePreviewTableSchema.FIELD_COUNT;

    /** DDL column names in index order — derived from the shared contract. */
    public static final String[] NAMES =
            CandlePreviewTableSchema.COLUMNS.toArray(new String[0]);

    /**
     * Stream type info for emitted preview rows, derived from the v1 DDL
     * column order. Declared explicitly because TypeExtractor cannot resolve
     * a bare {@code RowData} to a schema — it would fall back to
     * GenericTypeInfo and route RowData through Kryo at operator boundaries.
     */
    public static final TypeInformation<RowData> ROW_TYPE_INFO = InternalTypeInfo.ofFields(
            new LogicalType[] {
                new BigIntType(),                        // instrument_token
                new VarCharType(VarCharType.MAX_LENGTH), // exchange
                new VarCharType(VarCharType.MAX_LENGTH), // symbol
                new BigIntType(),                        // window_start
                new BigIntType(),                        // window_end
                new BigIntType(),                        // open_paise
                new BigIntType(),                        // high_paise
                new BigIntType(),                        // low_paise
                new BigIntType(),                        // close_paise
                new BigIntType(),                        // volume
                new IntType(),                           // tick_count
                new BooleanType(),                       // is_preview
                new BigIntType(),                        // output_ts
                new VarCharType(VarCharType.MAX_LENGTH)  // schema_version
            });
}
