package com.trading.compute.signaljob;

import java.util.Map;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

/**
 * Wave B (DEC-059): builds merged {@code candle_features} rows from a candle
 * row ({@code candle_live} or {@code candle_closed} layout — the same 15
 * columns) plus the feature snapshot and the seal flag.
 *
 * <p>Pure conversion; the host ({@code StrategyHostFunction}) decides when to
 * emit and is the only writer. The candle columns are copied verbatim —
 * nullability is preserved, not "filled in".
 */
public final class MergedCandleRows {

    private MergedCandleRows() {}

    /**
     * One merged row: the candle columns copied by type, then the feature
     * snapshot (DEC-057 map, may be empty) and the seal flag.
     */
    public static GenericRowData fromCandleRow(
            RowData candle, Map<Integer, Double> features, boolean sealed) {
        GenericRowData row = new GenericRowData(MergedCandleFeaturesColumns.FIELD_COUNT);
        row.setField(
                MergedCandleFeaturesColumns.INSTRUMENT_TOKEN,
                candle.getLong(CandleClosedColumns.INSTRUMENT_TOKEN));
        row.setField(
                MergedCandleFeaturesColumns.EXCHANGE,
                candle.isNullAt(CandleClosedColumns.EXCHANGE)
                        ? null
                        : candle.getString(CandleClosedColumns.EXCHANGE));
        row.setField(
                MergedCandleFeaturesColumns.SYMBOL,
                candle.isNullAt(CandleClosedColumns.SYMBOL)
                        ? null
                        : candle.getString(CandleClosedColumns.SYMBOL));
        row.setField(
                MergedCandleFeaturesColumns.TF,
                candle.getString(CandleClosedColumns.TF));
        row.setField(
                MergedCandleFeaturesColumns.WINDOW_START,
                candle.getLong(CandleClosedColumns.WINDOW_START));
        row.setField(
                MergedCandleFeaturesColumns.WINDOW_END,
                candle.getLong(CandleClosedColumns.WINDOW_END));
        row.setField(
                MergedCandleFeaturesColumns.OPEN_PAISE,
                candle.getLong(CandleClosedColumns.OPEN_PAISE));
        row.setField(
                MergedCandleFeaturesColumns.HIGH_PAISE,
                candle.getLong(CandleClosedColumns.HIGH_PAISE));
        row.setField(
                MergedCandleFeaturesColumns.LOW_PAISE,
                candle.getLong(CandleClosedColumns.LOW_PAISE));
        row.setField(
                MergedCandleFeaturesColumns.CLOSE_PAISE,
                candle.getLong(CandleClosedColumns.CLOSE_PAISE));
        row.setField(
                MergedCandleFeaturesColumns.VOLUME,
                candle.getLong(CandleClosedColumns.VOLUME));
        row.setField(
                MergedCandleFeaturesColumns.TICK_COUNT,
                candle.getInt(CandleClosedColumns.TICK_COUNT));
        row.setField(
                MergedCandleFeaturesColumns.LAST_EVENT_TIME,
                candle.getLong(CandleClosedColumns.LAST_EVENT_TIME));
        row.setField(
                MergedCandleFeaturesColumns.LAST_EVENT_FINGERPRINT,
                candle.isNullAt(CandleClosedColumns.LAST_EVENT_FINGERPRINT)
                        ? null
                        : candle.getString(CandleClosedColumns.LAST_EVENT_FINGERPRINT));
        row.setField(
                MergedCandleFeaturesColumns.SCHEMA_VERSION,
                candle.getString(CandleClosedColumns.SCHEMA_VERSION));
        row.setField(MergedCandleFeaturesColumns.FEATURES, new GenericMapData(features));
        row.setField(MergedCandleFeaturesColumns.SEALED, sealed);
        return row;
    }
}
