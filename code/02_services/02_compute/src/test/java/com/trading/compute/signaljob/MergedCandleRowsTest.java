package com.trading.compute.signaljob;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;

/**
 * Wave B (DEC-059): the candle row -> merged row conversion. The candle part
 * is copied verbatim (nulls included), the feature snapshot and the seal flag
 * are added — nothing else. Pure, no cluster.
 */
class MergedCandleRowsTest {

    private static RowData candle() {
        GenericRowData r = new GenericRowData(CandleClosedColumns.FIELD_COUNT);
        r.setField(CandleClosedColumns.INSTRUMENT_TOKEN, 7L);
        r.setField(CandleClosedColumns.EXCHANGE, StringData.fromString("NSE"));
        r.setField(CandleClosedColumns.SYMBOL, null); // nullable in the DDL — must stay null
        r.setField(CandleClosedColumns.TF, StringData.fromString("ONE_M"));
        r.setField(CandleClosedColumns.WINDOW_START, 60_000L);
        r.setField(CandleClosedColumns.WINDOW_END, 120_000L);
        r.setField(CandleClosedColumns.OPEN_PAISE, 100L);
        r.setField(CandleClosedColumns.HIGH_PAISE, 130L);
        r.setField(CandleClosedColumns.LOW_PAISE, 90L);
        r.setField(CandleClosedColumns.CLOSE_PAISE, 120L);
        r.setField(CandleClosedColumns.VOLUME, 500L);
        r.setField(CandleClosedColumns.TICK_COUNT, 9);
        r.setField(CandleClosedColumns.LAST_EVENT_TIME, 119_000L);
        r.setField(CandleClosedColumns.LAST_EVENT_FINGERPRINT, null);
        r.setField(CandleClosedColumns.SCHEMA_VERSION, StringData.fromString("1"));
        return r;
    }

    @Test
    void copiesTheCandleColumnsVerbatimAndAddsFeaturesAndSeal() {
        Map<Integer, Double> features = new LinkedHashMap<>();
        features.put(0, 12_345.0);
        features.put(3, 7.5);

        GenericRowData row = MergedCandleRows.fromCandleRow(candle(), features, true);

        assertThat(row.getArity()).isEqualTo(MergedCandleFeaturesColumns.FIELD_COUNT);
        assertThat(row.getLong(MergedCandleFeaturesColumns.INSTRUMENT_TOKEN)).isEqualTo(7L);
        assertThat(row.getString(MergedCandleFeaturesColumns.EXCHANGE).toString()).isEqualTo("NSE");
        assertThat(row.isNullAt(MergedCandleFeaturesColumns.SYMBOL)).isTrue();
        assertThat(row.getString(MergedCandleFeaturesColumns.TF).toString()).isEqualTo("ONE_M");
        assertThat(row.getLong(MergedCandleFeaturesColumns.WINDOW_START)).isEqualTo(60_000L);
        assertThat(row.getLong(MergedCandleFeaturesColumns.WINDOW_END)).isEqualTo(120_000L);
        assertThat(row.getLong(MergedCandleFeaturesColumns.OPEN_PAISE)).isEqualTo(100L);
        assertThat(row.getLong(MergedCandleFeaturesColumns.HIGH_PAISE)).isEqualTo(130L);
        assertThat(row.getLong(MergedCandleFeaturesColumns.LOW_PAISE)).isEqualTo(90L);
        assertThat(row.getLong(MergedCandleFeaturesColumns.CLOSE_PAISE)).isEqualTo(120L);
        assertThat(row.getLong(MergedCandleFeaturesColumns.VOLUME)).isEqualTo(500L);
        assertThat(row.getInt(MergedCandleFeaturesColumns.TICK_COUNT)).isEqualTo(9);
        assertThat(row.getLong(MergedCandleFeaturesColumns.LAST_EVENT_TIME)).isEqualTo(119_000L);
        assertThat(row.isNullAt(MergedCandleFeaturesColumns.LAST_EVENT_FINGERPRINT)).isTrue();
        assertThat(row.getString(MergedCandleFeaturesColumns.SCHEMA_VERSION).toString())
                .isEqualTo("1");
        MapData map = row.getMap(MergedCandleFeaturesColumns.FEATURES);
        assertThat(map.size()).isEqualTo(2);
        assertThat(map.keyArray().getInt(0)).isEqualTo(0);
        assertThat(map.valueArray().getDouble(0)).isEqualTo(12_345.0);
        assertThat(row.getBoolean(MergedCandleFeaturesColumns.SEALED)).isTrue();
    }

    @Test
    void emptyFeaturesAndFormingSealStillProduceARow() {
        GenericRowData row = MergedCandleRows.fromCandleRow(candle(), Map.of(), false);

        assertThat(row.getMap(MergedCandleFeaturesColumns.FEATURES).size()).isZero();
        assertThat(row.getBoolean(MergedCandleFeaturesColumns.SEALED)).isFalse();
    }

    @Test
    void readerPredicatesMatchTheSealFlag() {
        // DEC-059 reader rule: finished windows filter sealed=true, the
        // now-view reads the forming row.
        GenericRowData sealedRow = MergedCandleRows.fromCandleRow(candle(), Map.of(), true);
        GenericRowData formingRow = MergedCandleRows.fromCandleRow(candle(), Map.of(), false);

        assertThat(MergedCandleFeaturesColumns.isSealed(sealedRow)).isTrue();
        assertThat(MergedCandleFeaturesColumns.isForming(sealedRow)).isFalse();
        assertThat(MergedCandleFeaturesColumns.isSealed(formingRow)).isFalse();
        assertThat(MergedCandleFeaturesColumns.isForming(formingRow)).isTrue();
    }
}
