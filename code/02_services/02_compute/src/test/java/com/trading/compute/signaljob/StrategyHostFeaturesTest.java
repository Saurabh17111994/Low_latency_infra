package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.compute.feature.FeatureView;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * DEC-056 feature wiring in the strategy host: features update before the
 * fan-out, every strategy reads the same shared {@link FeatureView}, legacy
 * two-argument strategies keep working, and a failing feature update is
 * counted without blocking delivery.
 */
class StrategyHostFeaturesTest {

    private static final long TOKEN = 7L;

    private KeyedTwoInputStreamOperatorTestHarness<Long, RowData, RowData, RowData> harness;
    private StrategyHostFunction function;

    @BeforeEach
    void registerProbes() {
        Strategies.registerForTest(FeatureProbe.RULE_ID, (config, metrics) -> new FeatureProbe());
        Strategies.registerForTest(LegacyProbe.RULE_ID, (config, metrics) -> new LegacyProbe());
    }

    @AfterEach
    void tearDown() throws Exception {
        Strategies.unregisterForTest(FeatureProbe.RULE_ID);
        Strategies.unregisterForTest(LegacyProbe.RULE_ID);
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_WINDOW_ENTRIES", "2000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        return env;
    }

    private void open(String strategyId) throws Exception {
        function = new StrategyHostFunction(SignalJobConfig.from(env()), List.of(strategyId));
        harness = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
                function,
                r -> r.getLong(CandleLiveColumns.INSTRUMENT_TOKEN),
                r -> r.getLong(CandleClosedColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();
    }

    private void openWithFeatureRows(String strategyId) throws Exception {
        function = new StrategyHostFunction(SignalJobConfig.from(env()), List.of(strategyId), true);
        harness = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
                function,
                r -> r.getLong(CandleLiveColumns.INSTRUMENT_TOKEN),
                r -> r.getLong(CandleClosedColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();
    }

    private List<RowData> featureRows() {
        java.util.Queue<org.apache.flink.streaming.runtime.streamrecord.StreamRecord<RowData>> records =
                harness.getSideOutput(StrategyHostFunction.FEATURE_ROWS);
        List<RowData> out = new java.util.ArrayList<>();
        if (records == null) {
            return out; // no side output registered: nothing was ever emitted
        }
        for (org.apache.flink.streaming.runtime.streamrecord.StreamRecord<RowData> record : records) {
            out.add(record.getValue());
        }
        return out;
    }

    private static java.util.Map<Integer, Double> mapOf(RowData row) {
        MapData map = row.getMap(FeatureValuesColumns.FEATURES);
        java.util.Map<Integer, Double> out = new HashMap<>();
        for (int i = 0; i < map.size(); i++) {
            out.put(map.keyArray().getInt(i), map.valueArray().getDouble(i));
        }
        return out;
    }

    private FeatureProbe probe() {
        return (FeatureProbe) function.strategyForTest(TOKEN, FeatureProbe.RULE_ID);
    }

    private LegacyProbe legacy() {
        return (LegacyProbe) function.strategyForTest(TOKEN, LegacyProbe.RULE_ID);
    }

    // — row builders (same layouts as StrategyHostFunctionTest) ————————————

    private static RowData live(long token, Timeframe tf, long ws, long price, long volume, int ticks) {
        GenericRowData r = new GenericRowData(CandleLiveColumns.FIELD_COUNT);
        r.setField(CandleLiveColumns.INSTRUMENT_TOKEN, token);
        r.setField(CandleLiveColumns.EXCHANGE, StringData.fromString("NSE"));
        r.setField(CandleLiveColumns.SYMBOL, StringData.fromString("TEST"));
        r.setField(CandleLiveColumns.TF, StringData.fromString(tf.code()));
        r.setField(CandleLiveColumns.WINDOW_START, ws);
        r.setField(CandleLiveColumns.WINDOW_END, ws + tf.windowMs());
        r.setField(CandleLiveColumns.OPEN_PAISE, price);
        r.setField(CandleLiveColumns.HIGH_PAISE, price);
        r.setField(CandleLiveColumns.LOW_PAISE, price);
        r.setField(CandleLiveColumns.CLOSE_PAISE, price);
        r.setField(CandleLiveColumns.VOLUME, volume);
        r.setField(CandleLiveColumns.TICK_COUNT, ticks);
        r.setField(CandleLiveColumns.LAST_EVENT_TIME, ws + 1_000L);
        r.setField(CandleLiveColumns.LAST_EVENT_FINGERPRINT, StringData.fromString("fp"));
        r.setField(CandleLiveColumns.SCHEMA_VERSION, StringData.fromString("1"));
        return r;
    }

    private static RowData closed(long token, String tfCode, long ws, long close, long volume, int ticks) {
        GenericRowData r = new GenericRowData(CandleClosedColumns.FIELD_COUNT);
        r.setField(CandleClosedColumns.INSTRUMENT_TOKEN, token);
        r.setField(CandleClosedColumns.EXCHANGE, StringData.fromString("NSE"));
        r.setField(CandleClosedColumns.SYMBOL, StringData.fromString("TEST"));
        r.setField(CandleClosedColumns.TF, StringData.fromString(tfCode));
        r.setField(CandleClosedColumns.WINDOW_START, ws);
        r.setField(CandleClosedColumns.WINDOW_END, ws + 60_000L);
        r.setField(CandleClosedColumns.OPEN_PAISE, close);
        r.setField(CandleClosedColumns.HIGH_PAISE, close);
        r.setField(CandleClosedColumns.LOW_PAISE, close);
        r.setField(CandleClosedColumns.CLOSE_PAISE, close);
        r.setField(CandleClosedColumns.VOLUME, volume);
        r.setField(CandleClosedColumns.TICK_COUNT, ticks);
        r.setField(CandleClosedColumns.LAST_EVENT_TIME, ws + 1_000L);
        r.setField(CandleClosedColumns.LAST_EVENT_FINGERPRINT, StringData.fromString("fp"));
        r.setField(CandleClosedColumns.SCHEMA_VERSION, StringData.fromString("1"));
        return r;
    }

    @Test
    void liveTickUpdatesFeaturesBeforeTheStrategyReadsThem() throws Exception {
        open(FeatureProbe.RULE_ID);
        harness.processElement1(live(TOKEN, Timeframe.FIFTEEN_S, 0L, 12_345L, 100L, 5), 1_000L);

        FeatureProbe probe = probe();
        assertEquals(1, probe.liveCalls);
        assertEquals(12_345.0, probe.lastPrice, 1e-9, "strategy must read the fresh tick value");
        assertNotNull(probe.lastView);
        assertEquals(1, function.featureTickUpdatesForTest());
        assertEquals(
                12_345.0,
                function.featuresForTest(TOKEN).latest(0, Timeframe.FIFTEEN_S),
                1e-9);
        assertThrows(
                IllegalArgumentException.class,
                () -> probe.lastView.latest("no-such-feature", Timeframe.ONE_M));
    }

    private static RowData liveCode(long token, String tfCode, long ws, long price, long volume, int ticks) {
        GenericRowData r = (GenericRowData) live(token, Timeframe.FIFTEEN_S, ws, price, volume, ticks);
        r.setField(CandleLiveColumns.TF, StringData.fromString(tfCode));
        return r;
    }

    @Test
    void snapshotFallbackFeedsTickFeaturesOncePerSnapshot() throws Exception {
        // L3-4: the MULTITF_FAST_LIVE_FEED=false fallback feeds all six forming
        // rows of one snapshot into the host. TICK features are
        // timeframe-independent, so exactly one row may update them — the
        // canonical FIFTEEN_S row — while all six still fan out to strategies.
        open(FeatureProbe.RULE_ID);
        Timeframe[] tfs = {
            Timeframe.FIFTEEN_S,
            Timeframe.THIRTY_S,
            Timeframe.ONE_M,
            Timeframe.THREE_M,
            Timeframe.FIVE_M,
            Timeframe.FIFTEEN_M,
        };
        for (int i = 0; i < tfs.length; i++) {
            harness.processElement1(live(TOKEN, tfs[i], 0L, 100L + i, 10L, 1), 1_000L + i);
        }

        assertEquals(6, probe().liveCalls, "every TF row still fans out to strategies");
        assertEquals(
                1,
                function.featureTickUpdatesForTest(),
                "one tick-feature update per snapshot, not six (L3-4)");
        assertEquals(
                100.0,
                function.featuresForTest(TOKEN).latest(0, Timeframe.FIFTEEN_S),
                1e-9,
                "the FIFTEEN_S forming value must be the one fed to the tick feature");
        assertEquals(
                100.0,
                probe().lastPrice,
                1e-9,
                "every strategy reads the shared tick value (the FIFTEEN_S one)");
    }

    @Test
    void invalidLiveTimeframeIsCountedAndDoesNotBlockDelivery() throws Exception {
        open(FeatureProbe.RULE_ID);
        harness.processElement1(liveCode(TOKEN, "NOPE", 0L, 777L, 10L, 1), 1_000L);

        assertEquals(0, function.featureTickUpdatesForTest());
        assertEquals(1, function.featureFailuresForTest());
        assertEquals(1, probe().liveCalls, "strategy delivery must survive a feature failure");
    }

    @Test
    void closedCandleUpdatesCloseFeaturesPerTimeframe() throws Exception {
        open(FeatureProbe.RULE_ID);
        for (int i = 1; i <= 20; i++) {
            harness.processElement2(
                    closed(TOKEN, Timeframe.ONE_M.code(), i * 60_000L, i * 100L, 10L, 2), 1_000L + i);
        }

        FeatureProbe probe = probe();
        assertEquals(20, probe.closeCalls);
        assertEquals(1_050.0, probe.lastSmaOneMinute, 1e-9, "mean(1..20)*100");
        assertEquals(function.featuresForTest(TOKEN).latest(1, Timeframe.ONE_M), 1_050.0, 1e-9);
        assertTrue(
                Double.isNaN(function.featuresForTest(TOKEN).latest(1, Timeframe.FIVE_M)),
                "the 1 m SMA must not leak into the 5 m value");
        assertEquals(20, function.featureCloseUpdatesForTest());
    }

    @Test
    void legacyTwoArgumentStrategiesKeepWorking() throws Exception {
        open(LegacyProbe.RULE_ID);
        harness.processElement1(live(TOKEN, Timeframe.FIFTEEN_S, 0L, 500L, 10L, 1), 1_000L);
        harness.processElement2(
                closed(TOKEN, Timeframe.ONE_M.code(), 60_000L, 501L, 10L, 1), 2_000L);

        LegacyProbe legacy = legacy();
        assertEquals(1, legacy.liveCalls);
        assertEquals(1, legacy.closeCalls);
        // Features are computed for every instrument regardless of strategy shape.
        assertEquals(1, function.featureTickUpdatesForTest());
        assertEquals(1, function.featureCloseUpdatesForTest());
        assertFalse(Double.isNaN(function.featuresForTest(TOKEN).latest(0, Timeframe.ONE_M)));
    }

    @Test
    void invalidClosedTimeframeIsCountedAndDoesNotBlockDelivery() throws Exception {
        open(FeatureProbe.RULE_ID);
        harness.processElement2(
                closed(TOKEN, "NOPE", 60_000L, 777L, 10L, 1), 2_000L);

        assertEquals(0, function.featureCloseUpdatesForTest());
        assertEquals(1, function.featureFailuresForTest());
        assertEquals(1, probe().closeCalls, "strategy delivery must survive a feature failure");
    }

    @Test
    void closedCandleEmitsOneFeatureRowPerWindowWhenEnabled() throws Exception {
        openWithFeatureRows(LegacyProbe.RULE_ID);
        harness.processElement1(live(TOKEN, Timeframe.FIFTEEN_S, 0L, 12_345L, 100L, 5), 500L);
        for (int i = 1; i <= 20; i++) {
            harness.processElement2(
                    closed(TOKEN, Timeframe.ONE_M.code(), i * 60_000L, i * 100L, 10L, 2), 1_000L + i);
        }

        List<RowData> rows = featureRows();
        assertEquals(20, rows.size());
        assertEquals(20, function.featureRowsEmittedForTest());
        RowData last = rows.get(rows.size() - 1);
        assertEquals(TOKEN, last.getLong(FeatureValuesColumns.INSTRUMENT_TOKEN));
        assertEquals("ONE_M", last.getString(FeatureValuesColumns.TF).toString());
        assertEquals(20 * 60_000L, last.getLong(FeatureValuesColumns.WINDOW_START));
        java.util.Map<Integer, Double> features = mapOf(last);
        assertEquals(12_345.0, features.get(0).doubleValue(), 1e-9); // last_price (tick)
        assertEquals(1_050.0, features.get(1).doubleValue(), 1e-9); // sma_close_20
        assertEquals(100.0, features.get(2).doubleValue(), 1e-9); // rsi_close_14 (rising)
    }

    @Test
    void featureRowsStayOffByDefault() throws Exception {
        open(LegacyProbe.RULE_ID);
        harness.processElement1(live(TOKEN, Timeframe.FIFTEEN_S, 0L, 500L, 10L, 1), 500L);
        harness.processElement2(
                closed(TOKEN, Timeframe.ONE_M.code(), 60_000L, 100L, 10L, 1), 1_000L);

        assertTrue(
                featureRows().isEmpty(),
                "the stored layer must stay silent unless enabled");
        assertEquals(0, function.featureRowsEmittedForTest());
    }

    @Test
    void emptySnapshotEmitsNoRow() throws Exception {
        openWithFeatureRows(LegacyProbe.RULE_ID);
        // One close with no tick and no filled period: every feature is still NaN.
        harness.processElement2(
                closed(TOKEN, Timeframe.ONE_M.code(), 60_000L, 100L, 10L, 1), 1_000L);

        assertTrue(
                featureRows().isEmpty(),
                "not-ready features must not produce a placeholder row");
        assertEquals(0, function.featureRowsEmittedForTest());
    }

    /** Overrides the feature-aware overloads: the host calls these, not the legacy pair. */
    static final class FeatureProbe implements SignalStrategy {
        static final String RULE_ID = "feature-probe-v1";

        int liveCalls;
        int closeCalls;
        double lastPrice = Double.NaN;
        double lastSmaOneMinute = Double.NaN;
        FeatureView lastView;

        @Override
        public String ruleId() {
            return RULE_ID;
        }

        @Override
        public void onClosedCandle(RowData closed, Collector<RowData> out) {}

        @Override
        public void onLiveTick(RowData live, Collector<RowData> out) {}

        @Override
        public void onLiveTick(RowData live, FeatureView features, Collector<RowData> out) {
            liveCalls++;
            lastView = features;
            lastPrice = features.latest("last_price", Timeframe.FIFTEEN_S);
        }

        @Override
        public void onClosedCandle(RowData closed, FeatureView features, Collector<RowData> out) {
            closeCalls++;
            lastSmaOneMinute = features.latest("sma_close_20", Timeframe.ONE_M);
        }
    }

    /** Legacy shape: only the two-argument contract — the host must still deliver. */
    static final class LegacyProbe implements SignalStrategy {
        static final String RULE_ID = "feature-legacy-probe-v1";

        int liveCalls;
        int closeCalls;

        @Override
        public String ruleId() {
            return RULE_ID;
        }

        @Override
        public void onClosedCandle(RowData closed, Collector<RowData> out) {
            closeCalls++;
        }

        @Override
        public void onLiveTick(RowData live, Collector<RowData> out) {
            liveCalls++;
        }
    }
}
