package com.trading.compute.feature;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.compute.signaljob.Timeframe;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PerInstrumentFeaturesTest {

    private static void closeOneMinute(PerInstrumentFeatures features, long closePaise) {
        features.onClosedCandle(Timeframe.ONE_M, 0L, 59_999L, 0L, 0L, 0L, closePaise, 0L, 0L);
    }

    @Test
    void tickFeatureLandsInEveryDeclaredTimeframeSnapshot() {
        PerInstrumentFeatures features = new PerInstrumentFeatures();
        features.onTick(1_000L, 12_345L, 100L, 3L);
        Map<Integer, Double> out = new HashMap<>();
        for (Timeframe tf : Timeframe.values()) {
            assertEquals(12_345.0, features.latest(0, tf), 1e-9, "tf=" + tf);
            out.clear();
            features.snapshot(tf, out);
            assertEquals(Map.of(0, 12_345.0), out, "tf=" + tf);
        }
    }

    @Test
    void closeFeaturesUpdateOnlyTheirDeclaredTimeframes() {
        PerInstrumentFeatures features = new PerInstrumentFeatures();
        for (int i = 1; i <= 20; i++) {
            closeOneMinute(features, i * 100L);
        }
        Map<Integer, Double> oneMinute = new HashMap<>();
        features.snapshot(Timeframe.ONE_M, oneMinute);
        assertEquals(1_050.0, oneMinute.get(1).doubleValue(), 1e-9); // mean(1..20)*100
        assertEquals(100.0, oneMinute.get(2).doubleValue(), 1e-9); // strictly rising -> RSI 100
        Map<Integer, Double> threeMinute = new HashMap<>();
        features.snapshot(Timeframe.THREE_M, threeMinute);
        assertTrue(threeMinute.isEmpty(), "THREE_M computers are separate and were never fed");
        Map<Integer, Double> fifteenSecond = new HashMap<>();
        features.snapshot(Timeframe.FIFTEEN_S, fifteenSecond);
        assertTrue(fifteenSecond.isEmpty());
    }

    @Test
    void closeValueNeverLeaksAcrossTimeframes() {
        PerInstrumentFeatures features = new PerInstrumentFeatures();
        for (int i = 1; i <= 20; i++) {
            closeOneMinute(features, i * 100L);
        }
        assertEquals(1_050.0, features.latest(1, Timeframe.ONE_M), 1e-9);
        assertTrue(
                Double.isNaN(features.latest(1, Timeframe.THREE_M)),
                "the 1 m SMA must not surface as the 3 m SMA");
    }

    @Test
    void tickValuesFlowIntoEveryDeclaredTimeframeWhileCloseValuesStayPerTimeframe() {
        PerInstrumentFeatures features = new PerInstrumentFeatures();
        features.onTick(1_000L, 555L, 9L, 1L);
        for (int i = 1; i <= 20; i++) {
            closeOneMinute(features, i * 100L);
        }
        Map<Integer, Double> oneMinute = new HashMap<>();
        features.snapshot(Timeframe.ONE_M, oneMinute);
        assertEquals(Set.of(0, 1, 2), oneMinute.keySet());
        assertEquals(555.0, oneMinute.get(0).doubleValue(), 1e-9);
        Map<Integer, Double> threeMinute = new HashMap<>();
        features.snapshot(Timeframe.THREE_M, threeMinute);
        assertEquals(Set.of(0), threeMinute.keySet(), "only the tick feature is ready for THREE_M");
    }

    @Test
    void snapshotSkipsNotReadyFeatures() {
        PerInstrumentFeatures features = new PerInstrumentFeatures();
        closeOneMinute(features, 100L);
        Map<Integer, Double> out = new HashMap<>();
        features.snapshot(Timeframe.ONE_M, out);
        assertFalse(out.containsKey(1));
        assertFalse(out.containsKey(2));
        assertTrue(out.isEmpty(), "SMA/RSI are not ready after one close");
    }

    @Test
    void latestRejectsUnknownIds() {
        PerInstrumentFeatures features = new PerInstrumentFeatures();
        assertThrows(
                IllegalArgumentException.class, () -> features.latest(FeatureRegistry.SIZE, Timeframe.ONE_M));
        assertThrows(IllegalArgumentException.class, () -> features.latest(-1, Timeframe.ONE_M));
    }

    @Test
    void latestLivePreviewsCloseFeaturesWithTheFormingCandle() {
        PerInstrumentFeatures features = new PerInstrumentFeatures();
        for (int i = 1; i <= 19; i++) {
            closeOneMinute(features, i * 100L);
        }
        assertTrue(
                Double.isNaN(features.latest(1, Timeframe.ONE_M)),
                "19 closes: the closed SMA(20) is one short of ready");

        features.onFormingCandle(Timeframe.ONE_M, 1_200_000L, 0L, 0L, 0L, 2_000L, 0L, 0L);
        assertEquals(1_050.0, features.latestLive(1, Timeframe.ONE_M), 1e-9, "(19000 + 2000) / 20");
        features.onFormingCandle(Timeframe.ONE_M, 1_200_000L, 0L, 0L, 0L, 3_000L, 0L, 0L);
        assertEquals(
                1_100.0,
                features.latestLive(1, Timeframe.ONE_M),
                1e-9,
                "each tick moves the live value with the forming close");
        assertTrue(Double.isNaN(features.latest(1, Timeframe.ONE_M)), "the closed view is untouched");
        assertEquals(
                100.0,
                features.latestLive(2, Timeframe.ONE_M),
                1e-9,
                "monotonic closes + forming gain → the forming RSI(14) is 100");

        // The forming candle closes (same window): the live accessor falls back
        // to the closed value — the forming close is never counted twice.
        features.onClosedCandle(Timeframe.ONE_M, 1_200_000L, 1_259_999L, 0L, 0L, 0L, 3_000L, 0L, 0L);
        assertEquals(1_100.0, features.latestLive(1, Timeframe.ONE_M), 1e-9);
        assertEquals(1_100.0, features.latest(1, Timeframe.ONE_M), 1e-9);
    }

    @Test
    void latestLiveFallsBackWhenNoFormingCandleOrFeatureMissing() {
        PerInstrumentFeatures features = new PerInstrumentFeatures();
        for (int i = 1; i <= 20; i++) {
            closeOneMinute(features, i * 100L);
        }
        assertEquals(
                1_050.0, features.latestLive(1, Timeframe.ONE_M), 1e-9, "no forming candle → closed value");
        assertTrue(
                Double.isNaN(features.latestLive(1, Timeframe.THIRTY_S)),
                "the SMA is not declared for THIRTY_S");
        features.onFormingCandle(Timeframe.THIRTY_S, 0L, 0L, 0L, 0L, 999L, 0L, 0L);
        assertTrue(
                Double.isNaN(features.latestLive(1, Timeframe.THIRTY_S)),
                "a forming 30 s candle cannot invent an undeclared feature");
    }
}
