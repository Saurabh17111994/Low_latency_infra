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

    /**
     * CHG-526: every forming event materializes each declared CLOSE feature's
     * live value once — the slab holds the preview, the read returns it, and a
     * later forming event overwrites it. Undeclared timeframes get no slab.
     */
    @Test
    void formingEventMaterializesTheLiveSlabForEveryDeclaredCloseFeature() {
        PerInstrumentFeatures features = new PerInstrumentFeatures();
        features.onFormingCandle(Timeframe.ONE_M, 1_200_000L, 0L, 0L, 0L, 2_000L, 0L, 0L);
        assertTrue(
                Double.isNaN(features.liveValueForTest(1, Timeframe.ONE_M)),
                "no closed history: the materialized SMA(20) preview is not ready");
        for (int i = 1; i <= 19; i++) {
            closeOneMinute(features, i * 100L);
        }
        features.onFormingCandle(Timeframe.ONE_M, 1_200_000L, 0L, 0L, 0L, 2_000L, 0L, 0L);
        assertEquals(1_050.0, features.liveValueForTest(1, Timeframe.ONE_M), 1e-9);
        assertEquals(100.0, features.liveValueForTest(2, Timeframe.ONE_M), 1e-9);
        assertEquals(
                1_050.0,
                features.latestLive(1, Timeframe.ONE_M),
                1e-9,
                "the read returns the materialized value");
        features.onFormingCandle(Timeframe.ONE_M, 1_200_000L, 0L, 0L, 0L, 3_000L, 0L, 0L);
        assertEquals(
                1_100.0,
                features.liveValueForTest(1, Timeframe.ONE_M),
                1e-9,
                "each forming event overwrites the slab");
        assertTrue(
                Double.isNaN(features.liveValueForTest(1, Timeframe.THIRTY_S)),
                "undeclared timeframe: no slab entry, never another timeframe's value");
        assertTrue(Double.isNaN(features.latestLive(1, Timeframe.THIRTY_S)));
    }

    /** CHG-526: at the close the live slab is set to the just-closed value. */
    @Test
    void liveSlabMatchesTheClosedValueAtClose() {
        PerInstrumentFeatures features = new PerInstrumentFeatures();
        for (int i = 1; i <= 19; i++) {
            closeOneMinute(features, i * 100L);
        }
        features.onFormingCandle(Timeframe.ONE_M, 1_200_000L, 0L, 0L, 0L, 2_000L, 0L, 0L);
        assertEquals(1_050.0, features.liveValueForTest(1, Timeframe.ONE_M), 1e-9);
        closeOneMinute(features, 2_000L);
        assertEquals(
                features.latest(1, Timeframe.ONE_M),
                features.liveValueForTest(1, Timeframe.ONE_M),
                1e-9,
                "closed and live agree once the window closes");
        assertEquals(1_050.0, features.latestLive(1, Timeframe.ONE_M), 1e-9);
    }

    /** CHG-526 correctness guard: the storage snapshot never reads the live slab. */
    @Test
    void storageSnapshotNeverCarriesLiveValues() {
        PerInstrumentFeatures features = new PerInstrumentFeatures();
        for (int i = 1; i <= 20; i++) {
            closeOneMinute(features, i * 100L);
        }
        Map<Integer, Double> before = new HashMap<>();
        features.snapshot(Timeframe.ONE_M, before);
        assertEquals(1_050.0, before.get(1).doubleValue(), 1e-9);
        features.onFormingCandle(Timeframe.ONE_M, 1_200_000L, 0L, 0L, 0L, 9_999L, 0L, 0L);
        assertEquals(
                1_544.95,
                features.latestLive(1, Timeframe.ONE_M),
                1e-9,
                "the live value moved with the forming close (21000 - 100 + 9999) / 20");
        Map<Integer, Double> after = new HashMap<>();
        features.snapshot(Timeframe.ONE_M, after);
        assertEquals(before, after, "the sealed row keeps the closed values only");
    }

    /** CHG-526 kill switch: same numbers computed on read, no slab allocated. */
    @Test
    void killSwitchComputesTheSameLiveValueOnReadWithoutASlab() {
        PerInstrumentFeatures precomputed = new PerInstrumentFeatures();
        PerInstrumentFeatures onRead = new PerInstrumentFeatures(false);
        for (int i = 1; i <= 19; i++) {
            closeOneMinute(precomputed, i * 100L);
            closeOneMinute(onRead, i * 100L);
        }
        precomputed.onFormingCandle(Timeframe.ONE_M, 1_200_000L, 0L, 0L, 0L, 2_000L, 0L, 0L);
        onRead.onFormingCandle(Timeframe.ONE_M, 1_200_000L, 0L, 0L, 0L, 2_000L, 0L, 0L);
        assertEquals(
                precomputed.latestLive(1, Timeframe.ONE_M),
                onRead.latestLive(1, Timeframe.ONE_M),
                1e-9,
                "the kill switch must not change the numbers");
        assertEquals(
                precomputed.latestLive(2, Timeframe.ONE_M),
                onRead.latestLive(2, Timeframe.ONE_M),
                1e-9);
        assertTrue(
                Double.isNaN(onRead.liveValueForTest(1, Timeframe.ONE_M)),
                "no slab is allocated when precompute is off");
        precomputed.onClosedCandle(
                Timeframe.ONE_M, 1_200_000L, 1_259_999L, 0L, 0L, 0L, 2_000L, 0L, 0L);
        onRead.onClosedCandle(
                Timeframe.ONE_M, 1_200_000L, 1_259_999L, 0L, 0L, 0L, 2_000L, 0L, 0L);
        assertEquals(precomputed.latest(1, Timeframe.ONE_M), onRead.latest(1, Timeframe.ONE_M), 1e-9);
        assertEquals(
                precomputed.latestLive(1, Timeframe.ONE_M),
                onRead.latestLive(1, Timeframe.ONE_M),
                1e-9);
    }
}
