package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tracker 14 P5.1 (reworked 2026-09-03 for the heap-window redesign) —
 * baseline measurements of the live dedup hot path through the Flink 2.2.1
 * operator harness (no cluster). Each measurement prints as a {@code
 * RDES[...]} System.out line so the surefire report is a self-contained
 * evidence record.
 *
 * <p>The TTL/state-size legs are gone with the redesign (no MapState, no
 * expiry, no gauge machinery). What is measured instead: the structural
 * bound under volume, exact live-entry accounting through trim churn, and
 * restore amnesia — the three properties the production ceiling depends on.
 */
@DisplayName("Tracker 14 P5.1: heap-window dedup baseline measurements")
class DedupBaselineMeasurementTest {

    private static final long T0 = 1_700_000_000_000L;

    private KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> harness;
    private FingerprintDedupFunction function;

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    private static Map<String, String> env(int window) {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_WINDOW_ENTRIES", Integer.toString(window));
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        return env;
    }

    private void openHarness(int window) throws Exception {
        function = new FingerprintDedupFunction(SignalJobConfig.from(env(window)));
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                function,
                row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();
    }

    @Test
    @DisplayName("window bound, live totals, and trim churn across feed waves")
    void measureWindowAccountingAcrossFeedWaves() throws Exception {
        openHarness(2_000);
        int tokens = 4;
        int perTokenWave = 1_000;

        for (int wave = 0; wave < 3; wave++) {
            for (int i = 0; i < perTokenWave; i++) {
                for (long t = 1; t <= tokens; t++) {
                    harness.processElement(
                            TestRawRows.row(t, T0 + wave * 100_000L + i,
                                    "rd-wave-" + wave + "-" + i, "TRADE", 10_000L + i, 100L),
                            T0 + wave * 100_000L + i);
                }
            }
            long live = (long) tokens * Math.min(2_000, (wave + 1) * perTokenWave);
            for (long t = 1; t <= tokens; t++) {
                assertEquals(Math.min(2_000, (wave + 1) * perTokenWave),
                        function.windowSizeForTest(t), "per-token bound, wave " + wave);
            }
            assertEquals(live, function.totalEntriesForTest(), "exact live total, wave " + wave);
            System.out.println("RDES[waves] wave" + wave + " live=" + live
                    + " tokens=" + tokens);
        }
        // 12,000 distinct > 8,000 capacity: eldest trimmed, totals still exact.
        assertEquals(8_000L, function.totalEntriesForTest());
        assertTrue(harness.numKeyedStateEntries() == 0, "no managed state requested");
    }

    @Test
    @DisplayName("restore starts empty and rebuilds from live flow")
    void measureRestoreRebuild() throws Exception {
        openHarness(2_000);
        harness.processElement(TestRawRows.row(1L, T0, "rd-1", "TRADE", 100L, 1L), T0);
        assertEquals(1, function.totalEntriesForTest());
        harness.close();

        openHarness(2_000);
        assertEquals(0, function.totalEntriesForTest(), "restore starts empty by design");
        harness.processElement(TestRawRows.row(1L, T0 + 1, "rd-1", "TRADE", 100L, 1L), T0 + 1);
        assertEquals(1, function.totalEntriesForTest(), "rebuilds from live flow");
        System.out.println("RDES[restore] rebuilt=1");
    }
}
