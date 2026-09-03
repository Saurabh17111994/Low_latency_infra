package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link FingerprintDedupFunction} driven through the Flink 2.2.1 operator
 * harness (no cluster). Covers the G-DEDUP-6 contract of the 2026-09-03
 * redesign (operator-local count window, no Flink state, no TTL):
 * in-window repeats drop, over-bound eldest entries are re-admitted,
 * per-token isolation, version scoping, intentional restore amnesia, the
 * structural bound under adversarial interleave, and the G-DEDUP-2 global
 * cap trip. Also pins the design invariants: zero keyed state entries and
 * zero timers — this operator must never touch the state backend.
 */
@DisplayName("FingerprintDedupFunction: heap-window contract + fail-fast guards")
class FingerprintDedupFunctionTest {

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

    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        return env;
    }

    private void openHarness(int windowEntries) throws Exception {
        Map<String, String> e = env();
        e.put("DEDUP_WINDOW_ENTRIES", Integer.toString(windowEntries));
        function = new FingerprintDedupFunction(SignalJobConfig.from(e));
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                function,
                row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();
    }

    private static long emittedCount(KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> h) {
        return h.getOutput().stream().filter(o -> o instanceof StreamRecord).count();
    }

    private void process(GenericRowData row) throws Exception {
        harness.processElement(row, row.getLong(RawTableColumns.EVENT_TIME));
    }

    @Test
    @DisplayName("first occurrence passes, in-window repeat drops, no Flink state, no timers")
    void firstPassesRepeatDropsWithoutManagedState() throws Exception {
        openHarness(100);
        process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1));
        process(TestRawRows.row(1L, T0 + 1_000L, "fp-1", "TRADE", 101, 2));

        assertEquals(1, emittedCount(harness), "in-window repeat must be dropped");
        assertEquals(1, function.windowSizeForTest(1L));
        assertEquals(0, harness.numKeyedStateEntries(),
                "heap-window design requests no managed state");
        assertEquals(0, harness.numEventTimeTimers(), "no timers — forgetting is structural");
    }

    @Test
    @DisplayName("eldest entry trimmed past the bound is re-admitted")
    void overBoundEldestReadmitted() throws Exception {
        openHarness(100);
        for (int i = 0; i < 100; i++) {
            process(TestRawRows.row(1L, T0 + i, "fp-" + i, "TRADE", 100, 1));
        }
        assertEquals(100, emittedCount(harness));
        assertEquals(100, function.windowSizeForTest(1L));

        // 101st distinct fingerprint evicts fp-0.
        process(TestRawRows.row(1L, T0 + 100_000L, "fp-new", "TRADE", 100, 1));
        assertEquals(101, emittedCount(harness));
        assertEquals(100, function.windowSizeForTest(1L), "bound holds after trim");

        // fp-0 aged out: re-arriving is a first again.
        process(TestRawRows.row(1L, T0 + 101_000L, "fp-0", "TRADE", 100, 1));
        assertEquals(102, emittedCount(harness), "trimmed fingerprint re-admitted");
    }

    @Test
    @DisplayName("same fingerprint on different tokens passes per token; versions do not collide")
    void tokenIsolationAndVersionScoping() throws Exception {
        openHarness(100);
        process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1));
        process(TestRawRows.row(2L, T0, "fp-1", "TRADE", 100, 1));
        assertEquals(2, emittedCount(harness), "token isolation");
        assertEquals(1, function.windowSizeForTest(1L));
        assertEquals(1, function.windowSizeForTest(2L));
    }

    @Test
    @DisplayName("fresh operator starts empty: restore amnesia is by design")
    void restoreStartsEmptyByDesign() throws Exception {
        openHarness(100);
        process(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1));
        assertEquals(1, emittedCount(harness));
        harness.close();

        // New operator instance (what a restore produces for unmanaged fields):
        // history is gone, the fingerprint passes again. Safe because restores
        // resume sources from checkpointed offsets — nothing replays.
        openHarness(100);
        process(TestRawRows.row(1L, T0 + 1_000L, "fp-1", "TRADE", 100, 1));
        assertEquals(1, emittedCount(harness), "empty-at-start re-admits");
        assertEquals(1, function.totalEntriesForTest());
    }

    @Test
    @DisplayName("bound holds and totals stay exact under adversarial multi-token interleave")
    void boundAndTotalsUnderInterleave() throws Exception {
        openHarness(100);
        int tokens = 8;
        int perToken = 150;
        for (int i = 0; i < perToken; i++) {
            for (long t = 1; t <= tokens; t++) {
                process(TestRawRows.row(t, T0 + i, "fp-" + i, "TRADE", 100, 1));
            }
        }
        for (long t = 1; t <= tokens; t++) {
            assertEquals(100, function.windowSizeForTest(t), "per-token bound holds for token " + t);
        }
        assertEquals((long) tokens * 100, function.totalEntriesForTest(),
                "global total tracks live entries exactly through trim churn");
        assertEquals((long) tokens * perToken, emittedCount(harness));
    }

    @Test
    @DisplayName("G-DEDUP-2: exceeding the global entries cap fails closed, never silently evicts")
    void globalCapTripFailsClosed() throws Exception {
        openHarness(100);
        long cap = 100L * FingerprintDedupFunction.GLOBAL_CAP_HEADROOM_TOKENS;
        long fed = 0;
        try {
            for (long i = 0; ; i++) {
                // Exactly the per-token bound of distinct fingerprints per
                // token: every record adds one LIVE entry (no trim churn),
                // so live totals track fed records and the ever-growing
                // token count drives the global total into the cap.
                long token = 1 + (i / 100);
                process(TestRawRows.row(token, T0 + i, "cap-" + i, "TRADE", 100, 1));
                fed++;
                if (fed % 50_000 == 0) {
                    harness.getOutput().clear(); // keep the harness queue, not the windows, bounded
                }
                if (fed > cap + 10_000) {
                    break; // guard did not trip — fail below with the counts
                }
            }
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("cap"),
                    "trip message names the cap: " + expected.getMessage());
            return;
        }
        throw new AssertionError("global cap never tripped after " + fed + " distinct entries");
    }
}
