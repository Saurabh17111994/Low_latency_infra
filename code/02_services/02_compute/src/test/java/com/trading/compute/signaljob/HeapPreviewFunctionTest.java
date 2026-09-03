package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Differential equivalence: {@link HeapPreviewFunction} (heap accumulators,
 * chain-heap redesign OP5) vs the real preview window operator (oracle,
 * {@code CandlePreviewTrigger} + aggregate + {@code CandlePreviewEmitFunction}).
 * The SAME tick/watermark script runs through both; preview rows (minus
 * processing-time {@code output_ts}) must match exactly, including cadence
 * count and content per window.
 */
@DisplayName("heap previews: cadence equivalence with the preview window operator (OP5)")
class HeapPreviewFunctionTest {

    /** 15000-aligned epoch anchor. */
    private static final long T0 = (1_700_000_000_000L / 15_000L) * 15_000L;
    private static final long WINDOW_MS = 15_000L;
    private static final long INTERVAL_MS = 1_000L;
    private static final long TOKEN = 7L;

    private KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> oracle;
    private KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> heap;
    private HeapPreviewFunction heapFn;

    @AfterEach
    void tearDown() throws Exception {
        if (oracle != null) {
            oracle.close();
        }
        if (heap != null) {
            heap.close();
        }
    }

    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_WINDOW_ENTRIES", "2000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("ALLOWED_LATENESS_MS", "5000");
        env.put("PREVIEW_ENABLED", "true");
        env.put("PREVIEW_INTERVAL_MS", "1000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        return env;
    }

    private void openBoth() throws Exception {
        oracle = CandlePreviewTriggerTest.create(WINDOW_MS, INTERVAL_MS);
        heapFn = new HeapPreviewFunction(SignalJobConfig.from(env()));
        heap = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                heapFn,
                row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        heap.open();
    }

    private interface Op {
    }

    private record Tick(long token, long offsetMs, long price) implements Op {
    }

    private record Mark(long watermark) implements Op {
    }

    private void runBoth(List<Op> script) throws Exception {
        for (Op op : script) {
            if (op instanceof Tick t) {
                long ts = T0 + t.offsetMs();
                RowData row = TestRawRows.row(t.token(), ts, "fp-" + t.offsetMs(), "TRADE",
                        t.price(), 1L);
                oracle.processElement(new StreamRecord<>(row, ts));
                heap.processElement(row, ts);
            } else if (op instanceof Mark m) {
                Watermark wm = new Watermark(m.watermark());
                oracle.processWatermark(wm);
                heap.processWatermark(wm);
            }
        }
    }

    private static String previewKey(RowData r) {
        // Every column except OUTPUT_TS (processing-time emit instant).
        return r.getLong(CandlePreviewColumns.INSTRUMENT_TOKEN) + "|"
                + r.getLong(CandlePreviewColumns.WINDOW_START) + "|"
                + r.getLong(CandlePreviewColumns.WINDOW_END) + "|"
                + r.getLong(CandlePreviewColumns.OPEN_PAISE) + "|"
                + r.getLong(CandlePreviewColumns.HIGH_PAISE) + "|"
                + r.getLong(CandlePreviewColumns.LOW_PAISE) + "|"
                + r.getLong(CandlePreviewColumns.CLOSE_PAISE) + "|"
                + r.getLong(CandlePreviewColumns.VOLUME) + "|"
                + r.getInt(CandlePreviewColumns.TICK_COUNT) + "|"
                + r.getBoolean(CandlePreviewColumns.IS_PREVIEW) + "|"
                + r.getLong(CandlePreviewColumns.LAST_EVENT_TS) + "|"
                + r.getString(CandlePreviewColumns.SCHEMA_VERSION);
    }

    private static List<String> mains(
            KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> h) {
        return h.getOutput().stream()
                .filter(o -> o instanceof StreamRecord<?> sr && sr.getValue() instanceof RowData r)
                .map(o -> previewKey((RowData) ((StreamRecord<?>) o).getValue()))
                .toList();
    }

    private void assertParity(String what) {
        assertEquals(mains(oracle), mains(heap), what + " — preview rows differ");
    }

    private long heapCounter(String field) throws Exception {
        Field f = HeapPreviewFunction.class.getDeclaredField(field);
        f.setAccessible(true);
        return ((Counter) f.get(heapFn)).getCount();
    }

    // -- scenarios --

    @Test
    void cadenceMatchesWindowByWindow() throws Exception {
        openBoth();
        // Window 1: continuous ticks; window 2: same key starts late.
        runBoth(List.of(
                new Tick(TOKEN, 100L, 100),
                new Mark(T0 + 1_000L),
                new Mark(T0 + 2_000L),
                new Mark(T0 + 4_000L),
                new Mark(T0 + 7_000L),
                new Mark(T0 + 15_000L),
                new Tick(TOKEN, 15_100L, 200),
                new Mark(T0 + 16_000L),
                new Mark(T0 + 17_000L),
                new Mark(T0 + 30_000L)));
        assertParity("full cadence across two windows");
        // W1 previews: at 1s,2s,3s,...; every fire emits one row → count is
        // the number of preview intervals covered.
        assertTrue(mains(heap).size() >= 8, "expected a full preview cadence, got "
                + mains(heap).size());
    }

    @Test
    void previewAccumulatorGrowsLive() throws Exception {
        openBoth();
        runBoth(List.of(
                new Tick(TOKEN, 100L, 100),
                new Mark(T0 + 1_000L),
                new Tick(TOKEN, 1_200L, 110),
                new Mark(T0 + 2_000L),
                new Mark(T0 + 15_000L)));
        assertParity("live-growing preview");
        // One watermark jump fires EVERY queued interval timer (1s..15s), so
        // a fully-formed window yields 15 preview rows — same in both.
        assertEquals(15, mains(heap).size(), "full cadence: 15 interval fires (1s..15s)");
        // Preview #1 fired at 1 s (before tick 2 at 1.2 s): first tick only.
        // Preview #2 fired at 2 s: the accumulator grew with tick 2.
        String first = mains(heap).get(0);
        String second = mains(heap).get(1);
        assertTrue(first.contains("|100|100|100|100|1|1|"),
                "first preview holds the first tick only: " + first);
        assertTrue(second.contains("|100|110|100|110|2|2|"),
                "second preview grew with the second tick (O=100 H/L=110/100 C=110, vol 2, n 2): " + second);
    }

    @Test
    void lateTickDropsLoudlyNotGarbagePreview() throws Exception {
        openBoth();
        runBoth(List.of(
                new Tick(TOKEN, 100L, 100),
                new Mark(T0 + 15_000L), // window closed (full 15-row cadence)
                new Mark(T0 + 16_000L),
                // Straggler for the CLOSED window WITHIN lateness (end + 5 s
                // bound): the old window operator resurrects the purged window
                // and emits a garbage partial preview over the final row; the
                // heap version drops it loudly.
                new Tick(TOKEN, 200L, 100)));
        List<String> oracleRows = mains(oracle);
        List<String> heapRows = mains(heap);
        assertTrue(oracleRows.size() > heapRows.size(),
                "the oracle resurrects the purged window (garbage partial preview);"
                        + " the heap version must NOT follow — oracle=" + oracleRows.size()
                        + " heap=" + heapRows.size());
        assertEquals(15, heapRows.size(), "heap emits only the pre-close cadence");
        assertEquals(1, heapCounter("lateDroppedCounter"), "the drop is counted loudly");
    }

    @Test
    void heapRequestsNoManagedStateOnlyTimers() throws Exception {
        openBoth();
        runBoth(List.of(
                new Tick(TOKEN, 100L, 100),
                new Mark(T0 + 1_000L),
                new Mark(T0 + 2_000L)));
        assertEquals(0, heap.numKeyedStateEntries(), "heap previews use no managed state");
        assertTrue(heap.numEventTimeTimers() >= 1, "cadence timers are the managed piece");
        assertEquals(1, heapFn.slotCountForTest());
    }

    @Test
    void slotDisappearsAfterWindowDone() throws Exception {
        openBoth();
        runBoth(List.of(
                new Tick(TOKEN, 100L, 100),
                new Mark(T0 + 15_000L)));
        assertEquals(0, heapFn.slotCountForTest(),
                "no slot after the window ends (structural bound)");
        assertEquals(0, heapFn.liveWindowsForTest(TOKEN));
    }

    @Test
    void restoreAmnesiaNoopsCadenceTimerThenRebuilds() throws Exception {
        SignalJobConfig config = SignalJobConfig.from(env());
        KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> h1 =
                ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                        new HeapPreviewFunction(config),
                        row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                        Types.LONG);
        h1.open();
        long ts = T0 + 100L;
        h1.processElement(TestRawRows.row(TOKEN, ts, "fp-a", "TRADE", 100, 1), ts);
        var snapshot = h1.snapshot(1L, 1_000L);
        h1.close();

        HeapPreviewFunction fn2 = new HeapPreviewFunction(config);
        KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> h2 =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new org.apache.flink.streaming.api.operators.KeyedProcessOperator<>(fn2),
                        row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                        Types.LONG, 1, 1, 0);
        h2.initializeState(snapshot);
        h2.open();
        // Restored cadence timer fires with no heap slot: loud no-op.
        h2.processWatermark(new Watermark(T0 + 1_000L));
        assertEquals(0, mains(h2).size(), "restored cadence timer emits nothing");
        Field noopField =
                HeapPreviewFunction.class.getDeclaredField("restoredTimerNoopCounter");
        noopField.setAccessible(true);
        assertEquals(1, ((Counter) noopField.get(fn2)).getCount(), "no-op counted loudly");
        // A live tick after restore re-arms the cadence; previews resume.
        long ts2 = T0 + 16_000L;
        h2.processElement(TestRawRows.row(TOKEN, ts2, "fp-b", "TRADE", 200, 1), ts2);
        h2.processWatermark(new Watermark(T0 + 17_000L));
        assertTrue(mains(h2).size() >= 1, "previews resume after restore");
        h2.close();
    }

    @Test
    void heapHotPathStaysWithinBudget() throws Exception {
        openBoth();
        int n = 20_000;
        long[] lat = new long[n];
        for (int i = 0; i < n; i++) {
            long ts = T0 + (i % 14_000);
            RowData row = TestRawRows.row(TOKEN, ts, "fp-" + i, "TRADE", 100, 1);
            long s = System.nanoTime();
            heap.processElement(row, ts);
            lat[i] = System.nanoTime() - s;
        }
        java.util.Arrays.sort(lat);
        long p99 = lat[(int) (n * 0.99)];
        System.out.println("CHAIN[preview] p99ns=" + p99);
        assertTrue(p99 < 50_000L, "heap preview p99 " + p99 + "ns exceeds 50us budget");
    }

    @Test
    void intervalBeyondWindowFailsFastAtOpen() throws Exception {
        Map<String, String> bad = env();
        bad.put("PREVIEW_INTERVAL_MS", "16000");
        HeapPreviewFunction fn = new HeapPreviewFunction(SignalJobConfig.from(bad));
        Exception boom = assertThrows(Exception.class, () -> {
            KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> h =
                    ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                            fn,
                            row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                            Types.LONG);
            h.open();
        }, "interval past the window end must fail fast, never orphan accumulators");
        assertTrue(boom.getMessage() != null
                        && boom.getMessage().contains("PREVIEW_INTERVAL_MS"),
                "trip message names the offending keys: " + boom.getMessage());
    }
}
