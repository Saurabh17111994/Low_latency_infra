package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
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
 * Differential equivalence: {@link HeapCandleEmitFunction} (heap windows,
 * chain-heap redesign OP4) vs the real tumbling-window operator (oracle).
 * The SAME tick/watermark script runs through both harnesses; main rows
 * (minus processing-time {@code output_ts}), late-drop sides and quarantine
 * sides must match exactly.
 */
@DisplayName("heap candle windows: tick-for-tick equivalence with the window operator (OP4)")
class HeapCandleEmitFunctionTest {

    /** 15000-aligned epoch anchor — window [T0, T0+15000) holds every offset used. */
    private static final long T0 = 1_749_999_990_000L;
    private static final long WINDOW_MS = 15_000L;
    private static final long TOKEN = 2885L;

    private KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> oracle;
    private KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> heap;
    private HeapCandleEmitFunction heapFn;

    @AfterEach
    void tearDown() throws Exception {
        if (oracle != null) {
            oracle.close();
            oracle = null;
        }
        if (heap != null) {
            heap.close();
            heap = null;
        }
    }

    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_WINDOW_ENTRIES", "2000");
        env.put("CANDLE_WINDOW_MS", "15000");
        // Pinned to the production bound — the heap eviction math depends on it.
        env.put("ALLOWED_LATENESS_MS", "5000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        return env;
    }

    private void openBoth() throws Exception {
        oracle = CandleWindowTestHarness.create(WINDOW_MS, 5_000L);
        heapFn = new HeapCandleEmitFunction(SignalJobConfig.from(env()));
        heap = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                heapFn,
                row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        heap.open();
    }

    // -- script DSL: the same ops feed both harnesses in the same order --

    private interface Op {
    }

    private record Tick(long token, long offsetMs, long price, long qty, String fp) implements Op {
    }

    private record Mark(long watermark) implements Op {
    }

    private void runBoth(List<Op> script) throws Exception {
        for (Op op : script) {
            if (op instanceof Tick t) {
                long ts = T0 + t.offsetMs();
                RowData row = TestRawRows.row(t.token(), ts, t.fp(), "TRADE", t.price(), t.qty());
                oracle.processElement(new StreamRecord<>(row, ts));
                heap.processElement(row, ts);
            } else if (op instanceof Mark m) {
                Watermark wm = new Watermark(m.watermark());
                oracle.processWatermark(wm);
                heap.processWatermark(wm);
            }
        }
    }

    // -- output readers --

    private static String candleKey(RowData r) {
        // Every column except OUTPUT_TS (processing-time emit instant differs
        // between the two harness runs by construction).
        return r.getLong(CandleTableColumns.INSTRUMENT_TOKEN) + "|"
                + r.getString(CandleTableColumns.EXCHANGE) + "|"
                + r.getString(CandleTableColumns.SYMBOL) + "|"
                + r.getLong(CandleTableColumns.WINDOW_START) + "|"
                + r.getLong(CandleTableColumns.WINDOW_END) + "|"
                + r.getLong(CandleTableColumns.OPEN_PAISE) + "|"
                + r.getLong(CandleTableColumns.HIGH_PAISE) + "|"
                + r.getLong(CandleTableColumns.LOW_PAISE) + "|"
                + r.getLong(CandleTableColumns.CLOSE_PAISE) + "|"
                + r.getLong(CandleTableColumns.VOLUME) + "|"
                + r.getInt(CandleTableColumns.TICK_COUNT) + "|"
                + r.getString(CandleTableColumns.ALGORITHM_VERSION) + "|"
                + r.getString(CandleTableColumns.CONFIGURATION_VERSION) + "|"
                + r.getString(CandleTableColumns.SCHEMA_VERSION);
    }

    private static List<String> mains(KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> h) {
        List<String> out = new ArrayList<>();
        for (Object o : h.getOutput()) {
            if (o instanceof StreamRecord<?> sr && sr.getValue() instanceof RowData r) {
                out.add(candleKey(r));
            }
        }
        return out;
    }

    private static List<String> lateKeys(
            KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> h) {
        var side = h.getSideOutput(CandleLateDrop.OUTPUT);
        List<String> out = new ArrayList<>();
        if (side != null) {
            for (StreamRecord<RowData> sr : side) {
                RowData r = sr.getValue();
                out.add(r.getLong(RawTableColumns.INSTRUMENT_TOKEN) + "@"
                        + r.getLong(RawTableColumns.EVENT_TIME));
            }
        }
        return out;
    }

    private static List<String> quarantineKeys(
            KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> h) {
        var side = h.getSideOutput(CandleQuarantine.OUTPUT);
        List<String> out = new ArrayList<>();
        if (side != null) {
            for (StreamRecord<RowData> sr : side) {
                RowData r = sr.getValue();
                out.add(r.getLong(CandleQuarantineColumns.INSTRUMENT_TOKEN) + ":"
                        + r.getString(CandleQuarantineColumns.REASON));
            }
        }
        return out;
    }

    private void assertParity(String what) {
        assertEquals(mains(oracle), mains(heap), what + " — main rows differ");
        assertEquals(lateKeys(oracle), lateKeys(heap), what + " — late-drop sides differ");
        assertEquals(quarantineKeys(oracle), quarantineKeys(heap),
                what + " — quarantine sides differ");
    }

    private long heapCounter(String field) throws Exception {
        Field f = HeapCandleEmitFunction.class.getDeclaredField(field);
        f.setAccessible(true);
        return ((Counter) f.get(heapFn)).getCount();
    }

    // -- scenarios --

    @Test
    void cleanWindowEmitsIdenticalRow() throws Exception {
        openBoth();
        runBoth(List.of(
                new Tick(TOKEN, 1_000L, 100, 10, "fp-a"),
                new Tick(TOKEN, 5_000L, 110, 20, "fp-b"),
                new Mark(T0 + WINDOW_MS)));
        assertParity("clean window");
        assertEquals(1, mains(heap).size());
    }

    @Test
    void rolloverTwoWindowsEmitIdenticalRows() throws Exception {
        openBoth();
        runBoth(List.of(
                new Tick(TOKEN, 1_000L, 100, 10, "fp-a"),
                new Tick(TOKEN, 16_000L, 200, 5, "fp-b"),
                new Tick(TOKEN, 20_000L, 210, 5, "fp-c"),
                new Mark(T0 + WINDOW_MS)));
        assertParity("first window after rollover");
        assertEquals(1, mains(heap).size());
        assertEquals(String.valueOf(T0), mains(heap).get(0).split("\\|")[3],
                "closed row is window W");
        runBoth(List.of(new Mark(T0 + 2 * WINDOW_MS)));
        assertParity("second window");
        assertEquals(2, mains(heap).size());
    }

    @Test
    void lateWithinLatenessIsCountedNoopBothSides() throws Exception {
        openBoth();
        runBoth(List.of(
                new Tick(TOKEN, 1_000L, 100, 10, "fp-a"),
                new Mark(T0 + WINDOW_MS),
                // 2 s late, inside the 5 s bound: folds + re-fires → no-op.
                new Tick(TOKEN, 12_000L, 120, 5, "fp-late")));
        assertParity("late within lateness");
        assertEquals(1, mains(heap).size(), "no correction row (R-012)");
        assertEquals(1, heapCounter("lateUpdateCounter"), "heap counts the late re-fire");
    }

    @Test
    void lateBeyondLatenessDropsToSameSideOutput() throws Exception {
        openBoth();
        runBoth(List.of(
                new Tick(TOKEN, 1_000L, 100, 10, "fp-a"),
                new Mark(T0 + WINDOW_MS),
                // Watermark past end + lateness, then a straggler for W.
                new Mark(T0 + WINDOW_MS + 5_001L),
                new Tick(TOKEN, 2_000L, 100, 10, "fp-straggler")));
        assertParity("late beyond lateness");
        assertEquals(1, mains(heap).size(), "straggler never reopens the window");
        assertEquals(List.of(TOKEN + "@" + (T0 + 2_000L)), lateKeys(heap));
    }

    @Test
    void quarantineParityAndNoDoubleQuarantine() throws Exception {
        openBoth();
        runBoth(List.of(
                new Tick(TOKEN, 1_000L, 100, Long.MAX_VALUE, "fp-a"),
                new Tick(TOKEN, 5_000L, 110, Long.MAX_VALUE, "fp-b"),
                new Mark(T0 + WINDOW_MS)));
        assertParity("quarantined window");
        assertEquals(0, mains(heap).size(), "invalid candle never emitted");
        assertEquals(List.of(TOKEN + ":INVALID_CANDLE_VOLUME"), quarantineKeys(heap));
        // Late re-trigger of the quarantined window: still exactly one row.
        runBoth(List.of(new Tick(TOKEN, 12_000L, 120, Long.MAX_VALUE, "fp-late")));
        assertParity("late re-trigger of quarantined window");
        assertEquals(1, quarantineKeys(heap).size(), "no double quarantine");
    }

    @Test
    void crossBoundaryReorderFoldsIdentically() throws Exception {
        openBoth();
        runBoth(List.of(
                // Next-window tick FIRST, then a tick for the still-open window.
                new Tick(TOKEN, 16_000L, 200, 5, "fp-next"),
                new Tick(TOKEN, 14_000L, 150, 7, "fp-cur"),
                new Mark(T0 + WINDOW_MS)));
        assertParity("cross-boundary reorder");
        assertEquals(1, mains(heap).size());
        String row = mains(heap).get(0);
        assertTrue(row.contains("|150|150|150|150|7|1|"),
                "closed W row holds exactly the W tick (O=H=L=C=150, vol 7, n 1): " + row);
        runBoth(List.of(new Mark(T0 + 2 * WINDOW_MS)));
        assertParity("following window after reorder");
        assertEquals(2, mains(heap).size());
    }

    @Test
    void gapJumpSkipsEmptyWindowsBothSides() throws Exception {
        openBoth();
        runBoth(List.of(
                new Tick(TOKEN, 1_000L, 100, 10, "fp-a"),
                // Quiet for a full window, then a tick two windows ahead.
                new Tick(TOKEN, 31_000L, 300, 3, "fp-gap"),
                new Mark(T0 + WINDOW_MS)));
        assertParity("window before a gap");
        assertEquals(1, mains(heap).size());
        runBoth(List.of(new Mark(T0 + 3 * WINDOW_MS)));
        assertParity("window after a gap — empty middle never emits");
        assertEquals(2, mains(heap).size());
    }

    @Test
    void heapRequestsNoManagedStateOnlyTimers() throws Exception {
        openBoth();
        runBoth(List.of(
                new Tick(TOKEN, 1_000L, 100, 10, "fp-a"),
                new Tick(TOKEN, 16_000L, 200, 5, "fp-b")));
        assertEquals(0, heap.numKeyedStateEntries(),
                "heap windows request no managed state");
        assertTrue(heap.numEventTimeTimers() >= 1, "end-timers are the only managed piece");
        assertEquals(1, heapFn.slotCountForTest());
        assertEquals(1, heapFn.pendingSizeForTest(TOKEN), "rolled window parks as pending");
    }

    @Test
    void restoreAmnesiaNoopsTimerThenRebuilds() throws Exception {
        SignalJobConfig config = SignalJobConfig.from(env());
        KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> h1 =
                ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                        new HeapCandleEmitFunction(config),
                        row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                        Types.LONG);
        h1.open();
        long ts = T0 + 1_000L;
        h1.processElement(TestRawRows.row(TOKEN, ts, "fp-a", "TRADE", 100, 10), ts);
        var snapshot = h1.snapshot(1L, 1_000L);
        h1.close();

        HeapCandleEmitFunction fn2 = new HeapCandleEmitFunction(config);
        KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> h2 =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new org.apache.flink.streaming.api.operators.KeyedProcessOperator<>(fn2),
                        row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                        Types.LONG, 1, 1, 0);
        h2.initializeState(snapshot);
        h2.open();
        // Restored end-timer fires with no heap slot: loud no-op, no row.
        h2.processWatermark(new Watermark(T0 + WINDOW_MS));
        assertEquals(0, CandleWindowTestHarness.candleRowCount(h2),
                "restored timer with amnesiac heap emits nothing");
        Field noopField = HeapCandleEmitFunction.class.getDeclaredField("restoredTimerNoopCounter");
        noopField.setAccessible(true);
        assertEquals(1, ((Counter) noopField.get(fn2)).getCount(),
                "the no-op is counted loudly, never silent");
        // A straggler for the crash window is counted, never a partial emit.
        long tsCrash = T0 + 2_000L;
        h2.processElement(TestRawRows.row(TOKEN, tsCrash, "fp-b", "TRADE", 110, 5), tsCrash);
        assertEquals(0, CandleWindowTestHarness.candleRowCount(h2));
        // The NEXT window rebuilds from live ticks and closes exactly once.
        long tsNext = T0 + 16_000L;
        h2.processElement(TestRawRows.row(TOKEN, tsNext, "fp-c", "TRADE", 200, 3), tsNext);
        h2.processWatermark(new Watermark(T0 + 2 * WINDOW_MS));
        assertEquals(1, CandleWindowTestHarness.candleRowCount(h2),
                "rebuilt window closes exactly once");
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
        System.out.println("CHAIN[candle] p99ns=" + p99);
        assertTrue(p99 < 50_000L, "heap candle p99 " + p99 + "ns exceeds 50us budget");
    }
}
