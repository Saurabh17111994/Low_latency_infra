package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.common.model.FormingBar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@link FormingBarWriterFunction} driven through the Flink 2.2.1 operator
 * harness (no cluster) — the REAL production function, real config. Covers
 * the forming-bar KV persistence phase (2026-08-16) semantics: current-state
 * coalescing (the latest forming bar per instrument REPLACES any previous
 * state — never per-tick history), one row per instrument per cadence flush,
 * quiet instruments write nothing after their flush, window rollover replaces
 * the durable state, and the emitted row is the exact 11-column v1
 * {@code forming_bar} layout ({@link FormingBarTableColumns}).
 */
class FormingBarWriterFunctionTest {

    /** Epoch-aligned base: 1_710_000_000_000 / 15000 = 114_000_000 exactly. */
    private static final long T0 = 1_710_000_000_000L;

    private KeyedOneInputStreamOperatorTestHarness<Long, FormingBar, RowData> harness;
    private FormingBarWriterFunction function;

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    private static Map<String, String> envFor(long batchMs) {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_WINDOW_ENTRIES", "2000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        env.put("FORMING_BAR_WRITE_BATCH_MS", Long.toString(batchMs));
        return env;
    }

    private void openHarness(long batchMs) throws Exception {
        function = new FormingBarWriterFunction(SignalJobConfig.from(envFor(batchMs)));
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                function,
                bar -> bar.instrumentToken(),
                Types.LONG);
        harness.open();
    }

    private static FormingBar bar(long token, long windowStart, long close, long volume,
            long tickCount) {
        return new FormingBar(token, windowStart, windowStart + 15_000L,
                close - 1, close, close - 2, close, volume, tickCount,
                windowStart + 1_000L, "fp-" + token + "-" + windowStart,
                "NSE", "TEST");
    }

    private static List<RowData> emittedRows(
            KeyedOneInputStreamOperatorTestHarness<Long, FormingBar, RowData> h) {
        return h.getOutput().stream()
                .filter(o -> o instanceof StreamRecord)
                .map(o -> (RowData) ((StreamRecord<?>) o).getValue())
                .toList();
    }

    @Test
    void nothingEmittedBeforeTimerFires() throws Exception {
        openHarness(250);
        harness.processElement(bar(7L, T0, 100, 5, 1), T0 + 1_000L);
        assertEquals(0, emittedRows(harness).size(),
                "no durable write before the write-cadence timer fires");
    }

    @Test
    void coalescesLatestPerInstrumentOnTimerFlush() throws Exception {
        openHarness(250);
        harness.setProcessingTime(1_000L);
        // Three updates for instrument 7 (the LATEST wins), two for 8.
        harness.processElement(bar(7L, T0, 100, 5, 1), T0 + 1_000L);
        harness.processElement(bar(7L, T0, 102, 7, 2), T0 + 4_000L);
        harness.processElement(bar(7L, T0, 99, 10, 3), T0 + 7_000L);
        harness.processElement(bar(8L, T0, 200, 1, 1), T0 + 2_000L);
        harness.processElement(bar(8L, T0, 205, 2, 2), T0 + 5_000L);

        // Advance processing time past the cadence → one row per instrument.
        harness.setProcessingTime(1_250L);

        List<RowData> rows = emittedRows(harness);
        assertEquals(2, rows.size(), "one durable row per instrument per cadence — never per tick");
        RowData r7 = rows.get(0);
        RowData r8 = rows.get(1);
        assertEquals(7L, r7.getLong(FormingBarTableColumns.INSTRUMENT_TOKEN));
        assertEquals(99, r7.getLong(FormingBarTableColumns.CLOSE_PAISE),
                "the LAST tick of the cadence wins (current-state, last-write-wins)");
        assertEquals(10, r7.getLong(FormingBarTableColumns.VOLUME));
        assertEquals(8L, r8.getLong(FormingBarTableColumns.INSTRUMENT_TOKEN));
        assertEquals(205, r8.getLong(FormingBarTableColumns.CLOSE_PAISE));
    }

    @Test
    void quietInstrumentWritesNothingAfterItsFlush() throws Exception {
        openHarness(250);
        harness.setProcessingTime(1_000L);
        harness.processElement(bar(7L, T0, 100, 5, 1), T0 + 1_000L);
        harness.setProcessingTime(1_250L); // flush
        assertEquals(1, emittedRows(harness).size());

        // No new ticks → advancing time again must NOT re-emit (buffer cleared;
        // the durable authority is Fluss, not this operator's state).
        harness.setProcessingTime(1_500L);
        assertEquals(1, emittedRows(harness).size(),
                "a quiet instrument holds no buffered row after its flush");
    }

    @Test
    void windowRolloverReplacesDurableState() throws Exception {
        openHarness(250);
        harness.setProcessingTime(1_000L);
        harness.processElement(bar(7L, T0, 100, 5, 1), T0 + 1_000L);
        harness.setProcessingTime(1_250L); // flush window W

        // New window: the same instrument's row now carries the NEW window.
        long t1 = T0 + 15_000L;
        harness.processElement(bar(7L, t1, 150, 1, 1), t1 + 1_000L);
        harness.setProcessingTime(1_500L); // flush window W+1

        List<RowData> rows = emittedRows(harness);
        assertEquals(2, rows.size());
        assertEquals(T0, rows.get(0).getLong(FormingBarTableColumns.WINDOW_START));
        assertEquals(t1, rows.get(1).getLong(FormingBarTableColumns.WINDOW_START),
                "the durable row transitions to the new window (current-state, same key)");
        assertEquals(150, rows.get(1).getLong(FormingBarTableColumns.CLOSE_PAISE));
        assertEquals(149, rows.get(1).getLong(FormingBarTableColumns.OPEN_PAISE),
                "the fixture helper builds open = close - 1");
    }

    @Test
    void emittedRowIsExactV1Layout() throws Exception {
        openHarness(250);
        harness.setProcessingTime(1_000L);
        harness.processElement(bar(7L, T0, 100, 5, 3), T0 + 1_000L);
        harness.setProcessingTime(1_250L);

        RowData row = emittedRows(harness).get(0);
        assertEquals(FormingBarTableColumns.FIELD_COUNT, row.getArity(),
                "11-column v1 forming_bar layout");
        assertEquals(7L, row.getLong(FormingBarTableColumns.INSTRUMENT_TOKEN));
        assertEquals(T0, row.getLong(FormingBarTableColumns.WINDOW_START));
        assertEquals(99, row.getLong(FormingBarTableColumns.OPEN_PAISE));
        assertEquals(100, row.getLong(FormingBarTableColumns.HIGH_PAISE));
        assertEquals(98, row.getLong(FormingBarTableColumns.LOW_PAISE));
        assertEquals(100, row.getLong(FormingBarTableColumns.CLOSE_PAISE));
        assertEquals(5, row.getLong(FormingBarTableColumns.VOLUME));
        assertEquals(3, row.getInt(FormingBarTableColumns.TICK_COUNT));
        assertEquals(T0 + 1_000L, row.getLong(FormingBarTableColumns.LAST_EVENT_TIME));
        assertEquals("fp-7-" + T0, row.getString(FormingBarTableColumns.LAST_EVENT_FINGERPRINT).toString());
        assertEquals("1", row.getString(FormingBarTableColumns.SCHEMA_VERSION).toString());
        // windowEnd / exchange / symbol are in-process only — never persisted
        // (the 11-column v1 layout has no such columns).
    }

    @Test
    void checkpointRestoreKeepsTimerButDropsBuffer() throws Exception {
        SignalJobConfig config = SignalJobConfig.from(envFor(250));
        KeyedOneInputStreamOperatorTestHarness<Long, FormingBar, RowData> h1 =
                ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                        new FormingBarWriterFunction(config),
                        bar -> bar.instrumentToken(),
                        Types.LONG);
        h1.open();
        h1.setProcessingTime(1_000L);
        h1.processElement(bar(7L, T0, 100, 5, 1), T0 + 1_000L);
        // The event-time flush timer (managed) is in the snapshot; the
        // buffered bar (heap) is intentionally NOT.
        OperatorSubtaskState snapshot = h1.snapshot(1L, 1_000L);
        h1.close();

        // Simulated Flink restart: a FRESH operator instance restored from
        // the checkpoint. Heap amnesia by design: the restored timer fires
        // but flushes NOTHING (buffer lost); the next live tick re-buffers
        // and the KV row converges on the following flush. The restore
        // harness must be constructed DIRECTLY — ProcessFunctionTestHarnesses
        // already calls harness.open(), and initializeState() after that
        // fails (verified against flink-runtime 2.2.1 bytecode).
        KeyedOneInputStreamOperatorTestHarness<Long, FormingBar, RowData> h2 =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(new FormingBarWriterFunction(config)),
                        bar -> bar.instrumentToken(),
                        Types.LONG,
                        1, 1, 0);
        h2.initializeState(snapshot);
        h2.open();
        assertEquals(0, emittedRows(h2).size(),
                "restored operator emits nothing before the restored timer fires");
        assertEquals(0, h2.numKeyedStateEntries(),
                "heap snapshot takes no managed state — timer only");
        h2.setProcessingTime(1_250L);
        assertEquals(0, emittedRows(h2).size(),
                "restored timer fires but the pre-crash buffer is gone — no stale write");
        // Next live tick re-buffers; convergence resumes on the next flush.
        h2.processElement(bar(7L, T0, 102, 7, 2), T0 + 4_000L);
        h2.setProcessingTime(4_250L);
        List<RowData> rows = emittedRows(h2);
        assertEquals(1, rows.size(), "post-restore tick flushes on the next cadence");
        assertEquals(7L, rows.get(0).getLong(FormingBarTableColumns.INSTRUMENT_TOKEN));
        assertEquals(102, rows.get(0).getLong(FormingBarTableColumns.CLOSE_PAISE));
        h2.close();
    }

    @Test
    void sameWindowRepeatsReplaceNotAppend() throws Exception {
        openHarness(250);
        harness.setProcessingTime(1_000L);
        // Two cadences for the SAME window: row #1 then row #2 (both are the
        // current state of window W at their flush time — the table converges
        // on the same PK, last-write-wins).
        harness.processElement(bar(7L, T0, 100, 5, 1), T0 + 1_000L);
        harness.setProcessingTime(1_250L);
        harness.processElement(bar(7L, T0, 110, 9, 2), T0 + 8_000L);
        harness.setProcessingTime(1_500L);

        List<RowData> rows = emittedRows(harness);
        assertEquals(2, rows.size());
        assertEquals(T0, rows.get(0).getLong(FormingBarTableColumns.WINDOW_START));
        assertEquals(T0, rows.get(1).getLong(FormingBarTableColumns.WINDOW_START));
        assertEquals(110, rows.get(1).getLong(FormingBarTableColumns.CLOSE_PAISE));
    }

    @Test
    void heapRedesignBuffersWithoutManagedState() throws Exception {
        openHarness(250);
        harness.setProcessingTime(1_000L);
        harness.processElement(bar(7L, T0, 100, 5, 1), T0 + 1_000L);
        harness.processElement(bar(8L, T0, 200, 1, 1), T0 + 2_000L);
        assertEquals(0, harness.numKeyedStateEntries(),
                "heap redesign buffers snapshots without managed state");
        assertEquals(2, function.bufferedCountForTest(), "two live snapshots, one per key");
        harness.setProcessingTime(1_250L);
        assertEquals(2, emittedRows(harness).size(), "both instruments flush");
        assertEquals(0, function.bufferedCountForTest(), "flush clears the buffer");
    }
}
