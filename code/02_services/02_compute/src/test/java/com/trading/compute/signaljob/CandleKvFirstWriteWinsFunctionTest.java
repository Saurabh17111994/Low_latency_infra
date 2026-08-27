package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * KV-level first-write-wins guard (streaming-3000 hardening plan decision 25,
 * T5): the {@code CandleKvFirstWriteWinsFunction} between the candle window
 * operator and the {@code feature_candles_15s} Fluss KV sink must forward the
 * FIRST emission of a {@code (instrument_token, window_start)} pair and drop +
 * count any second emission ({@code compute.candles.duplicate_window}) — the
 * already-written candle row is never overwritten or corrected (R-012), and
 * empty windows (no element) produce no row.
 */
@DisplayName("CandleKvFirstWriteWinsFunction: emit-once per (token, window_start), duplicate counted")
class CandleKvFirstWriteWinsFunctionTest {

    private static final long T0 = 1_750_000_000_000L;

    private CandleKvFirstWriteWinsFunction function;
    private KeyedOneInputStreamOperatorTestHarness<Tuple2<Long, Long>, RowData, RowData> harness;

    @BeforeEach
    void setUp() throws Exception {
        function = new CandleKvFirstWriteWinsFunction();
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                function,
                CandleKvFirstWriteWinsFunction.keySelector(),
                Types.TUPLE(Types.LONG, Types.LONG));
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    /** A candle row for token/window in the CandleTableColumns layout. */
    private static GenericRowData candleRow(long token, long windowStart) {
        CandleAccumulator acc = new CandleAccumulator();
        acc.exchange = "NSE";
        acc.symbol = "TEST";
        acc.openPaise = 100;
        acc.highPaise = 110;
        acc.lowPaise = 99;
        acc.closePaise = 105;
        acc.volume = 25;
        acc.tickCount = 7;
        TimeWindow window = new TimeWindow(windowStart, windowStart + 15_000L);
        return CandleEmitFunction.buildRow(
                token, acc, window, 1_750_000_010_000L, SignalJobConfig.from(env()));
    }

    private void emit(long token, long windowStart) throws Exception {
        harness.processElement(new StreamRecord<>(candleRow(token, windowStart)));
    }

    private long forwardedCount() {
        return harness.getOutput().stream()
                .filter(o -> o instanceof StreamRecord)
                .count();
    }

    @Test
    @DisplayName("first emission forwards; a second emission of the same (token, window_start) is dropped and counted")
    void firstWinsSecondDroppedAndCounted() throws Exception {
        emit(2885L, T0);
        GenericRowData second = candleRow(2885L, T0);
        harness.processElement(new StreamRecord<>(second));

        assertEquals(1L, forwardedCount(),
                "exactly one row may reach the KV sink for a (token, window_start) pair");
        assertEquals(1L, function.duplicateWindowCountForTest(),
                "the second window emission must increment compute.candles.duplicate_window");

        // A third emission keeps counting and never re-writes.
        harness.processElement(new StreamRecord<>(candleRow(2885L, T0)));
        assertEquals(1L, forwardedCount(), "still exactly one forwarded row");
        assertEquals(2L, function.duplicateWindowCountForTest(),
                "every second+ emission is counted once");
    }

    @Test
    @DisplayName("the forwarded row is the first emission itself, unchanged")
    void forwardedRowIsTheFirstEmission() throws Exception {
        GenericRowData first = candleRow(2885L, T0);
        harness.processElement(new StreamRecord<>(first));
        harness.processElement(new StreamRecord<>(candleRow(2885L, T0)));

        StreamRecord<?> out = (StreamRecord<?>) harness.getOutput().stream()
                .filter(o -> o instanceof StreamRecord)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no forwarded row"));
        // The harness may hand the operator a defensive copy, so identity is
        // not guaranteed — the contract is content: the first emission's
        // exact fields reach the KV sink, never a rewritten/corrected row.
        RowData row = (RowData) out.getValue();
        assertEquals(first, row,
                "the forwarded row must carry the FIRST emission's content — never a rewritten/corrected row");
        assertEquals(2885L, row.getLong(CandleTableColumns.INSTRUMENT_TOKEN));
        assertEquals(T0, row.getLong(CandleTableColumns.WINDOW_START));
    }

    @Test
    @DisplayName("a different window or a different token is a fresh key — forwarded, not counted as duplicate")
    void distinctWindowOrTokenForwards() throws Exception {
        emit(2885L, T0);
        emit(2885L, T0 + 15_000L); // same token, next window
        emit(7L, T0);               // same window, different token

        assertEquals(3L, forwardedCount(),
                "each distinct (token, window_start) pair is a separate first write");
        assertEquals(0L, function.duplicateWindowCountForTest(),
                "no cross-key collision — duplicate_window stays at zero");
    }

    @Test
    @DisplayName("empty input emits nothing (empty windows produce no row) and counts nothing")
    void emptyInputEmitsNothing() {
        assertEquals(0L, forwardedCount(), "no element -> no row reaches the KV sink");
        assertEquals(0L, function.duplicateWindowCountForTest());
    }

    @Test
    @DisplayName("state contract: one Boolean marker per emitted key, no timers, no payload")
    void stateIsOneBooleanPerKeyNoTimers() throws Exception {
        emit(2885L, T0);
        emit(7L, T0 + 15_000L);

        // SIG-UNIT-008 style: two active keys -> exactly two state entries,
        // each a bare Boolean marker — never a candle payload or collection.
        assertEquals(2, harness.numKeyedStateEntries(),
                "one Boolean written-marker per emitted key, nothing else");
        assertEquals(0, harness.numEventTimeTimers(),
                "no hand-rolled timers — native StateTtlConfig expires markers");
    }

    @Test
    @DisplayName("after the marker TTL elapses a re-arrival is a fresh first write (deterministic rewrite is benign)")
    void expiredMarkerReAdmitsAsFirstWrite() throws Exception {
        emit(2885L, T0);
        harness.processElement(new StreamRecord<>(candleRow(2885L, T0)));
        assertEquals(1L, forwardedCount());
        assertEquals(1L, function.duplicateWindowCountForTest());

        // Native processing-time TTL: advance past the 24 h marker TTL; the
        // expired marker reads as absent and the re-arrival is re-admitted
        // (replay is deterministic — the rewritten content is identical).
        harness.setStateTtlProcessingTime(
                CandleKvFirstWriteWinsFunction.WRITTEN_MARK_TTL.toMillis() + 1L);

        emit(2885L, T0);
        assertEquals(2L, forwardedCount(), "post-TTL re-arrival forwards again");
        assertEquals(1L, function.duplicateWindowCountForTest(),
                "the post-TTL re-arrival is NOT counted as a duplicate");
        assertEquals(1, harness.numKeyedStateEntries(),
                "the re-admitted marker replaced the expired one — state never grows");
    }

    /** Same 5-key baseline as SignalJobConfigTest; tuning keys take defaults. */
    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_TTL_MS", "60000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        return env;
    }
}
