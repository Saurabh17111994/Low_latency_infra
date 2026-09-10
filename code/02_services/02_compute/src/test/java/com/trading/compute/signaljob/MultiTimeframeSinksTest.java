package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Phase 3 Track A unit tests — first-write-wins filter for {@code candle_closed}
 * (composite key {@code instrument_token, tf, window_start}).
 *
 * <p>Mirrors {@link CandleKvFirstWriteWinsFunctionTest} but generalised to the
 * multi-TF composite PK (DDL 33 v1). Feeds duplicate rows for the same
 * {@code (instrument,tf,window_start)} via the harness and asserts first
 * passes, second dropped and counted via {@code compute.candles.multitf.duplicate_window}.
 *
 * <p>Integration test using a live Fluss sink against a scratch
 * {@code candle_live}/{@code candle_closed} table (scratch-table pattern of
 * {@link FormingBarRehydrationIntegrationTest} / {@code ScratchTables}) is
 * <b>not run</b> here: that pattern needs an externally provisioned Fluss
 * cluster (or docker/compose) and is gated by
 * {@code COMPUTE_INT_TEST_FORMING_BAR_REHYDRATE=true} / {@code FLUSS_BOOTSTRAP}
 * env vars. In this repo those env vars are unset and no embedded Fluss
 * is available, so the test is skipped by design — rely on these unit tests
 * plus the existing {@link TableContractValidatorTest} / {@code CandleClosedColumnsAgreementTest}
 *contract tests which already pin DDL parity.
 */
@DisplayName("MultiTimeframeSinks: closed first-write-wins composite key (instrument,tf,window_start)")
class MultiTimeframeSinksTest {

    private static final long T0 = 1_750_000_000_000L;

    private MultiTimeframeSinks.MultiTimeframeClosedFirstWriteWinsFunction function;
    private KeyedOneInputStreamOperatorTestHarness<Tuple3<Long, String, Long>, RowData, RowData> harness;

    @BeforeEach
    void setUp() throws Exception {
        function = new MultiTimeframeSinks.MultiTimeframeClosedFirstWriteWinsFunction();
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                function,
                MultiTimeframeSinks.MultiTimeframeClosedFirstWriteWinsFunction.keySelector(),
                Types.TUPLE(Types.LONG, Types.STRING, Types.LONG));
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    private static GenericRowData closedRow(long token, Timeframe tf, long windowStart) {
        GenericRowData row = new GenericRowData(CandleClosedColumns.FIELD_COUNT);
        row.setField(CandleClosedColumns.INSTRUMENT_TOKEN, token);
        row.setField(CandleClosedColumns.EXCHANGE, StringData.fromString("NSE"));
        row.setField(CandleClosedColumns.SYMBOL, StringData.fromString("TEST"));
        row.setField(CandleClosedColumns.TF, StringData.fromString(tf.code()));
        row.setField(CandleClosedColumns.WINDOW_START, windowStart);
        row.setField(CandleClosedColumns.WINDOW_END, windowStart + tf.windowMs());
        row.setField(CandleClosedColumns.OPEN_PAISE, 100L);
        row.setField(CandleClosedColumns.HIGH_PAISE, 110L);
        row.setField(CandleClosedColumns.LOW_PAISE, 99L);
        row.setField(CandleClosedColumns.CLOSE_PAISE, 105L);
        row.setField(CandleClosedColumns.VOLUME, 25L);
        row.setField(CandleClosedColumns.TICK_COUNT, 7);
        row.setField(CandleClosedColumns.LAST_EVENT_TIME, windowStart + 1_000L);
        row.setField(CandleClosedColumns.LAST_EVENT_FINGERPRINT,
                StringData.fromString("fp-" + token + "-" + tf.code() + "-" + windowStart));
        row.setField(CandleClosedColumns.SCHEMA_VERSION,
                StringData.fromString(CandleClosedColumns.SCHEMA_VERSION_V1));
        return row;
    }

    private static GenericRowData closedRow(long token, String tfCode, long windowStart) {
        // Convenience for raw string tftype (used in tf-distinctness check where enum name matches code)
        return closedRow(token, Timeframe.valueOf(tfCode), windowStart);
    }

    private void emit(long token, Timeframe tf, long windowStart) throws Exception {
        harness.processElement(new StreamRecord<>(closedRow(token, tf, windowStart)));
    }

    @Test
    @DisplayName("null PK field fails fast with field name (P2-230)")
    void nullPkFailsFast() throws Exception {
        var selector = MultiTimeframeSinks.MultiTimeframeClosedFirstWriteWinsFunction.keySelector();
        GenericRowData nullToken = closedRow(2885L, Timeframe.ONE_M, T0);
        nullToken.setField(CandleClosedColumns.INSTRUMENT_TOKEN, null);
        assertThrows(IllegalArgumentException.class, () -> selector.getKey(nullToken));
        GenericRowData nullTf = closedRow(2885L, Timeframe.ONE_M, T0);
        nullTf.setField(CandleClosedColumns.TF, null);
        assertThrows(IllegalArgumentException.class, () -> selector.getKey(nullTf));
        GenericRowData nullWs = closedRow(2885L, Timeframe.ONE_M, T0);
        nullWs.setField(CandleClosedColumns.WINDOW_START, null);
        assertThrows(IllegalArgumentException.class, () -> selector.getKey(nullWs));
    }

    @Test
    @DisplayName("timeframe code contract: code==name, unique, fromCode round-trips (P2-178)")
    void timeframeCodeContract() {
        Timeframe[] tfs = Timeframe.values();
        long prevMs = -1L;
        for (Timeframe tf : tfs) {
            assertEquals(tf.name(), tf.code());
            assertEquals(tf, Timeframe.fromCode(tf.code()));
            assertEquals(true, tf.windowMs() > prevMs, "values() must stay ascending windowMs");
            prevMs = tf.windowMs();
        }
        assertThrows(IllegalArgumentException.class, () -> Timeframe.fromCode("NOPE"));
    }

    private long forwardedCount() {
        return harness.getOutput().stream()
                .filter(o -> o instanceof StreamRecord)
                .count();
    }

    @Test
    @DisplayName("first emission forwards; second emission of same (instrument,tf,window_start) is dropped and counted")
    void firstWinsSecondDroppedAndCounted() throws Exception {
        emit(2885L, Timeframe.FIFTEEN_S, T0);
        harness.processElement(new StreamRecord<>(closedRow(2885L, Timeframe.FIFTEEN_S, T0)));

        assertEquals(1L, forwardedCount(),
                "exactly one row may reach the KV sink for a (instrument,tf,window_start) composite key");
        assertEquals(1L, function.duplicateWindowCountForTest(),
                "second emission must increment compute.candles.multitf.duplicate_window");

        harness.processElement(new StreamRecord<>(closedRow(2885L, Timeframe.FIFTEEN_S, T0)));
        assertEquals(1L, forwardedCount(), "still exactly one forwarded row");
        assertEquals(2L, function.duplicateWindowCountForTest(),
                "every second+ emission is counted once");
    }

    @Test
    @DisplayName("forwarded row is the first emission itself, unchanged")
    void forwardedRowIsFirstEmission() throws Exception {
        GenericRowData first = closedRow(2885L, Timeframe.ONE_M, T0);
        harness.processElement(new StreamRecord<>(first));
        harness.processElement(new StreamRecord<>(closedRow(2885L, Timeframe.ONE_M, T0)));

        StreamRecord<?> out = (StreamRecord<?>) harness.getOutput().stream()
                .filter(o -> o instanceof StreamRecord)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no forwarded row"));
        RowData row = (RowData) out.getValue();
        assertEquals(first, row,
                "forwarded row must carry FIRST emission's content — never rewritten");
        assertEquals(2885L, row.getLong(CandleClosedColumns.INSTRUMENT_TOKEN));
        assertEquals("ONE_M", row.getString(CandleClosedColumns.TF).toString());
        assertEquals(T0, row.getLong(CandleClosedColumns.WINDOW_START));
    }

    @Test
    @DisplayName("same instrument+window_start but different tf is a fresh composite key — forwarded, not counted")
    void distinctTfIsFreshKey() throws Exception {
        emit(2885L, Timeframe.FIFTEEN_S, T0);
        emit(2885L, Timeframe.ONE_M, T0); // same token and window_start, different tf
        emit(2885L, Timeframe.FIVE_M, T0);

        assertEquals(3L, forwardedCount(),
                "each distinct tf for same (token,window_start) is a separate first write");
        assertEquals(0L, function.duplicateWindowCountForTest(),
                "no cross-tf collision — duplicate counter stays zero");
    }

    @Test
    @DisplayName("different window or different token is a fresh composite key — forwarded, not counted")
    void distinctWindowOrTokenForwards() throws Exception {
        emit(2885L, Timeframe.FIFTEEN_S, T0);
        emit(2885L, Timeframe.FIFTEEN_S, T0 + 15_000L); // same token+tf, next window
        emit(7L, Timeframe.FIFTEEN_S, T0); // same tf+window, different token

        assertEquals(3L, forwardedCount(),
                "each distinct (instrument,tf,window_start) is a separate first write");
        assertEquals(0L, function.duplicateWindowCountForTest());
    }

    @Test
    @DisplayName("empty input emits nothing and counts nothing")
    void emptyInputEmitsNothing() {
        assertEquals(0L, forwardedCount(), "no element -> no row");
        assertEquals(0L, function.duplicateWindowCountForTest());
    }

    @Test
    @DisplayName("state contract: one Boolean marker per distinct composite key, no timers, no payload")
    void stateIsOneBooleanPerKeyNoTimers() throws Exception {
        emit(2885L, Timeframe.FIFTEEN_S, T0);
        emit(7L, Timeframe.ONE_M, T0 + 60_000L);

        assertEquals(2, harness.numKeyedStateEntries(),
                "one Boolean written-marker per distinct composite key, nothing else");
        assertEquals(0, harness.numEventTimeTimers(),
                "no hand-rolled timers — native StateTtlConfig expires markers");
    }

    @Test
    @DisplayName("after marker TTL elapses a re-arrival is a fresh first write (deterministic rewrite is benign)")
    void expiredMarkerReAdmitsAsFirstWrite() throws Exception {
        emit(2885L, Timeframe.FIFTEEN_S, T0);
        harness.processElement(new StreamRecord<>(closedRow(2885L, Timeframe.FIFTEEN_S, T0)));
        assertEquals(1L, forwardedCount());
        assertEquals(1L, function.duplicateWindowCountForTest());

        harness.setStateTtlProcessingTime(
                MultiTimeframeSinks.MultiTimeframeClosedFirstWriteWinsFunction.WRITTEN_MARK_TTL.toMillis() + 1L);

        emit(2885L, Timeframe.FIFTEEN_S, T0);
        assertEquals(2L, forwardedCount(), "post-TTL re-arrival forwards again");
        assertEquals(1L, function.duplicateWindowCountForTest(),
                "post-TTL re-arrival is NOT counted as duplicate");
        assertEquals(1, harness.numKeyedStateEntries(),
                "re-admitted marker replaced expired one — state never grows");
    }

    @Test
    @DisplayName("mix: per-tf duplicate counted, cross-tf not counted — composite key isolates TF")
    void compositeKeyIsolation() throws Exception {
        // 15s duplicate
        emit(100L, Timeframe.FIFTEEN_S, T0);
        harness.processElement(new StreamRecord<>(closedRow(100L, Timeframe.FIFTEEN_S, T0)));
        // 30s same window_start but different TF — fresh
        emit(100L, Timeframe.THIRTY_S, T0);
        // 15s duplicate again
        harness.processElement(new StreamRecord<>(closedRow(100L, Timeframe.FIFTEEN_S, T0)));

        assertEquals(2L, forwardedCount(), "only 15s first + 30s first forwarded");
        assertEquals(2L, function.duplicateWindowCountForTest(),
                "both 15s re-emissions counted, 30s not");
    }
}
