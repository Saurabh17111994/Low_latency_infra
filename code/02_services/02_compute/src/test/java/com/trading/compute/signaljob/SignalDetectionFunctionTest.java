package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@link SignalDetectionFunction} driven through the Flink 2.2.1 operator
 * harness (KeyedOneInputStreamOperatorTestHarness — no cluster). Covers the
 * MVP rule contract (DEC-034): warm-up requires {@code lookback} completed
 * candles, a signal needs a bullish candle that closes strictly above the
 * highest high of the previous {@code lookback} candles, bearish candles and
 * non-breakouts never fire, fired candidate rows carry the full
 * execution-ready payload, and state is keyed per instrument.
 *
 * <p>Note on the trend filter: {@code close > mean(prev closes)} is implied by
 * {@code close > max(prev highs)} (every close ≤ some high ≤ maxHigh), so it
 * cannot fail independently — it is asserted only via the composite rule.
 */
class SignalDetectionFunctionTest {

    private static final long T0 = 1_700_000_000_000L;

    private KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> harness;

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    private void openHarness() throws Exception {
        SignalJobConfig config = SignalJobConfig.from(env());
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new SignalDetectionFunction(config),
                row -> row.getLong(CandleTableColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();
    }

    /** Baseline + lookback=3 so tests stay short; tuning keys take defaults. */
    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_TTL_MS", "60000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        env.put("SIGNAL_LOOKBACK_CANDLES", "3");
        return env;
    }

    private static GenericRowData candle(long token, long start, long end,
            long open, long high, long low, long close) {
        GenericRowData row = new GenericRowData(CandleTableColumns.FIELD_COUNT);
        row.setField(CandleTableColumns.INSTRUMENT_TOKEN, token);
        row.setField(CandleTableColumns.EXCHANGE, StringData.fromString("NSE"));
        row.setField(CandleTableColumns.SYMBOL, StringData.fromString("TEST"));
        row.setField(CandleTableColumns.WINDOW_START, start);
        row.setField(CandleTableColumns.WINDOW_END, end);
        row.setField(CandleTableColumns.OPEN_PAISE, open);
        row.setField(CandleTableColumns.HIGH_PAISE, high);
        row.setField(CandleTableColumns.LOW_PAISE, low);
        row.setField(CandleTableColumns.CLOSE_PAISE, close);
        row.setField(CandleTableColumns.VOLUME, 100L);
        row.setField(CandleTableColumns.TICK_COUNT, 5);
        row.setField(CandleTableColumns.ALGORITHM_VERSION, StringData.fromString("candle-15s-v1"));
        row.setField(CandleTableColumns.CONFIGURATION_VERSION, StringData.fromString("1.0.0"));
        row.setField(CandleTableColumns.OUTPUT_TS, end);
        row.setField(CandleTableColumns.SCHEMA_VERSION, StringData.fromString("2"));
        return row;
    }

    private void process(long token, long index, long open, long high, long low, long close)
            throws Exception {
        long end = T0 + index * 15_000L;
        harness.processElement(candle(token, end - 15_000L, end, open, high, low, close), end);
    }

    private static List<RowData> emittedRows(
            KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> h) {
        return h.getOutput().stream()
                .filter(StreamRecord.class::isInstance)
                .map(o -> (RowData) ((StreamRecord<?>) o).getValue())
                .toList();
    }

    private static String str(RowData row, int idx) {
        return row.getString(idx).toString();
    }

    @Test
    void noSignalBeforeLookbackCompletedCandles() throws Exception {
        openHarness();
        // Candles 1-3 complete the warm-up; candle 3 alone would satisfy every
        // condition against its two predecessors but must not fire.
        process(1L, 1, 100, 110, 95, 105);
        process(1L, 2, 105, 115, 100, 110);
        process(1L, 3, 110, 120, 105, 118);
        assertEquals(0, emittedRows(harness).size(), "warm-up candles must not fire");

        process(1L, 4, 115, 130, 112, 128);
        assertEquals(1, emittedRows(harness).size(), "signal fires once warm-up is complete");
    }

    @Test
    void firedCandidateCarriesFullExecutionReadyPayload() throws Exception {
        openHarness();
        process(1L, 1, 100, 110, 95, 105);
        process(1L, 2, 105, 115, 100, 110);
        process(1L, 3, 110, 120, 105, 118);
        process(1L, 4, 115, 130, 112, 128);

        RowData row = emittedRows(harness).get(0);
        assertEquals("breakout-20-bullish-trend-1-" + (T0 + 4 * 15_000L),
                str(row, SignalCandidatesTableColumns.CANDIDATE_ID));
        assertTrue(row.isNullAt(SignalCandidatesTableColumns.INSTRUCTION_ID));
        assertTrue(row.isNullAt(SignalCandidatesTableColumns.TRADE_CONTEXT_ID));
        assertEquals(1L, row.getLong(SignalCandidatesTableColumns.INSTRUMENT_TOKEN));
        assertEquals("NSE", str(row, SignalCandidatesTableColumns.EXCHANGE));
        assertEquals("TEST", str(row, SignalCandidatesTableColumns.SYMBOL));
        assertEquals("simple-breakout", str(row, SignalCandidatesTableColumns.STRATEGY_ID));
        assertEquals("1.0.0", str(row, SignalCandidatesTableColumns.STRATEGY_VERSION));
        assertEquals("breakout-20-bullish-trend", str(row, SignalCandidatesTableColumns.RULE_ID));
        assertEquals(T0 + 4 * 15_000L, row.getLong(SignalCandidatesTableColumns.DETECTION_TS));
        assertEquals(T0 + 4 * 15_000L, row.getLong(SignalCandidatesTableColumns.EVALUATION_TS));
        assertEquals("ENTRY", str(row, SignalCandidatesTableColumns.ACTION));
        assertEquals("BUY", str(row, SignalCandidatesTableColumns.SIDE));
        assertEquals(1L, row.getLong(SignalCandidatesTableColumns.QUANTITY));
        assertEquals("MARKET", str(row, SignalCandidatesTableColumns.ORDER_TYPE));
        assertTrue(row.isNullAt(SignalCandidatesTableColumns.LIMIT_PRICE_PAISE));
        assertTrue(row.isNullAt(SignalCandidatesTableColumns.SCORE_INPUTS));
        assertTrue(str(row, SignalCandidatesTableColumns.FORMATION_SNAPSHOT_REF)
                .startsWith("candle:" + (T0 + 4 * 15_000L - 15_000L) + ":" + (T0 + 4 * 15_000L)));
        assertEquals("VALID", str(row, SignalCandidatesTableColumns.VALIDITY_REASON));
        assertTrue(row.isNullAt(SignalCandidatesTableColumns.SUPERSEDES_CANDIDATE_ID));
        assertTrue(row.isNullAt(SignalCandidatesTableColumns.SUPERSEDED_BY_CANDIDATE_ID));
        assertEquals("2", str(row, SignalCandidatesTableColumns.SCHEMA_VERSION));
    }

    @Test
    void bearishCandleNeverFiresEvenWhenCloseBreaksOut() throws Exception {
        openHarness();
        process(1L, 1, 100, 110, 95, 105);
        process(1L, 2, 105, 115, 100, 110);
        process(1L, 3, 110, 120, 105, 118);
        // Bearish (close < open) but still a breakout: close 122 > max high 120.
        process(1L, 4, 130, 131, 120, 122);
        assertEquals(0, emittedRows(harness).size(), "bearish candle must not fire");

        // State rolls on: the next bullish breakout still fires.
        process(1L, 5, 120, 140, 118, 135);
        assertEquals(1, emittedRows(harness).size());
    }

    @Test
    void noFireWithoutBreakoutAbovePreviousHighs() throws Exception {
        openHarness();
        process(1L, 1, 100, 110, 95, 105);
        process(1L, 2, 105, 115, 100, 110);
        process(1L, 3, 110, 120, 105, 118);
        // Bullish, close 119 is below the previous max high of 120 -> no breakout.
        process(1L, 4, 112, 122, 110, 119);
        assertEquals(0, emittedRows(harness).size());
    }

    @Test
    void repeatedBreakoutsEmitDistinctCandidateIds() throws Exception {
        openHarness();
        // Rising series: candles 4 and 5 both break out (each above the rolling
        // 3-candle high) -> two signals, two distinct ids.
        process(1L, 1, 100, 110, 95, 105);
        process(1L, 2, 105, 115, 100, 110);
        process(1L, 3, 110, 120, 105, 118);
        process(1L, 4, 115, 130, 112, 128);
        process(1L, 5, 122, 136, 120, 132);
        List<RowData> rows = emittedRows(harness);
        assertEquals(2, rows.size());
        // Candidate ids must differ: last segment is the window_end.
        assertEquals(T0 + 4 * 15_000L + 15_000L,
                Long.parseLong(rows.get(1).getString(SignalCandidatesTableColumns.CANDIDATE_ID)
                        .toString().split("-")[5]));
        assertTrue(!rows.get(0).getString(SignalCandidatesTableColumns.CANDIDATE_ID).toString()
                .equals(rows.get(1).getString(SignalCandidatesTableColumns.CANDIDATE_ID).toString()));
    }

    @Test
    void stateIsKeyedPerInstrument() throws Exception {
        openHarness();
        // Instrument 1 fires; instrument 2 runs the same series but flat (never
        // bullish, never a breakout) -> no output for it.
        process(1L, 1, 100, 110, 95, 105);
        process(2L, 1, 100, 101, 99, 100);
        process(1L, 2, 105, 115, 100, 110);
        process(2L, 2, 100, 101, 99, 100);
        process(1L, 3, 110, 120, 105, 118);
        process(2L, 3, 100, 101, 99, 100);
        process(1L, 4, 115, 130, 112, 128);
        process(2L, 4, 100, 101, 99, 100);
        assertEquals(1, emittedRows(harness).size());
        assertEquals(1L, emittedRows(harness).get(0)
                .getLong(SignalCandidatesTableColumns.INSTRUMENT_TOKEN));
    }
}
