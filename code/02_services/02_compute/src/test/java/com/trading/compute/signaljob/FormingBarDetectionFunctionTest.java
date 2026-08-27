package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.common.model.FormingBar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@link FormingBarDetectionFunction} driven through the Flink 2.2.1 two-input
 * operator harness — the REAL production function (input 1 = live
 * {@link FormingBar} events, input 2 = completed candles for the lookback),
 * real config. No mocks, no logic reimplementation: the harness feeds the
 * actual CoProcess function and asserts on its actual emitted candidate rows.
 *
 * <p>Slice 2.2 tests 3-7: the forming-bar event reaches Business Logic BEFORE
 * the candle closes (no watermark advance — the harness feeds events directly),
 * the candidate payload carries the contract fields, the detector fires on the
 * placeholder mirrored-breakout condition (positive), stays silent when the
 * condition is false (negative), fires at most once per forming window
 * (placeholder-only), and warms up before any completed-candle history exists.
 */
class FormingBarDetectionFunctionTest {

    private static final long T0 = 1_710_000_000_000L;
    private static final long WINDOW_MS = 15_000L;

    private KeyedTwoInputStreamOperatorTestHarness<Long, FormingBar, RowData, RowData> harness;

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    private void openHarness() throws Exception {
        harness = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
                new FormingBarDetectionFunction(SignalJobConfig.from(env())),
                bar -> bar.instrumentToken(),
                candle -> candle.getLong(CandleTableColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();
    }

    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_TTL_MS", "60000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        // Forming-bar placeholder lookback: 3 completed candles (test speed).
        env.put("FORMING_LOOKBACK_CANDLES", "3");
        return env;
    }

    /** Completed candle (input 2) with the CandleTableColumns layout. */
    private static RowData candle(long token, long windowStart, long high, long close) {
        GenericRowData row = new GenericRowData(CandleTableColumns.FIELD_COUNT);
        row.setField(CandleTableColumns.INSTRUMENT_TOKEN, token);
        row.setField(CandleTableColumns.WINDOW_START, windowStart);
        row.setField(CandleTableColumns.WINDOW_END, windowStart + WINDOW_MS);
        row.setField(CandleTableColumns.HIGH_PAISE, high);
        row.setField(CandleTableColumns.CLOSE_PAISE, close);
        return row;
    }

    private static FormingBar formingBar(long token, long windowStart, long open, long high,
            long low, long close) {
        return new FormingBar(token, windowStart, windowStart + WINDOW_MS,
                open, high, low, close, 0L, 0L, windowStart + 7_000L, "fp-live", "NSE", "TEST");
    }

    private void sendCandle(long windowStart, long high, long close) throws Exception {
        harness.processElement2(candle(7L, windowStart, high, close),
                windowStart + WINDOW_MS - 1L);
    }

    private void sendFormingBar(FormingBar bar) throws Exception {
        harness.processElement1(bar, bar.lastEventTime());
    }

    private static List<RowData> candidates(
            KeyedTwoInputStreamOperatorTestHarness<Long, FormingBar, RowData, RowData> h) {
        return h.getOutput().stream()
                .filter(o -> o instanceof StreamRecord)
                .map(o -> (RowData) ((StreamRecord<?>) o).getValue())
                .toList();
    }

    @Test
    void formingBarEventReachesBusinessLogicBeforeCandleCloses() throws Exception {
        openHarness();
        // Completed history: 3 prior candles (highs 100, closes 95).
        sendCandle(T0 - 3 * WINDOW_MS, 100, 95);
        sendCandle(T0 - 2 * WINDOW_MS, 100, 95);
        sendCandle(T0 - WINDOW_MS, 100, 95);

        // Live forming-bar event for window [T0, T0+15s) at T0+7s — the window
        // is STILL OPEN (no watermark past T0+15s was ever injected). The
        // detector must evaluate and emit NOW, not wait for candle close.
        sendFormingBar(formingBar(7L, T0, 90, 110, 90, 110));

        List<RowData> candidates = candidates(harness);
        assertEquals(1, candidates.size(),
                "Business Logic receives the forming-bar event while the window is still forming");
    }

    @Test
    void candidatePayloadCarriesContractFields() throws Exception {
        openHarness();
        sendCandle(T0 - 3 * WINDOW_MS, 100, 95);
        sendCandle(T0 - 2 * WINDOW_MS, 100, 95);
        sendCandle(T0 - WINDOW_MS, 100, 95);
        sendFormingBar(formingBar(7L, T0, 90, 110, 90, 110));

        RowData candidate = candidates(harness).get(0);
        assertEquals("breakout-5-forming-bar", stringAt(candidate, SignalCandidatesTableColumns.RULE_ID));
        assertEquals("simple-breakout", stringAt(candidate, SignalCandidatesTableColumns.STRATEGY_ID));
        assertEquals("1.0.0", stringAt(candidate, SignalCandidatesTableColumns.STRATEGY_VERSION));
        assertEquals("2", stringAt(candidate, SignalCandidatesTableColumns.SCHEMA_VERSION));
        assertEquals(7L, candidate.getLong(SignalCandidatesTableColumns.INSTRUMENT_TOKEN));
        assertEquals("NSE", stringAt(candidate, SignalCandidatesTableColumns.EXCHANGE));
        assertEquals("TEST", stringAt(candidate, SignalCandidatesTableColumns.SYMBOL));
        assertEquals("ENTRY", stringAt(candidate, SignalCandidatesTableColumns.ACTION));
        assertEquals("BUY", stringAt(candidate, SignalCandidatesTableColumns.SIDE));
        assertEquals("MARKET", stringAt(candidate, SignalCandidatesTableColumns.ORDER_TYPE));
        assertEquals("VALID", stringAt(candidate, SignalCandidatesTableColumns.VALIDITY_REASON));
        assertEquals("breakout-5-forming-bar-7-" + T0,
                stringAt(candidate, SignalCandidatesTableColumns.CANDIDATE_ID));
        // Event timestamp/fingerprint ride the candidate (source metadata).
        assertEquals(T0 + 7_000L, candidate.getLong(SignalCandidatesTableColumns.DETECTION_TS));
        assertEquals(T0 + 7_000L, candidate.getLong(SignalCandidatesTableColumns.EVALUATION_TS));
        assertNull(candidate.isNullAt(SignalCandidatesTableColumns.INSTRUCTION_ID)
                ? null : candidate.getString(SignalCandidatesTableColumns.INSTRUCTION_ID));
        // Formation snapshot pins the live forming bar (window boundaries + OHLCV).
        String ref = stringAt(candidate, SignalCandidatesTableColumns.FORMATION_SNAPSHOT_REF);
        assertTrue(ref.startsWith("forming-bar:" + T0 + ":" + (T0 + WINDOW_MS)), ref);
        assertTrue(ref.contains("open=90"), ref);
        assertTrue(ref.contains("close=110"), ref);
    }

    @Test
    void detectorFiresOnPositiveCondition() throws Exception {
        openHarness();
        sendCandle(T0 - 3 * WINDOW_MS, 100, 95);
        sendCandle(T0 - 2 * WINDOW_MS, 100, 95);
        sendCandle(T0 - WINDOW_MS, 100, 95);
        // close 110 > open 90 (bullish), 110 > maxHigh 100 (breakout),
        // 110 * 3 > 285 (trend).
        sendFormingBar(formingBar(7L, T0, 90, 110, 90, 110));
        assertEquals(1, candidates(harness).size());
    }

    @Test
    void detectorDoesNotFireWhenConditionFalse() throws Exception {
        openHarness();
        sendCandle(T0 - 3 * WINDOW_MS, 100, 95);
        sendCandle(T0 - 2 * WINDOW_MS, 100, 95);
        sendCandle(T0 - WINDOW_MS, 100, 95);
        // close 99 < maxHigh 100 — no breakout → no fire.
        sendFormingBar(formingBar(7L, T0, 90, 99, 90, 99));
        assertEquals(0, candidates(harness).size(),
                "a flat/non-breakout forming bar must not produce a candidate");

        // A later bearish-bar update also stays silent.
        sendFormingBar(formingBar(7L, T0, 110, 110, 90, 90));
        assertEquals(0, candidates(harness).size());
    }

    @Test
    void firesAtMostOncePerFormingWindow() throws Exception {
        openHarness();
        sendCandle(T0 - 3 * WINDOW_MS, 100, 95);
        sendCandle(T0 - 2 * WINDOW_MS, 100, 95);
        sendCandle(T0 - WINDOW_MS, 100, 95);
        // Multiple qualifying updates of the SAME window (live snapshots) —
        // fire-once per window (placeholder-only semantics).
        sendFormingBar(formingBar(7L, T0, 90, 110, 90, 110));
        sendFormingBar(formingBar(7L, T0, 90, 112, 90, 112));
        sendFormingBar(formingBar(7L, T0, 90, 115, 90, 115));
        assertEquals(1, candidates(harness).size(),
                "duplicate forming-bar updates cannot generate duplicate candidates");

        // A NEW window is eligible again.
        sendFormingBar(formingBar(7L, T0 + WINDOW_MS, 90, 110, 90, 110));
        assertEquals(2, candidates(harness).size(),
                "a new forming window resets the fire-once latch");
    }

    @Test
    void driftingFeedFiresOncePerWarmWindow() throws Exception {
        openHarness();
        // Lookback highs/closes drift upward by 1 per window (100→102,
        // 95→97) — a miniature of the deterministic P6 feed, whose per-window
        // +50 paise high tick makes each window's forming close beat the
        // previous lookback max. Emission profile: EVERY warm window qualifies
        // once (up to N − lookback + 1 fires), not one fire ever.
        sendCandle(T0 - 3 * WINDOW_MS, 100, 95);
        sendCandle(T0 - 2 * WINDOW_MS, 101, 96);
        sendCandle(T0 - WINDOW_MS, 102, 97);
        // close 103 > maxHigh 102 by the drift increment — bullish (103 > 98),
        // breakout, trend (103 * 3 > 288).
        sendFormingBar(formingBar(7L, T0, 98, 103, 98, 103));
        assertEquals(1, candidates(harness).size(),
                "the drift close must beat the lookback max and fire");
        // Fire-once per window: a higher close of the SAME window stays silent.
        sendFormingBar(formingBar(7L, T0, 98, 104, 98, 104));
        assertEquals(1, candidates(harness).size(),
                "fire-once must still hold on the drifting feed");
        // The NEXT window fires again — one fire per warm window, not one ever.
        sendFormingBar(formingBar(7L, T0 + WINDOW_MS, 99, 104, 99, 104));
        assertEquals(2, candidates(harness).size(),
                "each warm window on a drifting feed fires exactly once");
    }

    @Test
    void qualifyingTickBeforeWarmUpIsLostForThatWindow() throws Exception {
        openHarness();
        // Only 2 of the 3 required candles (FORMING_LOOKBACK_CANDLES=3 in the
        // harness env) — the ring is one short.
        sendCandle(T0 - 2 * WINDOW_MS, 100, 95);
        sendCandle(T0 - WINDOW_MS, 100, 95);
        // The window's ONLY qualifying instant (close 110 > maxHigh 100) is
        // evaluated while not warm — no fire. The +50 tick is a single live
        // instant; it never comes again for this window.
        sendFormingBar(formingBar(7L, T0, 90, 110, 90, 110));
        assertEquals(0, candidates(harness).size(),
                "warm-up: the qualifying tick arrived before the ring was full");

        // The 3rd candle now completes the ring, but this window's remaining
        // updates (flat/bearish closes) never qualify — the warm-up race
        // skips the window PERMANENTLY (the P6 emission-profile variance).
        sendCandle(T0 - 3 * WINDOW_MS, 100, 95);
        sendFormingBar(formingBar(7L, T0, 90, 100, 90, 100));
        sendFormingBar(formingBar(7L, T0, 110, 110, 90, 90));
        assertEquals(0, candidates(harness).size(),
                "a window whose qualifying tick lost the warm-up race never fires");

        // The NEXT window is eligible (the ring is warm now).
        sendFormingBar(formingBar(7L, T0 + WINDOW_MS, 90, 120, 90, 120));
        assertEquals(1, candidates(harness).size(),
                "the next warm window must still fire");
    }

    @Test
    void warmUpRequiresLookbackHistory() throws Exception {
        openHarness();
        // Only 2 of the 3 required completed candles — no evaluation yet.
        sendCandle(T0 - 2 * WINDOW_MS, 100, 95);
        sendCandle(T0 - WINDOW_MS, 100, 95);
        sendFormingBar(formingBar(7L, T0, 90, 110, 90, 110));
        assertEquals(0, candidates(harness).size(), "warm-up: not enough completed-candle history");

        // Third candle arrives → the same forming-bar update now evaluates.
        sendCandle(T0 - 3 * WINDOW_MS, 100, 95);
        sendFormingBar(formingBar(7L, T0, 90, 110, 90, 110));
        assertEquals(1, candidates(harness).size());
    }

    @Test
    void lookbackNeverIncludesFormingOrFutureCandle() throws Exception {
        openHarness();
        sendCandle(T0 - 3 * WINDOW_MS, 100, 95);
        sendCandle(T0 - 2 * WINDOW_MS, 100, 95);
        sendCandle(T0 - WINDOW_MS, 100, 95);
        // A non-firing forming event for window [T0, T0+15s) establishes the
        // current forming window (bearish — no fire, warm-up satisfied).
        sendFormingBar(formingBar(7L, T0, 110, 110, 90, 90));
        assertEquals(0, candidates(harness).size());

        // The forming window's own candle (windowEnd == T0+15s > T0) and a
        // future candle (windowEnd == T0+30s) arrive mid-window — both must
        // be REJECTED by the strictly-prior guard and never enter the lookback
        // (if they did, their high 500 would break out any later close).
        sendCandle(T0, 500, 500);            // forming window's own candle
        sendCandle(T0 + WINDOW_MS, 500, 500); // future candle

        // A qualifying forming-bar update (close 110 > maxHigh 100) fires —
        // proof the own/future candles never polluted the history.
        sendFormingBar(formingBar(7L, T0, 90, 110, 90, 110));
        assertEquals(1, candidates(harness).size(),
                "the forming/future candles must not leak into the lookback");
    }

    private static String stringAt(RowData row, int index) {
        return row.isNullAt(index) ? null : row.getString(index).toString();
    }
}
