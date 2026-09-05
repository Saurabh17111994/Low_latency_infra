package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.*;

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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link N7SignalFunction} driven through the Flink 2.2.1 two-input operator
 * harness — the REAL production function (input 1 = live forming candles,
 * input 2 = completed candles), real config. No mocks: the harness feeds the
 * actual CoProcess function and asserts on its actual emitted candidate rows.
 *
 * <p>Covers the approved N7 contract (docs/plans/2026-09-05-n7-signal-design.md):
 * strict-N7 arming (7 candles, strictly narrowest), warm-up (no setup before 7
 * closed), BUY/SELL breakout, fire-once per setup, D-006 higher-timeframe
 * priority on simultaneous breach, setup replacement by a newer N7, and the
 * cross-stream ordering edge (arm-time check against the last trade).
 */
class N7SignalFunctionTest {

    private static final long TOKEN = 2885L;
    private static final long BASE = 1_752_000_000_000L; // in-session epoch
    private static final String FP = "fp";

    private KeyedTwoInputStreamOperatorTestHarness<Long, RowData, RowData, RowData> harness;
    private N7SignalFunction function;

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    private void open() throws Exception {
        function = new N7SignalFunction(SignalJobConfig.from(env()));
        harness = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
                function,
                r -> r.getLong(CandleLiveColumns.INSTRUMENT_TOKEN),
                r -> r.getLong(CandleClosedColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();
    }

    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_WINDOW_ENTRIES", "2000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        return env;
    }

    // — row builders (layouts identical for live/closed) ——————————————

    /** A closed candle row in CandleClosedColumns layout. */
    private static RowData closed(long token, Timeframe tf, long ws, long high, long low,
            long close) {
        GenericRowData r = new GenericRowData(CandleClosedColumns.FIELD_COUNT);
        r.setField(CandleClosedColumns.INSTRUMENT_TOKEN, token);
        r.setField(CandleClosedColumns.EXCHANGE, StringData.fromString("NSE"));
        r.setField(CandleClosedColumns.SYMBOL, StringData.fromString("TEST"));
        r.setField(CandleClosedColumns.TF, StringData.fromString(tf.code()));
        r.setField(CandleClosedColumns.WINDOW_START, ws);
        r.setField(CandleClosedColumns.WINDOW_END, ws + tf.windowMs());
        r.setField(CandleClosedColumns.OPEN_PAISE, close);
        r.setField(CandleClosedColumns.HIGH_PAISE, high);
        r.setField(CandleClosedColumns.LOW_PAISE, low);
        r.setField(CandleClosedColumns.CLOSE_PAISE, close);
        r.setField(CandleClosedColumns.VOLUME, 100L);
        r.setField(CandleClosedColumns.TICK_COUNT, 5);
        r.setField(CandleClosedColumns.LAST_EVENT_TIME, ws + 1_000L);
        r.setField(CandleClosedColumns.LAST_EVENT_FINGERPRINT, StringData.fromString(FP));
        r.setField(CandleClosedColumns.SCHEMA_VERSION, StringData.fromString("1"));
        return r;
    }

    /** A live forming candle row in CandleLiveColumns layout. */
    private static RowData live(long token, Timeframe tf, long ws, long price, long lastEventTime) {
        GenericRowData r = new GenericRowData(CandleLiveColumns.FIELD_COUNT);
        r.setField(CandleLiveColumns.INSTRUMENT_TOKEN, token);
        r.setField(CandleLiveColumns.EXCHANGE, StringData.fromString("NSE"));
        r.setField(CandleLiveColumns.SYMBOL, StringData.fromString("TEST"));
        r.setField(CandleLiveColumns.TF, StringData.fromString(tf.code()));
        r.setField(CandleLiveColumns.WINDOW_START, ws);
        r.setField(CandleLiveColumns.WINDOW_END, ws + tf.windowMs());
        r.setField(CandleLiveColumns.OPEN_PAISE, price);
        r.setField(CandleLiveColumns.HIGH_PAISE, price);
        r.setField(CandleLiveColumns.LOW_PAISE, price);
        r.setField(CandleLiveColumns.CLOSE_PAISE, price);
        r.setField(CandleLiveColumns.VOLUME, 100L);
        r.setField(CandleLiveColumns.TICK_COUNT, 5);
        r.setField(CandleLiveColumns.LAST_EVENT_TIME, lastEventTime);
        r.setField(CandleLiveColumns.LAST_EVENT_FINGERPRINT, StringData.fromString(FP));
        r.setField(CandleLiveColumns.SCHEMA_VERSION, StringData.fromString("1"));
        return r;
    }

    /**
     * Feed 7 closed candles where the LAST is the strict N7 (range 1, all
     * prior ranges 2..7). Candle i covers [ws0 + i*step, +tfMs), high = baseH
     * + i, low = baseH + i - range_i.
     */
    private void feedSevenClosedWithStrictN7Last(Timeframe tf) throws Exception {
        long step = tf.windowMs();
        long ws0 = BASE;
        // ranges 2..7 for candles 0..5, range 1 (strict min) for candle 6
        long[] ranges = {2, 3, 4, 5, 6, 7, 1};
        for (int i = 0; i < ranges.length; i++) {
            long ws = ws0 + i * step;
            long high = 10_000L + i;
            long low = high - ranges[i];
            harness.processElement2(closed(TOKEN, tf, ws, high, low, high - 1), ws + 1_000L);
        }
    }

    private static List<RowData> emitted(
            KeyedTwoInputStreamOperatorTestHarness<Long, RowData, RowData, RowData> h) {
        return h.getOutput().stream()
                .filter(o -> o instanceof StreamRecord)
                .map(o -> (RowData) ((StreamRecord<?>) o).getValue())
                .filter(v -> v instanceof RowData)
                .toList();
    }

    private static String str(RowData r, int idx) {
        return r.isNullAt(idx) ? null : r.getString(idx).toString();
    }

    // — tests ————————————————————————————————————————————————

    @Test
    @DisplayName("warm-up: fewer than 7 closed candles never arms, no signal")
    void warmupNoSignal() throws Exception {
        open();
        // 6 candles, last is narrow — still not 7, so no setup.
        long[] ranges = {5, 5, 5, 5, 5, 1};
        for (int i = 0; i < ranges.length; i++) {
            long ws = BASE + i * Timeframe.ONE_M.windowMs();
            long high = 10_000L + i;
            harness.processElement2(
                    closed(TOKEN, Timeframe.ONE_M, ws, high, high - ranges[i], high - 1),
                    ws + 1_000L);
        }
        assertNull(function.armedForTest(TOKEN, Timeframe.ONE_M), "no setup before 7 candles");

        // Live price far above any level — still nothing (nothing armed).
        harness.processElement1(live(TOKEN, Timeframe.ONE_M, BASE + 6 * 60_000L, 20_000L,
                BASE + 6 * 60_000L + 5_000L), BASE + 6 * 60_000L + 5_000L);
        assertEquals(0, emitted(harness).size());
    }

    @Test
    @DisplayName("7 closed with strict-N7 last arms a setup; live close above high -> BUY entry")
    void strictN7ArmsAndBuyFires() throws Exception {
        open();
        feedSevenClosedWithStrictN7Last(Timeframe.ONE_M);
        N7SignalFunction.ArmedSetup armed = function.armedForTest(TOKEN, Timeframe.ONE_M);
        assertNotNull(armed, "strict N7 must arm");
        assertEquals(10_006L, armed.highPaise);
        assertEquals(10_005L, armed.lowPaise);

        // A live row with a NEW trade price above the setup high fires BUY.
        long ws1 = BASE + 7 * Timeframe.ONE_M.windowMs();
        harness.processElement1(live(TOKEN, Timeframe.ONE_M, ws1, 10_100L, ws1 + 1_000L),
                ws1 + 1_000L);
        List<RowData> rows = emitted(harness);
        assertEquals(1, rows.size(), "one breakout must emit one row");
        RowData row = rows.get(0);
        assertEquals(SignalCandidatesTableColumns.ACTION_ENTRY, str(row, SignalCandidatesTableColumns.ACTION));
        assertEquals(SignalCandidatesTableColumns.SIDE_BUY, str(row, SignalCandidatesTableColumns.SIDE));
        assertEquals(SignalCandidatesTableColumns.CANONICAL_N7_RULE_ID, str(row, SignalCandidatesTableColumns.RULE_ID));
        assertEquals(TOKEN, row.getLong(SignalCandidatesTableColumns.INSTRUMENT_TOKEN));
        assertEquals("NSE", str(row, SignalCandidatesTableColumns.EXCHANGE));
        assertEquals("TEST", str(row, SignalCandidatesTableColumns.SYMBOL));
        assertFalse(row.isNullAt(SignalCandidatesTableColumns.CANDIDATE_ID));
        assertFalse(row.isNullAt(SignalCandidatesTableColumns.SCORE_INPUTS));
        assertFalse(row.isNullAt(SignalCandidatesTableColumns.FORMATION_SNAPSHOT_REF));
        assertFalse(row.isNullAt(SignalCandidatesTableColumns.DETECTION_TS));
        assertEquals(SignalCandidatesTableColumns.SCHEMA_VERSION_V2,
                str(row, SignalCandidatesTableColumns.SCHEMA_VERSION));
    }

    @Test
    @DisplayName("price below setup low -> SELL entry")
    void sellFiresBelowLow() throws Exception {
        open();
        feedSevenClosedWithStrictN7Last(Timeframe.ONE_M);
        long ws = BASE + 7 * Timeframe.ONE_M.windowMs();
        harness.processElement1(live(TOKEN, Timeframe.ONE_M, ws, 9_900L, ws + 1_000L),
                ws + 1_000L);
        List<RowData> rows = emitted(harness);
        assertEquals(1, rows.size());
        assertEquals(SignalCandidatesTableColumns.SIDE_SELL, str(rows.get(0), SignalCandidatesTableColumns.SIDE));
    }

    @Test
    @DisplayName("price inside the N7 range never fires")
    void noFireInsideRange() throws Exception {
        open();
        feedSevenClosedWithStrictN7Last(Timeframe.ONE_M);
        long ws = BASE + 7 * Timeframe.ONE_M.windowMs();
        // Setup range [10005,10006]; price 10005.5 not representable — use 10006 (== high,
        // strict > required) and 10005 (== low, strict < required) — both inside.
        harness.processElement1(live(TOKEN, Timeframe.ONE_M, ws, 10_006L, ws + 1_000L),
                ws + 1_000L);
        harness.processElement1(live(TOKEN, Timeframe.ONE_M, ws, 10_005L, ws + 2_000L),
                ws + 2_000L);
        assertEquals(0, emitted(harness).size(), "equal-to-level prices are not breakouts");
    }

    @Test
    @DisplayName("equal range (tie) is NOT a strict N7")
    void tieIsNotStrictN7() throws Exception {
        open();
        // Last candle range 1 == a prior range 1 — tie, not strict.
        long[] ranges = {1, 3, 4, 5, 6, 7, 1};
        for (int i = 0; i < ranges.length; i++) {
            long ws = BASE + i * Timeframe.ONE_M.windowMs();
            long high = 10_000L + i;
            harness.processElement2(
                    closed(TOKEN, Timeframe.ONE_M, ws, high, high - ranges[i], high - 1),
                    ws + 1_000L);
        }
        assertNull(function.armedForTest(TOKEN, Timeframe.ONE_M), "tie must not arm");
    }

    @Test
    @DisplayName("fire-once per setup: a second breakout of the same setup emits nothing")
    void fireOncePerSetup() throws Exception {
        open();
        feedSevenClosedWithStrictN7Last(Timeframe.ONE_M);
        long ws1 = BASE + 7 * Timeframe.ONE_M.windowMs();
        harness.processElement1(live(TOKEN, Timeframe.ONE_M, ws1, 10_100L, ws1 + 1_000L),
                ws1 + 1_000L);
        assertEquals(1, emitted(harness).size());
        // Same setup, even higher price — latched.
        harness.processElement1(live(TOKEN, Timeframe.ONE_M, ws1, 10_200L, ws1 + 2_000L),
                ws1 + 2_000L);
        assertEquals(1, emitted(harness).size(), "setup fires at most once");
    }

    @Test
    @DisplayName("a newer N7 candle replaces the setup and can fire again")
    void newerN7ReplacesSetup() throws Exception {
        open();
        feedSevenClosedWithStrictN7Last(Timeframe.ONE_M);
        long wsFirst = BASE + 7 * Timeframe.ONE_M.windowMs();
        harness.processElement1(live(TOKEN, Timeframe.ONE_M, wsFirst, 10_100L,
                wsFirst + 1_000L), wsFirst + 1_000L);
        assertEquals(1, emitted(harness).size());

        // Feed 7 more candles with a strict N7 at the end, HIGHER window.
        long step = Timeframe.ONE_M.windowMs();
        long ws0 = BASE + 8 * step;
        long[] ranges = {2, 3, 4, 5, 6, 7, 1};
        for (int i = 0; i < ranges.length; i++) {
            long ws = ws0 + i * step;
            long high = 11_000L + i;
            harness.processElement2(
                    closed(TOKEN, Timeframe.ONE_M, ws, high, high - ranges[i], high - 1),
                    ws + 1_000L);
        }
        assertNotNull(function.armedForTest(TOKEN, Timeframe.ONE_M));
        long ws2 = ws0 + 7 * step;
        harness.processElement1(live(TOKEN, Timeframe.ONE_M, ws2, 11_100L, ws2 + 1_000L),
                ws2 + 1_000L);
        assertEquals(2, emitted(harness).size(), "new setup fires independently");
    }

    @Test
    @DisplayName("D-006: simultaneous breach on 15s + 1m emits only the higher TF (1m)")
    void higherTimeframeWinsOnSimultaneousBreach() throws Exception {
        open();
        // Arm a 15s setup: levels [10005,10006].
        feedSevenClosedWithStrictN7Last(Timeframe.FIFTEEN_S);
        // Arm a 1m setup: levels [11005,11006] (different windows, higher levels).
        long step = Timeframe.ONE_M.windowMs();
        long ws0 = BASE + 100 * step;
        long[] ranges = {2, 3, 4, 5, 6, 7, 1};
        for (int i = 0; i < ranges.length; i++) {
            long ws = ws0 + i * step;
            long high = 11_000L + i;
            harness.processElement2(
                    closed(TOKEN, Timeframe.ONE_M, ws, high, high - ranges[i], high - 1),
                    ws + 1_000L);
        }
        assertNotNull(function.armedForTest(TOKEN, Timeframe.FIFTEEN_S));
        assertNotNull(function.armedForTest(TOKEN, Timeframe.ONE_M));

        // One price above BOTH highs (10100 > 10006 and 11006? No — 10100 < 11006).
        // Use 11100: above both. One live row, one trade time.
        long t = ws0 + 7 * step + 5_000L;
        harness.processElement1(live(TOKEN, Timeframe.ONE_M, t - 5_000L, 11_100L, t), t);
        List<RowData> rows = emitted(harness);
        assertEquals(1, rows.size(), "simultaneous breach emits exactly one row");
        String ref = str(rows.get(0), SignalCandidatesTableColumns.FORMATION_SNAPSHOT_REF);
        assertTrue(ref.contains(Timeframe.ONE_M.code()),
                "the higher timeframe (1m) must win, got ref: " + ref);
        assertFalse(ref.contains(Timeframe.FIFTEEN_S.code()));
        assertTrue(function.getSuppressedCountForTest() >= 1, "lower TF must be counted suppressed");
    }

    @Test
    @DisplayName("arm-time ordering edge: live price already breached fires on arming")
    void armTimeCheckFiresWhenPriceAlreadyBreached() throws Exception {
        open();
        // A live row with a NEW trade arrives BEFORE the closed candle that arms.
        // Feed 6 closed candles (not yet an N7), then a live price above the
        // would-be levels, then the 7th closed (the N7). The arm-time check
        // must fire immediately with the stored last price.
        long step = Timeframe.ONE_M.windowMs();
        long[] ranges = {2, 3, 4, 5, 6, 7};
        for (int i = 0; i < ranges.length; i++) {
            long ws = BASE + i * step;
            long high = 10_000L + i;
            harness.processElement2(
                    closed(TOKEN, Timeframe.ONE_M, ws, high, high - ranges[i], high - 1),
                    ws + 1_000L);
        }
        // Live trade at t inside the NEXT window, price 10_100 — above the level the
        // 7th candle will arm (high 10_006). The next window starts AFTER the
        // arming candle's windowEnd, so the trade is genuinely post-close.
        long wsLive = BASE + 7 * step + 1_000L;
        harness.processElement1(live(TOKEN, Timeframe.ONE_M, wsLive, 10_100L, wsLive),
                wsLive);
        assertEquals(0, emitted(harness).size(), "nothing armed yet — no fire");

        // Now the 7th closed candle (strict N7, high 10_006). Its windowEnd =
        // BASE + 7*step < wsLive, so the stored last trade is newer and already
        // breached — the arm-time check must fire immediately.
        long ws7 = BASE + 6 * step;
        harness.processElement2(
                closed(TOKEN, Timeframe.ONE_M, ws7, 10_006L, 10_005L, 10_005L),
                ws7 + 1_000L);
        assertEquals(1, emitted(harness).size(),
                "arm-time check must fire because the stored last trade is newer "
                        + "than the arming candle's window end and already breached");
    }

    @Test
    @DisplayName("arm-time check never fires on the arming candle's own window price")
    void armTimeCheckIgnoresStaleInWindowPrice() throws Exception {
        open();
        long step = Timeframe.ONE_M.windowMs();
        long[] ranges = {2, 3, 4, 5, 6};
        for (int i = 0; i < ranges.length; i++) {
            long ws = BASE + i * step;
            long high = 10_000L + i;
            harness.processElement2(
                    closed(TOKEN, Timeframe.ONE_M, ws, high, high - ranges[i], high - 1),
                    ws + 1_000L);
        }
        // Live trade INSIDE the 7th candle's own window (its last event time
        // < the candle's windowEnd). That price is the candle's own close, so it
        // must NOT trigger an arm-time fire when the 7th candle closes.
        long ws7 = BASE + 6 * step;
        // Live trade inside the arming candle's own window, price ABOVE the
        // would-be high. The arm-time guard must NOT fire: the price belongs
        // to the forming candle itself, so it can never be a post-close
        // breakout of a setup that only exists once that candle closes.
        harness.processElement1(live(TOKEN, Timeframe.ONE_M, ws7, 10_300L, ws7 + 500L),
                ws7 + 500L);
        harness.processElement2(
                closed(TOKEN, Timeframe.ONE_M, ws7, 10_100L, 10_099L, 10_300L),
                ws7 + step);
        assertEquals(0, emitted(harness).size(),
                "a trade inside the arming candle's own window is not a post-close breakout");
    }

    @Test
    @DisplayName("candidate_id deterministic across identical inputs")
    void candidateIdDeterministic() throws Exception {
        assertEquals(
                N7SignalFunction.candidateIdFor("n7-range-breakout-v1", TOKEN, Timeframe.ONE_M,
                        1_752_000_000_000L, SignalCandidatesTableColumns.SIDE_BUY),
                N7SignalFunction.candidateIdFor("n7-range-breakout-v1", TOKEN, Timeframe.ONE_M,
                        1_752_000_000_000L, SignalCandidatesTableColumns.SIDE_BUY));
        // Different side or window -> different id.
        assertNotEquals(
                N7SignalFunction.candidateIdFor("n7-range-breakout-v1", TOKEN, Timeframe.ONE_M,
                        1_752_000_000_000L, SignalCandidatesTableColumns.SIDE_BUY),
                N7SignalFunction.candidateIdFor("n7-range-breakout-v1", TOKEN, Timeframe.ONE_M,
                        1_752_000_000_000L, SignalCandidatesTableColumns.SIDE_SELL));
    }
}
