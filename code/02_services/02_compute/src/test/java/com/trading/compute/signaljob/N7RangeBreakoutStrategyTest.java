package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link N7RangeBreakoutStrategy} (batch 2 cutover): a port of
 * the retired {@code N7SignalFunctionTest} onto direct strategy calls — no
 * Flink harness, one instance serves one instrument by host contract.
 */
class N7RangeBreakoutStrategyTest {

    private static final long TOKEN = 777L;

    /** Heap metrics: counts per name, like the host's per-rule scope. */
    static final class HeapMetrics implements SignalStrategy.Metrics {
        final Map<String, Long> counts = new HashMap<>();

        @Override
        public void inc(String name, long n) {
            counts.merge(name, n, Long::sum);
        }

        long get(String name) {
            return counts.getOrDefault(name, 0L);
        }
    }

    static final class ListOut implements Collector<RowData> {
        final List<RowData> rows = new ArrayList<>();

        @Override
        public void collect(RowData row) {
            rows.add(row);
        }

        @Override
        public void close() {}
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

    private N7RangeBreakoutStrategy strategy;
    private HeapMetrics metrics;
    private ListOut out;

    private void open() {
        metrics = new HeapMetrics();
        out = new ListOut();
        strategy = new N7RangeBreakoutStrategy(SignalJobConfig.from(env()), metrics);
    }

    private static RowData closed(Timeframe tf, long ws, long high, long low) {
        GenericRowData r = new GenericRowData(CandleClosedColumns.FIELD_COUNT);
        r.setField(CandleClosedColumns.INSTRUMENT_TOKEN, TOKEN);
        r.setField(CandleClosedColumns.EXCHANGE, StringData.fromString("NSE"));
        r.setField(CandleClosedColumns.SYMBOL, StringData.fromString("TEST"));
        r.setField(CandleClosedColumns.TF, StringData.fromString(tf.code()));
        r.setField(CandleClosedColumns.WINDOW_START, ws);
        r.setField(CandleClosedColumns.WINDOW_END, ws + tf.windowMs());
        r.setField(CandleClosedColumns.OPEN_PAISE, low);
        r.setField(CandleClosedColumns.HIGH_PAISE, high);
        r.setField(CandleClosedColumns.LOW_PAISE, low);
        r.setField(CandleClosedColumns.CLOSE_PAISE, low);
        r.setField(CandleClosedColumns.VOLUME, 100L);
        r.setField(CandleClosedColumns.TICK_COUNT, 5);
        r.setField(CandleClosedColumns.LAST_EVENT_TIME, ws + 1_000L);
        r.setField(CandleClosedColumns.LAST_EVENT_FINGERPRINT, StringData.fromString("fp"));
        r.setField(CandleClosedColumns.SCHEMA_VERSION, StringData.fromString("1"));
        return r;
    }

    private static RowData live(Timeframe tf, long price, long tradeTime) {
        GenericRowData r = new GenericRowData(CandleLiveColumns.FIELD_COUNT);
        r.setField(CandleLiveColumns.INSTRUMENT_TOKEN, TOKEN);
        r.setField(CandleLiveColumns.EXCHANGE, StringData.fromString("NSE"));
        r.setField(CandleLiveColumns.SYMBOL, StringData.fromString("TEST"));
        r.setField(CandleLiveColumns.TF, StringData.fromString(tf.code()));
        r.setField(CandleLiveColumns.WINDOW_START, 0L);
        r.setField(CandleLiveColumns.WINDOW_END, tf.windowMs());
        r.setField(CandleLiveColumns.OPEN_PAISE, price);
        r.setField(CandleLiveColumns.HIGH_PAISE, price);
        r.setField(CandleLiveColumns.LOW_PAISE, price);
        r.setField(CandleLiveColumns.CLOSE_PAISE, price);
        r.setField(CandleLiveColumns.VOLUME, 100L);
        r.setField(CandleLiveColumns.TICK_COUNT, 5);
        r.setField(CandleLiveColumns.LAST_EVENT_TIME, tradeTime);
        r.setField(CandleLiveColumns.LAST_EVENT_FINGERPRINT, StringData.fromString("fp"));
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
        long baseH = 10_000L;
        int[] ranges = {7, 6, 5, 4, 3, 2, 1};
        for (int i = 0; i < 7; i++) {
            long high = baseH + i;
            strategy.onClosedCandle(closed(tf, i * step, high, high - ranges[i]), out);
        }
    }

    private static String field(RowData row, int idx) {
        return row.getString(idx).toString();
    }

    // — arming ————————————————————————————————————————————————————————————

    @Test
    @DisplayName("strict N7 seventh candle arms the setup")
    void strictN7Arms() throws Exception {
        open();
        feedSevenClosedWithStrictN7Last(Timeframe.FIFTEEN_S);

        assertEquals(7, strategy.ringSizeForTest(Timeframe.FIFTEEN_S));
        N7RangeBreakoutStrategy.ArmedSetup armed =
                strategy.armedForTest(Timeframe.FIFTEEN_S);
        assertNotNull(armed);
        assertEquals(6 * 15_000L, armed.windowStart);
        assertEquals(1L, strategy.armedCountForTest());
        assertEquals(1L, metrics.get("armed"));
        assertTrue(out.rows.isEmpty());
    }

    @Test
    @DisplayName("equal narrowest range is NOT an N7 — no arm")
    void equalRangeDoesNotArm() throws Exception {
        open();
        long step = Timeframe.FIFTEEN_S.windowMs();
        int[] ranges = {7, 6, 5, 4, 3, 2, 2};
        for (int i = 0; i < 7; i++) {
            long high = 10_000L + i;
            strategy.onClosedCandle(
                    closed(Timeframe.FIFTEEN_S, i * step, high, high - ranges[i]), out);
        }

        assertNull(strategy.armedForTest(Timeframe.FIFTEEN_S));
        assertEquals(0L, metrics.get("armed"));
    }

    @Test
    @DisplayName("fewer than 7 candles never arms")
    void shortRingDoesNotArm() throws Exception {
        open();
        for (int i = 0; i < 6; i++) {
            long high = 10_000L + i;
            strategy.onClosedCandle(
                    closed(Timeframe.FIFTEEN_S, i * 15_000L, high, high - (6 - i)), out);
        }

        assertNull(strategy.armedForTest(Timeframe.FIFTEEN_S));
    }

    @Test
    @DisplayName("duplicate/replayed window never double-enters the ring")
    void duplicateWindowIgnored() throws Exception {
        open();
        feedSevenClosedWithStrictN7Last(Timeframe.FIFTEEN_S);
        strategy.onClosedCandle(closed(Timeframe.FIFTEEN_S, 6 * 15_000L, 99_999L, 99_990L), out);

        assertEquals(7, strategy.ringSizeForTest(Timeframe.FIFTEEN_S));
        assertEquals(6 * 15_000L, strategy.armedForTest(Timeframe.FIFTEEN_S).windowStart);
    }

    // — firing ————————————————————————————————————————————————————————————

    @Test
    @DisplayName("price above setup high fires BUY with the pinned row shape")
    void breakoutHighFiresBuy() throws Exception {
        open();
        feedSevenClosedWithStrictN7Last(Timeframe.FIFTEEN_S);
        // Setup: 7th candle high = 10006, low = 10005.
        strategy.onLiveTick(live(Timeframe.FIFTEEN_S, 10_007L, 200_000L), out);

        assertEquals(1, out.rows.size());
        RowData row = out.rows.get(0);
        assertEquals(SignalCandidatesTableColumns.CANONICAL_N7_RULE_ID,
                field(row, SignalCandidatesTableColumns.RULE_ID));
        assertEquals(SignalCandidatesTableColumns.SIDE_BUY,
                field(row, SignalCandidatesTableColumns.SIDE));
        assertEquals(SignalCandidatesTableColumns.ACTION_ENTRY,
                field(row, SignalCandidatesTableColumns.ACTION));
        assertEquals(TOKEN, row.getLong(SignalCandidatesTableColumns.INSTRUMENT_TOKEN));
        assertEquals(200_000L, row.getLong(SignalCandidatesTableColumns.DETECTION_TS));
        assertEquals(
                N7RangeBreakoutStrategy.candidateIdFor(
                        SignalCandidatesTableColumns.CANONICAL_N7_RULE_ID,
                        TOKEN, Timeframe.FIFTEEN_S, 6 * 15_000L,
                        SignalCandidatesTableColumns.SIDE_BUY),
                field(row, SignalCandidatesTableColumns.CANDIDATE_ID));
        assertTrue(field(row, SignalCandidatesTableColumns.FORMATION_SNAPSHOT_REF)
                .startsWith("n7:FIFTEEN_S:"));
    }

    @Test
    @DisplayName("price below setup low fires SELL")
    void breakoutLowFiresSell() throws Exception {
        open();
        feedSevenClosedWithStrictN7Last(Timeframe.FIFTEEN_S);
        strategy.onLiveTick(live(Timeframe.FIFTEEN_S, 10_004L, 200_000L), out);

        assertEquals(1, out.rows.size());
        assertEquals(SignalCandidatesTableColumns.SIDE_SELL,
                field(out.rows.get(0), SignalCandidatesTableColumns.SIDE));
    }

    @Test
    @DisplayName("price inside the range fires nothing")
    void insideRangeFiresNothing() throws Exception {
        open();
        feedSevenClosedWithStrictN7Last(Timeframe.FIFTEEN_S);
        strategy.onLiveTick(live(Timeframe.FIFTEEN_S, 10_006L, 200_000L), out);
        strategy.onLiveTick(live(Timeframe.FIFTEEN_S, 10_005L, 200_001L), out);

        assertTrue(out.rows.isEmpty());
    }

    @Test
    @DisplayName("one setup fires at most once — second breach is latched")
    void fireOnceLatch() throws Exception {
        open();
        feedSevenClosedWithStrictN7Last(Timeframe.FIFTEEN_S);
        strategy.onLiveTick(live(Timeframe.FIFTEEN_S, 10_007L, 200_000L), out);
        strategy.onLiveTick(live(Timeframe.FIFTEEN_S, 10_009L, 200_001L), out);
        strategy.onLiveTick(live(Timeframe.FIFTEEN_S, 10_003L, 200_002L), out);

        assertEquals(1, out.rows.size());
    }

    @Test
    @DisplayName("same trade time twice evaluates once — no double suppress")
    void staleTradeTimeSkipped() throws Exception {
        open();
        feedSevenClosedWithStrictN7Last(Timeframe.FIFTEEN_S);
        feedSevenClosedWithStrictN7Last(Timeframe.ONE_M);
        // Breach both TFs at one price: D-006 suppresses the lower once.
        strategy.onLiveTick(live(Timeframe.FIFTEEN_S, 10_007L, 200_000L), out);
        strategy.onLiveTick(live(Timeframe.ONE_M, 10_007L, 200_000L), out);

        assertEquals(1L, metrics.get("suppressed"));
    }

    // — D-006 —————————————————————————————————————————————————————————————

    @Test
    @DisplayName("simultaneous breach on two TFs emits only the higher TF")
    void higherTfWins() throws Exception {
        open();
        feedSevenClosedWithStrictN7Last(Timeframe.FIFTEEN_S);
        feedSevenClosedWithStrictN7Last(Timeframe.ONE_M);
        // 15s setup: high 10006. 1M setup: same shape, high 10006.
        strategy.onLiveTick(live(Timeframe.FIFTEEN_S, 10_007L, 500_000L), out);

        assertEquals(1, out.rows.size());
        assertTrue(field(out.rows.get(0), SignalCandidatesTableColumns.FORMATION_SNAPSHOT_REF)
                .startsWith("n7:ONE_M:"));
        assertEquals(1L, metrics.get("suppressed"));
    }

    @Test
    @DisplayName("lone lower-TF breach still fires")
    void loneLowerTfFires() throws Exception {
        open();
        feedSevenClosedWithStrictN7Last(Timeframe.FIFTEEN_S);
        // 1M ring stays cold: only the 15s setup can breach.
        strategy.onLiveTick(live(Timeframe.FIFTEEN_S, 10_007L, 500_000L), out);

        assertEquals(1, out.rows.size());
        assertTrue(field(out.rows.get(0), SignalCandidatesTableColumns.FORMATION_SNAPSHOT_REF)
                .startsWith("n7:FIFTEEN_S:"));
        assertEquals(0L, metrics.get("suppressed"));
    }

    // — ordering edge + replacement ———————————————————————————————————————

    @Test
    @DisplayName("trade newer than the arming candle fires at arm time")
    void armTimeEdgeFires() throws Exception {
        open();
        // Six candles, then a live trade ABOVE where the 7th will arm...
        long step = Timeframe.FIFTEEN_S.windowMs();
        int[] ranges = {7, 6, 5, 4, 3, 2};
        for (int i = 0; i < 6; i++) {
            long high = 10_000L + i;
            strategy.onClosedCandle(
                    closed(Timeframe.FIFTEEN_S, i * step, high, high - ranges[i]), out);
        }
        // ...but the trade (price 10007) predates the 7th candle's window end
        // (7th covers [90000,105000)): inside-window price, no fire expected.
        strategy.onLiveTick(live(Timeframe.FIFTEEN_S, 10_007L, 95_000L), out);
        assertTrue(out.rows.isEmpty());

        // Seventh candle arms (high 10006, low 10005); last trade 10007 at
        // t=95000 is INSIDE its window [90000,105000) — still no fire.
        strategy.onClosedCandle(closed(Timeframe.FIFTEEN_S, 6 * step, 10_006L, 10_005L), out);
        assertTrue(out.rows.isEmpty());

        // A newer trade above the fresh levels fires immediately.
        strategy.onLiveTick(live(Timeframe.FIFTEEN_S, 10_008L, 110_000L), out);
        assertEquals(1, out.rows.size());
    }

    @Test
    @DisplayName("newer N7 candle replaces the setup")
    void newerSetupReplaces() throws Exception {
        open();
        feedSevenClosedWithStrictN7Last(Timeframe.FIFTEEN_S);
        assertEquals(6 * 15_000L, strategy.armedForTest(Timeframe.FIFTEEN_S).windowStart);

        // Eighth candle, strictly narrowest of its ring (flat candle, range
        // 0 < every prior 1..6): setup moves to window 7.
        strategy.onClosedCandle(
                closed(Timeframe.FIFTEEN_S, 7 * 15_000L, 10_007L, 10_007L), out);
        assertEquals(7 * 15_000L, strategy.armedForTest(Timeframe.FIFTEEN_S).windowStart);
        assertEquals(2L, strategy.armedCountForTest());
    }

    // — identity ——————————————————————————————————————————————————————————

    @Test
    @DisplayName("candidate id is deterministic per logical signal")
    void candidateIdDeterministic() {
        String a = N7RangeBreakoutStrategy.candidateIdFor(
                "n7-range-breakout-v1", TOKEN, Timeframe.FIFTEEN_S, 90_000L, "BUY");
        String b = N7RangeBreakoutStrategy.candidateIdFor(
                "n7-range-breakout-v1", TOKEN, Timeframe.FIFTEEN_S, 90_000L, "BUY");
        String c = N7RangeBreakoutStrategy.candidateIdFor(
                "n7-range-breakout-v1", TOKEN, Timeframe.FIFTEEN_S, 90_000L, "SELL");
        assertEquals(a, b);
        assertTrue(!a.equals(c));
    }
}
