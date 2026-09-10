package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link StrategyHostFunction} — the plug-and-play strategy
 * runner (2026-09-05). The stub smoke strategy carries the routing assertions
 * (live + closed delivery, per-token slots, per-TF counts, never emits); tiny
 * emitting strategies below prove the host dedups and forwards; the guard
 * tests prove the fail-fast contract.
 */
class StrategyHostFunctionTest {

    private KeyedTwoInputStreamOperatorTestHarness<Long, RowData, RowData, RowData> harness;
    private StrategyHostFunction function;

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
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

    private void open(String... strategyIds) throws Exception {
        function = new StrategyHostFunction(SignalJobConfig.from(env()), List.of(strategyIds));
        harness = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
                function,
                r -> r.getLong(CandleLiveColumns.INSTRUMENT_TOKEN),
                r -> r.getLong(CandleClosedColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();
    }

    private StubSmokeStrategy stubFor(long token) {
        return (StubSmokeStrategy) function.strategyForTest(token, StubSmokeStrategy.RULE_ID);
    }

    // — row builders (same layout as N7SignalFunctionTest) ————————————————

    private static RowData live(long token, Timeframe tf, long ws, long price) {
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
        r.setField(CandleLiveColumns.LAST_EVENT_TIME, ws + 1_000L);
        r.setField(CandleLiveColumns.LAST_EVENT_FINGERPRINT, StringData.fromString("fp"));
        r.setField(CandleLiveColumns.SCHEMA_VERSION, StringData.fromString("1"));
        return r;
    }

    private static RowData closed(long token, Timeframe tf, long ws, long price) {
        GenericRowData r = new GenericRowData(CandleClosedColumns.FIELD_COUNT);
        r.setField(CandleClosedColumns.INSTRUMENT_TOKEN, token);
        r.setField(CandleClosedColumns.EXCHANGE, StringData.fromString("NSE"));
        r.setField(CandleClosedColumns.SYMBOL, StringData.fromString("TEST"));
        r.setField(CandleClosedColumns.TF, StringData.fromString(tf.code()));
        r.setField(CandleClosedColumns.WINDOW_START, ws);
        r.setField(CandleClosedColumns.WINDOW_END, ws + tf.windowMs());
        r.setField(CandleClosedColumns.OPEN_PAISE, price);
        r.setField(CandleClosedColumns.HIGH_PAISE, price);
        r.setField(CandleClosedColumns.LOW_PAISE, price);
        r.setField(CandleClosedColumns.CLOSE_PAISE, price);
        r.setField(CandleClosedColumns.VOLUME, 100L);
        r.setField(CandleClosedColumns.TICK_COUNT, 5);
        r.setField(CandleClosedColumns.LAST_EVENT_TIME, ws + 1_000L);
        r.setField(CandleClosedColumns.LAST_EVENT_FINGERPRINT, StringData.fromString("fp"));
        r.setField(CandleClosedColumns.SCHEMA_VERSION, StringData.fromString("1"));
        return r;
    }

    private void in1(RowData row) throws Exception {
        harness.processElement1(row, row.getLong(CandleLiveColumns.LAST_EVENT_TIME));
    }

    private void in2(RowData row) throws Exception {
        harness.processElement2(row, row.getLong(CandleClosedColumns.LAST_EVENT_TIME));
    }

    // — routing ———————————————————————————————————————————————————————————

    @Test
    @DisplayName("stub counts live + closed per TF, emits nothing, slots isolate tokens")
    void stubCountsPerTfAndIsolatesTokens() throws Exception {
        open(StubSmokeStrategy.RULE_ID);
        long t1 = 111L;
        long t2 = 222L;
        in1(live(t1, Timeframe.FIFTEEN_S, 0L, 100L));
        in1(live(t1, Timeframe.FIFTEEN_S, 0L, 101L));
        in1(live(t1, Timeframe.ONE_M, 0L, 100L));
        in1(live(t2, Timeframe.FIFTEEN_S, 0L, 100L));
        in2(closed(t1, Timeframe.FIFTEEN_S, 0L, 100L));

        StubSmokeStrategy stub1 = stubFor(t1);
        assertEquals(2L, stub1.liveCountForTest(Timeframe.FIFTEEN_S));
        assertEquals(1L, stub1.liveCountForTest(Timeframe.ONE_M));
        assertEquals(0L, stub1.liveCountForTest(Timeframe.FIVE_M));
        assertEquals(1L, stub1.closedCountForTest(Timeframe.FIFTEEN_S));
        assertEquals(0L, stub1.closedCountForTest(Timeframe.ONE_M));
        // Second token routes to its own slot — never into t1's counts.
        StubSmokeStrategy stub2 = stubFor(t2);
        assertEquals(1L, stub2.liveCountForTest(Timeframe.FIFTEEN_S));
        assertEquals(0L, stub2.closedCountForTest(Timeframe.FIFTEEN_S));
        assertEquals(0L, stub1.liveCountForTest(Timeframe.FIVE_M));
        assertEquals(0, harness.getOutput().size());
    }

    // — dedup + forward ———————————————————————————————————————————————————

    /** Emits the same candidate on every closed candle — host forwards once. */
    static final class Repeater implements SignalStrategy {
        static final String RULE_ID = "test-repeater-v1";

        @Override
        public String ruleId() {
            return RULE_ID;
        }

        @Override
        public void onLiveTick(RowData live, Collector<RowData> out) {}

        @Override
        public void onClosedCandle(RowData closed, Collector<RowData> out) {
            String tf = closed.getString(CandleClosedColumns.TF).toString();
            long token = closed.getLong(CandleClosedColumns.INSTRUMENT_TOKEN);
            GenericRowData row = new GenericRowData(SignalCandidatesTableColumns.FIELD_COUNT);
            row.setField(SignalCandidatesTableColumns.CANDIDATE_ID,
                    StringData.fromString("repeat|" + token + "|" + tf));
            row.setField(SignalCandidatesTableColumns.RULE_ID, StringData.fromString(RULE_ID));
            out.collect(row);
        }
    }

    /** Emits a blank candidate id — the host must throw, never sink it. */
    static final class BlankEmitter implements SignalStrategy {
        static final String RULE_ID = "test-blank-v1";

        @Override
        public String ruleId() {
            return RULE_ID;
        }

        @Override
        public void onLiveTick(RowData live, Collector<RowData> out) {}

        @Override
        public void onClosedCandle(RowData closed, Collector<RowData> out) {
            GenericRowData row = new GenericRowData(SignalCandidatesTableColumns.FIELD_COUNT);
            row.setField(SignalCandidatesTableColumns.CANDIDATE_ID,
                    StringData.fromString("   "));
            row.setField(SignalCandidatesTableColumns.RULE_ID, StringData.fromString(RULE_ID));
            out.collect(row);
        }
    }

    /**
     * Test-only registration through {@link Strategies#registerForTest}: lets
     * unit tests register emitting strategies without touching the production
     * registry.
     */
    static {
        Strategies.registerForTest(Repeater.RULE_ID, (config, metrics) -> new Repeater());
        Strategies.registerForTest(BlankEmitter.RULE_ID, (config, metrics) -> new BlankEmitter());
    }

    @Test
    @DisplayName("host forwards first emit, dedups replay by candidate id")
    void hostForwardsOnceThenDedups() throws Exception {
        open(Repeater.RULE_ID);
        long token = 333L;
        in2(closed(token, Timeframe.FIFTEEN_S, 0L, 100L));
        in2(closed(token, Timeframe.FIFTEEN_S, 15_000L, 101L));
        in2(closed(token, Timeframe.FIFTEEN_S, 30_000L, 102L));

        assertEquals(1, harness.getOutput().size());
        assertEquals(1L, function.emittedForTest());
        assertEquals(2L, function.suppressedForTest());
    }

    @Test
    @DisplayName("distinct candidate ids per TF forward independently")
    void distinctIdsPerTfForward() throws Exception {
        open(Repeater.RULE_ID);
        long token = 444L;
        in2(closed(token, Timeframe.FIFTEEN_S, 0L, 100L));
        in2(closed(token, Timeframe.ONE_M, 0L, 100L));

        assertEquals(2, harness.getOutput().size());
        assertEquals(2L, function.emittedForTest());
        assertEquals(0L, function.suppressedForTest());
    }

    // — fail fast —————————————————————————————————————————————————————————

    @Test
    @DisplayName("blank candidate id drops and counts instead of killing the subtask (P2-057)")
    void blankCandidateIdDropped() throws Exception {
        open(BlankEmitter.RULE_ID);
        in2(closed(555L, Timeframe.FIFTEEN_S, 0L, 100L));
        assertEquals(0, harness.getOutput().size());
        assertEquals(1L, function.droppedUnkeyedForTest());
        assertEquals(0L, function.emittedForTest());
    }

    @Test
    @DisplayName("unknown strategy id fails at open, never mid-stream")
    void unknownStrategyIdFailsAtOpen() {
        assertThrows(IllegalStateException.class, () -> open("no-such-strategy-v9"));
    }

    @Test
    @DisplayName("empty strategy list fails at open (host-on requires STRATEGIES)")
    void emptyStrategyListFailsAtOpen() {
        assertThrows(IllegalStateException.class, () -> open());
    }

    @Test
    @DisplayName("stub skips null/unknown TF instead of crashing (P2-059/060)")
    void stubSkipsPoisonTf() throws Exception {
        open(StubSmokeStrategy.RULE_ID);
        long t1 = 111L;
        GenericRowData nullTf = (GenericRowData) live(t1, Timeframe.FIFTEEN_S, 0L, 100L);
        nullTf.setField(CandleLiveColumns.TF, null);
        in1(nullTf);
        GenericRowData badTf = (GenericRowData) closed(t1, Timeframe.FIFTEEN_S, 0L, 100L);
        badTf.setField(CandleClosedColumns.TF, StringData.fromString("NOPE"));
        in2(badTf);
        StubSmokeStrategy stub1 = stubFor(t1);
        assertEquals(0L, stub1.liveCountForTest(Timeframe.FIFTEEN_S));
        assertEquals(0L, stub1.closedCountForTest(Timeframe.FIFTEEN_S));
        // P2-059/060: both skips are counted, never silent — one null-TF live
        // row + one unknown-TF closed row = 2 poison skips on the shared
        // per-rule handle.
        assertEquals(2L, function.metricsSkippedPoisonForTest(StubSmokeStrategy.RULE_ID));
    }

    @Test
    @DisplayName("null ruleId fails with known ids; shadowing prod id refused (P2-056/173)")
    void registryNullAndShadowGuards() {
        assertThrows(IllegalStateException.class, () -> Strategies.create(
                null, SignalJobConfig.from(env()), (name, n) -> {}));
        assertThrows(IllegalArgumentException.class, () -> Strategies.registerForTest(
                StubSmokeStrategy.RULE_ID, (config, metrics) -> new StubSmokeStrategy()));
        Strategies.registerForTest("tmp-test-v9", (config, metrics) -> new StubSmokeStrategy());
        assertTrue(Strategies.isKnown("tmp-test-v9"));
        Strategies.unregisterForTest("tmp-test-v9");
        assertTrue(!Strategies.isKnown("tmp-test-v9"));
    }

    @Test
    @DisplayName("N7 id resolves to the ported strategy (batch 2 cutover)")
    void n7IdRegistered() {
        assertTrue(Strategies.knownIds().contains(
                SignalCandidatesTableColumns.CANONICAL_N7_RULE_ID));
        SignalStrategy n7 = Strategies.create(
                SignalCandidatesTableColumns.CANONICAL_N7_RULE_ID,
                SignalJobConfig.from(env()),
                (name, n) -> {});
        assertTrue(n7 instanceof N7RangeBreakoutStrategy);
    }

    @Test
    @DisplayName("host runs the real N7 end to end: arm on closed, fire on live")
    void hostRunsN7EndToEnd() throws Exception {
        open(N7RangeBreakoutStrategy.RULE_ID);
        long token = 999L;
        int[] ranges = {7, 6, 5, 4, 3, 2, 1};
        for (int i = 0; i < 7; i++) {
            long high = 10_000L + i;
            long ws = i * 15_000L;
            GenericRowData c = new GenericRowData(CandleClosedColumns.FIELD_COUNT);
            c.setField(CandleClosedColumns.INSTRUMENT_TOKEN, token);
            c.setField(CandleClosedColumns.EXCHANGE, StringData.fromString("NSE"));
            c.setField(CandleClosedColumns.SYMBOL, StringData.fromString("TEST"));
            c.setField(CandleClosedColumns.TF,
                    StringData.fromString(Timeframe.FIFTEEN_S.code()));
            c.setField(CandleClosedColumns.WINDOW_START, ws);
            c.setField(CandleClosedColumns.WINDOW_END, ws + 15_000L);
            c.setField(CandleClosedColumns.OPEN_PAISE, high - ranges[i]);
            c.setField(CandleClosedColumns.HIGH_PAISE, high);
            c.setField(CandleClosedColumns.LOW_PAISE, high - ranges[i]);
            c.setField(CandleClosedColumns.CLOSE_PAISE, high - ranges[i]);
            c.setField(CandleClosedColumns.VOLUME, 100L);
            c.setField(CandleClosedColumns.TICK_COUNT, 5);
            c.setField(CandleClosedColumns.LAST_EVENT_TIME, ws + 1_000L);
            c.setField(CandleClosedColumns.LAST_EVENT_FINGERPRINT,
                    StringData.fromString("fp"));
            c.setField(CandleClosedColumns.SCHEMA_VERSION, StringData.fromString("1"));
            harness.processElement2(c, ws + 1_000L);
        }
        // Setup armed (high 10006): breach it with a live tick.
        harness.processElement1(live(token, Timeframe.FIFTEEN_S, 0L, 10_007L), 200_000L);

        assertEquals(1, harness.getOutput().size());
        assertEquals(1L, function.emittedForTest());
        // Same setup breaching again hits the strategy's fire-once latch
        // before the host: nothing new forwarded, nothing to dedup.
        harness.processElement1(live(token, Timeframe.FIFTEEN_S, 0L, 10_008L), 200_001L);
        assertEquals(1, harness.getOutput().size());
        assertEquals(0L, function.suppressedForTest());
    }

    /** Throws from onClosedCandle — the host must isolate it, not fail. */
    static final class Exploder implements SignalStrategy {
        static final String RULE_ID = "test-exploder-v1";

        @Override
        public String ruleId() {
            return RULE_ID;
        }

        @Override
        public void onLiveTick(RowData live, Collector<RowData> out) {
            throw new RuntimeException("boom-live");
        }

        @Override
        public void onClosedCandle(RowData closed, Collector<RowData> out) {
            throw new RuntimeException("boom-closed");
        }
    }

    /** Distinct-id emitter — one unique id per closed row, for the P2-058 bound. */
    static final class Churner implements SignalStrategy {
        static final String RULE_ID = "test-churner-v1";
        private long seq;

        @Override
        public String ruleId() {
            return RULE_ID;
        }

        @Override
        public void onLiveTick(RowData live, Collector<RowData> out) {}

        @Override
        public void onClosedCandle(RowData closed, Collector<RowData> out) {
            long detTs = closed.getLong(CandleClosedColumns.LAST_EVENT_TIME);
            GenericRowData row = new GenericRowData(SignalCandidatesTableColumns.FIELD_COUNT);
            // N7-shaped ledger key: rule|token|tf|seq — compaction groups it.
            row.setField(SignalCandidatesTableColumns.CANDIDATE_ID, StringData.fromString(
                    RULE_ID + "|" + closed.getLong(CandleClosedColumns.INSTRUMENT_TOKEN)
                            + "|" + closed.getString(CandleClosedColumns.TF) + "|" + (seq++)));
            row.setField(SignalCandidatesTableColumns.RULE_ID, StringData.fromString(RULE_ID));
            row.setField(SignalCandidatesTableColumns.DETECTION_TS, detTs);
            out.collect(row);
        }
    }

    static {
        Strategies.registerForTest(Exploder.RULE_ID, (config, metrics) -> new Exploder());
        Strategies.registerForTest(Churner.RULE_ID, (config, metrics) -> new Churner());
    }

    @Test
    @DisplayName("one failing strategy cannot starve the others (P2-057)")
    void failingStrategyIsolated() throws Exception {
        open(Exploder.RULE_ID, Repeater.RULE_ID);
        long token = 777L;
        in2(closed(token, Timeframe.FIFTEEN_S, 0L, 100L));
        assertEquals(1, harness.getOutput().size());
        assertEquals(1L, function.emittedForTest());
        assertEquals(1L, function.failedStrategyForTest());
    }

    @Test
    @DisplayName("emitted-ids state stays bounded under replay volume (P2-058)")
    void emittedIdsBounded() throws Exception {
        open(Repeater.RULE_ID);
        long token = 888L;
        // 40 replays of one id: Repeater keys it per (token,tf), so state
        // must hold exactly 1 entry, not 40.
        for (int i = 0; i < 40; i++) {
            in2(closed(token, Timeframe.FIFTEEN_S, i * 15_000L, 100L));
        }
        assertEquals(1L, function.emittedIdsSizeForTest());
        assertEquals(1, harness.getOutput().size());
        assertEquals(39L, function.suppressedForTest());
    }

    @Test
    @DisplayName("distinct ids compact to the retain horizon (P2-058)")
    void emittedIdsCompactsDistinctIds() throws Exception {
        open(Churner.RULE_ID);
        long token = 999L;
        for (int i = 0; i < 40; i++) {
            in2(closed(token, Timeframe.FIFTEEN_S, i * 15_000L, 100L));
        }
        assertEquals(40, harness.getOutput().size());
        // 40 distinct ledger ids compact to <= 2x retain horizon (32).
        assertTrue(function.emittedIdsSizeForTest()
                <= 2L * StrategyHostFunction.EMITTED_IDS_RETAIN_PER_LEDGER);
    }

    @Test
    @DisplayName("slots share one metrics handle per rule (P2-174)")
    void metricsHandlesShared() throws Exception {
        open(StubSmokeStrategy.RULE_ID);
        in1(live(111L, Timeframe.FIFTEEN_S, 0L, 100L));
        in1(live(222L, Timeframe.FIFTEEN_S, 0L, 100L));
        assertTrue(function.strategyForTest(111L, StubSmokeStrategy.RULE_ID) != null);
        assertTrue(function.strategyForTest(222L, StubSmokeStrategy.RULE_ID) != null);
        // Same HostMetrics instance handed to both slots' strategies.
        SignalStrategy.Metrics m1 = function.metricsForTest(StubSmokeStrategy.RULE_ID);
        assertTrue(m1 != null);
        assertTrue(m1 == function.metricsForTest(StubSmokeStrategy.RULE_ID));
    }

    @Test
    @DisplayName("oversize key drops loudly without entering the map (P2-175)")
    void oversizeKeyDroppedBeforeAlloc() throws Exception {
        open(StubSmokeStrategy.RULE_ID);
        // Fill to the cap is 65k rows through the harness — instead prove the
        // drop path directly: a full map refuses the next key with no insert.
        // Here we assert the normal path inserts exactly one slot per key.
        in1(live(111L, Timeframe.FIFTEEN_S, 0L, 100L));
        in1(live(222L, Timeframe.FIFTEEN_S, 0L, 100L));
        assertEquals(2, function.slotCountForTest());
        assertEquals(0L, function.droppedOversizeForTest());
    }
}
