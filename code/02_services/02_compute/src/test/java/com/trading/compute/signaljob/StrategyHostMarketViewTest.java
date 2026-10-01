package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Host wiring for the native market view (2026-10-01): the canonical forming
 * row refreshes the per-instrument {@link MarketSnapshot}, a non-canonical row
 * never touches it, and every strategy callback — live, closed, context-ready —
 * receives the same stable {@link StrategyView} bundle.
 */
@DisplayName("StrategyHost: market view decode + delivery on every callback")
class StrategyHostMarketViewTest {

    private static final long TOKEN = 9L;

    private KeyedTwoInputStreamOperatorTestHarness<Long, RowData, RowData, RowData> harness;
    private StrategyHostFunction function;
    private StrategyHostFunctionTest.FakeFetcher fetcher;
    private final long[] contextNow = {1_000_000L};

    @BeforeEach
    void registerProbe() {
        Strategies.registerForTest(ViewProbe.RULE_ID, (config, metrics) -> new ViewProbe(true));
    }

    @AfterEach
    void tearDown() throws Exception {
        Strategies.unregisterForTest(ViewProbe.RULE_ID);
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
        env.put("STRATEGY_HOST_ENABLED", "true");
        env.put("MULTITF_ENABLED", "true");
        env.put("STRATEGIES", ViewProbe.RULE_ID);
        env.put("STRATEGY_CONTEXT_ENABLED", "true");
        return env;
    }

    private void open() throws Exception {
        fetcher = new StrategyHostFunctionTest.FakeFetcher();
        function = new StrategyHostFunction(
                SignalJobConfig.from(env()), List.of(ViewProbe.RULE_ID),
                (config, metrics) -> new ContextProvider(
                        fetcher,
                        config.contextCacheBytes(),
                        config.contextMaxInflight(),
                        config.contextFetchTimeoutMs(),
                        config.contextRetryCooldownMs(),
                        () -> contextNow[0],
                        metrics));
        harness = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
                function,
                r -> r.getLong(CandleLiveColumns.INSTRUMENT_TOKEN),
                r -> r.getLong(CandleClosedColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();
    }

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
        r.setField(CandleLiveColumns.VOLUME, 10L);
        r.setField(CandleLiveColumns.TICK_COUNT, 1);
        r.setField(CandleLiveColumns.LAST_EVENT_TIME, ws + 1_000L);
        r.setField(CandleLiveColumns.LAST_EVENT_FINGERPRINT, StringData.fromString("fp"));
        r.setField(CandleLiveColumns.SCHEMA_VERSION, StringData.fromString("1"));
        return r;
    }

    /** A full market section with a walkable ladder: 500 buy fills at avg 10006 (1 paise slippage). */
    private static RowData withMarket(RowData row) {
        GenericRowData r = (GenericRowData) row;
        r.setField(CandleLiveColumns.MKT_VWAP_PAISE, 12_345L);
        r.setField(CandleLiveColumns.MKT_BID_PX_1, 10_000L);
        r.setField(CandleLiveColumns.MKT_BID_QTY_1, 100L);
        r.setField(CandleLiveColumns.MKT_BID_PX_2, 9_999L);
        r.setField(CandleLiveColumns.MKT_BID_QTY_2, 400L);
        r.setField(CandleLiveColumns.MKT_ASK_PX_1, 10_005L);
        r.setField(CandleLiveColumns.MKT_ASK_QTY_1, 200L);
        r.setField(CandleLiveColumns.MKT_ASK_PX_2, 10_006L);
        r.setField(CandleLiveColumns.MKT_ASK_QTY_2, 150L);
        r.setField(CandleLiveColumns.MKT_ASK_PX_3, 10_007L);
        r.setField(CandleLiveColumns.MKT_ASK_QTY_3, 300L);
        r.setField(CandleLiveColumns.MKT_STATS_CHANGED_AT, 5_000L);
        r.setField(CandleLiveColumns.MKT_DEPTH_CHANGED_AT, 5_001L);
        return r;
    }

    private static RowData closed(long token, String tfCode, long ws, long close) {
        GenericRowData r = new GenericRowData(CandleClosedColumns.FIELD_COUNT);
        r.setField(CandleClosedColumns.INSTRUMENT_TOKEN, token);
        r.setField(CandleClosedColumns.EXCHANGE, StringData.fromString("NSE"));
        r.setField(CandleClosedColumns.SYMBOL, StringData.fromString("TEST"));
        r.setField(CandleClosedColumns.TF, StringData.fromString(tfCode));
        r.setField(CandleClosedColumns.WINDOW_START, ws);
        r.setField(CandleClosedColumns.WINDOW_END, ws + 60_000L);
        r.setField(CandleClosedColumns.OPEN_PAISE, close);
        r.setField(CandleClosedColumns.HIGH_PAISE, close);
        r.setField(CandleClosedColumns.LOW_PAISE, close);
        r.setField(CandleClosedColumns.CLOSE_PAISE, close);
        r.setField(CandleClosedColumns.VOLUME, 10L);
        r.setField(CandleClosedColumns.TICK_COUNT, 1);
        r.setField(CandleClosedColumns.LAST_EVENT_TIME, ws + 1_000L);
        r.setField(CandleClosedColumns.LAST_EVENT_FINGERPRINT, StringData.fromString("fp"));
        r.setField(CandleClosedColumns.SCHEMA_VERSION, StringData.fromString("1"));
        return r;
    }

    /**
     * The pre-market-section 16-column live row (2026-10-02): exactly what an
     * unaligned checkpoint taken by a pre-CHG-501 job replays as an in-flight
     * record. Same prefix, no market section.
     */
    private static RowData legacyLive(long token, Timeframe tf, long ws, long price) {
        GenericRowData r = new GenericRowData(CandleLiveColumns.MARKET_SECTION_START);
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
        r.setField(CandleLiveColumns.VOLUME, 10L);
        r.setField(CandleLiveColumns.TICK_COUNT, 1);
        r.setField(CandleLiveColumns.LAST_EVENT_TIME, ws + 1_000L);
        r.setField(CandleLiveColumns.LAST_EVENT_FINGERPRINT, StringData.fromString("fp"));
        r.setField(CandleLiveColumns.SCHEMA_VERSION, StringData.fromString("1"));
        r.setField(CandleLiveColumns.INGEST_TS, 0L);
        return r;
    }

    /** A TF=MKT row as the aggregator emits it (CHG-505): identity + market, no candle. */
    private static RowData marketTick(long token, long bidPx1, long statsChangedAt,
            long depthChangedAt) {
        GenericRowData r = new GenericRowData(CandleLiveColumns.FIELD_COUNT);
        r.setField(CandleLiveColumns.INSTRUMENT_TOKEN, token);
        r.setField(CandleLiveColumns.EXCHANGE, StringData.fromString("NSE"));
        r.setField(CandleLiveColumns.SYMBOL, StringData.fromString("TEST"));
        r.setField(CandleLiveColumns.TF, StringData.fromString(CandleLiveColumns.TF_MARKET_TICK));
        r.setField(CandleLiveColumns.WINDOW_START, 7_000L);
        r.setField(CandleLiveColumns.WINDOW_END, 7_000L);
        r.setField(CandleLiveColumns.OPEN_PAISE, 0L);
        r.setField(CandleLiveColumns.HIGH_PAISE, 0L);
        r.setField(CandleLiveColumns.LOW_PAISE, 0L);
        r.setField(CandleLiveColumns.CLOSE_PAISE, 0L);
        r.setField(CandleLiveColumns.VOLUME, 0L);
        r.setField(CandleLiveColumns.TICK_COUNT, 0);
        r.setField(CandleLiveColumns.LAST_EVENT_TIME, 7_000L);
        r.setField(CandleLiveColumns.SCHEMA_VERSION, StringData.fromString("1"));
        r.setField(CandleLiveColumns.INGEST_TS, 0L);
        r.setField(CandleLiveColumns.MKT_BID_PX_1, bidPx1);
        r.setField(CandleLiveColumns.MKT_BID_QTY_1, 10L);
        r.setField(CandleLiveColumns.MKT_STATS_CHANGED_AT, statsChangedAt);
        r.setField(CandleLiveColumns.MKT_DEPTH_CHANGED_AT, depthChangedAt);
        return r;
    }

    private ViewProbe probe() {
        return (ViewProbe) function.strategyForTest(TOKEN, ViewProbe.RULE_ID);
    }

    @Test
    void decodeReadsEveryColumnIncludingBothClocks() {
        GenericRowData r = new GenericRowData(CandleLiveColumns.FIELD_COUNT);
        for (int i = 0; i < CandleLiveColumns.MARKET_FIELD_COUNT; i++) {
            r.setField(CandleLiveColumns.MARKET_SECTION_START + i, 10_000L + i);
        }
        MarketSnapshot m = new MarketSnapshot();
        StrategyHostFunction.decodeMarketSnapshot(m, r);

        // 12 stats, section offsets 0..11
        assertEquals(10_000L, m.totalBuyQty());
        assertEquals(10_001L, m.totalSellQty());
        assertEquals(10_002L, m.dayOpenPaise());
        assertEquals(10_003L, m.dayHighPaise());
        assertEquals(10_004L, m.dayLowPaise());
        assertEquals(10_005L, m.prevClosePaise());
        assertEquals(10_006L, m.vwapPaise());
        assertEquals(10_007L, m.openInterest());
        assertEquals(10_008L, m.oiDayHigh());
        assertEquals(10_009L, m.oiDayLow());
        assertEquals(10_010L, m.lowerLimitPaise());
        assertEquals(10_011L, m.upperLimitPaise());
        // 30 depth values, section offsets 12..41 (attribute-major per side)
        assertEquals(10_012L, m.bidPxPaise(1));
        assertEquals(10_016L, m.bidPxPaise(5));
        assertEquals(10_017L, m.bidQty(1));
        assertEquals(10_021L, m.bidQty(5));
        assertEquals(10_022L, m.bidOrd(1));
        assertEquals(10_026L, m.bidOrd(5));
        assertEquals(10_027L, m.askPxPaise(1));
        assertEquals(10_031L, m.askPxPaise(5));
        assertEquals(10_032L, m.askQty(1));
        assertEquals(10_036L, m.askQty(5));
        assertEquals(10_037L, m.askOrd(1));
        assertEquals(10_041L, m.askOrd(5));
        // clocks, section offsets 42..43
        assertEquals(10_042L, m.statsChangedAt());
        assertEquals(10_043L, m.depthChangedAt());

        // NULL columns decode as "not provided" (0), never as garbage.
        MarketSnapshot empty = new MarketSnapshot();
        StrategyHostFunction.decodeMarketSnapshot(empty, new GenericRowData(CandleLiveColumns.FIELD_COUNT));
        assertEquals(0L, empty.vwapPaise());
        assertEquals(0L, empty.bidPxPaise(3));
        assertEquals(0L, empty.statsChangedAt());
        assertEquals(0L, empty.depthChangedAt());
        assertFalse(empty.hasDepth());
    }

    @Test
    @DisplayName("a pre-market-section 16-column row decodes nothing and never crashes")
    void legacyRowSkipsMarketDecode() {
        GenericRowData legacy =
                (GenericRowData) legacyLive(TOKEN, Timeframe.FIFTEEN_S, 120_000L, 1_000L);
        MarketSnapshot m = new MarketSnapshot();
        m.vwapPaise = 777L; // a value already in the snapshot must survive the skip

        assertFalse(StrategyHostFunction.decodeMarketSnapshot(m, legacy));
        assertEquals(777L, m.vwapPaise(), "a legacy row must not wipe the snapshot");
        assertFalse(m.hasDepth());
    }

    @Test
    @DisplayName("canonical row fills the view; non-canonical rows never touch it; closed sees the same snapshot")
    void canonicalRowFillsViewForLiveAndClosedCallbacks() throws Exception {
        open();
        long ws = 120_000L;
        harness.processElement1(withMarket(live(TOKEN, Timeframe.FIFTEEN_S, ws, 1_000L)), ws + 1_000L);

        ViewProbe probe = probe();
        assertEquals(1, probe.liveCalls);
        StrategyView view = probe.lastView;
        assertNotNull(view);
        assertEquals(10_000L, view.market().bidPxPaise(1));
        assertEquals(1L, view.market().slippagePaise(true, 500L));
        assertEquals(5_001L, view.market().depthChangedAt());
        assertEquals(12_345L, view.market().vwapPaise());
        assertTrue(view.market().hasDepth());
        assertNotNull(view.features());
        assertNotNull(view.context());

        // A non-canonical row carries no market section: the view must keep
        // the last canonical snapshot (decode is canonical-row only).
        harness.processElement1(live(TOKEN, Timeframe.THIRTY_S, ws, 1_000L), ws + 1_001L);
        assertEquals(2, probe.liveCalls);
        assertSame(view, probe.lastView, "the bundle is stable per slot");
        assertEquals(10_000L, probe.lastView.market().bidPxPaise(1), "non-canonical row must not wipe the snapshot");

        // The close path sees the same market snapshot.
        harness.processElement2(closed(TOKEN, Timeframe.ONE_M.code(), 60_000L, 1_001L), ws + 2_000L);
        assertEquals(1, probe.closeCalls);
        assertSame(view, probe.lastView);
        assertEquals(10_000L, probe.lastClosedBidPx1);
        assertEquals(5_001L, probe.lastView.market().depthChangedAt());
    }

    @Test
    @DisplayName("a legacy canonical row still fans out and is counted; the next full row refreshes")
    void legacyCanonicalRowFansOutAndIsCounted() throws Exception {
        open();
        long ws = 120_000L;
        harness.processElement1(legacyLive(TOKEN, Timeframe.FIFTEEN_S, ws, 1_000L), ws + 1_000L);

        ViewProbe probe = probe();
        assertEquals(1, probe.liveCalls, "a legacy row is still a valid candle tick");
        assertEquals(1L, function.legacyMarketRowsForTest(), "the skipped decode is counted");
        assertFalse(probe.lastView.market().hasDepth(), "nothing to decode from a legacy row");

        // The next full row refreshes the snapshot; only the legacy row counted.
        harness.processElement1(
                withMarket(live(TOKEN, Timeframe.FIFTEEN_S, ws, 1_000L)), ws + 2_000L);
        assertEquals(2, probe.liveCalls);
        assertEquals(10_000L, probe.lastView.market().bidPxPaise(1));
        assertEquals(1L, function.legacyMarketRowsForTest());
    }

    @Test
    @DisplayName("the context-ready callback receives the same market view")
    void contextReadyCarriesTheMarketView() throws Exception {
        open();
        long ws = 120_000L;
        // FIFTEEN_S: the canonical row is the market carrier.
        harness.processElement1(withMarket(live(TOKEN, Timeframe.FIFTEEN_S, ws, 1_000L)), ws + 1_000L);

        ViewProbe probe = probe();
        assertEquals(1, probe.liveCalls);
        assertEquals(1, function.pendingLiveCountForTest(), "the context request retained the live snapshot");
        StrategyView liveView = probe.lastView;

        // Complete the fetch the probe scheduled, then fire the wake-up timer.
        fetcher.of(new ContextKey(TOKEN, Timeframe.ONE_M, ViewProbe.WINDOW))
                .complete(new ContextCandle(TOKEN, Timeframe.ONE_M, ViewProbe.WINDOW,
                        ViewProbe.WINDOW + Timeframe.ONE_M.windowMs(), 1_234L, 1_284L, 1_184L,
                        1_234L, 10L, 3, ViewProbe.WINDOW + Timeframe.ONE_M.windowMs() - 1_000L));
        harness.setProcessingTime(10_000L);

        assertEquals(1, probe.readyCalls, "the ready callback fires on the live snapshot");
        assertSame(liveView, probe.lastView, "stable bundle across callbacks");
        assertEquals(10_000L, probe.lastReadyBidPx1, "the ready path reads the same market snapshot");
        assertEquals(5_001L, probe.lastView.market().depthChangedAt());
    }

    @Test
    @DisplayName("a TF=MKT row refreshes the view and calls onMarketUpdate only")
    void marketRowCallsOnMarketUpdateOnly() throws Exception {
        open();
        long ws = 120_000L;
        harness.processElement1(withMarket(live(TOKEN, Timeframe.FIFTEEN_S, ws, 1_000L)), ws + 1_000L);
        ViewProbe probe = probe();
        assertEquals(1, probe.liveCalls);
        StrategyView liveView = probe.lastView;

        harness.processElement1(marketTick(TOKEN, 9_999L, 8_000L, 8_001L), ws + 2_000L);

        assertEquals(1, probe.marketCalls, "exactly one market-update callback");
        assertEquals(1, probe.liveCalls, "a market row must not call onLiveTick");
        assertEquals(0, probe.closeCalls);
        assertEquals(9_999L, probe.lastMarketBidPx1, "the view carries the fresh market");
        assertEquals(9_999L, probe.lastView.market().bidPxPaise(1));
        assertEquals(8_001L, probe.lastView.market().depthChangedAt());
        assertSame(liveView, probe.lastView, "stable bundle across callbacks");
    }

    @Test
    @DisplayName("the default onMarketUpdate is a no-op: existing strategies never see a market row")
    void defaultOnMarketUpdateIsNoOp() throws Exception {
        Strategies.registerForTest(NoMarketProbe.RULE_ID, (config, metrics) -> new NoMarketProbe());
        try {
            StrategyHostFunction fn = new StrategyHostFunction(
                    SignalJobConfig.from(env()), List.of(NoMarketProbe.RULE_ID));
            harness = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
                    fn,
                    r -> r.getLong(CandleLiveColumns.INSTRUMENT_TOKEN),
                    r -> r.getLong(CandleClosedColumns.INSTRUMENT_TOKEN),
                    Types.LONG);
            harness.open();

            harness.processElement1(marketTick(TOKEN, 9_999L, 8_000L, 8_001L), 1_000L);

            NoMarketProbe probe = (NoMarketProbe) fn.strategyForTest(TOKEN, NoMarketProbe.RULE_ID);
            assertEquals(0, probe.liveCalls, "onLiveTick must not fire for a market row");
        } finally {
            Strategies.unregisterForTest(NoMarketProbe.RULE_ID);
        }
    }

    @Test
    @DisplayName("a throwing onMarketUpdate is isolated: the next strategy still gets the row")
    void marketRowFailureIsIsolated() throws Exception {
        Strategies.registerForTest(ThrowingMarketProbe.RULE_ID,
                (config, metrics) -> new ThrowingMarketProbe());
        try {
            StrategyHostFunction fn = new StrategyHostFunction(
                    SignalJobConfig.from(env()),
                    List.of(ThrowingMarketProbe.RULE_ID, ViewProbe.RULE_ID));
            harness = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
                    fn,
                    r -> r.getLong(CandleLiveColumns.INSTRUMENT_TOKEN),
                    r -> r.getLong(CandleClosedColumns.INSTRUMENT_TOKEN),
                    Types.LONG);
            harness.open();

            harness.processElement1(marketTick(TOKEN, 9_999L, 8_000L, 8_001L), 1_000L);

            ViewProbe probe = (ViewProbe) fn.strategyForTest(TOKEN, ViewProbe.RULE_ID);
            assertEquals(1, probe.marketCalls, "the failing rule must not starve the next one");
        } finally {
            Strategies.unregisterForTest(ThrowingMarketProbe.RULE_ID);
        }
    }

    @Test
    @DisplayName("a strategy can fire from onMarketUpdate: the row is forwarded and deduped")
    void marketUpdateCanFireASignal() throws Exception {
        Strategies.registerForTest(FiringMarketProbe.RULE_ID,
                (config, metrics) -> new FiringMarketProbe());
        try {
            StrategyHostFunction fn = new StrategyHostFunction(
                    SignalJobConfig.from(env()), List.of(FiringMarketProbe.RULE_ID));
            harness = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
                    fn,
                    r -> r.getLong(CandleLiveColumns.INSTRUMENT_TOKEN),
                    r -> r.getLong(CandleClosedColumns.INSTRUMENT_TOKEN),
                    Types.LONG);
            harness.open();

            harness.processElement1(marketTick(TOKEN, 9_999L, 8_000L, 8_001L), 1_000L);
            assertEquals(1, harness.getOutput().size(), "the book-move signal is forwarded");
            assertEquals(1L, fn.emittedForTest());
            @SuppressWarnings("unchecked")
            StreamRecord<RowData> rec = (StreamRecord<RowData>) harness.getOutput().peek();
            assertEquals("book-probe-9-9999",
                    rec.getValue().getString(SignalCandidatesTableColumns.CANDIDATE_ID).toString());

            // Same bid again -> same deterministic id -> the host dedups it.
            harness.processElement1(marketTick(TOKEN, 9_999L, 8_000L, 8_001L), 2_000L);
            assertEquals(1, harness.getOutput().size(),
                    "a repeated market row with the same id is deduped");
            assertEquals(1L, fn.suppressedForTest());
        } finally {
            Strategies.unregisterForTest(FiringMarketProbe.RULE_ID);
        }
    }

    /** View-aware probe: records the market on all three callbacks and asks for context once. */
    static final class ViewProbe implements SignalStrategy {
        private static final long serialVersionUID = 1L;
        static final String RULE_ID = "market-view-probe-v1";
        static final long WINDOW = 60_000L;

        private final boolean requestContext;
        private boolean requested;

        int liveCalls;
        int closeCalls;
        int readyCalls;
        int marketCalls;
        StrategyView lastView;
        long lastClosedBidPx1 = -1L;
        long lastReadyBidPx1 = -1L;
        long lastMarketBidPx1 = -1L;

        ViewProbe(boolean requestContext) {
            this.requestContext = requestContext;
        }

        @Override
        public String ruleId() {
            return RULE_ID;
        }

        @Override
        public void onClosedCandle(RowData closed, Collector<RowData> out) {}

        @Override
        public void onLiveTick(RowData live, Collector<RowData> out) {}

        @Override
        public void onLiveTick(RowData live, StrategyView view, Collector<RowData> out) {
            liveCalls++;
            lastView = view;
            if (requestContext && !requested) {
                requested = true;
                view.context().candle(TOKEN, Timeframe.ONE_M, WINDOW);
            }
        }

        @Override
        public void onClosedCandle(RowData closed, StrategyView view, Collector<RowData> out) {
            closeCalls++;
            lastView = view;
            lastClosedBidPx1 = view.market().bidPxPaise(1);
        }

        @Override
        public void onContextReady(RowData live, StrategyView view, Collector<RowData> out) {
            readyCalls++;
            lastView = view;
            lastReadyBidPx1 = view.market().bidPxPaise(1);
        }

        @Override
        public void onMarketUpdate(RowData market, StrategyView view, Collector<RowData> out) {
            marketCalls++;
            lastView = view;
            lastMarketBidPx1 = view.market().bidPxPaise(1);
        }
    }

    /** No onMarketUpdate override: the interface default must be a true no-op. */
    static final class NoMarketProbe implements SignalStrategy {
        private static final long serialVersionUID = 1L;
        static final String RULE_ID = "no-market-probe-v1";

        int liveCalls;

        @Override
        public String ruleId() {
            return RULE_ID;
        }

        @Override
        public void onClosedCandle(RowData closed, Collector<RowData> out) {}

        @Override
        public void onLiveTick(RowData live, Collector<RowData> out) {
            liveCalls++;
        }
    }

    /** Fails every market update: the host must isolate it and keep going. */
    static final class ThrowingMarketProbe implements SignalStrategy {
        private static final long serialVersionUID = 1L;
        static final String RULE_ID = "throwing-market-probe-v1";

        @Override
        public String ruleId() {
            return RULE_ID;
        }

        @Override
        public void onClosedCandle(RowData closed, Collector<RowData> out) {}

        @Override
        public void onLiveTick(RowData live, Collector<RowData> out) {}

        @Override
        public void onMarketUpdate(RowData market, StrategyView view, Collector<RowData> out) {
            throw new IllegalStateException("boom");
        }
    }

    /**
     * Fires one candidate row per distinct best bid from the market callback —
     * the capability the CHG-505 flag unlocks (a signal from a book move).
     */
    static final class FiringMarketProbe implements SignalStrategy {
        private static final long serialVersionUID = 1L;
        static final String RULE_ID = "firing-market-probe-v1";

        @Override
        public String ruleId() {
            return RULE_ID;
        }

        @Override
        public void onClosedCandle(RowData closed, Collector<RowData> out) {}

        @Override
        public void onLiveTick(RowData live, Collector<RowData> out) {}

        @Override
        public void onMarketUpdate(RowData market, StrategyView view, Collector<RowData> out) {
            long token = market.getLong(CandleLiveColumns.INSTRUMENT_TOKEN);
            long bid = view.market().bidPxPaise(1);
            GenericRowData row = new GenericRowData(SignalCandidatesTableColumns.FIELD_COUNT);
            row.setField(SignalCandidatesTableColumns.CANDIDATE_ID,
                    StringData.fromString("book-probe-" + token + "-" + bid));
            row.setField(SignalCandidatesTableColumns.DETECTION_TS, 7_000L);
            out.collect(row);
        }
    }
}
