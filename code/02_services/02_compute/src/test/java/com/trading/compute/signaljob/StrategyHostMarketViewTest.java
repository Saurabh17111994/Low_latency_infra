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
        StrategyView lastView;
        long lastClosedBidPx1 = -1L;
        long lastReadyBidPx1 = -1L;

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
    }
}
