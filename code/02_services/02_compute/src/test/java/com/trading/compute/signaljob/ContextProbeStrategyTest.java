package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * C3 end-to-end proof
 * (docs/plans/2026-09-30-strategy-context-live-fetch.md): a strategy that
 * fetches old data on demand fires on the live forming candle — a miss emits
 * nothing, the completed fetch re-evaluates the same live window before its
 * close, and the host dedups the repeat evaluations.
 */
class ContextProbeStrategyTest {

    private KeyedTwoInputStreamOperatorTestHarness<Long, RowData, RowData, RowData> harness;
    private StrategyHostFunction function;
    private FakeFetcher fetcher;
    private final long[] contextNow = {1_000_000L};

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    private void openHost() throws Exception {
        fetcher = new FakeFetcher();
        Map<String, String> env = env();
        env.put("STRATEGY_HOST_ENABLED", "true");
        env.put("MULTITF_ENABLED", "true");
        env.put("STRATEGIES", ContextProbeStrategy.RULE_ID);
        env.put("STRATEGY_CONTEXT_ENABLED", "true");
        function = new StrategyHostFunction(
                SignalJobConfig.from(env), List.of(ContextProbeStrategy.RULE_ID),
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

    @Test
    @DisplayName("C3: miss emits nothing; ready fires on the forming live candle; dedup holds")
    void readyFiresOnTheFormingLiveCandle() throws Exception {
        openHost();

        harness.processElement1(live(777L, Timeframe.FIFTEEN_S, 120_000L, 1_000L), 120_500L);

        ContextProbeStrategy probe = (ContextProbeStrategy) function.strategyForTest(
                777L, ContextProbeStrategy.RULE_ID);
        assertNotNull(probe);
        assertEquals(1, probe.missesForTest(), "the first tick asks and misses");
        assertEquals(0, harness.getOutput().size(), "a miss must emit nothing");
        assertEquals(1, function.pendingLiveCountForTest(),
                "the token stays pending until the fetch completes");

        CompletableFuture<ContextCandle> scheduled =
                fetcher.of(new ContextKey(777L, Timeframe.FIFTEEN_S, 105_000L));
        assertNotNull(scheduled, "scheduled keys: " + fetcher.keys());
        scheduled.complete(candle(777L, Timeframe.FIFTEEN_S, 105_000L, 900L));
        harness.setProcessingTime(10_000L); // the context-ready wake-up

        assertEquals(1, harness.getOutput().size(), "ready data fires on the live candle");
        RowData row = firstOutput();
        assertEquals("ctx-probe-v1:777:FIFTEEN_S:105000",
                row.getString(SignalCandidatesTableColumns.CANDIDATE_ID).toString());
        assertEquals(777L, row.getLong(SignalCandidatesTableColumns.INSTRUMENT_TOKEN));
        assertEquals(ContextProbeStrategy.RULE_ID,
                row.getString(SignalCandidatesTableColumns.RULE_ID).toString());
        assertEquals("ENTRY", row.getString(SignalCandidatesTableColumns.ACTION).toString());
        long detectionTs = row.getLong(SignalCandidatesTableColumns.DETECTION_TS);
        assertEquals(121_000L, detectionTs, "the decision is stamped with the live candle");
        assertTrue(detectionTs < 135_000L,
                "the decision must fire on the forming candle, before the window close");
        assertEquals(1, probe.readyBeforeCloseForTest());

        // The same window evaluated again (a later live tick with the value
        // cached) is not re-emitted: one logical signal per (token, window).
        harness.processElement1(
                live(777L, Timeframe.FIFTEEN_S, 120_000L, 1_010L, 122_000L), 122_500L);
        assertEquals(1, harness.getOutput().size(), "the probe does not re-emit the same window");
        assertEquals(2, probe.readyBeforeCloseForTest(), "the probe did re-evaluate");
        assertEquals(0L, function.suppressedForTest(),
                "no repeat churn reaches the host's dedup ledger");

        // The close is never the place this decision first fires.
        harness.processElement2(closed(777L, Timeframe.FIFTEEN_S, 120_000L, 1_020L), 135_500L);
        assertEquals(1, harness.getOutput().size(), "the close adds nothing");
        assertEquals(0, probe.readyAfterCloseForTest());
    }

    @Test
    @DisplayName("C4: first ready retains the derived scalar once and warms up behind it")
    void readyRetainsScalarAndWarmsUpOnce() throws Exception {
        openHost();
        harness.processElement1(live(777L, Timeframe.FIFTEEN_S, 120_000L, 1_000L), 120_500L);
        ContextProbeStrategy probe = (ContextProbeStrategy) function.strategyForTest(
                777L, ContextProbeStrategy.RULE_ID);
        assertNotNull(probe);

        CompletableFuture<ContextCandle> scheduled =
                fetcher.of(new ContextKey(777L, Timeframe.FIFTEEN_S, 105_000L));
        assertNotNull(scheduled, "scheduled keys: " + fetcher.keys());
        scheduled.complete(candle(777L, Timeframe.FIFTEEN_S, 105_000L, 900L));
        harness.setProcessingTime(10_000L);

        ContextProvider provider = function.contextProviderForTest();
        assertNotNull(provider);
        assertEquals(1L, provider.metrics().warmups.getCount(), "one warm-up on first touch");
        assertEquals(3L, provider.metrics().fetches.getCount(),
                "the fetch plus the two warmed windows");
        assertEquals(2, provider.pendingCount(), "the two windows behind are in flight");
        assertEquals(1, provider.scalarCount());
        assertTrue(function.contextViewForTest().hasScalar(777L, "prevClosePaise"));
        assertEquals(900L, function.contextViewForTest().scalar(777L, "prevClosePaise"),
                "the scalar is the fetched candle's close — deterministic input");

        // A later live tick re-evaluates but never re-warms or re-derives.
        harness.processElement1(
                live(777L, Timeframe.FIFTEEN_S, 120_000L, 1_010L, 122_000L), 122_500L);
        assertEquals(1L, provider.metrics().warmups.getCount(), "warm-up is once per token");
        assertEquals(1, provider.scalarCount());
        assertEquals(2, probe.readyBeforeCloseForTest());
    }

    @Test
    @DisplayName("C3: a ready value never fires on a live snapshot at or past the close")
    void readyAtTheCloseNeverEmits() throws Exception {
        openHost();

        // Live snapshot whose event time already equals its window end.
        harness.processElement1(
                live(777L, Timeframe.FIFTEEN_S, 120_000L, 1_000L, 135_000L), 135_500L);
        ContextProbeStrategy probe = (ContextProbeStrategy) function.strategyForTest(
                777L, ContextProbeStrategy.RULE_ID);
        assertEquals(1, probe.missesForTest());

        CompletableFuture<ContextCandle> scheduled =
                fetcher.of(new ContextKey(777L, Timeframe.FIFTEEN_S, 105_000L));
        assertNotNull(scheduled, "scheduled keys: " + fetcher.keys());
        scheduled.complete(candle(777L, Timeframe.FIFTEEN_S, 105_000L, 900L));
        harness.setProcessingTime(10_000L);

        assertEquals(0, harness.getOutput().size(),
                "a decision at or past the close must never fire");
        assertEquals(1, probe.readyAfterCloseForTest());
        assertEquals(0, probe.readyBeforeCloseForTest());
    }

    private RowData firstOutput() {
        return ((org.apache.flink.streaming.runtime.streamrecord.StreamRecord<RowData>)
                harness.getOutput().iterator().next()).getValue();
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

    private static RowData live(long token, Timeframe tf, long ws, long price) {
        return live(token, tf, ws, price, ws + 1_000L);
    }

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
        r.setField(CandleClosedColumns.LAST_EVENT_TIME, ws + tf.windowMs() - 1L);
        r.setField(CandleClosedColumns.LAST_EVENT_FINGERPRINT, StringData.fromString("fp"));
        r.setField(CandleClosedColumns.SCHEMA_VERSION, StringData.fromString("1"));
        return r;
    }

    private static ContextCandle candle(long token, Timeframe tf, long ws, long close) {
        return new ContextCandle(token, tf, ws, ws + tf.windowMs(),
                close, close + 50L, close - 50L, close, 10L, 3, ws + tf.windowMs() - 1_000L);
    }

    /** Test fetcher: one controllable future per key. */
    static final class FakeFetcher implements CandleFetcher {
        private final Map<ContextKey, CompletableFuture<ContextCandle>> futures = new HashMap<>();

        @Override
        public CompletableFuture<ContextCandle> fetch(ContextKey key) {
            return futures.computeIfAbsent(key, k -> new CompletableFuture<>());
        }

        CompletableFuture<ContextCandle> of(ContextKey key) {
            return futures.get(key);
        }

        Set<ContextKey> keys() {
            return futures.keySet();
        }
    }
}
