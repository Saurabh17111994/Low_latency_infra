package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.trading.common.model.FormingBar;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@link FormingBarBuilderFunction} driven through the Flink 2.2.1 operator
 * harness (no cluster) — the REAL production function, real config, real
 * {@code CandleAggregateFunction} accumulation (REQ-FC-002 semantics).
 *
 * <p>Slice 2.2 tests 1/2/8 + the REQ-FC-010 update counter: first tick
 * creates the forming bar (O=H=L=C), subsequent ticks update H/L/C and keep
 * O, volume/tick-count follow the candle aggregation rules, a tick in the
 * NEXT 15-second window starts a fresh bar (the builder never finalizes —
 * the completed-candle pipeline owns finalization), and every accepted tick
 * increments {@code compute.forming.bar.updates}.
 */
class FormingBarBuilderFunctionTest {

    /** Epoch-aligned base: 1_710_000_000_000 / 15000 = 114_000_000 exactly. */
    private static final long T0 = 1_710_000_000_000L;
    private static final long WINDOW_MS = 15_000L;

    private KeyedOneInputStreamOperatorTestHarness<Long, RowData, FormingBar> harness;
    private FormingBarBuilderFunction function;

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    private void openHarness() throws Exception {
        function = new FormingBarBuilderFunction(SignalJobConfig.from(env()));
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                function,
                row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
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
        return env;
    }

    private void process(RowData tick) throws Exception {
        harness.processElement(tick, tick.getLong(RawTableColumns.EVENT_TIME));
    }

    private static List<FormingBar> emittedBars(
            KeyedOneInputStreamOperatorTestHarness<Long, RowData, FormingBar> h) {
        return h.getOutput().stream()
                .filter(o -> o instanceof StreamRecord)
                .map(o -> (FormingBar) ((StreamRecord<?>) o).getValue())
                .toList();
    }

    private static long updateCount(FormingBarBuilderFunction fn) throws Exception {
        // No metric-group read-back can observe the increments: the Flink 2.2.1
        // harness opens the user function TWICE (once at construction, once at
        // harness.open()), and MetricGroup.counter(name) returns a FRESH
        // counter on name collision while the group keeps the FIRST
        // registration in its private map — so the map holds the stale
        // pre-second-open counter and every read-back via counter(name) sees
        // an unrelated counter at 0 (verified empirically 2026-08-16). The
        // function's own updateCounter field IS the object it increments per
        // accepted tick, so read it directly.
        Field counterField = FormingBarBuilderFunction.class.getDeclaredField("updateCounter");
        counterField.setAccessible(true);
        return ((Counter) counterField.get(fn)).getCount();
    }

    @Test
    void firstTickCreatesFormingBar() throws Exception {
        openHarness();
        process(TestRawRows.row(7L, T0 + 1_000L, "fp-1", "TRADE", 100, 5));

        List<FormingBar> bars = emittedBars(harness);
        assertEquals(1, bars.size(), "one event per accepted tick");
        FormingBar bar = bars.get(0);
        assertEquals(7L, bar.instrumentToken());
        assertEquals(T0, bar.windowStart());
        assertEquals(T0 + WINDOW_MS, bar.windowEnd());
        assertEquals(100, bar.openPaise());
        assertEquals(100, bar.highPaise());
        assertEquals(100, bar.lowPaise());
        assertEquals(100, bar.closePaise());
        assertEquals(5, bar.volume(), "TRADE with last_qty>0 contributes volume");
        assertEquals(1, bar.tickCount());
        assertEquals(T0 + 1_000L, bar.lastEventTime());
        assertEquals("fp-1", bar.lastFingerprint());
        assertEquals("NSE", bar.exchange());
        assertEquals("TEST", bar.symbol());
        assertEquals(1L, updateCount(function));
    }

    @Test
    void subsequentTicksUpdateHighLowCloseKeepOpen() throws Exception {
        openHarness();
        process(TestRawRows.row(7L, T0 + 1_000L, "fp-1", "TRADE", 100, 5));
        process(TestRawRows.row(7L, T0 + 4_000L, "fp-2", "TRADE", 102, 2));
        process(TestRawRows.row(7L, T0 + 7_000L, "fp-3", "TRADE", 99, 3));

        List<FormingBar> bars = emittedBars(harness);
        assertEquals(3, bars.size(), "one event per tick — live snapshots, never retained");
        FormingBar last = bars.get(2);
        assertEquals(100, last.openPaise(), "open stays the first tick's price");
        assertEquals(102, last.highPaise(), "high becomes the maximum");
        assertEquals(99, last.lowPaise(), "low becomes the minimum");
        assertEquals(99, last.closePaise(), "close becomes the latest tick's price");
        assertEquals(10, last.volume(), "volume accumulates across TRADE rows");
        assertEquals(3, last.tickCount());
        // All three events share the same window identity.
        assertEquals(T0, bars.get(0).windowStart());
        assertEquals(T0, bars.get(1).windowStart());
        assertEquals(T0, bars.get(2).windowStart());
        assertEquals(3L, updateCount(function));
    }

    @Test
    void quoteRowsUpdateOhlcButNotVolume() throws Exception {
        openHarness();
        process(TestRawRows.row(7L, T0 + 1_000L, "fp-1", "TRADE", 100, 5));
        process(TestRawRows.row(7L, T0 + 2_000L, "fp-2", "QUOTE", 105, 0));

        List<FormingBar> bars = emittedBars(harness);
        assertEquals(2, bars.size());
        assertEquals(105, bars.get(1).highPaise(), "quote row contributes OHLC via last price");
        assertEquals(5, bars.get(1).volume(), "quote rows contribute no volume");
        assertEquals(1, bars.get(1).tickCount(), "quote rows contribute no tick count");
    }

    @Test
    void windowTransitionStartsFreshBar() throws Exception {
        openHarness();
        // Last tick of window [T0, T0+15s): 10:00:14.900 → 14_900 ms offset.
        process(TestRawRows.row(7L, T0 + 14_900L, "fp-1", "TRADE", 100, 5));
        // First tick of the next window: 10:00:15.100 → offset 15_100.
        process(TestRawRows.row(7L, T0 + 15_100L, "fp-2", "TRADE", 200, 1));

        List<FormingBar> bars = emittedBars(harness);
        assertEquals(2, bars.size());
        assertEquals(T0, bars.get(0).windowStart());
        assertEquals(T0 + WINDOW_MS, bars.get(1).windowStart(),
                "new window starts at the next aligned boundary");
        assertEquals(200, bars.get(1).openPaise(),
                "new bar's open is the first tick of the new window");
        assertEquals(200, bars.get(1).highPaise());
        assertEquals(200, bars.get(1).lowPaise());
        assertEquals(200, bars.get(1).closePaise());
        assertEquals(1, bars.get(1).volume(), "new bar accumulates fresh, not carry-over");
        assertEquals(1, bars.get(1).tickCount());
    }

    @Test
    void builderNeverFinalizesOldBar() throws Exception {
        openHarness();
        process(TestRawRows.row(7L, T0 + 14_900L, "fp-1", "TRADE", 100, 5));
        process(TestRawRows.row(7L, T0 + 15_100L, "fp-2", "TRADE", 200, 1));

        // The builder's output is ONLY live FormingBar snapshots — exactly two
        // events for two ticks. No candle row, no finalization event, no
        // second artifact for the closed window (the window operator owns that).
        List<FormingBar> bars = emittedBars(harness);
        assertEquals(2, bars.size());
        assertNotNull(bars.get(0));
    }

    @Test
    void persistSideOutputCarriesEverySnapshot() throws Exception {
        openHarness();
        process(TestRawRows.row(7L, T0 + 1_000L, "fp-1", "TRADE", 100, 5));
        process(TestRawRows.row(7L, T0 + 4_000L, "fp-2", "TRADE", 102, 2));
        process(TestRawRows.row(8L, T0 + 2_000L, "fp-3", "TRADE", 50, 1));

        // The persistence leg sees the SAME snapshots as the Business Logic
        // stream — the writer coalesces, so this is never a per-tick Fluss
        // write, but the side output must carry every update (the durable
        // current-state must converge to the latest bar).
        List<FormingBar> persisted = harness.getSideOutput(
                        FormingBarBuilderFunction.PERSIST_OUTPUT)
                .stream()
                .filter(o -> o instanceof StreamRecord)
                .map(o -> (FormingBar) ((StreamRecord<?>) o).getValue())
                .toList();
        assertEquals(3, persisted.size(), "one persist snapshot per accepted tick");
        assertEquals(T0, persisted.get(0).windowStart());
        assertEquals(102, persisted.get(1).closePaise(), "same latest-bar semantics as main output");
        assertEquals(8L, persisted.get(2).instrumentToken());
    }
}
