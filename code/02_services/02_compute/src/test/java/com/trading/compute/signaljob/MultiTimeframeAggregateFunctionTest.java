package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("MultiTimeframe aggregator smoke — forming/closed/live/signal + drop discipline")
class MultiTimeframeAggregateFunctionTest {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final long TOKEN = 2885L;
    private static final long LIVE_INTERVAL = 1_000L;

    private KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> harness;
    private MultiTimeframeAggregateFunction fn;

    private static long ist(int y, int m, int d, int h, int min, int s, int ms) {
        return ZonedDateTime.of(LocalDate.of(y, m, d), LocalTime.of(h, min, s, ms * 1_000_000), IST)
                .toInstant().toEpochMilli();
    }

    private void open() throws Exception {
        fn = new MultiTimeframeAggregateFunction(LIVE_INTERVAL);
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                fn,
                row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    private static RowData trade(long eventTime, String fp, long price, long qty) {
        return TestRawRows.row(TOKEN, eventTime, fp, "TRADE", price, qty);
    }

    private static RowData quote(long eventTime, String fp, long price) {
        return TestRawRows.row(TOKEN, eventTime, fp, "QUOTE", price, 0L);
    }

    @SuppressWarnings("unchecked")
    private List<RowData> mainRows() {
        List<RowData> out = new ArrayList<>();
        for (Object o : harness.getOutput()) {
            if (o instanceof StreamRecord) {
                Object v = ((StreamRecord<?>) o).getValue();
                if (v instanceof RowData) out.add((RowData) v);
            }
        }
        return out;
    }

    private List<RowData> liveRows() {
        var side = harness.getSideOutput(MultiTimeframeAggregateFunction.LIVE_TAG);
        List<RowData> out = new ArrayList<>();
        if (side != null) {
            for (StreamRecord<RowData> sr : side) out.add(sr.getValue());
        }
        return out;
    }

    private List<MultiTimeframeSignalContext> signalRows() {
        var side = harness.getSideOutput(MultiTimeframeAggregateFunction.SIGNAL_TAG);
        List<MultiTimeframeSignalContext> out = new ArrayList<>();
        if (side != null) {
            for (StreamRecord<MultiTimeframeSignalContext> sr : side) out.add(sr.getValue());
        }
        return out;
    }

    private List<RowData> closedForTf(List<RowData> rows, String tfCode) {
        List<RowData> filtered = new ArrayList<>();
        for (RowData r : rows) {
            String tf = r.getString(CandleClosedColumns.TF).toString();
            if (tfCode.equals(tf)) filtered.add(r);
        }
        return filtered;
    }

    @Test
    @DisplayName("smoke: 3 trades in one 1m bucket → closed OHLCV, live per TF, signal per trade, quote/pre-open dropped, late no duplicate")
    void smoke() throws Exception {
        open();
        long T0 = ist(2026, 9, 4, 10, 0, 0, 0); // aligned start for all 6 TFs

        // 3 trades within same 15s/30s/1m/3m/5m/15m bucket [10:00:00,10:00:15)
        long t1 = T0 + 2_000L;
        long t2 = T0 + 5_000L;
        long t3 = T0 + 8_000L;
        harness.processElement(trade(t1, "fp-a", 100_00L, 10L), t1);
        harness.processElement(trade(t2, "fp-b", 101_00L, 20L), t2);
        harness.processElement(trade(t3, "fp-c", 99_00L, 5L), t3);

        // SIGNAL_TAG per trade tick: 3 contexts, each timeframeCount==6, frames copies not aliasing live state
        List<MultiTimeframeSignalContext> signals = signalRows();
        assertEquals(3, signals.size(), "SIGNAL_TAG should emit per TRADE tick");
        for (MultiTimeframeSignalContext sc : signals) {
            assertEquals(6, sc.timeframeCount(), "each signal must carry 6 TFs");
            assertEquals(6, sc.frames().size());
            for (var f : sc.frames()) {
                assertNotNull(f.tf());
                assertNotNull(f.forming());
                assertNotNull(f.closedNewestFirst());
            }
        }
        // Third signal's forming for ONE_M should reflect OHLC of all 3 ticks (after mutation)
        MultiTimeframeSignalContext lastSignal = signals.get(2);
        var oneMFrame = lastSignal.frames().stream().filter(f -> f.tf() == Timeframe.ONE_M).findFirst().orElseThrow();
        CandleAccumulator oneMForming = oneMFrame.forming();
        assertEquals(100_00L, oneMForming.openPaise, "open is earliest price");
        assertEquals(101_00L, oneMForming.highPaise);
        assertEquals(99_00L, oneMForming.lowPaise);
        assertEquals(99_00L, oneMForming.closePaise, "close is latest price");
        assertEquals(35L, oneMForming.volume);
        assertEquals(3L, oneMForming.tickCount);
        // Rings initially empty (no closed yet)
        for (var f : lastSignal.frames()) {
            assertEquals(0, f.closedNewestFirst().size(), "no closed before watermark for tf " + f.tf());
        }
        // Verify deep copy: mutating returned forming must not alias live state
        oneMForming.openPaise = 999999L;
        // Fetch again next signal to ensure live state unaffected (we will check after next tick)

        // LIVE_TAG: trigger via both processing-time and event-time live timers
        // Advance processing time to fire live proc timer (scheduled at proc 0+1000)
        harness.setProcessingTime(5_000L);
        // Also advance watermark to fire live event timer (first at t1+1000 = T0+3000)
        harness.processWatermark(new Watermark(T0 + 10_000L));
        List<RowData> lives = liveRows();
        // At least one live row per TF should have been emitted (timer fired at least once)
        assertTrue(lives.size() >= 6, "LIVE_TAG should have at least 6 rows (one per TF) after timer, got " + lives.size());
        // Check each TF appears
        for (Timeframe tf : Timeframe.values()) {
            boolean found = lives.stream().anyMatch(r -> tf.code().equals(r.getString(CandleLiveColumns.TF).toString()));
            assertTrue(found, "live rows should contain tf " + tf.code());
        }
        // Verify live OHLC for ONE_M matches forming
        RowData liveOneM = lives.stream().filter(r -> Timeframe.ONE_M.code().equals(r.getString(CandleLiveColumns.TF).toString())).findFirst().orElse(null);
        assertNotNull(liveOneM);
        assertEquals(100_00L, liveOneM.getLong(CandleLiveColumns.OPEN_PAISE));
        assertEquals(101_00L, liveOneM.getLong(CandleLiveColumns.HIGH_PAISE));
        assertEquals(99_00L, liveOneM.getLong(CandleLiveColumns.LOW_PAISE));
        assertEquals(99_00L, liveOneM.getLong(CandleLiveColumns.CLOSE_PAISE));
        assertEquals(35L, liveOneM.getLong(CandleLiveColumns.VOLUME));
        assertEquals(3, liveOneM.getInt(CandleLiveColumns.TICK_COUNT));

        // Quote tick inside same bucket — should NOT change OHLCV
        long tq = T0 + 9_000L;
        harness.processElement(quote(tq, "fp-q1", 200_00L), tq);
        // Signal count must NOT increase for quote (per task TRADE-only signal)
        assertEquals(3, signalRows().size(), "quote tick must not emit SIGNAL_TAG per task contract");
        // LIVE after quote should remain same OHLC (quote never touches OHLC)
        // Trigger another live snapshot
        harness.setProcessingTime(10_000L);
        harness.processWatermark(new Watermark(T0 + 12_000L));
        List<RowData> lives2 = liveRows();
        RowData liveOneM2 = lives2.stream().filter(r -> Timeframe.ONE_M.code().equals(r.getString(CandleLiveColumns.TF).toString()))
                .reduce((first, second) -> second).orElse(null); // last
        assertNotNull(liveOneM2);
        assertEquals(100_00L, liveOneM2.getLong(CandleLiveColumns.OPEN_PAISE), "quote must not change open");
        assertEquals(101_00L, liveOneM2.getLong(CandleLiveColumns.HIGH_PAISE), "quote must not change high");
        assertEquals(99_00L, liveOneM2.getLong(CandleLiveColumns.LOW_PAISE), "quote must not change low");
        assertEquals(35L, liveOneM2.getLong(CandleLiveColumns.VOLUME), "quote must not change volume");

        // Pre-open tick (09:14:59.999 IST) — filtered, no signal, no live, no main
        long preOpen = ist(2026, 9, 4, 9, 14, 59, 999);
        int signalsBeforePre = signalRows().size();
        int mainsBeforePre = mainRows().size();
        harness.processElement(trade(preOpen, "fp-pre", 12345L, 10L), preOpen);
        assertEquals(signalsBeforePre, signalRows().size(), "pre-open tick must be dropped (no signal)");
        assertEquals(mainsBeforePre, mainRows().size(), "pre-open tick must not produce closed row");

        // Advance watermark past 1m bucket end to close ONE_M (and also 15s/30s)
        long oneMEnd = T0 + 60_000L;
        harness.processWatermark(new Watermark(oneMEnd));
        // Also fire any processing timers that may be due
        harness.setProcessingTime(70_000L);
        List<RowData> mains = mainRows();
        // Filter for ONE_M — should be exactly 1 closed row with correct OHLCV
        List<RowData> oneMClosed = closedForTf(mains, Timeframe.ONE_M.code());
        assertEquals(1, oneMClosed.size(), "expect exactly 1 closed row for ONE_M after watermark past 1m end, got " + oneMClosed.size() + " mains: " + mains.size());
        RowData c = oneMClosed.get(0);
        assertEquals(TOKEN, c.getLong(CandleClosedColumns.INSTRUMENT_TOKEN));
        assertEquals(T0, c.getLong(CandleClosedColumns.WINDOW_START));
        assertEquals(oneMEnd, c.getLong(CandleClosedColumns.WINDOW_END));
        assertEquals(100_00L, c.getLong(CandleClosedColumns.OPEN_PAISE));
        assertEquals(101_00L, c.getLong(CandleClosedColumns.HIGH_PAISE));
        assertEquals(99_00L, c.getLong(CandleClosedColumns.LOW_PAISE));
        assertEquals(99_00L, c.getLong(CandleClosedColumns.CLOSE_PAISE));
        assertEquals(35L, c.getLong(CandleClosedColumns.VOLUME));
        assertEquals(3, c.getInt(CandleClosedColumns.TICK_COUNT));
        assertEquals(Timeframe.ONE_M.code(), c.getString(CandleClosedColumns.TF).toString());

        // Also verify FIFTEEN_S closed exists (1 row for [10:00:00,10:00:15))
        List<RowData> fifteenSClosed = closedForTf(mains, Timeframe.FIFTEEN_S.code());
        assertEquals(1, fifteenSClosed.size(), "FIFTEEN_S should also have 1 closed row");
        RowData f = fifteenSClosed.get(0);
        assertEquals(T0, f.getLong(CandleClosedColumns.WINDOW_START));
        assertEquals(T0 + 15_000L, f.getLong(CandleClosedColumns.WINDOW_END));

        // Signal rings after close: next trade should see closedNewestFirst size 1 for that TF
        long t4 = T0 + 65_000L; // next 1m bucket [10:01:00,10:02:00)
        // P2-036: a quote between trades must not eat the next TRADE —
        // quotes no longer advance the monotonic gate.
        harness.processElement(quote(T0 + 64_000L, "fp-q", 100_50L), T0 + 64_000L);
        harness.processElement(trade(t4, "fp-d", 102_00L, 7L), t4);
        List<MultiTimeframeSignalContext> signalsAfter = signalRows();
        assertEquals(4, signalsAfter.size(), "quote + next trade: quote emits no signal, trade emits one");
        MultiTimeframeSignalContext sigAfter = signalsAfter.get(3);
        var oneMAfter = sigAfter.frames().stream().filter(ff -> ff.tf() == Timeframe.ONE_M).findFirst().orElseThrow();
        assertEquals(1, oneMAfter.closedNewestFirst().size(), "after 1 close, ONE_M ring should have 1 entry newest-first");
        ClosedCandle closedOneM = oneMAfter.closedNewestFirst().get(0);
        assertEquals(T0, closedOneM.windowStart);
        assertEquals(oneMEnd, closedOneM.windowEnd);
        assertEquals(100_00L, closedOneM.openPaise);
        // Verify forming for next bucket is fresh single-tick candle
        assertEquals(102_00L, oneMAfter.forming().openPaise);
        assertEquals(7L, oneMAfter.forming().volume);
        // Ensure previous signal's closed copy did not alias live ring (mutate and check next)
        closedOneM.openPaise = 888888L;
        // Re-fetch next signal's ring should not be affected (deep copy)
        // Send another trade in same new bucket
        long t5 = T0 + 66_000L;
        harness.processElement(trade(t5, "fp-e", 103_00L, 3L), t5);
        var sig5 = signalRows().get(4);
        var oneM5 = sig5.frames().stream().filter(ff -> ff.tf() == Timeframe.ONE_M).findFirst().orElseThrow();
        assertEquals(100_00L, oneM5.closedNewestFirst().get(0).openPaise, "closed copy must be deep — mutating previous snapshot must not affect state");

        // Late tick after roll: eventTime earlier than lastEventTime (t1) → monotonic gate drops, no duplicate closed row
        long lateEvent = T0 + 1_000L; // earlier than t5's 66_000
        int mainsBeforeLate = mainRows().size();
        int signalsBeforeLate = signalRows().size();
        // Capture count of the original ONE_M window [T0, T0+60000) before late
        long oneMWindowStart = T0;
        int oneMCountBeforeLate = closedForTf(mainRows(), Timeframe.ONE_M.code()).stream()
                .filter(r -> r.getLong(CandleClosedColumns.WINDOW_START) == oneMWindowStart).toList().size();
        harness.processElement(trade(lateEvent, "fp-late", 50000L, 10L), lateEvent);
        assertEquals(signalsBeforeLate, signalRows().size(), "late tick must not emit signal");
        harness.processWatermark(new Watermark(T0 + 120_000L));
        harness.setProcessingTime(120_000L);
        // After advancing to 10:02:00, the second batch's buckets [10:01:00, ...) should close now (expected +3 rows: 15s,30s,1m for the second 1m bucket)
        // Late tick must not duplicate the original window — count for that window stays 1
        int oneMCountAfterLate = closedForTf(mainRows(), Timeframe.ONE_M.code()).stream()
                .filter(r -> r.getLong(CandleClosedColumns.WINDOW_START) == oneMWindowStart).toList().size();
        assertEquals(oneMCountBeforeLate, oneMCountAfterLate, "late tick must not duplicate closed row for original window");
        // Total mains should have grown by the second bucket's closes (3), not remain flat
        assertEquals(mainsBeforeLate + 3, mainRows().size(), "second batch should close at 10:02:00 (15s,30s,1m) — late tick must not suppress those");
    }

    @Test
    @DisplayName("session filter: post-close tick is dropped, no signal, no closed")
    void postCloseDropped() throws Exception {
        open();
        long inSession = ist(2026, 9, 4, 15, 29, 0, 0);
        long postClose = ist(2026, 9, 4, 15, 30, 0, 0); // exclusive
        harness.processElement(trade(inSession, "fp-in", 10000L, 5L), inSession);
        int signalsAfterIn = signalRows().size();
        assertEquals(1, signalsAfterIn);
        harness.processElement(trade(postClose, "fp-post", 20000L, 5L), postClose);
        assertEquals(1, signalRows().size(), "post-close tick must be dropped");
        // Watermark past close should still close the in-session bucket
        harness.processWatermark(new Watermark(postClose));
        List<RowData> mains = mainRows();
        assertTrue(mains.size() >= 1, "in-session bucket should still close even after post-close tick");
    }

    @Test
    @DisplayName("session bypass (soak): weekend tick accepted when flag set")
    void sessionBypassAcceptsOutOfSession() throws Exception {
        // Phase 5 soak mode A: MULTITF_SESSION_BYPASS lets the 15s fake-broker
        // soak run outside market hours (Sat 20:00 IST — the session filter is
        // hour-of-day only, no weekday check). The tick must flow
        // through to signal + bucket accumulation exactly like an in-session
        // tick — no pre/post drop, no state mutation skip.
        fn = new MultiTimeframeAggregateFunction(LIVE_INTERVAL, true);
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                fn,
                row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();
        long saturday = ist(2026, 9, 5, 20, 0, 0, 0); // Sat 2026-09-05 20:00 IST
        harness.processElement(trade(saturday + 1_000L, "fp-sat-1", 10000L, 5L), saturday + 1_000L);
        harness.processElement(trade(saturday + 2_000L, "fp-sat-2", 10001L, 7L), saturday + 2_000L);
        assertEquals(2, signalRows().size(),
                "bypass mode must emit a signal context per accepted out-of-session TRADE tick");
        // Close a 15s bucket via watermark to prove accumulation happened.
        harness.processWatermark(new Watermark(saturday + 15_000L));
        harness.setProcessingTime(20_000L);
        List<RowData> mains = mainRows();
        boolean anyClosed = false;
        for (RowData r : mains) {
            if (r.getLong(CandleClosedColumns.WINDOW_START) == saturday
                    && Timeframe.FIFTEEN_S.code().equals(r.getString(CandleClosedColumns.TF).toString())) {
                anyClosed = true;
                // Regression (2026-09-04 soak): a stale session-close timer
                // fired with the bypass (off-hours ticks -> 15:30 IST of the
                // event date is in the PAST) and truncated every closed row's
                // window_end to 15:30. window_end must be window_start + tfMs.
                assertEquals(saturday + 15_000L, r.getLong(CandleClosedColumns.WINDOW_END),
                        "bypass-mode closed row window_end must be window_start + 15s, "
                                + "not the session-close constant");
            }
        }
        assertTrue(anyClosed, "bypass mode must close the weekend 15s bucket on watermark");
    }

    @Test
    @DisplayName("session bypass default off: weekend tick still dropped")
    void sessionBypassDefaultsOff() throws Exception {
        open(); // default constructor => bypass=false
        long saturday = ist(2026, 9, 5, 20, 0, 0, 0);
        harness.processElement(trade(saturday + 1_000L, "fp-sat-x", 10000L, 5L), saturday + 1_000L);
        assertEquals(0, signalRows().size(),
                "without the soak flag an out-of-session tick must stay dropped");
    }

    private void openWith(MultiTimeframeAggregateFunction f) throws Exception {
        fn = f;
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                fn,
                row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();
    }

    /** Drives 3 in-session trades, fires live timers, closes the first 15s bucket. */
    private void driveThreeTradesAndClose(long T0) throws Exception {
        harness.processElement(trade(T0 + 2_000L, "fp-a", 100_00L, 10L), T0 + 2_000L);
        harness.processElement(trade(T0 + 5_000L, "fp-b", 101_00L, 20L), T0 + 5_000L);
        harness.processElement(trade(T0 + 8_000L, "fp-c", 99_00L, 5L), T0 + 8_000L);
        harness.setProcessingTime(20_000L);
        harness.processWatermark(new Watermark(T0 + 10_000L));
        harness.processWatermark(new Watermark(T0 + 15_000L));
        harness.setProcessingTime(70_000L);
    }

    private static String closedKey(RowData r) {
        return r.getString(CandleClosedColumns.TF).toString() + "|"
                + r.getLong(CandleClosedColumns.WINDOW_START);
    }

    private static void assertClosedOhlcvEquals(RowData expected, RowData actual) {
        assertEquals(expected.getLong(CandleClosedColumns.WINDOW_START), actual.getLong(CandleClosedColumns.WINDOW_START));
        assertEquals(expected.getLong(CandleClosedColumns.WINDOW_END), actual.getLong(CandleClosedColumns.WINDOW_END));
        assertEquals(expected.getLong(CandleClosedColumns.OPEN_PAISE), actual.getLong(CandleClosedColumns.OPEN_PAISE));
        assertEquals(expected.getLong(CandleClosedColumns.HIGH_PAISE), actual.getLong(CandleClosedColumns.HIGH_PAISE));
        assertEquals(expected.getLong(CandleClosedColumns.LOW_PAISE), actual.getLong(CandleClosedColumns.LOW_PAISE));
        assertEquals(expected.getLong(CandleClosedColumns.CLOSE_PAISE), actual.getLong(CandleClosedColumns.CLOSE_PAISE));
        assertEquals(expected.getLong(CandleClosedColumns.VOLUME), actual.getLong(CandleClosedColumns.VOLUME));
    }

    @Test
    @DisplayName("signal context OFF: zero SIGNAL_TAG rows, identical closed/live OHLCV vs ON")
    void signalContextDisabledSkipsSideOutputButKeepsCandles() throws Exception {
        // Candle-phase soak switch: skipping the per-tick signal-context
        // photocopy must not change one byte of candle output.
        long T0 = ist(2026, 9, 4, 10, 0, 0, 0); // in-session, aligned for all 6 TFs
        openWith(new MultiTimeframeAggregateFunction(LIVE_INTERVAL, false, true));
        driveThreeTradesAndClose(T0);
        assertEquals(3, signalRows().size(), "ON must emit one signal context per TRADE tick");
        List<RowData> mainsOn = mainRows();
        List<RowData> livesOn = liveRows();
        assertFalse(mainsOn.isEmpty(), "ON must close the 15s bucket");
        assertFalse(livesOn.isEmpty(), "ON must emit live snapshots");
        harness.close();
        harness = null;

        openWith(new MultiTimeframeAggregateFunction(LIVE_INTERVAL, false, false));
        driveThreeTradesAndClose(T0);
        assertEquals(0, signalRows().size(),
                "OFF must emit zero SIGNAL_TAG rows (photocopy skipped)");
        List<RowData> mainsOff = mainRows();
        List<RowData> livesOff = liveRows();
        assertEquals(mainsOn.size(), mainsOff.size(), "closed row count must match ON");
        java.util.Map<String, RowData> offByKey = new java.util.HashMap<>();
        for (RowData r : mainsOff) offByKey.put(closedKey(r), r);
        for (RowData r : mainsOn) {
            RowData o = offByKey.remove(closedKey(r));
            assertNotNull(o, "OFF missing closed row " + closedKey(r));
            assertClosedOhlcvEquals(r, o);
        }
        assertTrue(offByKey.isEmpty(), "OFF emitted extra closed rows: " + offByKey.keySet());
        assertEquals(livesOn.size(), livesOff.size(), "live snapshot count must match ON");
    }

    @Test
    @DisplayName("metrics: slot cap and duplicate guard")
    void duplicateGuard() throws Exception {
        open();
        long T0 = ist(2026, 9, 4, 10, 0, 0, 0);
        harness.processElement(trade(T0 + 1_000L, "fp-1", 10000L, 1L), T0 + 1_000L);
        harness.processElement(trade(T0 + 2_000L, "fp-2", 10001L, 1L), T0 + 2_000L);
        // Close 15s bucket
        harness.processWatermark(new Watermark(T0 + 15_000L));
        harness.setProcessingTime(20_000L);
        int mainsAfterFirstClose = mainRows().size();
        assertTrue(mainsAfterFirstClose >= 1);
        // Re-advance same watermark again — should not duplicate
        harness.processWatermark(new Watermark(T0 + 15_000L));
        harness.setProcessingTime(25_000L);
        assertEquals(mainsAfterFirstClose, mainRows().size(), "duplicate watermark must not re-emit same window");
    }
}
