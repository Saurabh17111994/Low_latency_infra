package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Phase 3 Track B — {@link MultiTimeframeSignalProducer} harness tests.
 *
 * <p>Constructs {@link MultiTimeframeSignalContext} directly (no aggregator)
 * and drives the producer through the Flink 2.2.1 keyed harness (no cluster).
 * Mirrors design §G warm-up and §F idempotency/row-field contracts.
 */
class MultiTimeframeSignalProducerTest {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final LocalDate FIXED_DATE = LocalDate.of(2026, 9, 4);
    private static final long TOKEN = 2885L;
    private static final String FP = "fp-trigger-123";
    private static final long BASE_TIME = ZonedDateTime.of(FIXED_DATE, LocalTime.of(10, 0, 5), IST).toInstant().toEpochMilli();

    private KeyedOneInputStreamOperatorTestHarness<Long, MultiTimeframeSignalContext, RowData> harness;
    private MultiTimeframeSignalProducer producer;

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    private void open() throws Exception {
        producer = new MultiTimeframeSignalProducer();
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                producer,
                ctx -> ctx.instrumentToken(),
                Types.LONG);
        harness.open();
    }

    private void openWithProducer(MultiTimeframeSignalProducer p) throws Exception {
        producer = p;
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                producer,
                ctx -> ctx.instrumentToken(),
                Types.LONG);
        harness.open();
    }

    // — helpers —

    static CandleAccumulator forming(long open, long high, long low, long close, long volume, long tickCount, long lastEventTime, String fingerprint) {
        CandleAccumulator acc = new CandleAccumulator();
        acc.exchange = "NSE";
        acc.symbol = "TEST";
        acc.openPaise = open;
        acc.highPaise = high;
        acc.lowPaise = low;
        acc.closePaise = close;
        acc.volume = volume;
        acc.tickCount = tickCount;
        acc.firstEventTime = lastEventTime - 1000;
        acc.firstFingerprint = "fp-first";
        acc.lastEventTime = lastEventTime;
        acc.lastFingerprint = fingerprint;
        acc.lastIngestTs = lastEventTime;
        return acc;
    }

    static CandleAccumulator emptyForming() {
        return new CandleAccumulator();
    }

    static ClosedCandle closed(long windowStart, long high, long close) {
        long windowEnd = windowStart + 15_000L;
        return new ClosedCandle(windowStart, windowEnd, 10_000L, high, 9_000L, close, 100L, 5, windowStart + 1000, "fp-closed");
    }

    /** Build a ring of size n, newest-first, where newest has highest high/close. */
    static List<ClosedCandle> ringNewestFirst(int size, long baseHigh, long baseClose, long step) {
        List<ClosedCandle> oldestFirst = new ArrayList<>();
        long baseStart = ZonedDateTime.of(FIXED_DATE, LocalTime.of(9, 0, 0), IST).toInstant().toEpochMilli();
        for (int i = 0; i < size; i++) {
            long high = baseHigh + i * step;
            long close = baseClose + i * step;
            long ws = baseStart + i * 15_000L;
            oldestFirst.add(closed(ws, high, close));
        }
        List<ClosedCandle> newestFirst = new ArrayList<>(oldestFirst);
        Collections.reverse(newestFirst);
        return newestFirst;
    }

    static List<ClosedCandle> warmRing() {
        // 15 closed, highs 10000..11400 step 100, closes 10050..11450
        return ringNewestFirst(15, 10_000L, 10_050L, 100L);
    }

    static List<ClosedCandle> warmRingCustom(long baseHigh, long baseClose) {
        return ringNewestFirst(15, baseHigh, baseClose, 100L);
    }

    static MultiTimeframeSignalContext contextFor(long instrumentToken, long eventTime, List<MultiTimeframeSignalContext.TimeframeContext> frames) {
        // frames must be exactly 6 in Timeframe.values() order — test helpers ensure that.
        return new MultiTimeframeSignalContext(instrumentToken, "NSE", "TEST", eventTime, frames);
    }

    /** Build 6 frames where target TF gets firing forming+ring, others get empty. */
    static List<MultiTimeframeSignalContext.TimeframeContext> framesOneFiring(Timeframe target, CandleAccumulator firingForming, List<ClosedCandle> firingRing) {
        List<MultiTimeframeSignalContext.TimeframeContext> out = new ArrayList<>();
        for (Timeframe tf : Timeframe.values()) {
            if (tf == target) {
                out.add(new MultiTimeframeSignalContext.TimeframeContext(tf, firingForming, firingRing));
            } else {
                out.add(new MultiTimeframeSignalContext.TimeframeContext(tf, emptyForming(), List.of()));
            }
        }
        return out;
    }

    /** Build 6 frames where every TF fires (distinct rings/formings). */
    static List<MultiTimeframeSignalContext.TimeframeContext> framesAllFiring(long eventTime, String baseFp) {
        List<MultiTimeframeSignalContext.TimeframeContext> out = new ArrayList<>();
        int idx = 0;
        for (Timeframe tf : Timeframe.values()) {
            // Each TF gets its own firing forming: open < close and close > maxHigh
            // Use same warm ring for all for simplicity, and forming close = 12000 > max 11400
            CandleAccumulator f = forming(11_500L, 12_100L, 11_400L, 12_000L, 50L, 3, eventTime, baseFp + "-" + tf.code());
            out.add(new MultiTimeframeSignalContext.TimeframeContext(tf, f, warmRing()));
            idx++;
        }
        return out;
    }

    static List<RowData> emitted(KeyedOneInputStreamOperatorTestHarness<Long, MultiTimeframeSignalContext, RowData> h) {
        List<RowData> out = new ArrayList<>();
        for (Object o : h.getOutput()) {
            if (o instanceof StreamRecord) {
                Object v = ((StreamRecord<?>) o).getValue();
                if (v instanceof RowData) out.add((RowData) v);
            }
        }
        return out;
    }

    static String str(RowData r, int idx) {
        return r.getString(idx).toString();
    }

    // — tests —

    @Test
    @DisplayName("firing forming + warm 15-ring -> one signal RowData with all mandatory columns non-null")
    void firingEmitsOneSignalWithAllMandatoryColumns() throws Exception {
        open();
        harness.setProcessingTime(BASE_TIME + 1000);
        CandleAccumulator f = forming(11_500L, 12_100L, 11_400L, 12_000L, 50L, 3, BASE_TIME, FP);
        List<ClosedCandle> ring = warmRing();
        var ctx = contextFor(TOKEN, BASE_TIME, framesOneFiring(Timeframe.ONE_M, f, ring));
        harness.processElement(ctx, BASE_TIME);

        List<RowData> rows = emitted(harness);
        assertEquals(1, rows.size(), "one TF firing should emit exactly one row");
        RowData row = rows.get(0);
        // All mandatory columns per task must be non-null
        assertFalse(row.isNullAt(SignalCandidatesTableColumns.CANDIDATE_ID));
        assertFalse(row.isNullAt(SignalCandidatesTableColumns.INSTRUMENT_TOKEN));
        assertFalse(row.isNullAt(SignalCandidatesTableColumns.EXCHANGE));
        assertFalse(row.isNullAt(SignalCandidatesTableColumns.SYMBOL));
        assertFalse(row.isNullAt(SignalCandidatesTableColumns.STRATEGY_ID));
        assertFalse(row.isNullAt(SignalCandidatesTableColumns.STRATEGY_VERSION));
        assertFalse(row.isNullAt(SignalCandidatesTableColumns.RULE_ID));
        assertFalse(row.isNullAt(SignalCandidatesTableColumns.DETECTION_TS));
        assertFalse(row.isNullAt(SignalCandidatesTableColumns.EVALUATION_TS));
        assertFalse(row.isNullAt(SignalCandidatesTableColumns.ACTION));
        assertFalse(row.isNullAt(SignalCandidatesTableColumns.SIDE));
        assertFalse(row.isNullAt(SignalCandidatesTableColumns.QUANTITY));
        assertFalse(row.isNullAt(SignalCandidatesTableColumns.ORDER_TYPE));
        // task requires these nullable DDL cols to be non-null in emitted row
        assertFalse(row.isNullAt(SignalCandidatesTableColumns.LIMIT_PRICE_PAISE), "limit_price_paise must be non-null per task");
        assertFalse(row.isNullAt(SignalCandidatesTableColumns.SCORE_INPUTS));
        assertFalse(row.isNullAt(SignalCandidatesTableColumns.FORMATION_SNAPSHOT_REF));
        assertFalse(row.isNullAt(SignalCandidatesTableColumns.SCHEMA_VERSION));

        assertEquals(TOKEN, row.getLong(SignalCandidatesTableColumns.INSTRUMENT_TOKEN));
        assertEquals("NSE", str(row, SignalCandidatesTableColumns.EXCHANGE));
        assertEquals("TEST", str(row, SignalCandidatesTableColumns.SYMBOL));
        assertEquals(MultiTimeframeSignalProducer.STRATEGY_ID, str(row, SignalCandidatesTableColumns.STRATEGY_ID));
        assertEquals(MultiTimeframeSignalProducer.STRATEGY_VERSION, str(row, SignalCandidatesTableColumns.STRATEGY_VERSION));
        assertEquals(MultiTimeframeSignalProducer.RULE_ID, str(row, SignalCandidatesTableColumns.RULE_ID));
        assertEquals(BASE_TIME, row.getLong(SignalCandidatesTableColumns.DETECTION_TS));
        assertTrue(row.getLong(SignalCandidatesTableColumns.EVALUATION_TS) >= BASE_TIME, "evaluation_ts = now >= detection_ts");
        assertEquals(SignalCandidatesTableColumns.ACTION_ENTRY, str(row, SignalCandidatesTableColumns.ACTION));
        assertEquals(SignalCandidatesTableColumns.SIDE_BUY, str(row, SignalCandidatesTableColumns.SIDE));
        assertEquals(MultiTimeframeSignalProducer.QUANTITY, row.getLong(SignalCandidatesTableColumns.QUANTITY));
        assertEquals(SignalCandidatesTableColumns.ORDER_TYPE_MARKET, str(row, SignalCandidatesTableColumns.ORDER_TYPE));
        assertEquals(MultiTimeframeSignalProducer.SCHEMA_VERSION, str(row, SignalCandidatesTableColumns.SCHEMA_VERSION));
        // formation snapshot must reference tf, window_start, fingerprint
        String snap = str(row, SignalCandidatesTableColumns.FORMATION_SNAPSHOT_REF);
        assertTrue(snap.contains(Timeframe.ONE_M.code()), "snapshot must reference TF");
        long expectedWindowStart = TimeframeBucket.bucketStart(Timeframe.ONE_M, BASE_TIME);
        assertTrue(snap.contains(String.valueOf(expectedWindowStart)), "snapshot must contain window_start");
        assertTrue(snap.contains(FP), "snapshot must contain trigger fingerprint");
        // score_inputs JSON-ish must contain tf and forming fields
        String score = str(row, SignalCandidatesTableColumns.SCORE_INPUTS);
        assertTrue(score.contains(Timeframe.ONE_M.code()));
        assertTrue(score.contains("\"close\":12000"));
        // validity_reason non-null (we set VALID)
        assertFalse(row.isNullAt(SignalCandidatesTableColumns.VALIDITY_REASON));
        assertEquals(SignalCandidatesTableColumns.VALIDITY_REASON_VALID, str(row, SignalCandidatesTableColumns.VALIDITY_REASON));
    }

    @Test
    @DisplayName("candidate_id deterministic: same input twice -> same id (fresh harness)")
    void candidateIdDeterministic() throws Exception {
        // Two fresh harnesses, same input -> same candidate_id
        CandleAccumulator f = forming(11_500L, 12_100L, 11_400L, 12_000L, 50L, 3, BASE_TIME, FP);
        List<ClosedCandle> ring = warmRing();
        var ctx = contextFor(TOKEN, BASE_TIME, framesOneFiring(Timeframe.FIFTEEN_S, f, ring));

        open();
        harness.setProcessingTime(BASE_TIME + 1000);
        harness.processElement(ctx, BASE_TIME);
        String id1 = str(emitted(harness).get(0), SignalCandidatesTableColumns.CANDIDATE_ID);
        harness.close();
        harness = null;

        open();
        harness.setProcessingTime(BASE_TIME + 1000);
        harness.processElement(ctx, BASE_TIME);
        String id2 = str(emitted(harness).get(0), SignalCandidatesTableColumns.CANDIDATE_ID);
        assertEquals(id1, id2, "deterministic candidate_id must be stable across replays");

        // Also check static helper matches
        long ws = TimeframeBucket.bucketStart(Timeframe.FIFTEEN_S, BASE_TIME);
        String expected = MultiTimeframeSignalProducer.candidateIdFor(TOKEN, Timeframe.FIFTEEN_S, ws, FP);
        assertEquals(expected, id1);
    }

    @Test
    @DisplayName("replay of identical context after simulated restore -> suppressed (no duplicate row, counter++)")
    void replaySuppressed() throws Exception {
        open();
        harness.setProcessingTime(BASE_TIME + 1000);
        CandleAccumulator f = forming(11_500L, 12_100L, 11_400L, 12_000L, 50L, 3, BASE_TIME, FP);
        var ctx = contextFor(TOKEN, BASE_TIME, framesOneFiring(Timeframe.FIVE_M, f, warmRing()));
        harness.processElement(ctx, BASE_TIME);
        assertEquals(1, emitted(harness).size());
        long emittedBefore = producer.getEmittedCountForTest();
        long suppressedBefore = producer.getSuppressedCountForTest();
        assertEquals(1, emittedBefore);

        // Replay identical context (same fingerprint, same window) — must be suppressed
        harness.processElement(ctx, BASE_TIME + 10);
        assertEquals(1, emitted(harness).size(), "replay must not emit duplicate row");
        assertEquals(1, producer.getEmittedCountForTest(), "emitted counter unchanged");
        assertTrue(producer.getSuppressedCountForTest() > suppressedBefore, "suppressed counter must increment on duplicate");
    }

    @Test
    @DisplayName("context that does NOT cross breakout threshold -> no emission")
    void noEmissionWhenNotBreakout() throws Exception {
        open();
        harness.setProcessingTime(BASE_TIME + 1000);
        // close 11300 < maxHigh 11400 -> no breakout
        CandleAccumulator f = forming(11_000L, 11_400L, 10_900L, 11_300L, 20L, 2, BASE_TIME, FP);
        var ctx = contextFor(TOKEN, BASE_TIME, framesOneFiring(Timeframe.ONE_M, f, warmRing()));
        harness.processElement(ctx, BASE_TIME);
        assertEquals(0, emitted(harness).size(), "non-breakout forming must not emit");
        // also test bearish (close <= open)
        CandleAccumulator f2 = forming(12_000L, 12_200L, 11_800L, 11_900L, 20L, 2, BASE_TIME, "fp-bearish");
        var ctx2 = contextFor(TOKEN, BASE_TIME + 1000, framesOneFiring(Timeframe.ONE_M, f2, warmRing()));
        harness.processElement(ctx2, BASE_TIME + 1000);
        assertEquals(0, emitted(harness).size(), "bearish must not emit");
        // also test trend failure: close just above maxHigh but sum too high
        // Build ring with high closes so sum is high: baseClose 11900, maxHigh 12040, forming close 12050 just above but trend fails?
        // sum of 15 closes starting 11900 step 100 -> last 13300, avg ~12600, sum ~189k, close*15=180750 < sum -> no trend
        List<ClosedCandle> highRing = ringNewestFirst(15, 11_000L, 11_900L, 100L);
        CandleAccumulator f3 = forming(11_500L, 12_060L, 11_400L, 12_050L, 20L, 2, BASE_TIME + 2000, "fp-trend-fail");
        var ctx3 = contextFor(TOKEN, BASE_TIME + 2000, framesOneFiring(Timeframe.THREE_M, f3, highRing));
        // For THREE_M, highRing max is 11900+14*100=13300 -> close 12050 < max -> actually still breakout fail, adjust
        // Use ring with max just below close but sum still high
        List<ClosedCandle> trenchRing = ringNewestFirst(15, 10_000L, 11_950L, 10L); // closes 11950..12090 step10, sum ~180k, maxHigh 10140
        CandleAccumulator f4 = forming(11_500L, 12_100L, 11_400L, 12_000L, 20L, 2, BASE_TIME + 3000, "fp-trend2");
        // maxHigh ~10140 so breakout true, but sum ~180k, close*15=180000 borderline - tweak to fail
        // Instead craft sum just over close*n: make closes all 12000 (sum 180000) and close 11999 -> trend false
        List<ClosedCandle> flatHighRing = new ArrayList<>();
        for (int i=0;i<15;i++) {
            long ws = ZonedDateTime.of(FIXED_DATE, LocalTime.of(9,0,0), IST).toInstant().toEpochMilli() + i*15000L;
            flatHighRing.add(new ClosedCandle(ws, ws+15000L, 11_000L, 11_500L, 10_900L, 11_999L, 100L, 5, ws+1000, "fp-"+i));
        }
        Collections.reverse(flatHighRing);
        CandleAccumulator f5 = forming(11_500L, 12_500L, 11_400L, 12_000L, 20L, 2, BASE_TIME + 4000, "fp-trend-fail2");
        var ctx5 = contextFor(TOKEN, BASE_TIME + 4000, framesOneFiring(Timeframe.FIFTEEN_M, f5, flatHighRing));
        // flat ring maxHigh 11500, close 12000 > max true, but sum =179985, close*15=180000 > sum true -> would actually fire, skip
        // To truly test trend failure, make ring closes huge avg 13000 sum 195000
        List<ClosedCandle> hugeRing = ringNewestFirst(15, 10_000L, 13_000L, 0L); // all closes 13000, sum 195000
        CandleAccumulator f6 = forming(11_500L, 12_100L, 11_400L, 12_000L, 20L, 2, BASE_TIME + 5000, "fp-huge");
        // this ring maxHigh =10000, close 12000 > max true, but close*15=180000 <195000 -> trend false -> no emit
        var ctx6 = contextFor(TOKEN, BASE_TIME + 5000, framesOneFiring(Timeframe.FIFTEEN_S, f6, hugeRing));
        harness.processElement(ctx6, BASE_TIME + 5000);
        // Should still be zero for those huge cases plus earlier zeros, but we already had 0, ensure no new
        assertEquals(0, emitted(harness).size(), "trend failure must not emit");
    }

    @Test
    @DisplayName("warm-up: ring with <15 closed -> no signal even if breakout true")
    void warmupNoSignal() throws Exception {
        open();
        harness.setProcessingTime(BASE_TIME + 1000);
        CandleAccumulator f = forming(11_500L, 12_100L, 11_400L, 12_000L, 50L, 3, BASE_TIME, FP);
        List<ClosedCandle> smallRing = ringNewestFirst(14, 10_000L, 10_050L, 100L);
        assertEquals(14, smallRing.size());
        var ctx = contextFor(TOKEN, BASE_TIME, framesOneFiring(Timeframe.ONE_M, f, smallRing));
        harness.processElement(ctx, BASE_TIME);
        assertEquals(0, emitted(harness).size(), "warm-up: <15 closed must not fire");

        // Same but exactly 15 should fire (sanity)
        var ctx15 = contextFor(TOKEN, BASE_TIME + 15_000L, framesOneFiring(Timeframe.ONE_M, f, warmRing()));
        harness.processElement(ctx15, BASE_TIME + 15_000L);
        assertEquals(1, emitted(harness).size(), "exactly 15 should be warm and fire");
    }

    @Test
    @DisplayName("warm-up is per-TF: 15s ring warm does not unblock 15m rule")
    void warmupPerTf() throws Exception {
        open();
        harness.setProcessingTime(BASE_TIME + 1000);
        CandleAccumulator f = forming(11_500L, 12_100L, 11_400L, 12_000L, 50L, 3, BASE_TIME, FP);
        // Build frames: FIFTEEN_S has 15, FIFTEEN_M has 14
        List<MultiTimeframeSignalContext.TimeframeContext> frames = new ArrayList<>();
        for (Timeframe tf : Timeframe.values()) {
            if (tf == Timeframe.FIFTEEN_S) {
                frames.add(new MultiTimeframeSignalContext.TimeframeContext(tf, forming(11_500L, 12_100L, 11_400L, 12_000L, 50L, 3, BASE_TIME, "fp-15s"), warmRing()));
            } else if (tf == Timeframe.FIFTEEN_M) {
                frames.add(new MultiTimeframeSignalContext.TimeframeContext(tf, forming(11_500L, 12_100L, 11_400L, 12_000L, 50L, 3, BASE_TIME, "fp-15m"), ringNewestFirst(14, 10_000L, 10_050L, 100L)));
            } else {
                frames.add(new MultiTimeframeSignalContext.TimeframeContext(tf, emptyForming(), List.of()));
            }
        }
        var ctx = contextFor(TOKEN, BASE_TIME, frames);
        harness.processElement(ctx, BASE_TIME);
        List<RowData> rows = emitted(harness);
        assertEquals(1, rows.size(), "only the warm TF should fire");
        String snap = str(rows.get(0), SignalCandidatesTableColumns.FORMATION_SNAPSHOT_REF);
        assertTrue(snap.contains(Timeframe.FIFTEEN_S.code()));
        assertFalse(snap.contains(Timeframe.FIFTEEN_M.code()));
    }

    @Test
    @DisplayName("each of the 6 TFs can fire independently in one context")
    void eachTfCanFireIndependently() throws Exception {
        open();
        harness.setProcessingTime(BASE_TIME + 1000);
        var frames = framesAllFiring(BASE_TIME, FP);
        var ctx = contextFor(TOKEN, BASE_TIME, frames);
        harness.processElement(ctx, BASE_TIME);
        List<RowData> rows = emitted(harness);
        assertEquals(6, rows.size(), "all 6 TFs with warm+breakout should each fire once");
        // Verify each TF appears exactly once in snapshots
        for (Timeframe tf : Timeframe.values()) {
            long count = rows.stream().filter(r -> str(r, SignalCandidatesTableColumns.FORMATION_SNAPSHOT_REF).contains(tf.code())).count();
            assertEquals(1, count, "TF " + tf.code() + " should appear once");
        }
        // Verify per-TF candidate_ids are distinct (different tf in key)
        List<String> ids = rows.stream().map(r -> str(r, SignalCandidatesTableColumns.CANDIDATE_ID)).toList();
        assertEquals(6, ids.stream().distinct().count(), "candidate_ids must be distinct per TF");
        // Metrics
        assertEquals(6, producer.getEmittedCountForTest());
        assertEquals(0, producer.getSuppressedCountForTest());
    }

    @Test
    @DisplayName("fire-once-per-window latch: second tick in same window suppressed")
    void fireOncePerWindowLatch() throws Exception {
        open();
        harness.setProcessingTime(BASE_TIME + 1000);
        long windowStart = TimeframeBucket.bucketStart(Timeframe.ONE_M, BASE_TIME);
        // First tick in window fires
        CandleAccumulator f1 = forming(11_500L, 12_100L, 11_400L, 12_000L, 50L, 3, BASE_TIME, "fp-first-in-window");
        var ctx1 = contextFor(TOKEN, BASE_TIME, framesOneFiring(Timeframe.ONE_M, f1, warmRing()));
        harness.processElement(ctx1, BASE_TIME);
        assertEquals(1, emitted(harness).size());

        // Second tick SAME window (BASE_TIME+5000 still same ONE_M bucket [10:00:00,10:01:00)) with different fingerprint but same windowStart — latch should suppress
        long secondTime = BASE_TIME + 5_000L;
        assertEquals(windowStart, TimeframeBucket.bucketStart(Timeframe.ONE_M, secondTime), "second tick must be same window");
        CandleAccumulator f2 = forming(11_600L, 12_200L, 11_500L, 12_100L, 50L, 3, secondTime, "fp-second-same-window");
        var ctx2 = contextFor(TOKEN, secondTime, framesOneFiring(Timeframe.ONE_M, f2, warmRing()));
        harness.processElement(ctx2, secondTime);
        assertEquals(1, emitted(harness).size(), "second tick in same window must be latch-suppressed");
        assertEquals(1, producer.getSuppressedCountForTest() >= 1 ? producer.getSuppressedCountForTest() : 0, "suppressed counter must increment");
        // Next window should fire again
        long nextWindowTime = windowStart + Timeframe.ONE_M.windowMs() + 1_000L;
        CandleAccumulator f3 = forming(11_500L, 12_100L, 11_400L, 12_000L, 50L, 3, nextWindowTime, "fp-next-window");
        var ctx3 = contextFor(TOKEN, nextWindowTime, framesOneFiring(Timeframe.ONE_M, f3, warmRing()));
        harness.processElement(ctx3, nextWindowTime);
        assertEquals(2, emitted(harness).size(), "next window should fire again");
    }

    @Test
    @DisplayName("RuleResult.evaluate is isolated, testable, static — warmup, bullish, breakout, trend")
    void evaluateIsolated() {
        // Direct static call without Flink harness
        // Warmup failure
        var warmFailCtx = new MultiTimeframeSignalContext.TimeframeContext(Timeframe.ONE_M, forming(11_500L, 12_100L, 11_400L, 12_000L, 50L, 3, BASE_TIME, FP), ringNewestFirst(10, 10_000L, 10_050L, 100L));
        assertFalse(MultiTimeframeSignalProducer.evaluate(warmFailCtx).fired);
        // Bullish failure
        var bearCtx = new MultiTimeframeSignalContext.TimeframeContext(Timeframe.ONE_M, forming(12_000L, 12_100L, 11_000L, 11_000L, 50L, 3, BASE_TIME, FP), warmRing());
        assertFalse(MultiTimeframeSignalProducer.evaluate(bearCtx).fired);
        // Breakout failure
        var noBreakCtx = new MultiTimeframeSignalContext.TimeframeContext(Timeframe.ONE_M, forming(11_000L, 11_400L, 10_900L, 11_300L, 50L, 3, BASE_TIME, FP), warmRing());
        assertFalse(MultiTimeframeSignalProducer.evaluate(noBreakCtx).fired);
        // Trend failure
        List<ClosedCandle> hugeRing = ringNewestFirst(15, 10_000L, 13_000L, 0L);
        var noTrendCtx = new MultiTimeframeSignalContext.TimeframeContext(Timeframe.FIFTEEN_S, forming(11_500L, 12_100L, 11_400L, 12_000L, 50L, 3, BASE_TIME, FP), hugeRing);
        assertFalse(MultiTimeframeSignalProducer.evaluate(noTrendCtx).fired);
        // Success
        var okCtx = new MultiTimeframeSignalContext.TimeframeContext(Timeframe.ONE_M, forming(11_500L, 12_100L, 11_400L, 12_000L, 50L, 3, BASE_TIME, FP), warmRing());
        assertTrue(MultiTimeframeSignalProducer.evaluate(okCtx).fired);
    }

    @Test
    @DisplayName("empty forming (tickCount=0) never fires")
    void emptyFormingNeverFires() throws Exception {
        open();
        harness.setProcessingTime(BASE_TIME + 1000);
        var ctx = contextFor(TOKEN, BASE_TIME, framesOneFiring(Timeframe.ONE_M, emptyForming(), warmRing()));
        harness.processElement(ctx, BASE_TIME);
        assertEquals(0, emitted(harness).size());
    }

    @Test
    @DisplayName("score_inputs and formation_snapshot_ref are compact JSON-ish and contain expected fields")
    void scoreInputsFormat() throws Exception {
        open();
        harness.setProcessingTime(BASE_TIME + 1000);
        CandleAccumulator f = forming(11_500L, 12_100L, 11_400L, 12_000L, 50L, 3, BASE_TIME, "fp-score-test");
        var ctx = contextFor(TOKEN, BASE_TIME, framesOneFiring(Timeframe.THREE_M, f, warmRing()));
        harness.processElement(ctx, BASE_TIME);
        RowData row = emitted(harness).get(0);
        String score = str(row, SignalCandidatesTableColumns.SCORE_INPUTS);
        assertTrue(score.startsWith("{"));
        assertTrue(score.contains("\"tf\":\"THREE_M\""));
        assertTrue(score.contains("\"maxHighPrev\""));
        assertTrue(score.contains("\"ringSize\":15"));
        String snap = str(row, SignalCandidatesTableColumns.FORMATION_SNAPSHOT_REF);
        assertTrue(snap.startsWith("forming:THREE_M:"));
    }
}
