package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@link EarlySignalFunction} driven through the Flink 2.2.1 operator harness
 * (KeyedTwoInputStreamOperatorTestHarness — no cluster). Covers the Phase 2
 * contract (low-latency candles plan 2026-08-29): a TENTATIVE candidate fires
 * once per window when the breakout rule first holds on a preview; the final
 * candle settles it with a CONFIRM (rule held) or CANCEL (rule failed) row
 * superseding the tentative via {@code supersedes_candidate_id}; no tentative
 * before warm-up or when the partial OHLCV does not hold; a late-dropped
 * final leaves no dangling tentative (timer CANCEL); supersession survives
 * checkpoint/restore.
 *
 * <p>Same 5-key baseline + lookback=3 as {@link SignalDetectionFunctionTest}
 * (windows are 15s; lookback 3 keeps tests short).
 */
class EarlySignalFunctionTest {

    private static final long T0 = 1_700_000_000_000L;

    private KeyedTwoInputStreamOperatorTestHarness<Long, RowData, RowData, RowData> harness;

    private EarlySignalFunction fn;

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
        fn = null;
    }

    private void openHarness() throws Exception {
        SignalJobConfig config = SignalJobConfig.from(env());
        fn = new EarlySignalFunction(config);
        harness = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
                fn,
                row -> row.getLong(CandlePreviewColumns.INSTRUMENT_TOKEN),
                row -> row.getLong(CandleTableColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();
    }

    private void openHarnessWithStateBackend() throws Exception {
        SignalJobConfig config = SignalJobConfig.from(env());
        fn = new EarlySignalFunction(config);
        harness = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
                fn,
                row -> row.getLong(CandlePreviewColumns.INSTRUMENT_TOKEN),
                row -> row.getLong(CandleTableColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.setStateBackend(new HashMapStateBackend());
        harness.open();
    }

    /** Baseline + lookback=3 so tests stay short; tuning keys take defaults. */
    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_TTL_MS", "60000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        env.put("SIGNAL_LOOKBACK_CANDLES", "3");
        return env;
    }

    /** Final candle row (15 columns, CandleTableColumns layout). */
    private static GenericRowData candle(long token, long start, long end,
            long open, long high, long low, long close) {
        GenericRowData row = new GenericRowData(CandleTableColumns.FIELD_COUNT);
        row.setField(CandleTableColumns.INSTRUMENT_TOKEN, token);
        row.setField(CandleTableColumns.EXCHANGE, StringData.fromString("NSE"));
        row.setField(CandleTableColumns.SYMBOL, StringData.fromString("TEST"));
        row.setField(CandleTableColumns.WINDOW_START, start);
        row.setField(CandleTableColumns.WINDOW_END, end);
        row.setField(CandleTableColumns.OPEN_PAISE, open);
        row.setField(CandleTableColumns.HIGH_PAISE, high);
        row.setField(CandleTableColumns.LOW_PAISE, low);
        row.setField(CandleTableColumns.CLOSE_PAISE, close);
        row.setField(CandleTableColumns.VOLUME, 100L);
        row.setField(CandleTableColumns.TICK_COUNT, 5);
        row.setField(CandleTableColumns.ALGORITHM_VERSION, StringData.fromString("candle-15s-v1"));
        row.setField(CandleTableColumns.CONFIGURATION_VERSION, StringData.fromString("1.0.0"));
        row.setField(CandleTableColumns.OUTPUT_TS, end);
        row.setField(CandleTableColumns.SCHEMA_VERSION, StringData.fromString("2"));
        return row;
    }

    /** Preview row (15 columns, CandlePreviewColumns v2 layout, is_preview=true). */
    private static GenericRowData preview(long token, long start, long end,
            long open, long high, long low, long close) {
        GenericRowData row = new GenericRowData(CandlePreviewColumns.FIELD_COUNT);
        row.setField(CandlePreviewColumns.INSTRUMENT_TOKEN, token);
        row.setField(CandlePreviewColumns.EXCHANGE, StringData.fromString("NSE"));
        row.setField(CandlePreviewColumns.SYMBOL, StringData.fromString("TEST"));
        row.setField(CandlePreviewColumns.WINDOW_START, start);
        row.setField(CandlePreviewColumns.WINDOW_END, end);
        row.setField(CandlePreviewColumns.OPEN_PAISE, open);
        row.setField(CandlePreviewColumns.HIGH_PAISE, high);
        row.setField(CandlePreviewColumns.LOW_PAISE, low);
        row.setField(CandlePreviewColumns.CLOSE_PAISE, close);
        row.setField(CandlePreviewColumns.VOLUME, 100L);
        row.setField(CandlePreviewColumns.TICK_COUNT, 5);
        row.setField(CandlePreviewColumns.IS_PREVIEW, true);
        row.setField(CandlePreviewColumns.OUTPUT_TS, end - 1_000L);
        row.setField(CandlePreviewColumns.LAST_EVENT_TS, end - 2_000L);
        row.setField(CandlePreviewColumns.SCHEMA_VERSION, StringData.fromString("2"));
        return row;
    }

    /** Feed a completed final candle for index i (window [T0+i*15s, T0+(i+1)*15s)). */
    private void feedFinal(long token, int index, long open, long high, long low, long close)
            throws Exception {
        long start = T0 + index * 15_000L;
        harness.processElement2(candle(token, start, start + 15_000L, open, high, low, close),
                start + 15_000L);
    }

    /** Feed a preview for index i's window (watermark = the preview's output ts). */
    private void feedPreview(long token, int index, long open, long high, long low, long close)
            throws Exception {
        long start = T0 + index * 15_000L;
        harness.processElement1(preview(token, start, start + 15_000L, open, high, low, close),
                start + 14_000L);
    }

    /** Copy current emitted rows AND clear the harness output (drain). */
    private static List<RowData> drain(
            KeyedTwoInputStreamOperatorTestHarness<Long, RowData, RowData, RowData> h) {
        List<RowData> result = new java.util.ArrayList<>();
        h.getOutput().stream()
                .filter(StreamRecord.class::isInstance)
                .map(o -> (RowData) ((StreamRecord<?>) o).getValue())
                .forEach(result::add);
        h.getOutput().clear();
        return result;
    }

    /** Current emitted rows without clearing. */
    private static List<RowData> rows(
            KeyedTwoInputStreamOperatorTestHarness<Long, RowData, RowData, RowData> h) {
        return drain(h);
    }

    private static String str(RowData row, int idx) {
        return row.getString(idx).toString();
    }

    @Test
    void noTentativeBeforeWarmUp() throws Exception {
        openHarness();
        // Only 2 completed candles buffered (lookback=3) → cold.
        feedFinal(1L, 0, 100, 110, 90, 105);
        feedFinal(1L, 1, 105, 115, 95, 110);
        // Preview breaks out hard — but lookback is not warm yet.
        feedPreview(1L, 2, 110, 130, 100, 128);
        assertTrue(rows(harness).isEmpty(), "no tentative before warm-up");
    }

    @Test
    void tentativeFiresOncePerWindowOnPreviewBreakout() throws Exception {
        openHarness();
        // Warm up with 3 completed candles (lookback=3).
        feedFinal(1L, 0, 100, 110, 90, 105);
        feedFinal(1L, 1, 105, 115, 95, 110);
        feedFinal(1L, 2, 110, 120, 100, 115);
        drain(harness); // drop nothing — finals produce no output

        // Window 3: first preview already breaks 120 → tentative fires once.
        feedPreview(1L, 3, 115, 130, 110, 128);
        feedPreview(1L, 3, 115, 135, 110, 132); // still holds — must NOT re-emit
        List<RowData> out = rows(harness);
        assertEquals(1, out.size(), "tentative fires exactly once per window");
        RowData tentative = out.get(0);
        assertEquals(SignalCandidatesTableColumns.VALIDITY_REASON_TENTATIVE,
                str(tentative, SignalCandidatesTableColumns.VALIDITY_REASON));
        assertTrue(str(tentative, SignalCandidatesTableColumns.CANDIDATE_ID).endsWith("-TENTATIVE"));
        assertNull(tentative.getString(SignalCandidatesTableColumns.SUPERSEDES_CANDIDATE_ID));
    }

    @Test
    void confirmPromotesTentativeWhenFinalHolds() throws Exception {
        openHarness();
        feedFinal(1L, 0, 100, 110, 90, 105);
        feedFinal(1L, 1, 105, 115, 95, 110);
        feedFinal(1L, 2, 110, 120, 100, 115);

        feedPreview(1L, 3, 115, 130, 110, 128); // tentative
        String tentativeId = str(rows(harness).get(0),
                SignalCandidatesTableColumns.CANDIDATE_ID);
        drain(harness);

        feedFinal(1L, 3, 115, 130, 110, 128); // final holds (close 128 > 120)
        List<RowData> out = rows(harness);
        assertEquals(1, out.size());
        RowData confirm = out.get(0);
        assertEquals(SignalCandidatesTableColumns.VALIDITY_REASON_CONFIRMED,
                str(confirm, SignalCandidatesTableColumns.VALIDITY_REASON));
        assertEquals(tentativeId,
                str(confirm, SignalCandidatesTableColumns.SUPERSEDES_CANDIDATE_ID));
        assertTrue(str(confirm, SignalCandidatesTableColumns.CANDIDATE_ID).endsWith("-CONFIRM"));
    }

    @Test
    void cancelSupersedesTentativeWhenFinalFails() throws Exception {
        openHarness();
        feedFinal(1L, 0, 100, 110, 90, 105);
        feedFinal(1L, 1, 105, 115, 95, 110);
        feedFinal(1L, 2, 110, 120, 100, 115);

        feedPreview(1L, 3, 115, 130, 110, 128); // tentative (breaks 120)
        String tentativeId = str(rows(harness).get(0),
                SignalCandidatesTableColumns.CANDIDATE_ID);
        drain(harness);

        feedFinal(1L, 3, 115, 122, 110, 118); // final close 118 < 120 → rule fails
        List<RowData> out = rows(harness);
        assertEquals(1, out.size());
        RowData cancel = out.get(0);
        assertEquals(SignalCandidatesTableColumns.ACTION_CANCEL,
                str(cancel, SignalCandidatesTableColumns.ACTION));
        assertEquals(SignalCandidatesTableColumns.VALIDITY_REASON_SUPERSEDED,
                str(cancel, SignalCandidatesTableColumns.VALIDITY_REASON));
        assertEquals(tentativeId,
                str(cancel, SignalCandidatesTableColumns.SUPERSEDES_CANDIDATE_ID));
        assertTrue(str(cancel, SignalCandidatesTableColumns.CANDIDATE_ID).endsWith("-CANCEL"));
    }

    @Test
    void noTentativeWhenPartialDoesNotHold() throws Exception {
        openHarness();
        feedFinal(1L, 0, 100, 110, 90, 105);
        feedFinal(1L, 1, 105, 115, 95, 110);
        feedFinal(1L, 2, 110, 120, 100, 115);
        // Preview stays below the 120 high → no tentative.
        feedPreview(1L, 3, 115, 119, 110, 117);
        assertTrue(rows(harness).isEmpty(), "no tentative when rule does not hold");
    }

    @Test
    void danglingTentativeCancelLedByTimer() throws Exception {
        openHarness();
        feedFinal(1L, 0, 100, 110, 90, 105);
        feedFinal(1L, 1, 105, 115, 95, 110);
        feedFinal(1L, 2, 110, 120, 100, 115);

        feedPreview(1L, 3, 115, 130, 110, 128); // tentative
        String tentativeId = str(rows(harness).get(0),
                SignalCandidatesTableColumns.CANDIDATE_ID);
        drain(harness);

        // Final never arrives; watermark passes windowEnd + allowedLateness.
        harness.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(
                T0 + 3 * 15_000L + 15_000L + 60_000L));
        List<RowData> out = rows(harness);
        assertEquals(1, out.size(), "dangling tentative must be CANCEL-led");
        RowData cancel = out.get(0);
        assertEquals(SignalCandidatesTableColumns.ACTION_CANCEL,
                str(cancel, SignalCandidatesTableColumns.ACTION));
        assertEquals(tentativeId,
                str(cancel, SignalCandidatesTableColumns.SUPERSEDES_CANDIDATE_ID));
    }

    @Test
    void supersessionSurvivesCheckpointRestore() throws Exception {
        openHarnessWithStateBackend();
        feedFinal(1L, 0, 100, 110, 90, 105);
        feedFinal(1L, 1, 105, 115, 95, 110);
        feedFinal(1L, 2, 110, 120, 100, 115);

        feedPreview(1L, 3, 115, 130, 110, 128); // tentative
        String tentativeId = str(rows(harness).get(0),
                SignalCandidatesTableColumns.CANDIDATE_ID);
        // Take a snapshot (checkpoint) BEFORE the final settles the window.
        OperatorSubtaskState state = harness.snapshot(1L, 0L);
        // Simulated Flink restart: a FRESH operator instance restored from the
        // checkpoint. The restore harness must be constructed DIRECTLY —
        // ProcessFunctionTestHarnesses already calls harness.open(), and
        // initializeState() after that fails (same pattern as
        // FormingBarWriterFunctionTest.restartRestoresBufferedBarAndTimer).
        KeyedTwoInputStreamOperatorTestHarness<Long, RowData, RowData, RowData> restored =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        new org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator<>(
                                new EarlySignalFunction(SignalJobConfig.from(env()))),
                        row -> row.getLong(CandlePreviewColumns.INSTRUMENT_TOKEN),
                        row -> row.getLong(CandleTableColumns.INSTRUMENT_TOKEN),
                        Types.LONG,
                        1, 1, 0);
        restored.setStateBackend(new HashMapStateBackend());
        restored.initializeState(state);
        restored.open();
        restored.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(
                T0 + 3 * 15_000L + 15_000L + 60_000L));
        // After restore the tentative must still be pending → timer CANCELs it.
        List<RowData> out = rows(restored);
        assertEquals(1, out.size(), "supersession must survive checkpoint restore");
        assertEquals(SignalCandidatesTableColumns.ACTION_CANCEL,
                str(out.get(0), SignalCandidatesTableColumns.ACTION));
        assertEquals(tentativeId,
                str(out.get(0), SignalCandidatesTableColumns.SUPERSEDES_CANDIDATE_ID));
        restored.close();
    }

    // --- Perf trim options 1+2 (2026-09-05): single-read snapshot + per-key
    // memory cache. Same signals, same timing, ~99% fewer state reads. ---

    private static List<String> idActions(List<RowData> out) {
        List<String> result = new java.util.ArrayList<>();
        for (RowData row : out) {
            result.add(str(row, SignalCandidatesTableColumns.CANDIDATE_ID)
                    + "|" + str(row, SignalCandidatesTableColumns.ACTION));
        }
        return result;
    }

    @Test
    void lookbackCachePopulatedOnPreviewAndInvalidatedOnFinal() throws Exception {
        openHarness();
        long token = 7L;
        feedFinal(token, 0, 100, 110, 90, 105);
        feedFinal(token, 1, 105, 115, 95, 110);
        feedFinal(token, 2, 110, 120, 100, 115);
        assertFalse(fn.isLookbackCached(token), "no evaluation yet -> cache must be empty");
        // Rule holds here (close 128 > max high 120, trend holds) -> tentative.
        feedPreview(token, 3, 115, 130, 110, 128);
        rows(harness);
        assertTrue(fn.isLookbackCached(token),
                "preview evaluation must populate the per-key cache");
        // Settle path evaluates BEFORE appending; append invalidates after.
        feedFinal(token, 3, 115, 130, 110, 128);
        rows(harness);
        assertFalse(fn.isLookbackCached(token),
                "final append must invalidate the per-key cache");
    }

    @Test
    void streakResetsOnFailThenRebuildsToEarlyConfirm() throws Exception {
        // Locks the guarded streak-clear path (perf trim 2026-09-05): a
        // failing preview must reset the streak so the NEXT holds start
        // from 1 — and the rebuilt streak must still early-confirm on
        // the 4th consecutive hold. Same answers with or without the
        // contains-guard; the soak's busy metric is the perf proof.
        openHarness();
        long token = 5L;
        feedFinal(token, 0, 100, 110, 90, 105);
        feedFinal(token, 1, 105, 115, 95, 110);
        feedFinal(token, 2, 110, 120, 100, 115);
        // Two holds build a streak of 2 (first emits the tentative).
        feedPreview(token, 3, 115, 130, 110, 128);
        String tentativeId = str(drain(harness).get(0),
                SignalCandidatesTableColumns.CANDIDATE_ID);
        feedPreview(token, 3, 115, 132, 110, 129);
        assertTrue(rows(harness).isEmpty(), "2nd hold emits nothing");
        // Failing preview resets the streak (close below open breaks it).
        feedPreview(token, 3, 135, 136, 110, 112);
        assertTrue(rows(harness).isEmpty(), "failed preview emits nothing");
        // Rebuild: 4 consecutive holds must early-confirm again.
        feedPreview(token, 3, 115, 130, 110, 128);
        feedPreview(token, 3, 115, 132, 110, 129);
        feedPreview(token, 3, 115, 133, 110, 130);
        assertTrue(rows(harness).isEmpty(), "rebuilt holds 1-3 emit nothing");
        feedPreview(token, 3, 115, 134, 110, 131); // 4th hold after reset
        List<RowData> out = drain(harness);
        assertEquals(1, out.size(), "4th hold after reset emits the early CONFIRM");
        assertEquals(SignalCandidatesTableColumns.VALIDITY_REASON_CONFIRMED,
                str(out.get(0), SignalCandidatesTableColumns.VALIDITY_REASON));
        assertEquals(tentativeId,
                str(out.get(0), SignalCandidatesTableColumns.SUPERSEDES_CANDIDATE_ID));
    }

    @Test
    void cachedEvaluationMatchesDirectEvaluationAfterRestore() throws Exception {
        // Restart must reload the (transient) cache from restored disk state:
        // a restored run must emit exactly what an uninterrupted run emits.
        openHarnessWithStateBackend();
        feedFinal(9L, 0, 100, 110, 90, 105);
        feedFinal(9L, 1, 105, 115, 95, 110);
        feedFinal(9L, 2, 110, 120, 100, 115);
        feedPreview(9L, 3, 115, 130, 110, 128); // tentative T3
        List<String> before = idActions(rows(harness));
        OperatorSubtaskState state = harness.snapshot(2L, 0L);
        feedFinal(9L, 3, 115, 130, 110, 128); // CONFIRM T3
        feedPreview(9L, 4, 128, 140, 125, 138); // tentative T4
        feedFinal(9L, 4, 128, 140, 125, 138); // CONFIRM T4
        before.addAll(idActions(rows(harness)));
        harness.close();
        harness = null;

        KeyedTwoInputStreamOperatorTestHarness<Long, RowData, RowData, RowData> restored =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        new org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator<>(
                                new EarlySignalFunction(SignalJobConfig.from(env()))),
                        row -> row.getLong(CandlePreviewColumns.INSTRUMENT_TOKEN),
                        row -> row.getLong(CandleTableColumns.INSTRUMENT_TOKEN),
                        Types.LONG,
                        1, 1, 0);
        restored.setStateBackend(new HashMapStateBackend());
        restored.initializeState(state);
        restored.open();
        // Cache is empty after restore: these previews/finals force the
        // reload path — outputs must still match the uninterrupted run.
        restored.processElement2(candle(9L, T0 + 3 * 15_000L, T0 + 4 * 15_000L,
                115, 130, 110, 128), T0 + 4 * 15_000L);
        restored.processElement1(preview(9L, T0 + 4 * 15_000L, T0 + 5 * 15_000L,
                128, 140, 125, 138), T0 + 4 * 15_000L + 14_000L);
        restored.processElement2(candle(9L, T0 + 4 * 15_000L, T0 + 5 * 15_000L,
                128, 140, 125, 138), T0 + 5 * 15_000L);
        List<String> after = idActions(rows(restored));
        // Suffix outputs (last 3 rows) must equal the uninterrupted suffix.
        assertEquals(before.subList(1, before.size()), after,
                "restored run must emit exactly what the uninterrupted run emitted");
        restored.close();
    }

    // --- Phase 3: confirm-window shortening (4 consecutive 1s previews) ---

    @Test
    void earlyConfirmFiresAfterFourConsecutiveHoldingPreviews() throws Exception {
        openHarness();
        feedFinal(1L, 0, 100, 110, 90, 105);
        feedFinal(1L, 1, 105, 115, 95, 110);
        feedFinal(1L, 2, 110, 120, 100, 115);

        // 1st holding preview → tentative; 2nd/3rd → streak grows (no output);
        // 4th consecutive hold → early CONFIRM superseding the tentative.
        feedPreview(1L, 3, 115, 130, 110, 128);
        List<RowData> out1 = drain(harness);
        assertEquals(1, out1.size(), "first hold emits the tentative");
        String tentativeId = str(out1.get(0), SignalCandidatesTableColumns.CANDIDATE_ID);

        feedPreview(1L, 3, 115, 132, 110, 129);
        feedPreview(1L, 3, 115, 133, 110, 130);
        assertTrue(rows(harness).isEmpty(), "holds 2-3 emit nothing");

        feedPreview(1L, 3, 115, 134, 110, 131); // 4th consecutive hold
        List<RowData> out = drain(harness);
        assertEquals(1, out.size(), "4th consecutive hold emits the early CONFIRM");
        RowData confirm = out.get(0);
        assertEquals(SignalCandidatesTableColumns.VALIDITY_REASON_CONFIRMED,
                str(confirm, SignalCandidatesTableColumns.VALIDITY_REASON));
        assertEquals(tentativeId,
                str(confirm, SignalCandidatesTableColumns.SUPERSEDES_CANDIDATE_ID));
        assertTrue(str(confirm, SignalCandidatesTableColumns.CANDIDATE_ID).endsWith("-CONFIRM"));
    }

    @Test
    void noEarlyConfirmWhenStreakBroken() throws Exception {
        openHarness();
        feedFinal(1L, 0, 100, 110, 90, 105);
        feedFinal(1L, 1, 105, 115, 95, 110);
        feedFinal(1L, 2, 110, 120, 100, 115);

        feedPreview(1L, 3, 115, 130, 110, 128); // tentative (hold 1)
        drain(harness);
        feedPreview(1L, 3, 115, 132, 110, 129); // hold 2
        feedPreview(1L, 3, 115, 125, 110, 118); // FAILED preview breaks the streak
        feedPreview(1L, 3, 115, 131, 110, 128); // hold 3 (restarted streak)
        feedPreview(1L, 3, 115, 133, 110, 130); // hold 4 — but only 2 consecutive
        assertTrue(rows(harness).isEmpty(),
                "a broken streak must not trigger the early confirm");

        // The window-end final still settles with the normal CONFIRM.
        feedFinal(1L, 3, 115, 133, 110, 130);
        List<RowData> out = drain(harness);
        assertEquals(1, out.size(), "window-end final still confirms");
        assertEquals(SignalCandidatesTableColumns.VALIDITY_REASON_CONFIRMED,
                str(out.get(0), SignalCandidatesTableColumns.VALIDITY_REASON));
    }

    @Test
    void noDoubleConfirmAfterEarlyConfirm() throws Exception {
        openHarness();
        feedFinal(1L, 0, 100, 110, 90, 105);
        feedFinal(1L, 1, 105, 115, 95, 110);
        feedFinal(1L, 2, 110, 120, 100, 115);

        feedPreview(1L, 3, 115, 130, 110, 128);
        feedPreview(1L, 3, 115, 132, 110, 129);
        feedPreview(1L, 3, 115, 133, 110, 130);
        feedPreview(1L, 3, 115, 134, 110, 131); // early CONFIRM at 4th hold
        drain(harness);

        // The window-end final arrives — the early-confirmed window must not
        // emit a second settle row (pending was cleared at early-confirm).
        feedFinal(1L, 3, 115, 134, 110, 131);
        assertTrue(rows(harness).isEmpty(), "no double confirm after early confirm");
    }

    // =====================================================================
    // CHG-121 (2026-09-01): F4 crash reconciliation via durable markers.
    // Scenario: tentative emitted pre-crash (marker written, LOG row
    // survives), TM dies before the next checkpoint, pending state rolls
    // back. On replay the final arrives with pending==null — the marker
    // must produce the settle (CONFIRM or CANCEL by the final rule).
    // =====================================================================

    /** In-memory marker hook — records marks, simulates crash survival. */
    private static class RecordingMarkerHook
            implements EarlySignalFunction.TentativeMarkerHook {
        final java.util.Set<String> markers = java.util.concurrent.ConcurrentHashMap
                .newKeySet();

        @Override
        public java.util.concurrent.CompletableFuture<Void> mark(String candidateId,
                long instrumentToken, long windowEnd) {
            markers.add(candidateId);
            return java.util.concurrent.CompletableFuture.completedFuture(null);
        }

        @Override
        public java.util.concurrent.CompletableFuture<Boolean> exists(String candidateId) {
            return java.util.concurrent.CompletableFuture.completedFuture(
                    markers.contains(candidateId));
        }

        @Override
        public java.util.concurrent.CompletableFuture<Void> clear(String candidateId) {
            markers.remove(candidateId);
            return java.util.concurrent.CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Feed a rule-failing preview for the given window — its only effect is
     * triggering the top-of-element drain (async marker path): the preview
     * itself emits nothing.
     */
    private void pumpDrain(long token, int windowIdx) throws Exception {
        feedPreview(token, windowIdx, 115, 116, 110, 112); // close 112 fails the rule
    }

    private void openHarnessWithHook(EarlySignalFunction.TentativeMarkerHook hook)
            throws Exception {
        SignalJobConfig config = SignalJobConfig.from(env());
        harness = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
                new EarlySignalFunction(config, hook),
                row -> row.getLong(CandlePreviewColumns.INSTRUMENT_TOKEN),
                row -> row.getLong(CandleTableColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();
    }

    @Test
    void crashOrphanTentativeReconciledFromMarker() throws Exception {
        RecordingMarkerHook hook = new RecordingMarkerHook();
        openHarnessWithHook(hook);
        feedFinal(1L, 0, 100, 110, 90, 105);
        feedFinal(1L, 1, 105, 115, 95, 110);
        feedFinal(1L, 2, 110, 120, 100, 115);
        feedPreview(1L, 3, 115, 130, 110, 128); // tentative + marker
        pumpDrain(1L, 3); // async drain: marker completed → tentative emitted
        String tentativeId = str(rows(harness).get(0),
                SignalCandidatesTableColumns.CANDIDATE_ID);
        assertTrue(hook.markers.contains(tentativeId), "marker written at emit");
        drain(harness);

        // CRASH: fresh harness with the same hook (durable table survived,
        // in-memory pending state did NOT — restored to a pre-tentative cp).
        harness.close();
        openHarnessWithHook(hook);
        // lookback warm-up replays (the final candles re-arrive)
        feedFinal(1L, 0, 100, 110, 90, 105);
        feedFinal(1L, 1, 105, 115, 95, 110);
        feedFinal(1L, 2, 110, 120, 100, 115);

        // The window-3 final arrives with pending==null → marker reconcile.
        feedFinal(1L, 3, 115, 130, 110, 128); // rule holds → CONFIRM
        pumpDrain(1L, 4); // async drain: lookup completed → settle emitted
        List<RowData> out = rows(harness);
        assertEquals(1, out.size(), "orphan tentative settled from marker");
        RowData confirm = out.get(0);
        assertEquals(SignalCandidatesTableColumns.VALIDITY_REASON_CONFIRMED,
                str(confirm, SignalCandidatesTableColumns.VALIDITY_REASON));
        assertEquals(tentativeId,
                str(confirm, SignalCandidatesTableColumns.SUPERSEDES_CANDIDATE_ID));
        assertTrue(hook.markers.isEmpty(), "marker cleared after settle");
    }

    @Test
    void crashOrphanTentativeCancelledWhenFinalRuleFails() throws Exception {
        RecordingMarkerHook hook = new RecordingMarkerHook();
        openHarnessWithHook(hook);
        feedFinal(1L, 0, 100, 110, 90, 105);
        feedFinal(1L, 1, 105, 115, 95, 110);
        feedFinal(1L, 2, 110, 120, 100, 115);
        feedPreview(1L, 3, 115, 130, 110, 128); // tentative on PARTIAL data
        drain(harness);

        harness.close();
        openHarnessWithHook(hook);
        feedFinal(1L, 0, 100, 110, 90, 105);
        feedFinal(1L, 1, 105, 115, 95, 110);
        feedFinal(1L, 2, 110, 120, 100, 115);

        // The FULL window final fails the rule (close 112 < 120) — the
        // orphan must settle as CANCEL, not stay dangling (F4).
        feedFinal(1L, 3, 115, 130, 110, 112);
        pumpDrain(1L, 4); // async drain: lookup completed → settle emitted
        List<RowData> out = rows(harness);
        assertEquals(1, out.size(), "orphan settled (CANCEL) from marker");
        RowData cancel = out.get(0);
        assertEquals(SignalCandidatesTableColumns.VALIDITY_REASON_SUPERSEDED,
                str(cancel, SignalCandidatesTableColumns.VALIDITY_REASON));
        assertTrue(str(cancel, SignalCandidatesTableColumns.CANDIDATE_ID)
                .endsWith("-CANCEL"));
        assertTrue(hook.markers.isEmpty(), "marker cleared after cancel");
    }

    @Test
    void noMarkerNoReconcileFinalStaysSilent() throws Exception {
        // Final with pending==null and NO marker → pre-CHG-121 behavior:
        // nothing emitted (no false CONFIRM from a nonexistent tentative).
        RecordingMarkerHook hook = new RecordingMarkerHook();
        openHarnessWithHook(hook);
        feedFinal(1L, 0, 100, 110, 90, 105);
        feedFinal(1L, 1, 105, 115, 95, 110);
        feedFinal(1L, 2, 110, 120, 100, 115);
        // (no preview → no tentative, no marker)
        feedFinal(1L, 3, 115, 130, 110, 128);
        pumpDrain(1L, 4); // async drain: lookup says absent → nothing
        assertTrue(rows(harness).isEmpty(), "no marker → no settle row");
    }

    @Test
    void markerWriteFailureSuppressesTentative() throws Exception {
        // Async rework (2026-09-02): a FAILED marker write must suppress
        // the tentative entirely — no LOG row, no pending state. That is
        // strictly safer than emitting an unmarked tentative (F4 orphan
        // risk) and safer than a false settle later.
        RecordingMarkerHook hook = new RecordingMarkerHook() {
            @Override
            public java.util.concurrent.CompletableFuture<Void> mark(String candidateId,
                    long instrumentToken, long windowEnd) {
                java.util.concurrent.CompletableFuture<Void> failed = new java.util.concurrent.CompletableFuture<>();
                failed.completeExceptionally(new RuntimeException("fluss down"));
                return failed;
            }
        };
        openHarnessWithHook(hook);
        feedFinal(1L, 0, 100, 110, 90, 105);
        feedFinal(1L, 1, 105, 115, 95, 110);
        feedFinal(1L, 2, 110, 120, 100, 115);
        feedPreview(1L, 3, 115, 130, 110, 128); // stages the mark → fails
        pumpDrain(1L, 3);
        pumpDrain(1L, 3);
        assertTrue(rows(harness).isEmpty(), "failed marker → no tentative row");
        // The final settles nothing (pending was never set) and stays silent.
        feedFinal(1L, 3, 115, 130, 110, 128);
        pumpDrain(1L, 4);
        assertTrue(rows(harness).isEmpty(), "failed marker → no settle row");
    }

    @Test
    void tentativeEmittedOnlyAfterMarkerCompletes() throws Exception {
        // Async rework (2026-09-02): the tentative LOG row must NOT be
        // emitted before the marker write is durable — that ordering IS
        // the F4 guarantee. Gate the future manually and observe both
        // sides.
        java.util.concurrent.CompletableFuture<Void> gate = new java.util.concurrent.CompletableFuture<>();
        RecordingMarkerHook hook = new RecordingMarkerHook() {
            @Override
            public java.util.concurrent.CompletableFuture<Void> mark(String candidateId,
                    long instrumentToken, long windowEnd) {
                markers.add(candidateId);
                return gate;
            }
        };
        openHarnessWithHook(hook);
        feedFinal(1L, 0, 100, 110, 90, 105);
        feedFinal(1L, 1, 105, 115, 95, 110);
        feedFinal(1L, 2, 110, 120, 100, 115);
        feedPreview(1L, 3, 115, 130, 110, 128); // marker in flight
        pumpDrain(1L, 3); // future not done → nothing emitted yet
        assertTrue(rows(harness).isEmpty(),
                "tentative must wait for the durable marker");
        gate.complete(null); // marker now durable
        pumpDrain(1L, 3); // drain emits the tentative
        List<RowData> out = rows(harness);
        assertEquals(1, out.size(), "tentative emitted after marker completes");
        assertTrue(str(out.get(0), SignalCandidatesTableColumns.CANDIDATE_ID)
                .endsWith("-TENTATIVE"));
    }

    @Test
    void markerHookSurvivesJobGraphSerialization() throws Exception {
        // 2026-09-02 root cause (drill tm-kill-full-load-20260902-014318):
        // the markerHook field was declared transient — Flink ships the
        // function from client to TaskManager by Java serialization, so the
        // hook arrived NULL on the TM and markers were silently disabled in
        // every cluster run (5,914 tentatives emitted, 0 markers written,
        // 34 F4 orphans — exactly the pre-CHG-121 behavior). Unit tests
        // construct the function directly and never serialize, which is why
        // this shipped broken twice. This test pins the fix.
        RecordingMarkerHook hook = new RecordingMarkerHook();
        EarlySignalFunction fn =
                new EarlySignalFunction(SignalJobConfig.from(env()), hook);
        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(bos)) {
            oos.writeObject(fn);
        }
        Object revived;
        try (java.io.ObjectInputStream ois = new java.io.ObjectInputStream(
                new java.io.ByteArrayInputStream(bos.toByteArray()))) {
            revived = ois.readObject();
        }
        java.lang.reflect.Field f =
                EarlySignalFunction.class.getDeclaredField("markerHook");
        f.setAccessible(true);
        Object shipped = f.get(revived);
        org.junit.jupiter.api.Assertions.assertNotNull(shipped,
                "markerHook must survive job-graph serialization (was transient → "
                        + "null on the TaskManager → markers silently disabled)");
        org.junit.jupiter.api.Assertions.assertTrue(
                shipped instanceof RecordingMarkerHook,
                "the shipped hook must be the configured hook instance type");
    }

    @Test
    void transientLookupFailureRetriesAndStillSettles() throws Exception {
        // 2026-09-02 root cause of the residual 4 orphans (drill
        // 20260902-022843): a single transient reconcile-lookup failure
        // permanently dropped the settle. The drain must RETRY a failed
        // lookup (bounded) and still settle the orphan tentative.
        java.util.concurrent.atomic.AtomicInteger failures =
                new java.util.concurrent.atomic.AtomicInteger(2);
        RecordingMarkerHook hook = new RecordingMarkerHook() {
            @Override
            public java.util.concurrent.CompletableFuture<Boolean> exists(
                    String candidateId) {
                if (failures.getAndDecrement() > 0) {
                    java.util.concurrent.CompletableFuture<Boolean> failed =
                            new java.util.concurrent.CompletableFuture<>();
                    failed.completeExceptionally(new RuntimeException("lookup timeout"));
                    return failed;
                }
                return java.util.concurrent.CompletableFuture.completedFuture(
                        markers.contains(candidateId));
            }
        };
        openHarnessWithHook(hook);
        feedFinal(1L, 0, 100, 110, 90, 105);
        feedFinal(1L, 1, 105, 115, 95, 110);
        feedFinal(1L, 2, 110, 120, 100, 115);
        feedPreview(1L, 3, 115, 130, 110, 128); // tentative + marker
        pumpDrain(1L, 3); // tentative emitted
        drain(harness);

        // Crash: fresh harness, marker table (hook) survives, state does not.
        harness.close();
        openHarnessWithHook(hook);
        feedFinal(1L, 0, 100, 110, 90, 105);
        feedFinal(1L, 1, 105, 115, 95, 110);
        feedFinal(1L, 2, 110, 120, 100, 115);
        feedFinal(1L, 3, 115, 130, 110, 128); // reconcile → lookup FAILS (1st)
        pumpDrain(1L, 4); // drain: fail → retry scheduled
        pumpDrain(1L, 5); // drain: fail again (2nd) → retry scheduled
        pumpDrain(1L, 6); // drain: 3rd lookup succeeds → settle emitted
        List<RowData> out = rows(harness);
        assertEquals(1, out.size(), "orphan settled after transient lookup failures");
        RowData settle = out.get(0);
        assertEquals(SignalCandidatesTableColumns.VALIDITY_REASON_CONFIRMED,
                str(settle, SignalCandidatesTableColumns.VALIDITY_REASON),
                "settle is a CONFIRM by the final rule");
        assertTrue(hook.markers.isEmpty(), "marker cleared after the retried settle");
    }

    @Test
    void everySettlePathClearsItsMarker() throws Exception {
        // 2026-09-02: only the reconcile path cleared markers; the normal
        // settle, early confirm, and late-drop cancel all left stale
        // markers (2d TTL). A stale marker + replayed final = duplicate
        // settle. Guard: after ALL settle paths the marker must be gone.
        // Path 1: normal final settle.
        RecordingMarkerHook hook = new RecordingMarkerHook();
        openHarnessWithHook(hook);
        feedFinal(1L, 0, 100, 110, 90, 105);
        feedFinal(1L, 1, 105, 115, 95, 110);
        feedFinal(1L, 2, 110, 120, 100, 115);
        feedPreview(1L, 3, 115, 130, 110, 128); // tentative + marker
        pumpDrain(1L, 3);
        assertFalse(hook.markers.isEmpty(), "marker present while tentative pending");
        feedFinal(1L, 3, 115, 130, 110, 125); // normal settle (rule holds)
        assertTrue(hook.markers.isEmpty(), "marker cleared by the NORMAL settle path");
        drain(harness); // the CONFIRM row itself is expected — clear it
        // The settled window's replayed final (pending==null) must find NO
        // marker — no duplicate settle.
        feedFinal(1L, 3, 115, 130, 110, 125);
        pumpDrain(1L, 4);
        // the replayed final enters the lookback only — no new candidate row
        assertTrue(rows(harness).isEmpty(),
                "no duplicate settle from the stale marker");

        // Path 2: late-drop cancel. Window 4 tentative, no final → timer
        // fires at windowEnd + lateness → CANCEL must clear the marker.
        // close must beat window-3's high (130) for the rule to hold.
        feedPreview(1L, 4, 115, 135, 110, 132);
        pumpDrain(1L, 4);
        drain(harness);
        assertFalse(hook.markers.isEmpty(), "marker present for window-4 tentative");
        harness.processBothWatermarks(new org.apache.flink.streaming.api.watermark.Watermark(
                T0 + 4 * 15_000L + 15_000L + 60_000L));
        drain(harness); // the CANCEL row itself is expected
        assertTrue(hook.markers.isEmpty(), "marker cleared by the LATE-DROP cancel");
    }

}
