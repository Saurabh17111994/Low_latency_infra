package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    private void openHarness() throws Exception {
        SignalJobConfig config = SignalJobConfig.from(env());
        harness = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
                new EarlySignalFunction(config),
                row -> row.getLong(CandlePreviewColumns.INSTRUMENT_TOKEN),
                row -> row.getLong(CandleTableColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();
    }

    private void openHarnessWithStateBackend() throws Exception {
        SignalJobConfig config = SignalJobConfig.from(env());
        harness = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
                new EarlySignalFunction(config),
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

    /** Preview row (14 columns, CandlePreviewColumns layout, is_preview=true). */
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
        row.setField(CandlePreviewColumns.SCHEMA_VERSION, StringData.fromString("1"));
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
}
