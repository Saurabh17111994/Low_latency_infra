package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Option B strict feedback: CLOSED clears active, next signal may pass.
 * Two-input harness: input1 = signals, input2 = Position_State.
 */
class ActiveSignalFeedbackFunctionTest {

    private KeyedTwoInputStreamOperatorTestHarness<Long, RowData, RowData, RowData> harness;

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) harness.close();
    }

    private void open() throws Exception {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_TTL_MS", "60000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        SignalJobConfig config = SignalJobConfig.from(env);
        harness = ProcessFunctionTestHarnesses.forKeyedCoProcessFunction(
                new ActiveSignalFeedbackFunction(config),
                row -> row.getLong(SignalCandidatesTableColumns.INSTRUMENT_TOKEN),
                row -> row.getLong(PositionStateTableColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();
    }

    private static RowData candidate(long token, String id) {
        GenericRowData r = new GenericRowData(SignalCandidatesTableColumns.FIELD_COUNT);
        r.setField(SignalCandidatesTableColumns.CANDIDATE_ID, StringData.fromString(id));
        r.setField(SignalCandidatesTableColumns.INSTRUMENT_TOKEN, token);
        r.setField(SignalCandidatesTableColumns.EXCHANGE, StringData.fromString("NSE"));
        r.setField(SignalCandidatesTableColumns.SYMBOL, StringData.fromString("TEST"));
        r.setField(SignalCandidatesTableColumns.STRATEGY_ID, StringData.fromString("simple-breakout"));
        r.setField(SignalCandidatesTableColumns.STRATEGY_VERSION, StringData.fromString("1.0.0"));
        r.setField(SignalCandidatesTableColumns.RULE_ID, StringData.fromString("breakout-20-bullish-trend"));
        r.setField(SignalCandidatesTableColumns.DETECTION_TS, 1000L);
        r.setField(SignalCandidatesTableColumns.EVALUATION_TS, 1000L);
        r.setField(SignalCandidatesTableColumns.ACTION, StringData.fromString("ENTRY"));
        r.setField(SignalCandidatesTableColumns.SIDE, StringData.fromString("BUY"));
        r.setField(SignalCandidatesTableColumns.QUANTITY, 1L);
        r.setField(SignalCandidatesTableColumns.ORDER_TYPE, StringData.fromString("MARKET"));
        r.setField(SignalCandidatesTableColumns.SCHEMA_VERSION, StringData.fromString("2"));
        return r;
    }

    private static RowData positionState(long token, String status) {
        GenericRowData r = new GenericRowData(PositionStateTableColumns.FIELD_COUNT);
        r.setField(PositionStateTableColumns.INSTRUMENT_TOKEN, token);
        r.setField(PositionStateTableColumns.STATUS, StringData.fromString(status));
        r.setField(PositionStateTableColumns.POSITION_ID, StringData.fromString("pos-" + token));
        r.setField(PositionStateTableColumns.UPDATED_TS, 2000L);
        r.setField(PositionStateTableColumns.CLOSED_TS, status.equals("CLOSED") ? 2000L : null);
        r.setField(PositionStateTableColumns.CLOSED_REASON, null);
        r.setField(PositionStateTableColumns.SCHEMA_VERSION, StringData.fromString("1"));
        return r;
    }

    private List<RowData> emitted() {
        return harness.getOutput().stream()
                .filter(o -> o instanceof StreamRecord)
                .map(o -> (RowData) ((StreamRecord<?>) o).getValue())
                .toList();
    }

    @Test
    void blockedUntilClosedThenPasses() throws Exception {
        open();
        harness.processElement1(candidate(1L, "id-1"), 1000L);
        assertEquals(1, emitted().size());

        harness.processElement1(candidate(1L, "id-2"), 2000L);
        assertEquals(1, emitted().size(), "still blocked indefinite");

        // CLOSED from Nautilus
        harness.processElement2(positionState(1L, "CLOSED"), 2500L);
        // next signal must pass
        harness.processElement1(candidate(1L, "id-3"), 3000L);
        assertEquals(2, emitted().size(), "after CLOSED next signal must pass");
    }

    @Test
    void openDoesNotClear() throws Exception {
        open();
        harness.processElement1(candidate(2L, "id-a"), 1000L);
        harness.processElement2(positionState(2L, "OPEN"), 1500L);
        harness.processElement1(candidate(2L, "id-b"), 2000L);
        assertEquals(1, emitted().size(), "OPEN must not clear active");
    }

    @Test
    void differentTokenNotAffected() throws Exception {
        open();
        harness.processElement1(candidate(10L, "id-1"), 1000L);
        harness.processElement2(positionState(20L, "CLOSED"), 1500L);
        harness.processElement1(candidate(10L, "id-2"), 2000L);
        assertEquals(1, emitted().size(), "CLOSED for 20 must not free 10");
        harness.processElement2(positionState(10L, "CLOSED"), 2500L);
        harness.processElement1(candidate(10L, "id-3"), 3000L);
        assertEquals(2, emitted().size());
    }

    @Test
    void indefiniteBlockSurvivesProcessingTimeAdvance() throws Exception {
        open();
        harness.processElement1(candidate(30L, "id-1"), 1000L);
        assertEquals(1, emitted().size());

        // Advance processing time far past any plausible TTL — the block must
        // persist (no timer, no TTL; only CLOSED/ADMIN_CLEAR clears it).
        harness.setProcessingTime(60_000L);
        harness.processElement1(candidate(30L, "id-2"), 1001L);
        assertEquals(1, emitted().size(), "processing-time advance must not clear the block");

        harness.setProcessingTime(3_700_000L); // 1h+
        harness.processElement1(candidate(30L, "id-3"), 1002L);
        assertEquals(1, emitted().size(), "still blocked after 1h — wait for CLOSED");
    }

    @Test
    void remainsBlockedAcrossManyWindows() throws Exception {
        open();
        harness.processElement1(candidate(40L, "id-1"), 1000L);
        assertEquals(1, emitted().size());

        // Simulate many window boundaries passing without any CLOSED feedback.
        for (int w = 1; w <= 10; w++) {
            long ts = w * 15_000L;
            harness.setProcessingTime(ts);
            harness.processElement1(candidate(40L, "id-" + (w + 1)), ts);
        }
        assertEquals(1, emitted().size(), "indefinite — no auto-free across windows");
    }
}
