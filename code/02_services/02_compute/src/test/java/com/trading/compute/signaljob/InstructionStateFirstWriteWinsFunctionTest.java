package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * P4-076 pure-JVM unit tests for
 * {@link TradeDecisionsSinks.InstructionStateFirstWriteWinsFunction}: the
 * keyed first-write-wins guard in front of the
 * {@code trade_instruction_state} KV index sink. Mirrors the
 * {@code MultiTimeframeClosedFirstWriteWinsFunction} tests in
 * {@code MultiTimeframeSinksTest}`.
 */
@DisplayName("P4-076: instruction-state first-write-wins filter")
class InstructionStateFirstWriteWinsFunctionTest {

    private TradeDecisionsSinks.InstructionStateFirstWriteWinsFunction function;
    private KeyedOneInputStreamOperatorTestHarness<String, RowData, RowData> harness;

    @BeforeEach
    void setUp() throws Exception {
        function = new TradeDecisionsSinks.InstructionStateFirstWriteWinsFunction();
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                function,
                TradeDecisionsSinks.InstructionStateFirstWriteWinsFunction.keySelector(),
                Types.STRING);
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    private static RowData decision(String instructionId) {
        TradeDecision d = TradeDecisionBuilderTest.sampleDecision();
        GenericRowData row = (GenericRowData) TradeDecisionBuilder.build(d);
        row.setField(TradeDecisionsTableColumns.INSTRUCTION_ID, StringData.fromString(instructionId));
        return row;
    }

    private long forwardedCount() {
        return harness.getOutput().stream()
                .filter(o -> o instanceof StreamRecord)
                .count();
    }

    @Test
    @DisplayName("first emission forwards; second emission of same instruction_id is dropped and counted")
    void firstWinsSecondDroppedAndCounted() throws Exception {
        harness.processElement(new StreamRecord<>(decision("ins-v1-aaa")));
        harness.processElement(new StreamRecord<>(decision("ins-v1-aaa")));

        assertEquals(1L, forwardedCount(),
                "exactly one row may reach the KV index sink per instruction_id");
        assertEquals(1L, function.duplicateInstructionCountForTest(),
                "second emission must increment compute.trade_decisions.duplicate_instruction");

        harness.processElement(new StreamRecord<>(decision("ins-v1-aaa")));
        assertEquals(1L, forwardedCount(), "still exactly one forwarded row");
        assertEquals(2L, function.duplicateInstructionCountForTest(),
                "every second+ emission is counted once");
    }

    @Test
    @DisplayName("forwarded row is the first emission itself, unchanged")
    void forwardedRowIsFirstEmission() throws Exception {
        RowData first = decision("ins-v1-bbb");
        harness.processElement(new StreamRecord<>(first));
        harness.processElement(new StreamRecord<>(decision("ins-v1-bbb")));

        StreamRecord<?> out = (StreamRecord<?>) harness.getOutput().stream()
                .filter(o -> o instanceof StreamRecord)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no forwarded row"));
        assertEquals(first, out.getValue(),
                "forwarded row must carry FIRST emission's content — never rewritten");
    }

    @Test
    @DisplayName("distinct instruction_id is a fresh key — forwarded, not counted")
    void distinctIdForwards() throws Exception {
        harness.processElement(new StreamRecord<>(decision("ins-v1-aaa")));
        harness.processElement(new StreamRecord<>(decision("ins-v1-bbb")));

        assertEquals(2L, forwardedCount(),
                "each distinct instruction_id is a separate first write");
        assertEquals(0L, function.duplicateInstructionCountForTest(),
                "no cross-id collision — duplicate counter stays zero");
    }

    @Test
    @DisplayName("empty input emits nothing and counts nothing")
    void emptyInputEmitsNothing() {
        assertEquals(0L, forwardedCount(), "no element -> no row");
        assertEquals(0L, function.duplicateInstructionCountForTest());
    }

    @Test
    @DisplayName("state contract: one Boolean marker per distinct instruction_id, no timers, no payload")
    void stateIsOneBooleanPerKeyNoTimers() throws Exception {
        harness.processElement(new StreamRecord<>(decision("ins-v1-aaa")));
        harness.processElement(new StreamRecord<>(decision("ins-v1-bbb")));

        assertEquals(2, harness.numKeyedStateEntries(),
                "one Boolean written-marker per distinct instruction_id, nothing else");
        assertEquals(0, harness.numEventTimeTimers(),
                "no hand-rolled timers — native StateTtlConfig expires markers");
    }

    @Test
    @DisplayName("after marker TTL elapses a re-arrival is a fresh first write")
    void expiredMarkerReAdmitsAsFirstWrite() throws Exception {
        harness.processElement(new StreamRecord<>(decision("ins-v1-aaa")));
        harness.processElement(new StreamRecord<>(decision("ins-v1-aaa")));
        assertEquals(1L, forwardedCount());
        assertEquals(1L, function.duplicateInstructionCountForTest());

        harness.setStateTtlProcessingTime(
                TradeDecisionsSinks.InstructionStateFirstWriteWinsFunction.WRITTEN_MARK_TTL
                        .toMillis() + 1L);

        harness.processElement(new StreamRecord<>(decision("ins-v1-aaa")));
        assertEquals(2L, forwardedCount(), "post-TTL re-arrival forwards again");
        assertEquals(1L, function.duplicateInstructionCountForTest(),
                "post-TTL re-arrival is NOT counted as duplicate");
        assertEquals(1, harness.numKeyedStateEntries(),
                "re-admitted marker replaced expired one — state never grows");
    }
}
