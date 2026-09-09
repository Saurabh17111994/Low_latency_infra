package com.trading.compute.safetyhalt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.common.safety.SlotAssignmentResolver;
import com.trading.common.safety.SlotSafetyRequest;
import com.trading.common.safety.SlotSafetyStatus;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * P2-010/P2-011/P2-117: the safety-halt apply function is never fatal on a
 * bad row, forwards only real transitions, and counts honestly. A null-column
 * row is a counted malformed skip (not a task-killing NPE); a duplicate or
 * trust-gate-rejected row is a counted skip at DEBUG (not an INFO forward and
 * not an applied count) — so full() replays neither loop the job nor
 * masquerade as new halts, and distrusted rows never reach downstream.
 */
@DisplayName("SafetyHaltApplyFunction: never-fatal, forward-only-real, honest counts")
class SafetyHaltApplyFunctionTest {

    private SlotAssignmentResolver assignment;
    private OneInputStreamOperatorTestHarness<RowData, SlotSafetyRequest> harness;

    @BeforeEach
    void setUp() throws Exception {
        assignment = SlotAssignmentResolver.of(List.of(1L, 2L, 3L), 1, 3);
        harness = new OneInputStreamOperatorTestHarness<>(
                new StreamFlatMap<>(new SafetyHaltJob.SafetyHaltApplyFunction(assignment)));
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    @Test
    @DisplayName("P2-010: null-column row is a counted skip, never fatal")
    void nullColumnRowSkippedNotFatal() throws Exception {
        GenericRowData row = validRow("hft-0", 5L, SlotSafetyStatus.UNSAFE, "FEED_STALLED");
        row.setField(0, null); // halt_request_id null -> bridge NPE before the fix
        harness.processElement(new StreamRecord<>(row));

        assertTrue(mainOutput().isEmpty(), "bad row must not move forward");
    }

    @Test
    @DisplayName("P2-011+P2-117: UNSAFE forwards once; its replay is a silent skip")
    void unsafeForwardsOnceThenReplaySkipped() throws Exception {
        harness.processElement(
                new StreamRecord<>(validRow("hft-0", 5L, SlotSafetyStatus.UNSAFE, "FEED_STALLED")));
        assertEquals(1, mainOutput().size(), "first UNSAFE must move forward");

        harness.processElement(
                new StreamRecord<>(validRow("hft-0", 5L, SlotSafetyStatus.UNSAFE, "FEED_STALLED")));
        assertEquals(1, mainOutput().size(), "duplicate replay must not move forward again");
    }

    @Test
    @DisplayName("P2-011: untrusted slot row never moves forward")
    void unknownSlotNeverForwarded() throws Exception {
        harness.processElement(
                new StreamRecord<>(validRow("hft-9", 5L, SlotSafetyStatus.UNSAFE, "FEED_STALLED")));
        assertTrue(mainOutput().isEmpty(), "distrusted row must never reach downstream");
    }

    @Test
    @DisplayName("P2-012: wrong-arity row fails as ParseException, never mis-maps")
    void wrongArityRejected() throws Exception {
        GenericRowData shortRow = new GenericRowData(RowKind.INSERT, 5);
        shortRow.setField(0, StringData.fromString("req-x"));
        harness.processElement(new StreamRecord<>(shortRow));
        assertTrue(mainOutput().isEmpty(), "short row must not move forward");
    }

    @Test
    @DisplayName("P2-013: DELETE row rejected by the bridge contract")
    void deleteRowRejected() throws Exception {
        GenericRowData del = validRow("hft-0", 5L, SlotSafetyStatus.UNSAFE, "FEED_STALLED");
        del.setRowKind(RowKind.DELETE);
        harness.processElement(new StreamRecord<>(del));
        assertTrue(mainOutput().isEmpty(), "DELETE must never parse as a halt");
    }

    @Test
    @DisplayName("P2-015: null epoch row is a counted skip, never epoch-0")
    void nullEpochSkipped() throws Exception {
        GenericRowData row = validRow("hft-0", 5L, SlotSafetyStatus.UNSAFE, "FEED_STALLED");
        row.setField(15, null);
        harness.processElement(new StreamRecord<>(row));
        assertTrue(mainOutput().isEmpty(), "null epoch must not become epoch 0");
    }

    private List<SlotSafetyRequest> mainOutput() {
        List<SlotSafetyRequest> out = new ArrayList<>();
        for (Object o : harness.getOutput()) {
            out.add((SlotSafetyRequest) ((StreamRecord<?>) o).getValue());
        }
        return out;
    }

    private GenericRowData validRow(String slotId, long epoch, SlotSafetyStatus status,
            String reason) {
        GenericRowData row = new GenericRowData(RowKind.INSERT, 21);
        row.setField(0, StringData.fromString("req-" + slotId + "-" + epoch));
        row.setField(4, StringData.fromString(
                SlotSafetyRequest.SOURCE_COMPONENT_INGESTION));
        row.setField(6, StringData.fromString(reason));
        row.setField(8, epoch * 1000L);
        row.setField(14, StringData.fromString(slotId));
        row.setField(15, epoch);
        row.setField(16, StringData.fromString(assignment.manifestFingerprint()));
        row.setField(17, StringData.fromString(
                "hft-9".equals(slotId) ? "bogus-hash" : assignment.tokenSetHashOf("hft-0")));
        row.setField(18, StringData.fromString(status.name()));
        row.setField(20, 2);
        return row;
    }
}
