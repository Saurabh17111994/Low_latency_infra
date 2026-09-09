package com.trading.compute.babysitter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.common.model.PositionState;
import com.trading.common.schema.KvStateUpdateProtocol;
import com.trading.common.schema.position.PositionSnapshot;
import com.trading.common.schema.position.PositionsColumns;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * BAB-OBS-001 (Task 7): the {@code PositionsObservationOperator} retains the
 * latest accepted version per {@code position_id} under {@code KvStateUpdateProtocol}
 * semantics, emits zero records on the MAIN stream (no {@code Position_Actions}),
 * exposes each row's disposition on the observability side output, and restores
 * its checkpointed observation state — replaying a duplicate after restore is a
 * no-op, not a re-apply. Cluster-free via the Flink 2.2.1 operator harness.
 */
@DisplayName("BAB-OBS-001: Positions observation operator (version gate + restore + zero main output)")
class PositionsObservationOperatorTest {

    private KeyedOneInputStreamOperatorTestHarness<String, PositionSnapshot, Void> harness;

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    private void openHarness(PositionsObservationOperator op) throws Exception {
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                op, PositionSnapshot::positionId, Types.STRING);
        harness.open();
    }

    private static PositionSnapshot snap(String pid, String event, long version,
            long open, long closed, long lastUpdate) {
        return new PositionSnapshot(pid, "tc", "acct", 7L, "NSE", "TEST", "BUY",
                PositionState.OPEN, open, closed, 0L, 0L, event, version, 1_000L,
                lastUpdate, PositionsColumns.SCHEMA_VERSION_V2);
    }

    private void process(PositionSnapshot snap) throws Exception {
        harness.processElement(snap, snap.lastUpdateTs());
    }

    /** Side-output dispositions in order. */
    private List<KvStateUpdateProtocol.Outcome> dispositions() {
        return harness.getSideOutput(PositionsObservationOperator.DISPOSITION).stream()
                .map(StreamRecord::getValue)
                .collect(Collectors.toList());
    }

    /** Stale side-output rows (empty when nothing stale arrived). */
    private List<PositionSnapshot> staleRows() {
        var q = harness.getSideOutput(PositionsObservationOperator.STALE);
        if (q == null) {
            return List.of();
        }
        return q.stream().map(StreamRecord::getValue).collect(Collectors.toList());
    }

    @Test
    @DisplayName("clean newer versions are applied (APPLIED) with zero main output")
    void appliesNewerVersion() throws Exception {
        PositionsObservationOperator op = new PositionsObservationOperator();
        openHarness(op);
        process(snap("POS-1", "ev-1", 1L, 10, 0, 2_000L));
        process(snap("POS-1", "ev-2", 2L, 20, 0, 3_000L));

        assertEquals(List.of(KvStateUpdateProtocol.Outcome.APPLIED,
                KvStateUpdateProtocol.Outcome.APPLIED), dispositions());
        assertTrue(harness.getOutput().isEmpty(), "main stream must stay empty (zero actions)");
    }

    @Test
    @DisplayName("duplicate (same version/event) is a no-op, not a re-apply")
    void duplicateIsNoOp() throws Exception {
        PositionsObservationOperator op = new PositionsObservationOperator();
        openHarness(op);
        process(snap("POS-1", "ev-1", 1L, 10, 0, 2_000L));
        process(snap("POS-1", "ev-1", 1L, 10, 0, 2_000L));

        assertEquals(List.of(KvStateUpdateProtocol.Outcome.APPLIED,
                KvStateUpdateProtocol.Outcome.DUPLICATE), dispositions());
        assertTrue(harness.getOutput().isEmpty());
    }

    @Test
    @DisplayName("stale (older version, same event) is counted and never regresses state")
    void staleIsIgnored() throws Exception {
        PositionsObservationOperator op = new PositionsObservationOperator();
        openHarness(op);
        process(snap("POS-1", "ev-5", 5L, 50, 0, 5_000L));
        process(snap("POS-1", "ev-5", 3L, 30, 0, 3_000L)); // same event, older version

        assertEquals(List.of(KvStateUpdateProtocol.Outcome.APPLIED,
                KvStateUpdateProtocol.Outcome.STALE), dispositions());
        assertTrue(harness.getOutput().isEmpty());
    }

    @Test
    @DisplayName("conflict (same version, different source event) is never applied")
    void conflictNeverApplied() throws Exception {
        PositionsObservationOperator op = new PositionsObservationOperator();
        openHarness(op);
        process(snap("POS-1", "ev-5", 5L, 50, 0, 5_000L));
        process(snap("POS-1", "ev-OTHER", 5L, 99, 0, 6_000L));

        assertEquals(List.of(KvStateUpdateProtocol.Outcome.APPLIED,
                KvStateUpdateProtocol.Outcome.CONFLICT), dispositions());
        assertTrue(harness.getOutput().isEmpty());
    }

    @Test
    @DisplayName("regression (older version, different event) is never applied")
    void regressionNeverApplied() throws Exception {
        PositionsObservationOperator op = new PositionsObservationOperator();
        openHarness(op);
        process(snap("POS-1", "ev-5", 5L, 50, 0, 5_000L));
        process(snap("POS-1", "ev-2", 2L, 25, 0, 2_000L)); // older version, different event

        assertEquals(List.of(KvStateUpdateProtocol.Outcome.APPLIED,
                KvStateUpdateProtocol.Outcome.REGRESSION), dispositions());
        assertTrue(harness.getOutput().isEmpty());
    }

    @Test
    @DisplayName("P2-006: first-seen negative version is UNKNOWN, key stays recoverable")
    void firstSeenNegativeVersionIsUnknown() throws Exception {
        PositionsObservationOperator op = new PositionsObservationOperator();
        openHarness(op);
        process(snap("POS-1", "ev-bad", -1L, 10, 0, 2_000L));
        assertEquals(List.of(KvStateUpdateProtocol.Outcome.UNKNOWN), dispositions());

        // Key was never poisoned: a clean v1 applies afterwards.
        process(snap("POS-1", "ev-1", 1L, 10, 0, 3_000L));
        assertEquals(List.of(KvStateUpdateProtocol.Outcome.UNKNOWN,
                KvStateUpdateProtocol.Outcome.APPLIED), dispositions());
        assertTrue(harness.getOutput().isEmpty());
    }

    @Test
    @DisplayName("P2-114: stale arrival is side-output when beyond the threshold")
    void staleArrivalSignalled() throws Exception {
        PositionsObservationOperator op = new PositionsObservationOperator(60_000L);
        openHarness(op);
        harness.setProcessingTime(200_000L);
        // lastUpdate 2_000 vs now 200_000: stale by 198s > 60s threshold.
        process(snap("POS-1", "ev-1", 1L, 10, 0, 2_000L));
        assertEquals(List.of(snap("POS-1", "ev-1", 1L, 10, 0, 2_000L)), staleRows());
        assertTrue(harness.getOutput().isEmpty());
    }

    @Test
    @DisplayName("P2-114: fresh arrival stays silent on the stale channel")
    void freshArrivalSilent() throws Exception {
        PositionsObservationOperator op = new PositionsObservationOperator(60_000L);
        openHarness(op);
        harness.setProcessingTime(10_000L);
        process(snap("POS-1", "ev-1", 1L, 10, 0, 2_000L));
        assertTrue(staleRows().isEmpty());
    }

    @Test
    @DisplayName("observation state survives checkpoint restore; a replayed duplicate is a no-op")
    void stateRestoresFromCheckpoint() throws Exception {
        // Phase 1: apply v1, checkpoint, close.
        PositionsObservationOperator op1 = new PositionsObservationOperator();
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                op1, PositionSnapshot::positionId, Types.STRING);
        harness.open();
        process(snap("POS-1", "ev-1", 1L, 10, 0, 2_000L));
        assertEquals(List.of(KvStateUpdateProtocol.Outcome.APPLIED), dispositions());
        OperatorSubtaskState state = harness.snapshot(1L, 0L);
        harness.close();
        harness = null;

        // Phase 2: fresh operator + DIRECT harness (the forKeyedProcessFunction
        // convenience auto-initializes and rejects initializeState), restores the
        // checkpoint on a different instance (like a fresh TaskManager).
        PositionsObservationOperator op2 = new PositionsObservationOperator();
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(op2),
                PositionSnapshot::positionId,
                Types.STRING);
        harness.initializeState(state);
        harness.open();

        // Replay v1 (was checkpointed) -> DUPLICATE, NOT re-applied: proves the
        // restored state survived the fresh-worker restore.
        process(snap("POS-1", "ev-1", 1L, 10, 0, 2_000L));
        assertEquals(List.of(KvStateUpdateProtocol.Outcome.DUPLICATE), dispositions(),
                "restored state must make the replayed v1 a duplicate, not a re-apply");

        // A clean newer version applies on top of the restored state.
        process(snap("POS-1", "ev-3", 3L, 30, 0, 4_000L));
        assertEquals(List.of(KvStateUpdateProtocol.Outcome.DUPLICATE,
                KvStateUpdateProtocol.Outcome.APPLIED), dispositions(),
                "v3 applies on top of the restored v1");

        // A different key observed only post-restore applies independently.
        process(snap("POS-2", "ev-1", 1L, 7, 0, 2_000L));
        assertEquals(List.of(KvStateUpdateProtocol.Outcome.DUPLICATE,
                KvStateUpdateProtocol.Outcome.APPLIED,
                KvStateUpdateProtocol.Outcome.APPLIED), dispositions(),
                "POS-2 applies independently after restore");
        assertTrue(harness.getOutput().isEmpty(),
                "main stream must stay empty across restore and multi-key (zero actions)");
    }
}
