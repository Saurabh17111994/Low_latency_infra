package com.trading.common.schema.execution;

import static org.assertj.core.api.Assertions.assertThat;

import com.trading.common.model.GateState;
import com.trading.common.schema.ownership.ExecutionGateColumns;
import java.lang.reflect.Method;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * P3-364/P3-369: {@code detection_time} was written as null with a "no source" note. The source
 * exists — {@code SafetyHaltRequest.detectionTime}, carried on the safety path and previously
 * discarded at the halt call site. It is now recorded, and it is a genuinely different event from
 * the transition time (evidence is detected, then replayed into the gate later).
 *
 * <p>The two timestamps below differ deliberately: conflating them must fail these assertions.
 */
class GateDetectionTimeTest {

    private static final long DETECTED_TS = 500L;
    private static final long HALT_TS = 9_999L;

    private static InMemoryGateStateStore store(GateState state) {
        InMemoryGateStateStore store = new InMemoryGateStateStore(Set.of("saurabh"));
        store.init(new GateRow("p1", "acct1", state, 0L, "boot", "h0", null, null, null, null, 0L,
                null, null, null));
        return store;
    }

    private static Object[] encode(GateRow r) throws Exception {
        Method m = FlussGateStateStore.class.getDeclaredMethod("encode", GateRow.class);
        m.setAccessible(true);
        return (Object[]) m.invoke(null, r);
    }

    @Test
    @DisplayName("a halt records the detection time distinctly from the transition time")
    void haltRecordsDetectionTimeDistinctFromTransition() {
        InMemoryGateStateStore s = store(GateState.ENABLED);

        GateRow halted = s.halt("p1", s.read("p1"), "SAFETY:HALT", "ev", HALT_TS, DETECTED_TS);

        assertThat(halted.detectionTs()).isEqualTo(DETECTED_TS);
        assertThat(halted.transitionTs()).isEqualTo(HALT_TS);
        assertThat(halted.detectionTs())
                .as("detection and application are different events and must not be conflated")
                .isNotEqualTo(halted.transitionTs());
    }

    @Test
    @DisplayName("the already-HALTED branch also records the detection time")
    void idempotentHaltBranchAlsoRecordsDetection() {
        InMemoryGateStateStore s = store(GateState.HALTED);

        GateRow halted = s.halt("p1", s.read("p1"), "SAFETY:HALT", "ev", HALT_TS, DETECTED_TS);

        assertThat(halted.state()).isEqualTo(GateState.HALTED);
        assertThat(halted.detectionTs()).isEqualTo(DETECTED_TS);
        assertThat(halted.epoch()).as("idempotent halt must not bump the epoch").isZero();
    }

    @Test
    @DisplayName("a halt with no detection records null, not a borrowed timestamp")
    void haltWithoutDetectionRecordsNull() {
        InMemoryGateStateStore s = store(GateState.ENABLED);

        GateRow halted = s.halt("p1", s.read("p1"), "manual halt", "ev", HALT_TS);

        assertThat(halted.detectionTs())
                .as("no detection occurred, so the nullable column stays null")
                .isNull();
        assertThat(halted.transitionTs()).isEqualTo(HALT_TS);
    }

    @Test
    @DisplayName("the durable encoder writes the detection time and leaves it null when absent")
    void durableEncodingRoundTripsDetectionTime() throws Exception {
        GateRow detected = store(GateState.ENABLED)
                .halt("p1", null, "SAFETY:HALT", "ev", HALT_TS, DETECTED_TS);
        GateRow undetected = store(GateState.ENABLED).halt("p1", null, "manual halt", "ev", HALT_TS);

        assertThat(((Number) encode(detected)[ExecutionGateColumns.DETECTION_TIME]).longValue())
                .isEqualTo(DETECTED_TS);
        assertThat(encode(undetected)[ExecutionGateColumns.DETECTION_TIME])
                .as("the column is nullable in Execution_Gate; null is honest")
                .isNull();
    }

    @Test
    @DisplayName("a later state transition does not carry a stale detection forward")
    void stateTransitionClearsAStaleDetection() {
        GateRow detected = new GateRow("p1", "acct1", GateState.HALTED, 1L, "SAFETY:HALT", "ev",
                null, null, null, null, 5L, null, null, HALT_TS, HALT_TS, DETECTED_TS);

        GateRow reenabled = detected.withState(GateState.ENABLED, "operator re-armed", "ev2", HALT_TS + 1);

        assertThat(reenabled.detectionTs())
                .as("the previous fault detection must not look like part of this transition")
                .isNull();
        assertThat(reenabled.transitionTs()).isEqualTo(HALT_TS + 1);
    }

    @Test
    @DisplayName("fence copies preserve the recorded detection")
    void fenceCopiesPreserveDetection() {
        GateRow detected = new GateRow("p1", "acct1", GateState.HALTED, 1L, "SAFETY:HALT", "ev",
                null, null, null, null, 5L, null, null, HALT_TS, HALT_TS, DETECTED_TS);

        assertThat(detected.withFence("inst-2", 9L, HALT_TS, 1_000L).detectionTs())
                .isEqualTo(DETECTED_TS);
        assertThat(detected.withFenceCleared(HALT_TS + 5).detectionTs()).isEqualTo(DETECTED_TS);
    }
}
