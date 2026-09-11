package com.trading.common.safety;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Decision gate (plan.md &sect; "Slot-scoped safety propagation", rules 2-3):
 * suppress new work and discard unpublished in-flight decisions while the
 * owning slot is unsafe; admit only post-recovery input after RECOVERED.
 */
@DisplayName("SuppressionGate: ALLOW / SUPPRESS_NEW / DISCARD_INFLIGHT")
class SuppressionGateTest {

    // hft-0 = [1,2,3], hft-1 = [4,5,6].
    private SlotAssignmentResolver assignment;
    private SafetyStateTracker tracker;

    @BeforeEach
    void setUp() {
        assignment = SlotAssignmentResolver.of(List.of(1L, 2L, 3L, 4L, 5L, 6L), 2, 3);
        tracker = new SafetyStateTracker(assignment);
    }

    @Test
    @DisplayName("safe slot and unassigned token → ALLOW")
    void allowWhenSafe() {
        assertEquals(SuppressionGate.Verdict.ALLOW,
                SuppressionGate.evaluate(tracker, 1L, 5L, null));
        assertEquals(SuppressionGate.Verdict.ALLOW,
                SuppressionGate.evaluate(tracker, 999L, 5L, null));
    }

    @Test
    @DisplayName("unsafe slot: new work suppressed, unpublished in-flight discarded")
    void unsafeSuppresses() {
        tracker.apply(newUnsafe("hft-0", 5L));
        assertEquals(SuppressionGate.Verdict.SUPPRESS_NEW,
                SuppressionGate.evaluate(tracker, 2L, 5L, null));
        assertEquals(SuppressionGate.Verdict.DISCARD_INFLIGHT,
                SuppressionGate.evaluate(tracker, 2L, 5L,
                        new SuppressionGate.InFlightDecision(false)));
        // Published decisions are never retracted.
        assertEquals(SuppressionGate.Verdict.ALLOW,
                SuppressionGate.evaluate(tracker, 2L, 5L,
                        new SuppressionGate.InFlightDecision(true)));
        // Healthy slot is untouched.
        assertEquals(SuppressionGate.Verdict.ALLOW,
                SuppressionGate.evaluate(tracker, 5L, 5L, null));
    }

    @Test
    @DisplayName("recovered slot: pre-recovery input still suppressed, post-recovery allowed")
    void recoveredAdmitsPostRecoveryOnly() {
        tracker.apply(newUnsafe("hft-0", 5L));
        tracker.apply(newRecovered("hft-0", 6L));
        assertEquals(SuppressionGate.Verdict.SUPPRESS_NEW,
                SuppressionGate.evaluate(tracker, 1L, 5L, null)); // old connection data
        assertEquals(SuppressionGate.Verdict.ALLOW,
                SuppressionGate.evaluate(tracker, 1L, 6L, null)); // new connection data
    }

    @Test
    @DisplayName("post-recovery re-unsafe: suppression resumes")
    void reUnsafeResumesSuppression() {
        tracker.apply(newUnsafe("hft-0", 5L));
        tracker.apply(newRecovered("hft-0", 6L));
        tracker.apply(newUnsafe("hft-0", 7L));
        assertEquals(SuppressionGate.Verdict.SUPPRESS_NEW,
                SuppressionGate.evaluate(tracker, 1L, 7L, null));
    }

    private SlotSafetyRequest newUnsafe(String slotId, long epoch) {
        return new SlotSafetyRequest(
                "req-" + slotId + "-" + epoch + "-UNSAFE",
                SlotSafetyRequest.SOURCE_COMPONENT_INGESTION,
                slotId, epoch, SlotSafetyStatus.UNSAFE, "FEED_STALLED",
                assignment.manifestFingerprint(), assignment.tokenSetHashOf(slotId),
                epoch * 1000L, 2);
    }

    private SlotSafetyRequest newRecovered(String slotId, long epoch) {
        return new SlotSafetyRequest(
                "req-" + slotId + "-" + epoch + "-RECOVERED",
                SlotSafetyRequest.SOURCE_COMPONENT_INGESTION,
                slotId, epoch, SlotSafetyStatus.RECOVERED, "",
                assignment.manifestFingerprint(), assignment.tokenSetHashOf(slotId),
                epoch * 1000L, 2);
    }
}
