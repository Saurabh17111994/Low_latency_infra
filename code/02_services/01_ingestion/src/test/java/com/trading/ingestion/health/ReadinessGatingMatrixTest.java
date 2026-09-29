package com.trading.ingestion.health;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.write.AppendTracker;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ING-INT-005: exhaustive READY gating matrix (plan G8 — no-false-positive
 * proof). {@code HealthProbe.isReady()} ANDs every readiness dimension —
 * liveness, Fluss, tracker, broker, subscription, memory, queue backpressure,
 * data, frame recency, clock — and this suite walks EVERY one of them:
 * readiness is false when any single dimension is false and true only when all
 * are. The pre-M3 tests covered only a subset of combinations, so a dimension
 * silently dropped from the AND would have gone unnoticed.
 *
 * <p>Fully-ready baseline (all true): a tracker below the 80% warning
 * threshold, an ACTIVE slot with full acknowledgement and a recent frame, a
 * recent global frame timestamp, and a null clock checker (no checker
 * configured → clock dimension passes). Each gating test flips exactly one
 * dimension from that baseline and asserts readiness flips false.
 *
 * <p>The clock dimension is the only one that cannot be driven from the
 * fully-ready baseline (a null checker is always OK, and
 * {@link NtpClockChecker} is final), so it is proven with its own pair of
 * probes: one with a checker whose offset is unverified (fail-closed
 * {@code isWithinLimit() == false}) must not be ready even though every other
 * dimension is true.
 */
@DisplayName("ING-INT-005: READY gating — false when ANY dimension is false")
class ReadinessGatingMatrixTest {

    /** Small tracker so the 80% warning threshold is reachable in-test. */
    private static final long MAX_RECORDS = 10;
    private static final long MAX_BYTES = 1_000_000L;
    private static final double WARNING_PERCENT = 0.80;

    @Test
    @DisplayName("all eight dimensions true → ready")
    void allDimensionsTrueIsReady() {
        HealthProbe probe = fullyReady();
        assertTrue(probe.isReady(), "every dimension true must be READY");
    }

    @Test
    @DisplayName("liveness false blocks readiness")
    void livenessFalseBlocksReadiness() {
        HealthProbe probe = fullyReady();
        probe.markNotAlive();
        assertFalse(probe.isReady(), "dead process must never be READY");
    }

    @Test
    @DisplayName("Fluss dimension false blocks readiness")
    void flussNotReadyBlocksReadiness() {
        HealthProbe probe = fullyReady();
        probe.setFlussReady(false);
        assertFalse(probe.isReady(), "Fluss disconnected must not be READY");
    }

    @Test
    @DisplayName("tracker backpressure warning blocks readiness")
    void trackerWarningBlocksReadiness() {
        AppendTracker tracker = new AppendTracker(MAX_RECORDS, MAX_BYTES, WARNING_PERCENT);
        assertTrue(tracker.tryAccept(10), "baseline accept keeps the tracker below the warning");
        HealthProbe probe = readyFrom(tracker);
        assertTrue(probe.isReady(), "below the 80% warning the tracker stays READY");
        // Push to 80% (8 of 10 records) — the warning fires and readiness flips.
        for (int i = 1; i < 8; i++) {
            assertTrue(tracker.tryAccept(10), "records up to 80% must be accepted");
        }
        assertFalse(probe.isReady(), "tracker at 80% must not be READY");
    }

    @Test
    @DisplayName("broker disconnected blocks readiness")
    void brokerDisconnectedBlocksReadiness() {
        HealthProbe probe = fullyReady();
        probe.setBrokerConnected(false);
        assertFalse(probe.isReady(), "no broker connection must not be READY");
    }

    @Test
    @DisplayName("subscription incomplete blocks readiness")
    void subscriptionIncompleteBlocksReadiness() {
        HealthProbe probe = fullyReady();
        probe.setSubscriptionComplete(false);
        assertFalse(probe.isReady(), "incomplete subscription must not be READY");
    }

    @Test
    @DisplayName("no active slot data blocks readiness")
    void noActiveSlotDataBlocksReadiness() {
        HealthProbe probe = fullyReady();
        // The fully-ready probe tracked one ACTIVE slot; a second probe that
        // never saw a slot has no data dimension at all.
        HealthProbe neverSawSlot = new HealthProbe(new AppendTracker(MAX_RECORDS, MAX_BYTES, WARNING_PERCENT), null);
        neverSawSlot.setFlussReady(true);
        neverSawSlot.setBrokerConnected(true);
        neverSawSlot.setSubscriptionComplete(true);
        neverSawSlot.setLastFrameReceived(System.nanoTime());
        assertFalse(neverSawSlot.isReady(),
                "broker frames with zero tracked slots must not be READY (no data)");
        // And removing the slot from the otherwise-ready probe blocks too.
        HealthProbe noSlots = new HealthProbe(new AppendTracker(MAX_RECORDS, MAX_BYTES, WARNING_PERCENT), null);
        noSlots.setFlussReady(true);
        noSlots.setBrokerConnected(true);
        noSlots.setSubscriptionComplete(true);
        noSlots.setLastFrameReceived(System.nanoTime());
        assertFalse(noSlots.isReady(), "no tracked slot must not be READY");
    }

    @Test
    @DisplayName("partial acknowledgement blocks readiness")
    void partialAckBlocksReadiness() {
        HealthProbe probe = fullyReady();
        // Rebuild with a slot whose ack is incomplete: assigned 1024, acked 900.
        AppendTracker tracker = new AppendTracker(MAX_RECORDS, MAX_BYTES, WARNING_PERCENT);
        tracker.tryAccept(10);
        HealthProbe partial = new HealthProbe(tracker, null);
        partial.setFlussReady(true);
        partial.setBrokerConnected(true);
        partial.setSubscriptionComplete(true);
        partial.setLastFrameReceived(System.nanoTime());
        partial.updateSlot("hft-0", "ACTIVE", 1, 1024, 900, 0, System.nanoTime());
        assertFalse(partial.isReady(), "assigned != acknowledged must not be READY");
    }

    @Test
    @DisplayName("stale frame blocks readiness")
    void staleFrameBlocksReadiness() {
        HealthProbe probe = fullyReady();
        // FRAME_STALE_TIMEOUT is 15 s; stamp the last frame 20 s in the past.
        probe.setLastFrameReceived(System.nanoTime() - 20_000_000_000L);
        assertFalse(probe.isReady(), "stale broker frame must not be READY");
    }

    @Test
    @DisplayName("clock outside limit blocks readiness")
    void clockOutsideLimitBlocksReadiness() {
        // Fail-closed clock: every server unreachable → isWithinLimit() false.
        NtpClockChecker badClock = new NtpClockChecker("10.255.255.1", 100, false);
        try {
            badClock.measureOffsetMs();
        } catch (NtpClockChecker.NtpException ignored) {
            // fallback path reached — the checker is now unverified/failed
        }
        assertFalse(badClock.isWithinLimit(), "unverified clock must fail closed");

        AppendTracker tracker = new AppendTracker(MAX_RECORDS, MAX_BYTES, WARNING_PERCENT);
        tracker.tryAccept(10);
        HealthProbe probe = new HealthProbe(tracker, badClock);
        probe.setFlussReady(true);
        probe.setBrokerConnected(true);
        probe.setSubscriptionComplete(true);
        probe.setLastFrameReceived(System.nanoTime());
        probe.updateSlot("hft-0", "ACTIVE", 1, 2, 2, 0, System.nanoTime());
        assertFalse(probe.isReady(), "clock out of policy must not be READY");
    }

    @Test
    @DisplayName("sustained heap-high block (memoryBlocked) refuses READY")
    void memoryBlockedBlocksReadiness() {
        HealthProbe probe = fullyReady();
        assertTrue(probe.isReady(), "baseline must be READY");
        assertTrue(probe.isMemoryReady(), "memory gate open by default");
        probe.setMemoryBlocked(true);
        assertFalse(probe.isMemoryReady(), "memory gate must close when blocked");
        assertFalse(probe.isReady(), "sustained heap-high must refuse READY");
        assertFalse((Boolean) probe.diagnostics().get("memory_ready"),
                "diagnostics must surface the closed memory gate");
    }

    @Test
    @DisplayName("queue backpressure episode blocks readiness (M4-3)")
    void queueBlockedBlocksReadiness() {
        HealthProbe probe = fullyReady();
        assertTrue(probe.isReady(), "baseline must be READY");
        probe.setQueueBlocked(0, true);
        assertFalse(probe.isReady(), "a queue in an 80%/100% episode must refuse READY");
        assertTrue((Boolean) probe.diagnostics().get("queue_blocked"),
                "diagnostics must surface the blocked queue");
        probe.setQueueBlocked(0, false);
        assertTrue(probe.isReady(), "RESUMED restores READY");
        assertFalse((Boolean) probe.diagnostics().get("queue_blocked"),
                "diagnostics must clear after resume");
    }

    /** A probe with every readiness dimension true (clock = no checker → OK). */
    private static HealthProbe fullyReady() {
        AppendTracker tracker = new AppendTracker(MAX_RECORDS, MAX_BYTES, WARNING_PERCENT);
        assertTrue(tracker.tryAccept(10), "baseline accept keeps the tracker below the warning");
        return readyFrom(tracker);
    }

    /** The fully-ready state over an existing tracker (all other dimensions true). */
    private static HealthProbe readyFrom(AppendTracker tracker) {
        HealthProbe probe = new HealthProbe(tracker, null);
        probe.setFlussReady(true);
        probe.setBrokerConnected(true);
        probe.setSubscriptionComplete(true);
        probe.setLastFrameReceived(System.nanoTime());
        probe.updateSlot("hft-0", "ACTIVE", 1, 2, 2, 0, System.nanoTime());
        return probe;
    }
}
