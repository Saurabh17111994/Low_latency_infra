package com.trading.ingestion.health;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.write.AppendTracker;
import org.junit.jupiter.api.Test;

class SlotHealthTest {
    @Test
    void dataReadinessRequiresFullAcknowledgementAndRecentFrame() {
        HealthProbe probe = new HealthProbe(new AppendTracker());
        probe.updateSlot("hft-0", "PARTIAL", 1, 10, 9, 1, System.nanoTime());
        assertFalse(probe.isDataReady());
        probe.updateSlot("hft-0", "ACTIVE", 2, 10, 10, 0, System.nanoTime());
        assertTrue(probe.isDataReady());
    }

    @Test
    void resetSlotsReturnsToAuthenticatingAndZeroCoverage() {
        HealthProbe probe = new HealthProbe(new AppendTracker());
        probe.updateSlot("hft-0", "ACTIVE", 3, 1024, 1024, 0, System.nanoTime());
        probe.setSlotCapacityRemaining("hft-0", 512);
        probe.setSlotUnsafe("hft-0", true);
        assertTrue(probe.isDataReady());
        probe.resetSlotsToAuthenticating();
        assertFalse(probe.isDataReady(), "after restart reset, slot must not be ready");
        HealthProbe.SlotHealth slot = probe.slot("hft-0");
        org.junit.jupiter.api.Assertions.assertEquals("AUTHENTICATING", slot.state);
        org.junit.jupiter.api.Assertions.assertEquals(0, slot.assigned);
        org.junit.jupiter.api.Assertions.assertEquals(0, slot.acknowledged);
        org.junit.jupiter.api.Assertions.assertEquals(0, slot.rejected);
        // P1-245: generation + headroom reset too — old epoch (3) must not mix
        // with the fresh AUTHENTICATING state, and no headroom is claimed.
        org.junit.jupiter.api.Assertions.assertEquals(0, slot.epoch);
        org.junit.jupiter.api.Assertions.assertEquals(0, slot.capacityRemaining);
        // Safety evidence MUST survive a bridge restart — only RECOVERED clears it,
        // otherwise a restart would silently lift a halt.
        org.junit.jupiter.api.Assertions.assertTrue(slot.unsafe);
        org.junit.jupiter.api.Assertions.assertTrue(slot.unsafeSinceNanos > 0);
    }

    @Test
    void perSlotFrameEvidenceKeepsSteadyStateReadyWithoutLifecycleEvents() throws Exception {
        // P1-081 successor to the R-031 test: steady-state ticks refresh ONLY
        // their own slot (via setSlotFrameReceived, wired per tick in
        // IngestionService.processTickEvent). An ACTIVE slot stays data-ready
        // well past the 15s timeout even though no lifecycle event arrives.
        HealthProbe probe = new HealthProbe(new AppendTracker());
        probe.updateSlot("hft-0", "ACTIVE", 1, 10, 10, 0, System.nanoTime());
        assertTrue(probe.isDataReady());

        // 16s of "steady-state" tick flow for hft-0 only.
        long now = System.nanoTime();
        for (int i = 0; i < 8; i++) {
            now += java.time.Duration.ofSeconds(2).toNanos();
            probe.setLastFrameReceived(now);
            probe.setSlotFrameReceived("hft-0", now);
        }
        assertTrue(probe.isDataReady(),
                "per-slot frame evidence must keep the ACTIVE slot ready (R-031, truthfully)");

        // Feed genuinely stops → slot goes stale again.
        Thread.sleep(25);
        HealthProbe.SlotHealth slot = probe.slot("hft-0");
        slot.lastFrameNanos = System.nanoTime() - java.time.Duration.ofSeconds(16).toNanos();
        assertFalse(probe.isDataReady(), "stale slot must not be data-ready");
    }

    @Test
    void globalFrameArrivalNeverRefreshesAnySlot() {
        // P1-081 core: the global setter is global recency ONLY. A frame that
        // is not evidence for a slot must not keep that slot fresh — the old
        // fan-out let slot A's frames mask slot B's stall.
        HealthProbe probe = new HealthProbe(new AppendTracker());
        long stale = System.nanoTime() - java.time.Duration.ofSeconds(16).toNanos();
        probe.updateSlot("hft-0", "ACTIVE", 1, 10, 10, 0, stale);
        assertFalse(probe.isDataReady(), "stale slot starts not-ready");
        for (int i = 0; i < 8; i++) {
            probe.setLastFrameReceived(System.nanoTime());
        }
        assertFalse(probe.isDataReady(),
                "global frames must not refresh per-slot recency (P1-081)");
    }

    @Test
    void frameEvidenceIsPerSlotAndCreatesNothing() {
        // One slot's ticks never touch another slot; an unknown slot id
        // plants no TERMINAL entry that would veto readiness.
        HealthProbe probe = new HealthProbe(new AppendTracker());
        long now = System.nanoTime();
        probe.updateSlot("hft-0", "ACTIVE", 1, 10, 10, 0, now);
        probe.updateSlot("hft-1", "ACTIVE", 1, 10, 10, 0, now);
        probe.setSlotFrameReceived("hft-0", System.nanoTime());
        probe.setSlotFrameReceived("ghost-9", System.nanoTime());
        assertFalse(probe.slotIds().contains("ghost-9"),
                "unknown slot must not be created by frame evidence");
        HealthProbe.SlotHealth b = probe.slot("hft-1");
        // hft-1 keeps its lifecycle stamp — hft-0's tick did not refresh it.
        org.junit.jupiter.api.Assertions.assertEquals(now, b.lastFrameNanos);
    }

    @Test
    void concurrentSlotUpdatesNeverTearAReadSnapshot() throws Exception {
        // P1-082 characterization: writers always publish internally
        // consistent triples (assigned == acknowledged == v, rejected 0, ACTIVE);
        // every diagnostics snapshot must show assigned == acknowledged.
        // A torn read (new assigned with old acknowledged) fails loudly.
        HealthProbe probe = new HealthProbe(new AppendTracker());
        probe.updateSlot("hft-0", "ACTIVE", 1, 0, 0, 0, System.nanoTime());
        java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean();
        java.util.concurrent.atomic.AtomicReference<String> tear = new java.util.concurrent.atomic.AtomicReference<>();
        Thread[] writers = new Thread[4];
        for (int w = 0; w < writers.length; w++) {
            final int base = w * 1_000_000;
            writers[w] = new Thread(() -> {
                for (int i = 1; i <= 25_000 && !stop.get(); i++) {
                    int v = base + i;
                    probe.updateSlot("hft-0", "ACTIVE", v, v, v, 0, System.nanoTime());
                }
            });
            writers[w].start();
        }
        Thread reader = new Thread(() -> {
            while (!stop.get()) {
                @SuppressWarnings("unchecked")
                java.util.Map<String, Object> slot =
                        (java.util.Map<String, Object>)
                                ((java.util.Map<String, Object>) probe.diagnostics().get("slots"))
                                        .get("hft-0");
                if (slot != null && !slot.get("assigned").equals(slot.get("acknowledged"))) {
                    tear.compareAndSet(null, "assigned=" + slot.get("assigned")
                            + " acknowledged=" + slot.get("acknowledged"));
                    stop.set(true);
                }
                if (!probe.isDataReady() && tear.get() == null) {
                    // isDataReady may legitimately be false (stale stamp vs
                    // nanoTime race) — only the snapshot-mismatch tears count.
                }
            }
        });
        reader.start();
        for (Thread w : writers) w.join();
        stop.set(true);
        reader.join();
        org.junit.jupiter.api.Assertions.assertNull(tear.get(),
                () -> "torn slot snapshot observed: " + tear.get());
    }

    @Test
    void probeWithNoFramesIsNotFrameRecent() throws Exception {
        // R-178: nanoTime() origin is arbitrary — a probe that has never seen a
        // frame (0) must NOT be considered frame-recent.
        HealthProbe probe = new HealthProbe(new AppendTracker());
        assertFalse(probe.isReady());
        // Drive all other dimensions ready except frame recency.
        probe.setFlussReady(true);
        probe.setBrokerConnected(true);
        probe.setSubscriptionComplete(true);
        probe.updateSlot("hft-0", "ACTIVE", 1, 10, 10, 0, System.nanoTime());
        assertFalse(probe.isReady(),
                "no frame ever received (0 nanos) must fail frame-recent (R-178)");
        probe.setLastFrameReceived(System.nanoTime());
        assertTrue(probe.isReady());
    }

    @Test
    void diagnosticsExposeTelemetryReadiness() {
        HealthProbe probe = new HealthProbe(new AppendTracker());
        probe.setOtlpHealthy(true);
        assertTrue((Boolean) probe.diagnostics().get("telemetry_ready"),
                "diagnostics must surface telemetry readiness (R-251)");
        probe.setOtlpHealthy(false);
        assertFalse((Boolean) probe.diagnostics().get("telemetry_ready"));
    }
}
