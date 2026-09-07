// T5-Q tests — BoundedQueue (contract §7.5): thresholds, byte accounting,
// boundedness. Deterministic: no real I/O, direct offer/take.

package com.trading.ingestion.write;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.TickPacketFixtures;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.util.concurrent.atomic.AtomicReference;

/** T5-Q1..Q3: BoundedQueue thresholds / byte accounting / boundedness. */
@DisplayName("T5-Q: BoundedQueue")
class BoundedQueueTest {

    @Test
    @DisplayName("T5-Q1: 80% warn, 100% halt, beyond rejected — no silent drop")
    void thresholds() {
        // budget: 1000 bytes / 10 records
        BoundedQueue q = new BoundedQueue(1000, 10);
        List<BoundedQueue.QueueListener.Level> events = new ArrayList<>();
        q.setListener((level, r, b, mr, mb) -> events.add(level));

        // < 80% (8 records * 100 = 800 bytes = 80%) — no events
        for (int i = 0; i < 7; i++) {
            assertTrue(q.offer(TickPacketFixtures.validTrade(i), 100), "offer below 80%");
        }
        assertTrue(events.isEmpty(), "no warn below 80%");

        // exactly 80% (8th = 800 bytes) — warning fires
        assertTrue(q.offer(TickPacketFixtures.validTrade(7), 100), "offer at 80%");
        assertEquals(List.of(BoundedQueue.QueueListener.Level.WARNING), events,
                "80% warning fired");

        // > 80% but < 100% — still accepted, no extra warn (latched)
        assertTrue(q.offer(TickPacketFixtures.validTrade(8), 100), "offer at 90%");
        assertEquals(1, events.size(), "warning fires once per episode");

        // 100% (10th = 1000 bytes) — accepted, queue exactly full
        assertTrue(q.offer(TickPacketFixtures.validTrade(9), 100), "offer at 100%");
        assertEquals(1, events.size(), "no CRITICAL yet at exactly 100%");

        // beyond 100% (11th) — rejected (halted), CRITICAL fires, no silent drop
        assertFalse(q.offer(TickPacketFixtures.validTrade(10), 100), "reject beyond 100%");
        assertEquals(2, events.size(), "CRITICAL fires on overflow");
        assertEquals(BoundedQueue.QueueListener.Level.CRITICAL, events.get(1));
        assertTrue(q.isHalted(), "halted after overflow");
        assertEquals(10, q.size(), "queue holds 10, 11th rejected");
    }

    @Test
    @DisplayName("T5-Q2: exact byte accounting")
    void byteAccounting() {
        BoundedQueue q = new BoundedQueue(1_000_000, 1000);
        // offer 3 packets of 200/300/500 bytes
        assertTrue(q.offer(TickPacketFixtures.validTrade(0), 200));
        assertTrue(q.offer(TickPacketFixtures.validTrade(1), 300));
        assertTrue(q.offer(TickPacketFixtures.validTrade(2), 500));
        assertEquals(1000, q.queuedBytes(), "sum of queued bytes");
        assertEquals(3, q.size());

        // take releases bytes
        try {
            q.take();
            q.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertEquals(500, q.queuedBytes(), "after 2 takes");
        assertEquals(1, q.size());
    }

    @Test
    @DisplayName("T5-Q3: bounded — concurrent producers never overflow")
    void boundedConcurrent() throws Exception {
        // tight budget: 200 records x 100 bytes = 20k
        BoundedQueue q = new BoundedQueue(20_000, 200);
        int producers = 4;
        int perProducer = 1000; // would be 4000 total if unbounded
        ExecutorService pool = Executors.newFixedThreadPool(producers);
        CountDownLatch start = new CountDownLatch(1);
        List<Boolean> results = java.util.Collections.synchronizedList(new ArrayList<>());

        for (int p = 0; p < producers; p++) {
            final int producerId = p;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perProducer; i++) {
                        boolean ok = q.offer(TickPacketFixtures.validTrade(i), 100);
                        results.add(ok);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS), "producers done");

        // bounded: at most 200 accepted, rest rejected (halted) — never > cap
        long accepted = results.stream().filter(b -> b).count();
        assertTrue(accepted <= 200, "accepted <= cap, got " + accepted);
        assertTrue(q.size() <= 200, "queue size <= cap");
        assertTrue(q.isHalted(), "halted after overflow");
        // no silent drop: every rejected was a visible false (counted)
        assertEquals(producers * perProducer, results.size(), "every offer returned a result");
    }

    @Test
    @DisplayName("T5-Q3b: dequeue after saturation works (recovery)")
    void dequeueAfterSaturation() throws Exception {
        BoundedQueue q = new BoundedQueue(1000, 10);
        for (int i = 0; i < 10; i++) {
            assertTrue(q.offer(TickPacketFixtures.validTrade(i), 100));
        }
        // 11th (1100 > 1000) rejected — queue full
        assertFalse(q.offer(TickPacketFixtures.validTrade(10), 100), "full");
        // dequeue all
        int drained = 0;
        BoundedQueue.Entry e;
        while ((e = q.poll()) != null) {
            drained++;
        }
        assertEquals(10, drained, "all dequeued");
        assertEquals(0, q.size());
        assertEquals(0, q.queuedBytes());
    }

    @Test
    @DisplayName("T5-Q3c: halt is transient — drains recover, episodes re-alert (P1-011/012)")
    void haltRecoversAfterDrain() {
        BoundedQueue q = new BoundedQueue(1000, 10);
        List<BoundedQueue.QueueListener.Level> events = new ArrayList<>();
        q.setListener((level, r, b, mr, mb) -> events.add(level));
        for (int i = 0; i < 10; i++) {
            assertTrue(q.offer(TickPacketFixtures.validTrade(i), 100));
        }
        assertFalse(q.offer(TickPacketFixtures.validTrade(10), 100), "reject while full");
        assertTrue(q.isHalted(), "halted while full");
        assertEquals(1, events.stream().filter(l -> l == BoundedQueue.QueueListener.Level.CRITICAL).count(),
                "CRITICAL once per episode");
        // still full: further offers rejected WITHOUT re-alerting
        assertFalse(q.offer(TickPacketFixtures.validTrade(11), 100));
        assertEquals(1, events.stream().filter(l -> l == BoundedQueue.QueueListener.Level.CRITICAL).count(),
                "no alert spam while full");
        // drain below 80% via take: episode ends, offers accepted again
        try {
            q.take();
            q.take();
            q.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertFalse(q.isHalted(), "halt clears after drain below 80%");
        assertTrue(q.offer(TickPacketFixtures.validTrade(20), 100), "offer accepted after recovery");
        // re-saturate: a NEW episode alerts again
        assertTrue(q.offer(TickPacketFixtures.validTrade(21), 100));
        assertTrue(q.offer(TickPacketFixtures.validTrade(22), 100));
        assertFalse(q.offer(TickPacketFixtures.validTrade(23), 100), "reject when full again");
        assertEquals(2, events.stream().filter(l -> l == BoundedQueue.QueueListener.Level.CRITICAL).count(),
                "second episode alerts again");
    }

    @Test
    @DisplayName("T5-Q3d: poll drains also clear the halt (P1-011/012)")
    void pollDrainRecovers() {
        BoundedQueue q = new BoundedQueue(1000, 10);
        for (int i = 0; i < 10; i++) {
            assertTrue(q.offer(TickPacketFixtures.validTrade(i), 100));
        }
        assertFalse(q.offer(TickPacketFixtures.validTrade(10), 100));
        assertTrue(q.isHalted());
        while (q.poll() != null) {
            // drain fully via poll
        }
        assertFalse(q.isHalted(), "halt clears after poll drain");
        assertTrue(q.offer(TickPacketFixtures.validTrade(30), 100), "offer accepted after poll recovery");
    }

    @Test
    @DisplayName("T5-Q3e: offer rejects null packet and non-positive rowBytes (P1-103/105)")
    void offerValidatesEntry() {
        BoundedQueue q = new BoundedQueue(1000, 10);
        org.junit.jupiter.api.Assertions.assertThrows(NullPointerException.class,
                () -> q.offer(null, 100));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> q.offer(TickPacketFixtures.validTrade(1), 0));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> q.offer(TickPacketFixtures.validTrade(1), -50));
        // validation fires before any state change: queue still usable
        org.junit.jupiter.api.Assertions.assertTrue(q.offer(TickPacketFixtures.validTrade(1), 100));
        org.junit.jupiter.api.Assertions.assertEquals(1, q.size());
    }

    @Test
    @DisplayName("T5-Q3f: constructor rejects non-positive budgets (P1-103/105)")
    void constructorValidatesBudgets() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> new BoundedQueue(0, 10));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> new BoundedQueue(1000, 0));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> new BoundedQueue(-5, -5));
    }

    @Test
    @DisplayName("T5-Q3g: slow listener does not block other producers (P1-104/106)")
    void slowListenerDoesNotBlock() throws Exception {
        BoundedQueue q = new BoundedQueue(1000, 10);
        java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        // 8 x 100 = 800 bytes hits the 80% warn band on the 8th offer.
        q.setListener((level, r, b, mr, mb) -> {
            entered.countDown();
            try {
                org.junit.jupiter.api.Assertions.assertTrue(
                        release.await(5, java.util.concurrent.TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        for (int i = 0; i < 7; i++) {
            org.junit.jupiter.api.Assertions.assertTrue(q.offer(TickPacketFixtures.validTrade(i), 100));
        }
        AtomicReference<Throwable> err = new AtomicReference<>();
        java.util.concurrent.atomic.AtomicLong elapsedMs = new java.util.concurrent.atomic.AtomicLong(0);
        Thread blocker = new Thread(() -> q.offer(TickPacketFixtures.validTrade(7), 100));
        Thread probe = new Thread(() -> {
            long start = System.nanoTime();
            try {
                q.offer(TickPacketFixtures.validTrade(8), 100);
            } catch (Throwable th) {
                err.set(th);
            } finally {
                elapsedMs.set((System.nanoTime() - start) / 1_000_000);
            }
        });
        blocker.start();
        org.junit.jupiter.api.Assertions.assertTrue(entered.await(5, java.util.concurrent.TimeUnit.SECONDS),
                "listener must fire");
        probe.start();
        probe.join(5000);
        // The probe's offer must complete while the listener is still blocked:
        // lock is released before the callback runs.
        org.junit.jupiter.api.Assertions.assertFalse(probe.isAlive(), "probe offer blocked behind listener");
        org.junit.jupiter.api.Assertions.assertNull(err.get());
        release.countDown();
        blocker.join(5000);
    }

    @Test
    @DisplayName("T5-Q3h: throwing listener leaves committed state intact (P1-104/106)")
    void throwingListenerKeepsState() {
        BoundedQueue q = new BoundedQueue(1000, 10);
        q.setListener((level, r, b, mr, mb) -> { throw new RuntimeException("listener boom"); });
        // 8th offer trips WARNING: throw propagates, but the enqueue stands.
        for (int i = 0; i < 7; i++) {
            org.junit.jupiter.api.Assertions.assertTrue(q.offer(TickPacketFixtures.validTrade(i), 100));
        }
        try {
            q.offer(TickPacketFixtures.validTrade(7), 100);
            org.junit.jupiter.api.Assertions.fail("listener throw must propagate");
        } catch (RuntimeException expected) {
            org.junit.jupiter.api.Assertions.assertEquals("listener boom", expected.getMessage());
        }
        org.junit.jupiter.api.Assertions.assertEquals(8, q.size(), "enqueue stands despite listener throw");
        org.junit.jupiter.api.Assertions.assertEquals(800, q.queuedBytes());
    }
}
