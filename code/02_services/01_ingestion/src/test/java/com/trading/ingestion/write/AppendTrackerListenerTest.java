package com.trading.ingestion.write;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * P1-262: the backpressure listener fires OUTSIDE the accept-gate lock.
 * A 200ms-sleeping listener must not stall concurrent tryAccept calls;
 * the snapshot values (incl. micros-truncated instant) must be exact;
 * a throwing listener must not break the true/false contract.
 */
@DisplayName("P1-262: listener fires outside the accept lock")
class AppendTrackerListenerTest {

    @Test
    @DisplayName("slow listener does not stall tryAccept at throughput")
    void slowListenerDoesNotStallGate() throws Exception {
        AppendTracker tracker = new AppendTracker(1_000_000, 1_000_000_000L, 0.80);
        tracker.setListener((level, pr, pb, mr, mb, now) -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        });
        // Trip WARNING on one thread (80% of 1M records), then hammer the
        // gate from 50 threads while the listener sleeps. Pre-fix every
        // tryAccept blocked on the lock for the full 200ms.
        for (int i = 0; i < 800_000; i++) {
            tracker.tryAccept(1);
        }
        ExecutorService pool = Executors.newFixedThreadPool(50);
        try {
            CountDownLatch start = new CountDownLatch(1);
            java.util.List<Future<Long>> futs = new java.util.ArrayList<>();
            for (int w = 0; w < 50; w++) {
                futs.add(pool.submit(() -> {
                    start.await();
                    long t0 = System.currentTimeMillis();
                    for (int i = 0; i < 200; i++) {
                        tracker.tryAccept(1);
                    }
                    return System.currentTimeMillis() - t0;
                }));
            }
            start.countDown();
            long worst = 0;
            for (Future<Long> f : futs) {
                worst = Math.max(worst, f.get(30, TimeUnit.SECONDS));
            }
            // 200 tryAccept x 50 threads all under ~1s even with a 200ms
            // listener sleeping — pre-fix this took 200ms+ per call.
            assertTrue(worst < 5_000, "gate stalled by slow listener: worst=" + worst + "ms");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("snapshot values exact and micros-truncated")
    void snapshotExactAndMicros() {
        AppendTracker tracker = new AppendTracker(100, 100_000L, 0.80);
        AtomicReference<Instant> gotAt = new AtomicReference<>();
        AtomicLong gotRecs = new AtomicLong(-1);
        tracker.setListener((level, pr, pb, mr, mb, now) -> {
            gotRecs.set(pr);
            gotAt.set(now);
        });
        for (int i = 0; i < 80; i++) {
            tracker.tryAccept(100);
        }
        assertEquals(80, gotRecs.get(), "listener must see post-totals snapshot (R-196)");
        assertTrue(gotAt.get() != null, "instant must be passed");
        assertEquals(0, gotAt.get().getNano() % 1000,
                "micros-only rule: nanos must be truncated, got " + gotAt.get());
    }

    @Test
    @DisplayName("throwing listener does not break tryAccept contract")
    void throwingListenerIgnored() {
        AppendTracker tracker = new AppendTracker(100, 100_000L, 0.80);
        tracker.setListener((level, pr, pb, mr, mb, now) -> {
            throw new RuntimeException("boom");
        });
        assertTrue(tracker.tryAccept(100), "WARNING path must still return true");
        AppendTracker halter = new AppendTracker(2, 1_000_000L, 0.80);
        halter.setListener((level, pr, pb, mr, mb, now) -> {
            throw new RuntimeException("boom");
        });
        assertTrue(halter.tryAccept(100));
        assertTrue(halter.tryAccept(100));
        // third trips halt — still false, not a throw
        try {
            boolean r = halter.tryAccept(100);
            assertTrue(!r, "halt path must return false even with throwing listener");
        } catch (RuntimeException e) {
            throw new AssertionError("listener throw escaped tryAccept", e);
        }
    }
}
