package com.trading.ingestion.write;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.TickPacketFixtures;
import com.trading.ingestion.model.TickPacket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Shutdown-path unit (P1-113/118 + P1-114 + P1-115): scheduler shutdown must
 * never let an exception escape, leak a reservation, or drive gauges
 * negative — and every accepted tick converges (terminal outcome or forgiven
 * zero, never a stuck slot).
 */
@DisplayName("Writer shutdown: no escape, no leak, no negative (G+H+I)")
class RawTickWriterShutdownTest {

    /** Converter whose append future is controlled by the test. */
    static final class ControllableConverter implements FlussRowConverter {
        volatile CompletableFuture<RawTickWriter.AppendResult> pending =
                new CompletableFuture<>();
        @Override
        public CompletableFuture<RawTickWriter.AppendResult> append(TickPacket packet) {
            return pending;
        }
        @Override public int estimatedRowSize(TickPacket packet) { return 100; }
        @Override public void close() {}
    }

    /** Converter whose append future fails immediately (drives retries). */
    static final class FailingConverter implements FlussRowConverter {
        final Throwable cause;
        FailingConverter(Throwable cause) { this.cause = cause; }
        @Override
        public CompletableFuture<RawTickWriter.AppendResult> append(TickPacket packet) {
            return CompletableFuture.failedFuture(cause);
        }
        @Override public int estimatedRowSize(TickPacket packet) { return 100; }
        @Override public void close() {}
    }

    private static RawTickWriter writer(FlussRowConverter c, Duration appendTimeout,
                                        Duration drainDeadline, AppendTracker t) {
        return new RawTickWriter(c, t, "default.raw_table_1", appendTimeout, drainDeadline);
    }

    @Test
    @DisplayName("close with in-flight append returns promptly with zero balance (G site 1)")
    void closeWithInflightReturnsBalanced() throws Exception {
        ControllableConverter c = new ControllableConverter();
        AppendTracker tracker = new AppendTracker();
        // Long timeout: the timeout task is still queued when drain expires,
        // so shutdownNow discards it — the old code path is exercised.
        RawTickWriter w = writer(c, Duration.ofSeconds(30), Duration.ofMillis(50), tracker);
        w.write(TickPacketFixtures.validTrade(21));
        assertEquals(1, tracker.pendingRecords());
        long start = System.nanoTime();
        w.close();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertTrue(elapsedMs < 5000, "close must return near the drain deadline, took " + elapsedMs + "ms");
        assertEquals(0, tracker.pendingRecords(), "forgiven to zero");
        assertEquals(0, tracker.pendingBytes());
        // Late completion after close: releases against the floor, no throw,
        // counters stay non-negative.
        c.pending.complete(new RawTickWriter.AppendResult(9L, "p0"));
        Thread.sleep(100);
        assertEquals(0, tracker.pendingRecords());
        assertTrue(tracker.pendingBytes() >= 0, "gauges never negative");
    }

    @Test
    @DisplayName("post-close retryable failure cannot escape and stays balanced (G site 2 + H(b))")
    void postCloseRetryFailureCannotEscape() {
        ControllableConverter c = new ControllableConverter();
        AppendTracker tracker = new AppendTracker();
        RawTickWriter w = writer(c, Duration.ofSeconds(30), Duration.ofMillis(50), tracker);
        w.write(TickPacketFixtures.validTrade(22));
        w.close();
        assertEquals(0, tracker.pendingRecords());
        // Fail AFTER shutdown: retry schedule rejects -> terminal FAILED path.
        // Old code swallowed the rejection into an unobserved stage (slot
        // already forgiven, so balanced by luck — but silent). This test
        // locks the new contract: release + error count, never silent.
        c.pending.completeExceptionally(new java.io.IOException("connection reset"));
        assertEquals(0, tracker.pendingRecords(), "still balanced after rejected retry");
        assertTrue(tracker.pendingBytes() >= 0, "gauges never negative");
    }

    @Test
    @DisplayName("discarded backoff tasks leave zero balance, close returns (H(a) + D)")
    void discardedBackoffLeavesZeroBalance() {
        AppendTracker tracker = new AppendTracker();
        // Immediate retryable failure -> backoff 100ms queued on scheduler.
        // Drain deadline 50ms expires first -> shutdownNow discards the
        // backoffs whose slots are still held. TWO writes: the old bulk
        // forgive freed exactly 1 record, so it leaks the second slot
        // (pending 1) — forceDrain zeroes both.
        RawTickWriter w = writer(new FailingConverter(
                new java.io.IOException("connection reset")),
                Duration.ofSeconds(30), Duration.ofMillis(50), tracker);
        w.write(TickPacketFixtures.validTrade(23));
        w.write(TickPacketFixtures.validTrade(24));
        assertEquals(2, tracker.pendingRecords(), "two slots held before close");
        long start = System.nanoTime();
        w.close();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertTrue(elapsedMs < 5000, "close must return, took " + elapsedMs + "ms");
        assertEquals(0, tracker.pendingRecords(),
                "discarded backoff slot must be forgiven, not leaked");
        assertEquals(0, tracker.pendingBytes());
    }

    @Test
    @DisplayName("forceDrain zeroes once and floors late releases (P1-115)")
    void forceDrainFloors() {
        AppendTracker tracker = new AppendTracker();
        assertTrue(tracker.tryAccept(100));
        assertTrue(tracker.tryAccept(200));
        long[] forgiven = tracker.forceDrain();
        assertEquals(2, forgiven[0], "both records forgiven");
        assertEquals(300, forgiven[1], "exact bytes forgiven, not 1-record worth");
        assertEquals(0, tracker.pendingRecords());
        assertEquals(0, tracker.pendingBytes());
        // Forgiven slots count as failed (R-260) — the journal pinned them.
        assertEquals(2, tracker.totalFailed());
        // Racing late completions release against the floor — never negative.
        tracker.onAppendFailure(100);
        tracker.onAppendSuccess(200);
        assertEquals(0, tracker.pendingRecords());
        assertEquals(0, tracker.pendingBytes());
    }
}
