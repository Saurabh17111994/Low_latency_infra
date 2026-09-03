package com.trading.ingestion.write;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.TickPacketFixtures;
import com.trading.ingestion.model.TickPacket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Fail-fast guards (A+B): append failures must PROPAGATE to the writer's
 * terminal-outcome path (never swallowed into fake success), and a stale-table
 * handle must terminate as FATAL (not busy-loop retry).
 *
 * <p>Regression for the 2026-09-03 incident: a long-lived writer kept writing
 * to a stale tableId after the table was dropped+recreated. The typed converter
 * path (.exceptionally swallow) and a non-halting FATAL outcome let it spin for
 * hours against a dead handle.
 */
@DisplayName("Fail-fast guards (A+B): append propagation + stale-handle FATAL")
class FailFastGuardsTest {

    /** A converter whose append future fails with the given cause. */
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

    /** A converter whose append future completes normally (control). */
    static final class OkConverter implements FlussRowConverter {
        @Override
        public CompletableFuture<RawTickWriter.AppendResult> append(TickPacket packet) {
            return CompletableFuture.completedFuture(
                    new RawTickWriter.AppendResult(1L, "p0"));
        }
        @Override public int estimatedRowSize(TickPacket packet) { return 100; }
        @Override public void close() {}
    }

    /** Writer + latch wired to the first delivered terminal outcome. */
    private record Harness(RawTickWriter writer,
                           AppendTracker tracker,
                           CountDownLatch done,
                           AtomicReference<RawTickWriter.AppendOutcome> outcome) {}

    private static Harness harness(FlussRowConverter converter) {
        AppendTracker tracker = new AppendTracker();
        RawTickWriter writer = new RawTickWriter(
                converter, tracker, "default.raw_table_1",
                Duration.ofSeconds(5), Duration.ofSeconds(10));
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<RawTickWriter.AppendOutcome> outcome = new AtomicReference<>();
        writer.setOutcomeListener(o -> { outcome.set(o); done.countDown(); });
        return new Harness(writer, tracker, done, outcome);
    }

    private static RawTickWriter.AppendOutcome awaitOutcome(Harness h) throws Exception {
        assertTrue(h.done().await(5, TimeUnit.SECONDS),
                "terminal outcome must arrive via the listener");
        return h.outcome().get();
    }

    @Test
    @DisplayName("append exception propagates to a terminal outcome, never fake success (A)")
    void appendExceptionPropagates() throws Exception {
        // A clearly transient (retryable) failure — e.g. a connection reset —
        // must be retried then surface FAILED through the listener. The key
        // regression guard: it is NEVER a SUCCESS (the old swallow).
        Harness h = harness(new FailingConverter(
                new java.io.IOException("connection reset")));
        h.writer.write(TickPacketFixtures.validTrade(1));
        RawTickWriter.AppendOutcome outcome = awaitOutcome(h);
        assertEquals(RawTickWriter.Status.FAILED, outcome.status(),
                "a retryable append exception must surface as FAILED after retries (not SUCCESS)");
        assertEquals(0, h.tracker().pendingRecords(),
                "reservation released at terminal outcome");
        h.writer.close();
    }

    @Test
    @DisplayName("unknown append exception fails closed as FATAL, never fake success (A+R-285)")
    void unknownExceptionFailsClosed() throws Exception {
        // R-285: an unrecognized exception is FATAL (cannot prove retry is
        // safe) — it must propagate as FATAL, never be swallowed to SUCCESS.
        Harness h = harness(new FailingConverter(
                new RuntimeException("unrecognized transport failure")));
        h.writer.write(TickPacketFixtures.validTrade(7));
        RawTickWriter.AppendOutcome outcome = awaitOutcome(h);
        assertEquals(RawTickWriter.Status.FATAL, outcome.status(),
                "unknown exception fails closed as FATAL (not SUCCESS)");
        h.writer.close();
    }

    @Test
    @DisplayName("CancellationException is NOT success — surfaces as UNCERTAIN (A)")
    void cancellationIsNotSuccess() throws Exception {
        // A cancelled in-flight append means the outcome is unknown (may have
        // persisted) — must be UNCERTAIN, never SUCCESS.
        Harness h = harness(new FailingConverter(
                new java.util.concurrent.CancellationException("cancelled")));
        h.writer.write(TickPacketFixtures.validTrade(2));
        RawTickWriter.AppendOutcome outcome = awaitOutcome(h);
        assertEquals(RawTickWriter.Status.UNCERTAIN, outcome.status(),
                "cancellation must surface as UNCERTAIN, never SUCCESS");
        h.writer.close();
    }

    @Test
    @DisplayName("stale-handle exception terminates as FATAL (B)")
    void staleHandleSurfacesFatal() throws Exception {
        // Simulates a surfaced PartitionNotExist (stale tableId after the table
        // was dropped+recreated under the writer). FATAL → the service halts.
        class FakePartitionNotExistException extends RuntimeException {}
        Harness h = harness(new FailingConverter(new FakePartitionNotExistException()));
        h.writer.write(TickPacketFixtures.validTrade(3));
        RawTickWriter.AppendOutcome outcome = awaitOutcome(h);
        assertEquals(RawTickWriter.Status.FATAL, outcome.status(),
                "stale-handle failure must terminate as FATAL (halt), not retry");
        assertEquals(0, h.tracker().pendingRecords());
        h.writer.close();
    }

    @Test
    @DisplayName("success stays SUCCESS and updates the zero-ack heartbeat (C signal)")
    void successUpdatesHeartbeat() throws Exception {
        Harness h = harness(new OkConverter());
        long before = h.writer().lastAppendSuccessEpochMs();
        h.writer.write(TickPacketFixtures.validTrade(4));
        RawTickWriter.AppendOutcome outcome = awaitOutcome(h);
        assertEquals(RawTickWriter.Status.SUCCESS, outcome.status());
        assertTrue(h.writer().lastAppendSuccessEpochMs() >= before,
                "a successful ack advances the zero-ack heartbeat");
        h.writer.close();
    }

    @Test
    @DisplayName("genuinely transient append (timeout-class) is RETRYABLE, retried, not halted")
    void transientAppendRetriedNotHalted() throws Exception {
        // Retryable classifier → writer retries up to MAX_RETRY_ATTEMPTS then
        // FAILED (not FATAL) — retry behavior unchanged for transient errors.
        final int[] calls = {0};
        FlussRowConverter transientFailing = new FlussRowConverter() {
            @Override
            public CompletableFuture<RawTickWriter.AppendResult> append(TickPacket packet) {
                calls[0]++;
                // A timeout-class cause is RETRYABLE (never FATAL).
                return CompletableFuture.failedFuture(
                        new java.util.concurrent.TimeoutException("append timeout"));
            }
            @Override public int estimatedRowSize(TickPacket packet) { return 100; }
            @Override public void close() {}
        };
        Harness h = harness(transientFailing);
        h.writer.write(TickPacketFixtures.validTrade(5));
        RawTickWriter.AppendOutcome outcome = awaitOutcome(h);
        assertEquals(RawTickWriter.Status.FAILED, outcome.status(),
                "retry-exhausted transient failure is FAILED (non-fatal), retry path intact");
        assertTrue(calls[0] > 1, "transient failure must have been retried: " + calls[0]);
        assertEquals(0, h.tracker().pendingRecords());
        h.writer.close();
    }
}
