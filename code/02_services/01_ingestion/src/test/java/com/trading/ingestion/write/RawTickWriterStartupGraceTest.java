package com.trading.ingestion.write;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.TickPacketFixtures;
import com.trading.ingestion.model.TickPacket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * CHG-326 — bounded write-path startup grace.
 *
 * <p>The 2026-09-26 daily-runner dry run observed the first appends after a
 * fresh start fail with {@code Failed to update metadata} (a
 * {@code FlussRuntimeException} availability symptom: the coordinator/tablet
 * metadata is not resolvable yet). Under R-285 that unknown class is FATAL, so
 * the JVM exited and the container restart policy revived it twice before the
 * feed stabilized.
 *
 * <p>Contract under test (M4-5 amended): inside a bounded window after the
 * FIRST append, the metadata-not-ready class is retried regardless of an
 * intervening ack (an ack does not mean the tablet finished replaying); every
 * other fatal class, and the expired grace, keep the exact pre-CHG-326
 * behavior (FATAL).
 */
@DisplayName("CHG-326/M4-5: write-path startup grace (metadata-not-ready, bounded, whole window)")
class RawTickWriterStartupGraceTest {

    /** Fails the first {@code failures} appends with {@code cause}, then succeeds. */
    static final class FlakyConverter implements FlussRowConverter {
        private final AtomicInteger calls = new AtomicInteger();
        private final int failures;
        private final Throwable cause;

        FlakyConverter(int failures, Throwable cause) {
            this.failures = failures;
            this.cause = cause;
        }

        @Override
        public CompletableFuture<RawTickWriter.AppendResult> append(TickPacket packet) {
            if (calls.incrementAndGet() <= failures) {
                return CompletableFuture.failedFuture(cause);
            }
            return CompletableFuture.completedFuture(new RawTickWriter.AppendResult(1L, "p0"));
        }

        @Override public int estimatedRowSize(TickPacket packet) { return 100; }
        @Override public void close() {}
    }

    private static final Throwable METADATA =
            new RuntimeException("Failed to update metadata");

    /** Succeeds the first {@code successes} appends, fails the next
     *  {@code failures} with {@code cause}, then succeeds — the early-ack
     *  shape of a real cold start (CHG-356). */
    static final class EarlyAckThenFlakyConverter implements FlussRowConverter {
        private final AtomicInteger calls = new AtomicInteger();
        private final int successes;
        private final int failures;
        private final Throwable cause;

        EarlyAckThenFlakyConverter(int successes, int failures, Throwable cause) {
            this.successes = successes;
            this.failures = failures;
            this.cause = cause;
        }

        @Override
        public CompletableFuture<RawTickWriter.AppendResult> append(TickPacket packet) {
            int call = calls.incrementAndGet();
            if (call > successes && call <= successes + failures) {
                return CompletableFuture.failedFuture(cause);
            }
            return CompletableFuture.completedFuture(new RawTickWriter.AppendResult(1L, "p0"));
        }

        @Override public int estimatedRowSize(TickPacket packet) { return 100; }
        @Override public void close() {}
    }

    private record Harness(RawTickWriter writer, AppendTracker tracker,
                           CountDownLatch done,
                           AtomicReference<RawTickWriter.AppendOutcome> outcome) {}

    private static Harness harness(FlussRowConverter converter, Duration grace) {
        AppendTracker tracker = new AppendTracker();
        RawTickWriter writer = grace == null
                ? new RawTickWriter(converter, tracker, "default.raw_table_1",
                        Duration.ofSeconds(5), Duration.ofSeconds(10))
                : new RawTickWriter(converter, tracker, "default.raw_table_1",
                        Duration.ofSeconds(5), Duration.ofSeconds(10), grace);
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
    @DisplayName("metadata-not-ready inside the grace retries and can still succeed")
    void metadataFailureWithinGraceRetriesUntilSuccess() throws Exception {
        Harness h = harness(new FlakyConverter(2, METADATA), Duration.ofSeconds(60));
        h.writer().write(TickPacketFixtures.validTrade(31));
        RawTickWriter.AppendOutcome outcome = awaitOutcome(h);
        assertEquals(RawTickWriter.Status.SUCCESS, outcome.status(),
                "cold-start metadata failures inside the grace must be retried, not fatal");
        assertEquals(0, h.tracker().pendingRecords(), "reservation released on success");
        h.writer().close();
    }

    @Test
    @DisplayName("metadata-not-ready after the grace expires is FATAL (fail closed, unchanged)")
    void metadataFailureAfterGraceIsFatal() throws Exception {
        Harness h = harness(new FlakyConverter(Integer.MAX_VALUE, METADATA),
                Duration.ofMillis(150));
        h.writer().write(TickPacketFixtures.validTrade(32));
        RawTickWriter.AppendOutcome outcome = awaitOutcome(h);
        assertEquals(RawTickWriter.Status.FATAL, outcome.status(),
                "the grace is bounded: expiry restores the R-285 fail-closed FATAL");
        h.writer().close();
    }

    @Test
    @DisplayName("the grace covers ONLY the metadata class — unknown fatal classes stay FATAL")
    void otherFatalClassesAreNotCovered() throws Exception {
        Harness h = harness(new FlakyConverter(Integer.MAX_VALUE,
                new RuntimeException("unrecognized transport failure")), Duration.ofSeconds(60));
        h.writer().write(TickPacketFixtures.validTrade(33));
        RawTickWriter.AppendOutcome outcome = awaitOutcome(h);
        assertEquals(RawTickWriter.Status.FATAL, outcome.status(),
                "R-285 stays fail-closed for every class except metadata-not-ready");
        h.writer().close();
    }

    @Test
    @DisplayName("legacy constructor (no grace) keeps the pre-CHG-326 FATAL behavior")
    void noGraceKeepsLegacyBehavior() throws Exception {
        Harness h = harness(new FlakyConverter(Integer.MAX_VALUE, METADATA), null);
        h.writer().write(TickPacketFixtures.validTrade(34));
        RawTickWriter.AppendOutcome outcome = awaitOutcome(h);
        assertEquals(RawTickWriter.Status.FATAL, outcome.status(),
                "without a configured grace the classifier result is untouched");
        h.writer().close();
    }

    @Test
    @DisplayName("grace clock starts with the first append and expires on wall time")
    void graceRemainingTracksFirstAppend() throws Exception {
        Harness h = harness(new FlakyConverter(Integer.MAX_VALUE, METADATA),
                Duration.ofMillis(300));
        assertTrue(h.writer().startupGraceRemainingMs() > 0,
                "configured but no data yet: the grace is armed, not expired");
        h.writer().write(TickPacketFixtures.validTrade(35));
        Thread.sleep(400);
        assertEquals(0, h.writer().startupGraceRemainingMs(),
                "the window starts with the first append (the bridge may connect minutes later)");
        h.writer().close();
    }

    @Test
    @DisplayName("an early ack does not close the window — later metadata failures still retry")
    void earlyAckDoesNotEndTheGrace() throws Exception {
        AppendTracker tracker = new AppendTracker();
        RawTickWriter writer = new RawTickWriter(
                new EarlyAckThenFlakyConverter(1, 3, METADATA), tracker,
                "default.raw_table_1", Duration.ofSeconds(5), Duration.ofSeconds(10),
                Duration.ofSeconds(60));
        java.util.concurrent.BlockingQueue<RawTickWriter.AppendOutcome> outcomes =
                new java.util.concurrent.LinkedBlockingQueue<>();
        writer.setOutcomeListener(outcomes::add);

        writer.write(TickPacketFixtures.validTrade(36));
        RawTickWriter.AppendOutcome first = outcomes.poll(5, TimeUnit.SECONDS);
        assertNotNull(first, "the early append must reach a terminal outcome");
        assertEquals(RawTickWriter.Status.SUCCESS, first.status(), "the early append acks");
        assertTrue(writer.lastAppendSuccessEpochMs() > 0, "the ack is recorded");

        writer.write(TickPacketFixtures.validTrade(37));
        RawTickWriter.AppendOutcome second = outcomes.poll(5, TimeUnit.SECONDS);
        assertNotNull(second, "the second append must reach a terminal outcome");
        assertEquals(RawTickWriter.Status.SUCCESS, second.status(),
                "an intervening ack must not close the metadata-retry window "
                        + "(an ack does not mean the tablet finished replaying)");
        assertEquals(0, tracker.pendingRecords(), "reservation released on success");
        writer.close();
    }
}
