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
 * R-037 — on timeout the in-flight append is cancelled and the backpressure
 * release is deferred to the append future's completion (never at the
 * {@code get()} timeout while the append may still be running in the Fluss
 * client's background).
 *
 * <p>Phase 2 (throughput redesign): {@code write()} is asynchronous — it
 * returns {@code ACCEPTED} on submission and the terminal outcome arrives on
 * the {@link RawTickWriter.OutcomeListener}, so every test here observes the
 * outcome through a listener latch instead of the write() return value.
 */
@DisplayName("R-037: RawTickWriter timeout accounting")
class RawTickWriterTimeoutTest {

    /** Stub converter whose append future is controlled by the test. */
    static final class ControllableConverter implements FlussRowConverter {
        volatile CompletableFuture<RawTickWriter.AppendResult> pending =
                new CompletableFuture<>();

        @Override
        public CompletableFuture<RawTickWriter.AppendResult> append(TickPacket packet) {
            return pending;
        }

        @Override
        public int estimatedRowSize(TickPacket packet) {
            return 100;
        }

        @Override
        public void close() {}
    }

    /** Writer + latch wired to the first delivered outcome. */
    private record Harness(RawTickWriter writer,
                           ControllableConverter converter,
                           AppendTracker tracker,
                           CountDownLatch done,
                           AtomicReference<RawTickWriter.AppendOutcome> outcome) {
    }

    private static Harness harness(Duration appendTimeout) {
        ControllableConverter converter = new ControllableConverter();
        AppendTracker tracker = new AppendTracker();
        RawTickWriter writer = new RawTickWriter(
                converter, tracker, "default.raw_table_1",
                appendTimeout, Duration.ofSeconds(1));
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<RawTickWriter.AppendOutcome> outcome =
                new AtomicReference<>();
        writer.setOutcomeListener(o -> {
            outcome.set(o);
            done.countDown();
        });
        return new Harness(writer, converter, tracker, done, outcome);
    }

    private static RawTickWriter.AppendOutcome awaitOutcome(Harness h) throws Exception {
        assertTrue(h.done().await(5, TimeUnit.SECONDS),
                "terminal outcome must arrive via the listener");
        return h.outcome().get();
    }

    @Test
    @DisplayName("timeout cancels the in-flight append and releases via the future")
    void timeoutCancelsAndReleasesViaFuture() throws Exception {
        Harness h = harness(Duration.ofMillis(50));
        RawTickWriter.AppendOutcome submitted = h.writer().write(TickPacketFixtures.validTrade(0));

        assertEquals(RawTickWriter.Status.ACCEPTED, submitted.status(),
                "write() returns ACCEPTED on submission; outcome is async");
        RawTickWriter.AppendOutcome outcome = awaitOutcome(h);

        assertEquals(RawTickWriter.Status.UNCERTAIN, outcome.status(),
                "timeout outcome stays UNCERTAIN (may be a duplicate)");
        assertTrue(h.converter().pending.isCancelled(),
                "in-flight append must be cancelled on timeout");

        // The reservation is released by the future's completion (cancellation),
        // not while the append might still be running — no leaked reservation,
        // and the tracker contract holds.
        assertEquals(0, h.tracker().pendingRecords(),
                "no leaked reservation after timeout");
        assertEquals(0, h.tracker().pendingBytes());
        assertEquals(1, h.writer().uncertainCount());
    }

    @Test
    @DisplayName("UNCERTAIN outcome carries packet identity (P1-110/116)")
    void uncertainCarriesIdentity() throws Exception {
        Harness h = harness(Duration.ofMillis(50));
        TickPacket packet = TickPacketFixtures.validTrade(7);
        h.writer().write(packet);
        RawTickWriter.AppendOutcome outcome = awaitOutcome(h);
        assertEquals(RawTickWriter.Status.UNCERTAIN, outcome.status());
        assertEquals(packet.eventFingerprint(), outcome.fingerprint(),
                "UNCERTAIN must be correlatable to its row");
        assertEquals(packet.instrumentToken(), outcome.instrumentToken());
        assertEquals(packet.eventTime(), outcome.eventTime());
    }

    @Test
    @DisplayName("late completion of a timed-out append does not double-release")
    void lateCompletionDoesNotDoubleRelease() throws Exception {
        Harness h = harness(Duration.ofMillis(50));
        h.writer().write(TickPacketFixtures.validTrade(1));
        RawTickWriter.AppendOutcome outcome = awaitOutcome(h);
        assertEquals(RawTickWriter.Status.UNCERTAIN, outcome.status());
        assertEquals(0, h.tracker().pendingRecords());

        // Simulate the abandoned append surfacing a late failure in the
        // client's background — the accounting must stay exact (no double
        // release below zero, no second reservation).
        h.converter().pending.completeExceptionally(new RuntimeException("late failure"));
        assertEquals(0, h.tracker().pendingRecords(),
                "late completion must not re-release the reservation");
        assertEquals(0, h.tracker().pendingBytes());
    }

    @Test
    @DisplayName("successful appends still release once")
    void successfulAppendReleasesOnce() throws Exception {
        Harness h = harness(Duration.ofSeconds(5));
        // Complete the append before write() so the ack path is exercised.
        h.converter().pending.complete(new RawTickWriter.AppendResult(42L, "p0"));
        RawTickWriter.AppendOutcome submitted = h.writer().write(TickPacketFixtures.validTrade(2));

        // The pre-completed future fires the listener synchronously, but
        // write() itself still reports ACCEPTED (submission status).
        assertEquals(RawTickWriter.Status.ACCEPTED, submitted.status());
        RawTickWriter.AppendOutcome outcome = awaitOutcome(h);
        assertEquals(RawTickWriter.Status.SUCCESS, outcome.status());
        assertEquals(0, h.tracker().pendingRecords(),
                "success releases the reservation exactly once");
        assertEquals(0, h.tracker().pendingBytes());
        assertEquals(1, h.writer().appendCount());
    }
}
