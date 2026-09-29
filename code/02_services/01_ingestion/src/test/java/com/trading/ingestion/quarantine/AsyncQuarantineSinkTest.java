package com.trading.ingestion.quarantine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * M4-4 — quarantine evidence writes must not park the reader on the evidence
 * backend. The direct {@link QuarantineWriter} calls {@code append()} on the
 * reader thread (Fluss's 30 s pool wait, R-297); the async decorator accepts
 * with a non-blocking offer, one daemon writer delivers, overflow routes to
 * the shared H2-2 handler, and {@code close()} drains under the evidence-writer
 * budget and counts what the deadline abandons.
 */
class AsyncQuarantineSinkTest {

    private static final QuarantineWriter.Reason REASON = QuarantineWriter.Reason.INVALID_SCHEMA;

    /** Delegate whose write blocks until released — a wedged-Fluss stand-in. */
    private static final class BlockingSink implements QuarantineSink {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger written = new AtomicInteger();
        final AtomicBoolean closed = new AtomicBoolean();
        private final long maxWaitMs;

        BlockingSink(long maxWaitMs) {
            this.maxWaitMs = maxWaitMs;
        }

        @Override
        public void write(byte[] raw, QuarantineWriter.Reason reason, String detail) {
            write(raw, reason, detail, null, null, null);
        }

        @Override
        public void write(byte[] raw, QuarantineWriter.Reason reason, String detail,
                          Long token, String exchange, String symbol) {
            entered.countDown();
            try {
                release.await(maxWaitMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            written.incrementAndGet();
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }

    /** Delegate that writes instantly. */
    private static final class CapturingSink implements QuarantineSink {
        final AtomicInteger written = new AtomicInteger();
        final java.util.concurrent.atomic.AtomicLong lastToken =
                new java.util.concurrent.atomic.AtomicLong(-1L);

        @Override
        public void write(byte[] raw, QuarantineWriter.Reason reason, String detail) {
            write(raw, reason, detail, null, null, null);
        }

        @Override
        public void write(byte[] raw, QuarantineWriter.Reason reason, String detail,
                          Long token, String exchange, String symbol) {
            if (token != null) {
                lastToken.set(token);
            }
            written.incrementAndGet();
        }

        @Override
        public void close() {
        }
    }

    @Test
    @DisplayName("write never blocks behind a wedged delegate (the reader keeps reading)")
    void writeIsNonBlockingBehindAWedgedDelegate() throws Exception {
        BlockingSink delegate = new BlockingSink(1_000);
        AsyncQuarantineSink sink = new AsyncQuarantineSink(delegate, (s, d) -> {}, 64,
                Duration.ofSeconds(2));
        sink.write(new byte[] {1}, REASON, "in-flight");
        assertTrue(delegate.entered.await(2, TimeUnit.SECONDS), "delegate must be entered");

        long start = System.nanoTime();
        for (int i = 0; i < 10; i++) {
            sink.write(new byte[] {1}, REASON, "queued-" + i);
        }
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
        assertTrue(elapsedMs < 500,
                "write must not wait on the wedged delegate (took " + elapsedMs + " ms)");
        assertEquals(0, sink.dropped(), "queue has room: nothing dropped");

        delegate.release.countDown();
        sink.close();
        assertEquals(11, delegate.written.get(), "every record lands once the delegate recovers");
        assertEquals(0, sink.leftovers(), "a healthy drain leaves no leftovers");
    }

    @Test
    @DisplayName("overflow is counted and routed to the shared handler (QUARANTINE_OVERFLOW)")
    void overflowIsCountedAndRouted() throws Exception {
        BlockingSink delegate = new BlockingSink(1_000);
        List<String> statuses = new CopyOnWriteArrayList<>();
        AsyncQuarantineSink sink = new AsyncQuarantineSink(delegate, (s, d) -> statuses.add(s),
                2, Duration.ofSeconds(2));
        sink.write(new byte[] {1}, REASON, "in-flight");
        assertTrue(delegate.entered.await(2, TimeUnit.SECONDS),
                "the worker holds the in-flight record");
        sink.write(new byte[] {1}, REASON, "q1");
        sink.write(new byte[] {1}, REASON, "q2");
        sink.write(new byte[] {1}, REASON, "overflow"); // queue full → handler

        assertEquals(List.of("QUARANTINE_OVERFLOW"), statuses,
                "a full quarantine queue must route into the H2-2 handler");
        assertEquals(1, sink.dropped());
        delegate.release.countDown();
        sink.close();
        assertEquals(3, delegate.written.get(),
                "in-flight + two queued records land; the overflow does not");
    }

    @Test
    @DisplayName("close drains every queued record; late writes are counted, never silent")
    void closeDrainsAndLateWritesAreCounted() throws Exception {
        CapturingSink delegate = new CapturingSink();
        List<String> statuses = new CopyOnWriteArrayList<>();
        AsyncQuarantineSink sink = new AsyncQuarantineSink(delegate, (s, d) -> statuses.add(s),
                1024, Duration.ofSeconds(2));
        for (int i = 0; i < 99; i++) {
            sink.write(new byte[] {1}, REASON, "q" + i);
        }
        // instrument-scoped form must forward its context through the queue
        sink.write(new byte[] {1}, REASON, "instrument-scoped", 12345L, "NSE", "SYM");
        sink.close();
        assertEquals(100, delegate.written.get(), "close drains before returning");
        assertEquals(12345L, delegate.lastToken.get(), "instrument context survives the queue");
        assertEquals(0, sink.leftovers());
        assertEquals(0, sink.queued());

        sink.write(new byte[] {1}, REASON, "after-close");
        assertEquals(1, sink.dropped(), "a post-close write is counted");
        assertEquals(List.of("QUARANTINE_OVERFLOW"), statuses, "and routed to the handler");
        sink.close(); // idempotent
        assertEquals(1, sink.dropped(), "a second close must not double-count");
    }

    @Test
    @DisplayName("close honors the drain budget and counts what the deadline abandons")
    void closeCountsWhatTheDeadlineAbandons() throws Exception {
        BlockingSink delegate = new BlockingSink(5_000);
        AsyncQuarantineSink sink = new AsyncQuarantineSink(delegate, (s, d) -> {}, 8,
                Duration.ofMillis(100));
        sink.write(new byte[] {1}, REASON, "in-flight");
        assertTrue(delegate.entered.await(2, TimeUnit.SECONDS),
                "the worker holds the in-flight record");
        for (int i = 0; i < 5; i++) {
            sink.write(new byte[] {1}, REASON, "queued-" + i);
        }
        long start = System.nanoTime();
        sink.close();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
        assertTrue(elapsedMs < 2_000,
                "close must honor the drain budget (took " + elapsedMs + " ms)");
        assertEquals(6, sink.leftovers(),
                "queued (5) + in-flight (1) are counted as an upper bound");
        assertEquals(0, delegate.written.get(),
                "the wedged record never completed before the deadline");
        assertTrue(delegate.closed.get(),
                "the delegate release still runs after an abandoned drain");
    }
}
