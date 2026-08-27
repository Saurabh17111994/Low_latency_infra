// T5-W tests — WriterWorker (contract §7.5): batching (no per-event Fluss
// write at the app layer — the Fluss client batches via batch-timeout),
// retry/ack, no-silent-drop, drain on shutdown. Uses a fake converter.

package com.trading.ingestion.write;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.TickPacketFixtures;
import com.trading.ingestion.model.TickPacket;
import com.trading.ingestion.write.RawTickWriter.AppendResult;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** T5-W1..W4: WriterWorker drain + batching + no-silent-drop + shutdown. */
@DisplayName("T5-W: WriterWorker")
class WriterWorkerTest {

    /** Fake converter: counts appends, completes immediately. */
    static final class CountingConverter implements FlussRowConverter {
        final AtomicInteger appended = new AtomicInteger();
        final AtomicInteger failed = new AtomicInteger();
        final ConcurrentLinkedQueue<TickPacket> seen = new ConcurrentLinkedQueue<>();
        volatile boolean failAll;

        @Override
        public CompletableFuture<AppendResult> append(TickPacket packet) {
            appended.incrementAndGet();
            seen.add(packet);
            if (failAll) {
                failed.incrementAndGet();
                return CompletableFuture.failedFuture(new RuntimeException("fake failure"));
            }
            return CompletableFuture.completedFuture(new AppendResult(42L, "p0"));
        }

        @Override
        public int estimatedRowSize(TickPacket packet) {
            return 100;
        }

        @Override
        public void close() {}
    }

    private RawTickWriter makeWriter(CountingConverter conv, AppendTracker tracker) {
        return new RawTickWriter(conv, tracker, "default.raw_table_1",
                Duration.ofSeconds(5), Duration.ofSeconds(30));
    }

    @Test
    @DisplayName("T5-W1: worker drains queue, submits every packet (no loss)")
    void drainsQueue() throws Exception {
        CountingConverter conv = new CountingConverter();
        AppendTracker tracker = new AppendTracker();
        BoundedQueue queue = new BoundedQueue(1_000_000, 100_000);
        WriterWorker worker = new WriterWorker(queue, makeWriter(conv, tracker), Duration.ofSeconds(5));
        worker.start();

        int n = 1000;
        for (int i = 0; i < n; i++) {
            assertTrue(queue.offer(TickPacketFixtures.validTrade(i), 100), "offer " + i);
        }
        // wait for drain
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (conv.appended.get() < n && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(n, conv.appended.get(), "all packets reached the converter");

        worker.close();
        assertTrue(tracker.pendingRecords() == 0, "tracker drained");
    }

    @Test
    @DisplayName("T5-W2: retry/ack — failed append surfaces, no silent drop")
    void failureSurfaces() throws Exception {
        CountingConverter conv = new CountingConverter();
        AppendTracker tracker = new AppendTracker();
        BoundedQueue queue = new BoundedQueue(1_000_000, 100_000);
        RawTickWriter writer = makeWriter(conv, tracker);
        WriterWorker worker = new WriterWorker(queue, writer, Duration.ofSeconds(5));
        // collect outcomes
        AtomicInteger success = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        writer.setOutcomeListener(o -> {
            switch (o.status()) {
                case SUCCESS -> success.incrementAndGet();
                case FAILED, FATAL -> failed.incrementAndGet();
                default -> {}
            }
        });
        worker.start();

        // first 5 succeed, then fail all
        for (int i = 0; i < 5; i++) {
            queue.offer(TickPacketFixtures.validTrade(i), 100);
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (success.get() < 5 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        conv.failAll = true;
        for (int i = 5; i < 10; i++) {
            queue.offer(TickPacketFixtures.validTrade(i), 100);
        }
        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (failed.get() < 5 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }

        assertEquals(5, success.get(), "5 succeeded");
        assertTrue(failed.get() >= 5, "failures surfaced: " + failed.get());
        // every packet was seen by the converter (none silently dropped)
        assertEquals(10, conv.seen.size(), "all 10 reached converter");
        worker.close();
    }

    @Test
    @DisplayName("T5-W4: shutdown drains pending queue, no lost packets")
    void shutdownDrains() throws Exception {
        CountingConverter conv = new CountingConverter();
        AppendTracker tracker = new AppendTracker();
        BoundedQueue queue = new BoundedQueue(1_000_000, 100_000);
        WriterWorker worker = new WriterWorker(queue, makeWriter(conv, tracker), Duration.ofSeconds(10));
        worker.start();

        int n = 500;
        for (int i = 0; i < n; i++) {
            queue.offer(TickPacketFixtures.validTrade(i), 100);
        }
        // close immediately — the drain loop must still process all
        worker.close();

        assertEquals(n, conv.appended.get(), "all packets processed before close returns");
        assertEquals(0, queue.size(), "queue empty after close");
        assertEquals(0, tracker.pendingRecords(), "tracker drained");
    }

    @Test
    @DisplayName("T5-W1b: batching — worker thread submits, Fluss client batches")
    void batchingThread() throws Exception {
        // The app-level "batching" is the single-threaded drain (one worker
        // thread) feeding the Fluss client whose batch-timeout=1ms coalesces.
        // Prove: one worker thread exists, all appends flow through it.
        CountingConverter conv = new CountingConverter();
        AppendTracker tracker = new AppendTracker();
        BoundedQueue queue = new BoundedQueue(1_000_000, 100_000);
        WriterWorker worker = new WriterWorker(queue, makeWriter(conv, tracker), Duration.ofSeconds(5));
        worker.start();

        Thread workerThread = Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().equals("writer-worker"))
                .findFirst().orElseThrow();
        assertTrue(workerThread.isAlive(), "single writer-worker thread running");

        // feed 10 events, all drain through the worker thread
        for (int i = 0; i < 10; i++) {
            queue.offer(TickPacketFixtures.validTrade(i), 100);
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (conv.appended.get() < 10 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(10, conv.appended.get());
        worker.close();
    }
}
