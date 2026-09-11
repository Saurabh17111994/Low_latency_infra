// WriterWorker — single writer thread that drains the BoundedQueue and
// submits each packet to RawTickWriter (T5, contract §3.3 Q17/O-1).
//
// The queue is the bounded buffer between the parser and the writer; this
// worker is the ONLY consumer. The Fluss AppendWriter does the actual
// transport batching (batch-timeout=1ms, O-2) — the worker keeps submission
// on one thread so the client's batching is effective (no cross-thread
// contention on the writer's internal buffer).
//
// Design:
//   - Single worker thread (O-1: 1 writer by default; N>1 only if Test D).
//     Daemon (B130): a wedged worker must never hold the JVM open past
//     shutdown — abandonment is counted, never silent.
//   - Drains in a loop: take() → write() (async append, ack via listener).
//   - Shutdown: close() stops acceptance and enforces ONE total budget
//     (B130: worker-stop wait + writer close share it — previously two
//     full sequential deadlines). Overrun is abandoned AND counted.
//   - Interrupt: the remainder is polled (non-blocking, bounded snapshot)
//     and submitted-or-counted, leftovers counted (B128) — never abandoned
//     silently.
//   - No silent drop: every accepted (dequeued) packet goes to write();
//     write() returns ACCEPTED/REJECTED/SKIPPED synchronously, terminal
//     outcomes via the listener.

package com.trading.ingestion.write;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public final class WriterWorker implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger("WriterWorker");

    private final BoundedQueue queue;
    private final RawTickWriter writer;
    /** TOTAL close budget (B130) — worker-stop wait and writer close share it. */
    private final Duration closeBudget;
    private final Thread thread;
    private final CountDownLatch stopped = new CountDownLatch(1);
    // P1-127/129: synchronously-dropped packets (REJECTED/SKIPPED return or
    // write() throw after dequeue). Counted as acknowledged loss — never
    // silent. No requeue: a throwing packet would poison-loop the drain.
    private final AtomicLong syncDropCount = new AtomicLong();
    // B128: queued-but-never-dequeued packets left behind by an interrupt
    // drain (new arrivals past the snapshot). B130: queue depth at
    // close-timeout. Both counted, never silent.
    private final AtomicLong interruptAbandoned = new AtomicLong();
    private final AtomicLong drainTimeoutAbandoned = new AtomicLong();

    public WriterWorker(BoundedQueue queue, RawTickWriter writer, Duration drainDeadline) {
        this.queue = queue;
        this.writer = writer;
        this.closeBudget = drainDeadline;
        this.thread = new Thread(this::run, "writer-worker");
        this.thread.setDaemon(true);
    }

    public void start() {
        thread.start();
    }

    /** Acknowledged-loss count: dequeued packets with no ACCEPTED outcome. */
    public long syncDropCount() {
        return syncDropCount.get();
    }

    /** Queued packets left behind by an interrupt drain (B128). */
    public long interruptAbandoned() {
        return interruptAbandoned.get();
    }

    /** Queue depth at close-timeout abandon (B130). */
    public long drainTimeoutAbandoned() {
        return drainTimeoutAbandoned.get();
    }

    /**
     * Submit one dequeued entry; every non-ACCEPTED outcome is counted
     * loudly (P1-127/129). Shared by the run loop and the interrupt drain.
     */
    private void submitCounted(BoundedQueue.Entry entry) {
        try {
            // Async append; terminal outcome via writer's OutcomeListener.
            // write() reserves tracker budget and returns ACCEPTED; on
            // REJECTED/SKIPPED the tracker is already released by write().
            RawTickWriter.AppendOutcome outcome = writer.write(entry.packet());
            if (outcome.status() != RawTickWriter.Status.ACCEPTED) {
                syncDropCount.incrementAndGet();
                LOG.warning("writer-worker: sync drop status=" + outcome.status()
                        + " detail=" + outcome.detail());
            }
        } catch (Exception e) {
            // P1-129: narrowed from Throwable — Error propagates, never
            // swallowed. The packet was already dequeued: count as
            // acknowledged loss and keep draining (no requeue — a
            // throwing packet would poison-loop).
            syncDropCount.incrementAndGet();
            LOG.severe("writer-worker: write threw, packet counted as loss: " + e);
        }
    }

    /**
     * B128 remainder drain (package-visible for tests): poll what's instantly
     * available — bounded by the snapshot so a flooding producer cannot hold
     * the interrupted thread here — submit-or-count each, then count whatever
     * is still queued. Never silent.
     */
    void drainRemainderAfterInterrupt() {
        int budget = queue.size();
        for (int i = 0; i < budget; i++) {
            BoundedQueue.Entry entry = queue.poll();
            if (entry == null) {
                break;
            }
            submitCounted(entry);
        }
        long left = queue.size();
        if (left > 0) {
            interruptAbandoned.addAndGet(left);
            LOG.severe("writer-worker: interrupt abandoned " + left
                    + " queued packets (counted, never silent)");
        }
    }

    private void run() {
        // Drain until closed AND empty (take() returns null): close() must
        // let the worker process everything already queued (T5-W4 shutdown
        // drain). No running-flag check — acceptance stops at the queue.
        while (true) {
            try {
                BoundedQueue.Entry entry = queue.take();
                if (entry == null) {
                    break; // closed + drained
                }
                submitCounted(entry);
            } catch (InterruptedException ie) {
                // B128: the old break abandoned the remainder silently.
                drainRemainderAfterInterrupt();
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // take() itself failed — nothing dequeued, nothing lost.
                // Log and continue; Error still propagates.
                LOG.severe("writer-worker: queue take failed: " + e);
            }
        }
        stopped.countDown();
    }

    /**
     * Stop accepting new items, drain the queue, wait for pending appends
     * (tracker drain), then stop. Enforces ONE total budget (B130):
     * worker-stop wait and writer close share {@link #closeBudget} —
     * previously two full sequential deadlines. Overrun is abandoned (the
     * worker is a daemon, so it cannot wedge JVM exit) AND counted.
     */
    @Override
    public void close() {
        close(closeBudget);
    }

    /**
     * Close with an explicit budget, so a caller releasing SEVERAL workers can
     * share one deadline instead of granting each a fresh full one (B130's
     * lesson applied one level up: the shutdown path used to hand N workers N
     * sequential full budgets, so the drain phase scaled with FLUSS_WRITERS).
     *
     * @param budget remaining time for the whole worker-stop wait plus the
     *               writer close; zero means "abandon now and count it"
     */
    public void close(Duration budget) {
        queue.close();
        long deadlineNanos = System.nanoTime() + budget.toNanos();
        try {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0
                    || !stopped.await(remaining, TimeUnit.NANOSECONDS)) {
                long left = queue.size();
                drainTimeoutAbandoned.addAndGet(left);
                LOG.severe("writer-worker: stop timed out — abandoned " + left
                        + " queued packets (counted; daemon thread cannot wedge JVM exit)");
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        writer.close(Duration.ofNanos(Math.max(0, deadlineNanos - System.nanoTime())));
    }
}
