// BoundedQueue — byte-budgeted bounded queue between the parser and the
// writer (T5, contract §3.3 Q17). The queue holds packets waiting to be
// written; its byte budget (192 MiB default, Q17) is tracked here. The
// AppendTracker (150k/192MiB, 80% warn / 100% halt) continues to account
// in-flight appends AFTER dequeue — the two budgets compose:
//
//   parser → [BoundedQueue: bytes waiting] → writer → [AppendTracker: in-flight]
//
// Design:
//   - offer() enforces maxQueuedBytes (192 MiB) + maxQueuedRecords (150k);
//     returns false when full (halt) — caller records acknowledged loss,
//     NEVER silent drop.
//   - 80% warn + readiness-false via a listener callback (queue >= 80%).
//   - take() blocks until an item is available (or closed).
//   - Exact byte accounting; no unbounded growth, no hidden overflow (T5-Q3).

package com.trading.ingestion.write;

import java.util.ArrayDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import com.trading.ingestion.model.TickPacket;

public final class BoundedQueue {

    /** Default budget: 192 MiB queued bytes, 150k queued records (Q17). */
    public static final long DEFAULT_MAX_BYTES = 201_326_592L; // 192 MiB
    public static final int DEFAULT_MAX_RECORDS = 150_000;
    public static final double WARNING_PERCENT = 0.80;

    /** Receives queue-level backpressure events (80% warn / 100% halt). */
    public interface QueueListener {
        void onQueueEvent(Level level, long records, long bytes, long maxRecords, long maxBytes);
        enum Level { WARNING, CRITICAL }
    }

    private final ArrayDeque<Entry> queue = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final long maxBytes;
    private final int maxRecords;
    private long queuedBytes;
    private boolean warningFired;
    private boolean halted;
    private volatile boolean closed;
    private volatile QueueListener listener = (l, r, b, mr, mb) -> {};

    /** One queued packet + its byte size (for exact accounting). */
    record Entry(TickPacket packet, int rowBytes) {}

    public BoundedQueue() {
        this(DEFAULT_MAX_BYTES, DEFAULT_MAX_RECORDS);
    }

    public BoundedQueue(long maxBytes, int maxRecords) {
        this.maxBytes = maxBytes;
        this.maxRecords = maxRecords;
    }

    public void setListener(QueueListener l) {
        this.listener = l != null ? l : (level, r, b, mr, mb) -> {};
    }

    /**
     * Enqueue one packet. Returns true if accepted, false if the queue is at
     * capacity (halt). Never silently drops — false is a visible REJECTED.
     */
    public boolean offer(TickPacket packet, int rowBytes) {
        lock.lock();
        try {
            if (closed) {
                return false;
            }
            if (halted) {
                return false;
            }
            long newRecords = queue.size() + 1L;
            long newBytes = queuedBytes + rowBytes;
            // 100% halt — immediate, no negotiation. The queue fills to
            // exactly 100% (accepted); the NEXT offer beyond 100% halts.
            if (newRecords > maxRecords || newBytes > maxBytes) {
                halted = true;
                listener.onQueueEvent(QueueListener.Level.CRITICAL,
                        queue.size(), queuedBytes, maxRecords, maxBytes);
                return false;
            }
            queue.addLast(new Entry(packet, rowBytes));
            queuedBytes = newBytes;
            // 80% warn — fire once per saturation episode
            if (!warningFired && (newBytes >= maxBytes * WARNING_PERCENT
                    || newRecords >= maxRecords * WARNING_PERCENT)) {
                warningFired = true;
                listener.onQueueEvent(QueueListener.Level.WARNING,
                        queue.size(), queuedBytes, maxRecords, maxBytes);
            }
            notEmpty.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Block until an item is available. Returns null when closed and drained. */
    public Entry take() throws InterruptedException {
        lock.lock();
        try {
            while (queue.isEmpty() && !closed) {
                notEmpty.await();
            }
            if (queue.isEmpty()) {
                return null;
            }
            Entry e = queue.removeFirst();
            queuedBytes -= e.rowBytes();
            // reset warn latch when back under 80%
            if (warningFired && (queuedBytes < maxBytes * WARNING_PERCENT
                    && queue.size() < maxRecords * WARNING_PERCENT)) {
                warningFired = false;
            }
            return e;
        } finally {
            lock.unlock();
        }
    }

    /** Non-blocking poll; returns null if empty. */
    public Entry poll() {
        lock.lock();
        try {
            if (queue.isEmpty()) {
                return null;
            }
            Entry e = queue.removeFirst();
            queuedBytes -= e.rowBytes();
            return e;
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    public long queuedBytes() {
        lock.lock();
        try {
            return queuedBytes;
        } finally {
            lock.unlock();
        }
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public boolean isHalted() {
        return halted;
    }

    public boolean isClosed() {
        return closed;
    }

    /** Stop accepting; take() drains remaining then returns null. */
    public void close() {
        lock.lock();
        try {
            closed = true;
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
