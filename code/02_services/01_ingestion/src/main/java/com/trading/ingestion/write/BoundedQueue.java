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
    // P1-011/012: halted is an EPISODE flag (full right now), not a sticky
    // latch. Rejection is decided by live fullness below; this only makes
    // the CRITICAL alert fire once per saturation episode. Volatile: read
    // outside the lock in isHalted(), written under it.
    private volatile boolean halted;
    private volatile boolean closed;
    private volatile QueueListener listener = (l, r, b, mr, mb) -> {};

    /** One queued packet + its byte size (for exact accounting). */
    record Entry(TickPacket packet, int rowBytes) {}

    public BoundedQueue() {
        this(DEFAULT_MAX_BYTES, DEFAULT_MAX_RECORDS);
    }

    public BoundedQueue(long maxBytes, int maxRecords) {
        // P1-103/105: fail fast on nonsense budgets — a zero/negative budget
        // silently rejects everything (or corrupts accounting) downstream.
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be > 0: " + maxBytes);
        }
        if (maxRecords <= 0) {
            throw new IllegalArgumentException("maxRecords must be > 0: " + maxRecords);
        }
        this.maxBytes = maxBytes;
        this.maxRecords = maxRecords;
    }

    /** P1-062: expose configured budgets so queue-split policy is testable. */
    public long maxBytes() {
        return maxBytes;
    }

    /** P1-062: expose configured budgets so queue-split policy is testable. */
    public int maxRecords() {
        return maxRecords;
    }

    public void setListener(QueueListener l) {
        this.listener = l != null ? l : (level, r, b, mr, mb) -> {};
    }

    /**
     * Enqueue one packet. Returns true if accepted, false if the queue is at
     * capacity (halt). Rejects only while full — drains recover (P1-011/012).
     * Never silently drops — false is a visible REJECTED.
     */
    public boolean offer(TickPacket packet, int rowBytes) {
        // P1-103/105: validate BEFORE the lock — null defers an NPE to the
        // writer, and rowBytes <= 0 corrupts queuedBytes math (negative
        // values shrink newBytes and bypass the > maxBytes halt check).
        if (packet == null) {
            throw new NullPointerException("packet");
        }
        if (rowBytes <= 0) {
            throw new IllegalArgumentException("rowBytes must be > 0: " + rowBytes);
        }
        // P1-104/106: transition under lock, listener fires AFTER unlock —
        // slow/throwing/re-entrant listener code must never run while the
        // lock is held. The snapshot travels out via locals, never fields.
        final OfferResult result;
        lock.lock();
        try {
            if (closed) {
                return false;
            }
            result = offerLocked(packet, rowBytes);
        } finally {
            lock.unlock();
        }
        PendingEvent fire = result.event();
        if (fire != null) {
            // listener is volatile: the current callback, read off-lock.
            // A throwing listener propagates AFTER state is committed and
            // the lock released — the enqueue/reject stands either way.
            listener.onQueueEvent(fire.level(), fire.records(), fire.bytes(),
                    maxRecords, maxBytes);
        }
        return result.accepted();
    }

    /**
     * P1-104/106: the state transition runs under lock, but the listener
     * fires AFTER unlock (see {@link #offer}). Returns the event to fire, or
     * null. Snapshot values are captured under lock so the callback sees a
     * consistent picture even as the queue keeps moving.
     */
    private record PendingEvent(QueueListener.Level level, long records, long bytes) {}

    private record OfferResult(boolean accepted, PendingEvent event) {}

    private OfferResult offerLocked(TickPacket packet, int rowBytes) {
        long newRecords = queue.size() + 1L;
        long newBytes = queuedBytes + rowBytes;
        // 100% halt — immediate, no negotiation. The queue fills to
        // exactly 100% (accepted); offers beyond 100% are rejected while
        // full. CRITICAL fires once per saturation episode (halted flag),
        // not once per rejected offer.
        if (newRecords > maxRecords || newBytes > maxBytes) {
            if (!halted) {
                halted = true;
                return new OfferResult(false, new PendingEvent(
                        QueueListener.Level.CRITICAL,
                        queue.size(), queuedBytes));
            }
            return new OfferResult(false, null);
        }
        queue.addLast(new Entry(packet, rowBytes));
        queuedBytes = newBytes;
        notEmpty.signal();
        // 80% warn — fire once per saturation episode
        if (!warningFired && (newBytes >= maxBytes * WARNING_PERCENT
                || newRecords >= maxRecords * WARNING_PERCENT)) {
            warningFired = true;
            return new OfferResult(true, new PendingEvent(
                    QueueListener.Level.WARNING,
                    queue.size(), queuedBytes));
        }
        return new OfferResult(true, null);
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
            // reset warn + halt episode when back under 80% (P1-011/012: the
            // halt clears here, so drains recover; hysteresis avoids alert
            // flapping at the boundary)
            if (queuedBytes < maxBytes * WARNING_PERCENT
                    && queue.size() < maxRecords * WARNING_PERCENT) {
                warningFired = false;
                halted = false;
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
            // same episode reset as take(): polls drain too (P1-011/012)
            if (queuedBytes < maxBytes * WARNING_PERCENT
                    && queue.size() < maxRecords * WARNING_PERCENT) {
                warningFired = false;
                halted = false;
            }
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
