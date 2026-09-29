package com.trading.ingestion.quarantine;

import com.trading.ingestion.shutdown.BoundedClose;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * M4-4 — quarantine evidence writes never stall the reader.
 *
 * <p>The direct {@link QuarantineWriter} path calls {@code append()} on the
 * reader thread; Fluss's bounded memory-pool wait (30 s, R-297) means a wedged
 * cluster parks tick processing for the whole wait (audit I4). This decorator
 * accepts records into a bounded in-memory queue with a <b>non-blocking</b>
 * offer and hands them to the delegate from ONE daemon writer thread, so the
 * reader is never behind the evidence backend.
 *
 * <p>Overflow is never silent: a full queue — or a write after {@link #close()}
 * — counts the record and invokes the overflow callback. Production wires that
 * callback to the shared H2-2 drop handler ({@code onWriterDrop}) with status
 * {@code QUARANTINE_OVERFLOW}: the loss is counted, the first episode pins one
 * durable journal record, and the fail-fast stop is requested (counted-only
 * during shutdown, where the handler already treats drops as a shedding
 * drain).
 *
 * <p>{@link #close()} stops acceptance, drains under the evidence-writer
 * budget ({@link BoundedClose#budget()}, injectable for tests), then counts
 * every record not yet delivered (queued + in-flight) as an upper bound — an
 * abandoned in-flight write may still land via the delegate's own bounded
 * flush.
 */
public final class AsyncQuarantineSink implements QuarantineSink {

    /** Default bound: 4096 queued quarantine records (M4-4). */
    public static final int DEFAULT_MAX_QUEUE = 4096;

    private static final Logger LOG = LoggerFactory.getLogger(AsyncQuarantineSink.class);

    private final QuarantineSink delegate;
    private final BiConsumer<String, String> overflow;
    private final int maxQueue;
    private final Duration drainBudget;
    private final ArrayBlockingQueue<Entry> queue;
    private final Thread worker;

    /** Guards acceptance vs. offer: close() cannot lose a concurrently offered record. */
    private final Object gate = new Object();
    private boolean accepting = true;

    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong inFlight = new AtomicLong();
    private final AtomicLong leftovers = new AtomicLong();

    /** One queued quarantine record (all six write() parameters). */
    private record Entry(byte[] rawPayload, QuarantineWriter.Reason reason, String detail,
                         Long instrumentToken, String exchange, String symbol) {}

    /** Production form: 4096-record queue, the shared evidence-writer budget. */
    public AsyncQuarantineSink(QuarantineSink delegate, BiConsumer<String, String> overflow) {
        this(delegate, overflow, DEFAULT_MAX_QUEUE, BoundedClose.budget());
    }

    /** Test seam: explicit queue bound and drain budget (same shape as BoundedClose.run). */
    AsyncQuarantineSink(QuarantineSink delegate, BiConsumer<String, String> overflow,
                        int maxQueue, Duration drainBudget) {
        if (delegate == null) {
            throw new IllegalArgumentException("delegate must not be null");
        }
        if (maxQueue < 1) {
            throw new IllegalArgumentException("maxQueue must be >= 1, got " + maxQueue);
        }
        if (drainBudget == null || drainBudget.isNegative()) {
            throw new IllegalArgumentException("drainBudget must not be negative");
        }
        this.delegate = delegate;
        this.overflow = overflow != null ? overflow : (status, detail) -> {};
        this.maxQueue = maxQueue;
        this.drainBudget = drainBudget;
        this.queue = new ArrayBlockingQueue<>(maxQueue);
        this.worker = new Thread(this::drainLoop, "quarantine-async-writer");
        this.worker.setDaemon(true); // an abandoned drain must not wedge JVM exit
        this.worker.start();
    }

    @Override
    public void write(byte[] rawPayload, QuarantineWriter.Reason reason, String detail) {
        enqueue(new Entry(rawPayload, reason, detail, null, null, null));
    }

    @Override
    public void write(byte[] rawPayload, QuarantineWriter.Reason reason, String detail,
                      Long instrumentToken, String exchange, String symbol) {
        enqueue(new Entry(rawPayload, reason, detail, instrumentToken, exchange, symbol));
    }

    private void enqueue(Entry e) {
        boolean closedNow = false;
        boolean overflowed = false;
        synchronized (gate) {
            if (!accepting) {
                closedNow = true;
            } else if (!queue.offer(e)) {
                overflowed = true;
            }
        }
        // Callback outside the gate: the handler logs/journal-spawns and must
        // never extend the critical section the reader shares with close().
        if (closedNow) {
            dropped.incrementAndGet();
            overflow.accept("QUARANTINE_OVERFLOW",
                    "quarantine sink closed; record dropped (reason=" + e.reason() + ")");
        } else if (overflowed) {
            dropped.incrementAndGet();
            overflow.accept("QUARANTINE_OVERFLOW",
                    "quarantine queue full (" + maxQueue + " records, reason=" + e.reason() + ")");
        }
    }

    /** The single daemon writer: drains until closed AND empty. */
    private void drainLoop() {
        while (true) {
            Entry e = queue.poll();
            if (e == null) {
                synchronized (gate) {
                    if (!accepting && queue.isEmpty()) {
                        return; // closed and fully drained
                    }
                }
                try {
                    e = queue.poll(50, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (e == null) {
                    continue;
                }
            }
            inFlight.incrementAndGet();
            try {
                delegate.write(e.rawPayload(), e.reason(), e.detail(),
                        e.instrumentToken(), e.exchange(), e.symbol());
            } catch (Throwable t) {
                // The delegate contract is never-throws; a broken substitute
                // must not kill the drain thread silently.
                LOG.error("quarantine-async: delegate write failed (reason={}): {}",
                        e.reason(), t.getMessage(), t);
            } finally {
                inFlight.decrementAndGet();
            }
        }
    }

    @Override
    public void close() {
        synchronized (gate) {
            if (!accepting) {
                return; // idempotent: leftovers counted once
            }
            accepting = false;
        }
        long deadline = System.nanoTime() + drainBudget.toNanos();
        long remainingMs = (deadline - System.nanoTime()) / 1_000_000L;
        if (remainingMs > 0) {
            try {
                worker.join(remainingMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        long queued = queue.size();
        long flying = inFlight.get();
        if (queued + flying > 0) {
            leftovers.addAndGet(queued + flying);
            LOG.warn("quarantine-async: close abandoned {} record(s) not yet delivered "
                            + "(queued={}, in-flight={}) — the delegate's bounded flush may still land them",
                    queued + flying, queued, flying);
        }
        delegate.close();
    }

    /** Records dropped by overflow or a post-close write (counted; routed to the handler). */
    public long dropped() {
        return dropped.get();
    }

    /** Records still undelivered when the close drain deadline expired (upper bound). */
    public long leftovers() {
        return leftovers.get();
    }

    /** Records waiting in the queue right now. */
    public int queued() {
        return queue.size();
    }
}
