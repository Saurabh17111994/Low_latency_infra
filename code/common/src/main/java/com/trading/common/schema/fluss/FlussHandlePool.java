package com.trading.common.schema.fluss;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Bounded pool of reusable Fluss client handles (Lookuper / UpsertWriter / AppendWriter).
 *
 * <p>Why a pool rather than a cached field: these handles are {@code @NotThreadSafe} and the
 * stores that use them serve a <b>concurrent</b> request path (the gateway's pooled
 * httpExecutor, P3-066). A single cached instance would be an unsynchronized cross-call
 * hazard, which is why those stores created one per call. Nor can they be closed — in Fluss
 * 0.9.1 {@code Lookuper} exposes only {@code lookup()} and {@code TableWriter} only
 * {@code flush()}, so there is no lifecycle to manage. The pool gives reuse without sharing:
 * each handle is borrowed by exactly one caller at a time and returned when the call
 * completes.
 *
 * <p>Two deliberate properties:
 * <ul>
 *   <li><b>Only healthy handles are returned.</b> If the operation throws, the handle is
 *       dropped rather than pooled — a writer whose call failed may be poisoned, and reusing
 *       it would turn one transient failure into a persistent one. Per-call creation
 *       discarded such handles implicitly; the pool must do so explicitly.</li>
 *   <li><b>Bounded idle, unbounded transient.</b> At most {@code maxIdle} handles are
 *       retained; if more callers run concurrently than that, the extras are created on
 *       demand and simply collected when returned instead of pooled. Steady state reuses;
 *       a burst degrades to the old per-call behaviour rather than blocking.</li>
 * </ul>
 *
 * <p>Not synchronized on purpose: borrowing and returning must never serialize the money
 * path. Reuse is tracked by plain counters for tests.
 */
public final class FlussHandlePool<T> {

    /** Default retained handles per pool — enough to cover a small executor's steady state. */
    public static final int DEFAULT_MAX_IDLE = 4;

    /** A handle-using operation that may throw (the stores' calls are all checked). */
    @FunctionalInterface
    public interface HandleUser<T, R> {
        R apply(T handle) throws Exception;
    }

    private final Supplier<T> factory;
    private final int maxIdle;
    private final Deque<T> idle = new ArrayDeque<>();
    private final AtomicInteger created = new AtomicInteger();
    private final AtomicInteger reused = new AtomicInteger();

    public FlussHandlePool(Supplier<T> factory) {
        this(factory, DEFAULT_MAX_IDLE);
    }

    public FlussHandlePool(Supplier<T> factory, int maxIdle) {
        if (factory == null) {
            throw new IllegalArgumentException("factory must not be null");
        }
        if (maxIdle < 0) {
            throw new IllegalArgumentException("maxIdle must be >= 0");
        }
        this.factory = factory;
        this.maxIdle = maxIdle;
    }

    /** Run {@code fn} against a borrowed handle, returning it to the pool only on success. */
    public <R> R with(HandleUser<T, R> fn) throws Exception {
        T handle = borrow();
        R result = fn.apply(handle);
        release(handle);
        return result;
    }

    public T borrow() {
        synchronized (idle) {
            T pooled = idle.pollFirst();
            if (pooled != null) {
                reused.incrementAndGet();
                return pooled;
            }
        }
        created.incrementAndGet();
        return factory.get();
    }

    public void release(T handle) {
        if (handle == null) {
            return;
        }
        synchronized (idle) {
            if (idle.size() < maxIdle) {
                idle.addLast(handle);
            }
            // else: over the idle bound — drop it and let it be collected.
        }
    }

    /** Drop every retained handle. Nothing to close (see the class javadoc) — just release. */
    public void clear() {
        synchronized (idle) {
            idle.clear();
        }
    }

    public int createdForTest() {
        return created.get();
    }

    public int reusedForTest() {
        return reused.get();
    }

    public int idleForTest() {
        synchronized (idle) {
            return idle.size();
        }
    }
}
