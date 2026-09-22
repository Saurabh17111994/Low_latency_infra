package com.trading.ingestion.shutdown;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs an evidence writer's release path (flush + Table/Connection close) on a
 * daemon thread under a hard deadline, so a wedged cluster costs a bounded wait
 * and a loud log instead of a hung JVM.
 *
 * <p><b>Why this exists (2026-09-12).</b> The evidence writers (safety-halt,
 * quarantine, discontinuity, typed-tick) released with {@code writer.flush()}
 * and then closed their Table and Connection. Every one of those calls is
 // Version note (2026-09-23): the 0.9.1 claims in this file were re-checked against Fluss 1.0.0 and still hold — flush()/close is still unbounded in 1.0.0 (`fluss-client/.../write/RecordAccumulator.java:149`, unchanged since 0.9.1) and `TableWriter`/`UpsertWriter` still expose no `close()`. Re-check on the next upgrade (DEC-052).
 * unbounded in Fluss 0.9.1:
 *
 * <ul>
 *   <li>{@code flush()} → {@code WriterClient.flush()} →
 *       {@code RecordAccumulator.awaitFlushCompletion()} → a bare
 *       {@code WriteBatch.RequestFuture.await()} ({@code latch.await()}, no
 *       delivery timeout);</li>
 *   <li>{@code FlussConnection.close()} calls
 *       {@code writerClient.close(Duration.ofMillis(Long.MAX_VALUE))} — and the
 *       same for the lookup client — so the release waits ~forever on that same
 *       latch. Bounding only the flush would therefore NOT have made shutdown
 *       bounded; both calls have to sit inside the deadline, which is why this
 *       helper wraps the whole release rather than a bare flush.</li>
 * </ul>
 *
 * <p>{@code RecordAccumulator.close()} does not complete pending batch futures —
 * it only releases memory — so a pending record is discarded at close and its
 * {@code observe()} callback never fires. That is why the flush is <b>kept</b>
 * here rather than deleted: it is what gives a pending write a chance to land
 * (unlike the stores, where the write path already awaited its own ack and the
 * flush was a provable no-op). It is bounded, not removed.
 *
 * <p>These writers use fire-and-forget {@code observe()} — they track no
 * futures — so there is nothing to drain precisely; the deadline is the only
 * available bound.
 *
 * <p>Abandonment is the trade the writer path already makes: {@code WriterWorker}
 * abandons queued packets and counts them, {@code RawTickWriter} forgives under
 * a deadline and logs. The abandoned thread here is a daemon, so it cannot wedge
 * JVM exit — it dies with the process.
 */
public final class BoundedClose {

    private static final Logger LOG = LoggerFactory.getLogger(BoundedClose.class);

    /**
     * Per-writer release budget. Deliberately modest: the three evidence writers
     * are released sequentially from one shutdown hook, so the worst case is
     * {@code 3 x budget}, which must stay inside the container's stop grace
     * period (Docker's default is 10 s). A healthy release takes milliseconds —
     * this bound only ever applies to a wedged cluster.
     *
     * <p>Overridable for tests ({@code EVIDENCE_CLOSE_BUDGET_SECONDS}) so the
     * bounded property is provable without waiting out the production default.
     */
    static final Duration DEFAULT_BUDGET = parseBudgetSeconds(
            System.getenv("EVIDENCE_CLOSE_BUDGET_SECONDS"), Duration.ofSeconds(2));

    private static final AtomicLong ABANDONED = new AtomicLong();

    private BoundedClose() {}

    /** The configured per-writer release budget. */
    public static Duration budget() {
        return DEFAULT_BUDGET;
    }

    /** How many release paths have been abandoned (overran the budget). */
    public static long abandonedCount() {
        return ABANDONED.get();
    }

    /** Test-only reset — production only ever increments. */
    static void resetAbandonedCountForTest() {
        ABANDONED.set(0);
    }

    /**
     * Release under the configured budget.
     *
     * @param who     writer name, for the log line (e.g. {@code "safety-halt-writer"})
     * @param release the release path — flush plus Table/Connection close
     * @return true when the release completed inside the budget
     */
    public static boolean run(String who, Runnable release) {
        return run(who, DEFAULT_BUDGET, release);
    }

    /**
     * Release under an explicit budget.
     *
     * <p>Never throws: a release failure is logged (the callers previously
     * swallowed it the same way) and an overrun is logged at ERROR and counted.
     * Callers are on the shutdown path — throwing there would abandon the
     * remaining shutdown steps.
     *
     * @return true when the release completed inside the budget
     */
    public static boolean run(String who, Duration budget, Runnable release) {
        Thread t = new Thread(() -> {
            try {
                release.run();
            } catch (Throwable e) {
                LOG.warn("{}: release failed: {}", who, e.getMessage(), e);
            }
        }, "evidence-close-" + who);
        // Daemon: an abandoned release must not keep the JVM alive.
        t.setDaemon(true);
        t.start();

        try {
            t.join(Math.max(0L, budget.toMillis()));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            long n = ABANDONED.incrementAndGet();
            LOG.error("{}: interrupted while waiting up to {} ms for release — abandoned "
                    + "(total abandoned={}); the daemon thread cannot wedge JVM exit",
                    who, budget.toMillis(), n);
            return false;
        }

        if (t.isAlive()) {
            long n = ABANDONED.incrementAndGet();
            LOG.error("{}: release did not complete within {} ms — abandoned "
                    + "(total abandoned={}). Pending evidence writes may be lost; the "
                    + "daemon thread cannot wedge JVM exit",
                    who, budget.toMillis(), n);
            return false;
        }
        return true;
    }

    /**
     * Parse the budget override. A malformed value must not break shutdown —
     * and must not throw from a static initializer — so fall back and say so.
     * Package-visible for testing.
     */
    static Duration parseBudgetSeconds(String raw, Duration fallback) {
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            long seconds = Long.parseLong(raw.trim());
            if (seconds <= 0) {
                LOG.warn("EVIDENCE_CLOSE_BUDGET_SECONDS='{}' must be positive — using {} s",
                        raw, fallback.toSeconds());
                return fallback;
            }
            return Duration.ofSeconds(seconds);
        } catch (NumberFormatException e) {
            LOG.warn("EVIDENCE_CLOSE_BUDGET_SECONDS='{}' is not a number — using {} s",
                    raw, fallback.toSeconds());
            return fallback;
        }
    }
}
