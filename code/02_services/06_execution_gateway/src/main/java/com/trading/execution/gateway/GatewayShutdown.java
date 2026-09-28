package com.trading.execution.gateway;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The gateway's SIGTERM drain contract, extracted from {@code ExecutionGatewayMain.main} so it can
 * be tested without a live cluster (P3-276).
 *
 * <p>Three things make a shutdown drain instead of being cut off mid-close:
 * <ol>
 *   <li>{@code main} parks on a latch that a shutdown signal can release. The previous
 *       {@code Thread.currentThread().join()} was a self-join: it released nothing, so SIGTERM left
 *       the process to be killed mid-close.</li>
 *   <li>The cleanup wait is BOUNDED. The JVM halts once every hook returns, so the hook cannot wait
 *       forever — and it has to report when it gave up, or "drained cleanly" and "never woke up"
 *       look identical from outside.</li>
 *   <li>The intent-reader poll thread is stopped <em>and joined</em> before the
 *       try-with-resources closes the reader. Fluss's {@code LogScanner} is not safe for
 *       multithreaded access: closing it while the poll loop was still inside threw
 *       {@code ConcurrentModificationException} out of {@code main}, observed live on the first
 *       SIGTERM this path ever saw.</li>
 * </ol>
 */
final class GatewayShutdown {
    private static final Logger LOG = LoggerFactory.getLogger(GatewayShutdown.class);

    /** How long the hook waits for {@link #markCleaned()}; the JVM halts when the hook returns. */
    static final long CLEANUP_WAIT_MILLIS = 15_000L;

    /** How long the drain waits for the reader thread to leave the poll loop. */
    static final long READER_JOIN_MILLIS = 10_000L;

    private final CountDownLatch stop = new CountDownLatch(1);
    private final CountDownLatch cleaned = new CountDownLatch(1);

    /** Registers the JVM hook: release the park, then wait (bounded) for main to finish closing. */
    void installHook() {
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> awaitCleanup(CLEANUP_WAIT_MILLIS), "gateway-shutdown"));
    }

    /** The hook body, separated from the JVM wiring so a test can drive it directly. */
    void awaitCleanup(long waitMillis) {
        stop.countDown();
        try {
            if (!cleaned.await(waitMillis, TimeUnit.MILLISECONDS)) {
                LOG.warn("gateway cleanup did not finish within {}ms of shutdown", waitMillis);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Releases the park. Called by the shutdown hook, and by the reader thread when it stops. */
    void signalStop() {
        stop.countDown();
    }

    /** Parks until a shutdown signal arrives. */
    void parkUntilStop() throws InterruptedException {
        stop.await();
    }

    /** Records that main finished closing the reader and the Fluss stores. */
    void markCleaned() {
        cleaned.countDown();
    }

    /**
     * Stops a poll thread and waits for it to leave the loop.
     *
     * <p>Used by the intent reader and (H1-1) the safety-halt consumer: both hold Fluss scanners
     * that must not be closed from another thread, so the drain has to be visible and bounded.
     *
     * @return false when the thread ignored the interrupt and is still running — main is then
     *     about to close stores underneath it, which is exactly the ConcurrentModificationException
     *     this guard makes visible instead of silent
     */
    boolean drainThread(Thread thread, long joinMillis, String label) {
        thread.interrupt();
        try {
            thread.join(joinMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (thread.isAlive()) {
            LOG.warn("{} did not stop within {}ms; closing under it", label, joinMillis);
            return false;
        }
        return true;
    }

    /** {@link #drainThread} for the intent reader. */
    boolean drainReader(Thread readerThread, long joinMillis) {
        return drainThread(readerThread, joinMillis, "intent reader");
    }
}
