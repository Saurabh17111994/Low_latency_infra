package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * P3-276 regression pin for {@link GatewayShutdown} — the drain contract that used to live inline
 * in {@code ExecutionGatewayMain.main}, where nothing but a live SIGTERM could reach it.
 *
 * <p>Each test drives one clause of that contract with a thread or a timer instead of a cluster:
 * the park is releasable, the cleanup wait is bounded in both directions, and the reader is joined
 * (or reported) rather than closed underneath a running poll loop.
 */
class GatewayShutdownTest {

    /** A released park: main must block until the signal, then return. */
    @Test
    void theStopSignalReleasesThePark() throws Exception {
        GatewayShutdown shutdown = new GatewayShutdown();
        CountDownLatch parked = new CountDownLatch(1);
        Thread main = new Thread(() -> {
            parked.countDown();
            try {
                shutdown.parkUntilStop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "park-test");
        main.setDaemon(true);
        main.start();
        assertThat(parked.await(5, TimeUnit.SECONDS)).as("thread started").isTrue();

        Thread.sleep(100);
        assertThat(main.isAlive())
                .as("the park must block: returning without a signal is the self-join defect inverted")
                .isTrue();

        shutdown.signalStop();
        main.join(5_000);
        assertThat(main.isAlive()).as("the signal must release the park").isFalse();
    }

    /** A hook that waits forever keeps the JVM from ever halting, so the bound must be real. */
    @Test
    void theCleanupWaitGivesUpAtItsBound() {
        GatewayShutdown shutdown = new GatewayShutdown();
        long start = System.nanoTime();
        shutdown.awaitCleanup(50); // nothing calls markCleaned
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(elapsedMs)
                .as("awaitCleanup must return at its bound, not block the halting JVM")
                .isLessThan(5_000);
    }

    /** It must wait for cleanup, and then return at once. */
    @Test
    void theCleanupWaitBlocksUntilMainReportsClosed() throws Exception {
        GatewayShutdown shutdown = new GatewayShutdown();
        Thread hook = new Thread(() -> shutdown.awaitCleanup(30_000), "hook-test");
        hook.setDaemon(true);
        hook.start();

        Thread.sleep(150);
        assertThat(hook.isAlive())
                .as("the hook must WAIT for cleanup: returning early lets the JVM halt mid-close")
                .isTrue();

        shutdown.markCleaned();
        hook.join(2_000);
        assertThat(hook.isAlive()).as("cleanup done must release the hook at once").isFalse();
    }

    /**
     * The join: closing LogScanner under a live poll loop is the observed crash.
     *
     * <p>The reader here needs 200ms to finish after its interrupt, so "the drain returned without
     * joining" is a deterministic assertion failure rather than a microsecond race.
     */
    @Test
    void drainReaderWaitsForThePollThreadToLeaveTheLoop() throws Exception {
        GatewayShutdown shutdown = new GatewayShutdown();
        AtomicBoolean leftLoop = new AtomicBoolean(false);
        Thread reader = new Thread(() -> {
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(30));
            } catch (InterruptedException e) {
                // the poll loop exits on interrupt; this one still has teardown to do
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            leftLoop.set(true);
        }, "reader-drain-test");
        reader.setDaemon(true);
        reader.start();

        assertThat(shutdown.drainReader(reader, 5_000))
                .as("an interrupted poll thread leaves the loop and the drain reports it")
                .isTrue();
        assertThat(leftLoop.get())
                .as("drain must not return while the poll thread is still in the loop: closing "
                        + "LogScanner under it is the ConcurrentModificationException")
                .isTrue();
        reader.join(2_000);
    }

    /** A reader that ignores the interrupt must be reported, not assumed stopped. */
    @Test
    void drainReaderReportsAPollThreadThatIgnoresTheInterrupt() throws Exception {
        GatewayShutdown shutdown = new GatewayShutdown();
        AtomicBoolean release = new AtomicBoolean(false);
        Thread stubborn = new Thread(() -> {
            while (!release.get()) {
                // deliberately ignores interrupts, as a wedged poll loop would
            }
        }, "stubborn-reader-test");
        stubborn.setDaemon(true);
        stubborn.start();

        assertThat(shutdown.drainReader(stubborn, 100))
                .as("a wedged reader must surface as a warning, not as a silent close")
                .isFalse();
        release.set(true);
        stubborn.join(5_000);
    }
}
