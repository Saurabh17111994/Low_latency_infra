package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * P3-064 regression pin for the reader loop in {@link ExecutionGatewayMain}.
 *
 * <p>The live defect, reproduced before the fix: the poll loop caught only
 * {@code RuntimeException}, so an {@code Error} — an {@code ExceptionInInitializerError} out of
 * Arrow's native memory access, in the observed run — killed the
 * {@code execution-intent-reader} thread with no {@code fail()}, no restart and no signal, while
 * {@code /readyz} kept answering 200. A dead execution path that reports healthy is worse than a
 * crash, because nothing ever restarts it.
 *
 * <p>The loop runs on its own thread here, exactly as {@code main} runs it, so a regression is an
 * assertion failure rather than an exception escaping the test body.
 *
 * <p>P3-276 is not pinned here: the drain latch and the shutdown park around this loop still live
 * inline in {@code main}, so testing them needs its own extraction.
 */
class ExecutionGatewayMainReaderLoopTest {

    /** An Error from the reader must fail the gateway once and stop the loop. */
    @Test
    void anErrorFromTheReaderFailsTheGatewayAndStopsTheLoop() throws Exception {
        GatewayReadiness readiness = new GatewayReadiness();
        // Precondition: the gateway is up and ready when the reader dies, as it is in production.
        GatewayStartup.applyStartupReadiness(readiness, true);
        assertThat(readiness.snapshot().executionReady())
                .as("precondition: the gateway starts execution-ready")
                .isTrue();

        AtomicBoolean readerFailed = new AtomicBoolean(false);
        CountDownLatch loopReturned = new CountDownLatch(1);
        Thread loop = new Thread(() -> {
            ExecutionGatewayMain.runReaderLoop(
                    () -> {
                        throw new ExceptionInInitializerError("arrow native access");
                    },
                    readiness, readerFailed);
            loopReturned.countDown();
        }, "reader-loop-test");
        loop.setDaemon(true);
        loop.start();

        assertThat(loopReturned.await(5, TimeUnit.SECONDS))
                .as("an Error must stop the loop, not just escape it")
                .isTrue();
        assertThat(readerFailed.get())
                .as("the failure flag is what makes main exit non-zero")
                .isTrue();
        assertThat(readiness.snapshot().healthy()).isFalse();
        assertThat(readiness.snapshot().executionReady()).isFalse();
        assertThat(readiness.snapshot().reason())
                .as("the reason must name the Error, not a generic message")
                .contains("arrow native access");
    }
}
