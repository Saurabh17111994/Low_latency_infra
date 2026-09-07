package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * P1-234: {@code StubFlussRowConverter.append()} and {@code close()} must
 * serialize on the same monitor, so no append can slip between the
 * closed-check and the ack. White-box guard: while a thread holds the
 * stub's monitor, both methods must block.
 */
@DisplayName("P1-234: stub append/close share one monitor")
class StubFlussRowConverterConcurrencyTest {

    private static ExecutorService daemonSingle() {
        return Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "stub-monitor-probe");
            t.setDaemon(true);
            return t;
        });
    }

    /** Holds the stub's monitor for ~1.5s; returns the holder thread. */
    private static Thread holdMonitor(Object stub, CountDownLatch held) {
        Thread holder = new Thread(() -> {
            synchronized (stub) {
                held.countDown();
                try {
                    Thread.sleep(1500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        holder.setDaemon(true);
        holder.start();
        return holder;
    }

    @Test
    @DisplayName("append blocks while the shared monitor is held")
    void appendBlocksOnSharedMonitor() throws Exception {
        StubFlussRowConverter stub = new StubFlussRowConverter("default.raw_table_1");
        CountDownLatch held = new CountDownLatch(1);
        Thread holder = holdMonitor(stub, held);
        assertTrue(held.await(5, TimeUnit.SECONDS), "holder must take the monitor");

        ExecutorService exec = daemonSingle();
        try {
            Future<?> f = exec.submit(
                    () -> stub.append(TickPacketFixtures.validTrade(0)));
            // Unsynchronized append returns at once; synchronized append
            // blocks until the holder releases the monitor.
            assertThrows(TimeoutException.class, () -> f.get(300, TimeUnit.MILLISECONDS),
                    "append must block on the shared monitor");
            f.cancel(true);
        } finally {
            holder.join(5000);
            exec.shutdownNow();
            stub.close();
        }
    }

    @Test
    @DisplayName("close blocks while the shared monitor is held")
    void closeBlocksOnSharedMonitor() throws Exception {
        StubFlussRowConverter stub = new StubFlussRowConverter("default.raw_table_1");
        CountDownLatch held = new CountDownLatch(1);
        Thread holder = holdMonitor(stub, held);
        assertTrue(held.await(5, TimeUnit.SECONDS), "holder must take the monitor");

        ExecutorService exec = daemonSingle();
        try {
            Future<?> f = exec.submit(stub::close);
            assertThrows(TimeoutException.class, () -> f.get(300, TimeUnit.MILLISECONDS),
                    "close must block on the shared monitor");
            f.cancel(true);
        } finally {
            holder.join(5000);
            exec.shutdownNow();
            stub.close();
        }
    }

    @Test
    @DisplayName("append after close still fails (R-113 sanity)")
    void appendAfterCloseFails() {
        StubFlussRowConverter stub = new StubFlussRowConverter("default.raw_table_1");
        stub.close();
        assertTrue(stub.append(TickPacketFixtures.validTrade(0)).isCompletedExceptionally(),
                "post-close append must fail, never ack");
    }
}
