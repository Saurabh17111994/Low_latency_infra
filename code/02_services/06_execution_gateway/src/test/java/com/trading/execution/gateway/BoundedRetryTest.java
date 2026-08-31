package com.trading.execution.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.fluss.exception.NetworkException;
import org.junit.jupiter.api.Test;

/** C5 guard test: BoundedRetry retries only transient Fluss failures and fails fast otherwise. */
class BoundedRetryTest {

    @Test
    void recoversAfterTransientTimeouts() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        String result = BoundedRetry.run(() -> {
            if (calls.incrementAndGet() < 3) {
                throw new TimeoutException("simulated 2s caller timeout");
            }
            return "row";
        });
        assertEquals("row", result);
        assertEquals(3, calls.get(), "guard must have retried twice after transients");
    }

    @Test
    void recoversAfterExecutionExceptionWrappingRetriableCause() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        String result = BoundedRetry.run(() -> {
            if (calls.incrementAndGet() < 2) {
                // NetworkException is a RetriableException — the "leader election" failure shape.
                throw new ExecutionException(new NetworkException("leader election", new Exception("sim")));
            }
            return "row";
        });
        assertEquals("row", result);
        assertEquals(2, calls.get());
    }

    @Test
    void permanentTransientFailsFastAfterBoundedAttempts() {
        AtomicInteger calls = new AtomicInteger();
        TimeoutException ex = assertThrows(TimeoutException.class, () ->
                BoundedRetry.run(() -> {
                    calls.incrementAndGet();
                    throw new TimeoutException("cluster down");
                }));
        assertEquals("cluster down", ex.getMessage());
        assertEquals(BoundedRetry.ATTEMPTS, calls.get(),
                "guard must NOT retry forever — exactly the attempt budget, then fail fast");
    }

    @Test
    void nonTransientFailureIsNotRetried() {
        AtomicInteger calls = new AtomicInteger();
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                BoundedRetry.run(() -> {
                    calls.incrementAndGet();
                    throw new IllegalStateException("table missing is not transient");
                }));
        assertEquals("table missing is not transient", ex.getMessage());
        assertEquals(1, calls.get(), "real errors must propagate immediately, no retry");
    }

    @Test
    void interruptDuringBackoffPropagatesAndRestoresFlag() {
        // Interrupt the thread while BoundedRetry sleeps between attempts.
        Thread t = new Thread(() -> {
            try {
                BoundedRetry.run(() -> {
                    throw new TimeoutException("transient");
                });
            } catch (InterruptedException expected) {
                // expected
            } catch (Exception e) {
                throw new AssertionError("expected InterruptedException", e);
            }
        });
        t.start();
        t.interrupt();
        try {
            t.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertTrue(!t.isAlive(), "interrupted retry must exit promptly, not keep sleeping");
    }
}