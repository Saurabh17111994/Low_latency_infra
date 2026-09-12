package com.trading.execution.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.common.schema.fluss.BoundedRetry;
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

    @Test
    void directRetriableExceptionIsRetried() throws Exception {
        // P3-270: a RetriableException thrown DIRECTLY (not wrapped in ExecutionException)
        // was previously misclassified as fatal, so a recoverable transient failed fast.
        AtomicInteger calls = new AtomicInteger();
        String result = BoundedRetry.run(() -> {
            if (calls.incrementAndGet() < 2) {
                throw new NetworkException("leader election", new Exception("sim"));
            }
            return "row";
        });
        assertEquals("row", result);
        assertEquals(2, calls.get());
    }

    @Test
    void retriableExceptionNestedDeeperInTheCauseChainIsRetried() throws Exception {
        // P3-270: only the DIRECT cause of an ExecutionException used to be inspected, so a
        // transient wrapped one level deeper was treated as fatal and never retried.
        AtomicInteger calls = new AtomicInteger();
        String result = BoundedRetry.run(() -> {
            if (calls.incrementAndGet() < 2) {
                throw new ExecutionException(new IllegalStateException(
                        "wrapper", new NetworkException("leader election", new Exception("sim"))));
            }
            return "row";
        });
        assertEquals("row", result);
        assertEquals(2, calls.get());
    }

    @Test
    void interruptThrownByActionPropagatesWithFlagRestored() throws Exception {
        // P3-269: an action that throws InterruptedException must have the interrupt flag
        // restored before the exception propagates, and must not be retried.
        AtomicInteger calls = new AtomicInteger();
        Throwable[] escaped = new Throwable[1];
        boolean[] flagRestored = new boolean[1];
        Thread t = new Thread(() -> {
            try {
                BoundedRetry.run(() -> {
                    calls.incrementAndGet();
                    throw new InterruptedException("cancelled mid-call");
                });
                escaped[0] = new AssertionError("InterruptedException must propagate");
            } catch (InterruptedException expected) {
                flagRestored[0] = Thread.currentThread().isInterrupted();
            } catch (Exception e) {
                escaped[0] = e;
            }
        });
        t.start();
        t.join(2000);
        assertTrue(!t.isAlive(), "interrupted action must exit promptly");
        assertTrue(escaped[0] == null, "unexpected escape: " + escaped[0]);
        assertTrue(flagRestored[0], "P3-269: interrupt flag must be restored before propagating");
        assertEquals(1, calls.get(), "an interrupted action must not be retried");
    }

    @Test
    void interruptDuringBackoffKeepsTheTransientCause() throws Exception {
        // P3-476: the backoff interrupt used to discard the stored transient cause, losing
        // the only explanation of why the retry was in flight.
        Throwable[] caught = new Throwable[1];
        Thread t = new Thread(() -> {
            try {
                BoundedRetry.run(() -> {
                    throw new TimeoutException("the transient that prompted the retry");
                });
            } catch (Exception e) {
                caught[0] = e;
            }
        });
        t.start();
        Thread.sleep(50);
        t.interrupt();
        t.join(2000);
        assertTrue(!t.isAlive(), "interrupted retry must exit promptly");
        assertTrue(caught[0] instanceof InterruptedException,
                "expected InterruptedException, got " + caught[0]);
        assertEquals(1, caught[0].getSuppressed().length,
                "P3-476: the transient that prompted the retry must stay attached");
        assertTrue(caught[0].getSuppressed()[0] instanceof TimeoutException);
    }
}