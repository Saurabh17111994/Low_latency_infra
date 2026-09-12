package com.trading.execution.gateway;

import com.trading.common.schema.fluss.BoundedRetry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A request's retry budget belongs to the request, not to each Fluss call inside it (C2).
 *
 * <p>Before this, every one of the eleven retried awaits on {@code /v1/events} was granted its own
 * {@link BoundedRetry#ATTEMPTS} attempts of {@code GATEWAY_REQUEST_TIMEOUT_MS}: 11 x 3 x 2s plus
 * backoff is a 72.4s hold on one request, on the single thread that also answers {@code /healthz}.
 *
 * <p>The action fails immediately, so these assertions are about budget arithmetic rather than
 * cluster timing - the attempt counter is exact where a stopwatch would be flaky.
 */
class RequestBudgetTest {

    private static final Duration SITE_BUDGET = Duration.ofMillis(400);

    @AfterEach
    void tearDown() {
        RequestBudget.clear();
    }

    private static Callable<String> transientFailure(AtomicInteger attempts) {
        return () -> {
            attempts.incrementAndGet();
            throw new TimeoutException("transient fluss failure");
        };
    }

    @Test
    void withoutARequestTheSiteKeepsItsOwnBudget() {
        AtomicInteger attempts = new AtomicInteger();
        assertThrows(TimeoutException.class, () -> RequestBudget.run(transientFailure(attempts)));
        assertEquals(BoundedRetry.ATTEMPTS, attempts.get(),
                "no request in flight: startup warm-up, operator tools and tests keep the "
                        + "per-site budget");
    }

    @Test
    void aRequestBudgetCapsAttemptsInsteadOfGrantingThemPerSite() {
        AtomicInteger attempts = new AtomicInteger();
        RequestBudget.begin(SITE_BUDGET);
        assertThrows(TimeoutException.class, () -> RequestBudget.run(transientFailure(attempts)));
        assertTrue(attempts.get() <= 2,
                "a 400ms request budget allows one retry, not the site's full "
                        + BoundedRetry.ATTEMPTS + " attempts; saw " + attempts.get());
        assertTrue(attempts.get() >= 1, "the call must still be attempted");
    }

    @Test
    void anExhaustedBudgetShedsWithoutAttempting() {
        AtomicInteger attempts = new AtomicInteger();
        RequestBudget.begin(Duration.ofMillis(1));
        sleep(15);
        assertThrows(RequestBudget.Exhausted.class,
                () -> RequestBudget.run(transientFailure(attempts)));
        assertEquals(0, attempts.get(),
                "an exhausted request must not hand each later site another "
                        + "GATEWAY_REQUEST_TIMEOUT_MS attempt");
    }

    @Test
    void clearingRestoresTheSiteBudget() {
        AtomicInteger attempts = new AtomicInteger();
        RequestBudget.begin(SITE_BUDGET);
        RequestBudget.clear();
        assertThrows(TimeoutException.class, () -> RequestBudget.run(transientFailure(attempts)));
        assertEquals(BoundedRetry.ATTEMPTS, attempts.get(),
                "a budget left behind would leak into the next request on this thread");
    }

    /**
     * The quarantine write on the event path catches exactly InterruptedException /
     * ExecutionException / TimeoutException, so the narrow-throws {@code await} form exists to
     * avoid a new catch arm. A shed request must still arrive as itself through that translation:
     * "the gateway ran out of budget" and "Fluss was slow" are different failures.
     */
    @Test
    void awaitShedsAsItsOwnTypeWithoutWrapping() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        RequestBudget.begin(Duration.ofMillis(1));
        sleep(15);
        assertThrows(RequestBudget.Exhausted.class,
                () -> RequestBudget.await(transientFailure(attempts)));
        assertEquals(0, attempts.get());
    }

    @Test
    void theBudgetBelongsToTheRequestThread() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        RequestBudget.begin(Duration.ofMillis(1));
        AtomicReference<Throwable> otherThread = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try {
                RequestBudget.run(transientFailure(attempts));
            } catch (Throwable t) {
                otherThread.set(t);
            }
        });
        worker.start();
        worker.join();
        assertTrue(otherThread.get() instanceof TimeoutException,
                "expected the site's own TimeoutException, got " + otherThread.get());
        assertEquals(BoundedRetry.ATTEMPTS, attempts.get(),
                "another thread must not inherit this thread's spent budget");
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new AssertionError(ie);
        }
    }
}
