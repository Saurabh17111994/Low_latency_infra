package com.trading.execution.gateway;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.fluss.exception.RetriableException;

/**
 * Bounded caller-side retry for Fluss raw-client awaits (C5 fail-fast guard).
 *
 * <p>Fluss 0.9.1's lookup client retries {@link RetriableException} transients internally
 * up to {@code client.lookup.max-retries} (default {@code Integer.MAX_VALUE}), and its
 * netty layer has no timeout on inflight requests (fluss-rpc
 * {@code ServerConnection} TODO at the inflight-request send site). Consequences,
 * verified against the 0.9.1 source:
 *
 * <ul>
 *   <li>Transient that outlasts the caller's {@code .get(timeout)} (e.g. the measured ~5s
 *       leader-election settle vs the default 2s {@code GATEWAY_REQUEST_TIMEOUT_MS})
 *       surfaces as an intermittent {@link TimeoutException} even though the RPC would
 *       have recovered — the "KV point-lookup flakiness" (C5).
 *   <li>Non-transient failures complete exceptionally immediately — already fail-fast.
 * </ul>
 *
 * <p>This wraps the await with a small number of attempts and a short backoff so the total
 * budget (3 × caller timeout + 2 × 200ms ≈ 6.4s with defaults) exceeds the observed
 * 5s settle. The internal retry loop must NOT be bounded — it is the mechanism that
 * eventually completes the same future once the cluster recovers; bounding it would
 * convert recoverable outages into guaranteed failures.
 */
final class BoundedRetry {

    /** Caller-side attempt budget: 3 × GATEWAY_REQUEST_TIMEOUT_MS + 2 × 200ms ≈ 6.4s > 5s settle. */
    static final int ATTEMPTS = 3;

    /** Backoff between caller-side attempts. */
    static final long BACKOFF_MILLIS = 200L;

    private BoundedRetry() {}

    /**
     * Run {@code action} up to {@link #ATTEMPTS} times, retrying only failures that mean
     * "cluster transient": {@link TimeoutException} from {@code .get(timeout)}, or an
     * {@link ExecutionException} whose cause is a Fluss {@link RetriableException}.
     * Anything else propagates immediately (fail fast — no retry on real errors).
     *
     * @throws Exception the last failure once the attempt budget is exhausted
     */
    static <T> T run(Callable<T> action) throws Exception {
        Exception last = null;
        for (int attempt = 1; attempt <= ATTEMPTS; attempt++) {
            try {
                return action.call();
            } catch (Exception e) {
                last = e;
                if (!isTransient(e)) {
                    throw e;
                }
                if (attempt == ATTEMPTS) {
                    throw e;
                }
                try {
                    Thread.sleep(BACKOFF_MILLIS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw ie;
                }
            }
        }
        throw last; // unreachable
    }

    private static boolean isTransient(Exception e) {
        if (e instanceof TimeoutException) {
            return true;
        }
        if (e instanceof ExecutionException && e.getCause() instanceof RetriableException) {
            return true;
        }
        return false;
    }
}