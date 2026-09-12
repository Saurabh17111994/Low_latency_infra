package com.trading.execution.gateway;

import com.trading.common.schema.fluss.BoundedRetry;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * The wall-clock budget for ONE inbound request, shared by every Fluss call that request makes.
 *
 * <p>Before this, each of the eleven retried awaits on {@code /v1/events} was granted its own
 * {@link BoundedRetry#ATTEMPTS} attempts of {@code GATEWAY_REQUEST_TIMEOUT_MS}: 3 x 2s + 400ms per
 * site, eleven sites, is a 72.4s hold on a single request - on the thread that also answers
 * {@code /healthz} (measured under load: 45ms -> 5620ms). A retry budget belongs to the request,
 * not to each step inside it.
 *
 * <p><b>Units.</b> The deadline is held in nanoseconds from {@link System#nanoTime()}, which is the
 * only monotonic clock in the JDK: {@code currentTimeMillis} can step backwards on an NTP
 * correction, which would silently expire or extend a budget. Every name carries its unit
 * ({@code deadlineNanos}, {@code remainingMillis}) and there is exactly one conversion, at the
 * {@link BoundedRetry#runWithin} call, which takes millis. Truncating nanos to millis rounds the
 * budget DOWN, so the worst case is shedding up to 1ms early - never holding too long.
 *
 * <p>The budget is carried on the calling thread. That assumes one thread per request: true today
 * because dispatch is serial (see the executor note on {@code GatewayHttpServer}), and still true
 * under a pool, because the handler chain runs synchronously on the thread that accepted the
 * request. Outside a request - startup warm-up, operator tools, tests - there is no budget to spend
 * and {@link #run} is exactly {@link BoundedRetry#run}.
 */
final class RequestBudget {

    /** No request in flight: {@code runWithin} treats {@code Long.MAX_VALUE} as "no deadline". */
    private static final long NO_REQUEST = Long.MAX_VALUE;

    private static final ThreadLocal<Long> deadlineNanos = new ThreadLocal<>();

    private RequestBudget() {}

    /** Start the budget for the request this thread is handling. */
    static void begin(Duration budget) {
        deadlineNanos.set(System.nanoTime() + budget.toNanos());
    }

    /**
     * Drop the budget. Must always be called in a {@code finally} - a budget left behind would be
     * inherited by the next request served on this thread, with whatever credit the previous one
     * had left.
     */
    static void clear() {
        deadlineNanos.remove();
    }

    /**
     * Run a Fluss call under the request's remaining budget, or under the site's own budget when no
     * request is in flight.
     *
     * <p>Sheds <em>before</em> calling once the budget is spent. Without that check every later site
     * would still be entitled to its own attempt, each up to {@code GATEWAY_REQUEST_TIMEOUT_MS}; an
     * exhausted request would then still hold for eleven more timeouts, which is the thing this
     * class exists to prevent.
     */
    static <T> T run(Callable<T> action) throws Exception {
        long remaining = remainingMillis();
        if (remaining == NO_REQUEST) {
            return BoundedRetry.run(action);
        }
        shedIfSpent(remaining);
        return BoundedRetry.runWithin(action, remaining);
    }

    /**
     * The budgeted twin of {@link BoundedRetry#await}, for sites whose callers already catch
     * {@link InterruptedException} / {@link ExecutionException} / {@link TimeoutException} and which
     * would otherwise need a new catch arm to adopt the budget. Same translation as {@code await},
     * so this is a drop-in at those sites.
     */
    static <T> T await(Callable<T> action)
            throws InterruptedException, ExecutionException, TimeoutException {
        long remaining = remainingMillis();
        if (remaining == NO_REQUEST) {
            return BoundedRetry.await(action);
        }
        if (remaining <= 0) {
            // Deliberately thrown, not wrapped: a shed request and a slow cluster are different
            // failures, and the narrow-throws translation must not blur them together.
            throw new Exhausted("request budget exhausted before this Fluss call; not attempted");
        }
        return BoundedRetry.awaitWithin(action, remaining);
    }

    private static void shedIfSpent(long remaining) throws Exhausted {
        if (remaining <= 0) {
            throw new Exhausted("request budget exhausted before this Fluss call; not attempted");
        }
    }

    /**
     * Remaining millis, or {@link #NO_REQUEST} when this thread holds no request budget. Exposed to
     * the package so the handler's wiring can be observed from a test: a budget nothing sets is
     * indistinguishable from no budget at every call site.
     */
    static long remainingMillis() {
        Long deadline = deadlineNanos.get();
        if (deadline == null) {
            return NO_REQUEST;
        }
        return (deadline - System.nanoTime()) / 1_000_000L;
    }

    /**
     * The request ran out of budget. Distinct from a Fluss timeout on purpose: this failure says the
     * gateway stopped, not that the cluster was slow, and the two are told apart in logs by type.
     */
    static final class Exhausted extends TimeoutException {
        Exhausted(String message) {
            super(message);
        }
    }
}
