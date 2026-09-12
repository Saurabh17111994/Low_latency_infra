package com.trading.common.schema.fluss;

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
 *   <li>Transient that outlasts the caller's {@code .get(timeout)} surfaces as an
 *       intermittent {@link TimeoutException} even though the RPC would have recovered
 *       — the C5 "KV point-lookup flakiness". On the projection path the same shape was
 *       measured on the first write to a freshly created table: 2.2-3.7s against the
 *       default 2s {@code GATEWAY_REQUEST_TIMEOUT_MS} (see "what the window actually is").
 *   <li>Non-transient failures complete exceptionally immediately — already fail-fast.
 * </ul>
 *
 * <p>This wraps the await with a small number of attempts and a short backoff so the total
 * budget (3 × caller timeout + 2 × 200ms ≈ 6.4s with defaults) covers the measured window.
 * The internal retry loop must NOT be bounded — it is the mechanism that eventually
 * completes the same future once the cluster recovers; bounding it would convert
 * recoverable outages into guaranteed failures.
 *
 * <p><b>What the window actually is (measured 2026-09-12, dev cluster).</b> It is not a
 * leader-election settle: election completes in ~4ms (tablet log, "Try to become
 * leaderAndFollower" -> "becomes leader"). The cost lands on the FIRST write to a freshly
 * created table, from post-election remote-log/tiering discovery
 * ({@code LogTieringTask.maybeUpdateCopiedOffset} ->
 * {@code remoteLog.getRemoteLogEndOffset()}), which reads the configured datalake per
 * bucket. Measured: first write after CREATE 2.2-3.7s under create/drop churn; 112-251ms
 * when that first write happens >=5s after CREATE; ~120ms in steady state; every later
 * write 9-29ms. At the default 2s bound attempt 1 succeeded 0/15, so this retry is
 * load-bearing for that first write rather than tail insurance.
 *
 * <p>The "~5s leader-election settle" this budget used to cite came from CHG-020 (a
 * different table, a different team) and was carried into CHG-119, whose own record says
 * "Not tested: live-cluster transient injection" — it was never observed here.
 *
 * <p>Pre-warming the projection tables when the gateway starts moves that first write off
 * the request path, after which request-path writes sit ~120ms against the 2s bound and
 * this retry is insurance again. Against a real datalake over the network the window is
 * expected to be longer and more variable than the dev numbers above, so the budget must
 * not be shrunk on their strength.
 *
 * <p>Lives in {@code common} so every Fluss store can share ONE retry path rather than
 * each deciding for itself: when this was gateway-private, only the control-read and
 * ledger paths used it and every other call site silently opted out (P3-270).
 *
 * <p><b>Idempotency precondition (P3-268).</b> A retry after a {@link TimeoutException}
 * re-invokes {@code action} while the previous attempt's future may still be in flight —
 * the orphaned request can still land. Retrying is therefore only safe for an action that
 * is idempotent under repetition: KV upserts keyed by primary key, or point reads.
 * Do NOT retry a LOG append that can duplicate a record unless its dedup key makes the
 * duplicate detectable downstream.
 *
 * <p><b>Budget by context.</b> The total budget must respect whoever is waiting. A caller
 * on a synchronous request path whose own deadline is shorter than the measured window
 * must NOT rely on this to ride it out — see {@link #runWithin(Callable, long)}.
 */
public final class BoundedRetry {

    /** Attempts per call: 3 × GATEWAY_REQUEST_TIMEOUT_MS + 2 × 200ms ≈ 6.4s (see javadoc). */
    public static final int ATTEMPTS = 3;

    /** Backoff between caller-side attempts. */
    public static final long BACKOFF_MILLIS = 200L;

    private BoundedRetry() {}

    /**
     * Run {@code action} up to {@link #ATTEMPTS} times, retrying only failures that mean
     * "cluster transient": {@link TimeoutException} from {@code .get(timeout)}, or a
     * {@link RetriableException} anywhere in the cause chain.
     * Anything else propagates immediately (fail fast — no retry on real errors).
     *
     * <p>Only for actions that satisfy the idempotency precondition in the class javadoc.
     *
     * @throws Exception the last failure once the attempt budget is exhausted
     */
    public static <T> T run(Callable<T> action) throws Exception {
        return runWithin(action, Long.MAX_VALUE);
    }

    /**
     * Run {@code action} with a hard total budget across all attempts, for callers that
     * have a deadline of their own (P3-268/budget-by-context).
     *
     * <p>A synchronous caller whose own deadline is shorter than the cluster's settle must
     * NOT use this to wait longer than its caller will: shed instead of holding the
     * request open. This overload exists so such a caller can bound the retry by its real
     * deadline rather than silently inheriting the full {@link #ATTEMPTS} budget.
     *
     * @param totalBudgetMillis wall-clock budget across all attempts, including backoff
     */
    public static <T> T runWithin(Callable<T> action, long totalBudgetMillis) throws Exception {
        long deadline = totalBudgetMillis == Long.MAX_VALUE
                ? Long.MAX_VALUE
                : System.nanoTime() + totalBudgetMillis * 1_000_000L;
        Exception last = null;
        for (int attempt = 1; attempt <= ATTEMPTS; attempt++) {
            try {
                return action.call();
            } catch (InterruptedException ie) {
                // P3-269: the action itself was interrupted, so restore the flag before
                // propagating — otherwise the caller's cancellation is silently dropped.
                Thread.currentThread().interrupt();
                throw ie;
            } catch (Exception e) {
                last = e;
                if (!isTransient(e)) {
                    throw e;
                }
                if (attempt == ATTEMPTS || System.nanoTime() >= deadline
                        || System.nanoTime() + BACKOFF_MILLIS * 1_000_000L >= deadline) {
                    throw e;
                }
                try {
                    Thread.sleep(BACKOFF_MILLIS);
                } catch (InterruptedException ie) {
                    // P3-476: keep the transient that prompted this retry attached, so the
                    // backoff interrupt does not discard the cause that explains the retry.
                    Thread.currentThread().interrupt();
                    ie.addSuppressed(last);
                    throw ie;
                }
            }
        }
        throw last; // unreachable
    }

    /**
     * Narrow-throws variant for the common case where {@code action} only awaits a Fluss
     * future — the same shape every store uses.
     *
     * <p>{@link #run} declares {@code throws Exception}, which forces every call site to add
     * a {@code catch (Exception)} arm. That friction is self-defeating: it is exactly what
     * makes a call site skip the shared retry path. This variant translates only the
     * residual checked case and lets the three types callers ALREADY catch propagate as
     * themselves, so adding retry requires no new catch arm:
     *
     * <ul>
     *   <li>{@link InterruptedException} — propagates (callers restore the flag).</li>
     *   <li>{@link ExecutionException} / {@link TimeoutException} — propagate as themselves.</li>
     *   <li>{@code RuntimeException} — propagates unchanged, so pinned behaviour such as the
     *       NPE from a released cached handle keeps its exact type.</li>
     *   <li>Any other checked failure — wrapped, since it is not a Fluss await failure.</li>
     * </ul>
     */
    public static <T> T await(Callable<T> action)
            throws InterruptedException, ExecutionException, TimeoutException {
        return awaitWithin(action, Long.MAX_VALUE);
    }

    /**
     * {@link #await}'s narrow-throws contract with {@link #runWithin}'s budget, for callers whose
     * own deadline must bound the retry. The translation is shared with {@link #await} so a budgeted
     * call site needs exactly the same catch arms as an unbudgeted one.
     *
     * @param totalBudgetMillis wall-clock budget across all attempts, including backoff
     */
    public static <T> T awaitWithin(Callable<T> action, long totalBudgetMillis)
            throws InterruptedException, ExecutionException, TimeoutException {
        try {
            return runWithin(action, totalBudgetMillis);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * "Cluster transient" = a {@link TimeoutException}, or a {@link RetriableException}
     * anywhere in the cause chain.
     *
     * <p>P3-270: the previous check tested only the direct cause of an
     * {@link ExecutionException}, so a Fluss transient wrapped one level deeper was
     * misclassified as fatal (no retry), and a {@link RetriableException} thrown directly
     * was missed entirely.
     */
    public static boolean isTransient(Exception e) {
        if (e instanceof TimeoutException) {
            return true;
        }
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof RetriableException) {
                return true;
            }
        }
        return false;
    }
}
