/*
 * Bounded awaits for raw-client write futures, plus the drop-safety rule that
 * follows from Fluss 0.9.1's Sender behaviour.
 *
 * WHY THIS EXISTS (measured 2026-09-18, live cluster):
 * Fluss's Sender busy-loops on records whose bucket leader is unknown —
 * Sender.sendWriteData carries the authors' own TODO ("The method sendWriteData
 * is in a busy loop ... we need to introduce delay logic"), and every iteration
 * calls MetadataUtils.sendMetadataRequestAndRebuildCluster. If the record's table
 * is DROPPED while its batch is still pending, the leader can never resolve, so
 * the loop spins without backoff. Observed: 81,386 NONODE metadata requests in
 * 3 s (~27k/s) for 20 dropped smoke twins, tablet pinned at 4.5 cores, ~380 MB
 * of logs in 7 h, and every other request on that server starved — which is what
 * turned ONE slow smoke write into 8-11 smoke timeouts across the suite.
 *
 * THE RULE: a write whose future has not resolved may still hold a pending
 * batch, so its table must not be dropped. Callers await with a budget, allow
 * exactly one more bounded wait for the SAME future (never re-issuing the
 * write), and drop the table only when the write RESOLVED.
 */
package com.trading.common.schema.fluss;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

public final class WriteAwait {

    /** Whether a write future settled inside the await budget(s). */
    public enum State {
        /** The future completed (normally or exceptionally) — no batch can be pending. */
        RESOLVED,
        /**
         * The future was still pending after both waits. A batch MAY still be pending: the
         * caller must keep the write's table alive so the Sender's metadata retry can resolve.
         */
        UNRESOLVED
    }

    private WriteAwait() {}

    /**
     * Awaits {@code write} for {@code budget}; on timeout waits once more for the SAME future
     * (timeout-only tolerance — the write is never re-issued, so no duplicate row). Returns
     * {@link State#UNRESOLVED} only when the future is still pending after both waits; a write
     * that completed exceptionally surfaces its own exception so callers keep their existing
     * error messages (e.g. the documented IcebergKeyEncoder limitation).
     */
    public static State await(CompletableFuture<?> write, String what, Duration budget)
            throws Exception {
        try {
            write.get(budget.toMillis(), TimeUnit.MILLISECONDS);
            return State.RESOLVED;
        } catch (TimeoutException firstTimeout) {
            System.out.println("write-await: " + what + " did not resolve within "
                    + budget.getSeconds() + "s — waiting once more (same future, write NOT re-issued)");
            try {
                write.get(budget.toMillis(), TimeUnit.MILLISECONDS);
                return State.RESOLVED;
            } catch (TimeoutException secondTimeout) {
                System.out.println("write-await: " + what + " unresolved after "
                        + (2 * budget.getSeconds()) + "s — a batch may still be pending; the caller"
                        + " MUST keep its table (dropping it spins the Sender's metadata loop)");
                return State.UNRESOLVED;
            }
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof Exception ex) {
                throw ex;
            }
            throw e;
        }
    }
}
