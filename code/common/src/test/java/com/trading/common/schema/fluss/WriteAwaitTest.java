package com.trading.common.schema.fluss;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

/**
 * Contract of the drop-safety rule: a table may only be dropped once its write future RESOLVED
 * (see {@link WriteAwait} for the measured Fluss Sender busy-loop this protects against).
 * Hermetic — no cluster, no docker, millisecond budgets.
 */
class WriteAwaitTest {

    private static final Duration FAST = Duration.ofMillis(80);

    /**
     * Budget and resolve time for the late-resolution case. The resolver must land inside the
     * SECOND wait window ([budget, 2 x budget]) with real margin: it originally completed at
     * exactly 2 x FAST, i.e. on the deadline, so a loaded machine could report a write
     * UNRESOLVED that had in fact acked (measured 2026-09-18: 1 failure in the plain suite).
     * 1000 ms against a 750 ms budget leaves 500 ms of slack before the 1500 ms deadline.
     */
    private static final Duration LATE_BUDGET = Duration.ofMillis(750);

    private static final long LATE_RESOLVE_AFTER_MS = 1000L;

    @Test
    void completedWriteResolvesImmediately() throws Exception {
        CompletableFuture<String> done = CompletableFuture.completedFuture("ok");
        long t0 = System.nanoTime();
        assertThat(WriteAwait.await(done, "append to t", FAST)).isEqualTo(WriteAwait.State.RESOLVED);
        assertThat(Duration.ofNanos(System.nanoTime() - t0)).isLessThan(FAST);
    }

    @Test
    void writeThatResolvesDuringSecondWaitIsResolvedAndTableMayBeDropped() throws Exception {
        CompletableFuture<String> slow = new CompletableFuture<>();
        Thread resolver = new Thread(() -> {
            try {
                Thread.sleep(LATE_RESOLVE_AFTER_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            slow.complete("late-ack");
        });
        resolver.setDaemon(true);
        resolver.start();
        long t0 = System.nanoTime();
        assertThat(WriteAwait.await(slow, "upsert to t", LATE_BUDGET))
                .isEqualTo(WriteAwait.State.RESOLVED);
        // The first wait must have expired unresolved: that is what makes this the "resolved
        // during the SECOND wait" case rather than a repeat of completedWriteResolvesImmediately.
        assertThat(Duration.ofNanos(System.nanoTime() - t0)).isGreaterThanOrEqualTo(LATE_BUDGET);
    }

    @Test
    void pendingWriteStaysUnresolvedAfterBothWaitsSoTheTableMustBeKept() throws Exception {
        CompletableFuture<String> never = new CompletableFuture<>();
        long t0 = System.nanoTime();
        assertThat(WriteAwait.await(never, "append to t", FAST))
                .isEqualTo(WriteAwait.State.UNRESOLVED);
        // Both budgets were spent before giving up — a single wait must never be enough to
        // declare a batch abandoned.
        assertThat(Duration.ofNanos(System.nanoTime() - t0))
                .isGreaterThanOrEqualTo(FAST.multipliedBy(2).minusMillis(20));
    }

    @Test
    void failedWriteSurfacesItsOwnMessageAndCountsAsResolved() {
        CompletableFuture<String> failed = new CompletableFuture<>();
        failed.completeExceptionally(new CompletionException(
                new IllegalStateException("IcebergKeyEncoder needs exactly one key field")));
        assertThatThrownBy(() -> WriteAwait.await(failed, "upsert to t", FAST))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("exactly one key field");
    }
}
