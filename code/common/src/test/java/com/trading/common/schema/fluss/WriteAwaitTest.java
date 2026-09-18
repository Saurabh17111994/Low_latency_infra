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
                Thread.sleep(FAST.toMillis() * 2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            slow.complete("late-ack");
        });
        resolver.setDaemon(true);
        resolver.start();
        assertThat(WriteAwait.await(slow, "upsert to t", FAST))
                .isEqualTo(WriteAwait.State.RESOLVED);
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
