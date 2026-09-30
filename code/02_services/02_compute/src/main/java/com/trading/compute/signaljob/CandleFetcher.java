package com.trading.compute.signaljob;

import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous closed-candle fetch seam of the strategy context provider.
 *
 * <p>{@link #fetch(ContextKey)} is called on the operator's mailbox thread;
 * the returned future completes on a Fluss client thread. Implementations
 * publish results only through that future — they must never touch operator
 * state, the cache, or strategy code (the provider moves completions into the
 * cache exclusively on the mailbox thread).
 *
 * <p>Single abstract method on purpose: tests fake it with a lambda.
 */
@FunctionalInterface
interface CandleFetcher extends AutoCloseable {

    /**
     * Starts one exact-key lookup. A completed future with a {@code null}
     * value means the window was never written (no trades) — absent, not an
     * error. A failed future means the lookup itself failed.
     */
    CompletableFuture<ContextCandle> fetch(ContextKey key);

    /** Closes the underlying client. Called from operator close (mailbox thread). */
    @Override
    default void close() {}
}
