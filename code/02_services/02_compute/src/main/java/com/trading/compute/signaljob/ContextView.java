package com.trading.compute.signaljob;

/**
 * Strategy-facing, read-only view of the signal job's on-demand context
 * (docs/plans/2026-09-30-strategy-context-live-fetch.md). A strategy asks for
 * old data here; a miss schedules exactly one asynchronous Fluss lookup
 * against {@code candle_closed} and returns {@code null} for the current tick
 * — the decision then fires on a later live tick of the same window, never
 * deferred to the candle close (the plan's hard constraint).
 *
 * <p><b>Mailbox thread only.</b> Every method must be called from the
 * strategy-host operator thread (strategy callbacks). Results are published
 * into the cache only by the host's drain on that same thread; a Fluss client
 * thread never touches this view.
 *
 * <p>C1 skeleton: the view is created with the provider behind
 * {@code STRATEGY_CONTEXT_ENABLED} and is not yet handed to strategies — the
 * contract addition ({@code onContextReady}) lands in C2.
 */
public final class ContextView {

    private final ContextProvider provider;

    ContextView(ContextProvider provider) {
        this.provider = provider;
    }

    /** True when the provider is open (always true for a view instance). */
    public boolean isEnabled() {
        return true;
    }

    /**
     * One exact closed candle, or {@code null} when it is not cached. A
     * {@code null} return schedules a fetch when the key has no in-flight
     * fetch (single flight) and the in-flight cap allows it; after the fetch
     * completes, the next call serves the cached value.
     */
    public ContextCandle candle(long token, Timeframe tf, long windowStart) {
        return provider.lookup(new ContextKey(token, tf, windowStart));
    }
}
