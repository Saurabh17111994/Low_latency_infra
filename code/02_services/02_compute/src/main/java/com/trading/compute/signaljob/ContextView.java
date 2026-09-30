package com.trading.compute.signaljob;

/**
 * Strategy-facing, read-only view of the signal job's on-demand context
 * (docs/plans/2026-09-30-strategy-context-live-fetch.md). A strategy asks for
 * old data here; a miss schedules exactly one asynchronous Fluss lookup
 * against {@code candle_closed} and returns {@code null} for the current tick
 * — the decision then fires on a later live tick of the same window, never
 * deferred to the candle close (the plan's hard constraint).
 *
 * <p><b>Always present (C2).</b> The host hands this view to every live tick.
 * When {@code STRATEGY_CONTEXT_ENABLED=false} (default) it is the disabled
 * instance ({@link #isEnabled()} {@code == false}, every read returns
 * {@code null} and schedules nothing) — zero cost, certified behavior
 * untouched.
 *
 * <p><b>Mailbox thread only.</b> Every method must be called from the
 * strategy-host operator thread (strategy callbacks). Results are published
 * into the cache only by the host's drain on that same thread; a Fluss client
 * thread never touches this view.
 */
public final class ContextView {

    private static final ContextView DISABLED = new ContextView(null);

    private final ContextProvider provider;

    ContextView(ContextProvider provider) {
        this.provider = provider;
    }

    /** The disabled instance handed out when the provider flag is off. */
    static ContextView disabled() {
        return DISABLED;
    }

    /** True when the on-demand provider is open; false on the disabled instance. */
    public boolean isEnabled() {
        return provider != null;
    }

    /**
     * One exact closed candle, or {@code null} when it is not cached. A
     * {@code null} return schedules a fetch when the key has no in-flight
     * fetch (single flight) and the in-flight cap allows it; after the fetch
     * completes, the next call serves the cached value. On the disabled
     * instance this always returns {@code null} and schedules nothing.
     */
    public ContextCandle candle(long token, Timeframe tf, long windowStart) {
        return provider == null ? null : provider.lookup(new ContextKey(token, tf, windowStart));
    }
}
