package com.trading.compute.signaljob;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
 *
 * <p><b>Slices and scalars (C4).</b> {@link #slice} reads a run of consecutive
 * windows (scheduling whatever is missing); {@link #warmUp} schedules the same
 * run without reading it, so a first live need is usually a cache hit.
 * {@link #retainScalar}/{@link #scalar} keep small derived values (prior-day
 * levels, opening range, …) inside a fixed 4096-entry LRU — the approved path
 * beyond ~500 raw windows per (instrument, timeframe), decision #4.
 */
public final class ContextView {

    /**
     * C4: the most windows one {@link #slice}/{@link #warmUp} call may
     * address. Beyond this the approved path is derived scalars, not raw
     * slices (decision #4).
     */
    public static final int MAX_SLICE = 64;

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

    /**
     * C4: a run of {@code count} consecutive windows, newest first
     * ({@code newestWindowStart}, − one window, …). Each entry is the cached
     * candle or {@code null} when it is missing (pending, absent, or cooling
     * down) — missing entries are scheduled exactly like {@link #candle}, so a
     * later call serves them.
     */
    public List<ContextCandle> slice(long token, Timeframe tf, long newestWindowStart, int count) {
        checkSliceCount(count);
        List<ContextCandle> slice = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            slice.add(candle(token, tf, newestWindowStart - (long) i * tf.windowMs()));
        }
        return slice;
    }

    /**
     * C4: schedules the same run as {@link #slice} without reading it back —
     * the optional warm-up on first touch (or at a close) that makes the next
     * live need a cache hit. Counted once per call as
     * {@code compute.context.warmups}; the windows show up as the provider's
     * normal fetch/miss counters, so single-flight, the in-flight cap, and the
     * cooldown apply unchanged. A no-op on the disabled instance.
     */
    public void warmUp(long token, Timeframe tf, long newestWindowStart, int count) {
        checkSliceCount(count);
        if (provider == null) {
            return;
        }
        provider.countWarmUp();
        for (int i = 0; i < count; i++) {
            provider.lookup(new ContextKey(token, tf, newestWindowStart - (long) i * tf.windowMs()));
        }
    }

    /** True when a scalar was retained for this (token, name) (C4). */
    public boolean hasScalar(long token, String name) {
        Objects.requireNonNull(name, "name");
        return provider != null
                && provider.getScalar(new ContextProvider.ScalarKey(token, name)) != null;
    }

    /**
     * The retained scalar, or 0 when absent — pair with {@link #hasScalar}
     * when 0 is a real value (C4).
     */
    public long scalar(long token, String name) {
        Objects.requireNonNull(name, "name");
        Long value = provider == null
                ? null
                : provider.getScalar(new ContextProvider.ScalarKey(token, name));
        return value == null ? 0L : value;
    }

    /**
     * Retains one derived scalar under (token, name) inside the provider's
     * fixed 4096-entry LRU (C4, decision #4: the escape hatch beyond ~500 raw
     * windows per (instrument, timeframe)). The value MUST be a pure function
     * of immutable closed-candle data — a re-derivation after a restore must
     * yield the same value; the provider stores, it never derives. No-op on
     * the disabled instance.
     */
    public void retainScalar(long token, String name, long value) {
        Objects.requireNonNull(name, "name");
        if (provider != null) {
            provider.putScalar(new ContextProvider.ScalarKey(token, name), value);
        }
    }

    private static void checkSliceCount(int count) {
        if (count < 0 || count > MAX_SLICE) {
            throw new IllegalArgumentException(
                    "slice count out of range [0, " + MAX_SLICE + "]: " + count);
        }
    }
}
