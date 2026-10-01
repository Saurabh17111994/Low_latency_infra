package com.trading.compute.signaljob;

/**
 * Strategy-facing, read-only view of one instrument's latest market snapshot
 * (2026-10-01 native design): the 42 raw market values the platform captures
 * from every accepted tick (trade or quote) plus two freshness clocks.
 *
 * <p><b>Semantics.</b> {@code 0} means "not provided by this feed/mode or not
 * yet seen" — never a fabricated zero. A value is latest-known, not
 * necessarily from the tick that formed the candle the strategy is reading.
 * Prices are paise; quantities are shares; order counts are raw.
 *
 * <p><b>Freshness.</b> The two clocks are <b>change</b> clocks, not carriage
 * clocks: {@link #statsChangedAt()} is the event time of the last tick that
 * changed any of the 12 stats values, {@link #depthChangedAt()} the event time
 * of the last tick that changed any of the 30 depth values. A feed that keeps
 * sending the same book does not move a clock — the age you read is "time
 * since this group last moved", which is the decision-relevant number. Age
 * methods take the caller's {@code now} so event-time and wall-clock callers
 * share one API. A never-seen group reports {@link Long#MAX_VALUE} age.
 *
 * <p><b>Depth ladder.</b> Five levels per side, level 1 = best. Prices and
 * quantities of one side at one level are both required for the level to be
 * usable; {@link #walkAvgPricePaise} and {@link #slippagePaise} walk the real
 * ladder and return {@link Long#MIN_VALUE} when the visible book cannot fill
 * the requested quantity — never a silent partial fill.
 *
 * <p><b>Cost.</b> Pure reads over plain fields; no allocation, no locks. The
 * one shared instance per instrument is updated by the host on the canonical
 * forming row, so every strategy of that instrument reads identical numbers.
 */
public interface MarketView {

    /** Depth levels carried per side (1 = best). */
    int DEPTH_LEVELS = 5;

    /** Sentinel: the visible ladder cannot fill the requested quantity. */
    long INSUFFICIENT_DEPTH = Long.MIN_VALUE;

    // ── presence ─────────────────────────────────────────────────────────

    /**
     * True once at least one of the 12 stats values has been observed
     * (tracked by {@link #statsChangedAt()}).
     */
    boolean hasStats();

    /**
     * True once at least one of the 30 depth values has been observed
     * (tracked by {@link #depthChangedAt()}).
     */
    boolean hasDepth();

    /**
     * True when bid and ask are both usable at {@code level}
     * (price and quantity present).
     *
     * @param level 1..{@link #DEPTH_LEVELS}
     * @throws IllegalArgumentException when the level is out of range
     */
    default boolean hasLevel(int level) {
        checkLevel(level);
        return bidPxPaise(level) > 0L && bidQty(level) > 0L
                && askPxPaise(level) > 0L && askQty(level) > 0L;
    }

    // ── freshness ────────────────────────────────────────────────────────

    /** Event time of the last tick that changed any stats value; 0 = never. */
    long statsChangedAt();

    /** Event time of the last tick that changed any depth value; 0 = never. */
    long depthChangedAt();

    /** The newer of the two change clocks; 0 when nothing was ever seen. */
    default long marketChangedAt() {
        return Math.max(statsChangedAt(), depthChangedAt());
    }

    /** Milliseconds since the stats last changed; {@link Long#MAX_VALUE} when never seen. */
    default long statsAgeMs(long now) {
        return age(statsChangedAt(), now);
    }

    /** Milliseconds since the depth last changed; {@link Long#MAX_VALUE} when never seen. */
    default long depthAgeMs(long now) {
        return age(depthChangedAt(), now);
    }

    /** Milliseconds since either group last changed; {@link Long#MAX_VALUE} when never seen. */
    default long ageMs(long now) {
        return age(marketChangedAt(), now);
    }

    /** True when nothing was seen or the newest change is older than {@code maxAgeMs}. */
    default boolean isStale(long now, long maxAgeMs) {
        return ageMs(now) > maxAgeMs;
    }

    /** Complement of {@link #isStale(long, long)}. */
    default boolean isFresh(long now, long maxAgeMs) {
        return !isStale(now, maxAgeMs);
    }

    // ── stats (12) ───────────────────────────────────────────────────────

    /** Cumulative day buy quantity (TBQ); 0 = not provided. */
    long totalBuyQty();

    /** Cumulative day sell quantity (TSQ); 0 = not provided. */
    long totalSellQty();

    /** Day open price in paise; 0 = not provided. */
    long dayOpenPaise();

    /** Day high price in paise; 0 = not provided. */
    long dayHighPaise();

    /** Day low price in paise; 0 = not provided. */
    long dayLowPaise();

    /** PREVIOUS day's close in paise; 0 = not provided. */
    long prevClosePaise();

    /** Day VWAP in paise; 0 = not provided. */
    long vwapPaise();

    /** Open interest; 0 = not provided. */
    long openInterest();

    /** Open-interest day high; 0 = not provided (standard token stream only). */
    long oiDayHigh();

    /** Open-interest day low; 0 = not provided (standard token stream only). */
    long oiDayLow();

    /** Lower circuit limit in paise; 0 = not provided. */
    long lowerLimitPaise();

    /** Upper circuit limit in paise; 0 = not provided. */
    long upperLimitPaise();

    // ── depth ladder (30) ────────────────────────────────────────────────

    /** Bid price at {@code level} in paise; 0 = absent. */
    long bidPxPaise(int level);

    /** Bid quantity at {@code level}; 0 = absent. */
    long bidQty(int level);

    /** Bid order count at {@code level}; 0 = absent. */
    long bidOrd(int level);

    /** Ask price at {@code level} in paise; 0 = absent. */
    long askPxPaise(int level);

    /** Ask quantity at {@code level}; 0 = absent. */
    long askQty(int level);

    /** Ask order count at {@code level}; 0 = absent. */
    long askOrd(int level);

    // ── derived (computed on read, never stored) ─────────────────────────

    /**
     * Level-1 spread in paise ({@code askPx − bidPx}); 0 when either side is
     * absent. Can be negative on a crossed book.
     */
    default long spreadPaise() {
        long bidPx = bidPxPaise(1);
        long askPx = askPxPaise(1);
        return (bidPx <= 0L || askPx <= 0L) ? 0L : askPx - bidPx;
    }

    /** Sum of the visible bid quantities across all five levels. */
    default long totalBidQty() {
        long sum = 0L;
        for (int level = 1; level <= DEPTH_LEVELS; level++) {
            sum += bidQty(level);
        }
        return sum;
    }

    /** Sum of the visible ask quantities across all five levels. */
    default long totalAskQty() {
        long sum = 0L;
        for (int level = 1; level <= DEPTH_LEVELS; level++) {
            sum += askQty(level);
        }
        return sum;
    }

    /**
     * Book imbalance in {@code [-1, 1]}: {@code (bid − ask) / (bid + ask)}
     * over the visible ladder; {@link Double#NaN} when both sides are empty
     * (not-ready convention, same as {@code FeatureView}).
     */
    default double depthImbalance() {
        long bid = totalBidQty();
        long ask = totalAskQty();
        long total = bid + ask;
        return total == 0L ? Double.NaN : (double) (bid - ask) / (double) total;
    }

    /**
     * Size-weighted level-1 price in paise (microprice):
     * {@code (bidPx·askQty + askPx·bidQty) / (bidQty + askQty)}, rounded
     * half-up. 0 when level 1 is not usable.
     */
    default long micropricePaise() {
        long bidPx = bidPxPaise(1);
        long askPx = askPxPaise(1);
        long bidQty = bidQty(1);
        long askQty = askQty(1);
        if (bidPx <= 0L || askPx <= 0L || bidQty <= 0L || askQty <= 0L) {
            return 0L;
        }
        long total = bidQty + askQty;
        return (bidPx * askQty + askPx * bidQty + total / 2L) / total;
    }

    /**
     * Volume-weighted average fill price in paise for {@code qty} shares on
     * the given side, walking levels 1..5 at their visible quantities.
     * Returns {@link #INSUFFICIENT_DEPTH} when {@code qty <= 0} or the visible
     * ladder cannot fill it — a strategy must see "cannot fill", never a
     * silent partial fill.
     *
     * @param buy true = lift asks, false = hit bids
     * @param qty requested quantity in shares
     */
    default long walkAvgPricePaise(boolean buy, long qty) {
        if (qty <= 0L) {
            return INSUFFICIENT_DEPTH;
        }
        long remaining = qty;
        long cost = 0L;
        for (int level = 1; level <= DEPTH_LEVELS && remaining > 0L; level++) {
            long px = buy ? askPxPaise(level) : bidPxPaise(level);
            long available = buy ? askQty(level) : bidQty(level);
            if (px <= 0L || available <= 0L) {
                continue;
            }
            long take = Math.min(remaining, available);
            cost += take * px;
            remaining -= take;
        }
        if (remaining > 0L) {
            return INSUFFICIENT_DEPTH;
        }
        return (cost + qty / 2L) / qty;
    }

    /**
     * Estimated adverse slippage in paise: how much worse than the best price
     * on that side the volume-weighted fill is (always {@code >= 0}; a buy
     * pays above the best ask, a sell receives below the best bid).
     * {@link #INSUFFICIENT_DEPTH} under the same condition as
     * {@link #walkAvgPricePaise}.
     */
    default long slippagePaise(boolean buy, long qty) {
        long avg = walkAvgPricePaise(buy, qty);
        if (avg == INSUFFICIENT_DEPTH) {
            return INSUFFICIENT_DEPTH;
        }
        long best = buy ? askPxPaise(1) : bidPxPaise(1);
        return buy ? avg - best : best - avg;
    }

    private static long age(long changedAt, long now) {
        if (changedAt <= 0L) {
            return Long.MAX_VALUE;
        }
        return now <= changedAt ? 0L : now - changedAt;
    }

    /** Level bounds guard shared by the depth accessors' callers. */
    private static void checkLevel(int level) {
        if (level < 1 || level > DEPTH_LEVELS) {
            throw new IllegalArgumentException(
                    "depth level out of range [1, " + DEPTH_LEVELS + "]: " + level);
        }
    }
}
