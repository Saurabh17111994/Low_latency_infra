package com.trading.compute.feature;

import com.trading.compute.signaljob.MarketView;

/**
 * One feature's O(1) rolling state for one (instrument, timeframe) pairing
 * (DEC-056: features are computed once and shared by every strategy).
 *
 * <p><b>Hot-path contract.</b> Neither callback may allocate, block, or throw
 * on the normal path: they run inside the strategy host per tick and per
 * closed candle. State is preallocated at construction; every update is O(1)
 * with primitive fields.
 *
 * <p>{@link #value()} returns {@link Double#NaN} until the feature has enough
 * input to be meaningful (e.g. an SMA before its period is filled); the
 * writer's snapshot skips NaN values, so "not ready" never lands in storage.
 */
public interface FeatureComputer {

    /**
     * Live forming snapshot — one call per accepted trade tick.
     *
     * @param eventTimeMs tick event time (epoch ms)
     * @param pricePaise last trade price in paise
     * @param formingVolume cumulative volume of the forming (smallest) window
     * @param formingTicks cumulative tick count of the forming window
     */
    default void onTick(
            long eventTimeMs, long pricePaise, long formingVolume, long formingTicks) {}

    /**
     * Closed candle of the feature's declared timeframe.
     *
     * @param windowStart window start (epoch ms)
     * @param windowEnd window end (epoch ms)
     * @param openPaise open price in paise
     * @param highPaise high price in paise
     * @param lowPaise low price in paise
     * @param closePaise close price in paise
     * @param volume traded volume
     * @param tickCount number of ticks in the window
     */
    default void onClose(
            long windowStart,
            long windowEnd,
            long openPaise,
            long highPaise,
            long lowPaise,
            long closePaise,
            long volume,
            long tickCount) {}

    /**
     * Latest per-instrument market snapshot (2026-10-02, CHG-512) — called on
     * every accepted tick for {@link FeatureCadence#MARKET} features, before
     * the strategy fan-out and before any sealed row of a closing window is
     * built. The view is the shared instance the host updates in place; a
     * feature reads only the fields it declares. Neither callback may
     * allocate, block, or throw on the normal path.
     *
     * @param view latest-known book + day stats (never null; absent = 0 /
     *     never seen, as {@link MarketView} documents)
     */
    default void onMarket(MarketView view) {}

    /**
     * Value this feature would have if the timeframe's forming candle closed
     * at its current state (2026-10-01), without mutating any closed-candle
     * state. The default is the latest closed value; features with a
     * closed-form preview override it. Recomputed per call and never stored.
     */
    default double previewOnForming(
            long openPaise,
            long highPaise,
            long lowPaise,
            long closePaise,
            long volume,
            long tickCount) {
        return value();
    }

    /** Latest computed value, or {@link Double#NaN} when not ready. */
    double value();
}
