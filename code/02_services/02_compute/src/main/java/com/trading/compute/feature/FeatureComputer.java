package com.trading.compute.feature;

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

    /** Latest computed value, or {@link Double#NaN} when not ready. */
    double value();
}
