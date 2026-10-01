package com.trading.compute.feature;

import com.trading.compute.signaljob.Timeframe;

/**
 * Read-only feature view the strategy host hands to every strategy instance
 * (DEC-056): the values are computed once per instrument and shared, so every
 * strategy of that instrument reads the same numbers — no per-strategy
 * recomputation.
 *
 * <p>TICK features are timeframe-independent (the same value for every
 * declared timeframe); CLOSE features have one value per declared timeframe.
 * A feature that has not produced enough input yet returns
 * {@link Double#NaN} — callers must not treat NaN as zero.
 */
public interface FeatureView {

    /** Latest value of {@code featureId} for {@code tf}; NaN when not yet ready. */
    double latest(int featureId, Timeframe tf);

    /**
     * Registry-name convenience. Throws on an unknown name: a strategy asking
     * for a feature that does not exist is a wiring error, not a runtime state.
     */
    default double latest(String name, Timeframe tf) {
        return latest(
                FeatureRegistry.byName(name)
                        .orElseThrow(() -> new IllegalArgumentException("unknown feature: " + name))
                        .id(),
                tf);
    }

    /**
     * Evolving value of {@code featureId} for {@code tf} (2026-10-01): for a
     * CLOSE feature the value as if the timeframe's forming candle closed at
     * its current state, recomputed from the forming candle the host fed on the
     * latest accepted tick; exactly {@link #latest} when no current forming
     * candle exists or for TICK features (already live). Pure read — never
     * mutates closed state and never lands in storage: stored rows keep the
     * closed-candle values.
     */
    default double latestLive(int featureId, Timeframe tf) {
        return latest(featureId, tf);
    }

    /** Registry-name convenience for {@link #latestLive(int, Timeframe)}. */
    default double latestLive(String name, Timeframe tf) {
        return latestLive(
                FeatureRegistry.byName(name)
                        .orElseThrow(() -> new IllegalArgumentException("unknown feature: " + name))
                        .id(),
                tf);
    }
}
