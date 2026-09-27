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
}
