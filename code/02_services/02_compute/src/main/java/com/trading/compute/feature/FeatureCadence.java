package com.trading.compute.feature;

/**
 * When a registered feature's computer is fed (DEC-056 registry).
 *
 * <p>Cadence is part of the registry line, not of the consumer: strategies
 * read {@code latest} values without knowing which input produced them.
 */
public enum FeatureCadence {

    /** Updated on every accepted trade tick (the live forming snapshot). */
    TICK,

    /** Updated on each closed candle of a declared timeframe. */
    CLOSE,

    /**
     * Updated from the per-instrument market snapshot (book + day stats) on
     * every accepted tick (2026-10-02, CHG-512). Timeframe-independent like
     * {@link #TICK}, but fed by {@link FeatureComputer#onMarket} instead of
     * the trade tick — the stored value is the latest-known market state, so
     * every declared timeframe's sealed row carries it at its close.
     */
    MARKET
}
