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
    CLOSE
}
