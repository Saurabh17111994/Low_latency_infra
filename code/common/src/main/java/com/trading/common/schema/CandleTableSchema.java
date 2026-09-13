package com.trading.common.schema;

/**
 * Canonical algorithm/configuration version pair for emitted candle rows
 * (tracker 14 P2 — CANDLE-CANONICAL-001). The pair is pinned here as the single
 * source of truth so {@code SignalJobConfig} (startup gate) and any validation
 * filter cannot drift. Changing the pair is a governed change, not a tuning knob.
 *
 * <p>History: this class also carried the {@code feature_candles_15s} KV column
 * contract (15 columns, primary key, bucket/routing constants, and a static
 * drift guard over those lists). That table was retired by the multi-timeframe
 * cutover (2026-09-05); the live candle contracts are {@code CandleLiveColumns}
 * and {@code CandleClosedColumns} (compute module, pinned against DDL 32/33 by
 * {@code CandleLiveColumnsAgreementTest} / {@code CandleClosedColumnsAgreementTest}).
 */
public final class CandleTableSchema {

    private CandleTableSchema() {}

    /** Canonical algorithm version stamped on emitted candle rows. */
    public static final String CANONICAL_ALGORITHM_VERSION = "candle-15s-v1";

    /** Canonical configuration version — paired with {@link #CANONICAL_ALGORITHM_VERSION}. */
    public static final String CANONICAL_CONFIGURATION_VERSION = "1.0.0";
}
