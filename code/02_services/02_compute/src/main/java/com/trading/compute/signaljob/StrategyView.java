package com.trading.compute.signaljob;

import com.trading.compute.feature.FeatureView;

/**
 * The per-instrument bundle the strategy host hands to every strategy callback
 * (2026-10-01 native design): market snapshot, computed features, and the
 * on-demand context view — one shared instance per instrument, so every
 * strategy of that instrument reads identical numbers and no per-strategy
 * copying happens on the hot path.
 *
 * <p>{@link SignalStrategy} exposes default overloads that take this bundle on
 * {@code onLiveTick}, {@code onClosedCandle}, and {@code onContextReady}; the
 * defaults delegate to the pre-existing forms, so a strategy written before
 * the bundle existed keeps working unchanged.
 *
 * <p><b>Stability.</b> The instance is created once per instrument slot and
 * reused for every callback; a strategy may cache the reference. The three
 * component views are never null.
 */
public interface StrategyView {

    /** Latest market snapshot for this instrument (never null; may be empty). */
    MarketView market();

    /** Shared feature state for this instrument (never null). */
    FeatureView features();

    /**
     * On-demand context view for this instrument (never null; the disabled
     * instance when {@code STRATEGY_CONTEXT_ENABLED=false}).
     */
    ContextView context();
}
