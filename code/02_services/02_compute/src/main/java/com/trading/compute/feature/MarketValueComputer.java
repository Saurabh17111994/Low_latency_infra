package com.trading.compute.feature;

import com.trading.compute.signaljob.MarketView;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

/**
 * One market-snapshot value stored as a registered feature (2026-10-02,
 * CHG-512): reads one field of the per-instrument {@link MarketView} on every
 * accepted tick and keeps the latest value for the sealed row of each closing
 * window.
 *
 * <p><b>Presence.</b> The gate decides whether the group/level the field
 * belongs to was ever observed. A false gate holds {@link Double#NaN} and the
 * writer skips NaN — storage never carries a group or level the feed never
 * provided. Inside a present group a {@code 0} value keeps the MarketView
 * convention ("not provided by this feed/mode"), never a fabricated zero.
 *
 * <p>Allocation-free on the tick path: the extractor and gate are stored
 * lambdas, and both calls are primitive.
 */
final class MarketValueComputer implements FeatureComputer {

    private final ToLongFunction<MarketView> extractor;
    private final Predicate<MarketView> present;
    private double value = Double.NaN;

    MarketValueComputer(ToLongFunction<MarketView> extractor, Predicate<MarketView> present) {
        this.extractor = extractor;
        this.present = present;
    }

    @Override
    public void onMarket(MarketView view) {
        value = present.test(view) ? extractor.applyAsLong(view) : Double.NaN;
    }

    @Override
    public double value() {
        return value;
    }
}
