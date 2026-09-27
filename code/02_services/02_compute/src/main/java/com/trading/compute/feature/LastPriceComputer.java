package com.trading.compute.feature;

/** Last trade price in paise (TICK). */
public final class LastPriceComputer implements FeatureComputer {

    private double pricePaise = Double.NaN;

    @Override
    public void onTick(
            long eventTimeMs, long pricePaise, long formingVolume, long formingTicks) {
        this.pricePaise = pricePaise;
    }

    @Override
    public double value() {
        return pricePaise;
    }
}
