package com.trading.compute.feature;

/**
 * Simple moving average of close prices over {@code period} closed candles
 * (CLOSE). O(1) per close with a preallocated ring; NaN until the period is
 * filled.
 */
public final class SmaComputer implements FeatureComputer {

    private final double[] ring;
    private int count;
    private int next;
    private double sum;

    public SmaComputer(int period) {
        if (period <= 0) {
            throw new IllegalArgumentException("SMA period must be positive, got " + period);
        }
        this.ring = new double[period];
    }

    @Override
    public void onClose(
            long windowStart,
            long windowEnd,
            long openPaise,
            long highPaise,
            long lowPaise,
            long closePaise,
            long volume,
            long tickCount) {
        if (count == ring.length) {
            sum -= ring[next];
        } else {
            count++;
        }
        ring[next] = closePaise;
        sum += closePaise;
        next = (next + 1) % ring.length;
    }

    @Override
    public double value() {
        return count < ring.length ? Double.NaN : sum / ring.length;
    }
}
