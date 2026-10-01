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

    /**
     * The SMA as if the forming candle closed now (2026-10-01): the forming
     * close enters the window and, once full, the oldest close rolls out. Pure
     * arithmetic — the closed state is untouched. NaN until the window would be
     * full counting the forming candle.
     */
    @Override
    public double previewOnForming(
            long openPaise,
            long highPaise,
            long lowPaise,
            long closePaise,
            long volume,
            long tickCount) {
        if (count == ring.length) {
            return (sum - ring[next] + closePaise) / ring.length;
        }
        if (count + 1 == ring.length) {
            return (sum + closePaise) / ring.length;
        }
        return Double.NaN;
    }

    @Override
    public double value() {
        return count < ring.length ? Double.NaN : sum / ring.length;
    }
}
