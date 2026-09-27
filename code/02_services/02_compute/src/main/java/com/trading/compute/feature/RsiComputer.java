package com.trading.compute.feature;

/**
 * Wilder's RSI over {@code period} closed candles (CLOSE). O(1) per close
 * with smoothed averages; NaN until the period is filled.
 *
 * <p>Standard Wilder smoothing: the first {@code period} changes seed the
 * averages as simple means, later changes use
 * {@code avg = (avg * (period - 1) + value) / period}.
 */
public final class RsiComputer implements FeatureComputer {

    private final int period;
    private long lastClose = Long.MIN_VALUE;
    private double avgGain;
    private double avgLoss;
    private int changes;

    public RsiComputer(int period) {
        if (period <= 0) {
            throw new IllegalArgumentException("RSI period must be positive, got " + period);
        }
        this.period = period;
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
        if (lastClose == Long.MIN_VALUE) {
            lastClose = closePaise;
            return;
        }
        double change = closePaise - lastClose;
        lastClose = closePaise;
        double gain = Math.max(change, 0.0);
        double loss = Math.max(-change, 0.0);
        changes++;
        if (changes <= period) {
            avgGain += gain / period;
            avgLoss += loss / period;
        } else {
            avgGain = (avgGain * (period - 1) + gain) / period;
            avgLoss = (avgLoss * (period - 1) + loss) / period;
        }
    }

    @Override
    public double value() {
        if (changes < period) {
            return Double.NaN;
        }
        if (avgLoss == 0.0) {
            return 100.0;
        }
        double rs = avgGain / avgLoss;
        return 100.0 - 100.0 / (1.0 + rs);
    }
}
