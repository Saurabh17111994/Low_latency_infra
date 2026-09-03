package com.trading.compute.signaljob;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

/**
 * Shared latency-histogram factory (step-2 observability).
 *
 * <p>Uses Flink's built-in {@link DescriptiveStatisticsHistogram} (sliding
 * window of 4096 samples, percentile-accurate, no external dep). The window
 * size is the upstream default; 4096 samples at 50k ticks/s is a sub-second
 * window — percentiles stay tight and current.
 */
public final class LatencyHistograms {

    private LatencyHistograms() {
    }

    public static Histogram create() {
        return new DescriptiveStatisticsHistogram(4096);
    }
}
