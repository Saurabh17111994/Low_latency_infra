package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.flink.metrics.Histogram;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Guard tests for the platform-speed KPI (2026-10-01):
 * {@link StrategyHostFunction#updateIngestToStrategy} must record a sane
 * ingestion-accept → host-read sample and skip unknown/clock-skewed ones,
 * mirroring the step-2 monitor's fail-safe shape.
 */
@DisplayName("StrategyHost ingest_to_strategy KPI guard")
class StrategyHostIngestKpiTest {

    @Test
    @DisplayName("a valid sample is recorded once, as now - ingestTs")
    void validSampleIsRecorded() {
        Histogram histogram = LatencyHistograms.create();
        StrategyHostFunction.updateIngestToStrategy(histogram, 1_000L, 1_050L);
        assertEquals(1L, histogram.getCount());
        assertEquals(50.0, histogram.getStatistics().getMean(), 0.001);
    }

    @Test
    @DisplayName("unset sentinel, zero, skew, and null histogram are all skipped")
    void invalidSamplesAreSkipped() {
        Histogram histogram = LatencyHistograms.create();
        StrategyHostFunction.updateIngestToStrategy(histogram, Long.MIN_VALUE, 10_000L);
        StrategyHostFunction.updateIngestToStrategy(histogram, 0L, 10_000L);
        StrategyHostFunction.updateIngestToStrategy(histogram, 20_000L, 10_000L); // clock skew
        StrategyHostFunction.updateIngestToStrategy(histogram, 1_000L, 1_000L); // zero delta is valid
        assertEquals(1L, histogram.getCount(),
                "only the zero-delta valid sample may be recorded");
        StrategyHostFunction.updateIngestToStrategy(null, 1_000L, 2_000L); // never throws
    }
}
