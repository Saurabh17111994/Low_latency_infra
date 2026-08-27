package com.trading.mockarrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/** Foundation workload gates MOCK-UNIT-001..003 and MOCK-PERF-001. */
class SyntheticWorkloadTest {
    private static final List<Long> INSTRUMENTS = java.util.stream.LongStream.range(0, 100)
            .map(i -> 100000L + i).boxed().toList();

    @Test
    void sameManifestSeedProfileAndClockAreReproducible() {
        var a = new SyntheticWorkload(new SyntheticWorkload.Config(
                INSTRUMENTS, 7L, SyntheticWorkload.Profile.BASELINE, 1_700_000_000_000L));
        var b = new SyntheticWorkload(new SyntheticWorkload.Config(
                INSTRUMENTS, 7L, SyntheticWorkload.Profile.BASELINE, 1_700_000_000_000L));
        assertEquals(a.sample(500), b.sample(500));
    }

    @Test
    void baselineIsVariableAndPeakNeverExceedsTwentyPerInstrument() {
        var baseline = new SyntheticWorkload(new SyntheticWorkload.Config(
                INSTRUMENTS, 9L, SyntheticWorkload.Profile.BASELINE, 0L));
        var baselineTicks = baseline.sample(20_000);
        long distinctTimes = baselineTicks.stream().map(SyntheticWorkload.Tick::eventTimeMs).distinct().count();
        assertTrue(distinctTimes > 100, "baseline must not use a fixed universal interval");

        var peak = new SyntheticWorkload(new SyntheticWorkload.Config(
                java.util.stream.LongStream.range(0, 3000).map(i -> 200000L + i).boxed().toList(),
                11L, SyntheticWorkload.Profile.PEAK, 0L));
        Map<Long, List<SyntheticWorkload.Tick>> byInstrument = peak.sample(90_000).stream()
                .collect(Collectors.groupingBy(SyntheticWorkload.Tick::instrumentToken));
        byInstrument.values().forEach(ticks -> {
            long first = ticks.stream().mapToLong(SyntheticWorkload.Tick::eventTimeMs).min().orElse(0L);
            long last = ticks.stream().mapToLong(SyntheticWorkload.Tick::eventTimeMs).max().orElse(first);
            if (last > first) assertTrue((ticks.size() - 1) * 1000L <= (last - first) * 20L);
        });
    }

    @Test
    void invalidWorkloadConfigurationIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new SyntheticWorkload(
                new SyntheticWorkload.Config(List.of(), 1L, SyntheticWorkload.Profile.BASELINE, 0L)));
        assertThrows(IllegalArgumentException.class, () -> new SyntheticWorkload.Config(
                INSTRUMENTS, 1L, null, 0L));
    }
}
