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

    /** MOCK-UNIT-002: 20 ticks/s/instrument hard maximum = 50 ms between ticks. */
    private static final long MIN_TICK_INTERVAL_MS = 50L;

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
        assertMinInterArrivalMs("baseline", baselineTicks);

        var peak = new SyntheticWorkload(new SyntheticWorkload.Config(
                java.util.stream.LongStream.range(0, 3000).map(i -> 200000L + i).boxed().toList(),
                11L, SyntheticWorkload.Profile.PEAK, 0L));
        assertMinInterArrivalMs("peak", peak.sample(90_000));
    }

    /**
     * MOCK-UNIT-002: for every instrument, consecutive event times must be at
     * least {@link #MIN_TICK_INTERVAL_MS} apart — the hard 20 ticks/s maximum.
     * This subsumes the old average-rate check: an average can only stay under
     * 20/s if no single interval drops below 50 ms.
     */
    private static void assertMinInterArrivalMs(String profile, List<SyntheticWorkload.Tick> ticks) {
        Map<Long, List<SyntheticWorkload.Tick>> byInstrument = ticks.stream()
                .collect(Collectors.groupingBy(SyntheticWorkload.Tick::instrumentToken));
        for (var entry : byInstrument.entrySet()) {
            long[] times = entry.getValue().stream()
                    .mapToLong(SyntheticWorkload.Tick::eventTimeMs).sorted().toArray();
            for (int i = 1; i < times.length; i++) {
                long intervalMs = times[i] - times[i - 1];
                assertTrue(intervalMs >= MIN_TICK_INTERVAL_MS, () -> String.format(
                        "%s profile: instrument %d got consecutive ticks %d ms apart, below the %d ms floor (20 ticks/s)",
                        profile, entry.getKey(), intervalMs, MIN_TICK_INTERVAL_MS));
            }
        }
    }

    /**
     * P3-233: same-millisecond ticks must drain in instrument-index order, not
     * in whatever order the heap happens to hold equal keys. startMs staggering
     * is modulo 1000 ms, so with more instruments than slots the initial offsets
     * collide and ties are common.
     */
    @Test
    void equalEventTimesDrainInInstrumentIndexOrder() {
        int instrumentCount = 3_000; // > 1000 stagger slots: initial offsets collide
        List<Long> instruments = java.util.stream.LongStream.range(0, instrumentCount)
                .map(i -> 900000L + i).boxed().toList();
        var workload = new SyntheticWorkload(new SyntheticWorkload.Config(
                instruments, 5L, SyntheticWorkload.Profile.BASELINE, 1_700_000_000_000L));

        Map<Long, Integer> indexByToken = new java.util.HashMap<>();
        for (int i = 0; i < instruments.size(); i++) indexByToken.put(instruments.get(i), i);

        List<SyntheticWorkload.Tick> ticks = workload.sample(instrumentCount + 500);
        for (int i = 1; i < ticks.size(); i++) {
            SyntheticWorkload.Tick prev = ticks.get(i - 1);
            SyntheticWorkload.Tick cur = ticks.get(i);
            boolean ordered = prev.eventTimeMs() < cur.eventTimeMs()
                    || (prev.eventTimeMs() == cur.eventTimeMs()
                        && indexByToken.get(prev.instrumentToken()) < indexByToken.get(cur.instrumentToken()));
            int at = i;
            assertTrue(ordered, () -> String.format(
                    "P3-233: tick %d (token %d, t=%d) must precede %d (token %d, t=%d)",
                    at - 1, prev.instrumentToken(), prev.eventTimeMs(),
                    at, cur.instrumentToken(), cur.eventTimeMs()));
        }
    }

    @Test
    void invalidWorkloadConfigurationIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new SyntheticWorkload(
                new SyntheticWorkload.Config(List.of(), 1L, SyntheticWorkload.Profile.BASELINE, 0L)));
        assertThrows(IllegalArgumentException.class, () -> new SyntheticWorkload.Config(
                INSTRUMENTS, 1L, null, 0L));
    }
}
