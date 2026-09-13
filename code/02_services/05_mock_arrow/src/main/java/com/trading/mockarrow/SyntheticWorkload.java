package com.trading.mockarrow;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.SplittableRandom;

/** Deterministic, per-instrument variable-arrival workload used by benchmarks. */
public final class SyntheticWorkload {
    public enum Profile { BASELINE, PEAK }

    public record Config(List<Long> instruments, long seed, Profile profile, long startMs) {
        public Config {
            if (instruments == null || instruments.isEmpty()) {
                throw new IllegalArgumentException("instrument manifest must not be empty");
            }
            if (profile == null) throw new IllegalArgumentException("profile is required");
            if (startMs < 0) throw new IllegalArgumentException("startMs must be non-negative");
            instruments = List.copyOf(instruments);
        }
    }

    public record Tick(long instrumentToken, long eventTimeMs, long sequence) {}

    private record Due(long timeMs, int instrumentIndex) {}

    private final Config config;
    private final SplittableRandom random;
    // P3-233: timeMs alone leaves same-millisecond polls in unspecified heap
    // order, which perturbs the shared RNG assignment; instrumentIndex is the
    // deterministic secondary key.
    private final PriorityQueue<Due> due = new PriorityQueue<>(
            Comparator.comparingLong(Due::timeMs).thenComparingInt(Due::instrumentIndex));
    private long sequence;

    public SyntheticWorkload(Config config) {
        this.config = config;
        this.random = new SplittableRandom(config.seed());
        for (int i = 0; i < config.instruments().size(); i++) {
            due.add(new Due(config.startMs() + initialOffset(i), i));
        }
    }

    public Config config() { return config; }

    /** Returns the next event in deterministic event-time order. */
    public Tick next() {
        Due next = due.remove();
        long interval = nextIntervalMs();
        due.add(new Due(next.timeMs() + interval, next.instrumentIndex()));
        return new Tick(config.instruments().get(next.instrumentIndex()), next.timeMs(), sequence++);
    }

    /** Generate a bounded sample without sleeping or using wall-clock time. */
    public List<Tick> sample(int count) {
        if (count < 0) throw new IllegalArgumentException("count must be non-negative");
        List<Tick> ticks = new ArrayList<>(count);
        for (int i = 0; i < count; i++) ticks.add(next());
        return ticks;
    }

    private long initialOffset(int instrumentIndex) {
        // Stagger instruments so the first measurement window is not a burst.
        return Math.floorMod((long) instrumentIndex * 31L + config.seed(), 1000L);
    }

    private long nextIntervalMs() {
        if (config.profile() == Profile.PEAK) {
            // 51..54 ms worst case gives <=19.6 ticks/s per instrument
            // (MOCK-UNIT-002: no instrument exceeds 20/s) and a variable
            // stream. ~52.5 ms mean = the real broker per-instrument peak
            // (1 tick/50 ms) with the same ~2% worst-case margin the 34 ms
            // mean gave the old 30/s cap.
            return 51L + random.nextLong(4L);
        }
        // 55..65 ms has a 60 ms mean: ≈16.7 ticks/s/instrument baseline
        // average, and 55 ms worst case = 18.2 ticks/s, so no instrument can
        // exceed the hard 20 ticks/s maximum (MOCK-UNIT-002; requirement:
        // docs/02_requirements/02-functional/01-ingestion.md).
        return 55L + random.nextLong(11L);
    }
}
