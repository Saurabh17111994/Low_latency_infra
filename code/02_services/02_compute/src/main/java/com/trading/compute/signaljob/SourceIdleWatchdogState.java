package com.trading.compute.signaljob;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-subtask idle-watchdog state (L3-3): the job-level episode latch and the job-wide
 * last-record wall clock, shared by every {@link SourceIdleWatchdogGenerator} created on
 * one subtask.
 *
 * <p>Why it exists: the latch and the job clock used to be static fields reset in the
 * generator constructor. Flink can create more than one generator per subtask at runtime
 * (split reassignment / rebuilt reader), and each construction re-armed the latch and
 * restamped the job clock — masking an idle episode that had already been reported and
 * making the next alert wait a full threshold. The state now lives in this object, held by
 * a {@link Supplier} the watermark strategy creates once per subtask; the first generator
 * stamps the job clock and every later one inherits it.
 *
 * <p>Scope: one instance per subtask (one supplier per deserialized strategy). A
 * distributed run therefore reports per TaskManager, exactly like the previous static
 * fields' documented JVM-wide limitation — but no longer resets on construction.
 */
final class SourceIdleWatchdogState {

    /** True while an idle episode has been reported; cleared by the next record. */
    private final AtomicBoolean episodeReported = new AtomicBoolean(false);

    /** Job-wide last-record wall clock (P2-055): the alert runs on this clock. */
    private final AtomicLong jobLastEventWallClockMs = new AtomicLong();

    /** True once the first generator of the subtask stamped the job clock. */
    private final AtomicBoolean stamped = new AtomicBoolean(false);

    /**
     * Stamps the job clock exactly once per subtask: a restored source at a frozen tail
     * has no first record, so the clock starts at the first generator's creation. Later
     * constructions (split reassignment) must not restamp it — that would hide the idle
     * time already elapsed.
     */
    void stampJobClockOnce(long nowMs) {
        if (stamped.compareAndSet(false, true)) {
            jobLastEventWallClockMs.set(nowMs);
        }
    }

    /** Every record anywhere refreshes the job-wide clock (P2-055). */
    void recordEvent(long nowMs) {
        jobLastEventWallClockMs.set(nowMs);
    }

    long jobLastEventWallClockMs() {
        return jobLastEventWallClockMs.get();
    }

    /** True when an episode was reported; clears it — a record resumed the feed. */
    boolean clearEpisodeIfReported() {
        return episodeReported.getAndSet(false);
    }

    /** True for exactly one caller while the job stays idle (one alert per episode). */
    boolean reportEpisodeOnce() {
        return episodeReported.compareAndSet(false, true);
    }

    boolean episodeReportedForTest() {
        return episodeReported.get();
    }

    /**
     * Serializable supplier: the strategy ships one supplier per subtask, so every
     * generator created on that subtask shares one state. The field is transient — a
     * deserialized subtask starts with a clean state, which is what a fresh job needs.
     */
    static final class Supplier
            implements java.util.function.Supplier<SourceIdleWatchdogState>, Serializable {

        private static final long serialVersionUID = 1L;

        private transient SourceIdleWatchdogState state;

        @Override
        public SourceIdleWatchdogState get() {
            if (state == null) {
                state = new SourceIdleWatchdogState();
            }
            return state;
        }
    }
}
