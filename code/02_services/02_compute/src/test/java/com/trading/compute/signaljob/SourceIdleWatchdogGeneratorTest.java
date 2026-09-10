package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * SourceIdleWatchdogGenerator behavior with an injectable clock (tracker 14
 * P7/P10 — 2026-08-13 misdiagnosis lesson; wall-clock idle marking added
 * 2026-09-01 CHG-120): the watchdog marks the split idle after
 * SOURCE_IDLE_MS of WALL-CLOCK silence (the withIdleness pausable clock is
 * frozen by backpressure under load — the CHG-120 zero-emission root cause),
 * reactivates it only on the first ADVANCING record (P2-170 — a stale first
 * record stays excluded so the combined minimum never regresses), never alters
 * the DELEGATE's watermark emission (pure pass-through), and logs ONE alert per
 * idle EPISODE against the job-wide last-record clock (P2-055 — quiet splits
 * never false-alarm while records flow anywhere).
 *
 * <p>CHG-023 item 1 (2026-08-17): the client-side {@code compute.source.idle.
 * at.tail} DELTA mirror was removed with ComputeOtlpEmitter — the episode
 * latch + WARN/INFO logs remain the observable contract, and the native idle
 * signal moved to the source operator's {@code numRecordsOutPerSecond} meter
 * and {@code currentOutputWatermark} gauge. Tests assert the latch (one alert
 * per episode) instead of the drained delta.
 */
class SourceIdleWatchdogGeneratorTest {

    private final AtomicLong clock = new AtomicLong(1_000_000L);

    private WatermarkGenerator<RowData> generator;

    @BeforeEach
    void setUp() {
        SourceIdleWatchdogGenerator.resetEpisodeForTest();
        generator =
                new SourceIdleWatchdogGenerator(
                        CandleWatermarkStrategy.boundedOutOfOrderGenerator(5_000L),
                        15_000L,
                        60_000L,
                        clock::get);
    }

    @AfterEach
    void tearDown() {
        SourceIdleWatchdogGenerator.resetEpisodeForTest();
    }

    @Test
    void noAlertWhileRecordsFlow() {
        RecordingOutput output = new RecordingOutput();
        generator.onEvent(null, 10_000L, output);
        clock.set(clock.get() + 10_000L); // 10 s gap — well under 60 s
        generator.onEvent(null, 11_000L, output);
        generator.onPeriodicEmit(output);
        generator.onPeriodicEmit(output);

        assertFalse(SourceIdleWatchdogGenerator.episodeReportedForTest(),
                "no idle episode while records flow");
        // Watermark emission unchanged: bounded-out-of-orderness still emits
        // 4_999 then 5_999 (event-driven, tracker-14 design).
        assertEquals(java.util.List.of(4_999L, 5_999L), output.timestamps);
    }

    @Test
    void alertsOnceAfterThresholdWhenIdle() {
        generator.onEvent(null, 10_000L, new RecordingOutput());
        clock.set(clock.get() + 61_000L); // cross the 60 s threshold

        generator.onPeriodicEmit(new RecordingOutput());

        assertTrue(SourceIdleWatchdogGenerator.episodeReportedForTest(),
                "episode latch must be set after the first alert");
    }

    @Test
    void noRepeatWhileStillIdle() {
        generator.onEvent(null, 10_000L, new RecordingOutput());
        clock.set(clock.get() + 61_000L);
        generator.onPeriodicEmit(new RecordingOutput());
        assertTrue(SourceIdleWatchdogGenerator.episodeReportedForTest());

        // Keep idling — later periodic ticks must NOT re-alert (one per episode).
        clock.set(clock.get() + 61_000L);
        generator.onPeriodicEmit(new RecordingOutput());
        generator.onPeriodicEmit(new RecordingOutput());
        assertTrue(SourceIdleWatchdogGenerator.episodeReportedForTest(),
                "no repeat alerts inside one idle episode");
    }

    @Test
    void recordArrivalResumesAndRearmsNextEpisode() {
        generator.onEvent(null, 10_000L, new RecordingOutput());
        clock.set(clock.get() + 61_000L);
        generator.onPeriodicEmit(new RecordingOutput());
        assertTrue(SourceIdleWatchdogGenerator.episodeReportedForTest());

        // A record arrives: episode ends, latch re-arms.
        clock.set(clock.get() + 5_000L);
        generator.onEvent(null, 12_000L, new RecordingOutput());
        assertFalse(SourceIdleWatchdogGenerator.episodeReportedForTest(),
                "a record must clear the episode latch (resume)");

        // New idle episode after the resume must alert again.
        clock.set(clock.get() + 61_000L);
        generator.onPeriodicEmit(new RecordingOutput());
        assertTrue(SourceIdleWatchdogGenerator.episodeReportedForTest(),
                "a fresh idle episode after resume must alert again");
    }

    @Test
    void belowThresholdDoesNotAlert() {
        generator.onEvent(null, 10_000L, new RecordingOutput());
        clock.set(clock.get() + 59_999L); // 1 ms under the 60 s threshold
        generator.onPeriodicEmit(new RecordingOutput());
        assertFalse(SourceIdleWatchdogGenerator.episodeReportedForTest());
    }

    @Test
    void startIdleFromSourceOpenCountsTowardThreshold() {
        // A restored source at a frozen tail has NO first record: idle time
        // starts at generator creation (source open), so a frozen-tail restore
        // alerts even though onEvent never ran.
        clock.set(clock.get() + 61_000L);
        generator.onPeriodicEmit(new RecordingOutput());
        assertTrue(SourceIdleWatchdogGenerator.episodeReportedForTest(),
                "idle measured from source-open (no records ever) must alert");
    }

    @Test
    void watermarksStillFlowWhileIdle() {
        // The watchdog must never block watermark emission even when idle.
        RecordingOutput output = new RecordingOutput();
        generator.onEvent(null, 10_000L, output);
        clock.set(clock.get() + 61_000L);
        generator.onPeriodicEmit(output);
        // The periodic emit still forwards the delegate's watermark.
        assertFalse(output.timestamps.isEmpty(),
                "periodic emits must still reach the delegate while idle");
    }

    // ------------------------------------------------------------------
    // Wall-clock idle marking (CHG-120): a split silent for SOURCE_IDLE_MS
    // of WALL time is marked idle even when the withIdleness pausable clock
    // would be frozen by backpressure — the mechanism that pinned the
    // combined watermark at Long.MIN_VALUE behind the empty future-day
    // partition splits under drill load.
    // ------------------------------------------------------------------

    @Test
    void marksSplitIdleAfterWallClockSilence() {
        RecordingOutput output = new RecordingOutput();
        generator.onEvent(null, 10_000L, output);
        clock.set(clock.get() + 15_000L); // exactly SOURCE_IDLE_MS

        generator.onPeriodicEmit(output);

        assertEquals(1, output.idleCalls,
                "wall-clock silence >= SOURCE_IDLE_MS must mark the split idle exactly once");
    }

    @Test
    void noIdleMarkingWhileRecordsFlow() {
        RecordingOutput output = new RecordingOutput();
        generator.onEvent(null, 10_000L, output);
        clock.set(clock.get() + 14_999L); // 1 ms under the threshold
        generator.onPeriodicEmit(output);
        assertEquals(0, output.idleCalls, "silence below SOURCE_IDLE_MS must not mark idle");

        // Regular traffic resets the wall clock: never idle.
        generator.onEvent(null, 11_000L, output);
        clock.set(clock.get() + 5_000L);
        generator.onPeriodicEmit(output);
        assertEquals(0, output.idleCalls, "flowing splits must never be marked idle");
    }

    @Test
    void idleMarkingIsEdgeTriggeredPerEpisode() {
        RecordingOutput output = new RecordingOutput();
        generator.onEvent(null, 10_000L, output);
        clock.set(clock.get() + 15_000L);
        generator.onPeriodicEmit(output);

        // Still silent — later periodic ticks must NOT re-mark (mirrors
        // WatermarksWithIdleness.isIdleNow edge semantics; PartialWatermark
        // would forward every repeated onIdleUpdate to the listener).
        clock.set(clock.get() + 60_000L);
        generator.onPeriodicEmit(output);
        generator.onPeriodicEmit(output);
        assertEquals(1, output.idleCalls, "one markIdle per idle episode, not per tick");
    }

    @Test
    void staleRecordAfterIdleMarkStaysExcluded() {
        RecordingOutput output = new RecordingOutput();
        generator.onEvent(null, 10_000L, output);
        clock.set(clock.get() + 15_000L);
        generator.onPeriodicEmit(output);
        assertEquals(1, output.idleCalls);

        // P2-170: a STALE record (older than the running max) makes the bounded
        // delegate emit NO watermark — reactivating now would re-add a split
        // whose last watermark is old and drag the combined minimum backwards.
        clock.set(clock.get() + 1_000L);
        generator.onEvent(null, 9_000L, output);
        assertEquals(0, output.activeCalls,
                "a non-advancing first record must NOT reactivate the split");

        // Still excluded: the next periodic tick must not re-mark either
        // (edge semantics) and emits no markActive.
        clock.set(clock.get() + 200L);
        generator.onPeriodicEmit(output);
        assertEquals(1, output.idleCalls, "no re-mark inside the idle episode");
        assertEquals(0, output.activeCalls, "still excluded while stale");
    }

    @Test
    void advancingRecordAfterIdleMarkReactivatesSplit() {
        RecordingOutput output = new RecordingOutput();
        generator.onEvent(null, 10_000L, output);
        clock.set(clock.get() + 15_000L);
        generator.onPeriodicEmit(output);
        assertEquals(1, output.idleCalls);

        // P2-170: only an ADVANCING record proves the split fresh and rejoins
        // it to the combined minimum (delegate emits 15_000 > 4_999).
        clock.set(clock.get() + 1_000L);
        generator.onEvent(null, 20_000L, output);
        assertEquals(1, output.activeCalls,
                "the first record after an idle-mark must reactivate the split");

        // And the next periodic emit does not re-mark idle (episode ended).
        clock.set(clock.get() + 200L);
        generator.onPeriodicEmit(output);
        assertEquals(1, output.idleCalls, "no re-mark inside the reactivated episode");
    }

    @Test
    void idleFromSourceOpenMarksIdleWithoutAnyEvent() {
        // Empty future-day partition splits NEVER see a record: idle time
        // runs from generator creation (source open) and must still mark.
        RecordingOutput output = new RecordingOutput();
        clock.set(clock.get() + 15_000L);
        generator.onPeriodicEmit(output);
        assertEquals(1, output.idleCalls,
                "a never-data split must be marked idle after SOURCE_IDLE_MS from open");
    }

    // ------------------------------------------------------------------
    // P2-055: the idle-at-tail alert runs on the job-wide last-record clock.
    // A permanently-quiet split (or the event-less main instance) must not
    // false-alarm while records flow on other splits; a fully quiet job must
    // still alert. (The cheaper !idleMarked && hadEvent guard was rejected:
    // alertMs > idleMs means idleMarked is always true at alert time, so that
    // guard would suppress every alert including the frozen-tail one.)
    // ------------------------------------------------------------------

    @Test
    void quietSplitDoesNotAlertWhileJobFlows() {
        WatermarkGenerator<RowData> live =
                new SourceIdleWatchdogGenerator(
                        CandleWatermarkStrategy.boundedOutOfOrderGenerator(5_000L),
                        15_000L,
                        60_000L,
                        clock::get);
        WatermarkGenerator<RowData> quiet =
                new SourceIdleWatchdogGenerator(
                        CandleWatermarkStrategy.boundedOutOfOrderGenerator(5_000L),
                        15_000L,
                        60_000L,
                        clock::get);
        RecordingOutput liveOut = new RecordingOutput();
        RecordingOutput quietOut = new RecordingOutput();

        // 70 s of healthy traffic on the live split; the quiet split only ticks.
        live.onEvent(null, 10_000L, liveOut);
        for (int i = 0; i < 7; i++) {
            clock.set(clock.get() + 10_000L);
            live.onEvent(null, 11_000L + i, liveOut);
            quiet.onPeriodicEmit(quietOut);
            assertFalse(SourceIdleWatchdogGenerator.episodeReportedForTest(),
                    "a quiet split must not alert while records flow anywhere (tick " + i + ")");
        }
        // Per-split idle marking still applies — only the job-wide alert is gated.
        assertEquals(1, quietOut.idleCalls, "quiet split still marks itself idle per-split");

        // The feed then stops everywhere: the same quiet split must alert.
        clock.set(clock.get() + 61_000L);
        quiet.onPeriodicEmit(quietOut);
        assertTrue(SourceIdleWatchdogGenerator.episodeReportedForTest(),
                "full-job silence must still alert from a quiet split");
    }

    // ------------------------------------------------------------------
    // P2-171: backward wall-clock steps resync instead of computing negative
    // idle gaps that silently miss both thresholds.
    // ------------------------------------------------------------------

    @Test
    void backwardClockJumpResyncsAndSkipsTick() {
        RecordingOutput output = new RecordingOutput();
        generator.onEvent(null, 10_000L, output);

        // NTP/step adjustment moves the wall clock backwards 30 s.
        clock.set(clock.get() - 30_000L);
        generator.onPeriodicEmit(output);
        assertEquals(0, output.idleCalls, "a backward step must not mark idle");
        assertFalse(SourceIdleWatchdogGenerator.episodeReportedForTest(),
                "a backward step must not alert");

        // Normal operation resumes from the resynced stamp: both thresholds
        // fire again after their full silence elapses.
        clock.set(clock.get() + 91_000L);
        generator.onPeriodicEmit(output);
        assertEquals(1, output.idleCalls, "idle marking recovers after resync");
        assertTrue(SourceIdleWatchdogGenerator.episodeReportedForTest(),
                "alerting recovers after resync");
    }

    // ------------------------------------------------------------------
    // P2-235: bad wiring/timeouts fail at construction, not as a first-tick
    // idle storm. P2-169: a fresh job re-arms the first-episode alert.
    // ------------------------------------------------------------------

    @Test
    void constructorRejectsBadTimeouts() {
        WatermarkGenerator<RowData> delegate =
                CandleWatermarkStrategy.boundedOutOfOrderGenerator(5_000L);
        assertThrows(IllegalArgumentException.class,
                () -> new SourceIdleWatchdogGenerator(delegate, 0, 60_000L, clock::get));
        assertThrows(IllegalArgumentException.class,
                () -> new SourceIdleWatchdogGenerator(delegate, -1, 60_000L, clock::get));
        assertThrows(IllegalArgumentException.class,
                () -> new SourceIdleWatchdogGenerator(delegate, 15_000L, 0, clock::get));
        assertThrows(IllegalArgumentException.class,
                () -> new SourceIdleWatchdogGenerator(delegate, 15_000L, -1, clock::get));
        assertThrows(NullPointerException.class,
                () -> new SourceIdleWatchdogGenerator(null, 15_000L, 60_000L, clock::get));
    }

    @Test
    void constructorRearmsEpisodeLatch() {
        generator.onEvent(null, 10_000L, new RecordingOutput());
        clock.set(clock.get() + 61_000L);
        generator.onPeriodicEmit(new RecordingOutput());
        assertTrue(SourceIdleWatchdogGenerator.episodeReportedForTest());

        // A fresh job (new generator, e.g. restart in a reused JVM) must report
        // its first idle episode instead of inheriting the set latch.
        new SourceIdleWatchdogGenerator(
                CandleWatermarkStrategy.boundedOutOfOrderGenerator(5_000L),
                15_000L,
                60_000L,
                clock::get);
        assertFalse(SourceIdleWatchdogGenerator.episodeReportedForTest(),
                "construction must re-arm the first-episode alert");
    }

    private static final class RecordingOutput implements WatermarkOutput {
        private final java.util.List<Long> timestamps = new java.util.ArrayList<>();
        private int idleCalls;
        private int activeCalls;

        @Override
        public void emitWatermark(Watermark watermark) {
            timestamps.add(watermark.getTimestamp());
        }

        @Override
        public void markIdle() {
            idleCalls++;
        }

        @Override
        public void markActive() {
            activeCalls++;
        }
    }
}
