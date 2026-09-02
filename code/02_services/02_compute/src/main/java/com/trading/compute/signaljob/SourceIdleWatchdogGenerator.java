package com.trading.compute.signaljob;

import java.util.function.LongSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wall-clock idle marking + source idle-at-tail watchdog (tracker 14 P7/P10
 * 2026-08-13; wall-clock idle marking added 2026-09-01, CHG-120).
 *
 * <p>A restored Signal job that resumes from a checkpoint at a <em>frozen
 * feed tail</em> consumes ZERO records for as long as the feed is stopped.
 * That is the CORRECT idle-tail state — not a stall (probe-verified
 * 2026-08-13: the "restore stall" was a 3 s time-boxed probe artifact, and
 * zero consumption at the frozen end is exactly what a subscribe-at-end probe
 * shows). This decorator makes that silence observable: when no record has
 * been consumed for {@code SOURCE_IDLE_ALERT_MS} (default 60000), it logs a
 * WARN naming the condition as expected idle-tail behavior and ships a
 * {@code compute.source.idle.at.tail} DELTA metric so OpenObserve can alert —
 * instead of the silence being misread as a hang.
 *
 * <p><b>Graph-safe by construction.</b> This is NOT a new stream operator: it
 * decorates the watermark generator that FLIP-27 runs INSIDE the source
 * operator (one generator per bucket split, created by the
 * {@link CandleWatermarkStrategy} supplier). Adding a mid-stream operator
 * would shift every downstream StreamGraphHasherV2 operator hash and break
 * {@code allowNonRestoredState=false} restore of the P10 archived checkpoints
 * (the KV sink comment documents the same discipline: added LAST to the
 * candles stream on purpose). A watermark-generator decorator adds zero graph
 * nodes, so operator IDs are bit-identical and archived-checkpoint restore is
 * unaffected.
 *
 * <p><b>Why {@code onPeriodicEmit} works as the tick.</b> Flink drives
 * {@code WatermarkGenerator.onPeriodicEmit} on a fixed-delay processing-time
 * timer (auto-watermark-interval, default 200 ms) INDEPENDENT of data flow —
 * verified in flink-runtime 2.2.1 {@code ProgressiveTimestampsAndWatermarks}:
 * {@code startPeriodicWatermarkEmits} schedules the timer and
 * {@code SourceOutputWithWatermarks.emitPeriodicWatermark} invokes
 * {@code onPeriodicEmit} even when the source produces nothing. So a frozen
 * tail still ticks the watchdog; {@code onEvent} only stamps the last-record
 * wall clock.
 *
 * <p><b>Episode semantics.</b> One WARN + one metric delta per idle EPISODE
 * job-wide, not per periodic tick and not per split: the first generator
 * (any bucket) to cross the threshold reports once via a static episode latch;
 * the latch clears when ANY record arrives (any split), so a resumed feed
 * logs INFO and the next idle episode reports again. The latch is a JVM-wide
 * static — exact for the embedded dev run (single process, parallelism 1),
 * the documented scope of the other ComputeOtlpEmitter statics.
 *
 * <p><b>Wall-clock idle marking (CHG-120 zero-emission fix).</b> The
 * {@code withIdleness(SOURCE_IDLE_MS)} wrapper measures split silence with a
 * per-split {@code PausableRelativeClock} that Flink 2.2 PAUSES while the
 * task is backpressured (ProgressiveTimestampsAndWatermarks registers it via
 * {@code taskIOMetricGroup.registerBackPressureListener}). Under drill-level
 * load the clock is effectively frozen, so a split that never receives
 * records — the daily-partition table always carries empty future-day (and
 * the randomly back-created yesterday) partitions — is NEVER marked idle;
 * {@code WatermarkOutputMultiplexer.updateCombinedWatermark} then pins the
 * operator watermark at min-over-active-splits = {@code Long.MIN_VALUE} and
 * no window ever fires (live-reproduced 2026-09-01, job e4b4c19f: zero
 * candles at bp~850-999ms/s; idle engaged within ~100s once backpressure
 * eased and 4096 candles fired immediately). This generator therefore marks
 * the split idle itself after {@code sourceIdleMs} of <em>wall-clock</em>
 * silence, measured on the same injectable clock as the alert. Calling
 * {@code output.markIdle()} from {@code onPeriodicEmit} sets the split's
 * {@code PartialWatermark} idle flag; the multiplexer excludes idle splits
 * from the combined minimum at the same periodic tick, so the operator
 * watermark advances past the empty splits. The first record afterwards
 * reactivates the split: the bounded delegate emits an advancing watermark
 * ({@code PartialWatermark.setWatermark} clears the idle flag by itself),
 * and {@code markActive()} on the event covers a non-advancing (stale)
 * first record. Wall-clock marking can in principle mark a backpressured
 * split with buffered records idle, letting the watermark run ahead of it —
 * the trade-off is deliberate: split silence of the full idle timeout is
 * already the production definition of an idle split, the late-drop
 * counters (G7c) monitor the cost, and the alternative (a frozen idle clock
 * under load) is a total pipeline stall. Edge-triggered per split episode,
 * mirroring {@code WatermarksWithIdleness}'s own {@code isIdleNow} flag.
 *
 * <p><b>Main (vacant) generator position.</b> ProgressiveTimestampsAndWatermarks
 * instantiates this generator twice: once per split (split-local outputs,
 * records flow here) and once for the main output. The Fluss reader emits
 * through split-local outputs, so the main instance sees no events and marks
 * its output idle after the timeout — which is exactly why the Flink
 * {@code IdlenessManager} guards the underlying operator output: it goes
 * idle only when the main AND the split-local (per-split multiplexer
 * combined) output are both idle, i.e. only when every split is quiet. The
 * main instance's wall-clock marking is therefore harmless under live data
 * and correct (operator-level idle) when the feed is fully stopped.
 *
 * <p>Healthy-path behavior otherwise is a pure pass-through: {@code onEvent}
 * delegates the bounded-out-of-orderness watermark emission unchanged; the
 * wrapper adds only a wall-clock stamp. No offsets are touched, no connector
 * patch, no defensive clamp.
 */
final class SourceIdleWatchdogGenerator implements WatermarkGenerator<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(SourceIdleWatchdogGenerator.class);

    /**
     * Job-level episode latch: true while an idle episode has been reported.
     * Cleared by the first {@code onEvent} from any split (any generator
     * instance), so a resumed feed re-arms the next episode. JVM-wide static:
     * exact for the single-process embedded dev run (parallelism 1); a
     * distributed run would report per TaskManager — documented limitation,
     * same as the emitter statics.
     */
    private static final java.util.concurrent.atomic.AtomicBoolean EPISODE_REPORTED =
            new java.util.concurrent.atomic.AtomicBoolean(false);

    private final WatermarkGenerator<RowData> delegate;
    /** Wall-clock silence after which the split is marked idle (SOURCE_IDLE_MS). */
    private final long sourceIdleMs;
    private final long sourceIdleAlertMs;
    private final LongSupplier clock;
    private long lastEventWallClockMs;
    /** Per-split idle-marking edge state: true while this split is wall-clock marked idle. */
    private boolean idleMarked;

    SourceIdleWatchdogGenerator(
            WatermarkGenerator<RowData> delegate, long sourceIdleMs, long sourceIdleAlertMs) {
        this(delegate, sourceIdleMs, sourceIdleAlertMs, System::currentTimeMillis);
    }

    /** Clock-injectable constructor (tests); the wall clock drives idle marking + alerting. */
    SourceIdleWatchdogGenerator(
            WatermarkGenerator<RowData> delegate,
            long sourceIdleMs,
            long sourceIdleAlertMs,
            LongSupplier clock) {
        this.delegate = delegate;
        this.sourceIdleMs = sourceIdleMs;
        this.sourceIdleAlertMs = sourceIdleAlertMs;
        this.clock = clock;
        // Start the idle clock at construction: a restored source at a frozen
        // tail has no events, so the first periodic tick after the threshold
        // correctly measures "idle since source start".
        this.lastEventWallClockMs = clock.getAsLong();
    }

    @Override
    public void onEvent(RowData event, long eventTimestamp, WatermarkOutput output) {
        // REQ-FC-010 source throughput: covered natively by the FLIP-27 source
        // operator's numRecordsOut / numRecordsOutPerSecond metrics (CHG-023
        // item 1 removed the client-side ComputeOtlpEmitter mirror).
        delegate.onEvent(event, eventTimestamp, output);
        if (idleMarked) {
            // The delegate just emitted an ADVANCING watermark (which clears
            // the split's idle flag inside PartialWatermark.setWatermark) in
            // the common case; markActive covers a non-advancing first record
            // after idle so the split is never left excluded from the
            // combined minimum while it is again delivering data.
            idleMarked = false;
            output.markActive();
            LOG.info("signal-job: source split reactivated after wall-clock idle — records "
                    + "flowing again");
        }
        long now = clock.getAsLong();
        if (EPISODE_REPORTED.getAndSet(false)) {
            LOG.info("signal-job: source resumed after {} ms idle at the tail — records flowing "
                    + "again (feed resumed or restore caught up)", now - lastEventWallClockMs);
        }
        lastEventWallClockMs = now;
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // REQ-FC-010 watermark lag: covered natively by the source operator's
        // currentOutputWatermark gauge (lag = now - watermark at scrape time)
        // — CHG-023 item 1 removed the client-side ComputeOtlpEmitter gauge.
        delegate.onPeriodicEmit(output);
        long now = clock.getAsLong();
        long idleMs = now - lastEventWallClockMs;
        // Wall-clock idle marking (edge-triggered, once per idle episode):
        // unblocks the combined watermark when withIdleness's PausableRelativeClock
        // is frozen by backpressure. Idempotent with the wrapper's own
        // markIdle — both target the same PartialWatermark.
        if (idleMs >= sourceIdleMs && !idleMarked) {
            idleMarked = true;
            output.markIdle();
            LOG.warn("signal-job: marking source split idle after {} ms of wall-clock silence "
                    + "(SOURCE_IDLE_MS={} ms) — withIdleness's pausable clock is frozen by "
                    + "backpressure, so the watchdog marks the split on the wall clock to keep "
                    + "the combined watermark advancing (CHG-120)", idleMs, sourceIdleMs);
        }
        // Edge-triggered: report once per idle episode (compareAndSet wins for
        // exactly one generator even if several splits cross together).
        if (idleMs >= sourceIdleAlertMs && EPISODE_REPORTED.compareAndSet(false, true)) {
            LOG.warn("signal-job: source idle at the tail — no records consumed for {} ms "
                    + "(>= SOURCE_IDLE_ALERT_MS={} ms). This is EXPECTED idle-tail behavior when "
                    + "the feed is stopped or a restored source sits at a frozen log end (NOT a "
                    + "stall; probe-verified 2026-08-13 — verify with a raw-scanner probe "
                    + "subscribing at the tail). Investigate only if the feed should be live.",
                    idleMs, sourceIdleAlertMs);
            // The old compute.source.idle.at.tail DELTA metric was removed with
            // the emitter (CHG-023 item 1): the native idle signal is the source
            // operator's numRecordsOutPerSecond meter dropping to 0 + the
            // currentOutputWatermark gauge freezing (O2 alert retargets to
            // those native series). The WARN/INFO episode logs remain the
            // primary operator-facing signal.
        }
    }

    /** TEST-ONLY: resets the job-wide episode latch (keeps tests independent). */
    static void resetEpisodeForTest() {
        EPISODE_REPORTED.set(false);
    }

    /** TEST-ONLY: current episode-latch state (package-visible for tests). */
    static boolean episodeReportedForTest() {
        return EPISODE_REPORTED.get();
    }
}
