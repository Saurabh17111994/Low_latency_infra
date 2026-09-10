package com.trading.compute.signaljob;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.data.RowData;

/**
 * Event-time strategy for the raw-tick stream (Signal dossier: bounded
 * out-of-orderness watermark on {@code event_time}, with source idleness).
 *
 * <p>{@code event_time} is the verified UTC broker timestamp in epoch
 * milliseconds (raw_table_1 DDL). {@code WATERMARK_OUT_OF_ORDER_MS} (default
 * 5000) bounds how far behind the watermark may lag the max seen event time;
 * {@code SOURCE_IDLE_MS} (default 15000) advances watermarks past Fluss
 * bucket splits that go quiet, so windows on idle instruments still close.
 */
public final class CandleWatermarkStrategy {

    private CandleWatermarkStrategy() {}

    public static WatermarkStrategy<RowData> of(SignalJobConfig config) {
        // P2-128: fail fast on a non-positive idle timeout — negative/zero
        // silently marks every split idle (or stalls windows forever).
        if (config.sourceIdleMs() <= 0) {
            throw new IllegalArgumentException(
                    "sourceIdleMs must be > 0, got " + config.sourceIdleMs());
        }
        // SourceIdleWatchdogGenerator wraps the bounded generator INSIDE the
        // source operator (FLIP-27 per-split generator): zero graph nodes
        // added, so StreamGraphHasherV2 operator IDs are bit-identical and
        // P10 archived-checkpoint restore (allowNonRestoredState=false)
        // remains safe. The watchdog also marks splits idle after
        // SOURCE_IDLE_MS of WALL-CLOCK silence: withIdleness's
        // PausableRelativeClock is frozen by backpressure under load, which
        // pinned the combined watermark at Long.MIN_VALUE behind the
        // daily-partition table's permanently-empty future-day splits
        // (CHG-120, live-reproduced 2026-09-01).
        // P2-022: the exact poison ceiling mirrors RawValidationFunction —
        // a row the gate would reject must never reach the watermark first.
        long maxSafeEventTime = Long.MAX_VALUE
                - config.candleWindowMs() - config.allowedLatenessMs() - 1;
        return WatermarkStrategy.<RowData>forGenerator(
                        context ->
                                new SourceIdleWatchdogGenerator(
                                        boundedOutOfOrderGenerator(
                                                config.outOfOrderMs(), maxSafeEventTime),
                                        config.sourceIdleMs(),
                                        config.sourceIdleAlertMs()))
                .withIdleness(Duration.ofMillis(config.sourceIdleMs()))
                .withTimestampAssigner((row, timestamp) -> {
                    // P2-022: null event_time never poisons the watermark —
                    // MIN_VALUE is ignored by the max() inside onEvent.
                    if (row == null || row.isNullAt(RawTableColumns.EVENT_TIME)) {
                        return Long.MIN_VALUE;
                    }
                    return row.getLong(RawTableColumns.EVENT_TIME);
                });
    }

    /**
     * Emits the normal bounded-out-of-orderness watermark as each new maximum event time
     * arrives. Flink's built-in generator emits only on its periodic timer; a replay can drain a
     * whole raw-log burst before that timer fires, incorrectly admitting an event that arrived
     * after a pusher in log order. Event-driven emission makes late-event handling independent of
     * replay speed. The surrounding {@link WatermarkStrategy#withIdleness(Duration)} still marks
     * inactive source splits idle.
     */
    static WatermarkGenerator<RowData> boundedOutOfOrderGenerator(long outOfOrderMs) {
        return new BoundedOutOfOrdernessOnEventGenerator(outOfOrderMs, Long.MAX_VALUE);
    }

    static WatermarkGenerator<RowData> boundedOutOfOrderGenerator(long outOfOrderMs, long maxSafeEventTime) {
        return new BoundedOutOfOrdernessOnEventGenerator(outOfOrderMs, maxSafeEventTime);
    }

    private static final class BoundedOutOfOrdernessOnEventGenerator
            implements WatermarkGenerator<RowData> {
        private final long outOfOrderMs;
        private final long maxSafeEventTime;
        private long maxTimestamp;
        private long lastEmittedWatermark = Long.MIN_VALUE;

        private BoundedOutOfOrdernessOnEventGenerator(long outOfOrderMs, long maxSafeEventTime) {
            // P2-023: a negative tolerance overflows the MIN_VALUE-seeded max
            // to near-MAX at startup — fail fast instead of emitting a
            // near-MAX watermark that drops the whole stream as late.
            if (outOfOrderMs < 0) {
                throw new IllegalArgumentException(
                        "outOfOrderMs must be >= 0, got " + outOfOrderMs);
            }
            this.outOfOrderMs = outOfOrderMs;
            this.maxSafeEventTime = maxSafeEventTime;
            this.maxTimestamp = Long.MIN_VALUE + outOfOrderMs + 1;
        }

        @Override
        public void onEvent(RowData event, long eventTimestamp, WatermarkOutput output) {
            // P2-022: ignore poison timestamps the validation gate would
            // reject — a single huge event_time must never pin maxTimestamp
            // near MAX (watermarks are monotonic, no recovery).
            if (eventTimestamp <= 0 || eventTimestamp > maxSafeEventTime) {
                return;
            }
            maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
            long nextWatermark = maxTimestamp - outOfOrderMs - 1;
            if (nextWatermark > lastEmittedWatermark) {
                output.emitWatermark(new Watermark(nextWatermark));
                lastEmittedWatermark = nextWatermark;
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // Watermarks are emitted synchronously in onEvent.
        }
    }
}
