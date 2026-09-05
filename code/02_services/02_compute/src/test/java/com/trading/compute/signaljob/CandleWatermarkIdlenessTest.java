package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.clock.ManualClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * SIG-HARNESS-001 — the IDLENESS half (the out-of-order/watermark half is
 * {@code CandleWatermarkStrategyTest}). Production config uses
 * {@code CandleWatermarkStrategy.of(config)} which wraps the bounded
 * out-of-orderness generator in {@code WatermarkStrategy.withIdleness(SOURCE_IDLE_MS)}:
 * a source split that goes quiet for the idle timeout is marked {@code idle},
 * so downstream watermarks keep advancing (windows on idle instruments still
 * close) instead of stalling at the last watermark.
 *
 * <p>Drives the SAME {@code WatermarksWithIdleness} wrapper the strategy
 * produces ({@code CandleWatermarkStrategy.of} returns
 * {@code forGenerator(...).withIdleness(...)}), with an injectable
 * {@link ManualClock} as the input-activity clock:
 * <ul>
 *   <li>events flowing → the split stays active and emits the bounded
 *       watermarks;</li>
 *   <li>no events for {@code SOURCE_IDLE_MS} → {@code onPeriodicEmit} marks
 *       the split {@code idle} (downstream watermarks unblock);</li>
 *   <li>a new event → {@code markActive()} and watermark emission resumes.</li>
 * </ul>
 */
@DisplayName("SIG-HARNESS-001 (idleness half): withIdleness marks an idle split idle and reactivates on arrival")
class CandleWatermarkIdlenessTest {

    private static final long T0 = 1_750_000_000_000L;

    @Test
    @DisplayName("idle split is marked idle after SOURCE_IDLE_MS and reactivates on a new event")
    void idleSplitMarksIdleAndReactivates() throws Exception {
        Map<String, String> env = env();
        long idleMs = Long.parseLong(env.get("SOURCE_IDLE_MS"));
        ManualClock clock = new ManualClock(T0);
        WatermarkGenerator<RowData> generator =
                strategy(env, clock).createWatermarkGenerator(context(clock));
        RecordingOutput out = new RecordingOutput();

        // Active: an event flows; bounded watermark emitted (maxTs - 5000).
        // (No markActive on the FIRST event — the split is active by default;
        // markActive is only emitted when leaving an idle episode.)
        generator.onEvent(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1), T0, out);
        assertEquals(0, out.idleCalls, "a flowing split is not idle");
        long watermarkAfterEvent = out.lastWatermark();
        assertTrue(watermarkAfterEvent <= T0, "bounded watermark follows the event");

        // Periodic emits keep flowing (auto-watermark-interval cadence) while
        // NO events arrive. The idleness timer arms on the first post-event
        // periodic emit (records the activity counter), then starts its
        // inactivity window on the next, and marks idle once the clock has
        // passed SOURCE_IDLE_MS since the last event.
        generator.onPeriodicEmit(out); // records the activity counter
        clock.advanceTime(200L, TimeUnit.MILLISECONDS);
        generator.onPeriodicEmit(out); // starts the inactivity window
        clock.advanceTime(idleMs + 1L, TimeUnit.MILLISECONDS);
        out.reset();
        generator.onPeriodicEmit(out);
        assertTrue(out.idleCalls > 0, "idle split must be marked idle past SOURCE_IDLE_MS");
        assertEquals(0, out.activeCalls, "no markActive while still idle");

        // A new event ends the idle episode: the idleness timer's activity
        // counter advances, so the next periodic emit is no longer idle — the
        // split is active again (WatermarksWithIdleness only emits markIdle;
        // markActive is the source-output layer's transition, driven by the
        // same activity counter).
        generator.onEvent(TestRawRows.row(1L, T0 + 1_000L, "fp-2", "TRADE", 101, 2), T0 + 1_000L, out);
        out.reset();
        generator.onPeriodicEmit(out);
        assertEquals(0, out.idleCalls,
                "a new event must clear the idle state (no markIdle on the next periodic emit)");
    }

    @Test
    @DisplayName("a split with constant traffic never goes idle")
    void activeSplitNeverIdles() throws Exception {
        Map<String, String> env = env();
        long idleMs = Long.parseLong(env.get("SOURCE_IDLE_MS"));
        ManualClock clock = new ManualClock(T0);
        WatermarkGenerator<RowData> generator =
                strategy(env, clock).createWatermarkGenerator(context(clock));
        RecordingOutput out = new RecordingOutput();

        generator.onEvent(TestRawRows.row(1L, T0, "fp-1", "TRADE", 100, 1), T0, out);
        // Traffic arrives faster than the idle timeout.
        clock.advanceTime(idleMs / 2L, TimeUnit.MILLISECONDS);
        generator.onEvent(TestRawRows.row(1L, T0 + 1L, "fp-2", "TRADE", 101, 2), T0 + 1L, out);
        generator.onPeriodicEmit(out);
        clock.advanceTime(idleMs / 2L, TimeUnit.MILLISECONDS);
        generator.onEvent(TestRawRows.row(1L, T0 + 2L, "fp-3", "TRADE", 102, 3), T0 + 2L, out);

        out.reset();
        generator.onPeriodicEmit(out);
        assertEquals(0, out.idleCalls,
                "a split with traffic inside the idle window must not be marked idle");
    }

    /** The exact config the job runs: SOURCE_IDLE_MS=15000 drives the idleness. */
    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_TTL_MS", "60000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("SOURCE_IDLE_MS", "15000");
        env.put("WATERMARK_OUT_OF_ORDER_MS", "5000");
        env.put("ALLOWED_LATENESS_MS", "5000");
        return env;
    }

    /** {@code CandleWatermarkStrategy.of(config)} — the withIdleness wrapper included. */
    private static WatermarkStrategy<RowData> strategy(Map<String, String> env, ManualClock clock) {
        return CandleWatermarkStrategy.of(SignalJobConfig.from(env));
    }

    /** A {@code WatermarkGeneratorSupplier.Context} backed by the manual clock. */
    private static WatermarkGeneratorSupplier.Context context(ManualClock clock) {
        return new WatermarkGeneratorSupplier.Context() {
            @Override
            public MetricGroup getMetricGroup() {
                return UnregisteredMetricsGroup.createOperatorMetricGroup();
            }

            @Override
            public org.apache.flink.util.clock.RelativeClock getInputActivityClock() {
                return clock;
            }
        };
    }

    private static final class RecordingOutput implements WatermarkOutput {
        private final java.util.List<Long> timestamps = new java.util.ArrayList<>();
        private int idleCalls;
        private int activeCalls;
        private long lastWatermark = Long.MIN_VALUE;

        void reset() {
            timestamps.clear();
            idleCalls = 0;
            activeCalls = 0;
            lastWatermark = Long.MIN_VALUE;
        }

        long lastWatermark() {
            return lastWatermark;
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            timestamps.add(watermark.getTimestamp());
            lastWatermark = watermark.getTimestamp();
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
