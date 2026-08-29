package com.trading.compute.signaljob;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * Trigger for the candle window that fires BOTH the final emission (at window
 * end, via the window's normal event-time end) AND periodic preview emissions
 * (every {@code interval} of processing time, low-latency candles Phase 1,
 * 2026-08-29).
 *
 * <p>Semantics:
 * <ul>
 *   <li>{@code onElement} — registers the EVENT-time preview timer at
 *       {@code window.start + interval} (the first preview fires 1s of
 *       EVENT time into the window). Event-time timers fire as the watermark
 *       advances through the window — fully decoupled from wall-clock
 *       processing backlog, so previews are correct even when the operator
 *       is behind (observed live 2026-08-29: processing-time preview timers
 *       registered at {@code now + interval} fired AT window end under a
 *       replay backlog — every fire hit the emit guard, 0 previews).</li>
 *   <li>{@code onEventTime} — at each preview interval (event time), fires a
 *       PREVIEW (FIRE, not FIRE_AND_PURGE) so the window's
 *       {@code ProcessWindowFunction} runs and emits the current OHLCV as a
 *       preview row; re-registers the next 1s event-time timer. At window
 *       end, returns FIRE_AND_PURGE so the final candle is emitted.</li>
 *   <li>{@code onEventTime} — at window end, returns FIRE_AND_PURGE so the
 *       final candle is emitted (unchanged from today).</li>
 * </ul>
 *
 * <p>Why a custom trigger instead of a separate 1s window? A separate window
 * would need its OWN state/accumulator; this trigger reuses the SAME
 * {@code CandleAccumulator} already accumulated by the 15s window — zero
 * additional aggregation state, and the preview always reflects exactly the
 * accumulator that will produce the final candle.
 *
 * <p>Restart-safety: Flink checkpoints registered event-time timers, so a
 * restore re-arms the next preview timer correctly; the window's own
 * event-time end trigger is unchanged.
 */
public final class CandlePreviewTrigger extends Trigger<Object, TimeWindow> {

    private static final long serialVersionUID = 1L;

    private final long intervalMs;

    private transient Counter eventTimeFires;
    private transient Counter previewFires;

    public CandlePreviewTrigger(long intervalMs) {
        this.intervalMs = intervalMs;
    }


    /**
     * The window whose preview timer is currently armed, tracked in keyed
     * state. The register-once flag MUST be per-window, not per-key: keyed
     * state is shared across ALL windows of a key, so a Boolean flag set on
     * the first window would suppress the preview timer for every later
     * window (observed live 2026-08-29: trigger fired ~20k× per subtask from
     * the same stale first-window timer, previews emitted 0 — the first
     * window's timer re-armed forever while its event-time end had long
     * passed, and the emit guard skipped every fire).
     */
    private static final ValueStateDescriptor<Long> REGISTERED_WINDOW =
            new ValueStateDescriptor<>("preview-timer-registered-window", Long.class);

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window,
            TriggerContext ctx) throws Exception {
        if (eventTimeFires == null) {
            eventTimeFires = ctx.getMetricGroup().counter("preview.trigger.eventTimeFires");
            previewFires = ctx.getMetricGroup().counter("preview.trigger.previewFires");
        }
        // Match the default EventTimeTrigger for LATE elements (watermark past
        // window end): re-fire so CandleEmitFunction's emitted-flag no-op and
        // late-updates counter behave exactly as today.
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            return TriggerResult.FIRE;
        }
        // Register the first EVENT-time preview timer ONCE per window, at
        // window.start + interval. Event-time timers fire as the watermark
        // advances, so the preview cadence is correct even when the operator
        // processes elements late (processing-time anchoring observed live
        // to fire at window end under backlog — 0 previews).
        ValueState<Long> registeredWindow = ctx.getPartitionedState(REGISTERED_WINDOW);
        if (registeredWindow.value() == null || registeredWindow.value() != window.getStart()) {
            ctx.registerEventTimeTimer(window.getStart() + intervalMs);
            registeredWindow.update(window.getStart());
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        if (eventTimeFires != null) eventTimeFires.inc();
        if (time >= window.getEnd()) {
            // Window end: emit the final candle (FIRE_AND_PURGE). Also
            // delete the last preview timer if any.
            ctx.deleteEventTimeTimer(window.getStart() + intervalMs);
            return TriggerResult.FIRE_AND_PURGE;
        }
        // Preview interval (event time): emit the current OHLCV as a
        // preview row, then re-arm the next interval timer.
        if (previewFires != null) previewFires.inc();
        ctx.registerEventTimeTimer(time + intervalMs);
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
        // No processing-time previews: previews are driven by EVENT-time
        // timers (see class javadoc — processing-time anchoring fired at
        // window end under replay backlog). Keep the default CONTINUE.
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) {
        ctx.deleteEventTimeTimer(window.getStart() + intervalMs);
        ctx.getPartitionedState(REGISTERED_WINDOW).clear();
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) {
        // Tumbling windows don't merge; keep the default (no-op).
    }

    /** Factory — mirrors {@code ContinuousProcessingTimeTrigger.of}. */
    public static CandlePreviewTrigger of(Duration interval) {
        return new CandlePreviewTrigger(interval.toMillis());
    }
}
