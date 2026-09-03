package com.trading.compute.signaljob;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * Live candle preview (low-latency candles Phase 1): heap accumulators
 * (chain-heap redesign, 2026-09-03 plan
 * {@code docs/plans/2026-09-03-candle-chain-heap-plan.md} OP5).
 *
 * <p>Replaces the second {@code keyBy → window → CandlePreviewTrigger →
 * aggregate → CandlePreviewEmitFunction} branch — a full second window
 * operator duplicating every tick's accumulator read-modify-write (42.9%
 * busy at 4.9 k/s) — with per-key in-heap accumulators plus one event-time
 * timer per preview cadence step. Cadence matches the old trigger exactly:
 * first preview at {@code windowStart + previewIntervalMs} of EVENT time,
 * every {@code previewIntervalMs} after, plus a final preview at
 * {@code windowEnd}; row building reuses
 * {@link CandlePreviewEmitFunction#buildRow} (same 15 columns, same
 * {@code IS_PREVIEW=true}, same {@code LAST_EVENT_TS} rule). Previews stay
 * idempotent KV upserts that never feed signal detection — that contract is
 * untouched.
 *
 * <p>One deliberate improvement: the old branch re-created a purged window
 * for a late tick and emitted garbage partial previews over the already
 * final row; the heap version drops late ticks and counts
 * {@code compute.candles.previews.late.dropped} instead.
 *
 * <p><b>Intentional amnesia:</b> accumulators are plain fields, NOT Flink
 * managed state. A restore restarts empty; restored cadence timers with no
 * accumulator no-op loudly
 * ({@code compute.candles.previews.restored_timer_noop}). Run under uid
 * {@code candle-preview-15s-v2} so pre-redesign checkpoints fail closed
 * (G-CHAIN-3).
 *
 * <p><b>Fail-fast guards:</b> G-CHAIN-1 — a window's accumulator lives only
 * from its first tick to its end-timer (structurally bounded, normally ≤2
 * live per key); G-CHAIN-2 — live-window and global key caps fail closed.
 */
public class HeapPreviewFunction extends KeyedProcessFunction<Long, RowData, RowData> {

    private static final long serialVersionUID = 2L;

    /** Headroom over the instrument universe for the global key cap (G-CHAIN-2). */
    static final int GLOBAL_KEY_CAP = 65_536;

    /** Bound on live preview windows per key (G-CHAIN-2): steady state ≤2. */
    static final int MAX_LIVE_WINDOWS = 8;

    /** Per-key preview slot: live window accumulators (start → acc). */
    static final class Slot implements Serializable {
        private static final long serialVersionUID = 1L;
        final LinkedHashMap<Long, CandleAccumulator> accs = new LinkedHashMap<>();
    }

    private final SignalJobConfig config;
    private final CandleAggregateFunction aggregate;

    /** Per-key preview accumulators. Plain fields: intentionally NOT checkpointed. */
    private final Map<Long, Slot> slots = new HashMap<>();

    private transient Counter previewCounter;
    private transient Counter lateDroppedCounter;
    private transient Counter restoredTimerNoopCounter;
    /** Step-2 latency probe: age of the newest tick in each preview row. */
    private transient org.apache.flink.metrics.Histogram outputLatency;

    public HeapPreviewFunction(SignalJobConfig config) {
        this.config = Preconditions.checkNotNull(config);
        this.aggregate = new CandleAggregateFunction();
    }

    @Override
    public void open(OpenContext openContext) {
        // Fail fast on a cadence that can never fire inside its window: an
        // interval past the window end would leave accumulators orphaned
        // until the live-window cap trips mid-job. Equality is fine (one
        // final preview at the window end, same as the old end fire).
        Preconditions.checkState(
                config.previewIntervalMs() <= config.candleWindowMs(),
                "PREVIEW_INTERVAL_MS (%s) must be <= CANDLE_WINDOW_MS (%s)",
                config.previewIntervalMs(),
                config.candleWindowMs());
        slots.clear();
        previewCounter =
                getRuntimeContext().getMetricGroup().counter("compute.candles.previews.emitted");
        lateDroppedCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("compute.candles.previews.late.dropped");
        restoredTimerNoopCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("compute.candles.previews.restored_timer_noop");
        outputLatency = getRuntimeContext().getMetricGroup().histogram(
                "compute.latency.ingest_to_preview",
                LatencyHistograms.create());
    }

    @Override
    public void processElement(RowData tick, Context ctx, Collector<RowData> out)
            throws Exception {
        long eventTime = tick.getLong(RawTableColumns.EVENT_TIME);
        long windowMs = config.candleWindowMs();
        long w = (eventTime / windowMs) * windowMs;

        Slot s = slotFor(ctx.getCurrentKey());
        try {
            CandleAccumulator acc = s.accs.get(w);
            if (acc == null) {
                long watermark = ctx.timerService().currentWatermark();
                if (w + windowMs <= watermark) {
                    // Late tick for a closed window: the old branch
                    // resurrected it and emitted garbage partial previews —
                    // drop loudly instead (deliberate improvement, §OP5).
                    lateDroppedCounter.inc();
                    return;
                }
                acc = new CandleAccumulator();
                s.accs.put(w, acc);
                // G-CHAIN-2: fail closed instead of leaking windows under a
                // stuck watermark.
                Preconditions.checkState(s.accs.size() <= MAX_LIVE_WINDOWS,
                        "heap-preview live windows %s exceeded cap %s for key %s",
                        s.accs.size(), MAX_LIVE_WINDOWS, ctx.getCurrentKey());
                // First preview one interval into the window (event time) —
                // the old trigger's onElement register-once.
                ctx.timerService().registerEventTimeTimer(w + config.previewIntervalMs());
            }
            aggregate.add(tick, acc);
        } finally {
            if (s.accs.isEmpty()) {
                slots.remove(ctx.getCurrentKey());
            }
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out)
            throws Exception {
        Slot s = slots.get(ctx.getCurrentKey());
        if (s == null) {
            restoredTimerNoopCounter.inc();
            return;
        }
        long windowMs = config.candleWindowMs();
        long intervalMs = config.previewIntervalMs();
        // Every timer this operator registers is in (wStart, wEnd] for the
        // window that owns it — interval cadence or end fire — so the
        // window start is the containing aligned boundary of timestamp - 1.
        long w = ((timestamp - 1L) / windowMs) * windowMs;
        CandleAccumulator acc = s.accs.get(w);
        if (acc == null) {
            // Stale timer (window already ended, or restored timer for a
            // forgotten window): no-op loudly.
            restoredTimerNoopCounter.inc();
            return;
        }
        long wEnd = w + windowMs;
        TimeWindow window = new TimeWindow(w, wEnd);
        previewCounter.inc();
        // Step-2: report the age of the newest tick that formed this preview
        // (acc.lastIngestTs rides the accumulator, observability only).
        if (acc.lastIngestTs > 0) {
            outputLatency.update(ctx.timerService().currentProcessingTime() - acc.lastIngestTs);
        }
        out.collect(CandlePreviewEmitFunction.buildRow(ctx.getCurrentKey(), acc, window,
                ctx.timerService().currentProcessingTime(), config));
        if (timestamp >= wEnd) {
            // END FIRE (the old trigger's FIRE_AND_PURGE): final preview
            // emitted, the window is done — drop the accumulator
            // (G-CHAIN-1 structural bound: a live window lives only from its
            // first tick to its end fire).
            s.accs.remove(w);
            if (s.accs.isEmpty()) {
                slots.remove(ctx.getCurrentKey());
            }
            return;
        }
        // Interval cadence fire: re-arm the next preview, and the end fire
        // once the next cadence would land at/past the end.
        long next = timestamp + intervalMs;
        ctx.timerService().registerEventTimeTimer(Math.min(next, wEnd));
    }

    private Slot slotFor(long key) {
        Slot s = slots.get(key);
        if (s == null) {
            s = new Slot();
            slots.put(key, s);
            // G-CHAIN-2: fail closed on runaway key growth instead of
            // silently dropping instruments.
            Preconditions.checkState(slots.size() <= GLOBAL_KEY_CAP,
                    "heap-preview slots %s exceeded cap %s — refusing silent eviction",
                    slots.size(), GLOBAL_KEY_CAP);
        }
        return s;
    }

    /** Test seams: live slot count; live windows for a token. */
    int slotCountForTest() {
        return slots.size();
    }

    int liveWindowsForTest(long token) {
        Slot s = slots.get(token);
        return s == null ? 0 : s.accs.size();
    }
}
