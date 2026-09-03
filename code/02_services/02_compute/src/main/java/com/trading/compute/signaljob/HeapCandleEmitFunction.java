package com.trading.compute.signaljob;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

/**
 * Tumbling 15 s candle close-out (Signal dossier operator 3, R-012): heap
 * windows (chain-heap redesign, 2026-09-03 plan
 * {@code docs/plans/2026-09-03-candle-chain-heap-plan.md} OP4).
 *
 * <p>Replaces {@code keyBy → TumblingEventTimeWindows → aggregate →
 * CandleEmitFunction} — the window operator's per-tick accumulator
 * read-modify-write plus the emit guard's window-state touch (2-3 RocksDB
 * touches per tick, 46.5% busy at 4.9 k/s) — with a per-key in-heap slot:
 * one forming accumulator, a small map of rolled windows awaiting their
 * end-timer, and a bounded set of recently closed windows. Accumulation math
 * is the untouched shared {@link CandleAggregateFunction#add}; row building
 * reuses {@link CandleEmitFunction#buildRow}; quarantine routing reuses
 * {@link CandleQuarantine#buildRow}; the late-drop leg reuses
 * {@link CandleLateDrop#OUTPUT} so the downstream counter is untouched.
 *
 * <p>Fire time is identical to the old trigger: an event-time timer at
 * {@code windowEnd}, fired when the watermark passes it. Emission content
 * and order match the old operator tick-for-tick (differential-tested
 * against the real window operator in {@code HeapCandleEmitFunctionTest}).
 * Deliberate edges (all counted, never silent, never duplicates): a tick
 * past end+lateness late-drops exactly like the old side-output leg and
 * can never reopen an evicted window as forming; a tick for a never-seen
 * older window on a FRESH slot (key idle since start/restore) counts as a
 * late update instead of emitting a partial crash-window candle.
 *
 * <p><b>Intentional amnesia:</b> slots are plain fields, NOT Flink managed
 * state. A restore restarts empty; restored end-timers with no accumulator
 * no-op loudly ({@code compute.candles.restored_timer_noop}). The forming
 * window in flight rebuilds from live ticks. Run under uid
 * {@code candle-15s-v2} so pre-redesign checkpoints fail closed (G-CHAIN-3).
 *
 * <p><b>Fail-fast guards:</b> G-CHAIN-1 — the closed-window set evicts
 * structurally past {@code windowEnd + allowedLateness} (bounded by the
 * lateness bound, ~2 entries); G-CHAIN-2 — global slot cap and pending-close
 * cap fail closed instead of silently dropping data.
 */
public class HeapCandleEmitFunction extends KeyedProcessFunction<Long, RowData, RowData> {

    private static final long serialVersionUID = 2L;

    /**
     * Headroom over the instrument universe for the global slot cap
     * (G-CHAIN-2): 1024 instruments expected; 64× headroom before failing
     * closed.
     */
    static final int GLOBAL_SLOT_CAP = 65_536;

    /**
     * Bound on rolled windows awaiting their end-timer per key (G-CHAIN-2).
     * Steady state holds ≤1; a stuck watermark plus cross-window reorder
     * grows it — past this, fail loudly instead of leaking memory.
     */
    static final int MAX_PENDING_CLOSES = 16;

    /** Per-key heap window slot. */
    static final class Slot implements Serializable {
        private static final long serialVersionUID = 1L;
        long formingStart = Long.MIN_VALUE;
        CandleAccumulator forming = new CandleAccumulator();
        /** Rolled windows (start → acc) awaiting their end-timer. */
        final LinkedHashMap<Long, CandleAccumulator> pending = new LinkedHashMap<>();
        /** Recently closed windows (start → true), for late-update counting. */
        final LinkedHashMap<Long, Boolean> emitted = new LinkedHashMap<>();
    }

    private final SignalJobConfig config;
    private final CandleAggregateFunction aggregate;

    /** Per-key heap windows. Plain fields: intentionally NOT checkpointed. */
    private final Map<Long, Slot> slots = new HashMap<>();

    private transient Counter emittedCounter;
    private transient Counter lateUpdateCounter;
    private transient Counter restoredTimerNoopCounter;

    public HeapCandleEmitFunction(SignalJobConfig config) {
        this.config = Preconditions.checkNotNull(config);
        this.aggregate = new CandleAggregateFunction();
    }

    @Override
    public void open(OpenContext openContext) {
        slots.clear();
        emittedCounter = getRuntimeContext().getMetricGroup().counter("compute.candles.emitted");
        lateUpdateCounter =
                getRuntimeContext().getMetricGroup().counter("compute.candles.late.updates");
        restoredTimerNoopCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("compute.candles.restored_timer_noop");
    }

    @Override
    public void processElement(RowData tick, Context ctx, Collector<RowData> out)
            throws Exception {
        long eventTime = tick.getLong(RawTableColumns.EVENT_TIME);
        long windowMs = config.candleWindowMs();
        long latenessMs = config.allowedLatenessMs();
        long w = (eventTime / windowMs) * windowMs;
        long wEnd = w + windowMs;

        Slot s = slotFor(ctx.getCurrentKey());
        try {
            long watermark = ctx.timerService().currentWatermark();
            evictClosed(s, watermark, windowMs, latenessMs);

            // Fast path: the forming window (every in-order tick lands here).
            if (w == s.formingStart) {
                aggregate.add(tick, s.forming);
                return;
            }
            // Late fold into a rolled window still awaiting its end-timer:
            // the old operator folded the tick into the window accumulator
            // the same way (its re-trigger was a no-op until close).
            CandleAccumulator rolled = s.pending.get(w);
            if (rolled != null) {
                aggregate.add(tick, rolled);
                return;
            }
            // Late re-fire of a closed window within lateness: counted
            // no-op, no correction row (R-012), exactly like the old
            // emitted-flag path.
            if (s.emitted.containsKey(w)) {
                lateUpdateCounter.inc();
                return;
            }
            // Dead window (end + lateness behind the watermark): the old
            // operator's sideOutputLateData leg. Checked BEFORE the
            // rollover so a long-dead window can never reopen as forming
            // after its closed marker evicted (duplicate-emit hole).
            if (wEnd + latenessMs < watermark) {
                ctx.output(CandleLateDrop.OUTPUT, tick);
                return;
            }
            // Tick for an older window we hold nothing for, still within
            // lateness.
            if (s.formingStart != Long.MIN_VALUE && w < s.formingStart) {
                if (wEnd <= watermark) {
                    // End passed: the old operator created the window and
                    // fired immediately on the element. Mirror it on a LIVE
                    // slot; on a FRESH slot (idle since start/restore) count
                    // instead of emitting a partial crash-window candle.
                    if (isFresh(s)) {
                        lateUpdateCounter.inc();
                        return;
                    }
                    CandleAccumulator acc = new CandleAccumulator();
                    aggregate.add(tick, acc);
                    closeWindow(ctx.getCurrentKey(), w, acc,
                            ctx.timerService(), ctx::output, out);
                    return;
                }
                // Window still open (watermark lags the forming window):
                // hold as pending with its own end-timer, exactly as if
                // seen in order.
                CandleAccumulator acc = new CandleAccumulator();
                aggregate.add(tick, acc);
                s.pending.put(w, acc);
                checkPendingCap(s);
                ctx.timerService().registerEventTimeTimer(wEnd);
                return;
            }
            // Fresh slot whose window already ended within lateness (key
            // idle since start/restore): count, never emit a partial.
            if (isFresh(s) && wEnd <= watermark) {
                lateUpdateCounter.inc();
                return;
            }
            // Rollover (or very first tick): park the forming window as
            // pending, open the new window with its end-timer. Gaps skip
            // cleanly — empty windows never held state in the old operator
            // either.
            if (s.formingStart != Long.MIN_VALUE) {
                s.pending.put(s.formingStart, s.forming);
                checkPendingCap(s);
            }
            s.forming = new CandleAccumulator();
            s.formingStart = w;
            ctx.timerService().registerEventTimeTimer(wEnd);
            aggregate.add(tick, s.forming);
        } finally {
            // Never retain empty slots: a tick that buffered nothing (drop
            // / count paths) must not grow the key map.
            if (isFresh(s)) {
                slots.remove(ctx.getCurrentKey());
            }
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out)
            throws Exception {
        Slot s = slots.get(ctx.getCurrentKey());
        if (s == null) {
            // Restored end-timer with no heap slot (restore amnesia): no-op
            // LOUDLY so a storm of them is diagnosable, never silent.
            restoredTimerNoopCounter.inc();
            return;
        }
        long windowMs = config.candleWindowMs();
        long w = timestamp - windowMs;
        if (w == s.formingStart) {
            CandleAccumulator acc = s.forming;
            s.forming = new CandleAccumulator();
            s.formingStart = Long.MIN_VALUE;
            closeWindow(ctx.getCurrentKey(), w, acc,
                    ctx.timerService(), ctx::output, out);
            return;
        }
        CandleAccumulator rolled = s.pending.remove(w);
        if (rolled == null) {
            // Stale/duplicate timer (already closed, or restored timer for a
            // forgotten window): no-op loudly.
            restoredTimerNoopCounter.inc();
            return;
        }
        closeWindow(ctx.getCurrentKey(), w, rolled,
                ctx.timerService(), ctx::output, out);
    }

    /** Side-output emitter (adapts the two Flink context types). */
    interface SideEmitter {
        <X> void emit(OutputTag<X> tag, X value);
    }

    private void closeWindow(long token, long windowStart, CandleAccumulator acc,
            org.apache.flink.streaming.api.TimerService timers, SideEmitter side,
            Collector<RowData> out) throws Exception {
        Slot s = slots.get(token);
        if (s != null) {
            s.emitted.put(windowStart, Boolean.TRUE);
        }
        TimeWindow window = new TimeWindow(windowStart, windowStart + config.candleWindowMs());
        long now = timers.currentProcessingTime();
        // Streaming-3000 T6: five OHLC invariants gate the emit — identical
        // to the old function, including the still-set closed flag above so
        // a late re-trigger cannot double-quarantine.
        CandleInvariantCheck.Reason violation =
                CandleInvariantCheck.firstViolation(acc, window, config.candleWindowMs());
        if (violation != null) {
            side.emit(CandleQuarantine.OUTPUT,
                    CandleQuarantine.buildRow(token, acc, window, violation,
                            "compute-candle-" + UUID.randomUUID(), now));
            return;
        }
        emittedCounter.inc();
        out.collect(CandleEmitFunction.buildRow(token, acc, window, now, config));
    }

    private Slot slotFor(long key) {
        Slot s = slots.get(key);
        if (s == null) {
            s = new Slot();
            slots.put(key, s);
            // G-CHAIN-2: fail closed on runaway key growth instead of
            // silently dropping instruments.
            Preconditions.checkState(slots.size() <= GLOBAL_SLOT_CAP,
                    "heap-candle slots %s exceeded cap %s — refusing silent eviction",
                    slots.size(), GLOBAL_SLOT_CAP);
        }
        return s;
    }

    private void checkPendingCap(Slot s) {
        Preconditions.checkState(s.pending.size() <= MAX_PENDING_CLOSES,
                "heap-candle pending closes %s exceeded cap %s — watermark stuck or wild reorder",
                s.pending.size(), MAX_PENDING_CLOSES);
    }

    private static boolean isFresh(Slot s) {
        return s.formingStart == Long.MIN_VALUE && s.pending.isEmpty() && s.emitted.isEmpty();
    }

    private static void evictClosed(Slot s, long watermark, long windowMs, long latenessMs) {
        if (s.emitted.isEmpty() || watermark == Long.MIN_VALUE) {
            return;
        }
        // G-CHAIN-1: structural bound — a closed window stays only while a
        // late tick could still reference it (end + lateness).
        java.util.Iterator<Long> it = s.emitted.keySet().iterator();
        while (it.hasNext()) {
            long start = it.next();
            if (start + windowMs + latenessMs < watermark) {
                it.remove();
            }
        }
    }

    /** Test seams: live slot count; pending/emitted sizes for a token. */
    int slotCountForTest() {
        return slots.size();
    }

    int pendingSizeForTest(long token) {
        Slot s = slots.get(token);
        return s == null ? 0 : s.pending.size();
    }

    int emittedSizeForTest(long token) {
        Slot s = slots.get(token);
        return s == null ? 0 : s.emitted.size();
    }
}
