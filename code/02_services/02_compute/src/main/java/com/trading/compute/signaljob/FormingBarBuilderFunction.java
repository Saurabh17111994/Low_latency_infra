package com.trading.compute.signaljob;

import com.trading.common.model.FormingBar;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

/**
 * Per-tick forming-bar builder (Signal dossier, REQ-FC-002/007): heap
 * accumulation (chain-heap redesign, 2026-09-03 plan
 * {@code docs/plans/2026-09-03-candle-chain-heap-plan.md} OP1).
 *
 * <p>Replaces the per-tick {@code ValueState} read-modify-write pair (2
 * RocksDB reads + 1-2 writes per tick, 47.6% busy at 4.9 k/s) with a per-key
 * in-heap slot: {@code windowStart + CandleAccumulator}. Accumulation math is
 * the untouched shared {@link CandleAggregateFunction#add}; only
 * state-holding moved. Output contract unchanged: one {@link FormingBar} to
 * main + the same reference to {@link #PERSIST_OUTPUT} per accepted tick.
 *
 * <p><b>Intentional amnesia</b> (same rationale as the dedup redesign): slots
 * are plain fields, NOT Flink managed state. A restore restarts empty and
 * rebuilds from live ticks; the forming window in flight at the crash
 * restarts partial. Completed candles, signals and saved tables are
 * unaffected. Run under uid {@code forming-bar-builder-v2} so pre-redesign
 * checkpoints fail closed (G-CHAIN-3).
 *
 * <p><b>Fail-fast guards:</b> G-CHAIN-1 — single slot per key is structural
 * (nothing to bound per key); G-CHAIN-2 — global key cap fails closed instead
 * of silently dropping instruments (a leak must shout, §OP1).
 */
public class FormingBarBuilderFunction extends KeyedProcessFunction<Long, RowData, FormingBar> {

    private static final long serialVersionUID = 2L;

    /**
     * Headroom over the instrument universe for the global slot cap
     * (G-CHAIN-2): 1024 instruments expected; 64× headroom before failing
     * closed. Growth beyond it is a key leak, not legitimate listings.
     */
    static final int GLOBAL_SLOT_CAP = 65_536;

    /** Per-key forming slot: current window + running accumulator. */
    static final class Slot implements Serializable {
        private static final long serialVersionUID = 1L;
        long windowStart = Long.MIN_VALUE;
        CandleAccumulator acc = new CandleAccumulator();
    }

    /**
     * Durable-persistence leg (forming_bar KV current-state home, 2026-08-16):
     * every accepted tick's {@link FormingBar} snapshot is also emitted here,
     * for the coalescing {@link FormingBarWriterFunction}. Same event, second
     * consumer — the main output remains the Business Logic stream exactly as
     * before (REQ-FC-007); the persist leg exists so the Fluss KV projection
     * can hold the latest forming bar per instrument without putting any
     * per-tick write on the hot path.
     */
    public static final OutputTag<FormingBar> PERSIST_OUTPUT = new OutputTag<>(
            "forming-bar-persist", FormingBarTypeInfo.INSTANCE);

    private final SignalJobConfig config;
    private final CandleAggregateFunction aggregate;

    /** Per-key forming slots. Plain fields: intentionally NOT checkpointed. */
    private final Map<Long, Slot> slots = new HashMap<>();

    private transient Counter updateCounter;

    public FormingBarBuilderFunction(SignalJobConfig config) {
        this.config = Preconditions.checkNotNull(config);
        this.aggregate = new CandleAggregateFunction();
    }

    @Override
    public void open(OpenContext openContext) {
        slots.clear();
        updateCounter = getRuntimeContext().getMetricGroup().counter("compute.forming.bar.updates");
    }

    @Override
    public void processElement(RowData tick, Context ctx, Collector<FormingBar> out)
            throws Exception {
        long eventTime = tick.getLong(RawTableColumns.EVENT_TIME);
        long windowMs = config.candleWindowMs();
        // Epoch-aligned window start — the same alignment as TumblingEventTimeWindows.
        long windowStart = (eventTime / windowMs) * windowMs;

        Slot slot = slots.get(ctx.getCurrentKey());
        if (slot == null) {
            slot = new Slot();
            slots.put(ctx.getCurrentKey(), slot);
            // G-CHAIN-2: fail closed on runaway key growth instead of
            // silently dropping instruments (eviction would corrupt bars).
            Preconditions.checkState(slots.size() <= GLOBAL_SLOT_CAP,
                    "forming-bar slots %s exceeded cap %s — refusing silent eviction",
                    slots.size(), GLOBAL_SLOT_CAP);
        }
        if (slot.windowStart != windowStart) {
            // First tick of a new window: start a fresh bar. A fresh object
            // (not in-place reset) so a future field added to
            // CandleAccumulator can never leak across windows silently —
            // one small alloc per 15 s window per key is negligible.
            slot.acc = new CandleAccumulator();
            slot.windowStart = windowStart;
        }

        // Same accumulation semantics as the candle path (REQ-FC-002).
        aggregate.add(tick, slot.acc);

        updateCounter.inc();

        CandleAccumulator acc = slot.acc;
        FormingBar bar = new FormingBar(
                tick.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                windowStart,
                windowStart + windowMs,
                acc.openPaise,
                acc.highPaise,
                acc.lowPaise,
                acc.closePaise,
                acc.volume,
                acc.tickCount,
                acc.lastEventTime == Long.MIN_VALUE ? eventTime : acc.lastEventTime,
                acc.lastFingerprint,
                acc.exchange,
                acc.symbol);
        out.collect(bar);
        // Same snapshot to the persistence leg (the writer coalesces — the
        // side output is in-process, never a Fluss write per tick).
        ctx.output(PERSIST_OUTPUT, bar);
    }

    /** Test seams: live slot count; window start tracked for a token. */
    int slotCountForTest() {
        return slots.size();
    }

    long windowStartForTest(long token) {
        Slot s = slots.get(token);
        return s == null ? Long.MIN_VALUE : s.windowStart;
    }
}
