package com.trading.compute.signaljob;

import com.trading.common.model.FormingBar;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * Forming-bar persistence coalescer (REQ-FC-007): heap snapshots (chain-heap
 * redesign, 2026-09-03 plan
 * {@code docs/plans/2026-09-03-candle-chain-heap-plan.md} OP3).
 *
 * <p>Replaces the per-tick {@code ValueState} read-modify-write on the
 * persist leg (2 touches per tick, 36.6% busy at 4.9 k/s) with a per-key
 * in-heap snapshot slot. The coalescing and timer cadence are unchanged: at
 * most {@code formingBatchMs} stale, one sink row per instrument per flush.
 * The processing-time flush timer survives as the ONLY managed piece
 * (timers cannot live in the heap — Flink owns the clock).
 *
 * <p><b>Intentional amnesia:</b> the buffered snapshot is a plain field, NOT
 * Flink managed state. A restore keeps the timer rhythm but drops the
 * pre-crash snapshot: the next post-restore tick re-buffers within a
 * fraction of a second and the KV row converges on the next flush — nobody
 * reads a half-second-stale in-progress bar on a restart path. Run under uid
 * {@code forming-bar-writer-v2} so pre-redesign checkpoints fail closed
 * (G-CHAIN-3).
 *
 * <p><b>Fail-fast guards:</b> G-CHAIN-1 — one slot per key is structural;
 * G-CHAIN-2 — global slot cap fails closed instead of silently dropping
 * instruments.
 */
public class FormingBarWriterFunction extends KeyedProcessFunction<Long, FormingBar, RowData> {

    private static final long serialVersionUID = 2L;

    /**
     * Headroom over the instrument universe for the global slot cap
     * (G-CHAIN-2): 1024 instruments expected; 64× headroom before failing
     * closed.
     */
    static final int GLOBAL_SLOT_CAP = 65_536;

    private final SignalJobConfig config;

    /**
     * Per-key latest snapshot. Plain fields: intentionally NOT checkpointed —
     * the Fluss KV table holds the durable row; this buffer only coalesces.
     */
    private final Map<Long, FormingBar> latestByInstrument = new HashMap<>();

    public FormingBarWriterFunction(SignalJobConfig config) {
        this.config = Preconditions.checkNotNull(config);
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        latestByInstrument.clear();
    }

    @Override
    public void processElement(FormingBar bar, Context ctx, Collector<RowData> out)
            throws Exception {
        // Buffer ONLY the latest snapshot per instrument per window (last
        // write wins) and collapse redundant processing-time timers into ONE
        // outstanding flush per instrument: the per-tick KvState read-
        // modify-write pair and the old enqueue-every-tick timer storm both
        // disappear.
        boolean fresh = !latestByInstrument.containsKey(ctx.getCurrentKey());
        latestByInstrument.put(ctx.getCurrentKey(), bar);
        if (fresh) {
            // G-CHAIN-2: fail closed on runaway key growth instead of
            // silently dropping instruments.
            Preconditions.checkState(latestByInstrument.size() <= GLOBAL_SLOT_CAP,
                    "forming-writer slots %s exceeded cap %s — refusing silent eviction",
                    latestByInstrument.size(), GLOBAL_SLOT_CAP);
            // Processing-time cadence UNCHANGED (same staleness bound as
            // before: worst durable-write window = cadence + sink batching).
            long fireAt = ctx.timerService().currentProcessingTime()
                    + config.formingBarWriteBatchMs();
            ctx.timerService().registerProcessingTimeTimer(fireAt);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out)
            throws Exception {
        FormingBar latest = latestByInstrument.remove(ctx.getCurrentKey());
        if (latest == null) {
            return;
        }
        out.collect(FormingBarRowMapper.toRow(latest));
    }

    /** Test seam: live buffered-snapshot count. */
    int bufferedCountForTest() {
        return latestByInstrument.size();
    }
}
