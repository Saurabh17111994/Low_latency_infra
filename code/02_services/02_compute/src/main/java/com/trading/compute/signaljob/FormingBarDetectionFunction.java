package com.trading.compute.signaljob;

import com.trading.common.model.FormingBar;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * Forming-bar breakout detection (Signal dossier, placeholder rule): heap
 * state (chain-heap redesign, 2026-09-03 plan
 * {@code docs/plans/2026-09-03-candle-chain-heap-plan.md} OP2).
 *
 * <p>Replaces 4 per-tick {@code ValueState} touches (1 write + 2 reads on
 * every forming bar — the write fired even for ticks the rule ignores,
 * 43.1% busy at 4.9 k/s) with a per-key in-heap slot. Evaluation ORDER and
 * rule math are unchanged: current-window update before evaluation, warm
 * gate, fire-once per forming window, strictly-prior guard on completed
 * candles, lookback ring buffers trimmed to {@code formingLookbackCandles}.
 *
 * <p><b>Intentional amnesia:</b> slots are plain fields, NOT Flink managed
 * state. A restore restarts empty and re-warms in lookback×15 s from live
 * completed candles. Run under uid {@code forming-bar-detection-v2} so
 * pre-redesign checkpoints fail closed (G-CHAIN-3).
 *
 * <p><b>Fail-fast guards:</b> G-CHAIN-1 — ring buffers trim structurally to
 * the lookback (never grow); G-CHAIN-2 — global slot cap fails closed instead
 * of silently dropping instruments.
 */
public class FormingBarDetectionFunction
        extends KeyedCoProcessFunction<Long, FormingBar, RowData, RowData> {

    private static final long serialVersionUID = 2L;

    /**
     * Headroom over the instrument universe for the global slot cap
     * (G-CHAIN-2): 1024 instruments expected; 64× headroom before failing
     * closed.
     */
    static final int GLOBAL_SLOT_CAP = 65_536;

    /** Per-key detection slot: reference window, fire flag, lookback rings. */
    static final class Slot implements Serializable {
        private static final long serialVersionUID = 1L;
        long currentWindow = Long.MIN_VALUE;
        long firedWindow = Long.MIN_VALUE;
        final Deque<Long> highs = new ArrayDeque<>();
        final Deque<Long> closes = new ArrayDeque<>();
    }

    private final SignalJobConfig config;

    /** Per-key detection slots. Plain fields: intentionally NOT checkpointed. */
    private final Map<Long, Slot> slots = new HashMap<>();

    private transient Counter detectedCounter;

    public FormingBarDetectionFunction(SignalJobConfig config) {
        this.config = Preconditions.checkNotNull(config);
    }

    @Override
    public void open(OpenContext openContext) {
        slots.clear();
        detectedCounter = getRuntimeContext().getMetricGroup().counter("compute.signals.detected.forming");
    }

    private Slot slotFor(long key) {
        Slot s = slots.get(key);
        if (s == null) {
            s = new Slot();
            slots.put(key, s);
            // G-CHAIN-2: fail closed on runaway key growth instead of
            // silently dropping instruments.
            Preconditions.checkState(slots.size() <= GLOBAL_SLOT_CAP,
                    "forming-detection slots %s exceeded cap %s — refusing silent eviction",
                    slots.size(), GLOBAL_SLOT_CAP);
        }
        return s;
    }

    /** Live forming-bar event (input 1): evaluate the placeholder rule. */
    @Override
    public void processElement1(FormingBar bar, Context ctx, Collector<RowData> out)
            throws Exception {
        Slot s = slotFor(ctx.getCurrentKey());
        // Track the current forming window (drives the strictly-prior guard on
        // completed-candle ingestion below). Updated BEFORE evaluation so this
        // event's own window is the reference for the history it compares
        // against; the fire-once flag resets naturally when a later event
        // carries a different windowStart.
        s.currentWindow = bar.windowStart();

        Deque<Long> highs = s.highs;
        Deque<Long> closes = s.closes;
        int lookback = config.formingLookbackCandles();

        boolean warm = highs.size() >= lookback && closes.size() >= lookback;
        if (!warm) {
            return; // warm-up: not enough completed-candle history yet
        }

        if (s.firedWindow == bar.windowStart()) {
            return; // fire-once per forming window (placeholder-only semantics)
        }

        long maxHigh = 0;
        for (long h : highs) {
            maxHigh = Math.max(maxHigh, h);
        }
        long sumCloses = 0;
        for (long c : closes) {
            sumCloses += c;
        }
        boolean bullish = bar.closePaise() > bar.openPaise();
        boolean breakout = bar.closePaise() > maxHigh;
        boolean trend = bar.closePaise() * (long) closes.size() > sumCloses;
        if (!(bullish && breakout && trend)) {
            return;
        }

        s.firedWindow = bar.windowStart();
        detectedCounter.inc();
        out.collect(toCandidate(bar));
    }

    /** Completed candle (input 2): maintain the lookback ring buffers. */
    @Override
    public void processElement2(RowData candle, Context ctx, Collector<RowData> out)
            throws Exception {
        long candleWindowEnd = candle.getLong(CandleTableColumns.WINDOW_END);
        // Strictly-prior guard: a completed candle enters the lookback only if
        // its window END is <= the current forming window START. The completed
        // candle whose window ends exactly at the forming window's start IS the
        // immediately-preceding closed candle and legitimately belongs in the
        // history; the forming window's own candle (windowEnd == forming
        // windowEnd > start) and any future candle never do. This closes the
        // mid-window arrival edge: the candle for [W-15, W) can be emitted
        // WHILE window [W, W+15) is still forming (watermark crosses W mid-
        // window), and the guard admits it (windowEnd == W <= W) while
        // rejecting anything from window [W, W+15) or later.
        Slot s = slotFor(ctx.getCurrentKey());
        if (s.currentWindow != Long.MIN_VALUE && candleWindowEnd > s.currentWindow) {
            return;
        }
        addToBuffers(s, candle);
    }

    private void addToBuffers(Slot s, RowData candle) {
        long high = candle.getLong(CandleTableColumns.HIGH_PAISE);
        long close = candle.getLong(CandleTableColumns.CLOSE_PAISE);

        int lookback = config.formingLookbackCandles();
        s.highs.addLast(high);
        s.closes.addLast(close);
        // G-CHAIN-1: structural trim — rings never exceed the lookback, so no
        // key can grow memory across windows.
        while (s.highs.size() > lookback) {
            s.highs.removeFirst();
        }
        while (s.closes.size() > lookback) {
            s.closes.removeFirst();
        }
    }

    private RowData toCandidate(FormingBar bar) {
        GenericRowData row = new GenericRowData(SignalCandidatesTableColumns.FIELD_COUNT);
        String candidateId = config.formingRuleId() + "-" + bar.instrumentToken()
                + "-" + bar.windowStart();
        row.setField(SignalCandidatesTableColumns.CANDIDATE_ID, StringData.fromString(candidateId));
        row.setField(SignalCandidatesTableColumns.INSTRUCTION_ID, null);
        row.setField(SignalCandidatesTableColumns.TRADE_CONTEXT_ID, null);
        row.setField(SignalCandidatesTableColumns.INSTRUMENT_TOKEN, bar.instrumentToken());
        row.setField(SignalCandidatesTableColumns.EXCHANGE, StringData.fromString(bar.exchange()));
        row.setField(SignalCandidatesTableColumns.SYMBOL, StringData.fromString(bar.symbol()));
        row.setField(SignalCandidatesTableColumns.STRATEGY_ID, StringData.fromString(config.signalStrategyId()));
        row.setField(SignalCandidatesTableColumns.STRATEGY_VERSION, StringData.fromString(config.signalStrategyVersion()));
        row.setField(SignalCandidatesTableColumns.RULE_ID, StringData.fromString(config.formingRuleId()));
        row.setField(SignalCandidatesTableColumns.DETECTION_TS, bar.lastEventTime());
        row.setField(SignalCandidatesTableColumns.EVALUATION_TS, bar.lastEventTime());
        row.setField(SignalCandidatesTableColumns.ACTION, StringData.fromString(SignalCandidatesTableColumns.ACTION_ENTRY));
        row.setField(SignalCandidatesTableColumns.SIDE, StringData.fromString(SignalCandidatesTableColumns.SIDE_BUY));
        row.setField(SignalCandidatesTableColumns.QUANTITY, config.signalQuantity());
        row.setField(SignalCandidatesTableColumns.ORDER_TYPE, StringData.fromString(SignalCandidatesTableColumns.ORDER_TYPE_MARKET));
        row.setField(SignalCandidatesTableColumns.LIMIT_PRICE_PAISE, null);
        row.setField(SignalCandidatesTableColumns.SCORE_INPUTS, null);
        row.setField(SignalCandidatesTableColumns.FORMATION_SNAPSHOT_REF,
                StringData.fromString("forming-bar:" + bar.windowStart() + ":" + bar.windowEnd()
                        + ":open=" + bar.openPaise() + ":high=" + bar.highPaise()
                        + ":low=" + bar.lowPaise() + ":close=" + bar.closePaise()
                        + ":volume=" + bar.volume()));
        row.setField(SignalCandidatesTableColumns.VALIDITY_REASON,
                StringData.fromString(SignalCandidatesTableColumns.VALIDITY_REASON_VALID));
        row.setField(SignalCandidatesTableColumns.SUPERSEDES_CANDIDATE_ID, null);
        row.setField(SignalCandidatesTableColumns.SUPERSEDED_BY_CANDIDATE_ID, null);
        row.setField(SignalCandidatesTableColumns.SCHEMA_VERSION,
                StringData.fromString(SignalCandidatesTableColumns.SCHEMA_VERSION_V2));
        return row;
    }

    /** Test seams: live slot count; ring sizes for a token. */
    int slotCountForTest() {
        return slots.size();
    }

    int ringSizeForTest(long token) {
        Slot s = slots.get(token);
        return s == null ? 0 : s.highs.size();
    }
}
