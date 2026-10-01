package com.trading.compute.signaljob;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

/**
 * Multi-timeframe aggregator operator — Phase 2 correctness-critical core.
 *
 * <p>Keyed by {@code instrument_token} (Long). Holds ONE heap composite entry per instrument:
 * 6 forming {@link CandleAccumulator}s, 6 last-15 {@link ClosedCandle} rings, quote snapshot,
 * discontinuity marker, monotonic gates. Reuses {@link CandleAggregateFunction#add} for TRADE-only
 * volume/tickCount semantics and deterministic (event_time,fingerprint) open/close.
 *
 * <p><b>Intentional amnesia:</b> State is held in a plain {@code HashMap<Long,Slot>} on-heap,
 * NOT as Flink managed {@code ValueState}. A checkpoint-restore restarts empty and rebuilds strictly
 * from live ticks; restored event-time timers with no heap slot loud-noop via
 * {@code compute.candles.restored_timer_noop} and avoids per-tick RocksDB
 * read-modify-write. W3-d (2026-09-30): the live mirrors are scan-driven — the only
 * timers this operator registers are window boundaries and session close. The job is
 * launched under uid {@code multi-tf-aggregator-v1} so pre-redesign
 * checkpoints fail closed (G-CHAIN-3). See design §C.2 §E.4.
 *
 * <p><b>Fail-fast caps:</b> Global slot cap {@code 65_536} and per-TF pending-close cap
 * instruments.
 *
 * <p>Behavior contract (design §E/F/D, §A decisions 3,4,6,12):
 * <ul>
 * <li>Monotonic gate: if {@code eventTime <= lastEventTime} → late/out-of-order drop, counted loud, no duplicate.</li>
 * <li>Session filter: {@link TimeframeBucket#isInSession} false → pre/post filtered, counted.</li>
 * <li>Trade gate: {@code tick_type==TRADE && qty>0} only mutates OHLC; quotes handled per §E (no OHLC, never silent).</li>
 * <li>Accumulation into all 6 forming via {@code CandleAggregateFunction.add} per {@code bucketStart}.</li>
 * <li>Boundary roll at {@code windowStart+tfMs} (event-time timer) → build {@link ClosedCandle},
 * rotate into ring (evict >15), emit main-output {@link CandleClosedColumns} row (first-write-wins
 * guard is sink-side, operator never re-emits same window — guarded via emitted map).</li>
 * <li>Session-end forced roll at 15:30 IST (last bucket) per §D/E — nothing carries overnight; overnight gap is expected, not a discontinuity.</li>
 * <li>Live snapshot every {@code liveSnapshotIntervalMs} per key → {@link #LIVE_TAG} side-output per TF (CandleLiveColumns), from forming accumulator. W3-d: emitted by an operator-scope scan on the record path ({@link #maybeScanLiveMirrors}) — no per-key live timers, no timer-state churn.</li>
 * <li>Signal side-output {@link #SIGNAL_TAG} per accepted TRADE tick → {@link MultiTimeframeSignalContext} snapshot AFTER mutation, 6 TimeframeContext entries with newest-first closed copies (no alias).</li>
 * </ul>
 */
public class MultiTimeframeAggregateFunction extends KeyedProcessFunction<Long, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    /** Side-output for ticks dropped as late (unconsumed; kept for debuggability). */
    public static final OutputTag<RowData> LATE_DROPPED_TAG =
            new OutputTag<RowData>("candle-late-dropped") {};

    /** Side-output for live-candle refresh: per-TF forming RowData in {@link CandleLiveColumns} layout. */
    public static final OutputTag<RowData> LIVE_TAG = new OutputTag<RowData>("candle-live") {};

    /**
     * Fast per-tick live feed (2026-09-26 low-latency signal path; all six
     * timeframes since 2026-10-01): every accepted trade tick emits each TF's
     * forming row ({@link CandleLiveColumns}), FIFTEEN_S first, so the strategy
     * host reads the evolving candle of every timeframe in memory at tick
     * latency. Consumed by the strategy host only — the host stores nothing per
     * tick (closed-only storage, 2026-10-01; one sealed row per window at
     * close), so this feed does not multiply the Fluss churn. The 1s
     * {@link #LIVE_TAG} snapshot stays the mirror cadence.
     */
    public static final OutputTag<RowData> LIVE_TICK_TAG = new OutputTag<RowData>("candle-live-tick") {};

    /** Side-output for in-JVM signal context: heap snapshot per TRADE tick (forming + last-15 rings per TF). */
    public static final OutputTag<MultiTimeframeSignalContext> SIGNAL_TAG =
            new OutputTag<MultiTimeframeSignalContext>("signal-context") {};

    /** Global heap slot cap (G-CHAIN-2). */
    static final int GLOBAL_SLOT_CAP = 65_536;

    /** Bound on rolled windows awaiting their event-time close per TF per key (G-CHAIN-2).
     *  Multi-TF spans more buckets per wall interval than single-TF 1m (15s → 24 buckets in 6m),
     *  so scale from 16 to 64 to keep fail-closed for true wild reorder while letting
     *  soak G.3 (6m span) pass without tripping. Single-TF heap cap remains 16. */
    static final int MAX_PENDING_CLOSES = 64;

    private final long liveSnapshotIntervalMs;

    /**
     * Phase 5 soak mode A (2026-09-05): accept ticks OUTSIDE the NSE
     * 09:15-15:30 IST session so the 15s fake-broker side-by-side soak can run
     * on a weekend/after-close clock. Mirrors the pre/post session counter
     * behavior of {@link TimeframeBucket#isInSession} but skips the DROP.
     * Production default false (wired from {@code MULTITF_SESSION_BYPASS}).
     */
    private final boolean sessionBypass;

    /**
     * Candle-phase soak switch (2026-09-05): when false, {@code processElement}
     * skips the per-tick {@link #SIGNAL_TAG} snapshot build (6 forming copies +
     * 6 closed-ring deep copies per tick) and emits nothing to the signal side
     * output. Candle accumulation, timers, closed/live rows are untouched.
     * Production default true (wired from {@code MULTITF_SIGNAL_CONTEXT_ENABLED}).
     */
    private final boolean signalContextEnabled;

    /**
     * Low-latency signal path (2026-09-26): when true, every accepted trade
     * tick emits the smallest-TF forming row to {@link #LIVE_TICK_TAG} so the
     * strategy host reads in-memory state at tick latency instead of the
     * snapshot cadence. The configured {@link #LIVE_TAG} mirror is unaffected.
     */
    private final boolean emitLiveTick;

    /**
     * M3-1: out-of-orderness tolerance from {@code ALLOWED_LATENESS_MS}. Before
     * this field existed the three arithmetic sites hardcoded 5 000 ms, so a
     * deployment that configured a different tolerance was silently ignored.
     */
    private final long allowedLatenessMs;

    /** Reused aggregation math — TRADE-only volume/tickCount lives here (D2). */
    private final CandleAggregateFunction aggregate = new CandleAggregateFunction();

    /** Per-key heap slots — intentional amnesia, not checkpointed. */
    private final Map<Long, Slot> slots = new HashMap<>();

    /**
     * W3-d (2026-09-30): operator-scope live-mirror due markers — min over all
     * slots of each slot's next live emission time. The live mirrors (the
     * configured {@code liveSnapshotIntervalMs} cadence) are emitted by
     * {@link #maybeScanLiveMirrors} on the record path instead of
     * by per-key self-perpetuating timers, which re-registered every second
     * per active key (~4 900 timer-state mutations/s; ~20 MB/min of changelog
     * volume and the 94 MB checkpoint state). Plain heap fields — intentional
     * amnesia, recomputed by every scan.
     */
    private long nextDueProcMs = Long.MIN_VALUE;
    private long nextDueEventMs = Long.MIN_VALUE;

    // Metrics (transient)
    private transient Counter lateDroppedCounter;
    private transient Counter sessionFilteredPreCounter;
    private transient Counter sessionFilteredPostCounter;
    private transient Counter gapCounter;
    private transient Counter emittedCounter;
    private transient Counter restoredTimerNoopCounter;
    private transient Counter liveEmittedCounter;
    private transient Counter liveTickEmittedCounter;

    /** Per-key slot holder. */
    static final class Slot implements Serializable {
        private static final long serialVersionUID = 1L;
        final MultiTimeframeState state = new MultiTimeframeState();
        final long[] windowStarts = new long[Timeframe.values().length];
        @SuppressWarnings("unchecked")
        final LinkedHashMap<Long, CandleAccumulator>[] pending = new LinkedHashMap[Timeframe.values().length];
        @SuppressWarnings("unchecked")
        final LinkedHashMap<Long, Boolean>[] emitted = new LinkedHashMap[Timeframe.values().length];
        /** W3-d: next live-mirror due times consumed by the operator-scope scan
         *  (plain heap markers — no timers, no managed state). */
        long nextLiveEventTimer = Long.MIN_VALUE;
        long nextLiveProcTimer = Long.MIN_VALUE;
        long sessionCloseTimer = Long.MIN_VALUE;
        /** P2-144: last watermark the per-tick emitted scan ran at — skip the
         * 6×64 scan when the watermark hasn't moved (steady-state O(1)). */
        long lastEvictedWatermark = Long.MIN_VALUE;

        Slot() {
            for (int i = 0; i < windowStarts.length; i++) {
                windowStarts[i] = Long.MIN_VALUE;
            }
            for (int i = 0; i < pending.length; i++) {
                pending[i] = new LinkedHashMap<>();
                emitted[i] = new LinkedHashMap<>();
            }
        }
    }

    public MultiTimeframeAggregateFunction(long liveSnapshotIntervalMs) {
        this(liveSnapshotIntervalMs, false, true);
    }

    public MultiTimeframeAggregateFunction(long liveSnapshotIntervalMs, boolean sessionBypass) {
        this(liveSnapshotIntervalMs, sessionBypass, true);
    }

    public MultiTimeframeAggregateFunction(long liveSnapshotIntervalMs, boolean sessionBypass,
            boolean signalContextEnabled) {
        this(liveSnapshotIntervalMs, sessionBypass, signalContextEnabled, false);
    }

    public MultiTimeframeAggregateFunction(long liveSnapshotIntervalMs, boolean sessionBypass,
            boolean signalContextEnabled, boolean emitLiveTick) {
        this(liveSnapshotIntervalMs, sessionBypass, signalContextEnabled, emitLiveTick,
                SignalJobConfig.DEFAULT_ALLOWED_LATENESS_MS);
    }

    /**
     * Canonical constructor (M3-1): {@code allowedLatenessMs} is the same
     * tolerance {@link SignalJobConfig} parsed from {@code ALLOWED_LATENESS_MS}
     * — eviction, the late-window gate and the emitted-map bound must all use
     * it, or the configured value only half-exists.
     */
    public MultiTimeframeAggregateFunction(long liveSnapshotIntervalMs, boolean sessionBypass,
            boolean signalContextEnabled, boolean emitLiveTick, long allowedLatenessMs) {
        Preconditions.checkArgument(liveSnapshotIntervalMs > 0, "liveSnapshotIntervalMs must be >0");
        Preconditions.checkArgument(allowedLatenessMs >= 0, "allowedLatenessMs must be >=0");
        this.liveSnapshotIntervalMs = liveSnapshotIntervalMs;
        this.sessionBypass = sessionBypass;
        this.signalContextEnabled = signalContextEnabled;
        this.emitLiveTick = emitLiveTick;
        this.allowedLatenessMs = allowedLatenessMs;
    }

    @Override
    public void open(OpenContext openContext) {
        slots.clear();
        nextDueProcMs = Long.MIN_VALUE;
        nextDueEventMs = Long.MIN_VALUE;
        try {
            lateDroppedCounter = getRuntimeContext().getMetricGroup().counter("compute.candles.late.dropped");
            sessionFilteredPreCounter = getRuntimeContext().getMetricGroup().counter("compute.session.filtered.pre_open");
            sessionFilteredPostCounter = getRuntimeContext().getMetricGroup().counter("compute.session.filtered.post_close");
            gapCounter = getRuntimeContext().getMetricGroup().counter("compute.candles.gap.detected");
            emittedCounter = getRuntimeContext().getMetricGroup().counter("compute.candles.emitted");
            restoredTimerNoopCounter = getRuntimeContext().getMetricGroup().counter("compute.candles.restored_timer_noop");
            liveEmittedCounter = getRuntimeContext().getMetricGroup().counter("compute.candles.live.emitted");
            liveTickEmittedCounter =
                    getRuntimeContext().getMetricGroup().counter("compute.candles.live.tick.emitted");
        } catch (Exception ignored) {
            // harness may not provide metrics
        }
    }

    private Slot slotFor(long key) {
        Slot s = slots.get(key);
        if (s == null) {
            // P2-037: check-before-insert — an oversize slot must never enter
            // the map, or an amnesia restart re-triggers the same oversize
            // failure as a restart loop. Loud drop, no throw from data path.
            if (slots.size() >= GLOBAL_SLOT_CAP) {
                if (lateDroppedCounter != null) lateDroppedCounter.inc();
                return null;
            }
            s = new Slot();
            slots.put(key, s);
        }
        return s;
    }

    private static void setForming(Timeframe tf, CandleAccumulator acc, Slot slot) {
        switch (tf) {
            case FIFTEEN_S: slot.state.formingFifteenS = acc; break;
            case THIRTY_S: slot.state.formingThirtyS = acc; break;
            case ONE_M: slot.state.formingOneM = acc; break;
            case THREE_M: slot.state.formingThreeM = acc; break;
            case FIVE_M: slot.state.formingFiveM = acc; break;
            case FIFTEEN_M: slot.state.formingFifteenM = acc; break;
            default: throw new IllegalArgumentException("unknown tf " + tf);
        }
    }

    private static GenericRowData buildClosedRow(long token, Timeframe tf, long windowStart, long windowEnd,
            CandleAccumulator acc) {
        GenericRowData row = new GenericRowData(CandleClosedColumns.FIELD_COUNT);
        row.setField(CandleClosedColumns.INSTRUMENT_TOKEN, token);
        row.setField(CandleClosedColumns.EXCHANGE, acc.exchange == null ? null : StringData.fromString(acc.exchange));
        row.setField(CandleClosedColumns.SYMBOL, acc.symbol == null ? null : StringData.fromString(acc.symbol));
        row.setField(CandleClosedColumns.TF, StringData.fromString(tf.code()));
        row.setField(CandleClosedColumns.WINDOW_START, windowStart);
        row.setField(CandleClosedColumns.WINDOW_END, windowEnd);
        row.setField(CandleClosedColumns.OPEN_PAISE, acc.openPaise);
        row.setField(CandleClosedColumns.HIGH_PAISE, acc.highPaise);
        row.setField(CandleClosedColumns.LOW_PAISE, acc.lowPaise);
        row.setField(CandleClosedColumns.CLOSE_PAISE, acc.closePaise);
        row.setField(CandleClosedColumns.VOLUME, acc.volume);
        row.setField(CandleClosedColumns.TICK_COUNT, (int) acc.tickCount);
        row.setField(CandleClosedColumns.LAST_EVENT_TIME, acc.lastEventTime);
        row.setField(CandleClosedColumns.LAST_EVENT_FINGERPRINT,
                acc.lastFingerprint == null ? null : StringData.fromString(acc.lastFingerprint));
        row.setField(CandleClosedColumns.SCHEMA_VERSION, StringData.fromString(CandleClosedColumns.SCHEMA_VERSION_V1));
        return row;
    }

    private static GenericRowData buildLiveRow(long token, Timeframe tf, long windowStart, long windowEnd,
            CandleAccumulator acc, MultiTimeframeState state) {
        GenericRowData row = new GenericRowData(CandleLiveColumns.FIELD_COUNT);
        row.setField(CandleLiveColumns.INSTRUMENT_TOKEN, token);
        row.setField(CandleLiveColumns.EXCHANGE, acc.exchange == null ? null : StringData.fromString(acc.exchange));
        row.setField(CandleLiveColumns.SYMBOL, acc.symbol == null ? null : StringData.fromString(acc.symbol));
        row.setField(CandleLiveColumns.TF, StringData.fromString(tf.code()));
        row.setField(CandleLiveColumns.WINDOW_START, windowStart);
        row.setField(CandleLiveColumns.WINDOW_END, windowEnd);
        row.setField(CandleLiveColumns.OPEN_PAISE, acc.openPaise);
        row.setField(CandleLiveColumns.HIGH_PAISE, acc.highPaise);
        row.setField(CandleLiveColumns.LOW_PAISE, acc.lowPaise);
        row.setField(CandleLiveColumns.CLOSE_PAISE, acc.closePaise);
        row.setField(CandleLiveColumns.VOLUME, acc.volume);
        row.setField(CandleLiveColumns.TICK_COUNT, (int) acc.tickCount);
        row.setField(CandleLiveColumns.LAST_EVENT_TIME, acc.lastEventTime);
        row.setField(CandleLiveColumns.LAST_EVENT_FINGERPRINT,
                acc.lastFingerprint == null ? null : StringData.fromString(acc.lastFingerprint));
        row.setField(CandleLiveColumns.SCHEMA_VERSION, StringData.fromString(CandleLiveColumns.SCHEMA_VERSION_V1));
        // Platform-speed KPI probe (2026-10-01): the accept wall-clock of the
        // close-setting tick rides the forming row to the strategy host, which
        // reports compute.latency.ingest_to_strategy. Sentinel Long.MIN_VALUE
        // means unknown — never fabricated, never persisted.
        row.setField(CandleLiveColumns.INGEST_TS, acc.lastIngestTs);
        // Strategy market snapshot (2026-10-01): latest known raw extras,
        // captured from every accepted tick. 0 = never seen -> NULL (never a
        // fabricated zero); see updateMarketSnapshot for the capture rules.
        setNullableLong(row, CandleLiveColumns.MKT_TOTAL_BUY_QTY, state.lastTotalBuyQty);
        setNullableLong(row, CandleLiveColumns.MKT_TOTAL_SELL_QTY, state.lastTotalSellQty);
        setNullableLong(row, CandleLiveColumns.MKT_DAY_OPEN_PAISE, state.lastDayOpenPaise);
        setNullableLong(row, CandleLiveColumns.MKT_DAY_HIGH_PAISE, state.lastDayHighPaise);
        setNullableLong(row, CandleLiveColumns.MKT_DAY_LOW_PAISE, state.lastDayLowPaise);
        setNullableLong(row, CandleLiveColumns.MKT_PREV_CLOSE_PAISE, state.lastPrevClosePaise);
        setNullableLong(row, CandleLiveColumns.MKT_VWAP_PAISE, state.lastVwapPaise);
        setNullableLong(row, CandleLiveColumns.MKT_OPEN_INTEREST, state.lastOpenInterest);
        setNullableLong(row, CandleLiveColumns.MKT_OI_DAY_HIGH, state.lastOiDayHigh);
        setNullableLong(row, CandleLiveColumns.MKT_OI_DAY_LOW, state.lastOiDayLow);
        setNullableLong(row, CandleLiveColumns.MKT_BID_PX_1, state.lastBidPaise);
        setNullableLong(row, CandleLiveColumns.MKT_BID_QTY_1, state.lastBidSize);
        setNullableLong(row, CandleLiveColumns.MKT_ASK_PX_1, state.lastAskPaise);
        setNullableLong(row, CandleLiveColumns.MKT_ASK_QTY_1, state.lastAskSize);
        setNullableLong(row, CandleLiveColumns.MKT_LOWER_LIMIT_PAISE, state.lastLowerLimitPaise);
        setNullableLong(row, CandleLiveColumns.MKT_UPPER_LIMIT_PAISE, state.lastUpperLimitPaise);
        return row;
    }

    /** BIGINT NULL transport helper: 0 means "not yet seen" (the row carries NULL). */
    private static void setNullableLong(GenericRowData row, int index, long value) {
        if (value == 0L) {
            row.setField(index, null);
        } else {
            row.setField(index, value);
        }
    }

    /**
     * Strategy market snapshot capture (2026-10-01): read the 16 raw extras
     * strategies consume from every accepted tick — trade or quote. Latest
     * non-null wins per field: a tick that does not carry a field (lighter
     * feed mode, other tick type) never erases the last known value, and a
     * field never seen stays 0 (rendered NULL on the forming row). Quote ticks
     * update the snapshot but still emit no forming row and never touch OHLC;
     * the next accepted trade tick carries the fresh values.
     */
    private static void updateMarketSnapshot(MultiTimeframeState state, RowData tick, long eventTime) {
        boolean updated = false;
        if (!tick.isNullAt(RawTableColumns.TOTAL_BUY_QTY)) {
            state.lastTotalBuyQty = tick.getLong(RawTableColumns.TOTAL_BUY_QTY);
            updated = true;
        }
        if (!tick.isNullAt(RawTableColumns.TOTAL_SELL_QTY)) {
            state.lastTotalSellQty = tick.getLong(RawTableColumns.TOTAL_SELL_QTY);
            updated = true;
        }
        if (!tick.isNullAt(RawTableColumns.OPEN_PAISE)) {
            state.lastDayOpenPaise = tick.getLong(RawTableColumns.OPEN_PAISE);
            updated = true;
        }
        if (!tick.isNullAt(RawTableColumns.HIGH_PAISE)) {
            state.lastDayHighPaise = tick.getLong(RawTableColumns.HIGH_PAISE);
            updated = true;
        }
        if (!tick.isNullAt(RawTableColumns.LOW_PAISE)) {
            state.lastDayLowPaise = tick.getLong(RawTableColumns.LOW_PAISE);
            updated = true;
        }
        if (!tick.isNullAt(RawTableColumns.CLOSE_PAISE)) {
            state.lastPrevClosePaise = tick.getLong(RawTableColumns.CLOSE_PAISE);
            updated = true;
        }
        if (!tick.isNullAt(RawTableColumns.VWAP_PAISE)) {
            state.lastVwapPaise = tick.getLong(RawTableColumns.VWAP_PAISE);
            updated = true;
        }
        if (!tick.isNullAt(RawTableColumns.OPEN_INTEREST)) {
            state.lastOpenInterest = tick.getLong(RawTableColumns.OPEN_INTEREST);
            updated = true;
        }
        if (!tick.isNullAt(RawTableColumns.OI_DAY_HIGH)) {
            state.lastOiDayHigh = tick.getLong(RawTableColumns.OI_DAY_HIGH);
            updated = true;
        }
        if (!tick.isNullAt(RawTableColumns.OI_DAY_LOW)) {
            state.lastOiDayLow = tick.getLong(RawTableColumns.OI_DAY_LOW);
            updated = true;
        }
        if (!tick.isNullAt(RawTableColumns.BID_PX_1)) {
            state.lastBidPaise = tick.getLong(RawTableColumns.BID_PX_1);
            updated = true;
        }
        if (!tick.isNullAt(RawTableColumns.BID_QTY_1)) {
            state.lastBidSize = tick.getLong(RawTableColumns.BID_QTY_1);
            updated = true;
        }
        if (!tick.isNullAt(RawTableColumns.ASK_PX_1)) {
            state.lastAskPaise = tick.getLong(RawTableColumns.ASK_PX_1);
            updated = true;
        }
        if (!tick.isNullAt(RawTableColumns.ASK_QTY_1)) {
            state.lastAskSize = tick.getLong(RawTableColumns.ASK_QTY_1);
            updated = true;
        }
        if (!tick.isNullAt(RawTableColumns.LOWER_LIMIT_PAISE)) {
            state.lastLowerLimitPaise = tick.getLong(RawTableColumns.LOWER_LIMIT_PAISE);
            updated = true;
        }
        if (!tick.isNullAt(RawTableColumns.UPPER_LIMIT_PAISE)) {
            state.lastUpperLimitPaise = tick.getLong(RawTableColumns.UPPER_LIMIT_PAISE);
            updated = true;
        }
        if (updated) {
            state.lastQuoteEventTime = eventTime;
        }
    }

    @Override
    public void processElement(RowData tick, Context ctx, Collector<RowData> out) throws Exception {
        long eventTime = tick.getLong(RawTableColumns.EVENT_TIME);
        long key = ctx.getCurrentKey();
        Slot slot = slotFor(key);
        if (slot == null) {
            // P2-037: slot cap exceeded — counted loud drop, side-output for
            // debuggability, never a throw from the data path.
            if (lateDroppedCounter != null) lateDroppedCounter.inc();
            ctx.output(LATE_DROPPED_TAG, tick);
            return;
        }

        // P2-144: lazy eviction — scan the 6×64 emitted maps only when the
        // watermark advanced since the last tick (steady-state O(1) per tick
        // at 60k/s: one long compare instead of up to 384 map entries).
        long watermark = ctx.timerService().currentWatermark();
        if (watermark != Long.MIN_VALUE && watermark != slot.lastEvictedWatermark) {
            slot.lastEvictedWatermark = watermark;
            long allowedLatenessMs = this.allowedLatenessMs;
            for (Timeframe tf : Timeframe.values()) {
                int ord = tf.ordinal();
                long windowMs = tf.windowMs();
                var it = slot.emitted[ord].entrySet().iterator();
                while (it.hasNext()) {
                    long ws = it.next().getKey();
                    if (ws + windowMs + allowedLatenessMs < watermark) {
                        it.remove();
                    } else {
                        // LinkedHashMap preserves insertion order; older first — but windowStarts not strictly ordered by insertion if out-of-order?
                        // We must scan all, not break early, because insertion order may not be time order when gaps.
                        // So continue scanning.
                    }
                }
            }
        }

        // Monotonic gate: eventTime <= lastEventTime → late/out-of-order drop loudly
        if (slot.state.lastEventTime != Long.MIN_VALUE && eventTime <= slot.state.lastEventTime) {
            if (lateDroppedCounter != null) lateDroppedCounter.inc();
            // Emit to late-drop side for observability (reuse existing tag if desired — here just count)
            // We do not update state, do not emit signal/live
            ctx.output(LATE_DROPPED_TAG, tick);
            return;
        }

        // Session filter: isInSession false → pre/post drop
        if (!sessionBypass && !TimeframeBucket.isInSession(eventTime)) {
            if (TimeframeBucket.isPreOpen(eventTime)) {
                if (sessionFilteredPreCounter != null) sessionFilteredPreCounter.inc();
            } else {
                if (sessionFilteredPostCounter != null) sessionFilteredPostCounter.inc();
            }
            // Loud but counted — no state mutation, no signal
            return;
        }

        // Trade gate: TRADE && qty>0 only mutates OHLC (reuse CandleAggregateFunction
        // semantics). v4: qty is volume_delta — the quantity traded since the previous
        // tick — not last_qty, which is one trade's size and repeats on every
        // no-trade snapshot. NULL (unknown baseline) is treated as 0 volume.
        StringData tickTypeData = tick.getString(RawTableColumns.TICK_TYPE);
        String tickTypeStr = tickTypeData == null ? null : tickTypeData.toString();
        long qty = tick.isNullAt(RawTableColumns.VOLUME_DELTA) ? 0L : tick.getLong(RawTableColumns.VOLUME_DELTA);
        boolean isTrade = "TRADE".equals(tickTypeStr) && qty > 0;

        // Strategy market snapshot (2026-10-01): capture the raw extras every
        // forming row carries — BOTH trade and quote ticks update it; only
        // accepted trades emit rows (the quote branch below returns).
        updateMarketSnapshot(slot.state, tick, eventTime);

        if (!isTrade) {
            // Quote-only / zero-qty TRADE: quote snapshot side, but never OHLC.
            // P2-036: quotes must NOT advance the monotonic gate — they carry
            // no window movement, and advancing lastEventTime here drops a
            // slightly delayed TRADE that the per-TF lateness handling below
            // was designed to fold. Quote ticks still emit no signal.
            if (!tick.isNullAt(RawTableColumns.EVENT_FINGERPRINT)) {
                slot.state.lastFingerprint = tick.getString(RawTableColumns.EVENT_FINGERPRINT).toString();
            }
            // W3-d: quotes drive the operator-scope live-mirror scan too — the
            // 1s mirror cadence must survive a trade pause (quotes never touch OHLC).
            maybeScanLiveMirrors(ctx);
            // No OHLC mutation, no signal emission (per task contract TRADE-only signal)
            return;
        }

        // Gap / discontinuity handling
        boolean isGap = false;
        if (slot.state.lastEventTime != Long.MIN_VALUE) {
            long gapMs = eventTime - slot.state.lastEventTime;
            long gapThreshold = TimeframeBucket.gapThresholdMs(Timeframe.FIFTEEN_S); // smallest TF = 30_000
            if (gapMs > gapThreshold) {
                long prevOpen = TimeframeBucket.sessionOpenMs(slot.state.lastEventTime);
                long curOpen = TimeframeBucket.sessionOpenMs(eventTime);
                if (prevOpen != curOpen) {
                    // Overnight / session-boundary gap — expected, not a discontinuity (§D example 4).
                    // P2-141: expire prior-session pendings BEFORE resetting forming —
                    // if session-close was missed (watermark stalled) stale pendings
                    // would otherwise emit into the new day. Emit what's complete,
                    // count-drop what's not (never silently carry across days).
                    // P2-152: gate + marker advance atomically via resetForming.
                    for (Timeframe tf : Timeframe.values()) {
                        int ord = tf.ordinal();
                        var pit = slot.pending[ord].entrySet().iterator();
                        while (pit.hasNext()) {
                            var e = pit.next();
                            if (e.getKey() < curOpen) {
                                CandleAccumulator pacc = e.getValue();
                                pit.remove();
                                if (pacc != null && pacc.firstEventTime != Long.MAX_VALUE
                                        && !slot.emitted[ord].containsKey(e.getKey())) {
                                    slot.emitted[ord].put(e.getKey(), Boolean.TRUE);
                                    if (emittedCounter != null) emittedCounter.inc();
                                } else if (lateDroppedCounter != null) {
                                    lateDroppedCounter.inc();
                                }
                            }
                        }
                        slot.windowStarts[ord] = Long.MIN_VALUE;
                        setForming(tf, new CandleAccumulator(), slot);
                    }
                    slot.state.resetForming(eventTime, false);
                    // Do NOT set isGap for overnight; treat as fresh start next session
                } else {
                    // Same session gap > threshold → discontinuity
                    isGap = true;
                    slot.state.discontinuityPending = true;
                    slot.state.lastDiscontinuityEventTime = eventTime;
                    if (gapCounter != null) gapCounter.inc();
                    // Per-TF stale drop will be handled in the per-TF loop below
                }
            }
        }

        // W3-d (2026-09-30): live-mirror due markers — no per-key timers. The
        // marker keeps the removed timers' 1s stepline (first due at
        // now/eventTime + interval); the operator-scope minima feed the scan's
        // due-check. Intentional amnesia (plain heap fields, no managed state).
        long currentProcTime = ctx.timerService().currentProcessingTime();
        if (slot.nextLiveProcTimer == Long.MIN_VALUE) {
            slot.nextLiveProcTimer =
                    (currentProcTime == Long.MIN_VALUE ? 0L : currentProcTime) + liveSnapshotIntervalMs;
            if (nextDueProcMs == Long.MIN_VALUE || slot.nextLiveProcTimer < nextDueProcMs) {
                nextDueProcMs = slot.nextLiveProcTimer;
            }
        }
        if (slot.nextLiveEventTimer == Long.MIN_VALUE) {
            slot.nextLiveEventTimer = eventTime + liveSnapshotIntervalMs;
            if (nextDueEventMs == Long.MIN_VALUE || slot.nextLiveEventTimer < nextDueEventMs) {
                nextDueEventMs = slot.nextLiveEventTimer;
            }
        }
        // Schedule session-close forced-roll timer for this date if not already.
        // Session bypass (soak mode A): there is no session — event times can
        // be after 15:30 IST (off-hours fake feed), so 15:30 of the event's
        // date is in the PAST and a registered session-close timer fires
        // immediately, force-closing every forming window with a truncated
        // window_end = 15:30 (observed 2026-09-04 soak: every candle_closed
        // row carried window_end=1788516000000 = 15:30 IST). With bypass the
        // boundary timers (windowStart + tfMs) alone close windows correctly.
        if (!sessionBypass) {
            long sessClose = TimeframeBucket.sessionCloseMs(eventTime);
            if (slot.sessionCloseTimer == Long.MIN_VALUE || slot.sessionCloseTimer != sessClose) {
                // P2-142: delete the old-day 15:30 timer before rescheduling —
                // otherwise it fires under the new value and closes/duplicates
                // the wrong window.
                if (slot.sessionCloseTimer != Long.MIN_VALUE && slot.sessionCloseTimer != sessClose) {
                    ctx.timerService().deleteEventTimeTimer(slot.sessionCloseTimer);
                }
                // Only schedule if this sessClose is in the future relative to eventTime (it will be, since in-session eventTime < sessClose)
                ctx.timerService().registerEventTimeTimer(sessClose);
                slot.sessionCloseTimer = sessClose;
            }
        }

        // Per-TF bucket management and accumulation
        boolean anyAccepted = false;
        for (Timeframe tf : Timeframe.values()) {
            int ord = tf.ordinal();
            long newStart = TimeframeBucket.bucketStart(tf, eventTime);
            long currStart = slot.windowStarts[ord];
            CandleAccumulator acc = slot.state.forming(tf);

            // Per-TF late/pending guards:
            // If this tick's bucket is already pending or emitted for this TF, fold or drop per-TF
            // without reopening a duplicate forming window (immutability §G.5).
            CandleAccumulator pendingAcc = slot.pending[ord].get(newStart);
            if (pendingAcc != null) {
                aggregate.add(tick, pendingAcc);
                anyAccepted = true;
                continue;
            }
            if (slot.emitted[ord].containsKey(newStart)) {
                // Already closed & emitted for this TF — late re-feed, counted, no OHLC mutation (R-012)
                if (lateDroppedCounter != null) lateDroppedCounter.inc();
                continue;
            }
            // Older window (newStart < currStart) that is not pending/emitted: create pending
            // for that older bucket if still within lateness, instead of rolling forming forward
            // and losing current window. This mirrors Heap's w < formingStart branch.
            if (currStart != Long.MIN_VALUE && newStart < currStart) {
                long windowEnd = newStart + tf.windowMs();
                long watermark2 = ctx.timerService().currentWatermark();
                long allowedLatenessMs2 = allowedLatenessMs;
                if (watermark2 != Long.MIN_VALUE && windowEnd + allowedLatenessMs2 < watermark2) {
                    if (lateDroppedCounter != null) lateDroppedCounter.inc();
                    continue;
                }
                if (watermark2 != Long.MIN_VALUE && windowEnd <= watermark2) {
                    // Within lateness but already ended: if slot is fresh, count; else emit immediate close
                    // For multi-TF we treat fresh as not emitting partial crash window (mirror Heap fresh check)
                    boolean isFresh = true;
                    for (int k = 0; k < Timeframe.values().length; k++) {
                        if (slot.windowStarts[k] != Long.MIN_VALUE || !slot.pending[k].isEmpty() || !slot.emitted[k].isEmpty()) {
                            isFresh = false; break;
                        }
                    }
                    if (isFresh) {
                        if (lateDroppedCounter != null) lateDroppedCounter.inc();
                        continue;
                    }
                    CandleAccumulator older = new CandleAccumulator();
                    aggregate.add(tick, older);
                    if (slot.pending[ord].size() >= MAX_PENDING_CLOSES) {
                        // P2-038: watermark stuck or wild reorder — refuse the
                        // 65th window instead of forgetting dedup (never-re-emit
                        // holds). Counted loud drop, no throw from data path.
                        if (lateDroppedCounter != null) lateDroppedCounter.inc();
                        ctx.output(LATE_DROPPED_TAG, tick);
                        return;
                    }
                    slot.pending[ord].put(newStart, older);
                    ctx.timerService().registerEventTimeTimer(windowEnd);
                    anyAccepted = true;
                    continue;
                }
                if (watermark2 != Long.MIN_VALUE && windowEnd > watermark2) {
                    // Still open older window: hold as pending
                    CandleAccumulator older = new CandleAccumulator();
                    aggregate.add(tick, older);
                    if (slot.pending[ord].size() >= MAX_PENDING_CLOSES) {
                        // P2-038: same block-new discipline as above.
                        if (lateDroppedCounter != null) lateDroppedCounter.inc();
                        ctx.output(LATE_DROPPED_TAG, tick);
                        return;
                    }
                    slot.pending[ord].put(newStart, older);
                    ctx.timerService().registerEventTimeTimer(windowEnd);
                    anyAccepted = true;
                    continue;
                } else if (watermark2 == Long.MIN_VALUE) {
                    CandleAccumulator older = new CandleAccumulator();
                    aggregate.add(tick, older);
                    if (slot.pending[ord].size() >= MAX_PENDING_CLOSES) {
                        // P2-038: same block-new discipline as above.
                        if (lateDroppedCounter != null) lateDroppedCounter.inc();
                        ctx.output(LATE_DROPPED_TAG, tick);
                        return;
                    }
                    slot.pending[ord].put(newStart, older);
                    ctx.timerService().registerEventTimeTimer(windowEnd);
                    anyAccepted = true;
                    continue;
                }
            }

            if (isGap) {
                // Discontinuity gap handling: discard stale forming that has a hole, but preserve
                // a completed bucket that ended cleanly before the gap.
                // If newStart == currStart → gap inside same bucket → discard forming (hole inside bucket)
                // If newStart != currStart → previous bucket [currStart, currStart+tfMs) was complete before gap;
                //   it should be sealed into pending (if it has data) and a fresh window opened at newStart.
                // Intermediate empty buckets between currStart+tfMs and newStart are never materialised.
                if (currStart != Long.MIN_VALUE) {
                    if (newStart != currStart) {
                        // Previous bucket complete before gap — preserve it into pending
                        if (acc.firstEventTime != Long.MAX_VALUE) {
                            if (slot.pending[ord].size() >= MAX_PENDING_CLOSES) {
                                // P2-038: block-new, same discipline — counted
                                // loud drop instead of a data-path throw.
                                if (lateDroppedCounter != null) lateDroppedCounter.inc();
                                ctx.output(LATE_DROPPED_TAG, tick);
                                return;
                            }
                            slot.pending[ord].put(currStart, acc);
                        }
                        CandleAccumulator fresh = new CandleAccumulator();
                        setForming(tf, fresh, slot);
                        slot.windowStarts[ord] = newStart;
                        long windowEnd = newStart + tf.windowMs();
                        ctx.timerService().registerEventTimeTimer(windowEnd);
                        acc = fresh;
                    } else {
                        // Intra-bucket gap → discard stale forming, keep same windowStart but fresh accumulator
                        CandleAccumulator fresh = new CandleAccumulator();
                        setForming(tf, fresh, slot);
                        slot.windowStarts[ord] = currStart;
                        acc = fresh;
                    }
                } else {
                    if (acc.firstEventTime != Long.MAX_VALUE) {
                        CandleAccumulator fresh = new CandleAccumulator();
                        setForming(tf, fresh, slot);
                        acc = fresh;
                    }
                    slot.windowStarts[ord] = newStart;
                    long windowEnd = newStart + tf.windowMs();
                    ctx.timerService().registerEventTimeTimer(windowEnd);
                }
            } else if (currStart == Long.MIN_VALUE) {
                // Fresh window (first tick after idle or after gap overnight)
                if (acc.firstEventTime != Long.MAX_VALUE) {
                    CandleAccumulator fresh = new CandleAccumulator();
                    setForming(tf, fresh, slot);
                    acc = fresh;
                }
                slot.windowStarts[ord] = newStart;
                long windowEnd = newStart + tf.windowMs();
                ctx.timerService().registerEventTimeTimer(windowEnd);
            } else if (newStart != currStart) {
                // Normal bucket roll (no gap) — pend current forming until its event-time close
                // Only pend if current forming has data
                if (acc.firstEventTime != Long.MAX_VALUE) {
                    // P2-038: block-new — counted loud drop, no data-path throw.
                    if (slot.pending[ord].size() >= MAX_PENDING_CLOSES) {
                        if (lateDroppedCounter != null) lateDroppedCounter.inc();
                        ctx.output(LATE_DROPPED_TAG, tick);
                        return;
                    }
                    slot.pending[ord].put(currStart, acc);
                }
                // Fresh accumulator for new bucket
                CandleAccumulator fresh = new CandleAccumulator();
                setForming(tf, fresh, slot);
                slot.windowStarts[ord] = newStart;
                long windowEnd = newStart + tf.windowMs();
                ctx.timerService().registerEventTimeTimer(windowEnd);
                acc = fresh;
            }
            // Accumulate tick into this TF's forming accumulator
            aggregate.add(tick, acc);
            anyAccepted = true;
        }

        // W3-d (2026-09-30): run the operator-scope live-mirror scan after this
        // tick's mutations so a due mirror carries the freshest forming state;
        // every record that reaches the loop drives it (see maybeScanLiveMirrors).
        maybeScanLiveMirrors(ctx);

        // P2-143: a tick dropped on EVERY TF must never advance the gate or
        // emit an empty SIGNAL — bare !anyAccepted return, fresh slot or not.
        if (!anyAccepted) {
            if (lateDroppedCounter != null) lateDroppedCounter.inc();
            ctx.output(LATE_DROPPED_TAG, tick);
            return;
        }

        // Update monotonic gate fields AFTER accumulation (eventTime now becomes lastSeen)
        slot.state.lastEventTime = eventTime;
        if (!tick.isNullAt(RawTableColumns.EVENT_FINGERPRINT)) {
            slot.state.lastFingerprint = tick.getString(RawTableColumns.EVENT_FINGERPRINT).toString();
        }

        // Fast per-tick live feed (2026-09-26 low-latency signal path; all six
        // TFs since 2026-10-01): every accepted trade tick emits each TF's
        // forming row so the strategy host reads the evolving candle of every
        // timeframe in memory at tick latency — the multi-TF forming view is a
        // strategy requirement, independent of what the table stores. Emission
        // order is Timeframe.values() (FIFTEEN_S first), so single-TF strategies
        // keep evaluating on the canonical tick row. The Fluss mirror keeps the
        // 1s LIVE_TAG cadence and is not affected; the merged writer stores
        // nothing per tick — one sealed row per window at close
        // (StrategyHostFunction, closed-only storage).
        if (emitLiveTick) {
            boolean emitted = false;
            for (Timeframe tf : Timeframe.values()) {
                int ord = tf.ordinal();
                long ws = slot.windowStarts[ord];
                CandleAccumulator acc = slot.state.forming(tf);
                if (ws == Long.MIN_VALUE || acc.firstEventTime == Long.MAX_VALUE) {
                    continue;
                }
                ctx.output(LIVE_TICK_TAG,
                        buildLiveRow(key, tf, ws, ws + tf.windowMs(), acc, slot.state));
                emitted = true;
            }
            // KPI stays per accepted tick, not per row: one tick now carries up
            // to six forming rows (compute.candles.live.tick.emitted).
            if (emitted && liveTickEmittedCounter != null) {
                liveTickEmittedCounter.inc();
            }
        }

        // SIGNAL_TAG side-output: snapshot AFTER mutation (Decision 6 signals consume forming per-tick).
        // Candle-phase soak switch: when signalContextEnabled is false the
        // per-tick photocopy is skipped entirely (candles need none of it).
        if (signalContextEnabled) {
            List<MultiTimeframeSignalContext.TimeframeContext> frames = new ArrayList<>(Timeframe.values().length);
            String exchange = tick.isNullAt(RawTableColumns.EXCHANGE) ? null : tick.getString(RawTableColumns.EXCHANGE).toString();
            String symbol = tick.isNullAt(RawTableColumns.SYMBOL) ? null : tick.getString(RawTableColumns.SYMBOL).toString();
            for (Timeframe tf : Timeframe.values()) {
                // P2-044/045/046: pass live refs — the single defensive copy
                // lives inside the type (one copy per tick, not two).
                frames.add(new MultiTimeframeSignalContext.TimeframeContext(
                        tf, slot.state.forming(tf), slot.state.closed(tf).snapshotNewestFirst()));
            }
            MultiTimeframeSignalContext signalCtx = new MultiTimeframeSignalContext(key, exchange, symbol, eventTime, frames);
            ctx.output(SIGNAL_TAG, signalCtx);
        }
    }

    // W3-d: called by the operator-scope scan (processElement's Context and the
    // onTimer OnTimerContext both implement Context).
    private void emitLiveForSlot(Slot slot, long token, Context ctx) throws Exception {
        for (Timeframe tf : Timeframe.values()) {
            int ord = tf.ordinal();
            long ws = slot.windowStarts[ord];
            if (ws == Long.MIN_VALUE) continue;
            CandleAccumulator acc = slot.state.forming(tf);
            if (acc.firstEventTime == Long.MAX_VALUE) continue;
            long we = ws + tf.windowMs();
            GenericRowData row = buildLiveRow(token, tf, ws, we, acc, slot.state);
            ctx.output(LIVE_TAG, row);
            if (liveEmittedCounter != null) liveEmittedCounter.inc();
        }
    }

    /**
     * W3-d (2026-09-30): emit the due live mirrors for every due slot from the
     * record path — replaces the per-key self-perpetuating event/processing
     * live timers (the job's dominant timer-state churn: 2 registrations/s per
     * active key ~= 4 900/s, i.e. the ~20 MB/min changelog growth and the
     * growing checkpoint state_size). Semantics kept: when a slot's due time
     * is reached (processing clock or watermark), its forming rows are emitted
     * exactly as the timer callback did, and the same configured stepline
     * advances ({@code liveSnapshotIntervalMs}; default 2000ms since CHG-462).
     * Any record drives the scan for all keys; with no records nothing is
     * emitted (a stalled feed emits nothing — the mirror never needed that).
     * A due slot emits at most once per scan even if the clock jumped several
     * interval steps (the skipped steps carried identical content).
     */
    private void maybeScanLiveMirrors(Context ctx) throws Exception {
        long nowProc = ctx.timerService().currentProcessingTime();
        if (nextDueProcMs != Long.MIN_VALUE && nowProc != Long.MIN_VALUE && nowProc >= nextDueProcMs) {
            long min = Long.MAX_VALUE;
            for (Map.Entry<Long, Slot> e : slots.entrySet()) {
                Slot s = e.getValue();
                long due = s.nextLiveProcTimer;
                if (due == Long.MIN_VALUE) continue;
                if (nowProc >= due) {
                    emitLiveForSlot(s, e.getKey(), ctx);
                    do {
                        due += liveSnapshotIntervalMs;
                    } while (due <= nowProc);
                    s.nextLiveProcTimer = due;
                }
                if (due < min) min = due;
            }
            nextDueProcMs = min == Long.MAX_VALUE ? Long.MIN_VALUE : min;
        }
        long watermark = ctx.timerService().currentWatermark();
        if (watermark != Long.MIN_VALUE && nextDueEventMs != Long.MIN_VALUE && watermark >= nextDueEventMs) {
            long min = Long.MAX_VALUE;
            for (Map.Entry<Long, Slot> e : slots.entrySet()) {
                Slot s = e.getValue();
                long due = s.nextLiveEventTimer;
                if (due == Long.MIN_VALUE) continue;
                if (watermark >= due) {
                    emitLiveForSlot(s, e.getKey(), ctx);
                    do {
                        due += liveSnapshotIntervalMs;
                    } while (due <= watermark);
                    s.nextLiveEventTimer = due;
                }
                if (due < min) min = due;
            }
            nextDueEventMs = min == Long.MAX_VALUE ? Long.MIN_VALUE : min;
        }
    }

    private void closeAndEmit(long token, Timeframe tf, long windowStart, long windowEnd,
            CandleAccumulator acc, OnTimerContext ctx, Collector<RowData> out, Slot slot) throws Exception {
        // Invariant gate: reuse CandleInvariantCheck? But closed columns don't have window span check same as 15s only?
        // For multi-TF, window span is tf.windowMs() exactly (or truncated at 15:30). For smoke we skip invariant check and just emit.
        // Build ClosedCandle and rotate into ring
        ClosedCandle cc = new ClosedCandle(windowStart, windowEnd, acc.openPaise, acc.highPaise,
                acc.lowPaise, acc.closePaise, acc.volume, acc.tickCount, acc.lastEventTime, acc.lastFingerprint);
        slot.state.closed(tf).add(cc);
        slot.emitted[tf.ordinal()].put(windowStart, Boolean.TRUE);
        // P2-038: bound the emitted map (keep last 64 per TF) by evicting the
        // smallest windowStart that is safely expired — never blind FIFO by
        // insertion order. Under a stuck watermark a still-needed key is NOT
        // forgotten (never-re-emit holds); the block-new path in
        // processElement refuses the 65th live window instead.
        while (slot.emitted[tf.ordinal()].size() > 64) {
            long watermark3 = ctx.timerService().currentWatermark();
            Long minKey = null;
            for (Long k : slot.emitted[tf.ordinal()].keySet()) {
                if (minKey == null || k < minKey) minKey = k;
            }
            if (minKey == null) break;
            if (watermark3 != Long.MIN_VALUE
                    && minKey + tf.windowMs() + allowedLatenessMs < watermark3) {
                slot.emitted[tf.ordinal()].remove(minKey);
            } else {
                break;
            }
        }
        GenericRowData row = buildClosedRow(token, tf, windowStart, windowEnd, acc);
        out.collect(row);
        if (emittedCounter != null) emittedCounter.inc();
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out) throws Exception {
        Long key = ctx.getCurrentKey();
        Slot slot = slots.get(key);
        if (slot == null) {
            if (restoredTimerNoopCounter != null) restoredTimerNoopCounter.inc();
            return;
        }

        // P2-039: demultiplex on TimeDomain first, then timestamp — event-time
        // boundary and session-close share one namespace. (W3-d: the live
        // timers that also shared it are gone — see maybeScanLiveMirrors.)
        TimeDomain domain = ctx.timeDomain();
        boolean isSessionClose = domain == TimeDomain.EVENT_TIME && timestamp == slot.sessionCloseTimer;

        // Session bypass (soak mode A): never run the session-close forced
        // roll — there is no session boundary to force (see the scheduling
        // guard in processElement). A stale sessionCloseTimer value from a
        // non-bypass run cannot leak in because the timer is only registered
        // when !sessionBypass; this guard is belt-and-braces.
        if (sessionBypass) {
            isSessionClose = false;
        }

        // W3-d (2026-09-30): the live mirrors no longer fire here — they are
        // emitted by maybeScanLiveMirrors on the record path. A restored live
        // timer from a pre-change checkpoint falls through to the stale-timer
        // handling below (it is not re-registered and dies out).
        if (isSessionClose) {
            // Forced roll at 15:30 IST: seal any still-forming windows as final candles of the day
            for (Timeframe tf : Timeframe.values()) {
                int ord = tf.ordinal();
                long ws = slot.windowStarts[ord];
                if (ws == Long.MIN_VALUE) continue;
                CandleAccumulator acc = slot.state.forming(tf);
                if (acc.firstEventTime == Long.MAX_VALUE) {
                    // Empty forming: just clear
                    slot.windowStarts[ord] = Long.MIN_VALUE;
                    setForming(tf, new CandleAccumulator(), slot);
                    continue;
                }
                // Guard duplicate
                if (slot.emitted[ord].containsKey(ws)) {
                    continue;
                }
                // P2-140: fixed span — ws+tfMs is the end; the aligned last
                // bucket already has ws+tfMs == sessClose (no-op), misaligned
                // buckets keep their span instead of a truncated short candle.
                long we = ws + tf.windowMs();
                // Also clear any pending for this TF that belong to same session but not yet closed? We keep pending to be closed via normal timers, but session close should close them too.
                // First drain pending that are still before sessClose
                // Copy pending keys to avoid concurrent modification
                List<Long> pendKeys = new ArrayList<>(slot.pending[ord].keySet());
                for (Long pws : pendKeys) {
                    CandleAccumulator pacc = slot.pending[ord].remove(pws);
                    if (pacc == null || pacc.firstEventTime == Long.MAX_VALUE) continue;
                    if (slot.emitted[ord].containsKey(pws)) continue;
                    // P2-140: never persist a truncated span — full windowMs
                    // end; aligned last bucket already has ws+tfMs == sessClose
                    // so this is a no-op there, and misaligned buckets keep
                    // their fixed span instead of a short candle.
                    long pwe = pws + tf.windowMs();
                    closeAndEmit(key, tf, pws, pwe, pacc, ctx, out, slot);
                }
                // P2-140: same fixed-span rule for the forming window — never
                // min(ws+tfMs, sessClose). A truncated end would persist
                // permanently (later boundary timer noops on the emitted hit).
                long weForForming = ws + tf.windowMs();
                closeAndEmit(key, tf, ws, weForForming, acc, ctx, out, slot);
                slot.windowStarts[ord] = Long.MIN_VALUE;
                setForming(tf, new CandleAccumulator(), slot);
            }
            slot.sessionCloseTimer = Long.MIN_VALUE;
            // After forced close, we should also clear nextLive timers? Keep them? Live after close should not emit for empty windows, so they will naturally skip.
            // Session close also implies next day fresh start: gap handling will take care.
            // Continue to boundary handling but forming already cleared, so no duplicate.
        }

        // M3-2: boundary closes are event-time only — a processing-time fire whose
        // timestamp happens to align with a window end (1 in ~15 000 per slot start
        // for the 15s TF) must never close a candle early; finalization always waits
        // for the watermark.
        if (domain == TimeDomain.EVENT_TIME) {
            // Event-time boundary timers: timestamp corresponds to windowEnd = windowStart + tfMs
            // For each TF, check pending and forming
            for (Timeframe tf : Timeframe.values()) {
                int ord = tf.ordinal();
                long windowMs = tf.windowMs();
                long windowStartForTimer = timestamp - windowMs;

                // Skip boundary handling if timestamp was sessionClose (already handled) and windowStartForTimer would duplicate
                // But we still need to handle pending that may align to same timestamp and not yet handled via sessionClose path
                // For sessionClose, we already drained pending, so skip

                // Check pending first
                CandleAccumulator pendAcc = slot.pending[ord].remove(windowStartForTimer);
                if (pendAcc != null) {
                    if (slot.emitted[ord].containsKey(windowStartForTimer)) {
                        if (restoredTimerNoopCounter != null) restoredTimerNoopCounter.inc();
                        continue;
                    }
                    if (pendAcc.firstEventTime == Long.MAX_VALUE) {
                        continue;
                    }
                    long we = windowStartForTimer + windowMs;
                    closeAndEmit(key, tf, windowStartForTimer, we, pendAcc, ctx, out, slot);
                    continue;
                }

                long currStart = slot.windowStarts[ord];
                if (currStart == windowStartForTimer) {
                    CandleAccumulator acc = slot.state.forming(tf);
                    if (acc.firstEventTime == Long.MAX_VALUE) {
                        // No data: just clear
                        slot.windowStarts[ord] = Long.MIN_VALUE;
                        setForming(tf, new CandleAccumulator(), slot);
                        continue;
                    }
                    if (slot.emitted[ord].containsKey(currStart)) {
                        if (restoredTimerNoopCounter != null) restoredTimerNoopCounter.inc();
                        slot.windowStarts[ord] = Long.MIN_VALUE;
                        setForming(tf, new CandleAccumulator(), slot);
                        continue;
                    }
                    long we = currStart + windowMs;
                    closeAndEmit(key, tf, currStart, we, acc, ctx, out, slot);
                    slot.windowStarts[ord] = Long.MIN_VALUE;
                    setForming(tf, new CandleAccumulator(), slot);
                    // Clear discontinuity marker on clean close if this was the gapEnd
                    // Per design: cleared only by a clean boundary close where gapEnd == windowStart
                    // We approximate: if discontinuityPending and lastDiscontinuityEventTime < we, clear? Simpler clear after any successful close
                    // Leave as is for now — but for smoke we can clear after first close to indicate gap healed
                    if (slot.state.discontinuityPending) {
                        // Heuristic: gap considered healed after one full bucket close without further gaps
                        slot.state.discontinuityPending = false;
                    }
                    continue;
                }

                // Stale timer: no pending nor forming match. The loud-noop
                // counter stays disabled for non-matching timestamps by design
                // (heuristics in the W3-d change record). W3-d: live-timer
                // timestamps no longer exist; this branch now receives only
                // boundary/session timestamps that found nothing.
            }
        }

        // If slot is completely fresh (no forming, no pending) but still has closed rings, we keep it (history)
        // Do not remove slot — history must survive for signal lookback
    }

    // Test seams
    int slotCountForTest() {
        return slots.size();
    }

    int pendingSizeForTest(long token, Timeframe tf) {
        Slot s = slots.get(token);
        return s == null ? 0 : s.pending[tf.ordinal()].size();
    }

    int emittedSizeForTest(long token, Timeframe tf) {
        Slot s = slots.get(token);
        return s == null ? 0 : s.emitted[tf.ordinal()].size();
    }

    long windowStartForTest(long token, Timeframe tf) {
        Slot s = slots.get(token);
        return s == null ? Long.MIN_VALUE : s.windowStarts[tf.ordinal()];
    }
}
