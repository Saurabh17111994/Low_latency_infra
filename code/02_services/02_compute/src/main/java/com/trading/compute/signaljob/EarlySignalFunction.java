package com.trading.compute.signaljob;

import java.util.Map;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;

/**
 * Early-signal machinery (low-latency candles Phase 2, 2026-08-29) —
 * tentative on preview, CONFIRM/CANCEL at window end, auditable
 * supersession via {@code supersedes_candidate_id}.
 *
 * <p>Consumes TWO keyed-by-instrument streams:
 * <ol>
 *   <li><b>Preview rows</b> ({@link CandlePreviewColumns}, {@code is_preview=true},
 *       one per 1s tick per window): when the breakout rule first holds on the
 *       in-progress OHLCV, emit a <b>TENTATIVE</b> candidate (once per window).</li>
 *   <li><b>Final candle rows</b> ({@link CandleTableColumns}, the same stream
 *       {@link SignalDetectionFunction} consumes): update the completed-candle
 *       lookback, then settle any pending tentative for that window —
 *       rule still holds → <b>CONFIRM</b> (supersedes the tentative); rule
 *       fails → <b>CANCEL</b> (supersedes the tentative). A dangling tentative
 *       whose final never arrives (late-dropped) is CANCEL-led by an
 *       event-time timer at {@code windowEnd + allowedLateness}.</li>
 * </ol>
 *
 * <p>Correctness contracts (locked in the plan):
 * <ul>
 *   <li>The completed-candle lookback is the SAME {@link SignalLookbackState}
 *       helper as detection — one rule implementation, no duplication.</li>
 *   <li>Previews never enter {@link SignalDetectionFunction}'s ring buffers
 *       (this is a parallel operator; finals remain authoritative).</li>
 *   <li>Rows emitted here (tentative/confirm/cancel) BYPASS
 *       {@link ActiveSignalFeedbackFunction} — the max-one-active gate must
 *       not swallow the confirm because a tentative set ACTIVE first. They
 *       flow to the LOG candidates sink only; the KV current-state stays
 *       finals-only (authoritative).</li>
 *   <li>Supersession is auditable, never a silent drop: every tentative is
 *       eventually CONFIRMed, CANCELled, or (late-window) CANCEL-led.</li>
 * </ul>
 *
 * <p>State (keyed by instrument_token, checkpointed):
 * <ul>
 *   <li>{@link SignalLookbackState} — highs/closes of completed candles.</li>
 *   <li>{@code pendingTentative} MapState {@code windowStart → candidate_id}
 *       — the tentative awaiting settlement (bounded: one per in-flight window).</li>
 *   <li>{@code pendingTimer} ValueState — the single settlement timer ts.</li>
 * </ul>
 */
public class EarlySignalFunction
        extends KeyedCoProcessFunction<Long, RowData, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private static final MapStateDescriptor<Long, String> PENDING_DESC =
            new MapStateDescriptor<>("early-signal-pending-tentative",
                    Types.LONG, Types.STRING);
    /** Consecutive preview-hold count per window (Phase 3 confirm-window
     *  shortening: 4 consecutive 1s previews → confirm at ~5s). */
    private static final MapStateDescriptor<Long, Integer> HOLDS_DESC =
            new MapStateDescriptor<>("early-signal-preview-holds",
                    Types.LONG, Types.INT);
    private static final ValueStateDescriptor<Long> PENDING_TIMER_DESC =
            new ValueStateDescriptor<>("early-signal-pending-timer", Types.LONG);

    private final SignalJobConfig config;
    /**
     * Optional durable-marker hook for F4 crash reconciliation (CHG-121,
     * 2026-09-01). Null in embedded tests → the function behaves exactly as
     * before (marker paths are no-ops). In the cluster deployment this is a
     * {@link FlussTentativeMarkerStore} wrapper owned by the operator
     * lifecycle (open/close in SignalJob wiring).
     */
    // NOT transient (2026-09-02 root cause of the 01:43 drill F4 miss):
    // Flink ships the function from client to TaskManager by serialization;
    // a transient field arrives null on the TM and markers are silently
    // disabled in every cluster run (unit tests construct the function
    // directly, so they never caught it). The hook impl is Serializable;
    // its task-side resources (store/executor) are transient inside the impl.
    private final TentativeMarkerHook markerHook;

    /**
     * Durable tentative-marker operations; null = markers disabled.
     *
     * <p>ASYNC contract (2026-09-02 rework): all operations run on a
     * background thread and return futures — the task thread NEVER blocks
     * on Fluss I/O (the first synchronous version stalled the mailbox
     * during post-crash replay catch-up until checkpoint RPCs timed out;
     * drill tm-kill-full-load-20260902-005900). The function stages work
     * and drains completed futures on subsequent elements/timers of the
     * same key, so the durable-marker-before-LOG-row ordering is preserved.
     */
    public interface TentativeMarkerHook extends java.io.Serializable {
        /** Task-side init (called from EarlySignalFunction.open). */
        default void open() throws Exception {}

        /** Task-side release (called from EarlySignalFunction.close). */
        default void close() throws Exception {}

        /** Record the marker BEFORE the tentative LOG row is emitted. */
        CompletableFuture<Void> mark(String candidateId, long instrumentToken, long windowEnd);

        /** Marker lookup for the post-crash settle path. */
        CompletableFuture<Boolean> exists(String candidateId);

        /** Marker removal after settlement. */
        CompletableFuture<Void> clear(String candidateId);
    }

    /**
     * A tentative whose marker write is in flight. The LOG row is emitted
     * only AFTER the marker is durable (drain below); a failed marker
     * suppresses the tentative entirely (no orphan, no false signal — the
     * final-candle detection path is unaffected).
     */
    private static final class StagedMark {
        final String candidateId;
        final long token;
        final long windowStart;
        final long windowEnd;
        final String exchange;
        final String symbol;
        final long open;
        final long high;
        final long low;
        final long close;
        final long volume;
        CompletableFuture<Void> marker;

        StagedMark(String candidateId, long token, long windowStart, long windowEnd,
                String exchange, String symbol, long open, long high, long low, long close,
                long volume) {
            this.candidateId = candidateId;
            this.token = token;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.exchange = exchange;
            this.symbol = symbol;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
            this.volume = volume;
        }
    }

    /**
     * A post-crash settle whose marker lookup is in flight. The rule was
     * already evaluated on the task thread (correct lookback order); only
     * the marker-present gate and the row emission are deferred.
     */
    private static final class StagedSettle {
        final String candidateId;
        final long token;
        final long windowStart;
        final long windowEnd;
        final String exchange;
        final String symbol;
        final long open;
        final long high;
        final long low;
        final long close;
        final long volume;
        final boolean ruleHolds;
        /** Failed-lookup retry count (bounded by MAX_SETTLE_ATTEMPTS). */
        int attempts;
        CompletableFuture<Boolean> lookup;

        StagedSettle(String candidateId, long token, long windowStart, long windowEnd,
                String exchange, String symbol, long open, long high, long low, long close,
                long volume, boolean ruleHolds) {
            this.candidateId = candidateId;
            this.token = token;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.exchange = exchange;
            this.symbol = symbol;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
            this.volume = volume;
            this.ruleHolds = ruleHolds;
        }
    }

    /** Cap on in-flight staged marker ops per subtask (replay backstop). */
    private static final int STAGE_CAP = 4096;
    /** Processing-time drain timer: bounds emit latency on an idle key. */
    private static final long DRAIN_TIMER_DELAY_MS = 250;
    /**
     * Bounded retries for a failed reconcile lookup (drill 20260902-022843:
     * 4/4876 settles lost to a transient lookup failure — give-up-on-first-
     * failure was too brittle; retry keeps the F4 guarantee without
     * unbounded work).
     */
    private static final int MAX_SETTLE_ATTEMPTS = 3;

    private transient SignalLookbackState lookback;
    private transient MapState<Long, String> pending;
    private transient MapState<Long, Integer> holds;
    private transient ValueState<Long> pendingTimer;
    private transient Counter tentativeCounter;
    private transient Counter confirmCounter;
    private transient Counter cancelCounter;
    private transient Counter earlyConfirmCounter;
    private transient Counter markerDroppedCounter;
    /**
     * Attribution meters (2026-09-05): nanoseconds spent in the rule-check
     * vs in streak/pending saved-state ops, per preview row. Exported as
     * Flink counters so the split is readable in OpenObserve; the local
     * fields back the unit test. Production read happens in a soak —
     * behavior is unchanged, only observed.
     */
    private transient Counter lookbackNsCounter;
    private transient Counter streakStateNsCounter;
    private transient long lookbackNs;
    private transient long streakStateNs;
    private transient LinkedHashMap<String, StagedMark> stagedMarks;
    private transient LinkedHashMap<String, StagedSettle> stagedSettles;
    /**
     * Perf trim options 1+2 (2026-09-05): per-key memory copy of the
     * lookback ring buffers. The buffers change ONLY on completed finals
     * (~81/s); previews (~2,600/s) evaluate against the copy, so ~99% of
     * state reads disappear. Transient: after a restart the map is empty
     * and the first evaluation per key reloads from restored disk state
     * (same answers — the rule is pure, see SignalLookbackState.ruleHolds).
     * Key universe is the fixed instrument set, so the map is bounded.
     */
    private transient Map<Long, SignalLookbackState.Snapshot> lookbackCache;

    public EarlySignalFunction(SignalJobConfig config) {
        this(config, null);
    }

    public EarlySignalFunction(SignalJobConfig config, TentativeMarkerHook markerHook) {
        this.config = config;
        this.markerHook = markerHook;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        lookback = new SignalLookbackState(
                getRuntimeContext().getState(SignalLookbackState.highsDescriptor()),
                getRuntimeContext().getState(SignalLookbackState.closesDescriptor()),
                config.signalLookbackCandles());
        pending = getRuntimeContext().getMapState(PENDING_DESC);
        holds = getRuntimeContext().getMapState(HOLDS_DESC);
        pendingTimer = getRuntimeContext().getState(PENDING_TIMER_DESC);
        tentativeCounter =
                getRuntimeContext().getMetricGroup().counter("compute.signals.early.tentative");
        confirmCounter =
                getRuntimeContext().getMetricGroup().counter("compute.signals.early.confirmed");
        cancelCounter =
                getRuntimeContext().getMetricGroup().counter("compute.signals.early.cancelled");
        earlyConfirmCounter =
                getRuntimeContext().getMetricGroup().counter("compute.signals.early.confirmed_early");
        markerDroppedCounter =
                getRuntimeContext().getMetricGroup().counter("compute.signals.early.marker_dropped");
        lookbackNsCounter =
                getRuntimeContext().getMetricGroup().counter("compute.signals.early.lookback_ns");
        streakStateNsCounter =
                getRuntimeContext().getMetricGroup().counter("compute.signals.early.streak_state_ns");
        stagedMarks = new LinkedHashMap<>();
        stagedSettles = new LinkedHashMap<>();
        lookbackCache = new java.util.HashMap<>();
        if (markerHook != null) {
            markerHook.open();
        }
    }

    @Override
    public void close() throws Exception {
        if (markerHook != null) {
            markerHook.close();
        }
    }

    /**
     * Rule answer for one row: cache hit = zero state reads; miss = one
     * single-pass snapshot read ({@link SignalLookbackState#readSnapshot}).
     * Callers must drop the key from the cache after every
     * {@code lookback.append(...)} (completed finals only).
     */
    private boolean evaluateLookback(long token, long open, long close) throws Exception {
        SignalLookbackState.Snapshot snap = lookbackCache.get(token);
        if (snap == null) {
            snap = lookback.readSnapshot();
            lookbackCache.put(token, snap);
        }
        return lookback.evaluateSnapshot(open, close, snap);
    }

    /** Test probe: is this key's lookback currently served from memory? */
    boolean isLookbackCached(long token) {
        return lookbackCache != null && lookbackCache.containsKey(token);
    }

    /** Test probes for the attribution meters. */
    long lookbackNsTotal() {
        return lookbackNs;
    }

    long streakStateNsTotal() {
        return streakStateNs;
    }

    // --- Input 1: previews (tentative, once per window) ---
    @Override
    public void processElement1(RowData preview, Context ctx, Collector<RowData> out)
            throws Exception {
        drain(ctx.getCurrentKey(), ctx, out);
        long windowStart = preview.getLong(CandlePreviewColumns.WINDOW_START);
        long open = preview.getLong(CandlePreviewColumns.OPEN_PAISE);
        long close = preview.getLong(CandlePreviewColumns.CLOSE_PAISE);
        long windowEnd = preview.getLong(CandlePreviewColumns.WINDOW_END);
        long token = preview.getLong(CandlePreviewColumns.INSTRUMENT_TOKEN);
        long ruleStart = System.nanoTime();
        boolean ruleHoldsNow = evaluateLookback(token, open, close);
        long ruleDt = System.nanoTime() - ruleStart;
        lookbackNs += ruleDt;
        lookbackNsCounter.inc(ruleDt);
        // Everything below is streak/pending saved-state work (plus rare
        // row building on tentative/confirm paths only) — metered as one
        // block so every exit stays covered.
        long stateStart = System.nanoTime();
        try {
            if (!ruleHoldsNow) {
            // A failed preview resets the consecutive-hold streak even when a
            // tentative exists — "4 consecutive holds" must be truly
            // consecutive for the Phase 3 early confirm.
            // Perf trim (2026-09-05): this runs on EVERY failed preview
            // (thousands/s) but a streak entry exists only for windows
            // currently holding. A blind remove writes a tombstone to disk
            // even when nothing is stored; the guard turns the common
            // no-streak case into a cheap cache-friendly read. Semantically
            // identical: remove-if-present == blind remove.
            if (holds.contains(windowStart)) {
                holds.remove(windowStart);
            }
            return; // rule does not hold on this partial OHLCV
            }

        // Phase 3 confirm-window shortening: count consecutive holding
        // previews; at CONFIRM_AFTER_MS of sustained hold, confirm early.
        // Single read (was: two reads of the same key).
        Integer current = holds.get(windowStart);
        int streak = (current == null ? 0 : current) + 1;
        holds.put(windowStart, streak);
        long confirmAfterMs = config.earlySignalConfirmAfterMs();
        int needed = (int) Math.max(1, confirmAfterMs / config.previewIntervalMs());
        if (streak >= needed && pending.contains(windowStart)) {
            // Early confirm — the tentative is superseded at ~5s, no wait
            // for the window-end final.
            String tentativeId = pending.get(windowStart);
            pending.remove(windowStart);
            holds.remove(windowStart);
            clearSettlementTimer(ctx);
            if (markerHook != null) {
                // Every settle clears its marker (2026-09-02: only the
                // reconcile path cleared before — a normally-settled
                // tentative left a stale marker for 2d, and a replayed
                // final hitting it would emit a DUPLICATE settle).
                markerHook.clear(tentativeId);
            }
            earlyConfirmCounter.inc();
            out.collect(candidateRow(
                    token,
                    exchangeOf(preview, true),
                    symbolOf(preview, true),
                    windowStart,
                    windowEnd,
                    open,
                    preview.getLong(CandlePreviewColumns.HIGH_PAISE),
                    preview.getLong(CandlePreviewColumns.LOW_PAISE),
                    close,
                    preview.getLong(CandlePreviewColumns.VOLUME),
                    config.signalRuleId() + "-" + token + "-" + windowEnd + "-CONFIRM",
                    tentativeId,
                    SignalCandidatesTableColumns.VALIDITY_REASON_CONFIRMED,
                    SignalCandidatesTableColumns.ACTION_ENTRY,
                    "preview-early"));
            return;
        }

        if (pending.contains(windowStart)) {
            return; // tentative exists; streak not yet at confirm threshold
        }
        // First holding preview → tentative.
        String candidateId = config.signalRuleId() + "-" + token + "-" + windowEnd + "-TENTATIVE";
        if (markerHook != null) {
            // Durable marker BEFORE the LOG row leaves the operator — but
            // asynchronously: stage the tentative, emit it from drain()
            // only once the marker future completes. If the TM dies before
            // the next checkpoint, the marker (not the rolled-back pending
            // state) is what the post-crash final path consults.
            if (stagedMarks.containsKey(candidateId)) {
                return; // marker already in flight for this window
            }
            if (stagedMarks.size() >= STAGE_CAP) {
                // Replay backstop: drop the tentative (never emit a row
                // whose marker we cannot track) — degradation to "no early
                // signal for this window", final detection is unaffected.
                markerDroppedCounter.inc();
                return;
            }
            StagedMark staged = new StagedMark(candidateId, token, windowStart, windowEnd,
                    exchangeOf(preview, true), symbolOf(preview, true),
                    openOf(preview, true), highOf(preview, true), lowOf(preview, true),
                    close, volumeOf(preview, true));
            staged.marker = markerHook.mark(candidateId, token, windowEnd);
            stagedMarks.put(candidateId, staged);
            ctx.timerService().registerProcessingTimeTimer(
                    ctx.timerService().currentProcessingTime() + DRAIN_TIMER_DELAY_MS);
            return;
        }
        pending.put(windowStart, candidateId);
        registerSettlementTimer(ctx, windowEnd);
        tentativeCounter.inc();
        out.collect(candidateRow(
                token,
                exchangeOf(preview, true),
                symbolOf(preview, true),
                windowStart,
                windowEnd,
                openOf(preview, true),
                highOf(preview, true),
                lowOf(preview, true),
                close,
                volumeOf(preview, true),
                candidateId,
                null,
                SignalCandidatesTableColumns.VALIDITY_REASON_TENTATIVE,
                SignalCandidatesTableColumns.ACTION_ENTRY,
                "preview"));
        } finally {
            long stateDt = System.nanoTime() - stateStart;
            streakStateNs += stateDt;
            streakStateNsCounter.inc(stateDt);
        }
    }

    // --- Input 2: finals (lookback update + settle tentative) ---
    @Override
    public void processElement2(RowData finalCandle, Context ctx, Collector<RowData> out)
            throws Exception {
        drain(ctx.getCurrentKey(), ctx, out);
        long windowStart = finalCandle.getLong(CandleTableColumns.WINDOW_START);
        long windowEnd = finalCandle.getLong(CandleTableColumns.WINDOW_END);
        long open = finalCandle.getLong(CandleTableColumns.OPEN_PAISE);
        long high = finalCandle.getLong(CandleTableColumns.HIGH_PAISE);
        long close = finalCandle.getLong(CandleTableColumns.CLOSE_PAISE);

        String tentativeId = pending.get(windowStart);
        if (tentativeId == null) {
            // No in-state tentative for this window. Two possibilities:
            // (a) none was ever emitted — silent as before;
            // (b) F4 crash path: a tentative WAS emitted pre-crash, the LOG
            //     row survived, but this pending state rolled back to the
            //     last checkpoint. The durable marker table distinguishes:
            //     marker present → reconcile (settle by the FINAL rule and
            //     clear the marker); marker absent → case (a).
            if (markerHook != null) {
                long token2 = finalCandle.getLong(CandleTableColumns.INSTRUMENT_TOKEN);
                String candidateId =
                        config.signalRuleId() + "-" + token2 + "-" + windowEnd + "-TENTATIVE";
                if (!stagedSettles.containsKey(candidateId)) {
                    if (stagedSettles.size() >= STAGE_CAP) {
                        // Replay backstop: skip this reconcile lookup — the
                        // worst case is one more F4 orphan, never a false
                        // signal (no marker → no row).
                        markerDroppedCounter.inc();
                    } else {
                        // Evaluate BEFORE appending (same strictly-before
                        // semantics as the normal settle path below) — the
                        // rule needs the task-thread lookback state, so it
                        // runs now; only the marker lookup and the row
                        // emission are deferred to drain().
                        boolean ruleHolds = evaluateLookback(token2, open, close);
                        StagedSettle staged = new StagedSettle(candidateId, token2,
                                windowStart, windowEnd,
                                exchangeOf(finalCandle, false),
                                symbolOf(finalCandle, false),
                                open, high,
                                finalCandle.getLong(CandleTableColumns.LOW_PAISE),
                                close,
                                finalCandle.getLong(CandleTableColumns.VOLUME),
                                ruleHolds);
                        staged.lookup = markerHook.exists(candidateId);
                        stagedSettles.put(candidateId, staged);
                        ctx.timerService().registerProcessingTimeTimer(
                                ctx.timerService().currentProcessingTime()
                                        + DRAIN_TIMER_DELAY_MS);
                    }
                }
            }
            // The completed candle still enters the lookback (ring buffers
            // always track finals) — both for case (a) and post-reconcile.
            lookback.append(high, close);
            lookbackCache.remove(finalCandle.getLong(CandleTableColumns.INSTRUMENT_TOKEN));
            return;
        }
        // Evaluate BEFORE appending: the rule compares against the PREVIOUS
        // completed candles only (strictly-before semantics, same as
        // SignalDetectionFunction, which evaluates then appends).
        long settleToken = finalCandle.getLong(CandleTableColumns.INSTRUMENT_TOKEN);
        boolean holds = evaluateLookback(settleToken, open, close);
        lookback.append(high, close);
        lookbackCache.remove(settleToken);
        pending.remove(windowStart);
        clearSettlementTimer(ctx);
        if (markerHook != null) {
            markerHook.clear(tentativeId); // fire-and-forget
        }
        long token = finalCandle.getLong(CandleTableColumns.INSTRUMENT_TOKEN);
        if (holds) {
            confirmCounter.inc();
            out.collect(candidateRow(
                    token,
                    exchangeOf(finalCandle, false),
                    symbolOf(finalCandle, false),
                    windowStart,
                    windowEnd,
                    open,
                    high,
                    finalCandle.getLong(CandleTableColumns.LOW_PAISE),
                    close,
                    finalCandle.getLong(CandleTableColumns.VOLUME),
                    config.signalRuleId() + "-" + token + "-" + windowEnd + "-CONFIRM",
                    tentativeId,
                    SignalCandidatesTableColumns.VALIDITY_REASON_CONFIRMED,
                    SignalCandidatesTableColumns.ACTION_ENTRY,
                    "final"));
        } else {
            cancelCounter.inc();
            out.collect(candidateRow(
                    token,
                    exchangeOf(finalCandle, false),
                    symbolOf(finalCandle, false),
                    windowStart,
                    windowEnd,
                    open,
                    high,
                    finalCandle.getLong(CandleTableColumns.LOW_PAISE),
                    close,
                    finalCandle.getLong(CandleTableColumns.VOLUME),
                    config.signalRuleId() + "-" + token + "-" + windowEnd + "-CANCEL",
                    tentativeId,
                    SignalCandidatesTableColumns.VALIDITY_REASON_SUPERSEDED,
                    SignalCandidatesTableColumns.ACTION_CANCEL,
                    "final"));
        }
    }

    // --- Timer: settle a dangling tentative whose final never arrived ---
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out)
            throws Exception {
        // Processing-time drain timers land here too — always drain first
        // (drain timers never match pendingTimer, so the stale check below
        // simply returns for them).
        drain(ctx.getCurrentKey(), ctx, out);
        Long t = pendingTimer.value();
        if (t == null || t != timestamp) {
            return; // stale timer (final settled the window first)
        }
        pendingTimer.clear();
        for (Map.Entry<Long, String> e : pending.entries()) {
            long windowStart = e.getKey();
            long windowEnd = windowStart + config.candleWindowMs();
            if (windowEnd <= timestamp) {
                cancelCounter.inc();
                String tentativeId = e.getValue();
                pending.remove(windowStart);
                if (markerHook != null) {
                    markerHook.clear(tentativeId); // fire-and-forget
                }
                out.collect(candidateRow(
                        ctx.getCurrentKey(),
                        null,
                        null,
                        windowStart,
                        windowEnd,
                        0L, 0L, 0L, 0L, 0L,
                        config.signalRuleId() + "-" + ctx.getCurrentKey() + "-"
                                + windowEnd + "-CANCEL",
                        tentativeId,
                        SignalCandidatesTableColumns.VALIDITY_REASON_SUPERSEDED,
                        SignalCandidatesTableColumns.ACTION_CANCEL,
                        "late-drop"));
            }
        }
    }

    /**
     * Complete staged marker work for the CURRENT key only (keyed-state and
     * timer access are only valid for the key whose element/timer we are
     * processing). Completed futures are applied in staging order; pending
     * futures stay staged until the next drain.
     */
    private void drain(long currentKey, Context ctx, Collector<RowData> out) throws Exception {
        if (markerHook == null || (stagedMarks.isEmpty() && stagedSettles.isEmpty())) {
            return;
        }
        if (!stagedMarks.isEmpty()) {
            Iterator<Map.Entry<String, StagedMark>> it = stagedMarks.entrySet().iterator();
            while (it.hasNext()) {
                StagedMark m = it.next().getValue();
                if (m.token != currentKey || !m.marker.isDone()) {
                    continue;
                }
                it.remove();
                try {
                    m.marker.get();
                } catch (Exception e) {
                    // Marker write failed → suppress the tentative. No LOG
                    // row, no pending state: nothing to orphan, no false
                    // settle. The window's final-candle detection path is
                    // unaffected.
                    markerDroppedCounter.inc();
                    continue;
                }
                pending.put(m.windowStart, m.candidateId);
                registerSettlementTimer(ctx, m.windowEnd);
                tentativeCounter.inc();
                out.collect(candidateRow(
                        m.token, m.exchange, m.symbol, m.windowStart, m.windowEnd,
                        m.open, m.high, m.low, m.close, m.volume,
                        m.candidateId, null,
                        SignalCandidatesTableColumns.VALIDITY_REASON_TENTATIVE,
                        SignalCandidatesTableColumns.ACTION_ENTRY,
                        "preview"));
            }
        }
        if (!stagedSettles.isEmpty()) {
            Iterator<Map.Entry<String, StagedSettle>> it = stagedSettles.entrySet().iterator();
            while (it.hasNext()) {
                StagedSettle s = it.next().getValue();
                if (s.token != currentKey || !s.lookup.isDone()) {
                    continue;
                }
                Boolean present;
                try {
                    present = s.lookup.get();
                } catch (Exception e) {
                    // Lookup failed → bounded retry, then give up (worst
                    // case one more F4 orphan; never a false signal). A
                    // single transient failure (drill 20260902-022843: 4
                    // settles lost to 5s lookup timeouts) must not orphan
                    // a tentative.
                    s.attempts++;
                    if (s.attempts < MAX_SETTLE_ATTEMPTS) {
                        s.lookup = markerHook.exists(s.candidateId);
                        ctx.timerService().registerProcessingTimeTimer(
                                ctx.timerService().currentProcessingTime()
                                        + DRAIN_TIMER_DELAY_MS);
                    } else {
                        it.remove();
                        markerDroppedCounter.inc();
                    }
                    continue;
                }
                it.remove();
                if (!Boolean.TRUE.equals(present)) {
                    continue; // no marker → case (a): silent as before
                }
                markerHook.clear(s.candidateId); // fire-and-forget
                if (s.ruleHolds) {
                    confirmCounter.inc();
                    out.collect(candidateRow(
                            s.token, s.exchange, s.symbol, s.windowStart, s.windowEnd,
                            s.open, s.high, s.low, s.close, s.volume,
                            s.candidateId.replace("-TENTATIVE", "-CONFIRM"),
                            s.candidateId,
                            SignalCandidatesTableColumns.VALIDITY_REASON_CONFIRMED,
                            SignalCandidatesTableColumns.ACTION_ENTRY,
                            "final-reconciled"));
                } else {
                    cancelCounter.inc();
                    out.collect(candidateRow(
                            s.token, s.exchange, s.symbol, s.windowStart, s.windowEnd,
                            s.open, s.high, s.low, s.close, s.volume,
                            s.candidateId.replace("-TENTATIVE", "-CANCEL"),
                            s.candidateId,
                            SignalCandidatesTableColumns.VALIDITY_REASON_SUPERSEDED,
                            SignalCandidatesTableColumns.ACTION_CANCEL,
                            "final-reconciled"));
                }
            }
        }
    }

    private void registerSettlementTimer(Context ctx, long windowEnd) throws Exception {
        long fireAt = windowEnd + config.allowedLatenessMs();
        Long existing = pendingTimer.value();
        if (existing != null && existing == fireAt) {
            return; // already armed for this window
        }
        if (existing != null) {
            ctx.timerService().deleteEventTimeTimer(existing);
        }
        ctx.timerService().registerEventTimeTimer(fireAt);
        pendingTimer.update(fireAt);
    }

    private void clearSettlementTimer(Context ctx) throws Exception {
        Long t = pendingTimer.value();
        if (t != null) {
            ctx.timerService().deleteEventTimeTimer(t);
            pendingTimer.clear();
        }
    }

    private RowData candidateRow(long token, String exchange, String symbol,
            long windowStart, long windowEnd, long open, long high, long low, long close,
            long volume, String candidateId, String supersedes, String validityReason,
            String action, String sourceKind) {
        GenericRowData row = new GenericRowData(SignalCandidatesTableColumns.FIELD_COUNT);
        row.setField(SignalCandidatesTableColumns.CANDIDATE_ID, StringData.fromString(candidateId));
        row.setField(SignalCandidatesTableColumns.INSTRUCTION_ID, null);
        row.setField(SignalCandidatesTableColumns.TRADE_CONTEXT_ID, null);
        row.setField(SignalCandidatesTableColumns.INSTRUMENT_TOKEN, token);
        row.setField(SignalCandidatesTableColumns.EXCHANGE,
                exchange == null ? null : StringData.fromString(exchange));
        row.setField(SignalCandidatesTableColumns.SYMBOL,
                symbol == null ? null : StringData.fromString(symbol));
        row.setField(SignalCandidatesTableColumns.STRATEGY_ID,
                StringData.fromString(config.signalStrategyId()));
        row.setField(SignalCandidatesTableColumns.STRATEGY_VERSION,
                StringData.fromString(config.signalStrategyVersion()));
        row.setField(SignalCandidatesTableColumns.RULE_ID,
                StringData.fromString(config.signalRuleId()));
        row.setField(SignalCandidatesTableColumns.DETECTION_TS, windowEnd);
        row.setField(SignalCandidatesTableColumns.EVALUATION_TS, windowEnd);
        row.setField(SignalCandidatesTableColumns.ACTION, StringData.fromString(action));
        row.setField(SignalCandidatesTableColumns.SIDE,
                StringData.fromString(SignalCandidatesTableColumns.SIDE_BUY));
        row.setField(SignalCandidatesTableColumns.QUANTITY, config.signalQuantity());
        row.setField(SignalCandidatesTableColumns.ORDER_TYPE,
                StringData.fromString(SignalCandidatesTableColumns.ORDER_TYPE_MARKET));
        row.setField(SignalCandidatesTableColumns.LIMIT_PRICE_PAISE, null);
        row.setField(SignalCandidatesTableColumns.SCORE_INPUTS, null);
        row.setField(SignalCandidatesTableColumns.FORMATION_SNAPSHOT_REF,
                StringData.fromString("source=" + sourceKind + ":candle:" + windowStart + ":"
                        + windowEnd + ":open=" + open + ":high=" + high + ":low=" + low
                        + ":close=" + close + ":volume=" + volume));
        row.setField(SignalCandidatesTableColumns.VALIDITY_REASON,
                StringData.fromString(validityReason));
        row.setField(SignalCandidatesTableColumns.SUPERSEDES_CANDIDATE_ID,
                supersedes == null ? null : StringData.fromString(supersedes));
        row.setField(SignalCandidatesTableColumns.SUPERSEDED_BY_CANDIDATE_ID, null);
        row.setField(SignalCandidatesTableColumns.SCHEMA_VERSION,
                StringData.fromString(SignalCandidatesTableColumns.SCHEMA_VERSION_V2));
        return row;
    }

    private static String exchangeOf(RowData row, boolean preview) {
        return row.getString(preview ? CandlePreviewColumns.EXCHANGE
                : CandleTableColumns.EXCHANGE).toString();
    }

    private static String symbolOf(RowData row, boolean preview) {
        return row.getString(preview ? CandlePreviewColumns.SYMBOL
                : CandleTableColumns.SYMBOL).toString();
    }

    private static long openOf(RowData row, boolean preview) {
        return row.getLong(preview ? CandlePreviewColumns.OPEN_PAISE
                : CandleTableColumns.OPEN_PAISE);
    }

    private static long highOf(RowData row, boolean preview) {
        return row.getLong(preview ? CandlePreviewColumns.HIGH_PAISE
                : CandleTableColumns.HIGH_PAISE);
    }

    private static long lowOf(RowData row, boolean preview) {
        return row.getLong(preview ? CandlePreviewColumns.LOW_PAISE
                : CandleTableColumns.LOW_PAISE);
    }

    private static long volumeOf(RowData row, boolean preview) {
        return row.getLong(preview ? CandlePreviewColumns.VOLUME
                : CandleTableColumns.VOLUME);
    }
}
