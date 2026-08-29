package com.trading.compute.signaljob;

import java.util.Map;
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

    private transient SignalLookbackState lookback;
    private transient MapState<Long, String> pending;
    private transient MapState<Long, Integer> holds;
    private transient ValueState<Long> pendingTimer;
    private transient Counter tentativeCounter;
    private transient Counter confirmCounter;
    private transient Counter cancelCounter;
    private transient Counter earlyConfirmCounter;

    public EarlySignalFunction(SignalJobConfig config) {
        this.config = config;
    }

    @Override
    public void open(OpenContext openContext) {
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
    }

    // --- Input 1: previews (tentative, once per window) ---
    @Override
    public void processElement1(RowData preview, Context ctx, Collector<RowData> out)
            throws Exception {
        long windowStart = preview.getLong(CandlePreviewColumns.WINDOW_START);
        long open = preview.getLong(CandlePreviewColumns.OPEN_PAISE);
        long close = preview.getLong(CandlePreviewColumns.CLOSE_PAISE);
        long windowEnd = preview.getLong(CandlePreviewColumns.WINDOW_END);
        long token = preview.getLong(CandlePreviewColumns.INSTRUMENT_TOKEN);
        if (!lookback.evaluate(open, close)) {
            // A failed preview resets the consecutive-hold streak even when a
            // tentative exists — "4 consecutive holds" must be truly
            // consecutive for the Phase 3 early confirm.
            holds.remove(windowStart);
            return; // rule does not hold on this partial OHLCV
        }

        // Phase 3 confirm-window shortening: count consecutive holding
        // previews; at CONFIRM_AFTER_MS of sustained hold, confirm early.
        int streak = (holds.get(windowStart) == null ? 0 : holds.get(windowStart)) + 1;
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
    }

    // --- Input 2: finals (lookback update + settle tentative) ---
    @Override
    public void processElement2(RowData finalCandle, Context ctx, Collector<RowData> out)
            throws Exception {
        long windowStart = finalCandle.getLong(CandleTableColumns.WINDOW_START);
        long windowEnd = finalCandle.getLong(CandleTableColumns.WINDOW_END);
        long open = finalCandle.getLong(CandleTableColumns.OPEN_PAISE);
        long high = finalCandle.getLong(CandleTableColumns.HIGH_PAISE);
        long close = finalCandle.getLong(CandleTableColumns.CLOSE_PAISE);

        String tentativeId = pending.get(windowStart);
        if (tentativeId == null) {
            // No tentative for this window — the completed candle still
            // enters the lookback (ring buffers always track finals).
            lookback.append(high, close);
            return;
        }
        // Evaluate BEFORE appending: the rule compares against the PREVIOUS
        // completed candles only (strictly-before semantics, same as
        // SignalDetectionFunction, which evaluates then appends).
        boolean holds = lookback.evaluate(open, close);
        lookback.append(high, close);
        pending.remove(windowStart);
        clearSettlementTimer(ctx);
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
