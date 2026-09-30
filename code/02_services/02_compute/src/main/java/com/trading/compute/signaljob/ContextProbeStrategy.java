package com.trading.compute.signaljob;

import com.trading.compute.feature.FeatureView;
import java.util.Map;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;

/**
 * Live-candle context probe (C3,
 * docs/plans/2026-09-30-strategy-context-live-fetch.md): a test-only consumer
 * proving that old data fetched asynchronously on demand drives a decision on
 * the live forming candle — never on the close.
 *
 * <p>Registered in {@link Strategies} but inert unless the config
 * {@code STRATEGIES} list names {@code ctx-probe-v1}; no production config
 * lists it. It is not a trading rule.
 *
 * <p>Behavior: on the live forming row of window {@code W} (the host's fast
 * feed carries the smallest TF) it asks the context view for the previous
 * window of that same TF ({@code W − tf.windowMs()}). A miss emits nothing (the
 * fetch is scheduled, single-flight) and is retried on later ticks; when the
 * value is ready — a later live tick of a forming window (cache hit) or the
 * host's context-ready wake-up — it emits one {@code Signal_Candidates} row
 * whose deterministic id embeds the token and the fetched window. Repeat
 * evaluations re-emit the same id and the host suppresses them. A snapshot at
 * or past its window close never emits (counted) — the hard constraint of the
 * plan.
 */
public final class ContextProbeStrategy implements SignalStrategy {

    private static final long serialVersionUID = 1L;

    /** Registered (but opt-in via {@code STRATEGIES}) rule id of the probe. */
    public static final String RULE_ID = "ctx-probe-v1";

    static final String MISSES = "misses";
    static final String READY_BEFORE_CLOSE = "ready_before_close";
    static final String READY_AFTER_CLOSE = "ready_after_close";

    private final SignalJobConfig config;
    private final Metrics metrics;

    private long token = -1L;
    private String exchange;
    private String symbol;
    private long misses;
    private long readyBeforeClose;
    private long readyAfterClose;
    /** One window per TF: the last window already emitted (bounded, max 6). */
    private transient Map<Timeframe, Long> lastEmitted;

    public ContextProbeStrategy(SignalJobConfig config, Metrics metrics) {
        this.config = config;
        this.metrics = metrics;
    }

    @Override
    public String ruleId() {
        return RULE_ID;
    }

    /** The probe never fires on a completed candle (the hard constraint). */
    @Override
    public void onClosedCandle(RowData closed, Collector<RowData> out) {}

    /** The context-aware live form is the only entry point the host uses. */
    @Override
    public void onLiveTick(RowData live, Collector<RowData> out) {}

    @Override
    public void onLiveTick(RowData live, ContextView context, FeatureView features,
            Collector<RowData> out) {
        evaluate(live, context, out);
    }

    @Override
    public void onContextReady(RowData live, ContextView context, FeatureView features,
            Collector<RowData> out) {
        evaluate(live, context, out);
    }

    private void evaluate(RowData live, ContextView context, Collector<RowData> out) {
        token = live.getLong(CandleLiveColumns.INSTRUMENT_TOKEN);
        if (!live.isNullAt(CandleLiveColumns.EXCHANGE)) {
            exchange = live.getString(CandleLiveColumns.EXCHANGE).toString();
        }
        if (!live.isNullAt(CandleLiveColumns.SYMBOL)) {
            symbol = live.getString(CandleLiveColumns.SYMBOL).toString();
        }
        // The host's fast live feed carries the smallest-TF forming row
        // (MultiTimeframeAggregateFunction.LIVE_TICK_TAG), so the probe asks
        // for the previous window of whatever TF it actually sees.
        Timeframe tf = Timeframe.fromCode(live.getString(CandleLiveColumns.TF).toString());
        long windowStart = live.getLong(CandleLiveColumns.WINDOW_START);
        long windowEnd = live.getLong(CandleLiveColumns.WINDOW_END);
        long lastEventTime = live.getLong(CandleLiveColumns.LAST_EVENT_TIME);
        long previous = windowStart - tf.windowMs();

        ContextCandle candle = context.candle(token, tf, previous);
        if (candle == null) {
            misses++;
            metrics.inc(MISSES, 1);
            return;
        }
        if (lastEventTime >= windowEnd) {
            // The live snapshot is already at/past its close: never the moment
            // a context-driven decision may fire.
            readyAfterClose++;
            metrics.inc(READY_AFTER_CLOSE, 1);
            return;
        }
        readyBeforeClose++;
        metrics.inc(READY_BEFORE_CLOSE, 1);
        if (lastEmitted == null) {
            lastEmitted = new java.util.EnumMap<>(Timeframe.class);
        }
        Long last = lastEmitted.get(tf);
        if (last != null && last == previous) {
            return; // one logical signal per (token, window)
        }
        lastEmitted.put(tf, previous);
        out.collect(buildRow(tf, windowStart, previous, candle, lastEventTime));
    }

    private GenericRowData buildRow(Timeframe tf, long windowStart, long previous,
            ContextCandle candle, long detectionTs) {
        GenericRowData row = new GenericRowData(SignalCandidatesTableColumns.FIELD_COUNT);
        row.setField(SignalCandidatesTableColumns.CANDIDATE_ID,
                StringData.fromString(candidateIdFor(token, tf, previous)));
        row.setField(SignalCandidatesTableColumns.INSTRUCTION_ID, null);
        row.setField(SignalCandidatesTableColumns.TRADE_CONTEXT_ID, null);
        row.setField(SignalCandidatesTableColumns.INSTRUMENT_TOKEN, token);
        row.setField(SignalCandidatesTableColumns.EXCHANGE,
                StringData.fromString(exchange != null ? exchange : "NSE"));
        row.setField(SignalCandidatesTableColumns.SYMBOL,
                StringData.fromString(symbol != null ? symbol : "UNKNOWN"));
        row.setField(SignalCandidatesTableColumns.STRATEGY_ID,
                StringData.fromString(config.signalStrategyId()));
        row.setField(SignalCandidatesTableColumns.STRATEGY_VERSION,
                StringData.fromString(config.signalStrategyVersion()));
        row.setField(SignalCandidatesTableColumns.RULE_ID, StringData.fromString(RULE_ID));
        row.setField(SignalCandidatesTableColumns.DETECTION_TS, detectionTs);
        row.setField(SignalCandidatesTableColumns.EVALUATION_TS, System.currentTimeMillis());
        row.setField(SignalCandidatesTableColumns.ACTION,
                StringData.fromString(SignalCandidatesTableColumns.ACTION_ENTRY));
        row.setField(SignalCandidatesTableColumns.SIDE,
                StringData.fromString(SignalCandidatesTableColumns.SIDE_BUY));
        row.setField(SignalCandidatesTableColumns.QUANTITY, config.signalQuantity());
        row.setField(SignalCandidatesTableColumns.ORDER_TYPE,
                StringData.fromString(SignalCandidatesTableColumns.ORDER_TYPE_MARKET));
        row.setField(SignalCandidatesTableColumns.LIMIT_PRICE_PAISE, null);
        row.setField(SignalCandidatesTableColumns.SCORE_INPUTS, StringData.fromString(
                "{\"tf\":\"" + tf.code() + "\",\"windowStart\":" + windowStart
                        + ",\"prevWindowStart\":" + previous
                        + ",\"prevClosePaise\":" + candle.closePaise()
                        + ",\"prevHighPaise\":" + candle.highPaise()
                        + ",\"prevLowPaise\":" + candle.lowPaise() + "}"));
        row.setField(SignalCandidatesTableColumns.FORMATION_SNAPSHOT_REF, StringData.fromString(
                "ctx-probe:" + tf.code() + ":" + windowStart + ":prev=" + previous
                        + ":prevClose=" + candle.closePaise() + ":liveTs=" + detectionTs));
        row.setField(SignalCandidatesTableColumns.VALIDITY_REASON,
                StringData.fromString(SignalCandidatesTableColumns.VALIDITY_REASON_VALID));
        row.setField(SignalCandidatesTableColumns.SUPERSEDES_CANDIDATE_ID, null);
        row.setField(SignalCandidatesTableColumns.SUPERSEDED_BY_CANDIDATE_ID, null);
        row.setField(SignalCandidatesTableColumns.SCHEMA_VERSION,
                StringData.fromString(SignalCandidatesTableColumns.SCHEMA_VERSION_V2));
        return row;
    }

    /**
     * Deterministic id: one logical signal per (token, fetched window); embeds
     * the instrument token (P2-176) so host dedup stays per-key equivalent.
     */
    static String candidateIdFor(long token, Timeframe tf, long previousWindowStart) {
        return RULE_ID + ":" + token + ":" + tf.code() + ":" + previousWindowStart;
    }

    long missesForTest() {
        return misses;
    }

    long readyBeforeCloseForTest() {
        return readyBeforeClose;
    }

    long readyAfterCloseForTest() {
        return readyAfterClose;
    }
}
