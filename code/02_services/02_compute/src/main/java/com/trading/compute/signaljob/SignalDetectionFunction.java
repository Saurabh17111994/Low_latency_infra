package com.trading.compute.signaljob;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;

/**
 * MVP signal detection (docs/08_implementation/04-signal-job.md §Slice 2.1,
 * DEC-034): consumes closed {@code feature_candles_15s} candle rows, keyed by
 * {@code instrument_token}, and emits one {@code Signal_Candidates} row per
 * fired signal. Ranking/reservations/decisions are postponed — a fired signal
 * is appended as an immutable candidate record and nothing else happens
 * downstream in this phase.
 *
 * <p><b>Rule v1 — "20-candle breakout"</b> (placeholder strategy; the user's
 * real trading logic replaces it later via {@code rule_id}/{@code strategy_id}
 * without any pipeline change). Evaluated on each completed 15-second candle:
 * <ol>
 *   <li><b>Bullish</b>: {@code close > open} (strict; a flat candle never fires).</li>
 *   <li><b>Breakout</b>: {@code close > max(high of the previous
 *       {@code SIGNAL_LOOKBACK_CANDLES} completed candles)} (strict).</li>
 *   <li><b>Trend filter</b>: {@code close > mean(close of the previous
 *       {@code SIGNAL_LOOKBACK_CANDLES} completed candles)} — exact integer
 *       comparison {@code close * n > sum}, no rounding.</li>
 * </ol>
 * No signal before {@code lookback} completed candles exist per instrument
 * (warm-up); conditions 2/3 use only candles strictly before the signal
 * candle. Every fired signal gets a fresh {@code candidate_id}
 * ({@code rule_id-instrument_token-window_end}) — unique because one candle
 * closes per (instrument, window_end).
 *
 * <p>Keyed state: two bounded ring buffers (highs, closes) of the last
 * {@code lookback} completed candles per instrument, held in the shared
 * {@link SignalLookbackState} helper (Phase 2, 2026-08-29 — same descriptors
 * as before, restore-compatible; the early-signal path reuses the same
 * helper). Candidate lifecycle (max-one-active, supersession, expiry) is
 * intentionally NOT implemented in this function — the active-signal gate is
 * a separate operator.
 */
public class SignalDetectionFunction extends KeyedProcessFunction<Long, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private final SignalJobConfig config;

    private transient SignalLookbackState lookback;
    private transient Counter detectedCounter;

    public SignalDetectionFunction(SignalJobConfig config) {
        this.config = config;
    }

    @Override
    public void open(OpenContext openContext) {
        lookback = new SignalLookbackState(
                getRuntimeContext().getState(SignalLookbackState.highsDescriptor()),
                getRuntimeContext().getState(SignalLookbackState.closesDescriptor()),
                config.signalLookbackCandles());
        detectedCounter = getRuntimeContext().getMetricGroup().counter("compute.signals.detected");
    }

    @Override
    public void processElement(RowData candle, Context ctx, Collector<RowData> out)
            throws Exception {
        long instrumentToken = candle.getLong(CandleTableColumns.INSTRUMENT_TOKEN);
        long windowStart = candle.getLong(CandleTableColumns.WINDOW_START);
        long windowEnd = candle.getLong(CandleTableColumns.WINDOW_END);
        long open = candle.getLong(CandleTableColumns.OPEN_PAISE);
        long high = candle.getLong(CandleTableColumns.HIGH_PAISE);
        long low = candle.getLong(CandleTableColumns.LOW_PAISE);
        long close = candle.getLong(CandleTableColumns.CLOSE_PAISE);
        long volume = candle.getLong(CandleTableColumns.VOLUME);
        String exchange = candle.getString(CandleTableColumns.EXCHANGE).toString();
        String symbol = candle.getString(CandleTableColumns.SYMBOL).toString();

        boolean fired = lookback.evaluate(open, close);
        // Ring buffers over completed candles: append current, drop oldest beyond lookback.
        lookback.append(high, close);

        if (fired) {
            detectedCounter.inc();
            out.collect(toCandidate(instrumentToken, exchange, symbol, windowStart, windowEnd,
                    open, high, low, close, volume));
        }
    }

    private RowData toCandidate(long instrumentToken, String exchange, String symbol,
            long windowStart, long windowEnd, long open, long high, long low, long close,
            long volume) {
        GenericRowData row = new GenericRowData(SignalCandidatesTableColumns.FIELD_COUNT);
        String candidateId = config.signalRuleId() + "-" + instrumentToken + "-" + windowEnd;
        row.setField(SignalCandidatesTableColumns.CANDIDATE_ID, StringData.fromString(candidateId));
        row.setField(SignalCandidatesTableColumns.INSTRUCTION_ID, null);
        row.setField(SignalCandidatesTableColumns.TRADE_CONTEXT_ID, null);
        row.setField(SignalCandidatesTableColumns.INSTRUMENT_TOKEN, instrumentToken);
        row.setField(SignalCandidatesTableColumns.EXCHANGE, StringData.fromString(exchange));
        row.setField(SignalCandidatesTableColumns.SYMBOL, StringData.fromString(symbol));
        row.setField(SignalCandidatesTableColumns.STRATEGY_ID, StringData.fromString(config.signalStrategyId()));
        row.setField(SignalCandidatesTableColumns.STRATEGY_VERSION, StringData.fromString(config.signalStrategyVersion()));
        row.setField(SignalCandidatesTableColumns.RULE_ID, StringData.fromString(config.signalRuleId()));
        row.setField(SignalCandidatesTableColumns.DETECTION_TS, windowEnd);
        row.setField(SignalCandidatesTableColumns.EVALUATION_TS, windowEnd);
        row.setField(SignalCandidatesTableColumns.ACTION, StringData.fromString(SignalCandidatesTableColumns.ACTION_ENTRY));
        row.setField(SignalCandidatesTableColumns.SIDE, StringData.fromString(SignalCandidatesTableColumns.SIDE_BUY));
        row.setField(SignalCandidatesTableColumns.QUANTITY, config.signalQuantity());
        row.setField(SignalCandidatesTableColumns.ORDER_TYPE, StringData.fromString(SignalCandidatesTableColumns.ORDER_TYPE_MARKET));
        row.setField(SignalCandidatesTableColumns.LIMIT_PRICE_PAISE, null);
        row.setField(SignalCandidatesTableColumns.SCORE_INPUTS, null);
        row.setField(SignalCandidatesTableColumns.FORMATION_SNAPSHOT_REF,
                StringData.fromString("candle:" + windowStart + ":" + windowEnd + ":open=" + open
                        + ":high=" + high + ":low=" + low + ":close=" + close + ":volume=" + volume));
        row.setField(SignalCandidatesTableColumns.VALIDITY_REASON,
                StringData.fromString(SignalCandidatesTableColumns.VALIDITY_REASON_VALID));
        row.setField(SignalCandidatesTableColumns.SUPERSEDES_CANDIDATE_ID, null);
        row.setField(SignalCandidatesTableColumns.SUPERSEDED_BY_CANDIDATE_ID, null);
        row.setField(SignalCandidatesTableColumns.SCHEMA_VERSION,
                StringData.fromString(SignalCandidatesTableColumns.SCHEMA_VERSION_V2));
        return row;
    }
}
