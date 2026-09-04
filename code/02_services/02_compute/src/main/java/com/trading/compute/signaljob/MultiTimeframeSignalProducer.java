package com.trading.compute.signaljob;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;

/**
 * Phase 3 TRACK B — signal-context consumer.
 *
 * <p>Converts {@link MultiTimeframeSignalContext} (the aggregator's
 * {@link MultiTimeframeAggregateFunction#SIGNAL_TAG} side-output) into
 * full-detail {@code Signal_Candidates} rows. Design §A Decisions 6,7,8,9:
 * signals consume forming per-tick (Decision 6, side-output, zero Fluss reads),
 * act immediately on forming (Decision 7, Option A, no wait for close),
 * keep every signal forever as append-only LOG+KV (Decision 8, never retracted
 * — ACCEPTED-BY-DESIGN bounce), and stop at the Fluss signal table
 * (Decision 9, never calls Arrow/broker).
 *
 * <p><b>State model — why KeyedProcessFunction:</b> Idempotency must survive
 * checkpoint-restore so replay converges to one row per logical signal. A plain
 * MapFunction with heap state would lose dedup on restore (intentional
 * amnesia, as in {@link FingerprintDedupFunction}), violating §F. This class
 * is therefore a {@code KeyedProcessFunction<Long, MultiTimeframeSignalContext,
 * RowData>} keyed by {@code instrument_token} with Flink-managed
 * {@code MapState} dedup, checkpointed per key-group and restored atomically
 * with the source offset — same pattern as
 * {@link CandleKvFirstWriteWinsFunction}'s written marker, but for signals.
 * A non-keyed variant would hold global state and could not scale by
 * instrument.
 *
 * <p><b>Demo rule (placeholder for soak proof — NOT the final signal library):
 * </b> {@code breakout-15-forming-trend} — a strict breakout evaluated on the
 * forming candle vs the {@code closedNewestFirst} 15-ring, act-immediately:
 * <ol>
 *   <li>Warm-up: {@code closedNewestFirst.size() >= 15} per TF that the rule
 *       uses (design §G warm-up, per-TF, mirrors
 *       {@link SignalLookbackState#isWarm()} but per TF).</li>
 *   <li>Bullish: {@code forming.closePaise > forming.openPaise} (strict).</li>
 *   <li>Breakout: {@code forming.closePaise > max(high of ring)} (strict).</li>
 *   <li>Trend: {@code forming.closePaise * ringSize > sum(close of ring)}
 *       (exact integer, no rounding — same as {@link SignalLookbackState}).</li>
 * </ol>
 * Isolated as {@link #evaluate(TimeframeContext)} ({@code static RuleResult})
 * so Phase 4 can plug real user rules without touching operator wiring. The
 * rule itself is intentionally minimal and test-visible.
 *
 * <p><b>Row emission:</b> One {@code Signal_Candidates} {@link RowData} per
 * firing TF per tick (up to 6 per context), all NOT-NULL columns set per
 * {@link SignalCandidatesTableColumns}: {@code candidate_id},
 * {@code instrument_token}, {@code exchange}, {@code symbol},
 * {@code strategy_id}, {@code strategy_version}, {@code rule_id},
 * {@code detection_ts}, {@code evaluation_ts}, {@code action}, {@code side},
 * {@code quantity}, {@code order_type}, {@code limit_price_paise},
 * {@code score_inputs} (JSON-ish snapshot of forming + ring), 
 * {@code formation_snapshot_ref} ({@code tf, window_start, trigger
 * fingerprint} reference), {@code validity_reason}, {@code schema_version}.
 * Timestamps: {@code detection_ts = context.eventTime} (broker ts),
 * {@code evaluation_ts = now} (processing time). Formation snapshot is
 * parseable (design §F).
 *
 * <p><b>Idempotency:</b> Deterministic key
 * {@code (rule_id, instrument_token, tf, window_start, triggerFingerprint)}
 * where {@code window_start = TimeframeBucket.bucketStart(tf, eventTime)} and
 * {@code triggerFingerprint = forming.lastFingerprint} (empty if null).
 * Hashed via {@code UUID.nameUUIDFromBytes} (MD5-based deterministic UUID) into
 * {@code candidate_id}. A per-key
 * {@code MapState<String,Boolean> emittedIds} (full candidate_id) suppresses
 * re-emission on replay/restart; a second per-key
 * {@code MapState<String,Boolean> firedWindows} (key =
 * {@code rule_id|token|tf|window_start}) implements the fire-once-per-window
 * latch (design §F). Together they bound volume to one row per window per rule
 * and guarantee replay converges — same contract as
 * {@link CandleKvFirstWriteWinsFunction} but for signals. Only identical
 * re-emissions are suppressed; every NEW logical signal is always emitted
 * (append-only downstream).
 *
 * <p><b>Metrics:</b> {@code compute.multitf.signal.emitted} and
 * {@code compute.multitf.signal.suppressed} (suppressed = duplicate-or-latch).
 */
public class MultiTimeframeSignalProducer
        extends KeyedProcessFunction<Long, MultiTimeframeSignalContext, RowData> {

    private static final long serialVersionUID = 1L;

    /** Placeholder rule identity — wired for the soak proof, not the final library. */
    public static final String STRATEGY_ID = SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID;
    public static final String STRATEGY_VERSION = SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION;
    /** Demo breakout rule — placeholder, see class javadoc. */
    public static final String RULE_ID = "breakout-15-forming-trend";
    public static final long QUANTITY = 1L;
    public static final String SCHEMA_VERSION = SignalCandidatesTableColumns.SCHEMA_VERSION_V2;

    /** Warm-up: need 15 closed per TF (same as MultiTimeframeClosedRing.CAPACITY). */
    public static final int WARMUP_CLOSED = MultiTimeframeClosedRing.CAPACITY;

    private static final MapStateDescriptor<String, Boolean> EMITTED_IDS_DESC =
            new MapStateDescriptor<>("multitf-signal-emitted-ids", Types.STRING, Types.BOOLEAN);
    private static final MapStateDescriptor<String, Boolean> FIRED_WINDOWS_DESC =
            new MapStateDescriptor<>("multitf-signal-fired-windows", Types.STRING, Types.BOOLEAN);

    private transient MapState<String, Boolean> emittedIds;
    private transient MapState<String, Boolean> firedWindows;
    private transient Counter emittedCounter;
    private transient Counter suppressedCounter;
    // heap mirrors for tests that bypass metric registry
    private transient long emittedHeap;
    private transient long suppressedHeap;

    /**
     * Placeholder breakout rule result — isolated for Phase 4 plug-in.
     * Documented as placeholder: real user rules replace the body without
     * changing operator wiring/state/metrics.
     */
    public static final class RuleResult {
        public final boolean fired;
        public final long maxHigh;
        public final long sumCloses;
        public final String reason;

        private RuleResult(boolean fired, long maxHigh, long sumCloses, String reason) {
            this.fired = fired;
            this.maxHigh = maxHigh;
            this.sumCloses = sumCloses;
            this.reason = reason;
        }

        public static RuleResult notFired(String reason) {
            return new RuleResult(false, 0L, 0L, reason);
        }

        public static RuleResult fired(long maxHigh, long sumCloses) {
            return new RuleResult(true, maxHigh, sumCloses, "fired");
        }
    }

    /**
     * Demo breakout evaluation — placeholder rule wired for the soak proof.
     * See class javadoc for the four conditions. Returns a {@link RuleResult}
     * so callers can log/emit diagnostics without re-computing.
     *
     * <p>Not the final signal library: Phase 4 replaces this method body with
     * real user rules (strategy-per-TF, long/short, filters) while keeping the
     * same signature.
     */
    public static RuleResult evaluate(MultiTimeframeSignalContext.TimeframeContext ctx) {
        if (ctx == null) {
            return RuleResult.notFired("null ctx");
        }
        // Warm-up per TF (design §G) — need 15 closed.
        if (ctx.closedNewestFirst().size() < WARMUP_CLOSED) {
            return RuleResult.notFired("warmup:" + ctx.closedNewestFirst().size());
        }
        CandleAccumulator forming = ctx.forming();
        if (forming == null) {
            return RuleResult.notFired("null forming");
        }
        if (forming.tickCount == 0) {
            return RuleResult.notFired("empty forming tickCount=0");
        }
        // Treat an accumulator that never received a trade as empty (open==0 etc.)
        // and require a fingerprint (trigger identity) to be present.
        if (forming.lastFingerprint == null || forming.lastFingerprint.isEmpty()) {
            return RuleResult.notFired("no fingerprint");
        }
        long maxHigh = Long.MIN_VALUE;
        long sumCloses = 0L;
        for (ClosedCandle c : ctx.closedNewestFirst()) {
            if (c.highPaise > maxHigh) {
                maxHigh = c.highPaise;
            }
            sumCloses += c.closePaise;
        }
        int n = ctx.closedNewestFirst().size();
        boolean bullish = forming.closePaise > forming.openPaise;
        if (!bullish) {
            return RuleResult.notFired("not bullish close=" + forming.closePaise + " open=" + forming.openPaise);
        }
        boolean breakout = forming.closePaise > maxHigh;
        if (!breakout) {
            return RuleResult.notFired("no breakout close=" + forming.closePaise + " maxHigh=" + maxHigh);
        }
        boolean trend = forming.closePaise * (long) n > sumCloses;
        if (!trend) {
            return RuleResult.notFired("no trend close*n=" + (forming.closePaise * (long) n) + " sum=" + sumCloses);
        }
        return RuleResult.fired(maxHigh, sumCloses);
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        emittedIds = getRuntimeContext().getMapState(EMITTED_IDS_DESC);
        firedWindows = getRuntimeContext().getMapState(FIRED_WINDOWS_DESC);
        emittedHeap = 0L;
        suppressedHeap = 0L;
        try {
            emittedCounter = getRuntimeContext().getMetricGroup().counter("compute.multitf.signal.emitted");
            suppressedCounter = getRuntimeContext().getMetricGroup().counter("compute.multitf.signal.suppressed");
        } catch (Exception ignored) {
            // harness may not provide metrics; heap counters still work
        }
    }

    @Override
    public void processElement(MultiTimeframeSignalContext ctx, Context flinkCtx, Collector<RowData> out)
            throws Exception {
        long instrumentToken = ctx.instrumentToken();
        String exchange = ctx.exchange();
        String symbol = ctx.symbol();
        long eventTime = ctx.eventTime();
        // evaluation_ts = now (processing time). Prefer the operator's processing-time clock for harness determinism.
        long evaluationTs;
        try {
            long proc = flinkCtx.timerService().currentProcessingTime();
            evaluationTs = proc != Long.MIN_VALUE ? proc : System.currentTimeMillis();
            if (evaluationTs == Long.MIN_VALUE) {
                evaluationTs = System.currentTimeMillis();
            }
        } catch (Exception e) {
            evaluationTs = System.currentTimeMillis();
        }

        for (MultiTimeframeSignalContext.TimeframeContext tfCtx : ctx.frames()) {
            RuleResult rr = evaluate(tfCtx);
            if (!rr.fired) {
                continue;
            }
            Timeframe tf = tfCtx.tf();
            long windowStart = TimeframeBucket.bucketStart(tf, eventTime);
            String triggerFingerprint = tfCtx.forming().lastFingerprint != null ? tfCtx.forming().lastFingerprint : "";
            String rawKey = RULE_ID + "|" + instrumentToken + "|" + tf.code() + "|" + windowStart + "|" + triggerFingerprint;
            String candidateId = UUID.nameUUIDFromBytes(rawKey.getBytes(StandardCharsets.UTF_8)).toString();
            String windowKey = RULE_ID + "|" + instrumentToken + "|" + tf.code() + "|" + windowStart;

            // Idempotency: same logical signal (same fingerprint) already emitted
            if (emittedIds.contains(candidateId)) {
                incSuppressed();
                continue;
            }
            // Fire-once-per-window latch: second tick in same bucket with same rule suppressed
            if (firedWindows.contains(windowKey)) {
                incSuppressed();
                continue;
            }

            emittedIds.put(candidateId, Boolean.TRUE);
            firedWindows.put(windowKey, Boolean.TRUE);
            incEmitted();

            RowData row = buildRow(ctx, tfCtx, tf, windowStart, triggerFingerprint, candidateId, rr, eventTime, evaluationTs);
            out.collect(row);
        }
    }

    private void incEmitted() {
        emittedHeap++;
        if (emittedCounter != null) {
            emittedCounter.inc();
        }
    }

    private void incSuppressed() {
        suppressedHeap++;
        if (suppressedCounter != null) {
            suppressedCounter.inc();
        }
    }

    /** Accessor for test harness metrics. */
    public long getEmittedCountForTest() {
        if (emittedCounter != null) {
            return emittedCounter.getCount();
        }
        return emittedHeap;
    }

    /** Accessor for test harness metrics. */
    public long getSuppressedCountForTest() {
        if (suppressedCounter != null) {
            return suppressedCounter.getCount();
        }
        return suppressedHeap;
    }

    private RowData buildRow(MultiTimeframeSignalContext ctx,
            MultiTimeframeSignalContext.TimeframeContext tfCtx,
            Timeframe tf,
            long windowStart,
            String triggerFingerprint,
            String candidateId,
            RuleResult rr,
            long detectionTs,
            long evaluationTs) {
        CandleAccumulator forming = tfCtx.forming();
        long windowEnd = windowStart + tf.windowMs();
        // Fallbacks for exchange/symbol: prefer context top-level, then forming snapshot
        String exchange = ctx.exchange() != null ? ctx.exchange()
                : (forming.exchange != null ? forming.exchange : "NSE");
        String symbol = ctx.symbol() != null ? ctx.symbol()
                : (forming.symbol != null ? forming.symbol : "UNKNOWN");

        // score_inputs: compact JSON-ish snapshot of forming + ring context that fired
        String scoreInputs = buildScoreInputs(tf, forming, rr);

        // formation_snapshot_ref: parseable snapshot per design §F
        String formationSnapshotRef = buildFormationSnapshotRef(tf, windowStart, windowEnd, forming, triggerFingerprint);

        GenericRowData row = new GenericRowData(SignalCandidatesTableColumns.FIELD_COUNT);
        row.setField(SignalCandidatesTableColumns.CANDIDATE_ID, StringData.fromString(candidateId));
        row.setField(SignalCandidatesTableColumns.INSTRUCTION_ID, null);
        row.setField(SignalCandidatesTableColumns.TRADE_CONTEXT_ID, null);
        row.setField(SignalCandidatesTableColumns.INSTRUMENT_TOKEN, ctx.instrumentToken());
        row.setField(SignalCandidatesTableColumns.EXCHANGE, StringData.fromString(exchange));
        row.setField(SignalCandidatesTableColumns.SYMBOL, StringData.fromString(symbol));
        row.setField(SignalCandidatesTableColumns.STRATEGY_ID, StringData.fromString(STRATEGY_ID));
        row.setField(SignalCandidatesTableColumns.STRATEGY_VERSION, StringData.fromString(STRATEGY_VERSION));
        row.setField(SignalCandidatesTableColumns.RULE_ID, StringData.fromString(RULE_ID));
        row.setField(SignalCandidatesTableColumns.DETECTION_TS, detectionTs);
        row.setField(SignalCandidatesTableColumns.EVALUATION_TS, evaluationTs);
        row.setField(SignalCandidatesTableColumns.ACTION, StringData.fromString(SignalCandidatesTableColumns.ACTION_ENTRY));
        row.setField(SignalCandidatesTableColumns.SIDE, StringData.fromString(SignalCandidatesTableColumns.SIDE_BUY));
        row.setField(SignalCandidatesTableColumns.QUANTITY, QUANTITY);
        row.setField(SignalCandidatesTableColumns.ORDER_TYPE, StringData.fromString(SignalCandidatesTableColumns.ORDER_TYPE_MARKET));
        // MARKET order: no limit price, but task requires non-null. Use 0 as sentinel.
        row.setField(SignalCandidatesTableColumns.LIMIT_PRICE_PAISE, 0L);
        row.setField(SignalCandidatesTableColumns.SCORE_INPUTS, StringData.fromString(scoreInputs));
        row.setField(SignalCandidatesTableColumns.FORMATION_SNAPSHOT_REF, StringData.fromString(formationSnapshotRef));
        row.setField(SignalCandidatesTableColumns.VALIDITY_REASON, StringData.fromString(SignalCandidatesTableColumns.VALIDITY_REASON_VALID));
        row.setField(SignalCandidatesTableColumns.SUPERSEDES_CANDIDATE_ID, null);
        row.setField(SignalCandidatesTableColumns.SUPERSEDED_BY_CANDIDATE_ID, null);
        row.setField(SignalCandidatesTableColumns.SCHEMA_VERSION, StringData.fromString(SCHEMA_VERSION));
        return row;
    }

    private static String buildScoreInputs(Timeframe tf, CandleAccumulator forming, RuleResult rr) {
        // Compact JSON-ish, no need for full serde — diagnostic snapshot.
        StringBuilder sb = new StringBuilder(256);
        sb.append("{");
        sb.append("\"tf\":\"").append(tf.code()).append("\",");
        sb.append("\"open\":").append(forming.openPaise).append(",");
        sb.append("\"high\":").append(forming.highPaise).append(",");
        sb.append("\"low\":").append(forming.lowPaise).append(",");
        sb.append("\"close\":").append(forming.closePaise).append(",");
        sb.append("\"volume\":").append(forming.volume).append(",");
        sb.append("\"tickCount\":").append(forming.tickCount).append(",");
        sb.append("\"lastEventTs\":").append(forming.lastEventTime).append(",");
        sb.append("\"fingerprint\":\"").append(escapeJson(forming.lastFingerprint)).append("\",");
        sb.append("\"maxHighPrev\":").append(rr.maxHigh).append(",");
        sb.append("\"sumClosePrev\":").append(rr.sumCloses).append(",");
        sb.append("\"ringSize\":").append(WARMUP_CLOSED);
        sb.append("}");
        return sb.toString();
    }

    private static String buildFormationSnapshotRef(Timeframe tf, long windowStart, long windowEnd,
            CandleAccumulator forming, String triggerFingerprint) {
        // design §F: forming:{tf}:{window_start}:{window_end}:{open}/{high}/{low}/{close}/{volume}/{tickCount}:{lastEventTs}:{fingerprint}
        StringBuilder sb = new StringBuilder(256);
        sb.append("forming:").append(tf.code()).append(":");
        sb.append(windowStart).append(":").append(windowEnd).append(":");
        sb.append(forming.openPaise).append("/").append(forming.highPaise).append("/")
                .append(forming.lowPaise).append("/").append(forming.closePaise).append("/");
        sb.append(forming.volume).append("/").append(forming.tickCount).append(":");
        sb.append(forming.lastEventTime).append(":").append(triggerFingerprint);
        return sb.toString();
    }

    private static String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * Deterministic candidate_id builder — public for tests to assert determinism
     * without going through the operator. Same hashing as {@link #processElement}.
     */
    public static String candidateIdFor(long instrumentToken, Timeframe tf, long windowStart,
            String triggerFingerprint) {
        String rawKey = RULE_ID + "|" + instrumentToken + "|" + tf.code() + "|" + windowStart + "|" + (triggerFingerprint != null ? triggerFingerprint : "");
        return UUID.nameUUIDFromBytes(rawKey.getBytes(StandardCharsets.UTF_8)).toString();
    }
}
