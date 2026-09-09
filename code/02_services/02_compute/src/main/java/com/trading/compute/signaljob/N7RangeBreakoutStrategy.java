package com.trading.compute.signaljob;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.UUID;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * N7 range-contraction entry strategy (2026-09-05 cutover, batch 2): a
 * line-for-line port of the retired {@code N7SignalFunction} operator onto
 * the {@link SignalStrategy} contract. Same rings, same arming, same D-006
 * evaluation, same row shape, same deterministic candidate ids — only the
 * shell changed (host owns routing, slots, emit-dedup, and UIDs).
 *
 * <p>N7 = Narrow Range 7. A completed candle whose range (high − low) is
 * strictly smaller than each of the previous 6 completed candles of the same
 * timeframe arms a setup. Price trading strictly above the setup high emits a
 * BUY entry; strictly below the setup low emits a SELL entry. One setup fires
 * at most once, in one direction. A newer N7 candle of the same timeframe
 * replaces the setup.
 *
 * <p><b>Instance scope.</b> The host creates one instance per instrument, so
 * the operator's per-key slot collapses to plain fields: per-timeframe rings
 * and setups, last evaluated trade, identity. Intentional amnesia holds — a
 * restore rebuilds from replayed closed candles. Exactly-once emission is the
 * host's managed {@code candidate_id} map, keyed by the same deterministic
 * ids the old operator wrote, so the KV current-state converges across the
 * cutover instead of duplicating.
 *
 * <p><b>Evaluation model (D-006).</b> All forming candles of one instrument
 * share the same last trade price at the same instant. Each new trade price
 * evaluates against every armed setup of every timeframe; when more than one
 * timeframe breaches, only the highest emits. Lone lower-timeframe breaches
 * still fire.
 *
 * <p><b>Cross-stream ordering edge.</b> Live rows can arrive before the
 * closed candle that arms the breached setup. Arming therefore also checks
 * the last known trade price, but only when that trade is newer than the
 * arming candle's own window end — a trade inside the arming window is that
 * candle's own price and can never breach its levels.
 *
 * <p>Rule identity comes from config, exactly like the old operator
 * (defaults are the pinned canonical identity): an overridden rule id feeds
 * rows the LOG keeps and the KV filter drops.
 */
public class N7RangeBreakoutStrategy implements SignalStrategy {

    private static final long serialVersionUID = 1L;

    /** Registry id = the pinned canonical N7 rule id. */
    public static final String RULE_ID = SignalCandidatesTableColumns.CANONICAL_N7_RULE_ID;

    /** Ring depth: 7 candles (the N7 candle + 6 prior) per timeframe. */
    public static final int RING_CAPACITY = 7;

    private final SignalJobConfig config;
    private final Metrics metrics;

    @SuppressWarnings("unchecked")
    private final Deque<RingCandle>[] rings = new ArrayDeque[Timeframe.values().length];
    private final ArmedSetup[] armed = new ArmedSetup[Timeframe.values().length];

    /** Last trade time evaluated against every armed setup. */
    private long lastEvalTime = Long.MIN_VALUE;

    /** Last trade price seen (used for the arm-time ordering edge). */
    private long lastEvalPrice = 0L;

    /** Instrument identity, refreshed from every row. */
    private long token = -1L;
    private String exchange;
    private String symbol;

    private long armedHeap;

    public N7RangeBreakoutStrategy(SignalJobConfig config, Metrics metrics) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.metrics = Preconditions.checkNotNull(metrics, "metrics");
        for (int i = 0; i < rings.length; i++) {
            rings[i] = new ArrayDeque<>(RING_CAPACITY);
        }
    }

    @Override
    public String ruleId() {
        return config.n7RuleId();
    }

    /**
     * Live forming candle: breakout check on each new trade price.
     * At most one evaluation per {@code (token, last_event_time)}.
     */
    @Override
    public void onLiveTick(RowData live, Collector<RowData> out) throws Exception {
        long tradeTime = live.getLong(CandleLiveColumns.LAST_EVENT_TIME);
        token = live.getLong(CandleLiveColumns.INSTRUMENT_TOKEN);
        exchange = stringAt(live, CandleLiveColumns.EXCHANGE);
        symbol = stringAt(live, CandleLiveColumns.SYMBOL);
        if (tradeTime <= lastEvalTime) {
            return; // no new trade since the last evaluation — price is identical
        }
        long price = live.getLong(CandleLiveColumns.CLOSE_PAISE);
        evaluate(price, tradeTime, out);
        lastEvalPrice = price;
        lastEvalTime = tradeTime;
    }

    /** Completed candle: maintain the N7 ring, arm setups. */
    @Override
    public void onClosedCandle(RowData closed, Collector<RowData> out) throws Exception {
        token = closed.getLong(CandleClosedColumns.INSTRUMENT_TOKEN);
        Timeframe tf = parseTimeframe(closed);
        long ws = closed.getLong(CandleClosedColumns.WINDOW_START);
        long we = closed.getLong(CandleClosedColumns.WINDOW_END);
        long high = closed.getLong(CandleClosedColumns.HIGH_PAISE);
        long low = closed.getLong(CandleClosedColumns.LOW_PAISE);

        Deque<RingCandle> ring = rings[tf.ordinal()];
        // Monotonic guard: closed candles arrive in windowStart order per tf.
        // A duplicate/replayed window (same ws) must not double-enter the ring.
        if (!ring.isEmpty() && ws <= ring.peekLast().windowStart) {
            return;
        }
        ring.addLast(new RingCandle(ws, high, low));
        while (ring.size() > RING_CAPACITY) {
            ring.removeFirst();
        }

        if (ring.size() == RING_CAPACITY && isStrictN7(ring)) {
            ArmedSetup setup = new ArmedSetup(ws, we, high, low);
            armed[tf.ordinal()] = setup;
            armedHeap++;
            metrics.inc("armed", 1);
            // Cross-stream ordering edge: if a trade newer than this candle's
            // window already breached the fresh levels, fire now.
            if (lastEvalTime > we) {
                evaluateArmedSetup(tf, setup, lastEvalPrice, lastEvalTime, out);
            }
        }
    }

    /** Check every armed setup at one price; emit the highest breached TF only. */
    private void evaluate(long price, long tradeTime, Collector<RowData> out)
            throws Exception {
        Timeframe winnerTf = null;
        ArmedSetup winner = null;
        int breachCount = 0;
        for (Timeframe tf : Timeframe.values()) {
            ArmedSetup setup = armed[tf.ordinal()];
            if (setup == null || setup.firedSide != null) {
                continue;
            }
            if (sideFor(price, setup.highPaise, setup.lowPaise) == null) {
                continue;
            }
            breachCount++;
            // Timeframe.values() is ascending window size, so the last breach
            // seen is the highest timeframe (D-006).
            winnerTf = tf;
            winner = setup;
        }
        if (winner == null) {
            return;
        }
        if (breachCount > 1) {
            // D-006: lower-timeframe simultaneous breaches are suppressed.
            metrics.inc("suppressed", breachCount - 1);
        }
        evaluateArmedSetup(winnerTf, winner, price, tradeTime, out);
    }

    /**
     * Fire one setup at one price if it breaches and has not fired. The host
     * dedups on the deterministic candidate id across restores; the latch
     * stops re-emits within this instance.
     */
    private void evaluateArmedSetup(Timeframe tf, ArmedSetup setup, long price,
            long tradeTime, Collector<RowData> out) throws Exception {
        String side = sideFor(price, setup.highPaise, setup.lowPaise);
        if (side == null || setup.firedSide != null) {
            return;
        }
        if ((!SignalCandidatesTableColumns.SIDE_BUY.equals(side)
                && !SignalCandidatesTableColumns.SIDE_SELL.equals(side))
                || price < 0
                || config.signalQuantity() <= 0) {
            metrics.inc("rejected_invalid", 1);
            return;
        }
        setup.firedSide = side;
        out.collect(buildRow(tf, setup, side, price, tradeTime));
    }

    /** Side implied by price vs the setup levels, or null when inside. */
    static String sideFor(long price, long setupHigh, long setupLow) {
        if (price > setupHigh) {
            return SignalCandidatesTableColumns.SIDE_BUY;
        }
        if (price < setupLow) {
            return SignalCandidatesTableColumns.SIDE_SELL;
        }
        return null;
    }

    /**
     * Strict N7: the newest of 7 ring candles is strictly narrower than each
     * of the previous 6. An equal range is NOT an N7.
     */
    static boolean isStrictN7(Deque<RingCandle> ring) {
        if (ring.size() != RING_CAPACITY) {
            return false;
        }
        List<RingCandle> all = new ArrayList<>(ring);
        RingCandle newest = all.get(all.size() - 1);
        long newestRange = newest.range();
        for (int i = 0; i < all.size() - 1; i++) {
            if (all.get(i).range() <= newestRange) {
                return false;
            }
        }
        return true;
    }

    /** One armed setup per timeframe: levels from the N7 candle. */
    static final class ArmedSetup implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        final long windowStart;
        final long windowEnd;
        final long highPaise;
        final long lowPaise;

        /** Direction the setup already fired: {@code BUY}, {@code SELL}, or null. */
        String firedSide;

        ArmedSetup(long windowStart, long windowEnd, long highPaise, long lowPaise) {
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.highPaise = highPaise;
            this.lowPaise = lowPaise;
        }
    }

    /** Ring entry: one closed candle's identity + range levels. */
    static final class RingCandle implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        final long windowStart;
        final long highPaise;
        final long lowPaise;

        RingCandle(long windowStart, long highPaise, long lowPaise) {
            this.windowStart = windowStart;
            this.highPaise = highPaise;
            this.lowPaise = lowPaise;
        }

        long range() {
            return highPaise - lowPaise;
        }
    }

    private static Timeframe parseTimeframe(RowData row) {
        return Timeframe.valueOf(row.getString(CandleClosedColumns.TF).toString());
    }

    private static String stringAt(RowData row, int idx) {
        if (row == null || row.isNullAt(idx)) {
            return null;
        }
        return row.getString(idx).toString();
    }

    private GenericRowData buildRow(Timeframe tf, ArmedSetup setup, String side, long price,
            long tradeTime) {
        // One instance serves one instrument: token and identity refresh from
        // every row. Tests that feed only closed candles still stamp the row
        // correctly; live rows add exchange/symbol. Same fallbacks as the
        // retired operator.
        String ruleId = config.n7RuleId();
        String candidateId = candidateIdFor(ruleId, token, tf, setup.windowStart, side);
        long evaluationTs = System.currentTimeMillis();

        GenericRowData row = new GenericRowData(SignalCandidatesTableColumns.FIELD_COUNT);
        row.setField(SignalCandidatesTableColumns.CANDIDATE_ID, StringData.fromString(candidateId));
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
        row.setField(SignalCandidatesTableColumns.RULE_ID, StringData.fromString(ruleId));
        row.setField(SignalCandidatesTableColumns.DETECTION_TS, tradeTime);
        row.setField(SignalCandidatesTableColumns.EVALUATION_TS, evaluationTs);
        row.setField(SignalCandidatesTableColumns.ACTION,
                StringData.fromString(SignalCandidatesTableColumns.ACTION_ENTRY));
        row.setField(SignalCandidatesTableColumns.SIDE, StringData.fromString(side));
        row.setField(SignalCandidatesTableColumns.QUANTITY, config.signalQuantity());
        row.setField(SignalCandidatesTableColumns.ORDER_TYPE,
                StringData.fromString(SignalCandidatesTableColumns.ORDER_TYPE_MARKET));
        // MARKET order: no limit price (null, not the old-chain 0 sentinel:
        // ExecutionIntentBuilder rejects a present-but-non-positive limit).
        row.setField(SignalCandidatesTableColumns.LIMIT_PRICE_PAISE, null);
        row.setField(SignalCandidatesTableColumns.SCORE_INPUTS,
                StringData.fromString(scoreInputs(tf, setup, side, price)));
        row.setField(SignalCandidatesTableColumns.FORMATION_SNAPSHOT_REF,
                StringData.fromString(formationRef(tf, setup, side, price, tradeTime)));
        row.setField(SignalCandidatesTableColumns.VALIDITY_REASON,
                StringData.fromString(SignalCandidatesTableColumns.VALIDITY_REASON_VALID));
        row.setField(SignalCandidatesTableColumns.SUPERSEDES_CANDIDATE_ID, null);
        row.setField(SignalCandidatesTableColumns.SUPERSEDED_BY_CANDIDATE_ID, null);
        row.setField(SignalCandidatesTableColumns.SCHEMA_VERSION,
                StringData.fromString(SignalCandidatesTableColumns.SCHEMA_VERSION_V2));
        return row;
    }

    private static String scoreInputs(Timeframe tf, ArmedSetup setup, String side, long price) {
        return "{"
                + "\"tf\":\"" + tf.code() + "\","
                + "\"side\":\"" + side + "\","
                + "\"n7WindowStart\":" + setup.windowStart + ","
                + "\"levelHigh\":" + setup.highPaise + ","
                + "\"levelLow\":" + setup.lowPaise + ","
                + "\"n7Range\":" + (setup.highPaise - setup.lowPaise) + ","
                + "\"triggerPrice\":" + price
                + "}";
    }

    private static String formationRef(Timeframe tf, ArmedSetup setup, String side,
            long price, long tradeTime) {
        return "n7:" + tf.code() + ":" + setup.windowStart + ":" + setup.windowEnd
                + ":high=" + setup.highPaise + ":low=" + setup.lowPaise
                + ":trigger=" + price + ":tradeTime=" + tradeTime + ":side=" + side;
    }

    /**
     * Deterministic candidate id — same hashing scheme as the retired
     * operator: {@code UUID.nameUUIDFromBytes} over
     * {@code rule|token|tf|setupWindowStart|side}. One logical signal per
     * setup per direction, stable across replays AND across the cutover, so
     * the host's dedup map converges with rows the old operator wrote.
     */
    public static String candidateIdFor(String ruleId, long token, Timeframe tf,
            long setupWindowStart, String side) {
        String rawKey = ruleId + "|" + token + "|" + tf.code() + "|"
                + setupWindowStart + "|" + side;
        return UUID.nameUUIDFromBytes(rawKey.getBytes(StandardCharsets.UTF_8)).toString();
    }

    /** Test seams. */
    int ringSizeForTest(Timeframe tf) {
        return rings[tf.ordinal()].size();
    }

    ArmedSetup armedForTest(Timeframe tf) {
        return armed[tf.ordinal()];
    }

    long armedCountForTest() {
        return armedHeap;
    }
}
