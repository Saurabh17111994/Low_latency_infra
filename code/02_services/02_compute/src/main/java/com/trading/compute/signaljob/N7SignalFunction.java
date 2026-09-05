package com.trading.compute.signaljob;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * N7 range-contraction entry signal (design
 * {@code docs/plans/2026-09-05-n7-signal-design.md}, approved 2026-09-05).
 *
 * <p>N7 = Narrow Range 7. A completed candle whose range (high − low) is
 * strictly smaller than each of the previous 6 completed candles of the same
 * timeframe arms a setup. Price trading strictly above the setup high emits a
 * BUY entry; strictly below the setup low emits a SELL entry. One setup fires
 * at most once, in one direction. A newer N7 candle of the same timeframe
 * replaces the setup.
 *
 * <p><b>Operator shape.</b> {@code KeyedCoProcessFunction} keyed by
 * {@code instrument_token}:
 * <ul>
 *   <li>Input 1 (live): the multi-TF aggregator's {@link CandleLiveColumns}
 *       rows at a 1s cadence. Drives the breakout check.</li>
 *   <li>Input 2 (closed): the aggregator's main-output {@link CandleClosedColumns}
 *       rows. Drives the N7 ring and setup arming.</li>
 * </ul>
 * Both layouts are field-identical (the closed/live DDLs mirror each other).
 *
 * <p><b>Evaluation model (D-006).</b> All forming candles of one instrument
 * share the same last trade price at the same instant. The operator therefore
 * evaluates once per NEW trade price (a live row whose {@code last_event_time}
 * advanced past the previously evaluated trade), against every armed setup of
 * every timeframe. When more than one timeframe breaches at that price, only
 * the highest timeframe emits (D-006); a lone lower-timeframe breach still
 * fires. This makes the higher-timeframe rule a single in-element decision,
 * never a cross-row race.
 *
 * <p><b>Memory model (intentional amnesia).</b> Detection state — the 7-candle
 * rings, armed setups, fired latch, last evaluated price/time — lives in plain
 * heap slots, NOT Flink managed state, mirroring
 * {@link MultiTimeframeAggregateFunction} / {@link FormingBarDetectionFunction}
 * (G-CHAIN-3). A restore restarts empty and rebuilds rings + setups from
 * replayed closed candles. Exactly-once emission is guaranteed by one managed
 * {@code MapState<String,Boolean> emittedIds} keyed by the deterministic
 * candidate id — the same pattern the retired producer used. The emit, the
 * emittedIds write, and the transactional Fluss LOG commit land under the
 * same checkpoint barrier, so a restored replay converges to one LOG row per
 * logical signal.
 *
 * <p><b>Structural bounds.</b> Heap: ≤ 7 ring candles per timeframe per
 * instrument (G-CHAIN-1) and a global slot cap of 65 536 instruments that
 * fails closed (G-CHAIN-2). Managed: one string key per emitted signal;
 * signals are rare (a setup fires once), so the map is bounded by real signal
 * volume, not tick volume.
 *
 * <p><b>Cross-stream ordering edge.</b> The live input and the closed input
 * have no cross-stream order guarantee. A live row can arrive before the
 * closed candle that arms the setup it breached. Arming therefore also checks
 * the instrument's last known trade price, but only when that trade is newer
 * than the arming candle's own window end. A trade inside the arming candle's
 * window is that candle's own price and can never breach its own high/low, so
 * the check cannot false-fire on the arming candle.
 *
 * <p><b>Row contract.</b> Emitted rows are full 22-column Signal_Candidates
 * rows ({@link SignalCandidatesTableColumns}): {@code action=ENTRY},
 * {@code side=BUY|SELL}, MARKET order, deterministic {@code candidate_id},
 * parseable {@code formation_snapshot_ref}, JSON-ish {@code score_inputs}.
 * Rule identity comes from config (defaults are the pinned canonical
 * identity {@code simple-breakout} / {@code n7-range-breakout-v1} / schema
 * v2), so overrides behave like the old chain: LOG keeps the row, the KV
 * canonical filter drops it.
 */
public class N7SignalFunction
        extends KeyedCoProcessFunction<Long, RowData, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    /** Ring depth: 7 candles (the N7 candle + 6 prior) per timeframe. */
    public static final int RING_CAPACITY = 7;

    /** Global heap slot cap (G-CHAIN-2). */
    static final int GLOBAL_SLOT_CAP = 65_536;

    /** Managed emit-dedup state: candidate ids already emitted for this key. */
    private static final MapStateDescriptor<String, Boolean> EMITTED_IDS_DESC =
            new MapStateDescriptor<>("n7-emitted-ids", Types.STRING, Types.BOOLEAN);

    private final SignalJobConfig config;

    private transient MapState<String, Boolean> emittedIds;
    private transient Counter emittedCounter;
    private transient Counter suppressedCounter;
    private transient Counter armedCounter;

    /** Per-instrument heap slots — intentional amnesia, not checkpointed. */
    private final Map<Long, Slot> slots = new HashMap<>();

    // Heap mirrors for tests that bypass the metric registry.
    private transient long emittedHeap;
    private transient long suppressedHeap;
    private transient long armedHeap;

    /** One armed setup per timeframe: levels from the N7 candle. */
    static final class ArmedSetup implements Serializable {
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
    static final class RingCandle implements Serializable {
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

    /** Per-key detection slot: ring + armed setup per timeframe + identity. */
    static final class Slot implements Serializable {
        private static final long serialVersionUID = 1L;

        @SuppressWarnings("unchecked")
        final Deque<RingCandle>[] rings = new ArrayDeque[Timeframe.values().length];
        final ArmedSetup[] armed = new ArmedSetup[Timeframe.values().length];

        /** Last trade time evaluated against every armed setup. */
        long lastEvalTime = Long.MIN_VALUE;

        /** Last trade price seen (used for the arm-time ordering edge). */
        long lastEvalPrice = 0L;

        /** Instrument identity, refreshed from every live row. */
        String exchange;
        String symbol;

        Slot() {
            for (int i = 0; i < rings.length; i++) {
                rings[i] = new ArrayDeque<>(RING_CAPACITY);
            }
        }
    }

    public N7SignalFunction(SignalJobConfig config) {
        this.config = Preconditions.checkNotNull(config, "config");
    }

    @Override
    public void open(OpenContext openContext) {
        slots.clear();
        emittedIds = getRuntimeContext().getMapState(EMITTED_IDS_DESC);
        emittedHeap = 0L;
        suppressedHeap = 0L;
        armedHeap = 0L;
        try {
            emittedCounter = getRuntimeContext().getMetricGroup().counter("compute.n7.signal.emitted");
            suppressedCounter = getRuntimeContext().getMetricGroup().counter("compute.n7.signal.suppressed");
            armedCounter = getRuntimeContext().getMetricGroup().counter("compute.n7.setup.armed");
        } catch (Exception ignored) {
            // harness may not provide metrics; heap counters still work
        }
    }

    private Slot slotFor(long key) {
        Slot s = slots.get(key);
        if (s == null) {
            s = new Slot();
            slots.put(key, s);
            Preconditions.checkState(slots.size() <= GLOBAL_SLOT_CAP,
                    "n7 heap slots %s exceeded cap %s — refusing silent eviction",
                    slots.size(), GLOBAL_SLOT_CAP);
        }
        return s;
    }

    /** Live forming candle (input 1): breakout check on each new trade price. */
    @Override
    public void processElement1(RowData live, Context ctx, Collector<RowData> out)
            throws Exception {
        long token = live.getLong(CandleLiveColumns.INSTRUMENT_TOKEN);
        long tradeTime = live.getLong(CandleLiveColumns.LAST_EVENT_TIME);
        Slot s = slotFor(token);
        s.exchange = stringAt(live, CandleLiveColumns.EXCHANGE);
        s.symbol = stringAt(live, CandleLiveColumns.SYMBOL);
        if (tradeTime <= s.lastEvalTime) {
            return; // no new trade since the last evaluation — price is identical
        }
        long price = live.getLong(CandleLiveColumns.CLOSE_PAISE);
        evaluate(s, price, tradeTime, ctx, out);
        s.lastEvalPrice = price;
        s.lastEvalTime = tradeTime;
    }

    /** Completed candle (input 2): maintain the N7 ring, arm setups. */
    @Override
    public void processElement2(RowData closed, Context ctx, Collector<RowData> out)
            throws Exception {
        long token = ctx.getCurrentKey();
        Timeframe tf = parseTimeframe(closed);
        long ws = closed.getLong(CandleClosedColumns.WINDOW_START);
        long we = closed.getLong(CandleClosedColumns.WINDOW_END);
        long high = closed.getLong(CandleClosedColumns.HIGH_PAISE);
        long low = closed.getLong(CandleClosedColumns.LOW_PAISE);

        Slot s = slotFor(token);
        Deque<RingCandle> ring = s.rings[tf.ordinal()];
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
            ArmedSetup armed = new ArmedSetup(ws, we, high, low);
            s.armed[tf.ordinal()] = armed;
            incArmed();
            // Cross-stream ordering edge: if a trade newer than this candle's
            // window already breached the fresh levels, fire now. A price from
            // inside the candle's own window cannot breach its own high/low,
            // so the trade-time guard makes this safe (see class javadoc).
            if (s.lastEvalTime > we) {
                evaluateArmedSetup(s, tf, armed, s.lastEvalPrice, s.lastEvalTime, ctx, out);
            }
        }
    }

    /** Check every armed setup at one price; emit the highest breached TF only. */
    private void evaluate(Slot s, long price, long tradeTime, Context ctx,
            Collector<RowData> out) throws Exception {
        Timeframe winnerTf = null;
        ArmedSetup winner = null;
        int breachCount = 0;
        for (Timeframe tf : Timeframe.values()) {
            ArmedSetup armed = s.armed[tf.ordinal()];
            if (armed == null || armed.firedSide != null) {
                continue;
            }
            String side = sideFor(price, armed.highPaise, armed.lowPaise);
            if (side == null) {
                continue;
            }
            breachCount++;
            // Timeframe.values() is ascending window size, so the last breach
            // seen is the highest timeframe (D-006).
            winnerTf = tf;
            winner = armed;
        }
        if (winner == null) {
            return;
        }
        if (breachCount > 1) {
            // D-006: lower-timeframe simultaneous breaches are suppressed.
            incSuppressed(breachCount - 1);
        }
        evaluateArmedSetup(s, winnerTf, winner, price, tradeTime, ctx, out);
    }

    /**
     * Fire one setup at one price if it breaches and has not fired. Dedup via
     * the managed emittedIds so a restored replay converges.
     */
    private void evaluateArmedSetup(Slot s, Timeframe tf, ArmedSetup armed, long price,
            long tradeTime, Context ctx, Collector<RowData> out) throws Exception {
        String side = sideFor(price, armed.highPaise, armed.lowPaise);
        if (side == null || armed.firedSide != null) {
            return;
        }
        long token = ctx.getCurrentKey();
        String ruleId = config.n7RuleId();
        String candidateId = candidateIdFor(ruleId, token, tf, armed.windowStart, side);
        if (emittedIds.contains(candidateId)) {
            incSuppressed(1);
            return;
        }
        armed.firedSide = side;
        emittedIds.put(candidateId, Boolean.TRUE);
        incEmitted();
        out.collect(buildRow(ruleId, token, s, tf, armed, side, price, tradeTime));
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

    private static Timeframe parseTimeframe(RowData row) {
        String code = row.getString(CandleClosedColumns.TF).toString();
        return Timeframe.valueOf(code);
    }

    private static String stringAt(RowData row, int idx) {
        if (row == null || row.isNullAt(idx)) {
            return null;
        }
        return row.getString(idx).toString();
    }

    private GenericRowData buildRow(String ruleId, long token, Slot s, Timeframe tf,
            ArmedSetup armed, String side, long price, long tradeTime) {
        String exchange = s.exchange != null ? s.exchange : "NSE";
        String symbol = s.symbol != null ? s.symbol : "UNKNOWN";
        String candidateId = candidateIdFor(ruleId, token, tf, armed.windowStart, side);
        long evaluationTs = System.currentTimeMillis();

        GenericRowData row = new GenericRowData(SignalCandidatesTableColumns.FIELD_COUNT);
        row.setField(SignalCandidatesTableColumns.CANDIDATE_ID, StringData.fromString(candidateId));
        row.setField(SignalCandidatesTableColumns.INSTRUCTION_ID, null);
        row.setField(SignalCandidatesTableColumns.TRADE_CONTEXT_ID, null);
        row.setField(SignalCandidatesTableColumns.INSTRUMENT_TOKEN, token);
        row.setField(SignalCandidatesTableColumns.EXCHANGE, StringData.fromString(exchange));
        row.setField(SignalCandidatesTableColumns.SYMBOL, StringData.fromString(symbol));
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
        // MARKET order: no limit price. The old-chain rows used 0 as a sentinel.
        row.setField(SignalCandidatesTableColumns.LIMIT_PRICE_PAISE, 0L);
        row.setField(SignalCandidatesTableColumns.SCORE_INPUTS,
                StringData.fromString(scoreInputs(tf, armed, side, price)));
        row.setField(SignalCandidatesTableColumns.FORMATION_SNAPSHOT_REF,
                StringData.fromString(formationRef(tf, armed, side, price, tradeTime)));
        row.setField(SignalCandidatesTableColumns.VALIDITY_REASON,
                StringData.fromString(SignalCandidatesTableColumns.VALIDITY_REASON_VALID));
        row.setField(SignalCandidatesTableColumns.SUPERSEDES_CANDIDATE_ID, null);
        row.setField(SignalCandidatesTableColumns.SUPERSEDED_BY_CANDIDATE_ID, null);
        row.setField(SignalCandidatesTableColumns.SCHEMA_VERSION,
                StringData.fromString(SignalCandidatesTableColumns.SCHEMA_VERSION_V2));
        return row;
    }

    private static String scoreInputs(Timeframe tf, ArmedSetup armed, String side, long price) {
        return "{"
                + "\"tf\":\"" + tf.code() + "\","
                + "\"side\":\"" + side + "\","
                + "\"n7WindowStart\":" + armed.windowStart + ","
                + "\"levelHigh\":" + armed.highPaise + ","
                + "\"levelLow\":" + armed.lowPaise + ","
                + "\"n7Range\":" + (armed.highPaise - armed.lowPaise) + ","
                + "\"triggerPrice\":" + price
                + "}";
    }

    private static String formationRef(Timeframe tf, ArmedSetup armed, String side,
            long price, long tradeTime) {
        return "n7:" + tf.code() + ":" + armed.windowStart + ":" + armed.windowEnd
                + ":high=" + armed.highPaise + ":low=" + armed.lowPaise
                + ":trigger=" + price + ":tradeTime=" + tradeTime + ":side=" + side;
    }

    /**
     * Deterministic candidate id — public for tests. Same hashing scheme as
     * the retired producer: {@code UUID.nameUUIDFromBytes} over
     * {@code rule|token|tf|setupWindowStart|side}. One logical signal per
     * setup per direction, stable across replays.
     */
    public static String candidateIdFor(String ruleId, long token, Timeframe tf,
            long setupWindowStart, String side) {
        String rawKey = ruleId + "|" + token + "|" + tf.code() + "|"
                + setupWindowStart + "|" + side;
        return UUID.nameUUIDFromBytes(rawKey.getBytes(StandardCharsets.UTF_8)).toString();
    }

    private void incEmitted() {
        emittedHeap++;
        if (emittedCounter != null) {
            emittedCounter.inc();
        }
    }

    private void incSuppressed(int n) {
        suppressedHeap += n;
        if (suppressedCounter != null) {
            suppressedCounter.inc(n);
        }
    }

    private void incArmed() {
        armedHeap++;
        if (armedCounter != null) {
            armedCounter.inc();
        }
    }

    /** Test seams. */
    int slotCountForTest() {
        return slots.size();
    }

    int ringSizeForTest(long token, Timeframe tf) {
        Slot s = slots.get(token);
        return s == null ? 0 : s.rings[tf.ordinal()].size();
    }

    ArmedSetup armedForTest(long token, Timeframe tf) {
        Slot s = slots.get(token);
        return s == null ? null : s.armed[tf.ordinal()];
    }

    long getEmittedCountForTest() {
        return emittedCounter != null ? emittedCounter.getCount() : emittedHeap;
    }

    long getSuppressedCountForTest() {
        return suppressedCounter != null ? suppressedCounter.getCount() : suppressedHeap;
    }
}
