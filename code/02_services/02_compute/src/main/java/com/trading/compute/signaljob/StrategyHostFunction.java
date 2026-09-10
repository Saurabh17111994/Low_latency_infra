package com.trading.compute.signaljob;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Strategy host (strategy-host design, 2026-09-05): one fixed operator that
 * runs every {@code STRATEGIES}-listed {@link SignalStrategy} for every
 * instrument. Strategies plug in by config id; the topology, UIDs, sinks,
 * and filter wiring never change per strategy.
 *
 * <p><b>Operator shape.</b> {@code KeyedCoProcessFunction} keyed by
 * {@code instrument_token} (same inputs the retired N7 operator used):
 * input 1 = live forming candles ({@link CandleLiveColumns}, 1s cadence),
 * input 2 = completed candles ({@link CandleClosedColumns}). Each input
 * fans out to every registered strategy of that instrument, in registration
 * order.
 *
 * <p><b>State.</b> Per-instrument strategy instances live on the heap
 * (intentional amnesia, same rationale as N7: a restore rebuilds from
 * replayed candles). Exactly-once emission is the host's job, not the
 * strategies': one managed {@code MapState<String,Long>} per instrument,
 * keyed by the emitted row's {@code candidate_id}, dedups every strategy
 * identically, so a restored replay converges (P2-176: keyed state is
 * partitioned by instrument_token, so this is per-instrument dedup — it
 * equals global dedup only because the {@link SignalStrategy} contract
 * requires candidate_id to embed instrument_token). A null or blank id is
 * dropped and counted, never failed: one bad row must not kill the subtask.
 *
 * <p><b>Bounds.</b> One slot per subtask-key with the per-subtask cap below
 * that fails closed (G-CHAIN-2 pattern: slotFor checks before allocating, an
 * oversize key is dropped loudly, never inserted). Per-rule counters ride a
 * {@code strategy/<ruleId>} metric group, so a new strategy is observable
 * with no dashboard change.
 */
public class StrategyHostFunction
        extends KeyedCoProcessFunction<Long, RowData, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(StrategyHostFunction.class);

    /**
     * Per-subtask heap slot cap (G-CHAIN-2 pattern): one slot per key seen by
     * this parallel instance. Named per-subtask on purpose (P2-175) — slots
     * is a plain per-instance HashMap, so N subtasks hold N*CAP in total.
     */
    static final int SUBTASK_SLOT_CAP = 65_536;

    /** @deprecated Renamed to {@link #SUBTASK_SLOT_CAP} — the cap was never global. */
    @Deprecated
    static final int GLOBAL_SLOT_CAP = SUBTASK_SLOT_CAP;

    /** P2-058 horizon: newest-N ids kept per (rule, tf) ledger on compaction. */
    static final int EMITTED_IDS_RETAIN_PER_LEDGER = 16;

    private static final MapStateDescriptor<String, Long> EMITTED_IDS_DESC =
            new MapStateDescriptor<>("strategy-host-emitted-ids", Types.STRING, Types.LONG);

    /** Validated strategy ids, registration order (from config). */
    private final List<String> strategyIds;

    /** Job config handed to strategy constructors (rule identity, quantities). */
    private final SignalJobConfig config;

    /** Per-instrument heap slots — intentional amnesia, not checkpointed. */
    private final Map<Long, HostSlot> slots = new HashMap<>();

    private transient MapState<String, Long> emittedIds;
    private transient Counter suppressed;
    private transient Counter failedStrategy;
    private transient Counter droppedUnkeyed;
    private transient Counter droppedOversize;
    private transient Map<String, Counter> emittedByRule;
    private transient Map<String, HostMetrics> sharedMetrics;

    // Heap mirrors for tests that bypass the metric registry.
    private transient long emittedHeap;
    private transient long suppressedHeap;
    private transient long failedStrategyHeap;
    private transient long droppedUnkeyedHeap;
    private transient long droppedOversizeHeap;
    private final transient Map<String, Long> skippedPoisonHeap = new HashMap<>();

    public StrategyHostFunction(SignalJobConfig config, List<String> strategyIds) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.strategyIds = List.copyOf(Preconditions.checkNotNull(strategyIds, "strategyIds"));
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        if (strategyIds.isEmpty()) {
            throw new IllegalStateException("strategy-host opened with zero strategies — "
                    + "STRATEGY_HOST_ENABLED=true requires a non-empty STRATEGIES list");
        }
        slots.clear();
        emittedIds = getRuntimeContext().getMapState(EMITTED_IDS_DESC);
        suppressed = getRuntimeContext().getMetricGroup().counter("compute.strategy.suppressed");
        failedStrategy =
                getRuntimeContext().getMetricGroup().counter("compute.strategy.failed");
        droppedUnkeyed =
                getRuntimeContext().getMetricGroup().counter("compute.strategy.dropped.unkeyed");
        droppedOversize =
                getRuntimeContext().getMetricGroup().counter("compute.strategy.dropped.oversize");
        emittedByRule = new HashMap<>();
        // P2-174: one metrics handle per rule per subtask, shared by every
        // slot — not one HostMetrics per (token, ruleId).
        sharedMetrics = new HashMap<>();
        for (String id : strategyIds) {
            // Resolve eagerly: unknown ids fail here at startup, never mid-stream.
            Strategies.create(id, config, new HostMetrics(id));
            sharedMetrics.put(id, new HostMetrics(id));
            emittedByRule.put(id, getRuntimeContext().getMetricGroup()
                    .addGroup("strategy", id).counter("emitted"));
        }
    }

    /** Live forming candle (input 1): fan out to every strategy. */
    @Override
    public void processElement1(RowData live, Context ctx, Collector<RowData> out)
            throws Exception {
        HostSlot slot = slotFor(ctx.getCurrentKey());
        if (slot == null) {
            return;
        }
        for (SignalStrategy s : slot.strategies.values()) {
            // P2-057: one strategy must not starve the others — isolate,
            // count, and continue to the next strategy.
            try {
                s.onLiveTick(live, new DedupCollector(out, s.ruleId()));
            } catch (Exception e) {
                countFailedStrategy();
                LOG.warn("strategy-host: dropping failed onLiveTick rule={} token={}: {}",
                        s.ruleId(), ctx.getCurrentKey(), e.toString());
            }
        }
    }

    /** Completed candle (input 2): fan out to every strategy. */
    @Override
    public void processElement2(RowData closed, Context ctx, Collector<RowData> out)
            throws Exception {
        HostSlot slot = slotFor(ctx.getCurrentKey());
        if (slot == null) {
            return;
        }
        for (SignalStrategy s : slot.strategies.values()) {
            // P2-057: same isolation on the closed path.
            try {
                s.onClosedCandle(closed, new DedupCollector(out, s.ruleId()));
            } catch (Exception e) {
                countFailedStrategy();
                LOG.warn("strategy-host: dropping failed onClosedCandle rule={} token={}: {}",
                        s.ruleId(), ctx.getCurrentKey(), e.toString());
            }
        }
    }

    private HostSlot slotFor(long key) {
        HostSlot slot = slots.get(key);
        if (slot == null) {
            // P2-175: check-before-allocate — an oversize key is dropped
            // loudly and never enters the map (same class as P2-037). The
            // cap is per subtask, hence the name.
            if (slots.size() >= SUBTASK_SLOT_CAP) {
                countDroppedOversize();
                LOG.warn("strategy-host: slots {} reached per-subtask cap {} — "
                        + "dropping key {}", slots.size(), SUBTASK_SLOT_CAP, key);
                return null;
            }
            slot = new HostSlot();
            for (String id : strategyIds) {
                slot.strategies.put(
                        id, Strategies.create(id, config, sharedMetrics.get(id)));
            }
            slots.put(key, slot);
        }
        return slot;
    }

    /** One slot per instrument: one strategy instance per registered id. */
    static final class HostSlot {
        final Map<String, SignalStrategy> strategies = new LinkedHashMap<>();
    }

    /**
     * Per-rule metrics scope ({@code strategy/<ruleId>}): strategy callbacks
     * report here; the host's own forward/dedup counters stay separate.
     * Counters create lazily so strategies that never report cost nothing.
     * One instance per rule per subtask (P2-174), shared by every slot —
     * an inner class so all shares resolve counters against this operator's
     * runtime context.
     */
    final class HostMetrics implements SignalStrategy.Metrics {
        private static final long serialVersionUID = 1L;

        private final String ruleId;
        private final transient Map<String, Counter> counters = new HashMap<>();

        HostMetrics(String ruleId) {
            this.ruleId = ruleId;
        }

        @Override
        public void inc(String name, long n) {
            Counter c = counters.get(name);
            if (c == null) {
                c = getRuntimeContext().getMetricGroup()
                        .addGroup("strategy", ruleId).counter(name);
                counters.put(name, c);
            }
            c.inc(n);
            // Heap mirror so unit tests see per-rule skip counts without the
            // metric registry (P2-059/060 poison-skip visibility).
            if (StubSmokeStrategy.SKIPPED_POISON_TF.equals(name)) {
                skippedPoisonHeap.merge(ruleId, n, Long::sum);
            }
        }
    }

    /**
     * Dedups strategy emissions on {@code candidate_id} via the managed
     * emitted-ids map before forwarding. Null/blank ids are dropped and
     * counted (P2-057): one bad row must not kill the subtask.
     */
    private final class DedupCollector implements Collector<RowData> {
        private final Collector<RowData> delegate;
        private final String ruleId;

        DedupCollector(Collector<RowData> delegate, String ruleId) {
            this.delegate = delegate;
            this.ruleId = ruleId;
        }

        @Override
        public void collect(RowData row) {
            String id = row.isNullAt(SignalCandidatesTableColumns.CANDIDATE_ID)
                    ? null
                    : row.getString(SignalCandidatesTableColumns.CANDIDATE_ID).toString();
            if (id == null || id.isBlank()) {
                // P2-057: drop-and-count (+ WARN with ruleId), never throw
                // from the data path.
                countDroppedUnkeyed();
                LOG.warn("strategy-host: dropping unkeyed emission rule={}", ruleId);
                return;
            }
            boolean seen;
            try {
                seen = emittedIds.contains(id);
            } catch (Exception e) {
                throw new IllegalStateException("strategy-host emitted-ids read failed", e);
            }
            if (seen) {
                if (suppressed != null) {
                    suppressed.inc();
                }
                suppressedHeap++;
                return;
            }
            try {
                // P2-058: value = detection_ts when the row carries one,
                // else wall-clock — the compaction ledger keys on it.
                long detTs = row.isNullAt(SignalCandidatesTableColumns.DETECTION_TS)
                        ? System.currentTimeMillis()
                        : row.getLong(SignalCandidatesTableColumns.DETECTION_TS);
                emittedIds.put(id, detTs);
            } catch (Exception e) {
                throw new IllegalStateException("strategy-host emitted-ids write failed", e);
            }
            // P2-058: opportunistic horizon-compaction — one extra state read
            // per emission path, full ledger scan only past 2x the retain
            // horizon, so the steady-state cost is ~zero.
            try {
                maybeCompactEmittedIds(ruleId);
            } catch (Exception e) {
                throw new IllegalStateException("strategy-host emitted-ids compact failed", e);
            }
            Counter ruleCounter = emittedByRule == null ? null : emittedByRule.get(ruleId);
            if (ruleCounter != null) {
                ruleCounter.inc();
            }
            emittedHeap++;
            delegate.collect(row);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    /**
     * P2-058 horizon-compaction: keep the newest
     * {@link #EMITTED_IDS_RETAIN_PER_LEDGER} ids whose key starts with
     * {@code ruleId + "|"}. N7 candidate ids embed
     * {@code rule|token|tf|windowStart|side} (see
     * {@link N7RangeBreakoutStrategy#candidateIdFor}), so the ledger groups
     * one rule's replays and the detection-ts value orders them. Non-N7 id
     * shapes (no pipe prefix) never match a ledger and are kept — compaction
     * only deletes what it can prove stale, never what it cannot parse.
     */
    private void maybeCompactEmittedIds(String ruleId) throws Exception {
        String prefix = ruleId + "|";
        // Fast path: count ledger entries; only scan fully past 2x horizon.
        int count = 0;
        for (String key : emittedIds.keys()) {
            if (key.startsWith(prefix) && ++count > 2 * EMITTED_IDS_RETAIN_PER_LEDGER) {
                break;
            }
        }
        if (count <= 2 * EMITTED_IDS_RETAIN_PER_LEDGER) {
            return;
        }
        String oldestKey = null;
        long oldestTs = Long.MAX_VALUE;
        for (String key : emittedIds.keys()) {
            if (!key.startsWith(prefix)) {
                continue;
            }
            Long ts = emittedIds.get(key);
            long t = ts == null ? Long.MAX_VALUE : ts;
            if (t < oldestTs) {
                oldestTs = t;
                oldestKey = key;
            }
        }
        if (oldestKey != null) {
            emittedIds.remove(oldestKey);
        }
    }

    private void countFailedStrategy() {
        if (failedStrategy != null) {
            failedStrategy.inc();
        }
        failedStrategyHeap++;
    }

    private void countDroppedUnkeyed() {
        if (droppedUnkeyed != null) {
            droppedUnkeyed.inc();
        }
        droppedUnkeyedHeap++;
    }

    private void countDroppedOversize() {
        if (droppedOversize != null) {
            droppedOversize.inc();
        }
        droppedOversizeHeap++;
    }

    /** Test seam: live strategy instance for one instrument. */
    SignalStrategy strategyForTest(long token, String ruleId) {
        HostSlot slot = slots.get(token);
        return slot == null ? null : slot.strategies.get(ruleId);
    }

    /** Test seam: forwarded emission count (metric-registry bypass). */
    long emittedForTest() {
        return emittedHeap;
    }

    /** Test seam: dedup-suppressed count (metric-registry bypass). */
    long suppressedForTest() {
        return suppressedHeap;
    }

    /** Test seam: per-strategy failures isolated by the fan-out (P2-057). */
    long failedStrategyForTest() {
        return failedStrategyHeap;
    }

    /** Test seam: unkeyed emissions dropped, not thrown (P2-057). */
    long droppedUnkeyedForTest() {
        return droppedUnkeyedHeap;
    }

    /** Test seam: oversize-cap keys dropped before allocation (P2-175). */
    long droppedOversizeForTest() {
        return droppedOversizeHeap;
    }

    /** Test seam: emitted-ids entries for this key (P2-058 bound). */
    long emittedIdsSizeForTest() throws Exception {
        long n = 0;
        for (String ignored : emittedIds.keys()) {
            n++;
        }
        return n;
    }

    /** Test seam: shared metrics handle for one rule (P2-174). */
    SignalStrategy.Metrics metricsForTest(String ruleId) {
        return sharedMetrics == null ? null : sharedMetrics.get(ruleId);
    }

    /** Test seam: poison-TF skips counted on the shared handle (P2-059/060). */
    long metricsSkippedPoisonForTest(String ruleId) {
        return skippedPoisonHeap.getOrDefault(ruleId, 0L);
    }

    /** Test seam: heap slot count (P2-175 never-inserted check). */
    int slotCountForTest() {
        return slots.size();
    }
}
