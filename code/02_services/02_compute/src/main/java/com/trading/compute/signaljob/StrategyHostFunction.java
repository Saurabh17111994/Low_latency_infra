package com.trading.compute.signaljob;

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
 * strategies': one managed {@code MapState<String,Boolean>} keyed by the
 * emitted row's {@code candidate_id} dedups every strategy identically, so a
 * restored replay converges. A null or blank id fails the emission fast.
 *
 * <p><b>Bounds.</b> One slot per instrument with the same 65 536 global cap
 * that fails closed (G-CHAIN-2). Per-rule counters ride a
 * {@code strategy/<ruleId>} metric group, so a new strategy is observable
 * with no dashboard change.
 */
public class StrategyHostFunction
        extends KeyedCoProcessFunction<Long, RowData, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    /** Global heap slot cap (G-CHAIN-2): one slot per instrument. */
    static final int GLOBAL_SLOT_CAP = 65_536;

    private static final MapStateDescriptor<String, Boolean> EMITTED_IDS_DESC =
            new MapStateDescriptor<>("strategy-host-emitted-ids", Types.STRING, Types.BOOLEAN);

    /** Validated strategy ids, registration order (from config). */
    private final List<String> strategyIds;

    /** Job config handed to strategy constructors (rule identity, quantities). */
    private final SignalJobConfig config;

    /** Per-instrument heap slots — intentional amnesia, not checkpointed. */
    private final Map<Long, HostSlot> slots = new HashMap<>();

    private transient MapState<String, Boolean> emittedIds;
    private transient Counter suppressed;
    private transient Map<String, Counter> emittedByRule;

    // Heap mirrors for tests that bypass the metric registry.
    private transient long emittedHeap;
    private transient long suppressedHeap;

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
        emittedByRule = new HashMap<>();
        for (String id : strategyIds) {
            // Resolve eagerly: unknown ids fail here at startup, never mid-stream.
            Strategies.create(id, config, new HostMetrics(id));
            emittedByRule.put(id, getRuntimeContext().getMetricGroup()
                    .addGroup("strategy", id).counter("emitted"));
        }
    }

    /** Live forming candle (input 1): fan out to every strategy. */
    @Override
    public void processElement1(RowData live, Context ctx, Collector<RowData> out)
            throws Exception {
        HostSlot slot = slotFor(ctx.getCurrentKey());
        for (SignalStrategy s : slot.strategies.values()) {
            s.onLiveTick(live, new DedupCollector(out, s.ruleId()));
        }
    }

    /** Completed candle (input 2): fan out to every strategy. */
    @Override
    public void processElement2(RowData closed, Context ctx, Collector<RowData> out)
            throws Exception {
        HostSlot slot = slotFor(ctx.getCurrentKey());
        for (SignalStrategy s : slot.strategies.values()) {
            s.onClosedCandle(closed, new DedupCollector(out, s.ruleId()));
        }
    }

    private HostSlot slotFor(long key) {
        HostSlot slot = slots.get(key);
        if (slot == null) {
            slot = new HostSlot();
            for (String id : strategyIds) {
                slot.strategies.put(id, Strategies.create(id, config, new HostMetrics(id)));
            }
            slots.put(key, slot);
            Preconditions.checkState(slots.size() <= GLOBAL_SLOT_CAP,
                    "strategy-host slots %s exceeded cap %s — refusing silent eviction",
                    slots.size(), GLOBAL_SLOT_CAP);
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
     */
    private final class HostMetrics implements SignalStrategy.Metrics {
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
        }
    }

    /**
     * Dedups strategy emissions on {@code candidate_id} via the managed
     * emitted-ids map before forwarding. Null/blank ids fail fast: unkeyed
     * rows would defeat exactly-once.
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
            Preconditions.checkState(id != null && !id.isBlank(),
                    "strategy emitted a row with no candidate_id — refusing unkeyed emission");
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
                emittedIds.put(id, Boolean.TRUE);
            } catch (Exception e) {
                throw new IllegalStateException("strategy-host emitted-ids write failed", e);
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
}
