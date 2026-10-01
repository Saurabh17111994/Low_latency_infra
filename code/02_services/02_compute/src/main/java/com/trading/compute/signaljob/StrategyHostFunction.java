package com.trading.compute.signaljob;

import com.trading.compute.feature.FeatureView;
import com.trading.compute.feature.PerInstrumentFeatures;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
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
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
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
 * input 1 = live forming candles ({@link CandleLiveColumns}; one row per
 * timeframe per accepted trade tick on the fast feed, or the six-row 1s
 * snapshot fallback), input 2 = completed candles
 * ({@link CandleClosedColumns}). Each input fans out to every registered
 * strategy of that instrument, in registration order. Every callback receives
 * the slot's stable {@link StrategyView} bundle (market + features + context,
 * 2026-10-01): the market snapshot is decoded from the canonical
 * {@code FIFTEEN_S} forming row only and is available on the live, closed and
 * context-ready paths.
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

    /**
     * C2: delay between a context miss and the keyed processing-time wake-up
     * that promotes the completed fetch and re-evaluates waiting strategies
     * (~2 ms; the lookup itself typically completes in single-digit ms).
     */
    static final long CONTEXT_WAKEUP_DELAY_MS = 2L;

    /** P2-058 horizon: newest-N ids kept per (rule, tf) ledger on compaction. */
    static final int EMITTED_IDS_RETAIN_PER_LEDGER = 16;

    /**
     * H5-1: ids written after the upgrade live in one map per rule, so pruning
     * scans one rule's ledger instead of the whole subtask map. The legacy
     * descriptor below stays registered (read-only) so a pre-upgrade
     * checkpoint's ids still suppress.
     */
    private static final String EMITTED_IDS_V2_PREFIX = "strategy-host-emitted-ids-v2/";

    private static final MapStateDescriptor<String, Long> EMITTED_IDS_DESC =
            new MapStateDescriptor<>("strategy-host-emitted-ids", Types.STRING, Types.LONG);

    private static MapStateDescriptor<String, Long> emittedIdsV2Desc(String ruleId) {
        return new MapStateDescriptor<>(EMITTED_IDS_V2_PREFIX + ruleId, Types.STRING, Types.LONG);
    }

    /**
     * Wave B (DEC-059): side output of merged {@code candle_features} rows —
     * the sealed row of each closed window (closed-only storage, 2026-10-01:
     * the live path writes nothing). Wired unconditionally to the merged sink
     * in {@code SignalJob}; the host is the ONLY candle writer (DEC-059).
     */
    public static final OutputTag<RowData> MERGED_ROWS =
            new OutputTag<RowData>("merged-candle-rows") {};

    /** Validated strategy ids, registration order (from config). */
    private final List<String> strategyIds;

    /** Job config handed to strategy constructors (rule identity, quantities). */
    private final SignalJobConfig config;

    /**
     * C1 test seam: builds the context provider. {@code null} (production)
     * resolves to {@link ContextProvider#open}. Transient — a test sets it on
     * the instance before the harness opens; Flink never serializes it.
     */
    private transient ContextProviderFactory providerFactory;

    /** C1: on-demand closed-candle provider — null unless {@code STRATEGY_CONTEXT_ENABLED}. */
    private transient ContextProvider contextProvider;

    /**
     * Strategy-facing view over {@link #contextProvider}; always non-null
     * after {@code open()} — the disabled singleton when the flag is off (C2).
     */
    private transient ContextView contextView;

    /**
     * C2: last live forming-candle snapshot per token, kept only while that
     * token has a pending context request (bounded by the in-flight cap), plus
     * the armed flag so at most one wake-up timer per token is outstanding.
     */
    private transient Map<Long, PendingLive> pendingLiveSnapshot;

    /** Per-instrument heap slots — intentional amnesia, not checkpointed. */
    private final Map<Long, HostSlot> slots = new HashMap<>();

    private transient MapState<String, Long> emittedIds;
    /** H5-1: per-rule ledgers for ids written after the upgrade (lazy). */
    private transient Map<String, MapState<String, Long>> emittedIdsByRule;
    /** H5-1: new ids since the last prune, per rule (transient, not checkpointed). */
    private transient Map<String, Integer> emittedIdsSincePrune;
    /** H5-1 test seam: prune scans performed (must stay ~emissions / horizon). */
    private transient long pruneScans;
    /** H5-1 test seam: a legacy id to seed on the next collect (keyed context active). */
    private transient String seedLegacyOnNextCollectId;
    private transient long seedLegacyOnNextCollectTs;
    private transient Counter suppressed;
    private transient Counter failedStrategy;
    private transient Counter droppedUnkeyed;
    private transient Counter droppedOversize;
    private transient Map<String, Counter> emittedByRule;
    private transient Map<String, HostMetrics> sharedMetrics;
    /** In-memory signal-read age: tick event-time -> strategy evaluation (low-latency KPI). */
    private transient Histogram liveAge;

    /**
     * Platform-speed KPI (2026-10-01): ingestion accept wall-clock
     * ({@code raw.ingest_ts}, carried on the forming row) -> strategy
     * evaluation. Feed-independent, unlike {@link #liveAge}.
     */
    private transient Histogram ingestToStrategy;

    /** DEC-056 feature-layer counters (per subtask). */
    private transient Counter featureTickUpdates;
    private transient Counter featureCloseUpdates;
    private transient Counter featureFailures;
    private transient Counter mergedRowsEmitted;

    // Heap mirrors for tests that bypass the metric registry.
    private transient long emittedHeap;
    private transient long suppressedHeap;
    private transient long failedStrategyHeap;
    private transient long droppedUnkeyedHeap;
    private transient long droppedOversizeHeap;
    private transient long featureTickUpdatesHeap;
    private transient long featureCloseUpdatesHeap;
    private transient long featureFailuresHeap;
    private transient long mergedRowsEmittedHeap;
    private final transient Map<String, Long> skippedPoisonHeap = new HashMap<>();

    public StrategyHostFunction(SignalJobConfig config, List<String> strategyIds) {
        this(config, strategyIds, null);
    }

    /**
     * @param providerFactory C1 test seam for the context provider; {@code null}
     *     resolves to {@link ContextProvider#open} (a real Fluss connection)
     *     when {@code STRATEGY_CONTEXT_ENABLED=true}
     */
    StrategyHostFunction(
            SignalJobConfig config,
            List<String> strategyIds,
            ContextProviderFactory providerFactory) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.strategyIds = List.copyOf(Preconditions.checkNotNull(strategyIds, "strategyIds"));
        this.providerFactory = providerFactory;
    }

    /**
     * Builds the operator's context provider (C1). The production default is
     * {@link ContextProvider#open}; tests inject a fake-backed provider so the
     * open/close lifecycle is provable without a Fluss cluster.
     */
    @FunctionalInterface
    interface ContextProviderFactory {
        ContextProvider create(SignalJobConfig config, MetricGroup metrics) throws Exception;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        if (strategyIds.isEmpty()) {
            throw new IllegalStateException("strategy-host opened with zero strategies — "
                    + "STRATEGY_HOST_ENABLED=true requires a non-empty STRATEGIES list");
        }
        slots.clear();
        emittedIds = getRuntimeContext().getMapState(EMITTED_IDS_DESC);
        emittedIdsByRule = new HashMap<>();
        emittedIdsSincePrune = new HashMap<>();
        pruneScans = 0;
        suppressed = getRuntimeContext().getMetricGroup().counter("compute.strategy.suppressed");
        failedStrategy =
                getRuntimeContext().getMetricGroup().counter("compute.strategy.failed");
        droppedUnkeyed =
                getRuntimeContext().getMetricGroup().counter("compute.strategy.dropped.unkeyed");
        droppedOversize =
                getRuntimeContext().getMetricGroup().counter("compute.strategy.dropped.oversize");
        // Low-latency KPI (2026-09-26): tick event-time -> in-memory signal
        // read, sampled at evaluation. Same histogram shape as
        // compute.latency.ingest_to_monitor.
        liveAge = getRuntimeContext().getMetricGroup()
                .histogram("compute.latency.tick_to_strategy", LatencyHistograms.create());
        // Platform-speed KPI (2026-10-01): ingestion accept (raw.ingest_ts,
        // carried on the forming row by the aggregator) -> in-memory signal
        // read. Skips unset/clock-skewed probes (see updateIngestToStrategy).
        ingestToStrategy = getRuntimeContext().getMetricGroup()
                .histogram("compute.latency.ingest_to_strategy", LatencyHistograms.create());
        featureTickUpdates =
                getRuntimeContext().getMetricGroup().counter("compute.features.updates.tick");
        featureCloseUpdates =
                getRuntimeContext().getMetricGroup().counter("compute.features.updates.close");
        featureFailures =
                getRuntimeContext().getMetricGroup().counter("compute.features.failed");
        mergedRowsEmitted =
                getRuntimeContext().getMetricGroup().counter("compute.merged.rows.emitted");
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
        if (config.strategyContextEnabled()) {
            ContextProviderFactory factory =
                    providerFactory != null ? providerFactory : ContextProvider::open;
            contextProvider = factory.create(config, getRuntimeContext().getMetricGroup());
            contextView = contextProvider.view();
            pendingLiveSnapshot = new HashMap<>();
            LOG.info("strategy-host: context provider ready (table={}, cacheBytes={}, "
                            + "maxInflight={}, fetchTimeoutMs={})",
                    config.candleContextTable(), config.contextCacheBytes(),
                    config.contextMaxInflight(), config.contextFetchTimeoutMs());
        } else {
            contextView = ContextView.disabled();
        }
    }

    /**
     * C1/C2: closes the context provider (and its Fluss client) when
     * {@code STRATEGY_CONTEXT_ENABLED} opened one; a no-op otherwise, so
     * flag-off behavior is unchanged.
     */
    @Override
    public void close() throws Exception {
        if (contextProvider != null) {
            contextProvider.close();
            contextProvider = null;
            contextView = null;
            pendingLiveSnapshot = null;
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
        String tfCode = live.isNullAt(CandleLiveColumns.TF)
                ? null : live.getString(CandleLiveColumns.TF).toString();
        // 2026-10-02 (CHG-505): market-only row (TF="MKT") — refresh the shared
        // snapshot and fan out to the market-update callback only. Never a
        // forming candle: no features, no KPI, no onLiveTick, no context. The
        // callback's default is a no-op, so existing strategies are unchanged.
        if (CandleLiveColumns.TF_MARKET_TICK.equals(tfCode)) {
            decodeMarketSnapshot(slot.market, live);
            for (SignalStrategy s : slot.strategies.values()) {
                try {
                    s.onMarketUpdate(live, slot.view, new DedupCollector(out, s.ruleId()));
                } catch (Exception e) {
                    countFailedStrategy();
                    LOG.warn("strategy-host: dropping failed onMarketUpdate rule={} token={}: {}",
                            s.ruleId(), ctx.getCurrentKey(), e.toString());
                }
            }
            return;
        }
        if (contextProvider != null) {
            // C2: promote any completed fetch before the fan-out, so a
            // strategy whose data arrived since the last tick sees it now
            // (the wake-up timer covers the between-ticks case).
            contextProvider.drainArrivals();
        }
        // Low-latency KPI: age of the tick this in-memory read is based on.
        long evtTime = live.getLong(CandleLiveColumns.LAST_EVENT_TIME);
        // 2026-10-01: all six TF forming rows arrive per tick (fast feed). The
        // canonical FIFTEEN_S row owns the per-tick KPI and the C2 pending
        // snapshot; every row fans out to strategies below.
        boolean canonicalTick = Timeframe.FIFTEEN_S.code().equals(tfCode);
        if (canonicalTick) {
            // 2026-10-01 native design: the canonical row is the only carrier
            // of the market snapshot (one row per tick, not six). Decode it
            // into the slot's MarketSnapshot before the fan-out so every
            // strategy reads the fresh values through the shared view. A
            // non-canonical row never touches the snapshot.
            decodeMarketSnapshot(slot.market, live);
        }
        if (canonicalTick && liveAge != null && evtTime > 0L) {
            liveAge.update(Math.max(0L, System.currentTimeMillis() - evtTime));
        }
        // Platform-speed KPI (2026-10-01): same canonical row, but anchored at
        // our own accept time — the feed-independent "how fast are we" number.
        if (canonicalTick && ingestToStrategy != null
                && !live.isNullAt(CandleLiveColumns.INGEST_TS)) {
            updateIngestToStrategy(ingestToStrategy,
                    live.getLong(CandleLiveColumns.INGEST_TS),
                    System.currentTimeMillis());
        }
        // DEC-056: features update before the fan-out so every strategy reads the
        // fresh value; a failing feature update is counted, never blocks delivery.
        // (Inside, only FIFTEEN_S updates the timeframe-independent tick features.)
        updateFeaturesOnTick(slot, live, evtTime);
        // Closed-only storage (2026-10-01, DEC-059): the live path writes
        // NOTHING to Fluss. Forming candles and their features live in Flink
        // memory (this host + the aggregator) for the strategy fan-out; the
        // merged table receives exactly one sealed row per window at close
        // (processElement2).
        for (SignalStrategy s : slot.strategies.values()) {
            // P2-057: one strategy must not starve the others — isolate,
            // count, and continue to the next strategy.
            try {
                s.onLiveTick(live, slot.view, new DedupCollector(out, s.ruleId()));
            } catch (Exception e) {
                countFailedStrategy();
                LOG.warn("strategy-host: dropping failed onLiveTick rule={} token={}: {}",
                        s.ruleId(), ctx.getCurrentKey(), e.toString());
            }
        }
        // C2: while a request is pending the retained snapshot is the freshest
        // live row. With the all-TF feed that is the last TF row of the tick
        // (same event time/price); the KPI and stored forming row above stay
        // pinned to the canonical FIFTEEN_S row.
        if (contextProvider != null) {
            trackPendingContext(ctx.getCurrentKey(), live, ctx.timerService());
        }
    }

    /**
     * Guarded {@code compute.latency.ingest_to_strategy} update (platform-speed
     * KPI): skips an unset probe (sentinel/non-positive) and a clock-skewed
     * sample ({@code now < ingestTs}) — the same fail-safe shape as the step-2
     * monitor. Package-private for the unit guard test.
     */
    static void updateIngestToStrategy(Histogram histogram, long ingestTs, long now) {
        if (histogram != null && ingestTs > 0L && now >= ingestTs) {
            histogram.update(now - ingestTs);
        }
    }

    /**
     * Decodes the 44-column market section of the canonical forming row into
     * the slot's {@link MarketSnapshot} (2026-10-01 native design). NULL means
     * "never seen / not provided" -> 0, the {@link MarketView} convention; the
     * aggregator already carries the latest-known value of every field on this
     * row, so overwriting is exact — no merge against the previous row.
     * Package-private for the unit guard test.
     */
    static void decodeMarketSnapshot(MarketSnapshot m, RowData live) {
        m.totalBuyQty = longOrZero(live, CandleLiveColumns.MKT_TOTAL_BUY_QTY);
        m.totalSellQty = longOrZero(live, CandleLiveColumns.MKT_TOTAL_SELL_QTY);
        m.dayOpenPaise = longOrZero(live, CandleLiveColumns.MKT_DAY_OPEN_PAISE);
        m.dayHighPaise = longOrZero(live, CandleLiveColumns.MKT_DAY_HIGH_PAISE);
        m.dayLowPaise = longOrZero(live, CandleLiveColumns.MKT_DAY_LOW_PAISE);
        m.prevClosePaise = longOrZero(live, CandleLiveColumns.MKT_PREV_CLOSE_PAISE);
        m.vwapPaise = longOrZero(live, CandleLiveColumns.MKT_VWAP_PAISE);
        m.openInterest = longOrZero(live, CandleLiveColumns.MKT_OPEN_INTEREST);
        m.oiDayHigh = longOrZero(live, CandleLiveColumns.MKT_OI_DAY_HIGH);
        m.oiDayLow = longOrZero(live, CandleLiveColumns.MKT_OI_DAY_LOW);
        m.lowerLimitPaise = longOrZero(live, CandleLiveColumns.MKT_LOWER_LIMIT_PAISE);
        m.upperLimitPaise = longOrZero(live, CandleLiveColumns.MKT_UPPER_LIMIT_PAISE);
        m.bidPx1 = longOrZero(live, CandleLiveColumns.MKT_BID_PX_1);
        m.bidPx2 = longOrZero(live, CandleLiveColumns.MKT_BID_PX_2);
        m.bidPx3 = longOrZero(live, CandleLiveColumns.MKT_BID_PX_3);
        m.bidPx4 = longOrZero(live, CandleLiveColumns.MKT_BID_PX_4);
        m.bidPx5 = longOrZero(live, CandleLiveColumns.MKT_BID_PX_5);
        m.bidQty1 = longOrZero(live, CandleLiveColumns.MKT_BID_QTY_1);
        m.bidQty2 = longOrZero(live, CandleLiveColumns.MKT_BID_QTY_2);
        m.bidQty3 = longOrZero(live, CandleLiveColumns.MKT_BID_QTY_3);
        m.bidQty4 = longOrZero(live, CandleLiveColumns.MKT_BID_QTY_4);
        m.bidQty5 = longOrZero(live, CandleLiveColumns.MKT_BID_QTY_5);
        m.bidOrd1 = longOrZero(live, CandleLiveColumns.MKT_BID_ORD_1);
        m.bidOrd2 = longOrZero(live, CandleLiveColumns.MKT_BID_ORD_2);
        m.bidOrd3 = longOrZero(live, CandleLiveColumns.MKT_BID_ORD_3);
        m.bidOrd4 = longOrZero(live, CandleLiveColumns.MKT_BID_ORD_4);
        m.bidOrd5 = longOrZero(live, CandleLiveColumns.MKT_BID_ORD_5);
        m.askPx1 = longOrZero(live, CandleLiveColumns.MKT_ASK_PX_1);
        m.askPx2 = longOrZero(live, CandleLiveColumns.MKT_ASK_PX_2);
        m.askPx3 = longOrZero(live, CandleLiveColumns.MKT_ASK_PX_3);
        m.askPx4 = longOrZero(live, CandleLiveColumns.MKT_ASK_PX_4);
        m.askPx5 = longOrZero(live, CandleLiveColumns.MKT_ASK_PX_5);
        m.askQty1 = longOrZero(live, CandleLiveColumns.MKT_ASK_QTY_1);
        m.askQty2 = longOrZero(live, CandleLiveColumns.MKT_ASK_QTY_2);
        m.askQty3 = longOrZero(live, CandleLiveColumns.MKT_ASK_QTY_3);
        m.askQty4 = longOrZero(live, CandleLiveColumns.MKT_ASK_QTY_4);
        m.askQty5 = longOrZero(live, CandleLiveColumns.MKT_ASK_QTY_5);
        m.askOrd1 = longOrZero(live, CandleLiveColumns.MKT_ASK_ORD_1);
        m.askOrd2 = longOrZero(live, CandleLiveColumns.MKT_ASK_ORD_2);
        m.askOrd3 = longOrZero(live, CandleLiveColumns.MKT_ASK_ORD_3);
        m.askOrd4 = longOrZero(live, CandleLiveColumns.MKT_ASK_ORD_4);
        m.askOrd5 = longOrZero(live, CandleLiveColumns.MKT_ASK_ORD_5);
        m.statsChangedAt = longOrZero(live, CandleLiveColumns.MKT_STATS_CHANGED_AT);
        m.depthChangedAt = longOrZero(live, CandleLiveColumns.MKT_DEPTH_CHANGED_AT);
    }

    private static long longOrZero(RowData row, int index) {
        return row.isNullAt(index) ? 0L : row.getLong(index);
    }

    /** Completed candle (input 2): fan out to every strategy. */
    @Override
    public void processElement2(RowData closed, Context ctx, Collector<RowData> out)
            throws Exception {
        HostSlot slot = slotFor(ctx.getCurrentKey());
        if (slot == null) {
            return;
        }
        // DEC-056: close features update before the fan-out (same contract as the tick path).
        updateFeaturesOnClose(slot, closed);
        // Wave B (DEC-059): the final sealed write — the terminal row of
        // this window (late ticks can never rewrite it). The only Fluss write
        // on the host's path (closed-only storage, 2026-10-01).
        emitSealedMergedRow(ctx, slot, closed);
        for (SignalStrategy s : slot.strategies.values()) {
            // P2-057: same isolation on the closed path.
            try {
                s.onClosedCandle(closed, slot.view, new DedupCollector(out, s.ruleId()));
            } catch (Exception e) {
                countFailedStrategy();
                LOG.warn("strategy-host: dropping failed onClosedCandle rule={} token={}: {}",
                        s.ruleId(), ctx.getCurrentKey(), e.toString());
            }
        }
    }

    /**
     * C2 context wake-up: promotes completed fetches and re-evaluates waiting
     * strategies on the live snapshot, then re-arms while the token still has
     * a pending request. Never fires unless the provider flag is on and a
     * strategy asked for context.
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out)
            throws Exception {
        ContextProvider provider = contextProvider;
        if (provider == null || pendingLiveSnapshot == null) {
            return;
        }
        long key = ctx.getCurrentKey();
        PendingLive self = pendingLiveSnapshot.get(key);
        if (self != null) {
            self.wakeupArmed = false;
        }
        List<ContextKey> promoted = provider.drainArrivals();
        for (ContextKey ready : promoted) {
            long token = ready.token();
            HostSlot slot = slots.get(token);
            PendingLive pending = pendingLiveSnapshot.get(token);
            if (slot == null || pending == null || pending.live == null) {
                continue;
            }
            for (SignalStrategy s : slot.strategies.values()) {
                try {
                    s.onContextReady(pending.live, slot.view,
                            new DedupCollector(out, s.ruleId()));
                } catch (Exception e) {
                    countFailedStrategy();
                    LOG.warn("strategy-host: dropping failed onContextReady rule={} token={}: {}",
                            s.ruleId(), token, e.toString());
                }
            }
        }
        // The firing key's wake-up lifecycle. Timers are keyed, so only this
        // key can be re-armed from here; a still-pending token keeps its own
        // timer (or its next live tick re-arms it).
        if (provider.hasPendingForToken(key)) {
            armWakeup(key, ctx.timerService());
        } else {
            pendingLiveSnapshot.remove(key);
        }
        // A token whose data was just dispatched and has no further request
        // needs no snapshot anymore.
        for (ContextKey ready : promoted) {
            long token = ready.token();
            if (token != key && !provider.hasPendingForToken(token)) {
                pendingLiveSnapshot.remove(token);
            }
        }
    }

    /**
     * C2: after a live fan-out, keep the freshest live snapshot for the token
     * while any strategy has a pending context request and make sure a wake-up
     * timer is armed; once nothing is pending the snapshot is dropped.
     */
    private void trackPendingContext(long token, RowData live, TimerService timers) {
        if (pendingLiveSnapshot == null) {
            return;
        }
        if (contextProvider.hasPendingForToken(token)) {
            PendingLive pending = pendingLiveSnapshot.get(token);
            if (pending == null) {
                pending = new PendingLive();
                pendingLiveSnapshot.put(token, pending);
            }
            pending.live = copyLiveRow(live);
            armWakeup(token, timers);
        } else {
            pendingLiveSnapshot.remove(token);
        }
    }

    /** C2: arms at most one processing-time wake-up per pending token. */
    private void armWakeup(long token, TimerService timers) {
        PendingLive pending = pendingLiveSnapshot.get(token);
        if (pending == null || pending.wakeupArmed) {
            return;
        }
        pending.wakeupArmed = true;
        timers.registerProcessingTimeTimer(timers.currentProcessingTime() + CONTEXT_WAKEUP_DELAY_MS);
    }

    /**
     * C2: private copy of the live row retained while a token has a pending
     * request. Generic rows are copied field-by-field so the snapshot can
     * never alias a reused input object; any other RowData implementation is
     * retained by reference (this job never enables object reuse, and every
     * row on this path is a GenericRowData).
     */
    private static RowData copyLiveRow(RowData live) {
        if (live instanceof GenericRowData generic) {
            GenericRowData copy = new GenericRowData(generic.getArity());
            for (int i = 0; i < generic.getArity(); i++) {
                copy.setField(i, generic.getField(i));
            }
            return copy;
        }
        return live;
    }

    /** C2: the live snapshot for one token while its context request is pending. */
    private static final class PendingLive {
        RowData live;
        boolean wakeupArmed;
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
            slot = new HostSlot(contextView);
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

        /** DEC-056: this instrument's shared feature state — computed once, read by every strategy. */
        final PerInstrumentFeatures features = new PerInstrumentFeatures();

        /** Latest market snapshot, decoded from the canonical forming row (2026-10-01). */
        final MarketSnapshot market = new MarketSnapshot();

        /** The shared bundle handed to every strategy callback (stable per slot). */
        final StrategyView view;

        HostSlot(ContextView context) {
            this.view = new SlotView(market, features, context);
        }
    }

    /**
     * The per-slot {@link StrategyView} implementation: three fixed
     * references, no per-callback allocation. A null context (only possible
     * if a slot were created outside the open lifecycle) resolves to the
     * disabled singleton so the view never exposes a null component.
     */
    private static final class SlotView implements StrategyView {
        private final MarketView market;
        private final FeatureView features;
        private final ContextView context;

        SlotView(MarketView market, FeatureView features, ContextView context) {
            this.market = market;
            this.features = features;
            this.context = context == null ? ContextView.disabled() : context;
        }

        @Override
        public MarketView market() {
            return market;
        }

        @Override
        public FeatureView features() {
            return features;
        }

        @Override
        public ContextView context() {
            return context;
        }
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
            if (seedLegacyOnNextCollectId != null) {
                try {
                    emittedIds.put(seedLegacyOnNextCollectId, seedLegacyOnNextCollectTs);
                } catch (Exception e) {
                    throw new IllegalStateException("strategy-host legacy seed failed", e);
                }
                seedLegacyOnNextCollectId = null;
            }
            MapState<String, Long> ruleLedger;
            boolean seen;
            try {
                ruleLedger = ruleLedger(ruleId);
                seen = emittedIds.contains(id) || ruleLedger.contains(id);
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
                ruleLedger.put(id, detTs);
            } catch (Exception e) {
                throw new IllegalStateException("strategy-host emitted-ids write failed", e);
            }
            // H5-1: prune this rule's ledger every EMITTED_IDS_RETAIN_PER_LEDGER new
            // ids — the emission path never scans the map, and the legacy map is
            // never rewritten.
            try {
                int since = emittedIdsSincePrune.merge(ruleId, 1, Integer::sum);
                if (since >= EMITTED_IDS_RETAIN_PER_LEDGER) {
                    emittedIdsSincePrune.put(ruleId, 0);
                    pruneRuleLedger(ruleLedger);
                }
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

    /** H5-1: the per-rule ledger, created on first use. */
    private MapState<String, Long> ruleLedger(String ruleId) throws Exception {
        MapState<String, Long> ledger = emittedIdsByRule.get(ruleId);
        if (ledger == null) {
            ledger = getRuntimeContext().getMapState(emittedIdsV2Desc(ruleId));
            emittedIdsByRule.put(ruleId, ledger);
        }
        return ledger;
    }

    /**
     * H5-1 horizon-compaction: keep the newest
     * {@link #EMITTED_IDS_RETAIN_PER_LEDGER} ids of ONE rule's ledger, evicting
     * the oldest detection-ts entries; a null value is never evicted. Called once
     * per {@link #EMITTED_IDS_RETAIN_PER_LEDGER} new ids for that rule, so the
     * emission path never scans the map.
     */
    private void pruneRuleLedger(MapState<String, Long> ledger) throws Exception {
        pruneScans++;
        List<Map.Entry<String, Long>> entries = new ArrayList<>();
        for (Map.Entry<String, Long> e : ledger.entries()) {
            entries.add(e);
        }
        if (entries.size() <= EMITTED_IDS_RETAIN_PER_LEDGER) {
            return;
        }
        entries.removeIf(e -> e.getValue() == null);
        entries.sort(Comparator.comparingLong(e -> e.getValue()));
        int evict = entries.size() - EMITTED_IDS_RETAIN_PER_LEDGER;
        for (int i = 0; i < evict; i++) {
            ledger.remove(entries.get(i).getKey());
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

    /**
     * DEC-056 feature update on the live tick. A throwing computer is counted
     * and dropped — it must never kill the subtask or block the strategy
     * fan-out (same isolation contract as strategies themselves). Every row
     * records the evolving candle for its timeframe (2026-10-01) so a strategy
     * can read any timeframe's forming features in memory; L3-4: only the
     * canonical tick timeframe updates the timeframe-independent TICK features
     * (the other five forming rows of a snapshot still fan out to strategies
     * but must not touch them).
     */
    private void updateFeaturesOnTick(HostSlot slot, RowData live, long eventTimeMs) {
        try {
            Timeframe tf = Timeframe.fromCode(live.getString(CandleLiveColumns.TF).toString());
            // 2026-10-01: every forming row (all six TFs per tick on the fast
            // feed, all six of a snapshot on the fallback) updates the live
            // feature view — FeatureView.latestLive previews a CLOSE feature as
            // if this candle closed now. Heap only: no storage, no state growth.
            slot.features.onFormingCandle(
                    tf,
                    live.getLong(CandleLiveColumns.WINDOW_START),
                    live.getLong(CandleLiveColumns.OPEN_PAISE),
                    live.getLong(CandleLiveColumns.HIGH_PAISE),
                    live.getLong(CandleLiveColumns.LOW_PAISE),
                    live.getLong(CandleLiveColumns.CLOSE_PAISE),
                    live.getLong(CandleLiveColumns.VOLUME),
                    live.getInt(CandleLiveColumns.TICK_COUNT));
            // L3-4: the MULTITF_FAST_LIVE_FEED=false fallback feeds all six TF
            // forming rows of one snapshot into this operator. TICK features are
            // timeframe-independent (one computer per instrument), so feeding every
            // row recomputed the same value and counted six updates per snapshot;
            // only the canonical tick timeframe updates them. Every row still fans
            // out to strategies (the caller's loop is unchanged). An unparseable TF
            // is counted like the close path — never thrown into the fan-out.
            if (tf != Timeframe.FIFTEEN_S) {
                return;
            }
            slot.features.onTick(
                    eventTimeMs,
                    live.getLong(CandleLiveColumns.CLOSE_PAISE),
                    live.getLong(CandleLiveColumns.VOLUME),
                    live.getInt(CandleLiveColumns.TICK_COUNT));
            featureTickUpdates.inc();
            featureTickUpdatesHeap++;
        } catch (Exception e) {
            countFeatureFailure("tick", e);
        }
    }

    /**
     * DEC-056 feature update on the closed candle, before the fan-out. The
     * timeframe comes from the row; an unknown code is counted, never thrown
     * into the fan-out.
     */
    private void updateFeaturesOnClose(HostSlot slot, RowData closed) {
        try {
            Timeframe tf = Timeframe.fromCode(closed.getString(CandleClosedColumns.TF).toString());
            slot.features.onClosedCandle(
                    tf,
                    closed.getLong(CandleClosedColumns.WINDOW_START),
                    closed.getLong(CandleClosedColumns.WINDOW_END),
                    closed.getLong(CandleClosedColumns.OPEN_PAISE),
                    closed.getLong(CandleClosedColumns.HIGH_PAISE),
                    closed.getLong(CandleClosedColumns.LOW_PAISE),
                    closed.getLong(CandleClosedColumns.CLOSE_PAISE),
                    closed.getLong(CandleClosedColumns.VOLUME),
                    closed.getInt(CandleClosedColumns.TICK_COUNT));
            featureCloseUpdates.inc();
            featureCloseUpdatesHeap++;
        } catch (Exception e) {
            countFeatureFailure("close", e);
        }
    }

    private void countFeatureFailure(String kind, Exception e) {
        featureFailures.inc();
        featureFailuresHeap++;
        LOG.warn("strategy-host: feature update failed kind={}: {}", kind, e.toString());
    }

    /**
     * Closed-only storage (2026-10-01, DEC-059): one merged
     * {@code candle_features} row for a closed window — the candle row's 15
     * columns + every ready feature of that timeframe + the seal flag. The
     * host is the only writer and this is its only storage call; a failure
     * here is counted, never delivered into the strategy fan-out.
     */
    private void emitSealedMergedRow(Context ctx, HostSlot slot, RowData candle) {
        try {
            Timeframe tf = Timeframe.fromCode(candle.getString(CandleClosedColumns.TF).toString());
            Map<Integer, Double> snapshot = new HashMap<>();
            slot.features.snapshot(tf, snapshot);
            GenericRowData row = MergedCandleRows.fromCandleRow(candle, snapshot, true);
            ctx.output(MERGED_ROWS, row);
            mergedRowsEmitted.inc();
            mergedRowsEmittedHeap++;
        } catch (Exception e) {
            countFeatureFailure("emit-merged", e);
        }
    }

    /** Test seam: merged rows emitted by this subtask. */
    long mergedRowsEmittedForTest() {
        return mergedRowsEmittedHeap;
    }

    /** Test seam: live strategy instance for one instrument. */
    SignalStrategy strategyForTest(long token, String ruleId) {
        HostSlot slot = slots.get(token);
        return slot == null ? null : slot.strategies.get(ruleId);
    }

    /** C1 test seam: the context provider built by open() (null when disabled). */
    ContextProvider contextProviderForTest() {
        return contextProvider;
    }

    /** C1 test seam: the strategy-facing context view (null when disabled). */
    ContextView contextViewForTest() {
        return contextView;
    }

    /** C2 test seam: tokens with a retained live snapshot (pending context). */
    int pendingLiveCountForTest() {
        return pendingLiveSnapshot == null ? 0 : pendingLiveSnapshot.size();
    }

    /** Test seam: one instrument's shared feature state, or null when no slot exists. */
    PerInstrumentFeatures featuresForTest(long token) {
        HostSlot slot = slots.get(token);
        return slot == null ? null : slot.features;
    }

    /** Test seam: tick feature updates applied by this subtask. */
    long featureTickUpdatesForTest() {
        return featureTickUpdatesHeap;
    }

    /** Test seam: close feature updates applied by this subtask. */
    long featureCloseUpdatesForTest() {
        return featureCloseUpdatesHeap;
    }

    /** Test seam: feature updates dropped by the fail-open guard. */
    long featureFailuresForTest() {
        return featureFailuresHeap;
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

    /** Test seam: emitted-ids entries across the legacy and per-rule ledgers (P2-058/H5-1). */
    long emittedIdsSizeForTest() throws Exception {
        long n = 0;
        for (String ignored : emittedIds.keys()) {
            n++;
        }
        for (MapState<String, Long> ledger : emittedIdsByRule.values()) {
            for (String ignored : ledger.keys()) {
                n++;
            }
        }
        return n;
    }

    /** Test seam: prune scans performed (H5-1 — must stay ~emissions / horizon). */
    long pruneScansForTest() {
        return pruneScans;
    }

    /** Test seam: seed a legacy (pre-upgrade) id on the next collect, inside the keyed context. */
    void seedLegacyOnNextCollectForTest(String id, long detTs) {
        this.seedLegacyOnNextCollectId = id;
        this.seedLegacyOnNextCollectTs = detTs;
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
