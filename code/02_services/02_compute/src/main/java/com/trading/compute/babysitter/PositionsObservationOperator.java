package com.trading.compute.babysitter;

import com.trading.common.schema.KvStateUpdateProtocol;
import com.trading.common.schema.position.PositionSnapshot;
import java.time.Duration;
import java.util.Objects;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Keyed (by {@code position_id}) observation operator (Task 7).
 *
 * <p>Applies {@link KvStateUpdateProtocol} semantics to the versioned
 * {@code Positions} changelog: same version/same source event is a no-op
 * (DUPLICATE); a lower version is STALE; an equal version with different
 * source event is CONFLICT (or REGRESSION for a backward version); any
 * ambiguous/unknown disposition is counted as a conflict and never applied.
 * The operator stores only the latest accepted version/freshness metadata in
 * checkpointed {@link ValueState} and <b>never emits a row</b> to its
 * collector — the terminal sink is a deliberate no-op, so no
 * {@code Position_Actions}, lifecycle, position, or execution record can be
 * produced, and there is no broker/Arrow call surface (the Babysitter cannot
 * issue broker commands).
 */
public final class PositionsObservationOperator
        extends KeyedProcessFunction<String, PositionSnapshot, Void> {

    private static final long serialVersionUID = 1L;

    /**
     * P2-115: observation-state retention — one trading day. A deleted
     * position_id never sends a DELETE through the deserializer (tombstones
     * fail validation), so without TTL every position ever seen lingers in
     * keyed state forever and a re-created key can hit a stale version gate.
     * Native TTL bounds growth and heals re-creation; processing-time (no
     * watermarks on this source) + OnCreateAndWrite + NeverReturnExpired,
     * mirroring MultiTimeframeSinks. Package-visible for the TTL-expiry test.
     */
    static final Duration OBSERVATION_STATE_TTL = Duration.ofHours(24L);

    /**
     * P2-114: staleness is enforced on arrival, not with timers — timers add
     * per-key timer state to a no-op observer. A non-positive threshold
     * disables the signal (default ctor: observer only, no freshness gate).
     */
    static final OutputTag<PositionSnapshot> STALE =
            new OutputTag<PositionSnapshot>("babysitter-observation-stale") {};

    private final long freshnessThresholdMs;

    /** Production path: freshness gate from {@code BabysitterConfig}. */
    public PositionsObservationOperator(long freshnessThresholdMs) {
        this.freshnessThresholdMs = freshnessThresholdMs;
    }

    /** Observer-only path: no freshness gate (existing tests + harness use). */
    public PositionsObservationOperator() {
        this(0L);
    }

    /**
     * Observability side-output carrying each row's {@link KvStateUpdateProtocol.Outcome}.
     * Routed to no sink in production; read by tests/health so stale, conflict,
     * and duplicate streams are observable while the MAIN output stays empty
     * (zero {@code Position_Actions}).
     */
    public static final OutputTag<KvStateUpdateProtocol.Outcome> DISPOSITION =
            new OutputTag<KvStateUpdateProtocol.Outcome>("babysitter-observation-disposition") {};

    private transient ValueState<PositionsObservationState> state;
    private transient Counter observed;
    private transient Counter applied;
    private transient Counter duplicate;
    private transient Counter stale;
    private transient Counter conflict;
    private transient Counter staleArrival;
    /**
     * P2-007: processing-thread mirror of the latest applied version for the
     * {@code latest_observed_version} gauge. Keyed state cannot be read from
     * the metrics-reporter thread (no key, wrong thread, swallowed to -1L),
     * so the gauge reads this field and {@code processElement} writes it on
     * the APPLIED branch. Gauge-only: never consulted for decisions.
     */
    private transient volatile long latestAppliedVersion = -1L;

    @Override
    public void open(OpenContext ctx) {
        latestAppliedVersion = -1L;
        ValueStateDescriptor<PositionsObservationState> desc =
                new ValueStateDescriptor<>(
                        "babysitter-position-observation",
                        TypeInformation.of(PositionsObservationState.class));
        // P2-115: bound abandoned/deleted keys instead of growing forever.
        desc.enableTimeToLive(StateTtlConfig.newBuilder(OBSERVATION_STATE_TTL)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build());
        state = getRuntimeContext().getState(desc);

        observed = getRuntimeContext().getMetricGroup()
                .counter("babysitter.positions.observed");
        applied = getRuntimeContext().getMetricGroup()
                .counter("babysitter.positions.applied");
        duplicate = getRuntimeContext().getMetricGroup()
                .counter("babysitter.positions.duplicate");
        stale = getRuntimeContext().getMetricGroup()
                .counter("babysitter.positions.stale");
        conflict = getRuntimeContext().getMetricGroup()
                .counter("babysitter.positions.conflict");
        staleArrival = getRuntimeContext().getMetricGroup()
                .counter("babysitter.positions.stale_arrival");
        getRuntimeContext().getMetricGroup().gauge(
                "babysitter.positions.latest_observed_version",
                (Gauge<Long>) this::peekSourceVersion);
    }

    @Override
    public void processElement(PositionSnapshot snap, Context ctx, Collector<Void> out)
            throws Exception {
        observed.inc();
        PositionsObservationState cur = state.value();
        // Fresh key (no prior state): the P2-006 gate below rejects a
        // negative version as UNKNOWN per protocol instead of applying it.
        KvStateUpdateProtocol.Outcome outcome;
        if (cur == null) {
            // P2-006: even first-seen must reject invalid input — a negative
            // version is UNKNOWN per protocol, not APPLIED. Only APPLIED
            // writes state, so the key stays recoverable instead of poisoned.
            outcome = snap.sourceVersion() < 0
                    ? KvStateUpdateProtocol.Outcome.UNKNOWN
                    : KvStateUpdateProtocol.Outcome.APPLIED;
        } else {
            boolean contentMatches =
                    Objects.equals(cur.getSourceEventId(), snap.sourceEventId());
            outcome = KvStateUpdateProtocol.evaluate(
                    cur.getSourceVersion(), snap.sourceVersion(), contentMatches);
        }

        switch (outcome) {
            case APPLIED -> {
                state.update(new PositionsObservationState(
                        snap.sourceVersion(),
                        snap.sourceEventId(),
                        snap.lastUpdateTs(),
                        snap.schemaVersion()));
                applied.inc();
                // P2-007 write-side: mirror for the gauge (read-side is below).
                latestAppliedVersion = snap.sourceVersion();
            }
            case DUPLICATE -> duplicate.inc();
            case STALE -> stale.inc();
            // CONFLICT, REGRESSION, UNKNOWN — any ambiguous/stale-backward
            // disposition is a conflict: never applied, never an action.
            default -> conflict.inc();
        }

        // Intentionally never call out.collect(...): the Babysitter observes
        // only. No Position_Actions, no persistence, no broker command can be
        // issued from this operator. The disposition is observable on the side
        // channel for health, never on the main stream.
        ctx.output(DISPOSITION, outcome);

        // P2-114: arrival-check freshness gate — no timers, no per-key timer
        // state. Processing-time now (no watermarks on this source) vs the
        // snapshot's lastUpdateTs; a stale arrival is counted and side-output
        // for health, never an action.
        if (freshnessThresholdMs > 0
                && ctx.timerService().currentProcessingTime() - snap.lastUpdateTs()
                        > freshnessThresholdMs) {
            staleArrival.inc();
            ctx.output(STALE, snap);
        }
    }

    /**
     * Latest accepted source version for the subtask gauge (P2-007 read-side:
     * returns the processing-thread mirror, never keyed state — no key exists
     * on the metrics thread and the backend is not thread-safe).
     */
    private long peekSourceVersion() {
        return latestAppliedVersion;
    }

}
