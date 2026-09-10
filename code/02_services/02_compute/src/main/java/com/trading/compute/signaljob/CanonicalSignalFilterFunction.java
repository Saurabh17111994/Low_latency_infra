package com.trading.compute.signaljob;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.table.data.RowData;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Canonical-signal filter (DEC-035, tracker 14 re-scoped P2 —
 * SIGNAL-SCHEMA-001): stateless pass-through for rows whose
 * {@code (schema_version, strategy_id, strategy_version, rule_id)} identity
 * equals the pinned canonical identity, drop for everything else.
 *
 * <p>Wired between the {@code signals} stream and ONLY the
 * {@code Signal_Candidates_current} KV sink — the LOG twin
 * ({@code Signal_Candidates}) keeps every emitted signal, so no audit history
 * is lost. The filter therefore never reorders, retimes, or rewrites rows: a
 * pass-through row is byte-identical to what the LOG sink writes.
 *
 * <p>Dropped rows are counted in the {@code compute.signal.kv.filtered.noncanonical}
 * MetricGroup counter — exported by the native flink-metrics-otel reporter
 * (CHG-023 item 1; the client-side ComputeOtlpEmitter mirror is gone) — and
 * WARN-logged with the row's instrument/identity so a misconfigured
 * {@code SIGNAL_STRATEGY_*} override is visible in both the O2 stream and the
 * job log.
 *
 * <p>Stateless and deterministic: {@code filter} holds no state and performs
 * no I/O, so replay/restore produces identical KV rows.
 */
public class CanonicalSignalFilterFunction extends RichFilterFunction<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(CanonicalSignalFilterFunction.class);

    /**
     * Extra admitted rule ids from the {@code STRATEGIES} config (strategy-host
     * design, 2026-09-05). Empty by default: today's behavior, pins only. A
     * strategy-host branch passes the configured ids so its rows reach the KV
     * current-state with no code change per strategy. The stub smoke id is
     * never admitted — it must stay LOG-only.
     */
    private final Set<String> extraRuleIds;

    public CanonicalSignalFilterFunction() {
        this(Set.of());
    }

    public CanonicalSignalFilterFunction(Set<String> extraRuleIds) {
        // P2-221: fail fast with a named culprit instead of a raw JDK NPE;
        // blank entries can never match (policy contract) so reject at build.
        java.util.Objects.requireNonNull(extraRuleIds, "extraRuleIds");
        for (String id : extraRuleIds) {
            if (id == null || id.isBlank()) {
                throw new IllegalArgumentException(
                        "extraRuleIds must not contain null/blank entries");
            }
        }
        Set<String> admitted = new HashSet<>(extraRuleIds);
        // The stub smoke id is LOG-only by construction: even if a config
        // lists it, its rows must never reach the KV current-state (it is a
        // counter, not a signal). Refuse it here, not just at the wiring.
        admitted.remove(StubSmokeStrategy.RULE_ID);
        this.extraRuleIds = Set.copyOf(admitted);
    }

    private transient Counter nonCanonical;

    @Override
    public void open(OpenContext openContext) {
        nonCanonical = getRuntimeContext().getMetricGroup().counter(
                "compute.signal.kv.filtered.noncanonical");
    }

    @Override
    public boolean filter(RowData row) {
        String schemaVersion = stringAt(row, SignalCandidatesTableColumns.SCHEMA_VERSION);
        String strategyId = stringAt(row, SignalCandidatesTableColumns.STRATEGY_ID);
        String strategyVersion = stringAt(row, SignalCandidatesTableColumns.STRATEGY_VERSION);
        String ruleId = stringAt(row, SignalCandidatesTableColumns.RULE_ID);
        boolean canonical = CanonicalSignalPolicy.isCanonical(
                schemaVersion, strategyId, strategyVersion, ruleId,
                SignalCandidatesTableColumns.SCHEMA_VERSION_V2,
                SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID,
                SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION,
                SignalCandidatesTableColumns.CANONICAL_RULE_ID,
                // Slice 2.2 (Phase C): the forming-bar placeholder rule is
                // the second pinned canonical rule id — its candidates reach
                // the KV current-state like candle candidates (REQ-SS-003 +
                // DEC-035 dual-sink). N7 (2026-09-05): the N7 range-breakout
                // rule is the third admitted id.
                SignalCandidatesTableColumns.CANONICAL_FORMING_RULE_ID,
                SignalCandidatesTableColumns.CANONICAL_N7_RULE_ID)
                || (!extraRuleIds.isEmpty() && CanonicalSignalPolicy.isCanonicalIn(
                        schemaVersion, strategyId, strategyVersion, ruleId,
                        SignalCandidatesTableColumns.SCHEMA_VERSION_V2,
                        SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID,
                        SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION,
                        extraRuleIds));
        if (!canonical) {
            // P2-129: direct unit-test invocation without open() must not NPE.
            if (nonCanonical != null) {
                nonCanonical.inc();
            }
            // P2-025: a benign drop must never become a task-killing NPE —
            // guard the token like every other identity column.
            Object instrumentToken = row.isNullAt(SignalCandidatesTableColumns.INSTRUMENT_TOKEN)
                    ? null : row.getLong(SignalCandidatesTableColumns.INSTRUMENT_TOKEN);
            LOG.warn("signal-canonical-filter: dropping non-canonical signal from the KV "
                    + "current-state (instrument={}, schema={}, strategy={}:{}, rule={}) — the "
                    + "LOG twin keeps every signal",
                    instrumentToken,
                    schemaVersion, strategyId, strategyVersion, ruleId);
        }
        return canonical;
    }

    /** Counter-source accessor (tests): the value the MetricGroup counter exports. */
    long filteredCountForTest() {
        return nonCanonical == null ? 0L : nonCanonical.getCount();
    }

    private static String stringAt(RowData row, int index) {
        return row.isNullAt(index) ? null : row.getString(index).toString();
    }
}
