package com.trading.compute.signaljob;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raw-tick schema/validity gate (Signal dossier operator 1).
 *
 * <p>Passes only rows that the candle accumulator may consume; everything else
 * is dropped with a per-reason metric. The ingestion pipeline already quarantines
 * invalid values, so this gate is a defense-in-depth re-check at the compute
 * boundary — it must never silently accept a row the contract excludes:
 * <ul>
 *   <li>{@code schema_version} equals the pinned raw schema version (v2) — a
 *       future v3 DDL must not feed v2 logic.</li>
 *   <li>{@code validity_state} is VALID-prefixed (R-128: ingestion writes
 *       {@code VALID_TRADE}/{@code VALID_NON_TRADE}, never the literal "VALID").</li>
 *   <li>{@code last_price_paise > 0} — a non-positive price cannot form OHLC.</li>
 *   <li>{@code last_qty >= 0} — negative quantity is corrupt (REQ-FC-002:
 *       volume only from rows with {@code last_qty > 0}).</li>
 *   <li>Row kind INSERT — {@code raw_table_1} is a LOG table; any other kind
 *       is a protocol violation.</li>
 *   <li>{@code event_fingerprint} present and non-blank (tracker 14 P6.3) —
 *       a blank fingerprint would collapse every blank row of a token into
 *       one dedup key for the whole TTL, silently dropping distinct ticks.</li>
 *   <li>{@code event_time} inside the window-arithmetic range (tracker 14
 *       P6.3) — Flink's {@code EventTimeTrigger} computes
 *       {@code window.maxTimestamp() + allowedLateness}; a near-{@link
 *       Long#MAX_VALUE} event time overflows that sum to a negative timer and
 *       fires the window early on a single tick. Non-positive epoch millis are
 *       rejected too (they would underflow the bounded-out-of-orderness
 *       watermark). Historical replay (~1.5e12 ms) sits orders of magnitude
 *       below the cap, so no legitimate row is affected.</li>
 * </ul>
 */
public class RawValidationFunction extends RichFlatMapFunction<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RawValidationFunction.class);

    private final SignalJobConfig config;

    private transient Counter invalidTotal;
    private transient MetricGroup invalidByReasonGroup;
    private transient Map<String, Counter> reasonCounters;

    public RawValidationFunction(SignalJobConfig config) {
        this.config = config;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        invalidTotal = getRuntimeContext().getMetricGroup().counter("compute.invalid.rows");
        invalidByReasonGroup =
                getRuntimeContext().getMetricGroup().addGroup("compute.invalid.byReason");
        reasonCounters = new HashMap<>();
        // Tracker 14 P8.1 box 850: startup-mode gauge on the Flink reporter
        // path — distributed-safe (the client-side ComputeOtlpEmitter gauge
        // dies with the submitting JVM under `flink run -d`; observed 0 live
        // series 2026-08-11). 0 = RESTORE, 1 = FULL_REPLAY. Same value as the
        // emitter's compute.startup.mode; the alert SIGNAL-crit-full-replay-
        // started and the dashboard panel target this stream.
        getRuntimeContext().getMetricGroup().gauge(
                "compute.startup.mode",
                () -> config.startupMode() == SignalJobConfig.StartupMode.FULL_REPLAY ? 1L : 0L);
        // Tracker 14 box 906 (2026-08-12): container memory gauges. Read the
        // cgroup files once at open to decide presence (failure => gauges
        // absent, never a crash); the value supplier re-reads on every scrape
        // so usage stays live. The Flink reporter path turns the dots into
        // underscores (o2-provision.py header documents the O2 rename rule):
        // container_memory_usage_bytes / container_memory_limit_bytes.
        ContainerMemory.Snapshot containerMem = ContainerMemory.read();
        if (containerMem != null) {
            MetricGroup memGroup = getRuntimeContext().getMetricGroup();
            memGroup.gauge("container.memory.usage.bytes", () -> {
                ContainerMemory.Snapshot s = ContainerMemory.read();
                return s == null ? -1L : s.usageBytes();
            });
            memGroup.gauge("container.memory.limit.bytes", () -> {
                ContainerMemory.Snapshot s = ContainerMemory.read();
                return s == null ? -1L : s.limitBytes();
            });
            LOG.info("Container memory gauges registered: usage={} limit={} ({}), "
                            + "cgroup path=({},{})",
                    containerMem.usageBytes(),
                    containerMem.unlimited() ? "unlimited" : containerMem.limitBytes(),
                    containerMem.unlimited() ? "limit absent (max)" : "bounded",
                    ContainerMemory.CGROUP_V2_CURRENT, ContainerMemory.CGROUP_V1_USAGE);
        } else {
            LOG.info("Container memory gauges NOT registered (no readable cgroup memory files)");
        }
        // One-shot visibility into the accepted input contract — a mismatched
        // producer label shows up here as a clear startup-time fact.
        LOG.info("Raw validation gate accepts schema_version={}", config.rawSchemaVersion());
    }

    @Override
    public void flatMap(RowData row, Collector<RowData> out) {
        String reason = invalidReason(row);
        if (reason == null) {
            out.collect(row);
            return;
        }
        invalidTotal.inc();
        // Register each reason counter once (Flink 2.2.1 logs a name-collision
        // warning on every re-registration of the same name — per-row
        // registration flooded the log during the 2026-08-10 live replay).
        reasonCounters.computeIfAbsent(reason, invalidByReasonGroup::counter).inc();
        // Process rule 2 (2026-08-10): schema-version rejections reach
        // OpenObserve as the compute.invalid.byReason.schema-version MetricGroup
        // counter above, exported by the native flink-metrics-otel reporter
        // (CHG-023 item 1) — the client-side ComputeOtlpEmitter mirror is gone.
    }

    /** Package-visible for direct classification tests (no runtime context needed). */
    String invalidReason(RowData row) {
        if (row.getRowKind() != RowKind.INSERT) {
            return "rowkind-not-insert";
        }
        if (row.isNullAt(RawTableColumns.EVENT_FINGERPRINT)
                || row.getString(RawTableColumns.EVENT_FINGERPRINT).toString().isBlank()) {
            return "blank-fingerprint";
        }
        if (row.isNullAt(RawTableColumns.FINGERPRINT_VERSION)
                || row.getString(RawTableColumns.FINGERPRINT_VERSION).toString().isBlank()) {
            return "blank-fingerprint-version";
        }
        if (row.isNullAt(RawTableColumns.SCHEMA_VERSION)
                || !config.rawSchemaVersion().equals(row.getString(RawTableColumns.SCHEMA_VERSION).toString())) {
            return "schema-version";
        }
        if (row.isNullAt(RawTableColumns.VALIDITY_STATE)
                || (!"VALID_TRADE".equals(row.getString(RawTableColumns.VALIDITY_STATE).toString())
                        && !"VALID_NON_TRADE".equals(row.getString(RawTableColumns.VALIDITY_STATE).toString()))) {
            return "validity-state";
        }
        if (row.isNullAt(RawTableColumns.LAST_PRICE_PAISE)
                || row.getLong(RawTableColumns.LAST_PRICE_PAISE) <= 0) {
            return "non-positive-price";
        }
        if (!row.isNullAt(RawTableColumns.LAST_QTY)
                && row.getLong(RawTableColumns.LAST_QTY) < 0) {
            return "negative-qty";
        }
        // Tracker 14 P6.3: keep event_time inside the window arithmetic range.
        // windowEnd + allowedLateness must not overflow Long (EventTimeTrigger),
        // and the bounded-out-of-orderness watermark must not underflow.
        // GenericRowData.getLong on a null field throws NPE, so guard first.
        if (row.isNullAt(RawTableColumns.EVENT_TIME)) {
            return "non-positive-event-time";
        }
        long eventTime = row.getLong(RawTableColumns.EVENT_TIME);
        if (eventTime <= 0) {
            return "non-positive-event-time";
        }
        long maxSafeEventTime =
                Long.MAX_VALUE - config.candleWindowMs() - config.allowedLatenessMs() - 1;
        if (eventTime > maxSafeEventTime) {
            return "event-time-overflow-window";
        }
        return null;
    }
}
