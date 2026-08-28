package com.trading.compute.signaljob;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

/**
 * Fingerprint deduplication (Signal dossier operator 2, REQ-FC-003),
 * state-authoritative since 2026-08-16 (DEC-038 superseded — see
 * {@code docs/08_implementation/04-signal-job.md} §Design B): the complete
 * 5-minute dedup set lives in THIS operator's Flink keyed state, check
 * pointed atomically with the source offset. There is no external store and
 * no query-on-miss — a cold start after checkpoint restore is correct by
 * construction, never an empty dedup set.
 *
 * <p>Keyed by {@code instrument_token}; per key the state is a map whose key
 * is {@code fingerprint_version + scope + event_fingerprint} and whose value
 * is {@code (first_seen, nominal_expiry)} — no raw bytes, no decoded fields,
 * no candle or candidate values, no event objects (dossier state contract). A
 * fingerprint seen again before its expiry is counted as a duplicate and
 * dropped; the first occurrence passes downstream. Once expired, a
 * re-arriving fingerprint is eligible again.
 *
 * <p><b>Expiry (native, CHG-023 item 2, 2026-08-17):</b> the MapState is
 * enabled with {@link StateTtlConfig} — TTL {@code DEDUP_TTL_MS},
 * {@code OnCreateAndWrite} (the dedup never rewrites a value, so the TTL
 * stays anchored at first-seen) and {@code NeverReturnExpired} (an expired
 * entry reads as absent — the re-arrival is re-admitted, never
 * double-accepted). Flink's only TTL time characteristic is ProcessingTime
 * (wall clock) — the correct semantics for a 5-minute real-time dedup rule.
 * The previous build's hand-rolled expiry index
 * ({@code Long → List<String>} MapState) and per-entry event-time timers are
 * REMOVED: they replaced native TTL, added +100–150% state, and carried the
 * orphaned-timer bug class. Background physical cleanup is native too —
 * RocksDB runs the TTL compaction filter by default
 * ({@code state.backend.rocksdb.compaction.filter.query-time-after-num-entries}
 * = 1000; compatible with this job's incremental checkpoints), heap runs
 * incremental cleanup; no configuration needed.
 *
 * <p><b>Restart semantics (native migration):</b> Flink 2.2.0+ migrates state
 * between TTL and non-TTL layouts seamlessly — a pre-CHG-023 checkpoint's
 * plain {@code DedupEntry} values are wrapped at restore with the current
 * processing time as their TTL anchor, so restored fingerprints stay
 * deduplicated for one more full TTL from the restore moment (never an empty
 * set, never instant expiry).
 *
 * <p><b>Boundedness by construction, not by eviction:</b> entries = accepted
 * rate × TTL horizon. The DEC-038 bounded-cache machinery (eviction to
 * {@code DEDUP_CACHE_MAX_ENTRIES}/{@code DEDUP_CACHE_MAX_BYTES}, Fluss
 * query-on-miss rehydration, the durable-write side output, the wall-clock
 * cleanup pass) was removed on 2026-08-16 — the state IS authoritative and
 * eviction would be a correctness hole, not a bound.
 */
public class FingerprintDedupFunction extends KeyedProcessFunction<Long, RowData, RowData> {

    private static final long serialVersionUID = 1L;

    /**
     * Gauge resync cadence per instrument. TTL entries expire invisibly
     * (NeverReturnExpired — the operator is never told), so the pure
     * insertion-side count would drift upward forever; every this-many rows
     * per instrument the operator re-scans the CURRENT key's map ({@code
     * MapState.entries()} skips expired entries) and folds the actual live
     * count in. The per-token scan is small (steady state ≈ 6k entries per
     * instrument at 20 480 t/s) and runs ≈ 2×/s across all instruments —
     * negligible, and it keeps the cumulative gauge honest between restores.
     * Package-visible test seam: expiry-leg tests lower it to force an
     * immediate resync.
     */
    static long GAUGE_RESYNC_INTERVAL_ROWS = 10_000L;

    private final SignalJobConfig config;

    private transient MapState<String, DedupEntry> dedup;
    private transient Counter firstEvents;
    private transient Counter duplicates;

    /**
     * Gauge state (tracker 14 P5.1): cumulative firsts seen plus a conservative
     * bytes estimate — NOT the live MapState size (see the resync javadoc for
     * the RocksDB divergence). Keyed {@code MapState} cannot be iterated in
     * {@code open()} (no key namespace — verified: the harness throws "No key
     * set" on {@code entries()} outside a keyed callback), so each token's
     * restored size is folded in EXACTLY on its first post-restore row.
     */
    private transient long dedupCount;
    private transient long bytesEstimate;

    /** Per-token tracked dedup size; replace-with-actual on resync. */
    private final Map<Long, Long> tokenStateCount = new java.util.HashMap<>();

    /** Per-token rows since the last gauge resync (TTL-drift correction). */
    private final Map<Long, Long> tokenRowsSinceResync = new java.util.HashMap<>();

    /**
     * Upper-bound per-entry estimate: state key ({@code version|token|fingerprint},
     * ~40 chars UTF-16 = 80 B) + {@link DedupEntry} (2 longs = 16 B) + MapState
     * overhead (~32 B) + TTL timestamp (RocksDB adds 8 B per map entry — Flink
     * TTL docs). Measured upper bound — see {@code DedupStateSizeTest} for the
     * serialized-size check.
     */
    static final long PER_ENTRY_ESTIMATE_BYTES = 136L;

    public FingerprintDedupFunction(SignalJobConfig config) {
        this.config = config;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(
                        Duration.ofMillis(config.dedupTtlMs()))
                .updateTtlOnCreateAndWrite()
                .neverReturnExpired()
                .build();
        MapStateDescriptor<String, DedupEntry> descriptor = new MapStateDescriptor<>(
                "fingerprint-dedup", Types.STRING,
                TypeInformation.of(DedupEntry.class));
        descriptor.enableTimeToLive(ttlConfig);
        dedup = getRuntimeContext().getMapState(descriptor);

        firstEvents = getRuntimeContext().getMetricGroup().counter("compute.dedup.first");
        duplicates = getRuntimeContext().getMetricGroup().counter("compute.dedup.duplicates");

        getRuntimeContext().getMetricGroup().gauge("compute.dedup.firsts.cumulative",
                (Gauge<Long>) () -> dedupCount);
        getRuntimeContext().getMetricGroup().gauge("compute.dedup.firsts.bytes.estimate",
                (Gauge<Long>) () -> bytesEstimate);
    }

    /**
     * Fold the current key's ACTUAL stored count into the gauges. This is a
     * CUMULATIVE counter, NOT the live MapState size: {@code MapState.entries()}
     * returns ALL stored entries on the RocksDB state backend — TTL-expired
     * entries are NOT filtered at iteration time (verified in
     * {@code RocksDBMapState$RocksDBMapIterator} bytecode, Flink 2.2.1; the
     * TTL filter runs only during compaction). The heap backend DOES filter at
     * iteration, so the gauge was accurate only on heap — tests run on heap
     * (harness default) and passed; production RocksDB diverged, over-reporting
     * ~39× (see logs/tracker-14/dedup-ttl-diagnosis-20260828.md).
     *
     * <p>The resync corrects the restore case (fresh gauge fields vs restored
     * state) and folds in compaction-removed entries (small downward deltas),
     * but the gauge remains a monotonic cumulative-firsts signal. Monitoring
     * must read {@code flink_jobmanager_job_lastCheckpointSize} for the true
     * live state; the alerts were repointed accordingly (o2-provision.py).
     */
    private void resyncTokenGauges(long token) throws Exception {
        long tracked = tokenStateCount.getOrDefault(token, 0L);
        long actual = 0;
        for (Map.Entry<String, DedupEntry> ignored : dedup.entries()) {
            actual++;
        }
        long delta = actual - tracked;
        if (delta != 0) {
            dedupCount += delta;
            bytesEstimate += delta * PER_ENTRY_ESTIMATE_BYTES;
        }
        tokenStateCount.put(token, actual);
    }

    @Override
    public void processElement(RowData row, Context ctx, Collector<RowData> out) throws Exception {
        String version = row.getString(RawTableColumns.FINGERPRINT_VERSION).toString();
        String fingerprint = row.getString(RawTableColumns.EVENT_FINGERPRINT).toString();
        long token = ctx.getCurrentKey();
        // State key = version | scope | fingerprint. Scope is the instrument
        // token (the keyBy key) — explicit here so the state key contract is
        // self-contained rather than relying on the Flink key namespace alone.
        String stateKey = version + "|" + token + "|" + fingerprint;

        // Gauge resync: first row per token (restore correction) + every
        // GAUGE_RESYNC_INTERVAL_ROWS per token (TTL-drift correction).
        long rows = tokenRowsSinceResync.merge(token, 1L, Long::sum);
        if (!tokenStateCount.containsKey(token) || rows >= GAUGE_RESYNC_INTERVAL_ROWS) {
            resyncTokenGauges(token);
            tokenRowsSinceResync.put(token, 0L);
        }

        if (dedup.contains(stateKey)) {
            duplicates.inc();
            return;
        }

        // First occurrence (or expired — NeverReturnExpired reads it absent) —
        // accept and record. The state IS the authority; there is no eviction
        // and no external store to consult. The stored value is informational:
        // the AUTHORITATIVE expiry is the native TTL on the map entry.
        long eventTime = row.getLong(RawTableColumns.EVENT_TIME);
        long nominalExpiry = DedupExpiry.expiryMs(eventTime, config.dedupTtlMs());
        if (nominalExpiry < eventTime) {
            // Event-time overflow boundary (P5.3): clamp the NOMINAL value to
            // MAX_VALUE — the fingerprint stays deduplicated for the whole
            // runtime (the native TTL is unaffected; no timer is registered).
            nominalExpiry = Long.MAX_VALUE;
        }
        dedup.put(stateKey, new DedupEntry(eventTime, nominalExpiry));

        firstEvents.inc();
        dedupCount++;
        bytesEstimate += PER_ENTRY_ESTIMATE_BYTES;
        tokenStateCount.merge(token, 1L, Long::sum);
        out.collect(row);
    }

    /**
     * Gauge-source accessors (tests): the exact values the {@code MetricGroup}
     * gauges registered in {@link #open} export. The fields are the gauges'
     * source — no separate mirror (CHG-023 item 1 removed the client-side
     * ComputeOtlpEmitter mirror; the native reporter reads the MetricGroup
     * gauges directly).
     */
    long dedupStateCountForTest() {
        return dedupCount;
    }

    long bytesEstimateForTest() {
        return bytesEstimate;
    }

    /** Compact value: {@code (first_seen, nominal_expiry)} — the TTL is the expiry authority. */
    public record DedupEntry(long firstSeenMs, long expiryMs) implements Serializable {}
}
