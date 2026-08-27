package com.trading.ingestion.telemetry;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lightweight OTLP metrics emitter — accumulates metrics in-process
 * and periodically flushes to the OpenTelemetry Collector at
 * {@code http://otel-collector:4318/v1/metrics} (HTTP OTLP).
 *
 * <p>Uses zero external dependencies — pure JDK HttpURLConnection + JSON.
 * Counters use {@link AtomicLong}; gauges are volatile primitives. A background
 * thread flushes every 10 seconds.
 *
 * <p>12 metrics per dossier:
 * <ol>
 *   <li>tick.throughput — counter (ticks/sec)</li>
 *   <li>tick.bytes — counter (bytes/sec)</li>
 *   <li>append.latency.ms — histogram (p50/p90/p99)</li>
 *   <li>append.pending.records — gauge</li>
 *   <li>append.pending.bytes — gauge</li>
 *   <li>bridge.reconnects — counter</li>
 *   <li>bridge.connected — gauge (0/1)</li>
 *   <li>manifest.version — gauge</li>
 *   <li>decode.errors — counter (by reason)</li>
 *   <li>fingerprint.count — counter</li>
 *   <li>clock.offset.ms — gauge</li>
 *   <li>ingestion.ready — gauge (0/1)</li>
 * </ol>
 */
public final class OtlpMetricsEmitter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(OtlpMetricsEmitter.class);

    // G6 (ING-UNIT-021): the OTLP export body must never carry credentials or
    // raw payloads. The only caller-supplied strings that reach the payload
    // are the decode-error reason labels, so they are scrubbed with the same
    // two-pass pattern family as the quarantine/safety writers (ING-SEC-RED-
    // 001) before emission. Kept local — the writers' sanitizeDetail is a
    // static utility that lives in the quarantine package and bounds to 512
    // chars, which the reason labels do not need; the pattern itself must not
    // drift, so the secret classes are enumerated identically.
    private static final java.util.regex.Pattern SECRET_PATTERN = java.util.regex.Pattern.compile(
            "(?i)(ARROW_APP_SECRET|ARROW_PASSWORD|ARROW_TOTP_KEY|ARROW_TOKEN|access_token|authorization|appID|token)([=:][^&\\s,}]+)");
    // Runs first so `Bearer <token>` (space-separated) is consumed before the
    // name=value pattern can eat only the literal `Bearer` and leak the token.
    private static final java.util.regex.Pattern BEARER_PATTERN = java.util.regex.Pattern.compile(
            "(?i)\\bBearer[=:\\s]+[^\\s,}]+");

    private final String collectorUrl;
    private final String instanceId;
    private final String serviceName = "ingestion";
    private final ScheduledExecutorService scheduler;
    private volatile boolean closed;

    // ---- Counters ----
    private final AtomicLong tickCount = new AtomicLong(0);
    private final AtomicLong byteCount = new AtomicLong(0);
    private final AtomicLong bridgeReconnects = new AtomicLong(0);
    private final AtomicLong decodeErrors = new AtomicLong(0);
    private final AtomicLong fingerprintCount = new AtomicLong(0);
    private final AtomicLong acknowledgedLoss = new AtomicLong(0);
    private final AtomicLong sequenceGaps = new AtomicLong(0);
    private final AtomicLong heartbeatFailures = new AtomicLong(0);
    private final AtomicLong feedStalls = new AtomicLong(0);
    private final AtomicLong subscriptionRetries = new AtomicLong(0);
    private final AtomicLong partialSubscriptions = new AtomicLong(0);
    private final AtomicLong authRefreshes = new AtomicLong(0);
    private final AtomicLong authFailures = new AtomicLong(0);

    // ---- Histogram (approximate via linear buckets) ----
    private final AtomicLong appendLatencyTotalMs = new AtomicLong(0);
    private final AtomicLong appendLatencyCount = new AtomicLong(0);
    private final AtomicLong appendLatencyP50 = new AtomicLong(0);
    private final AtomicLong appendLatencyP90 = new AtomicLong(0);
    private final AtomicLong appendLatencyP99 = new AtomicLong(0);
    // Simple percentile tracking: wrapping ring buffer (R-065/R-179).
    // All access is synchronized on LATENCY_LOCK: the recorder thread and the
    // background flush thread share the array, and a plain read-check-then-write
    // on the position could lose samples or read a torn position.
    private static final int LATENCY_RING_SIZE = 1024;
    private static final Object LATENCY_LOCK = new Object();
    private final long[] latencyRing = new long[LATENCY_RING_SIZE];
    private int latencyRecordPos; // next write slot (monotonic; masked into the ring)

    // ---- Gauges ----
    private volatile long pendingRecords;
    private volatile long pendingBytes;
    private volatile int bridgeConnected; // 0 or 1
    private volatile long manifestVersion;
    private volatile long clockOffsetMs;
    private volatile long connectionEpoch;
    private volatile int ingestionReady; // 0 or 1

    // ---- Reason counters ----
    private final ConcurrentMap<String, AtomicLong> decodeReasonCounters = new ConcurrentHashMap<>();

    // ---- Slot metrics (plan §Monitoring — labeled by slot only) ----
    private final ConcurrentMap<String, SlotMetricState> slotStates = new ConcurrentHashMap<>();

    /** Per-slot gauge snapshot. Never keyed by token/symbol. */
    public static final class SlotMetricState {
        public volatile int active;            // 0 or 1 (ACTIVE state)
        public volatile long assigned;
        public volatile long acknowledged;
        public volatile long rejected;
        public volatile long lastFrameNanos;
        public volatile double capacityUsedPercent;
        // Safety evidence (plan Amendment §Slot-scoped safety propagation):
        // safetyState is 0 (SAFE) or 1 (UNSAFE); unsafeSinceNanos is the
        // monotonic (System.nanoTime) instant the slot turned unsafe, 0 when
        // safe. unsafe_duration_ms is derived at flush time from it.
        public volatile int safetyState;       // 0 or 1 (UNSAFE)
        public volatile long unsafeSinceNanos; // monotonic; 0 when safe
        public volatile long capacityRemaining;
    }

    // ---- Resource metrics (plan Amendment §Resource) ----
    private volatile long reconnectConsecutive;
    private volatile int activeSockets;
    private volatile int childProcessAlive;    // 0 or 1
    private volatile long processOpenFds;
    private volatile long processFdLimit;
    private volatile double processFdUsagePercent;
    private volatile long processRssBytes;
    private volatile long goGoroutines;
    private volatile long jvmThreadsLive;
    private volatile boolean otlpHealthy;       // most recent export success

    /** Invoked after each flush with success(true)/failure(false) — feeds telemetry readiness. */
    private volatile java.util.function.Consumer<Boolean> healthCallback;

    public OtlpMetricsEmitter(String collectorHostPort, String instanceId) {
        this.collectorUrl = "http://" + collectorHostPort + "/v1/metrics";
        this.instanceId = instanceId;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "otlp-metrics-flush");
            t.setDaemon(true);
            return t;
        });
    }

    /** Set a callback invoked with flush success/failure (telemetry readiness feed). */
    public void setHealthCallback(java.util.function.Consumer<Boolean> callback) {
        this.healthCallback = callback;
    }

    /** Start periodic flush (every 10s). Call once after wiring all recording sites. */
    public void start() {
        scheduler.scheduleAtFixedRate(this::flush, 10, 10, TimeUnit.SECONDS);
        LOG.info("otlp-metrics: started (collector={}, interval=10s)", collectorUrl);
    }

    @Override
    public void close() {
        // R-035: the final flush must run BEFORE the closed flag is set —
        // flush() returns immediately when closed, so setting it first would
        // silently discard up to 10s of buffered metrics on every shutdown.
        scheduler.shutdown();
        flush(); // final flush before shutdown
        closed = true;
        LOG.info("otlp-metrics: closed");
    }

    // ---- Recording API ----

    public void recordTick(int byteSize) {
        tickCount.incrementAndGet();
        byteCount.addAndGet(byteSize);
    }

    public void recordAppendLatencyMs(long latencyMs) {
        appendLatencyTotalMs.addAndGet(latencyMs);
        appendLatencyCount.incrementAndGet();
        // R-065/R-179: thread-safe wrapping ring — never drops samples once
        // full, and never races with the flush thread's percentile snapshot.
        synchronized (LATENCY_LOCK) {
            latencyRing[latencyRecordPos & (LATENCY_RING_SIZE - 1)] = latencyMs;
            latencyRecordPos++;
        }
    }

    public void setPendingRecords(long v) { this.pendingRecords = v; }
    public void setPendingBytes(long v) { this.pendingBytes = v; }
    public void incrementBridgeReconnects() { bridgeReconnects.incrementAndGet(); }
    public void setBridgeConnected(boolean v) { bridgeConnected = v ? 1 : 0; }
    public void setManifestVersion(long v) { manifestVersion = v; }
    public void incrementDecodeError(String reason) {
        decodeErrors.incrementAndGet();
        // R-258: bounded reason map — beyond 32 distinct reasons everything
        // aggregates into "other" so a long-running process cannot grow the
        // map without bound.
        if (decodeReasonCounters.size() >= 32 && !decodeReasonCounters.containsKey(reason)) {
            decodeReasonCounters.computeIfAbsent("other", k -> new AtomicLong(0)).incrementAndGet();
            return;
        }
        decodeReasonCounters.computeIfAbsent(reason, k -> new AtomicLong(0)).incrementAndGet();
    }
    public void incrementFingerprint() { fingerprintCount.incrementAndGet(); }
    public void setClockOffsetMs(long v) { clockOffsetMs = v; }
    public void setIngestionReady(boolean v) { ingestionReady = v ? 1 : 0; }
    public void incrementAcknowledgedLoss() { acknowledgedLoss.incrementAndGet(); }
    /** T7-F11: one per detected feed_sequence_local gap. */
    public void incrementSequenceGaps() { sequenceGaps.incrementAndGet(); }
    public void incrementHeartbeatFailure() { heartbeatFailures.incrementAndGet(); }
    public void incrementFeedStall() { feedStalls.incrementAndGet(); }
    public void incrementSubscriptionRetry() { subscriptionRetries.incrementAndGet(); }
    public void incrementPartialSubscription() { partialSubscriptions.incrementAndGet(); }
    public void incrementAuthRefresh() { authRefreshes.incrementAndGet(); }
    public void incrementAuthFailure() { authFailures.incrementAndGet(); }
    public void setConnectionEpoch(long v) { connectionEpoch = v; }

    // ---- Slot + resource recording (plan §Monitoring / Amendment §Resource) ----

    /** Snapshot per-slot coverage state. Slot label only — never token/symbol. */
    public void setSlotState(String slotId, boolean active, long assigned,
                             long acknowledged, long rejected, long lastFrameNanos) {
        SlotMetricState s = slotStates.computeIfAbsent(slotId, ignored -> new SlotMetricState());
        s.active = active ? 1 : 0;
        s.assigned = assigned;
        s.acknowledged = acknowledged;
        s.rejected = rejected;
        s.lastFrameNanos = lastFrameNanos;
    }

    /** Capacity used percent for a slot (plan ING-CAP-001). */
    public void setSlotCapacityUsedPercent(String slotId, double percent) {
        SlotMetricState s = slotStates.computeIfAbsent(slotId, ignored -> new SlotMetricState());
        s.capacityUsedPercent = percent;
    }

    /**
     * Slot safety evidence (plan Amendment). safetyState 1 = UNSAFE, 0 =
     * SAFE; unsafeSinceNanos is the monotonic timestamp the slot turned
     * unsafe (0 when safe) — the flush derives unsafe_duration_ms from it, so
     * re-emitting the same unsafe state must not reset the clock. Callers
     * pass the value stamped by HealthProbe.
     */
    public void setSlotSafetyState(String slotId, int safetyState, long unsafeSinceNanos) {
        SlotMetricState s = slotStates.computeIfAbsent(slotId, ignored -> new SlotMetricState());
        s.safetyState = safetyState;
        if (safetyState == 1) {
            // Keep the FIRST unsafe timestamp for the duration gauge — only a
            // safe→unsafe transition may stamp it.
            if (s.unsafeSinceNanos == 0) s.unsafeSinceNanos = unsafeSinceNanos;
        } else {
            s.unsafeSinceNanos = 0;
        }
    }

    /** Remaining subscription capacity for a slot (plan ING-CAP-001):
     *  arrowHftMaxTokensPerConnection − assigned. */
    public void setSlotCapacityRemaining(String slotId, long remaining) {
        SlotMetricState s = slotStates.computeIfAbsent(slotId, ignored -> new SlotMetricState());
        s.capacityRemaining = remaining;
    }

    public void setReconnectConsecutive(long v) { reconnectConsecutive = v; }
    public void setActiveSockets(int v) { activeSockets = v; }
    public void setChildProcessAlive(boolean v) { childProcessAlive = v ? 1 : 0; }
    public void setProcessOpenFds(long v) { processOpenFds = v; }
    public void setProcessFdLimit(long v) { processFdLimit = v; }
    public void setProcessFdUsagePercent(double v) { processFdUsagePercent = v; }
    public void setProcessRssBytes(long v) { processRssBytes = v; }
    public void setGoGoroutines(long v) { goGoroutines = v; }
    public void setJvmThreadsLive(long v) { jvmThreadsLive = v; }

    // ---- package-private test accessors ----

    SlotMetricState slotState(String slotId) { return slotStates.get(slotId); }
    int childProcessAlive() { return childProcessAlive; }
    void forceFlush() { flush(); }

    // ---- Flush ----

    private void flush() {
        if (closed) return;
        try {
            String json = buildMetricsJson();
            if (json == null) return;

            URL url = URI.create(collectorUrl).toURL();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            conn.setConnectTimeout(5_000);
            conn.setReadTimeout(5_000);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(json.getBytes(StandardCharsets.UTF_8));
            }

            // R-067: health must reflect the actual HTTP status — a rejected
            // or malformed payload (>=400) is NOT a healthy export.
            int code = conn.getResponseCode();
            if (code >= 400) {
                LOG.warn("otlp-metrics: flush HTTP {} (url={})", code, collectorUrl);
                reportHealth(false);
            } else {
                reportHealth(true);
            }
            conn.disconnect();

        } catch (Exception e) {
            LOG.debug("otlp-metrics: flush failed (collector may not be running): {}",
                    e.getMessage());
            reportHealth(false);
        }
    }

    private void reportHealth(boolean healthy) {
        this.otlpHealthy = healthy;
        java.util.function.Consumer<Boolean> cb = healthCallback;
        if (cb != null) {
            try {
                cb.accept(healthy);
            } catch (Exception ignore) { /* never let a callback break flushing */ }
        }
    }

    // ---- JSON builder (minimal OTLP metrics format) ----

    String buildMetricsJson() {
        long now = System.currentTimeMillis() * 1_000_000L; // epoch nanos
        // Compute approximate percentiles from ring buffer
        computeLatencyPercentiles();

        StringBuilder sb = new StringBuilder(4096);
        sb.append("{\"resourceMetrics\":[{\"resource\":{\"attributes\":[");
        appendAttr(sb, "service.name", serviceName);
        appendAttr(sb, "service.instance.id", instanceId);
        sb.setLength(sb.length() - 1); // R-036: drop trailing comma after the last attribute
        sb.append("]},\"scopeMetrics\":[{\"scope\":{\"name\":\"ingestion\"},\"metrics\":[");

        // Counter: tick.throughput
        appendSum(sb, "tick.throughput", "ticks", tickCount.get(), now);
        appendSum(sb, "tick.bytes", "bytes", byteCount.get(), now);
        appendSum(sb, "bridge.reconnects", "reconnects", bridgeReconnects.get(), now);
        appendSum(sb, "decode.errors", "errors", decodeErrors.get(), now);
        // R-258: emit the per-reason decode-error breakdown so the recorded
        // state reaches the collector (previously write-only).
        decodeReasonCounters.forEach((reason, counter) ->
                appendReasonSum(sb, reason, counter.get(), now));
        appendSum(sb, "fingerprint.count", "fingerprints", fingerprintCount.get(), now);
        appendSum(sb, "append.acknowledged.loss", "records", acknowledgedLoss.get(), now);
        appendSum(sb, "sequence.gaps", "gaps", sequenceGaps.get(), now);
        appendSum(sb, "heartbeat.failures", "failures", heartbeatFailures.get(), now);
        appendSum(sb, "feed.stalls", "stalls", feedStalls.get(), now);
        appendSum(sb, "subscription.retries", "retries", subscriptionRetries.get(), now);
        appendSum(sb, "subscription.partial", "events", partialSubscriptions.get(), now);
        appendSum(sb, "auth.refreshes", "refreshes", authRefreshes.get(), now);
        appendSum(sb, "auth.failures", "failures", authFailures.get(), now);

        // Histogram: append.latency.ms
        appendHistogram(sb, "append.latency.ms", "ms", appendLatencyCount.get(),
                appendLatencyTotalMs.get(), appendLatencyP50.get(),
                appendLatencyP90.get(), appendLatencyP99.get(), now);

        // Gauge: append.pending.records
        appendGaugeLong(sb, "append.pending.records", "records", pendingRecords, now);
        appendGaugeLong(sb, "append.pending.bytes", "bytes", pendingBytes, now);
        appendGaugeInt(sb, "bridge.connected", "1", bridgeConnected, now);
        appendGaugeLong(sb, "manifest.version", "version", manifestVersion, now);
        appendGaugeLong(sb, "clock.offset.ms", "ms", clockOffsetMs, now);
        appendGaugeLong(sb, "bridge.connection.epoch", "epoch", connectionEpoch, now);
        appendGaugeInt(sb, "ingestion.ready", "1", ingestionReady, now);

        // ---- Slot gauges (labeled by slot only) ----
        long nowNanos = now;
        slotStates.forEach((slotId, s) -> {
            long frameAgeMs = s.lastFrameNanos > 0
                    ? Math.max(0, (System.nanoTime() - s.lastFrameNanos) / 1_000_000L)
                    : -1;
            appendGaugeIntLabeled(sb, "bridge.slot.active", "1", s.active, slotId, nowNanos);
            appendGaugeLongLabeled(sb, "bridge.slot.assigned", "tokens", s.assigned, slotId, nowNanos);
            appendGaugeLongLabeled(sb, "bridge.slot.acknowledged", "tokens", s.acknowledged, slotId, nowNanos);
            appendGaugeLongLabeled(sb, "bridge.slot.rejected", "tokens", s.rejected, slotId, nowNanos);
            appendGaugeLongLabeled(sb, "bridge.slot.last_frame_age_ms", "ms", frameAgeMs, slotId, nowNanos);
            appendGaugeDoubleLabeled(sb, "bridge.slot.capacity_used_percent", "%",
                    s.capacityUsedPercent, slotId, nowNanos);
            // Safety + capacity evidence (plan Amendment §Resource/capacity):
            // safety_state 0/1, unsafe_duration_ms since the first unsafe
            // transition (0 while safe), capacity_remaining tokens.
            appendGaugeIntLabeled(sb, "bridge.slot.safety_state", "1", s.safetyState, slotId, nowNanos);
            long unsafeMs = s.safetyState == 1 && s.unsafeSinceNanos > 0
                    ? Math.max(0, (System.nanoTime() - s.unsafeSinceNanos) / 1_000_000L)
                    : 0;
            appendGaugeLongLabeled(sb, "bridge.slot.unsafe_duration_ms", "ms", unsafeMs, slotId, nowNanos);
            appendGaugeLongLabeled(sb, "bridge.slot.capacity_remaining", "tokens",
                    s.capacityRemaining, slotId, nowNanos);
        });

        // ---- Resource + capacity gauges (Amendment §Resource) ----
        appendGaugeLong(sb, "bridge.reconnect.consecutive", "reconnects", reconnectConsecutive, now);
        appendGaugeInt(sb, "bridge.active_sockets", "sockets", activeSockets, now);
        appendGaugeInt(sb, "bridge.child_process_alive", "1", childProcessAlive, now);
        appendGaugeLong(sb, "process.open_fds", "fds", processOpenFds, now);
        appendGaugeLong(sb, "process.fd_limit", "fds", processFdLimit, now);
        appendGaugeDouble(sb, "process.fd_usage_percent", "%", processFdUsagePercent, now);
        appendGaugeLong(sb, "process.rss_bytes", "bytes", processRssBytes, now);
        appendGaugeLong(sb, "go.goroutines", "goroutines", goGoroutines, now);
        appendGaugeLong(sb, "jvm.threads.live", "threads", jvmThreadsLive, now);
        appendGaugeInt(sb, "otel.collector.healthy", "1", otlpHealthy ? 1 : 0, now);

        // Trim last comma
        sb.setLength(sb.length() - 1);

        sb.append("]}]}]}");
        return sb.toString();
    }

    private void computeLatencyPercentiles() {
        int count;
        long[] sorted;
        synchronized (LATENCY_LOCK) {
            // Copy the LAST min(pos, ringSize) samples in arrival order, then
            // reset the position so each 10s window covers exactly its own
            // samples (R-065/R-179 — wrapping ring, no lost samples).
            int n = Math.min(latencyRecordPos, LATENCY_RING_SIZE);
            if (n == 0) return;
            sorted = new long[n];
            for (int i = 0; i < n; i++) {
                sorted[i] = latencyRing[(latencyRecordPos - n + i) & (LATENCY_RING_SIZE - 1)];
            }
            count = n;
            latencyRecordPos = 0;
        }
        java.util.Arrays.sort(sorted);
        appendLatencyP50.set(sorted[count / 2]);                    // p50
        appendLatencyP90.set(sorted[(int) (count * 0.90)]);         // p90
        appendLatencyP99.set(sorted[(int) (count * 0.99)]);         // p99
    }

    private void appendSum(StringBuilder sb, String name, String unit, long value, long timeNanos) {
        // R-036: OTLP protobuf-JSON sums require aggregationTemporality and
        // isMonotonic; int64-as-string is legal for sums but we emit them as
        // numbers for consistency.
        sb.append("{\"name\":\"").append(esc(name)).append("\",")
          .append("\"unit\":\"").append(esc(unit)).append("\",")
          .append("\"sum\":{\"aggregationTemporality\":\"AGGREGATION_TEMPORALITY_CUMULATIVE\",")
          .append("\"isMonotonic\":true,")
          .append("\"dataPoints\":[{\"asInt\":").append(value).append(",")
          .append("\"timeUnixNano\":\"").append(timeNanos).append("\"}]}},");
    }

    /**
     * Per-reason decode-error sum (R-258) — emitted so the breakdown reaches
     * the collector. The reason label is scrubbed first (G6/ING-UNIT-021): a
     * decode-error detail can carry the raw offending line, and that line can
     * contain ARROW_* values, Bearer tokens, or appID/token query strings —
     * which must never leave the process on the OTLP wire.
     */
    private void appendReasonSum(StringBuilder sb, String reason, long value, long timeNanos) {
        sb.append("{\"name\":\"decode.errors.by_reason\",")
          .append("\"unit\":\"errors\",")
          .append("\"sum\":{\"aggregationTemporality\":\"AGGREGATION_TEMPORALITY_CUMULATIVE\",")
          .append("\"isMonotonic\":true,")
          .append("\"dataPoints\":[{\"asInt\":").append(value).append(",")
          .append("\"attributes\":[{\"key\":\"reason\",\"value\":{\"stringValue\":\"")
          .append(esc(scrubReason(reason))).append("\"}}],")
          .append("\"timeUnixNano\":\"").append(timeNanos).append("\"}]}},");
    }

    /**
     * G6: redact credential classes from a reason label before emission
     * (mirrors ING-SEC-RED-001), then fail closed: if ANY credential marker
     * survives the two passes (an unrecognized form, or a reason that is
     * itself a literal secret), collapse the whole label — a collector must
     * never see an ARROW_* name, Bearer marker, or raw_payload token even
     * with its value redacted.
     */
    private static String scrubReason(String reason) {
        if (reason == null) return "";
        String safe = BEARER_PATTERN.matcher(reason).replaceAll("Bearer=[REDACTED]");
        safe = SECRET_PATTERN.matcher(safe).replaceAll("$1=[REDACTED]");
        if (safe.matches("(?is).*(ARROW_|\\bBearer\\b|raw_payload|appID|access_token|\\btoken\\b).*")) {
            return "REDACTED";
        }
        return safe;
    }

    private void appendHistogram(StringBuilder sb, String name, String unit,
                                  long count, long sum, long p50, long p90, long p99,
                                  long timeNanos) {
        // R-036: OTLP histograms require explicitBounds to be one element
        // shorter than bucketCounts, and bucket totals to reconcile with count.
        // We carry the whole sample in the first bucket over the [p50,p90,p99]
        // bounds; p50/p90/p99 are preserved as data-point attributes.
        sb.append("{\"name\":\"").append(esc(name)).append("\",")
          .append("\"unit\":\"").append(esc(unit)).append("\",")
          .append("\"histogram\":{\"dataPoints\":[{")
          .append("\"count\":").append(count).append(",")
          .append("\"sum\":").append(sum).append(",")
          .append("\"bucketCounts\":[").append(count).append(",0,0,0],")
          .append("\"explicitBounds\":[").append(p50).append(",").append(p90).append(",").append(p99).append("],")
          .append("\"attributes\":[{")
          .append("\"key\":\"p50\",\"value\":{\"intValue\":").append(p50).append("}},")
          .append("{\"key\":\"p90\",\"value\":{\"intValue\":").append(p90).append("}},")
          .append("{\"key\":\"p99\",\"value\":{\"intValue\":").append(p99).append("}}")
          .append("],")
          .append("\"timeUnixNano\":\"").append(timeNanos).append("\"}]}},");
    }

    private void appendGaugeLong(StringBuilder sb, String name, String unit,
                                  long value, long timeNanos) {
        sb.append("{\"name\":\"").append(esc(name)).append("\",")
          .append("\"unit\":\"").append(esc(unit)).append("\",")
          .append("\"gauge\":{\"dataPoints\":[{\"asInt\":\"").append(value).append("\",")
          .append("\"timeUnixNano\":\"").append(timeNanos).append("\"}]}},");
    }

    private void appendGaugeInt(StringBuilder sb, String name, String unit,
                                 int value, long timeNanos) {
        sb.append("{\"name\":\"").append(esc(name)).append("\",")
          .append("\"unit\":\"").append(esc(unit)).append("\",")
          .append("\"gauge\":{\"dataPoints\":[{\"asInt\":\"").append(value).append("\",")
          .append("\"timeUnixNano\":\"").append(timeNanos).append("\"}]}},");
    }

    private void appendGaugeDouble(StringBuilder sb, String name, String unit,
                                   double value, long timeNanos) {
        // R-036: protobuf JSON requires double fields to be JSON numbers, not
        // strings — asDouble must be unquoted.
        sb.append("{\"name\":\"").append(esc(name)).append("\",")
          .append("\"unit\":\"").append(esc(unit)).append("\",")
          .append("\"gauge\":{\"dataPoints\":[{\"asDouble\":").append(value).append(",")
          .append("\"timeUnixNano\":\"").append(timeNanos).append("\"}]}},");
    }

    private void appendGaugeIntLabeled(StringBuilder sb, String name, String unit,
                                       int value, String slotId, long timeNanos) {
        sb.append("{\"name\":\"").append(esc(name)).append("\",")
          .append("\"unit\":\"").append(esc(unit)).append("\",")
          .append("\"gauge\":{\"dataPoints\":[{\"asInt\":\"").append(value).append("\",")
          .append("\"attributes\":[{\"key\":\"slot\",\"value\":{\"stringValue\":\"")
          .append(esc(slotId)).append("\"}}],")
          .append("\"timeUnixNano\":\"").append(timeNanos).append("\"}]}},");
    }

    private void appendGaugeLongLabeled(StringBuilder sb, String name, String unit,
                                        long value, String slotId, long timeNanos) {
        sb.append("{\"name\":\"").append(esc(name)).append("\",")
          .append("\"unit\":\"").append(esc(unit)).append("\",")
          .append("\"gauge\":{\"dataPoints\":[{\"asInt\":\"").append(value).append("\",")
          .append("\"attributes\":[{\"key\":\"slot\",\"value\":{\"stringValue\":\"")
          .append(esc(slotId)).append("\"}}],")
          .append("\"timeUnixNano\":\"").append(timeNanos).append("\"}]}},");
    }

    private void appendGaugeDoubleLabeled(StringBuilder sb, String name, String unit,
                                          double value, String slotId, long timeNanos) {
        // R-036: asDouble as a JSON number (protobuf-JSON compliance).
        sb.append("{\"name\":\"").append(esc(name)).append("\",")
          .append("\"unit\":\"").append(esc(unit)).append("\",")
          .append("\"gauge\":{\"dataPoints\":[{\"asDouble\":").append(value).append(",")
          .append("\"attributes\":[{\"key\":\"slot\",\"value\":{\"stringValue\":\"")
          .append(esc(slotId)).append("\"}}],")
          .append("\"timeUnixNano\":\"").append(timeNanos).append("\"}]}},");
    }

    private void appendAttr(StringBuilder sb, String key, String value) {
        sb.append("{\"key\":\"").append(key).append("\",")
          .append("\"value\":{\"stringValue\":\"").append(esc(value)).append("\"}},");
    }

    private static String esc(String s) {
        if (s == null) return "";
        // R-066: escape backslash, double-quote, and ALL control characters
        // (< 0x20) — an embedded \n/\t/\r in a reason string would otherwise
        // produce invalid JSON and fail the entire 10s POST.
        StringBuilder out = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\' -> out.append("\\\\");
                case '"' -> out.append("\\\"");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.toString();
    }
}
