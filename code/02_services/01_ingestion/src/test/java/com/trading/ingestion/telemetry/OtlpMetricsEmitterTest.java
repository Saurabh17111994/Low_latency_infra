package com.trading.ingestion.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Phase 5 — slot + resource metrics (plan §Monitoring / Amendment §Resource).
 *
 * <p>Verifies the recording API snapshots slot coverage and resource gauges,
 * and that the health callback feeds telemetry readiness.
 */
@DisplayName("ING-UNIT-012: slot + resource metrics")
class OtlpMetricsEmitterTest {

    // ---- log4j2 capture (same minimal AbstractAppender as
    // BridgeShutdownRegressionTest) — pins the harness-facing
    // `otlp-metrics-payload:` line that stage-capture.sh tails. ----

    static final class CapturingAppender extends AbstractAppender {
        final List<String> messages = new CopyOnWriteArrayList<>();

        CapturingAppender(String name) {
            super(name, null, (Layout<?>) null);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage() == null
                    ? "" : event.getMessage().getFormattedMessage());
        }
    }

    private static CapturingAppender attachLogCapture(String name) {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration cfg = ctx.getConfiguration();
        CapturingAppender appender = new CapturingAppender(name);
        appender.start();
        cfg.getRootLogger().addAppender(appender, null, null);
        ctx.updateLoggers();
        return appender;
    }

    private static void detachLogCapture(CapturingAppender appender) {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration cfg = ctx.getConfiguration();
        cfg.getRootLogger().removeAppender(appender.getName());
        ctx.updateLoggers();
        appender.stop();
    }

    /**
     * METRICS_LOCAL_LOG=1 (soak harness): every flush also emits
     * {@code otlp-metrics-payload: <json>} even when the collector is
     * healthy, so stage-capture.sh's ingestion.tsv leg has data to tail.
     * Default off — the env gate must not change normal log volume.
     */
    @Test
    @DisplayName("METRICS_LOCAL_LOG=1 logs the payload on a healthy-path flush")
    void localLogEmitsPayloadOnSuccess() throws Exception {
        // Minimal HTTP collector: answers 200 to any POST so the emitter
        // takes the healthy flush path (payload logging only happens there
        // when localLogEnabled; the failure path always logs).
        com.sun.net.httpserver.HttpServer server =
                com.sun.net.httpserver.HttpServer.create(
                        new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try (java.io.InputStream body = exchange.getRequestBody()) {
                body.readAllBytes();
            }
            byte[] ok = "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, ok.length);
            try (java.io.OutputStream os = exchange.getResponseBody()) {
                os.write(ok);
            }
        });
        server.start();
        try {
            String hostPort = "127.0.0.1:" + server.getAddress().getPort();

            // Flag ON: payload line must appear after a healthy flush.
            CapturingAppender cap = attachLogCapture("localLogCapture");
            OtlpMetricsEmitter on = new OtlpMetricsEmitter(hostPort, "test-instance", true);
            try {
                on.recordTick(100);
                on.forceFlush();
                assertTrue(cap.messages.stream().anyMatch(
                                m -> m.startsWith("otlp-metrics-payload: {")),
                        "METRICS_LOCAL_LOG=1 must log the payload on a healthy flush");
            } finally {
                on.close();
                detachLogCapture(cap);
            }

            // Flag OFF (default): healthy flush logs NO payload line.
            CapturingAppender cap2 = attachLogCapture("localLogCapture2");
            OtlpMetricsEmitter off = new OtlpMetricsEmitter(hostPort, "test-instance", false);
            try {
                off.recordTick(100);
                off.forceFlush();
                assertFalse(cap2.messages.stream().anyMatch(
                                m -> m.startsWith("otlp-metrics-payload:")),
                        "default must not log the payload on a healthy flush (log volume)");
            } finally {
                off.close();
                detachLogCapture(cap2);
            }
        } finally {
            server.stop(0);
        }
    }

    /**
     * Regression (observed 2026-09-04 soak): the harness exports
     * {@code METRICS_LOCAL_LOG=1} and Boolean.parseBoolean("1") is false —
     * the flag silently stayed OFF and no payload lines appeared even
     * though flushes succeeded. The env parse must accept "1" and "true".
     */
    @Test
    @DisplayName("METRICS_LOCAL_LOG env parse accepts 1 and true, rejects 0/absent")
    void parseLocalLogEnvAcceptsOneAndTrue() {
        assertTrue(OtlpMetricsEmitter.parseLocalLogEnv("1"),
                "METRICS_LOCAL_LOG=1 must enable local logging");
        assertTrue(OtlpMetricsEmitter.parseLocalLogEnv("true"),
                "METRICS_LOCAL_LOG=true must enable local logging");
        assertTrue(OtlpMetricsEmitter.parseLocalLogEnv("TRUE"),
                "parse must be case-insensitive for true");
        assertFalse(OtlpMetricsEmitter.parseLocalLogEnv("0"),
                "METRICS_LOCAL_LOG=0 must stay off");
        assertFalse(OtlpMetricsEmitter.parseLocalLogEnv("false"),
                "METRICS_LOCAL_LOG=false must stay off");
        assertFalse(OtlpMetricsEmitter.parseLocalLogEnv(null),
                "absent METRICS_LOCAL_LOG must stay off (default)");
    }

    @Test
    @DisplayName("slot state snapshot records coverage and capacity")
    void slotStateSnapshot() {
        OtlpMetricsEmitter emitter = new OtlpMetricsEmitter("127.0.0.1:1", "test-instance");
        try {
            emitter.setSlotState("hft-0", true, 1024, 1024, 0, System.nanoTime());
            OtlpMetricsEmitter.SlotMetricState s = emitter.slotState("hft-0");
            assertNotNull(s, "slot state must be tracked");
            assertEquals(1, s.active);
            assertEquals(1024, s.assigned);
            assertEquals(1024, s.acknowledged);
            assertEquals(0, s.rejected);

            emitter.setSlotCapacityUsedPercent("hft-0", 87.5);
            assertEquals(87.5, s.capacityUsedPercent, 0.001);

            // Safety + capacity evidence (Step 5): unsafe stamp, duration
            // source, and remaining capacity.
            emitter.setSlotCapacityRemaining("hft-0", 256);
            assertEquals(256, s.capacityRemaining);
            assertEquals(0, s.safetyState, "fresh slot must be SAFE");
            assertEquals(0, s.unsafeSinceNanos);

            long now = System.nanoTime();
            emitter.setSlotSafetyState("hft-0", 1, now);
            assertEquals(1, s.safetyState);
            assertEquals(now, s.unsafeSinceNanos, "first unsafe transition stamps the clock");
            // Re-emission of the same unsafe state must NOT reset the stamp —
            // the unsafe-duration gauge keeps counting from the first transition.
            Thread.sleep(5);
            emitter.setSlotSafetyState("hft-0", 1, System.nanoTime());
            assertEquals(now, s.unsafeSinceNanos, "re-emitted unsafe state must keep the first stamp");
            // SAFE clears the stamp.
            emitter.setSlotSafetyState("hft-0", 0, 0);
            assertEquals(0, s.safetyState);
            assertEquals(0, s.unsafeSinceNanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        } finally {
            emitter.close();
        }
    }

    @Test
    @DisplayName("resource gauges snapshot")
    void resourceGaugesSnapshot() {
        OtlpMetricsEmitter emitter = new OtlpMetricsEmitter("127.0.0.1:1", "test-instance");
        try {
            emitter.setProcessOpenFds(12);
            emitter.setProcessFdLimit(1024);
            emitter.setProcessFdUsagePercent(1.17);
            emitter.setProcessRssBytes(123456L);
            emitter.setJvmThreadsLive(42);
            emitter.setGoGoroutines(7);
            emitter.setActiveSockets(1);
            emitter.setChildProcessAlive(true);
            emitter.setReconnectConsecutive(3);
            // no crash = gauges accepted; values verified via SlotMetricState-style access
            assertEquals(1, emitter.childProcessAlive());
        } finally {
            emitter.close();
        }
    }

    @Test
    @DisplayName("health callback fires on flush result")
    void healthCallbackFires() {
        OtlpMetricsEmitter emitter = new OtlpMetricsEmitter("127.0.0.1:1", "test-instance");
        try {
            AtomicBoolean seen = new AtomicBoolean(false);
            emitter.setHealthCallback(healthy -> {
                seen.set(true);
                // collector at 127.0.0.1:1 is unreachable → false expected
                assertEquals(false, healthy);
            });
            emitter.forceFlush();
            assertEquals(true, seen.get(), "health callback must fire on flush");
        } finally {
            emitter.close();
        }
    }

    @Test
    @DisplayName("close() performs the final flush before marking closed (R-035)")
    void closeFlushesBeforeClosed() {
        OtlpMetricsEmitter emitter = new OtlpMetricsEmitter("127.0.0.1:1", "test-instance");
        AtomicBoolean callbackFired = new AtomicBoolean(false);
        emitter.setHealthCallback(h -> callbackFired.set(true));
        emitter.recordTick(100);
        emitter.close();
        assertTrue(callbackFired.get(),
                "close() must run flush() while still open (R-035) — callback never fired");
    }

    @Test
    @DisplayName("payload is protobuf-JSON compliant (R-036)")
    void payloadIsSpecCompliant() throws Exception {
        OtlpMetricsEmitter emitter = new OtlpMetricsEmitter("127.0.0.1:1", "test-instance");
        try {
            emitter.setProcessFdUsagePercent(42.5);
            emitter.setSlotCapacityUsedPercent("hft-0", 12.75);
            emitter.setSlotCapacityRemaining("hft-0", 768);
            emitter.setSlotSafetyState("hft-0", 1, System.nanoTime());
            emitter.recordAppendLatencyMs(5);
            emitter.recordAppendLatencyMs(7);
            emitter.incrementDecodeError("Malformed\nJSON");
            String json = emitter.buildMetricsJson();

            // Must parse as valid JSON (control chars escaped — R-066).
            JsonNode root = new ObjectMapper().readTree(json);
            assertNotNull(root, "payload must be valid JSON");

            // Locate the asDouble gauges and assert they are numbers, not strings.
            JsonNode metrics = root.at("/resourceMetrics/0/scopeMetrics/0/metrics");
            for (JsonNode metric : metrics) {
                String name = metric.path("name").asText();
                JsonNode dp = metric.path("gauge").path("dataPoints").get(0);
                if (name.equals("process.fd_usage_percent")) {
                    assertTrue(dp.path("asDouble").isNumber(),
                            "asDouble must be a JSON number (R-036)");
                    assertEquals(42.5, dp.path("asDouble").asDouble(), 0.001);
                }
                if (name.equals("bridge.slot.capacity_used_percent")) {
                    assertTrue(dp.path("asDouble").isNumber(),
                            "labeled asDouble must be a JSON number (R-036)");
                    assertEquals(12.75, dp.path("asDouble").asDouble(), 0.001);
                }
                if (name.equals("bridge.slot.capacity_remaining")) {
                    assertEquals(768, dp.path("asInt").asLong(),
                            "capacity_remaining = connection limit − assigned");
                }
                if (name.equals("bridge.slot.safety_state")) {
                    assertEquals(1, dp.path("asInt").asInt(), "unsafe slot must export 1");
                }
                if (name.equals("bridge.slot.unsafe_duration_ms")) {
                    long ms = dp.path("asInt").asLong();
                    assertTrue(ms >= 0, "unsafe_duration_ms must be non-negative");
                }
            }

            // Sums carry aggregationTemporality + isMonotonic.
            boolean sumHasTemporality = false;
            for (JsonNode metric : metrics) {
                if (metric.has("sum")) {
                    assertEquals("AGGREGATION_TEMPORALITY_CUMULATIVE",
                            metric.path("sum").path("aggregationTemporality").asText(),
                            "sum must declare aggregationTemporality (R-036)");
                    assertTrue(metric.path("sum").path("isMonotonic").asBoolean(),
                            "sum must declare isMonotonic (R-036)");
                    sumHasTemporality = true;
                }
            }
            assertTrue(sumHasTemporality);

            // Histogram: explicitBounds is one shorter than bucketCounts, and
            // bucket totals reconcile with count.
            for (JsonNode metric : metrics) {
                if (metric.path("name").asText().equals("append.latency.ms")) {
                    JsonNode hist = metric.path("histogram").path("dataPoints").get(0);
                    long count = hist.path("count").asLong();
                    long[] buckets = new ObjectMapper().convertValue(
                            hist.path("bucketCounts"), long[].class);
                    double[] bounds = new ObjectMapper().convertValue(
                            hist.path("explicitBounds"), double[].class);
                    assertEquals(bounds.length + 1, buckets.length,
                            "bucketCounts must be one element longer than explicitBounds (R-036)");
                    long bucketSum = 0;
                    for (long b : buckets) bucketSum += b;
                    assertEquals(count, bucketSum, "bucket totals must reconcile with count (R-036)");
                }
            }

            // Reason breakdown is emitted (R-258) and the control character is
            // escaped so the payload remains valid JSON.
            String payload = json;
            assertTrue(payload.contains("decode.errors.by_reason"),
                    "decode-error reason breakdown must be emitted (R-258)");
            assertTrue(!payload.contains("Malformed\nJSON"),
                    "control characters must be escaped in the payload (R-066)");
        } finally {
            emitter.close();
        }
    }

    @Test
    @DisplayName("latency ring wraps and keeps the latest window (R-179)")
    void latencyRingWraps() throws Exception {
        OtlpMetricsEmitter emitter = new OtlpMetricsEmitter("127.0.0.1:1", "test-instance");
        try {
            for (int i = 0; i < 2000; i++) {
                emitter.recordAppendLatencyMs(i);
            }
            String json = emitter.buildMetricsJson();
            // 1024-sample window of values 976..1999 → median ≈ 1487.
            assertTrue(json.contains("\"intValue\":1487") || json.contains("\"intValue\":1488"),
                    "p50 must reflect the latest wrapped window, not the first 1024 samples (R-179)");
        } finally {
            emitter.close();
        }
    }

    // ---- ING-UNIT-021: metrics-side secret scrubbing (G6) ----

    /**
     * ING-UNIT-021: the OTLP export body is never scanned for leaked
     * credentials or raw payloads — log scrubbing (ING-SEC-RED-001) is tested,
     * the metrics side is not. Drive every recording path with secret-shaped
     * values (the only caller-supplied strings that reach the payload are the
     * decode-error reason labels and the slot labels), serialize the full
     * export body, and assert no secret class survives: ARROW_* env values,
     * Bearer tokens, raw_payload markers, token/appID values, or the secret
     * literals themselves (mirrors ING-SEC-RED-001 for logs).
     */
    @Test
    @DisplayName("ING-UNIT-021: OTLP export body carries no credentials or raw payloads")
    void exportBodyScrubsSecrets() throws Exception {
        OtlpMetricsEmitter emitter = new OtlpMetricsEmitter("127.0.0.1:1", "test-instance");
        try {
            // Exercise EVERY recording path (sums, histogram, gauges, slots,
            // reasons, resources) while seeding the only user-controlled
            // strings with secret-shaped values.
            emitter.recordTick(100);
            emitter.recordAppendLatencyMs(5);
            emitter.recordAppendLatencyMs(7);
            emitter.setPendingRecords(3);
            emitter.setPendingBytes(1500);
            emitter.incrementBridgeReconnects();
            emitter.setBridgeConnected(true);
            emitter.setManifestVersion(24);
            emitter.incrementDecodeError("ARROW_TOKEN=eyJhbGciOiJIUzI1NiJ9.secret");
            emitter.incrementDecodeError("Authorization=Bearer secretBearerToken123");
            emitter.incrementDecodeError("raw_payload contains appID=b3b40c832fcd token=secretQueryToken");
            emitter.incrementFingerprint();
            emitter.setClockOffsetMs(12);
            emitter.setIngestionReady(true);
            emitter.incrementAcknowledgedLoss();
            emitter.incrementHeartbeatFailure();
            emitter.incrementFeedStall();
            emitter.incrementSubscriptionRetry();
            emitter.incrementPartialSubscription();
            emitter.incrementAuthRefresh();
            emitter.incrementAuthFailure();
            emitter.setConnectionEpoch(1);
            emitter.setSlotState("hft-0", true, 1024, 1024, 0, System.nanoTime());
            emitter.setSlotCapacityUsedPercent("hft-0", 87.5);
            emitter.setSlotSafetyState("hft-0", 1, System.nanoTime());
            emitter.setSlotCapacityRemaining("hft-0", 256);
            emitter.setReconnectConsecutive(2);
            emitter.setActiveSockets(1);
            emitter.setChildProcessAlive(true);
            emitter.setProcessOpenFds(12);
            emitter.setProcessFdLimit(1024);
            emitter.setProcessFdUsagePercent(1.17);
            emitter.setProcessRssBytes(123456L);
            emitter.setGoGoroutines(7);
            emitter.setJvmThreadsLive(42);

            String payload = emitter.buildMetricsJson();
            assertTrue(new ObjectMapper().readTree(payload).isObject(),
                    "payload must remain valid JSON after scrubbing");

            // The secret-shaped strings fed as decode-error reasons must never
            // reach the wire. The only legal occurrence of any of these words
            // is nothing — the payload must not contain them at all.
            assertFalse(payload.contains("eyJhbGciOiJIUzI1NiJ9"),
                    "JWT/ARROW_TOKEN value leaked into the OTLP export body");
            assertFalse(payload.contains("secretBearerToken123"),
                    "Bearer token leaked into the OTLP export body");
            assertFalse(payload.contains("b3b40c832fcd"),
                    "appID value leaked into the OTLP export body");
            assertFalse(payload.contains("secretQueryToken"),
                    "query-token value leaked into the OTLP export body");
            assertFalse(payload.contains("ARROW_TOKEN"),
                    "ARROW_* env name leaked into the OTLP export body");
            assertFalse(payload.contains("Bearer"),
                    "Authorization bearer marker leaked into the OTLP export body");
            assertFalse(payload.contains("raw_payload"),
                    "raw_payload marker leaked into the OTLP export body (R-036 invariant)");
            assertFalse(payload.contains("appID"),
                    "appID key leaked into the OTLP export body");
            assertFalse(payload.contains("password") || payload.contains("secret"),
                    "credential-class words leaked into the OTLP export body");
        } finally {
            emitter.close();
        }
    }

    // ---- ING-UNIT-022: bounded metric-label cardinality ----

    /**
     * ING-UNIT-022: every attribute key in the serialized OTLP payload must
     * come from a fixed set — no token/symbol/high-cardinality label keys
     * (dossier §Telemetry: "bounded-cardinality metrics"). Enumerate the
     * label keys the way a collector would (resource attributes, data-point
     * attributes) and assert the exact set.
     */
    @Test
    @DisplayName("ING-UNIT-022: emitted label keys come from the fixed bounded set")
    void labelCardinalityBounded() throws Exception {
        OtlpMetricsEmitter emitter = new OtlpMetricsEmitter("127.0.0.1:1", "test-instance");
        try {
            emitter.incrementDecodeError("MALFORMED_JSON");
            emitter.incrementDecodeError("UNKNOWN_FEED");
            emitter.setSlotCapacityUsedPercent("hft-0", 50.0);
            emitter.setSlotCapacityUsedPercent("hft-1", 25.0);
            emitter.recordAppendLatencyMs(3);
            emitter.recordAppendLatencyMs(9);
            String payload = emitter.buildMetricsJson();

            JsonNode root = new ObjectMapper().readTree(payload);
            Set<String> keys = new HashSet<>();
            collectAttributeKeys(root, keys);

            // The complete fixed set: 2 resource attributes, slot + reason on
            // labeled data points, and the 3 histogram percentile attributes.
            Set<String> allowed = Set.of(
                    "service.name",
                    "service.instance.id",
                    "slot",
                    "reason",
                    "p50",
                    "p90",
                    "p99");
            assertEquals(allowed, keys,
                    "label keys must come from the fixed set exactly — no token/symbol/high-cardinality keys");

            // The label VALUES must stay bounded too: slot ids are hft-<n>,
            // reasons are from the fixed quarantine vocabulary — never token
            // numbers or symbols.
            for (JsonNode attr : root.findValues("attributes")) {
                for (JsonNode pair : attr) {
                    String k = pair.path("key").asText();
                    String v = pair.path("value").path("stringValue").asText("");
                    if (k.equals("slot")) {
                        assertTrue(v.matches("hft-\\d+"),
                                "slot label must be a bounded slot id, got: " + v);
                    }
                }
            }
        } finally {
            emitter.close();
        }
    }

    /** Recursively collect every "key" under any "attributes" array, plus resource attributes. */
    private static void collectAttributeKeys(JsonNode node, Set<String> out) {
        if (node == null) return;
        if (node.isArray()) {
            for (JsonNode child : node) {
                collectAttributeKeys(child, out);
            }
            return;
        }
        if (node.isObject()) {
            Iterator<String> names = node.fieldNames();
            while (names.hasNext()) {
                String n = names.next();
                JsonNode child = node.get(n);
                if (n.equals("attributes") && child.isArray()) {
                    for (JsonNode pair : child) {
                        out.add(pair.path("key").asText());
                    }
                } else {
                    collectAttributeKeys(child, out);
                }
            }
        }
    }
}
