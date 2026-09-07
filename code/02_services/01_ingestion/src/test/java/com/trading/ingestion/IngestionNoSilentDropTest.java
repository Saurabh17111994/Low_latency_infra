package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.config.IngestionConfig;
import com.trading.ingestion.discontinuity.DiscontinuitySink;
import com.trading.ingestion.discontinuity.DiscontinuityWriter;
import com.trading.ingestion.health.NtpClockChecker;
import com.trading.ingestion.model.Instrument;
import com.trading.ingestion.quarantine.QuarantineSink;
import com.trading.ingestion.quarantine.QuarantineWriter;
import com.trading.ingestion.safety.SafetyHaltWriter;
import com.trading.ingestion.safety.SafetySink;
import com.google.protobuf.ByteString;
import com.trading.ingestion.telemetry.OtlpMetricsEmitter;
import com.trading.ingestion.transport.TickEvent;
import com.trading.ingestion.write.FlussRowConverter;
import com.trading.ingestion.write.RawTickWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ING-DQ-010: no-silent-drop global invariant.
 *
 * <p>Every proto {@code TickEvent} fed to {@code processTickEvent} must
 * produce EXACTLY one outcome — an append or a quarantine — and no event may
 * vanish without evidence or throw out of the method. This test feeds a mixed
 * corpus (valid, unknown-feed, missing-instrument, invalid-values, stale,
 * future) through the real service pipeline and reconciles the outcome
 * ledger:
 *
 * <pre>{@code
 *   appendCalls + quarantineCalls == eventsFed
 *   frameCount == eventsFed        (every event processed)
 *   errorCount == 0                (no uncaught exception)
 * }</pre>
 *
 * <p><i>2026-08-29:</i> migrated from NDJSON lines to proto TickEvents. The
 * malformed-JSON / 1 MB non-JSON / NUL-char classes moved to the frame layer
 * (ProtoFrameReader rejects them before the service — covered by
 * FuzzIngestionTest.rejectBucketConsumedWithoutEvidence).
 *
 * <p><b>Default-run:</b> the service is constructed with no-op evidence sinks
 * ({@link QuarantineSink}, {@link DiscontinuitySink}, {@link SafetySink}), so
 * no reachable Fluss is required and the test always runs. The no-silent-drop
 * ledger is asserted from in-process state (append calls + decode-error
 * metrics); the sinks only need to accept the writes.
 */
@DisplayName("ING-DQ-010: no-silent-drop — every TickEvent maps to exactly one outcome")
class IngestionNoSilentDropTest {

    private static final long TOKEN_A = 100_000L;
    private static final long TOKEN_B = 100_100L;

    @Test
    @DisplayName("mixed corpus reconciles: appends + quarantines == events fed, zero uncaught errors")
    void mixedCorpusNeverDropsSilently() throws Exception {
        IngestionConfig config = buildConfig("localhost:9123");
        CountingConverter converter = new CountingConverter();
        NtpClockChecker clock = new NtpClockChecker("127.0.0.1:9", 100, false);
        IngestionService service = new IngestionService(
                "ing-dq-010", instruments(), converter, config, clock,
                noopQuarantine(), noopDiscontinuity(), noopSafety());

        long now = System.currentTimeMillis();
        List<Line> corpus = new ArrayList<>();
        // ---- valid ticks (must be appended) ----
        corpus.add(new Line(tick("hft", "full", TOKEN_A, now, 100), Outcome.APPEND));
        corpus.add(new Line(tick("hft", "ltpc", TOKEN_B, now, 200), Outcome.APPEND));
        corpus.add(new Line(tick("hft", "quote", TOKEN_A, now, 0), Outcome.APPEND)); // VALID_NON_TRADE → QUOTE
        // ---- quarantined classes (must never reach the writer) ----
        corpus.add(new Line(tick("standard", "ltp", TOKEN_A, now, 100), Outcome.QUARANTINE_METRIC)); // unknown feed
        corpus.add(new Line(tick("hft", "ltpc", 999_999L, now, 100), Outcome.QUARANTINE_METRIC));   // missing instrument
        corpus.add(new Line(tick("hft", "ltpc", TOKEN_A, now, 0), Outcome.QUARANTINE_METRIC));      // invalid values
        corpus.add(new Line(tick("hft", "full", TOKEN_A, now - 10_000, 100), Outcome.QUARANTINE_METRIC)); // stale
        corpus.add(new Line(tick("hft", "full", TOKEN_B, now + 10_000, 100), Outcome.QUARANTINE_NO_METRIC)); // future

        for (Line line : corpus) {
            service.processTickEvent(line.event(), "hft-0", 1L);
            // any uncaught exception fails the test directly
        }

        long expectedAppends = corpus.stream().filter(l -> l.outcome() == Outcome.APPEND).count();
        long expectedMetricQuarantines =
                corpus.stream().filter(l -> l.outcome() == Outcome.QUARANTINE_METRIC).count();
        long expectedNoMetricQuarantines =
                corpus.stream().filter(l -> l.outcome() == Outcome.QUARANTINE_NO_METRIC).count();

        // The observable ledger: every line is an append or a quarantine; nothing
        // may vanish, nothing may throw.
        assertEquals(corpus.size(), frameCount(service), "every fed event must be processed");
        assertEquals(0, errorCount(service), "no event may throw out of processTickEvent");
        assertEquals(expectedAppends, converter.appendCalls.get(),
                "exactly the valid ticks reach the writer — none of the "
                        + (corpus.size() - expectedAppends) + " bad events may append");
        assertEquals(expectedMetricQuarantines + expectedNoMetricQuarantines,
                corpus.size() - expectedAppends, "corpus classification sanity");
        assertEquals(expectedMetricQuarantines, decodeErrors(service),
                "every metric-bumping quarantine class must fire its decode-error metric "
                        + "(FUTURE is quarantined without a metric bump)");
        assertEquals(0, service.tracker().pendingRecords(),
                "writer pending must drain to zero (no leaked reservations)");
        assertEquals(0, service.tracker().pendingBytes());

        // The appended set is exactly the valid corpus subset.
        assertEquals(expectedAppends, converter.packets.size());
        assertEquals(List.of(TOKEN_A, TOKEN_B, TOKEN_A),
                converter.packets.stream().map(p -> p.instrumentToken()).toList());
        assertTrue(converter.packets.stream().allMatch(p -> p.eventFingerprint() != null),
                "every appended packet carries a fingerprint");
        assertFalse(converter.packets.stream().anyMatch(p -> p.raw() == null || p.raw().rawPayload() == null),
                "every appended packet preserves the raw payload bytes");
    }

    /** Expected outcome of one corpus line (the test's own classification). */
    private enum Outcome {
        APPEND,
        /** Quarantined AND the decode-error metric is bumped (MALFORMED_JSON, UNKNOWN_VERSION, …). */
        QUARANTINE_METRIC,
        /** Quarantined but the decode-error metric is NOT bumped (FUTURE_BROKER_TIMESTAMP). */
        QUARANTINE_NO_METRIC
    }

    /** One corpus event with its expected outcome. */
    private record Line(TickEvent event, Outcome outcome) {}

    // ---- fixtures and harness ----

    private static IngestionConfig buildConfig(String bootstrap) throws Exception {
        Map<String, String> env = new HashMap<>();
        env.put("DEPLOYMENT_ENV", "dev");
        env.put("ARROW_APP_ID", "test-app");
        env.put("ARROW_APP_SECRET", "test-secret");
        env.put("ARROW_USER_ID", "test-user");
        env.put("ARROW_PASSWORD", "test-pass");
        env.put("ARROW_TOTP_KEY", "JBSWY3DPEHPK3PXP");
        env.put("FLUSS_BOOTSTRAP", bootstrap);
        env.put("RAW_TABLE_NAME", "raw_table_1");
        env.put("ARROW_MAX_EVENT_AGE_MS", "5000");
        env.put("ARROW_MAX_FUTURE_EVENT_SKEW_MS", "2000");
        Method validateFrom = IngestionConfig.class.getDeclaredMethod("validateFrom", Map.class);
        validateFrom.setAccessible(true);
        return (IngestionConfig) validateFrom.invoke(null, env);
    }

    private static List<Instrument> instruments() {
        return List.of(
                new Instrument.Builder().instrumentToken(TOKEN_A).tradingSymbol("SYM1-EQ")
                        .exchange("NSE").segment("CM").lotSize(1).manifestVersion(1).build(),
                new Instrument.Builder().instrumentToken(TOKEN_B).tradingSymbol("SYM2-EQ")
                        .exchange("NSE").segment("CM").lotSize(1).manifestVersion(1).build());
    }

    /** One proto TickEvent with a valid payload + hash (mirrors the bridge emitter). */
    private static TickEvent tick(String feed, String mode, long token, long tsMs, long ltpPaise) {
        return TickEvent.newBuilder()
                .setSlotId("hft-0")
                .setMode(mode)
                .setToken((int) token)
                .setFeed(feed)
                .setTsMs(tsMs)
                // receive time is the wall clock at emission — the freshness
                // gate compares broker ts_ms against this, never against itself.
                .setReceivedMs(System.currentTimeMillis())
                .setFeedSequenceLocal(1)
                .setLtpPaise(ltpPaise)
                .setVolume(100)
                .setRawPayload(ByteString.copyFrom(FRAME_PAYLOAD))
                .setPayloadHash(ByteString.copyFrom(sha256Hex(FRAME_PAYLOAD).getBytes(StandardCharsets.UTF_8)))
                .build();
    }

    private static final byte[] FRAME_PAYLOAD =
            new byte[] {0x01, 0x02, 0x03, 0x04, (byte) 0xFF, 0x00, 0x10};

    private static String sha256Hex(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(data);
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ---- no-op evidence sinks (ING-DQ-010 default-run seam) ----

    private static QuarantineSink noopQuarantine() {
        return new QuarantineSink() {
            @Override
            public void write(byte[] rawPayload, QuarantineWriter.Reason reason, String detail) {
            }

            @Override
            public void write(byte[] rawPayload, QuarantineWriter.Reason reason, String detail,
                              Long instrumentToken, String exchange, String symbol) {
            }

            @Override
            public void close() {
            }
        };
    }

    private static DiscontinuitySink noopDiscontinuity() {
        return new DiscontinuitySink() {
            @Override
            public void write(DiscontinuityWriter.Reason reason, String note,
                              DiscontinuityWriter.LastTickSnapshot before) {
            }

            @Override
            public void write(DiscontinuityWriter.Reason reason, String note,
                              DiscontinuityWriter.LastTickSnapshot before,
                              Long instrumentToken, String exchange, String symbol) {
            }

            @Override
            public void writeBridgeEvent(com.trading.ingestion.bridge.BridgeEvent event,
                                         DiscontinuityWriter.LastTickSnapshot before) {
            }

            @Override
            public void close() {
            }
        };
    }

    private static SafetySink noopSafety() {
        return new SafetySink() {
            @Override
            public String write(String slotId, long connectionEpoch,
                                SafetyHaltWriter.SafetyState state,
                                SafetyHaltWriter.ReasonCode reasonCode, String assignedTokenHash,
                                String evidenceReference, long detectedTsMs) {
                return "noop";
            }

            @Override
            public void close() {
            }
        };
    }

    /** Fake converter: counts appends, completes instantly, records packets. */
    static final class CountingConverter implements FlussRowConverter {
        final AtomicInteger appendCalls = new AtomicInteger();
        final List<com.trading.ingestion.model.TickPacket> packets = new CopyOnWriteArrayList<>();

        @Override
        public CompletableFuture<RawTickWriter.AppendResult> append(
                com.trading.ingestion.model.TickPacket packet) {
            appendCalls.incrementAndGet();
            packets.add(packet);
            return CompletableFuture.completedFuture(new RawTickWriter.AppendResult(1, "p0"));
        }

        @Override
        public int estimatedRowSize(com.trading.ingestion.model.TickPacket packet) {
            return 256;
        }

        @Override
        public void close() {
        }
    }

    // ---- reflection accessors (private state; IngestionServiceTest uses the same pattern) ----

    private static long frameCount(IngestionService service) throws Exception {
        Field f = IngestionService.class.getDeclaredField("frameCount");
        f.setAccessible(true);
        return ((AtomicLong) f.get(service)).get();
    }

    private static long errorCount(IngestionService service) throws Exception {
        Field f = IngestionService.class.getDeclaredField("errorCount");
        f.setAccessible(true);
        return ((AtomicLong) f.get(service)).get();
    }

    private static long decodeErrors(IngestionService service) throws Exception {
        Field metricsField = IngestionService.class.getDeclaredField("metrics");
        metricsField.setAccessible(true);
        OtlpMetricsEmitter metrics = (OtlpMetricsEmitter) metricsField.get(service);
        Field decodeField = OtlpMetricsEmitter.class.getDeclaredField("decodeErrors");
        decodeField.setAccessible(true);
        return ((AtomicLong) decodeField.get(metrics)).get();
    }
}
