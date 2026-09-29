package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.trading.common.config.ConfigKeys;
import com.trading.ingestion.config.IngestionConfig;
import com.trading.ingestion.discontinuity.DiscontinuitySink;
import com.trading.ingestion.discontinuity.DiscontinuityWriter;
import com.trading.ingestion.health.NtpClockChecker;
import com.trading.ingestion.model.Instrument;
import com.trading.ingestion.model.TickPacket;
import com.trading.ingestion.quarantine.QuarantineSink;
import com.trading.ingestion.quarantine.QuarantineWriter;
import com.trading.ingestion.safety.SafetyHaltWriter;
import com.trading.ingestion.safety.SafetySink;
import com.trading.ingestion.transport.TickEvent;
import com.trading.ingestion.write.FlussRowConverter;
import com.trading.ingestion.write.RawTickWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * M4-6 — payload integrity at tick admission is always-on.
 *
 * <p>The audit found the hash switch parsed but never read on the proto path:
 * {@code INGEST_VALIDATE_PAYLOAD_HASH} defaulted off and
 * {@link com.trading.ingestion.bridge.PayloadHashValidator} had no production
 * caller, so a tick whose bytes did not match its digest appended like any
 * other. The fix verifies the byte[] digest before any other admission work
 * (freshness, instrument, validity) and deletes the switch — integrity is not
 * optional, and a switch is a mode to remember.
 *
 * <p>Pinned here: valid → append; tampered 32-byte digest → {@code HASH_MISMATCH}
 * quarantine + metric; empty hash → {@code MISSING_PAYLOAD_HASH}; non-32-byte
 * hash or an oversized packet → {@code INVALID_SCHEMA}; and no config key,
 * field or diagnostic can turn the gate off.
 */
@DisplayName("M4-6: payload hash gate is always-on at admission")
class IngestionPayloadHashGateTest {

    private static final long TOKEN = 100_000L;

    /** Captures quarantine evidence (reason + detail). */
    private static final class CapturingQuarantine implements QuarantineSink {
        final List<String> reasons = new CopyOnWriteArrayList<>();
        final List<String> details = new CopyOnWriteArrayList<>();

        @Override
        public void write(byte[] rawPayload, QuarantineWriter.Reason reason, String detail) {
            reasons.add(reason.name());
            details.add(detail);
        }

        @Override
        public void write(byte[] rawPayload, QuarantineWriter.Reason reason, String detail,
                          Long instrumentToken, String exchange, String symbol) {
            reasons.add(reason.name());
            details.add(detail);
        }

        @Override
        public void close() {
        }
    }

    /** Fake converter: counts appends, completes instantly. */
    private static final class CountingConverter implements FlussRowConverter {
        final AtomicInteger appendCalls = new AtomicInteger();

        @Override
        public CompletableFuture<RawTickWriter.AppendResult> append(TickPacket packet) {
            appendCalls.incrementAndGet();
            return CompletableFuture.completedFuture(new RawTickWriter.AppendResult(1, "p0"));
        }

        @Override
        public int estimatedRowSize(TickPacket packet) {
            return 256;
        }

        @Override
        public void close() {
        }
    }

    private static IngestionConfig config() throws Exception {
        return config(Map.of());
    }

    private static IngestionConfig config(Map<String, String> extra) throws Exception {
        Map<String, String> env = new HashMap<>();
        env.put("DEPLOYMENT_ENV", "dev");
        env.put("ARROW_APP_ID", "test-app");
        env.put("ARROW_APP_SECRET", "test-secret");
        env.put("ARROW_USER_ID", "test-user");
        env.put("ARROW_PASSWORD", "test-pass");
        env.put("ARROW_TOTP_KEY", "JBSWY3DPEHPK3PXP");
        env.put("FLUSS_BOOTSTRAP", "localhost:9123");
        env.put("RAW_TABLE_NAME", "raw_table_1");
        env.put("ARROW_MAX_EVENT_AGE_MS", "5000");
        env.put("ARROW_MAX_FUTURE_EVENT_SKEW_MS", "2000");
        env.putAll(extra);
        Method validateFrom = IngestionConfig.class.getDeclaredMethod("validateFrom", Map.class);
        validateFrom.setAccessible(true);
        return (IngestionConfig) validateFrom.invoke(null, env);
    }

    private static IngestionService service(CapturingQuarantine quarantine,
                                            CountingConverter converter,
                                            IngestionConfig config) throws Exception {
        List<Instrument> instruments = List.of(new Instrument.Builder()
                .instrumentToken(TOKEN).tradingSymbol("SYM1-EQ").exchange("NSE")
                .segment("CM").lotSize(1).manifestVersion(1).build());
        return new IngestionService("ing-payload-hash", instruments, converter, config,
                new NtpClockChecker("127.0.0.1:9", 100, false),
                quarantine, noopDiscontinuity(), noopSafety());
    }

    /** A fresh, manifest-known full-mode tick carrying exactly the given bytes + digest. */
    private static TickEvent tick(byte[] payload, byte[] digest) {
        long now = System.currentTimeMillis();
        return TickEvent.newBuilder()
                .setSlotId("hft-0")
                .setMode("full")
                .setToken((int) TOKEN)
                .setFeed("hft")
                .setTsMs(now)
                .setReceivedMs(now)
                .setFeedSequenceLocal(1)
                .setLtpPaise(100)
                .setVolume(10)
                .setRawPayload(ByteString.copyFrom(payload))
                .setPayloadHash(ByteString.copyFrom(digest))
                .build();
    }

    /** The append/quarantine path is async — poll until the ledger settles. */
    private static void awaitOutcome(CountingConverter converter, CapturingQuarantine quarantine,
                                     int expected) throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (converter.appendCalls.get() + quarantine.reasons.size() < expected) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("pipeline did not settle (appends="
                        + converter.appendCalls.get() + ", quarantines="
                        + quarantine.reasons.size() + ")");
            }
            Thread.sleep(10);
        }
    }

    @Test
    @DisplayName("a tick whose bytes hash to its digest appends")
    void validDigestAppends() throws Exception {
        byte[] payload = "m4-6-valid-packet".getBytes(StandardCharsets.UTF_8);
        CapturingQuarantine quarantine = new CapturingQuarantine();
        CountingConverter converter = new CountingConverter();
        IngestionService service = service(quarantine, converter, config());

        service.processTickEvent(tick(payload, ProtoTickFactory.sha256(payload)), "hft-0", 1L);
        awaitOutcome(converter, quarantine, 1);

        assertEquals(1, converter.appendCalls.get(), "a verified tick must append");
        assertTrue(quarantine.reasons.isEmpty(), "a verified tick must not be quarantined");
    }

    @Test
    @DisplayName("a tampered digest is quarantined as HASH_MISMATCH and never appends")
    void tamperedDigestIsQuarantined() throws Exception {
        byte[] payload = "m4-6-tampered-packet".getBytes(StandardCharsets.UTF_8);
        byte[] wrongDigest = ProtoTickFactory.sha256("different-bytes".getBytes(StandardCharsets.UTF_8));
        CapturingQuarantine quarantine = new CapturingQuarantine();
        CountingConverter converter = new CountingConverter();
        IngestionService service = service(quarantine, converter, config());

        service.processTickEvent(tick(payload, wrongDigest), "hft-0", 1L);
        awaitOutcome(converter, quarantine, 1);

        assertEquals(List.of("HASH_MISMATCH"), quarantine.reasons,
                "a tampered tick must be quarantined as HASH_MISMATCH");
        assertEquals(0, converter.appendCalls.get(), "a tampered tick must never append");
        assertTrue(service.metrics().buildMetricsJson().contains("HASH_MISMATCH"),
                "the mismatch must be counted for dashboards");
    }

    @Test
    @DisplayName("a missing payload_hash is quarantined as MISSING_PAYLOAD_HASH")
    void missingHashIsQuarantined() throws Exception {
        byte[] payload = "m4-6-missing-hash".getBytes(StandardCharsets.UTF_8);
        CapturingQuarantine quarantine = new CapturingQuarantine();
        CountingConverter converter = new CountingConverter();
        IngestionService service = service(quarantine, converter, config());

        service.processTickEvent(tick(payload, new byte[0]), "hft-0", 1L);
        awaitOutcome(converter, quarantine, 1);

        assertEquals(List.of("MISSING_PAYLOAD_HASH"), quarantine.reasons,
                "a hashless tick (proto3 default) must be quarantined, not appended");
        assertTrue(quarantine.details.get(0).contains("missing_hash"), quarantine.details.get(0));
        assertEquals(0, converter.appendCalls.get(), "a hashless tick must never append");
        assertTrue(service.metrics().buildMetricsJson().contains("MISSING_PAYLOAD_HASH"),
                "the missing hash must be counted for dashboards");
    }

    @Test
    @DisplayName("a non-32-byte digest is quarantined as INVALID_SCHEMA")
    void malformedHashIsQuarantined() throws Exception {
        byte[] payload = "m4-6-malformed-hash".getBytes(StandardCharsets.UTF_8);
        CapturingQuarantine quarantine = new CapturingQuarantine();
        CountingConverter converter = new CountingConverter();
        IngestionService service = service(quarantine, converter, config());

        service.processTickEvent(tick(payload, new byte[] {1, 2, 3}), "hft-0", 1L);
        awaitOutcome(converter, quarantine, 1);

        assertEquals(List.of("INVALID_SCHEMA"), quarantine.reasons,
                "a 3-byte digest is malformed wire data, not a mismatch");
        assertTrue(quarantine.details.get(0).contains("malformed_hash"), quarantine.details.get(0));
        assertEquals(0, converter.appendCalls.get(), "a malformed digest must never append");
        assertTrue(service.metrics().buildMetricsJson().contains("INVALID_SCHEMA"),
                "the malformed digest must be counted for dashboards");
    }

    @Test
    @DisplayName("an oversized packet fails the 540 B bound before hashing (INVALID_SCHEMA)")
    void oversizedPacketIsQuarantined() throws Exception {
        byte[] payload = new byte[541];
        Arrays.fill(payload, (byte) 7);
        CapturingQuarantine quarantine = new CapturingQuarantine();
        CountingConverter converter = new CountingConverter();
        IngestionService service = service(quarantine, converter, config());

        service.processTickEvent(tick(payload, ProtoTickFactory.sha256(payload)), "hft-0", 1L);
        awaitOutcome(converter, quarantine, 1);

        assertEquals(List.of("INVALID_SCHEMA"), quarantine.reasons,
                "a packet above the broker's largest frame must be rejected even with a matching digest");
        assertTrue(quarantine.details.get(0).contains("malformed_payload"), quarantine.details.get(0));
        assertEquals(0, converter.appendCalls.get());
    }

    @Test
    @DisplayName("no config key, field or diagnostic can disable the gate")
    void noOffSwitch() throws Exception {
        // The deleted flag's env key is ignored by config validation...
        byte[] payload = "m4-6-no-off-switch".getBytes(StandardCharsets.UTF_8);
        byte[] wrongDigest = ProtoTickFactory.sha256("other-bytes".getBytes(StandardCharsets.UTF_8));
        CapturingQuarantine quarantine = new CapturingQuarantine();
        CountingConverter converter = new CountingConverter();
        IngestionConfig config = config(Map.of("INGEST_VALIDATE_PAYLOAD_HASH", "false"));
        IngestionService service = service(quarantine, converter, config);

        service.processTickEvent(tick(payload, wrongDigest), "hft-0", 1L);
        awaitOutcome(converter, quarantine, 1);

        assertEquals(List.of("HASH_MISMATCH"), quarantine.reasons,
                "the deleted flag must not be readable — integrity is not optional");
        assertEquals(0, converter.appendCalls.get());

        // ...and the key cannot reappear silently: no constant, no field, no diagnostic.
        for (Field f : ConfigKeys.class.getDeclaredFields()) {
            assertFalse(f.getName().toUpperCase(Locale.ROOT).contains("PAYLOAD_HASH"),
                    "ConfigKeys." + f.getName() + " re-introduces the deleted integrity switch");
        }
        for (Field f : IngestionConfig.class.getDeclaredFields()) {
            assertFalse(f.getName().toLowerCase(Locale.ROOT).contains("payloadhash"),
                    "IngestionConfig." + f.getName() + " re-introduces the deleted integrity switch");
        }
        for (Method m : IngestionConfig.class.getDeclaredMethods()) {
            assertFalse(m.getName().toLowerCase(Locale.ROOT).contains("payloadhash"),
                    "IngestionConfig." + m.getName() + " re-introduces the deleted integrity switch");
        }
        for (String key : config.toMap().keySet()) {
            assertFalse(key.toUpperCase(Locale.ROOT).contains("PAYLOAD_HASH"),
                    "diagnostics re-introduce the deleted integrity switch: " + key);
        }
    }

    // ---- ING-DQ-010 seam: no live Fluss in unit tests ----

    private static DiscontinuitySink noopDiscontinuity() {
        return new DiscontinuitySink() {
            public void write(DiscontinuityWriter.Reason reason, String note,
                              DiscontinuityWriter.LastTickSnapshot before) {}
            public void write(DiscontinuityWriter.Reason reason, String note,
                              DiscontinuityWriter.LastTickSnapshot before,
                              Long instrumentToken, String exchange, String symbol) {}
            public void writeBridgeEvent(com.trading.ingestion.bridge.BridgeEvent event,
                                         DiscontinuityWriter.LastTickSnapshot before) {}
            public void close() {}
        };
    }

    private static SafetySink noopSafety() {
        return new SafetySink() {
            public String write(String slotId, long connectionEpoch,
                                SafetyHaltWriter.SafetyState state,
                                SafetyHaltWriter.ReasonCode reasonCode, String assignedTokenHash,
                                String evidenceReference, long detectedTsMs) {
                return "noop";
            }
            public void close() {}
        };
    }
}
