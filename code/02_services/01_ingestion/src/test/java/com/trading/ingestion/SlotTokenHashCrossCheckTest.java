package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.bridge.BridgeEvent;
import com.trading.ingestion.config.IngestionConfig;
import com.trading.ingestion.discontinuity.DiscontinuitySink;
import com.trading.ingestion.discontinuity.DiscontinuityWriter;
import com.trading.ingestion.health.NtpClockChecker;
import com.trading.ingestion.model.Instrument;
import com.trading.ingestion.quarantine.QuarantineSink;
import com.trading.ingestion.quarantine.QuarantineWriter;
import com.trading.ingestion.safety.SafetyHaltWriter;
import com.trading.ingestion.safety.SafetySink;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * H2-4 — {@code processBridgeEvent} must judge the bridge's per-slot
 * {@code assigned_token_set_hash} against the per-slot carve, never the
 * full-set digest. Pre-fix this test fails: a CORRECT acknowledgement on a
 * multi-socket plan carried the full-set digest into TOKEN_HASH_MISMATCH — the
 * bridge was right and the check was wrong. The real datastream shape (2433
 * NSE instruments over 3 sockets) is pinned here and in the shared fixture.
 */
class SlotTokenHashCrossCheckTest {

    private static Instrument instrument(long token) {
        return new Instrument.Builder()
                .instrumentToken(token).tradingSymbol("SYM" + token).exchange("NSE")
                .segment("EQ").lotSize(1).manifestVersion(1).build();
    }

    private static List<Instrument> instruments(long... tokens) {
        List<Instrument> out = new ArrayList<>();
        for (long token : tokens) {
            out.add(instrument(token));
        }
        return out;
    }

    /** Tokens 1..count — the only shape that actually fills more than one socket. */
    private static List<Long> sequence(int count) {
        List<Long> out = new ArrayList<>(count);
        for (long token = 1; token <= count; token++) {
            out.add(token);
        }
        return out;
    }

    private static List<Instrument> sequentialInstruments(int count) {
        List<Instrument> out = new ArrayList<>(count);
        for (long token = 1; token <= count; token++) {
            out.add(instrument(token));
        }
        return out;
    }

    private static IngestionConfig config(int connections) throws Exception {
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
        // 2 sockets for the small cases, 3 for the real 2433-token datastream.
        env.put("ARROW_HFT_CONNECTIONS", String.valueOf(connections));
        Method validateFrom = IngestionConfig.class.getDeclaredMethod("validateFrom", Map.class);
        validateFrom.setAccessible(true);
        return (IngestionConfig) validateFrom.invoke(null, env);
    }

    private static IngestionService service(List<Instrument> instruments, int connections) throws Exception {
        return new IngestionService(
                "ing-slot-hash", instruments, new StubFlussRowConverter("raw_table_1"),
                config(connections), new NtpClockChecker("127.0.0.1:9", 100, false),
                noopQuarantine(), noopDiscontinuity(), noopSafety());
    }

    private static void drive(IngestionService service, String slotId, int tokens,
                              String manifestFingerprint, String slotHash) throws Exception {
        BridgeEvent event = new BridgeEvent("slot_state", BridgeEvent.CONTRACT_VERSION, slotId,
                "ingestion-local/" + slotId, 3L, "ACTIVE", tokens, tokens, 0,
                "", 1_000L, manifestFingerprint, slotHash);
        Method m = IngestionService.class.getDeclaredMethod("processBridgeEvent", BridgeEvent.class);
        m.setAccessible(true);
        m.invoke(service, event);
    }

    private static String metrics(IngestionService service) {
        return service.metrics().buildMetricsJson();
    }

    @Test
    @DisplayName("a correct multi-slot ack is not TOKEN_HASH_MISMATCH (pre-fix: it was)")
    void correctPerSlotAckPassesTheCrossCheck() throws Exception {
        // 1025 tokens over 2 sockets: hft-0 = 1..1024, hft-1 = [1025]. Only now
        // does hft-0's slice digest differ from the full-set digest — exactly
        // the case the old comparison flagged on a correct bridge.
        IngestionService service = service(sequentialInstruments(1025), 2);
        String fullSet = SafetyHaltWriter.computeAssignedTokenHash(sequence(1025));
        String slot0 = SafetyHaltWriter.computeAssignedTokenHash(sequence(1024));

        // Exactly what Go emits: assigned_token_set_hash is hft-0's own slice
        // digest, manifest_fingerprint is the plan-wide full-set digest.
        drive(service, "hft-0", 1024, fullSet, slot0);

        String json = metrics(service);
        assertFalse(json.contains("TOKEN_HASH_MISMATCH"),
                "a correct per-slot ack must not raise TOKEN_HASH_MISMATCH:\n" + json);
        assertFalse(json.contains("FINGERPRINT_MISMATCH"),
                "a correct full-set manifest fingerprint must pass:\n" + json);
        assertFalse(json.contains("TOKEN_HASH_UNKNOWN_SLOT"));
        assertFalse(json.contains("TOKEN_HASH_EMPTY_SLICE"));
    }

    @Test
    @DisplayName("the real datastream (2433 tokens over 3 sockets) tail ack is clean")
    void datastreamUniverseTailSocketAckPasses() throws Exception {
        // The daily VM runs UNIVERSE=full: 2433 NSE instruments over 3 sockets
        // (1024+1024+385, day_run.py). Before H2-4 every one of these acks
        // raised TOKEN_HASH_MISMATCH.
        IngestionService service = service(sequentialInstruments(2433), 3);
        String fullSet = SafetyHaltWriter.computeAssignedTokenHash(sequence(2433));
        // hft-2 = tokens 2049..2433 (385 of them).
        String tail = SafetyHaltWriter.computeAssignedTokenHash(sequence(2433).subList(2048, 2433));

        drive(service, "hft-2", 385, fullSet, tail);

        String json = metrics(service);
        assertFalse(json.contains("TOKEN_HASH_MISMATCH"),
                "the real datastream's tail socket must pass the cross-check:\n" + json);
        assertFalse(json.contains("TOKEN_HASH_UNKNOWN_SLOT"));
        assertFalse(json.contains("TOKEN_HASH_EMPTY_SLICE"));
    }

    @Test
    @DisplayName("a wrong per-slot digest raises TOKEN_HASH_MISMATCH")
    void wrongPerSlotDigestIsReported() throws Exception {
        IngestionService service = service(sequentialInstruments(1025), 2);
        String fullSet = SafetyHaltWriter.computeAssignedTokenHash(sequence(1025));
        String slot1 = SafetyHaltWriter.computeAssignedTokenHash(List.of(1025L));

        // hft-0 reporting hft-1's digest: right shape, wrong slice.
        drive(service, "hft-0", 1024, fullSet, slot1);

        assertTrue(metrics(service).contains("TOKEN_HASH_MISMATCH"),
                "a wrong per-slot digest must raise TOKEN_HASH_MISMATCH");
    }

    @Test
    @DisplayName("a socket outside the carved plan raises TOKEN_HASH_UNKNOWN_SLOT")
    void unknownSlotIsReported() throws Exception {
        IngestionService service = service(instruments(1L, 2L, 3L), 2);
        String fullSet = SafetyHaltWriter.computeAssignedTokenHash(List.of(1L, 2L, 3L));

        drive(service, "hft-9", 3, fullSet, fullSet);

        assertTrue(metrics(service).contains("TOKEN_HASH_UNKNOWN_SLOT"),
                "a socket outside the carved plan must raise TOKEN_HASH_UNKNOWN_SLOT");
    }

    @Test
    @DisplayName("a configured-but-empty socket raises TOKEN_HASH_EMPTY_SLICE")
    void emptySliceIsReported() throws Exception {
        // One token, two configured sockets → the carve has hft-0 only. Go never
        // emits hft-1 for this plan, so an event for it is layout drift.
        IngestionService service = service(instruments(1L), 2);
        String fullSet = SafetyHaltWriter.computeAssignedTokenHash(List.of(1L));

        drive(service, "hft-1", 1, fullSet, fullSet);

        assertTrue(metrics(service).contains("TOKEN_HASH_EMPTY_SLICE"),
                "an event for an unfilled configured socket must raise TOKEN_HASH_EMPTY_SLICE");
    }

    // ---- ING-DQ-010 seam: no live Fluss in unit tests (same pattern as
    // SafetyTransitionMappingTest) ----
    private static QuarantineSink noopQuarantine() {
        return new QuarantineSink() {
            public void write(byte[] rawPayload, QuarantineWriter.Reason reason, String detail) {}
            public void write(byte[] rawPayload, QuarantineWriter.Reason reason, String detail,
                              Long instrumentToken, String exchange, String symbol) {}
            public void close() {}
        };
    }

    private static DiscontinuitySink noopDiscontinuity() {
        return new DiscontinuitySink() {
            public void write(DiscontinuityWriter.Reason reason, String note,
                              DiscontinuityWriter.LastTickSnapshot before) {}
            public void write(DiscontinuityWriter.Reason reason, String note,
                              DiscontinuityWriter.LastTickSnapshot before,
                              Long instrumentToken, String exchange, String symbol) {}
            public void writeBridgeEvent(BridgeEvent event, DiscontinuityWriter.LastTickSnapshot before) {}
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
