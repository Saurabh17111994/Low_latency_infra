package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import com.trading.ingestion.transport.ControlRecord;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * M4-2 — the control record's OWN {@code contract_version} is the gate. Before
 * the fix {@code handleControlRecord} passed {@code BridgeEvent.CONTRACT_VERSION}
 * into the event and never read {@code cr.getContractVersion()}, so a v3 — or a
 * version-less record (proto3 default 0) — was processed as v2 (audit I2).
 * Unknown/missing/0 must quarantine {@code INVALID_SCHEMA} +
 * {@code CONTROL_VERSION_MISMATCH} and return: nothing downstream (slot
 * health, metrics, bridge events) may see it.
 */
class ControlContractVersionTest {

    private static final String HASH_64 = "a".repeat(64);

    /** Captures quarantine evidence (reason + scrubbed detail). */
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

    private static IngestionConfig config() throws Exception {
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
        Method validateFrom = IngestionConfig.class.getDeclaredMethod("validateFrom", Map.class);
        validateFrom.setAccessible(true);
        return (IngestionConfig) validateFrom.invoke(null, env);
    }

    private static IngestionService service(CapturingQuarantine quarantine) throws Exception {
        List<Instrument> instruments = List.of(new Instrument.Builder()
                .instrumentToken(1L).tradingSymbol("SYM1").exchange("NSE")
                .segment("EQ").lotSize(1).manifestVersion(1).build());
        return new IngestionService("ing-control-version", instruments,
                new StubFlussRowConverter("raw_table_1"), config(),
                new NtpClockChecker("127.0.0.1:9", 100, false),
                quarantine, noopDiscontinuity(), noopSafety());
    }

    private static void handle(IngestionService service, ControlRecord record) throws Exception {
        Method m = IngestionService.class.getDeclaredMethod("handleControlRecord", ControlRecord.class);
        m.setAccessible(true);
        m.invoke(service, record);
    }

    private static ControlRecord.Builder bridgeEvent(int contractVersion) {
        return ControlRecord.newBuilder()
                .setRecordType("bridge_event")
                .setContractVersion(contractVersion)
                .setEvent("slot_state")
                .setSlotId("hft-0")
                .setConnectionId("conn-1")
                .setConnectionEpoch(1)
                .setState("ACTIVE")
                .setAssignedTokens(1)
                .setAcknowledgedTokens(1)
                .setRejectedTokens(0)
                .setReason("")
                .setReceivedTsMs(System.currentTimeMillis())
                .setManifestFingerprint(HASH_64)
                .setAssignedTokenSetHash(HASH_64);
    }

    @Test
    @DisplayName("a v2 control record is processed (slot health reflects the ACTIVE slot)")
    void v2IsProcessed() throws Exception {
        CapturingQuarantine quarantine = new CapturingQuarantine();
        IngestionService service = service(quarantine);
        handle(service, bridgeEvent(BridgeEvent.CONTRACT_VERSION).build());
        assertTrue(quarantine.reasons.isEmpty(), "a v2 record must not be quarantined");
        assertFalse(service.metrics().buildMetricsJson().contains("CONTROL_VERSION_MISMATCH"));
        assertEquals(Boolean.TRUE, service.health().diagnostics().get("broker_connected"),
                "an ACTIVE v2 slot_state must reach slot health");
    }

    @Test
    @DisplayName("v3, missing (0) and any other version are quarantined and never processed")
    void wrongVersionsAreQuarantined() throws Exception {
        for (int version : new int[] {3, 0, 1}) {
            CapturingQuarantine quarantine = new CapturingQuarantine();
            IngestionService service = service(quarantine);
            handle(service, bridgeEvent(version).build());

            assertEquals(List.of("INVALID_SCHEMA"), quarantine.reasons,
                    "v" + version + " must be quarantined as INVALID_SCHEMA");
            assertTrue(quarantine.details.get(0).contains("contract_version=" + version),
                    quarantine.details.get(0));
            assertTrue(service.metrics().buildMetricsJson().contains("CONTROL_VERSION_MISMATCH"),
                    "v" + version + " must count CONTROL_VERSION_MISMATCH");
            assertEquals(Boolean.FALSE, service.health().diagnostics().get("broker_connected"),
                    "v" + version + " must not touch slot health (zero downstream)");
        }
    }

    @Test
    @DisplayName("a wrong-version bridge_metrics is quarantined and never applied")
    void wrongVersionMetricsAreNotApplied() throws Exception {
        CapturingQuarantine quarantine = new CapturingQuarantine();
        IngestionService service = service(quarantine);
        handle(service, ControlRecord.newBuilder()
                .setRecordType("bridge_metrics").setContractVersion(3)
                .setActiveSockets(7).setReconnectConsecutive(1).setGoGoroutines(9)
                .setTsMs(System.currentTimeMillis()).build());

        assertEquals(List.of("INVALID_SCHEMA"), quarantine.reasons);
        assertTrue(service.metrics().buildMetricsJson().contains("CONTROL_VERSION_MISMATCH"));
        Matcher m = Pattern.compile("bridge\\.active_sockets[^0-9]*([0-9]+)")
                .matcher(service.metrics().buildMetricsJson());
        assertTrue(m.find(), "the active_sockets gauge must be present");
        assertEquals("0", m.group(1),
                "a wrong-version bridge_metrics must not apply (active_sockets stays 0)");
    }

    // ---- ING-DQ-010 seam: no live Fluss in unit tests (same pattern as
    // SafetyTransitionMappingTest) ----
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
