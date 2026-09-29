package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.bridge.BridgeEvent;
import com.trading.ingestion.config.IngestionConfig;
import com.trading.ingestion.discontinuity.DiscontinuitySink;
import com.trading.ingestion.discontinuity.DiscontinuityWriter;
import com.trading.ingestion.health.NtpClockChecker;
import com.trading.ingestion.quarantine.QuarantineSink;
import com.trading.ingestion.quarantine.QuarantineWriter;
import com.trading.ingestion.safety.SafetyHaltWriter;
import com.trading.ingestion.safety.SafetySink;
import com.trading.ingestion.write.BoundedQueue;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * M4-3 — the bounded queue's 80%/100% listener is wired into readiness. Before
 * the fix {@code BoundedQueue.setListener} had no production caller, so a queue
 * at the warning band (or a stalled writer behind a full queue) left the service
 * "ready" (audit I3). The queue's threshold-to-event behavior is pinned by
 * {@code BoundedQueueTest}; this suite proves the SERVICE stored a listener on
 * every queue and that the stored listener drives the readiness dimension both
 * ways (a default no-op listener would leave readiness untouched).
 */
class QueueBackpressureReadinessTest {

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

    private static IngestionService service() throws Exception {
        return new IngestionService(
                "ing-queue-bp", List.of(), new StubFlussRowConverter("raw_table_1"),
                config(), new NtpClockChecker("127.0.0.1:9", 100, false),
                noopQuarantine(), noopDiscontinuity(), noopSafety());
    }

    /** The listener the service stored on the queue (the default no-op if unwired). */
    private static BoundedQueue.QueueListener wiredListener(BoundedQueue q) throws Exception {
        Field f = BoundedQueue.class.getDeclaredField("listener");
        f.setAccessible(true);
        return (BoundedQueue.QueueListener) f.get(q);
    }

    @Test
    @DisplayName("every queue carries the service's readiness listener, both ways")
    void everyQueueIsWiredToReadiness() throws Exception {
        IngestionService service = service();
        Field queuesField = IngestionService.class.getDeclaredField("queues");
        queuesField.setAccessible(true);
        BoundedQueue[] queues = (BoundedQueue[]) queuesField.get(service);
        assertTrue(queues.length > 0, "at least one queue");
        assertFalse((Boolean) service.health().diagnostics().get("queue_blocked"),
                "no episode at construction");
        for (int i = 0; i < queues.length; i++) {
            wiredListener(queues[i]).onQueueEvent(
                    BoundedQueue.QueueListener.Level.WARNING, 1, 1, 10, 100);
            assertTrue((Boolean) service.health().diagnostics().get("queue_blocked"),
                    "queue " + i + " warning must block readiness");
            wiredListener(queues[i]).onQueueEvent(
                    BoundedQueue.QueueListener.Level.RESUMED, 0, 0, 10, 100);
            assertFalse((Boolean) service.health().diagnostics().get("queue_blocked"),
                    "queue " + i + " resume must clear the block");
        }
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
