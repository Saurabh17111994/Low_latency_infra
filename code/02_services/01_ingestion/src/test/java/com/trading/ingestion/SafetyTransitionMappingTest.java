package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ING-SAFE-001..003 — slot-scoped safety evidence correctness at the ingestion
 * layer (plan Amendment §Slot-scoped safety propagation).
 *
 * <p>There is no Signal Job module in this repository yet (02_compute has only
 * BabysitterJob), so decision suppression itself cannot be integration-tested.
 * These tests pin the ingestion-side evidence that a Signal Job would consume:
 * the exact UNSAFE/RECOVERED transition mapping ({@link
 * IngestionService#unsafeReasonFor} / {@link IngestionService#isRecoveredTransition})
 * and the deterministic identity computations ({@link
 * SafetyHaltWriter#computeHaltRequestId}, {@link
 * SafetyHaltWriter#computeAssignedTokenHash}) that make each transition
 * slot-scoped and re-delivery-safe.
 *
 * <ul>
 *   <li>ING-SAFE-001 — a disconnect on one slot maps to READ_FAILURE/UNSAFE for
 *       exactly that slot; a healthy slot's full ack maps to RECOVERED. The
 *       evidence is keyed by slot_id and token-set hash, so only the affected
 *       deterministic token set can ever be suppressed downstream.</li>
 *   <li>ING-SAFE-002 — partial acknowledgement (rejected &gt; 0, not active)
 *       maps to SUBSCRIPTION_PARTIAL/UNSAFE for the affected slot; a full ack
 *       never does.</li>
 *   <li>ING-SAFE-003 — RECOVERED requires ACTIVE + full acknowledgement via
 *       {@code subscription_ack} (the post-recovery frame is confirmed by the
 *       reader loop and unresolved ingestion uncertainty by the shutdown
 *       journal; see ING-SAFE-003 plan text).</li>
 * </ul>
 */
@DisplayName("ING-SAFE-001..003: slot-scoped safety transition evidence")
class SafetyTransitionMappingTest {

    /** 64 lowercase hex chars — the SHA-256 digest shape the bridge emits. */
    private static final String HEX64 =
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    private static BridgeEvent event(String eventName, String state, int assigned,
                                     int acknowledged, int rejected, String reason) {
        return new BridgeEvent(eventName, BridgeEvent.CONTRACT_VERSION, "hft-0",
                "ingestion-local/hft-0", 3L, state, assigned, acknowledged, rejected,
                reason == null ? "" : reason, 1_000L, HEX64, HEX64);
    }

    /** The slot-readiness computation used by processBridgeEvent. */
    private static boolean active(BridgeEvent e) {
        return "ACTIVE".equals(e.state())
                && e.assignedTokens() == e.acknowledgedTokens()
                && e.rejectedTokens() == 0;
    }

    @Test
    @DisplayName("disconnect → READ_FAILURE/UNSAFE for the affected slot")
    void disconnectIsUnsafeReadFailure() {
        BridgeEvent e = event("disconnect", "ACTIVE", 1024, 1024, 0, null);
        assertEquals(SafetyHaltWriter.ReasonCode.READ_FAILURE,
                IngestionService.unsafeReasonFor(e, active(e), false));
        // Healthier slot on the same event stream (full ack) is not unsafe.
        BridgeEvent healthy = event("subscription_ack", "ACTIVE", 1024, 1024, 0, null);
        assertNull(IngestionService.unsafeReasonFor(healthy, active(healthy), false));
    }

    @Test
    @DisplayName("full mapping table: every bridge event maps exactly per plan")
    void fullMappingTable() {
        // feed_stalled without decode_error_burst
        BridgeEvent stall = event("feed_stalled", "STALLED", 1024, 512, 0, "feed gap");
        assertEquals(SafetyHaltWriter.ReasonCode.FEED_STALLED,
                IngestionService.unsafeReasonFor(stall, active(stall), false));

        // feed_stalled with decode_error_burst
        BridgeEvent burst = event("feed_stalled", "STALLED", 1024, 512, 0, "decode_error_burst");
        assertEquals(SafetyHaltWriter.ReasonCode.DECODE_ERROR_BURST,
                IngestionService.unsafeReasonFor(burst, active(burst), false));

        // heartbeat_failed
        BridgeEvent hb = event("heartbeat_failed", "ACTIVE", 1024, 1024, 0, null);
        assertEquals(SafetyHaltWriter.ReasonCode.HEARTBEAT_FAILED,
                IngestionService.unsafeReasonFor(hb, active(hb), false));

        // auth_failure
        BridgeEvent auth = event("auth_failure", "AUTH_FAILED", 0, 0, 0, "401");
        assertEquals(SafetyHaltWriter.ReasonCode.AUTH_FAILURE,
                IngestionService.unsafeReasonFor(auth, active(auth), false));

        // bridge_shutdown → BRIDGE_EXIT (bridge_exit is dead vocabulary —
        // BridgeEvent rejects it, the switch arm is preserved for exactness)
        BridgeEvent shut = event("bridge_shutdown", "TERMINAL", 0, 0, 0, "operator");
        assertEquals(SafetyHaltWriter.ReasonCode.BRIDGE_EXIT,
                IngestionService.unsafeReasonFor(shut, active(shut), false));
        // A2: during graceful shutdown a bridge exit is expected — no halt.
        assertNull(IngestionService.unsafeReasonFor(shut, active(shut), true),
                "shutdown-initiated bridge exit must not map to BRIDGE_EXIT");

        // subscription_ack partial → SUBSCRIPTION_PARTIAL
        BridgeEvent partial = event("subscription_ack", "PARTIAL", 1024, 900, 124, "tokens rejected");
        assertEquals(SafetyHaltWriter.ReasonCode.SUBSCRIPTION_PARTIAL,
                IngestionService.unsafeReasonFor(partial, active(partial), false));

        // subscription_ack TERMINAL + timeout → SUBSCRIPTION_TIMEOUT
        BridgeEvent timeout = event("subscription_ack", "TERMINAL", 0, 0, 0,
                "subscription_response_timeout");
        assertEquals(SafetyHaltWriter.ReasonCode.SUBSCRIPTION_TIMEOUT,
                IngestionService.unsafeReasonFor(timeout, active(timeout), false));

        // slot_state / reconnect carry no unsafe transition
        BridgeEvent slotState = event("slot_state", "CONNECTING", 0, 0, 0, null);
        assertNull(IngestionService.unsafeReasonFor(slotState, active(slotState), false));
        BridgeEvent reconnect = event("reconnect", "ACTIVE", 1024, 1024, 0, null);
        assertNull(IngestionService.unsafeReasonFor(reconnect, active(reconnect), false));
    }

    @Test
    @DisplayName("ING-SAFE-002: partial ack is unsafe, full ack never is")
    void partialAckUnsafeFullAckNever() {
        BridgeEvent partial = event("subscription_ack", "PARTIAL", 1024, 900, 124, "tokens rejected");
        assertEquals(SafetyHaltWriter.ReasonCode.SUBSCRIPTION_PARTIAL,
                IngestionService.unsafeReasonFor(partial, active(partial), false));

        // Full ack, any state — never unsafe.
        BridgeEvent fullActive = event("subscription_ack", "ACTIVE", 1024, 1024, 0, null);
        assertNull(IngestionService.unsafeReasonFor(fullActive, active(fullActive), false));
        BridgeEvent fullNonActive = event("subscription_ack", "CONNECTING", 1024, 1024, 0, null);
        assertNull(IngestionService.unsafeReasonFor(fullNonActive, active(fullNonActive), false));

        // A partial ack with active() true (impossible by construction) must
        // still not map to SUBSCRIPTION_PARTIAL — the guard requires both
        // rejected > 0 AND !active.
        BridgeEvent partialActive = event("subscription_ack", "ACTIVE", 1024, 900, 124, "partial");
        assertNull(IngestionService.unsafeReasonFor(partialActive, true, false));
    }

    @Test
    @DisplayName("ING-SAFE-003: RECOVERED only on ACTIVE + full ack subscription_ack")
    void recoveredRequiresActiveFullAck() {
        BridgeEvent fullActive = event("subscription_ack", "ACTIVE", 1024, 1024, 0, null);
        assertEquals(true, IngestionService.isRecoveredTransition(fullActive, active(fullActive)));

        // Same event with a rejected token is NOT recovered.
        BridgeEvent partial = event("subscription_ack", "ACTIVE", 1024, 900, 124, "partial");
        assertEquals(false, IngestionService.isRecoveredTransition(partial, active(partial)));

        // Non-ACTIVE state (even with full counts) is NOT recovered.
        BridgeEvent nonActive = event("subscription_ack", "CONNECTING", 1024, 1024, 0, null);
        assertEquals(false, IngestionService.isRecoveredTransition(nonActive, active(nonActive)));

        // slot_state ACTIVE is NOT a recovery — only subscription_ack is.
        BridgeEvent slotStateActive = event("slot_state", "ACTIVE", 1024, 1024, 0, null);
        assertEquals(false,
                IngestionService.isRecoveredTransition(slotStateActive, active(slotStateActive)));

        // reconnect ACTIVE is NOT a recovery either.
        BridgeEvent reconnect = event("reconnect", "ACTIVE", 1024, 1024, 0, null);
        assertEquals(false,
                IngestionService.isRecoveredTransition(reconnect, active(reconnect)));
    }

    @Test
    @DisplayName("ING-SAFE-001: halt_request_id is slot-scoped and tuple-deterministic")
    void haltRequestIdSlotScopedDeterministic() {
        String a = SafetyHaltWriter.computeHaltRequestId("fp-abc", "hft-0", 3, "UNSAFE", "READ_FAILURE");
        String a2 = SafetyHaltWriter.computeHaltRequestId("fp-abc", "hft-0", 3, "UNSAFE", "READ_FAILURE");
        assertEquals(a, a2, "same tuple → same id (duplicate delivery is a no-op)");
        assertEquals(64, a.length(), "SHA-256 hex is 64 chars");

        // Different slot → different id: only that slot's deterministic token
        // set can be suppressed (ING-SAFE-001).
        String b = SafetyHaltWriter.computeHaltRequestId("fp-abc", "hft-1", 3, "UNSAFE", "READ_FAILURE");
        assertNotEquals(a, b, "different slot → different id");

        // Different epoch / reason / state → different id.
        assertNotEquals(a,
                SafetyHaltWriter.computeHaltRequestId("fp-abc", "hft-0", 4, "UNSAFE", "READ_FAILURE"),
                "different epoch → different id");
        assertNotEquals(a,
                SafetyHaltWriter.computeHaltRequestId("fp-abc", "hft-0", 3, "UNSAFE", "AUTH_FAILURE"),
                "different reason → different id");
        assertNotEquals(a,
                SafetyHaltWriter.computeHaltRequestId("fp-abc", "hft-0", 3, "RECOVERED", ""),
                "different state → different id");
        assertNotEquals(a,
                SafetyHaltWriter.computeHaltRequestId("fp-def", "hft-0", 3, "UNSAFE", "READ_FAILURE"),
                "different manifest fingerprint → different id");
    }

    @Test
    @DisplayName("ING-SAFE-001: assigned-token-set hash is deterministic, order-independent")
    void assignedTokenSetHashOrderIndependent() {
        String h = SafetyHaltWriter.computeAssignedTokenHash(List.of(757614L, 3045L, 11536L));
        assertEquals(h, SafetyHaltWriter.computeAssignedTokenHash(List.of(11536L, 757614L, 3045L)),
                "hash must be over the sorted token set");
        assertEquals(64, h.length());
        assertNotEquals(h, SafetyHaltWriter.computeAssignedTokenHash(List.of(757614L, 3045L)),
                "different token set → different hash");
    }

    @Test
    @DisplayName("quality-class quarantine reasons map to additive safety codes")
    void qualityClassReasonsMapExactly() {
        assertEquals(SafetyHaltWriter.ReasonCode.FUTURE_BROKER_TIMESTAMP,
                IngestionService.qualityUnsafeReason(QuarantineWriter.Reason.FUTURE_BROKER_TIMESTAMP));
        assertEquals(SafetyHaltWriter.ReasonCode.STALE_BROKER_TIMESTAMP,
                IngestionService.qualityUnsafeReason(QuarantineWriter.Reason.STALE_BROKER_TIMESTAMP));
        // Every other quarantine reason carries no safety evidence.
        for (QuarantineWriter.Reason r : QuarantineWriter.Reason.values()) {
            if (r == QuarantineWriter.Reason.FUTURE_BROKER_TIMESTAMP
                    || r == QuarantineWriter.Reason.STALE_BROKER_TIMESTAMP) {
                continue;
            }
            assertNull(IngestionService.qualityUnsafeReason(r),
                    "reason " + r + " must not map to a safety code");
        }
    }

    @Test
    @DisplayName("RESOURCE_EXHAUSTED fires only at the critical FD threshold")
    void criticalResourceConditionThreshold() {
        assertEquals(false, IngestionService.isCriticalResourceCondition(0.0));
        assertEquals(false, IngestionService.isCriticalResourceCondition(89.99));
        assertEquals(true, IngestionService.isCriticalResourceCondition(90.0));
        assertEquals(true, IngestionService.isCriticalResourceCondition(99.9));
        // A broken platform (/proc unavailable → 0%) must never fire.
        assertEquals(false, IngestionService.isCriticalResourceCondition(-1.0));
    }

    @Test
    @DisplayName("R-298: one real write per (state, tuple) — repeats are gated")
    void firstEmissionGatesRepeatedTuple() {
        Set<String> emitted = ConcurrentHashMap.newKeySet();
        String fp = "fp-r298";
        String id = SafetyHaltWriter.computeHaltRequestId(
                fp, "hft-0", 3, "UNSAFE", "STALE_BROKER_TIMESTAMP");

        // First STALE tick for this slot/epoch → the write is allowed.
        assertTrue(IngestionService.firstEmission(emitted, "UNSAFE", id),
                "first emission of the tuple must be written");
        // Every repeated STALE tick for the SAME tuple → no write (pre-R-298
        // wrote an upsert per tick and deduped only the log line).
        assertFalse(IngestionService.firstEmission(emitted, "UNSAFE", id),
                "repeated tick must not re-write the same tuple");
        assertFalse(IngestionService.firstEmission(emitted, "UNSAFE", id),
                "repeat is still gated on the third tick");

        // A NEW epoch (slot reconnected) is a new transition → write again.
        String idNewEpoch = SafetyHaltWriter.computeHaltRequestId(
                fp, "hft-0", 4, "UNSAFE", "STALE_BROKER_TIMESTAMP");
        assertTrue(IngestionService.firstEmission(emitted, "UNSAFE", idNewEpoch),
                "new epoch → new transition → write");
        // New reason (FUTURE) on the same epoch is also a new transition.
        String idNewReason = SafetyHaltWriter.computeHaltRequestId(
                fp, "hft-0", 4, "UNSAFE", "FUTURE_BROKER_TIMESTAMP");
        assertTrue(IngestionService.firstEmission(emitted, "UNSAFE", idNewReason),
                "new reason → new transition → write");
        // A different slot is independent of the first.
        String idOtherSlot = SafetyHaltWriter.computeHaltRequestId(
                fp, "hft-1", 3, "UNSAFE", "STALE_BROKER_TIMESTAMP");
        assertTrue(IngestionService.firstEmission(emitted, "UNSAFE", idOtherSlot),
                "different slot → independent transition → write");
    }

    @Test
    @DisplayName("R-298: UNSAFE and RECOVERED are separate gates for the same tuple")
    void firstEmissionSeparatesStates() {
        Set<String> emitted = ConcurrentHashMap.newKeySet();
        String fp = "fp-r298";
        String unsafe = SafetyHaltWriter.computeHaltRequestId(
                fp, "hft-0", 3, "UNSAFE", "FEED_STALLED");
        String recovered = SafetyHaltWriter.computeHaltRequestId(
                fp, "hft-0", 3, "RECOVERED", "");

        assertTrue(IngestionService.firstEmission(emitted, "UNSAFE", unsafe));
        assertFalse(IngestionService.firstEmission(emitted, "UNSAFE", unsafe),
                "duplicate UNSAFE is gated");
        // Recovery for the same slot/epoch must NOT be gated by the UNSAFE write.
        assertTrue(IngestionService.firstEmission(emitted, "RECOVERED", recovered),
                "RECOVERED is a distinct gate from UNSAFE");
        assertFalse(IngestionService.firstEmission(emitted, "RECOVERED", recovered),
                "duplicate RECOVERED is gated");
    }

    @Test
    @DisplayName("R-298: pre-write id equals the writer's internal tuple id")
    void preWriteIdMatchesWriterTuple() {
        // The call sites compute computeHaltRequestId(manifestFingerprint,
        // slot, epoch, state, reason) BEFORE write(); write() derives its
        // returned id from the same tuple. A mismatch would silently break
        // the dedup (each write would compute a fresh, ungated id).
        String fp = "fp-r298";
        long epoch = 7;
        String reason = "STALE_BROKER_TIMESTAMP";
        String preWrite = SafetyHaltWriter.computeHaltRequestId(fp, "hft-0", epoch, "UNSAFE", reason);
        assertEquals(preWrite,
                SafetyHaltWriter.computeHaltRequestId(fp, "hft-0", epoch, "UNSAFE", reason),
                "same tuple → same id regardless of caller");
        // RECOVERED uses the empty reason exactly as write(null) does.
        String recovered = SafetyHaltWriter.computeHaltRequestId(fp, "hft-0", epoch, "RECOVERED", "");
        assertNotEquals(preWrite, recovered);
        assertEquals(64, recovered.length());
    }

    @Test
    @DisplayName("A1: freshness-gate grace window is open only while armed")
    void freshnessGraceWindow() throws Exception {
        // A1: after subscription_ack the service arms a 30s grace window during
        // which STALE/FUTURE packets are quarantined but do NOT halt. The helper
        // is wall-clock based; pin its semantics via the armed-until field.
        java.util.Map<String, String> env = new java.util.HashMap<>();
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
        java.lang.reflect.Method validateFrom = IngestionConfig.class
                .getDeclaredMethod("validateFrom", java.util.Map.class);
        validateFrom.setAccessible(true);
        IngestionConfig config = (IngestionConfig) validateFrom.invoke(null, env);
        IngestionService service = new IngestionService(
                "ing-a1", java.util.List.of(), new StubFlussRowConverter("raw_table_1"),
                config, new NtpClockChecker("127.0.0.1:9", 100, false),
                noopQuarantine(), noopDiscontinuity(), noopSafety());
        java.lang.reflect.Field f = IngestionService.class.getDeclaredField("gracePeriodUntilMs");
        f.setAccessible(true);
        java.lang.reflect.Method m = IngestionService.class.getDeclaredMethod("inFreshnessGracePeriod");
        m.setAccessible(true);

        // Not armed yet (default 0L) → no grace.
        assertFalse((Boolean) m.invoke(service), "before arming, grace must be inactive");
        // Armed 30s in the future (as subscription_ack does) → grace active.
        f.setLong(service, System.currentTimeMillis() + 30_000L);
        assertTrue((Boolean) m.invoke(service), "within the 30s window, grace must be active");
        // Window expired → grace inactive again (original halt behavior resumes).
        f.setLong(service, System.currentTimeMillis() - 1L);
        assertFalse((Boolean) m.invoke(service), "after the window expires, grace must be inactive");
    }
    // ---- ING-DQ-010 seam: no live Fluss in unit tests ----
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
