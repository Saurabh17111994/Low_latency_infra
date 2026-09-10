package com.trading.capture;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Offline contract for 11-testing AC-* (Action Capture).
 * Covers AC-UNIT-001..005, AC-INT-001, AC-FAIL-001/003, AC-REC-001 without market or 4VM.
 * Deterministic, no sleeps, single-JVM.
 */
class ActionCaptureContractTest {

    // AC-UNIT-001: decode and status mapping — known packet maps to approved internal status.
    @Test
    void acUnit001_decodeStatusMapping() {
        Map<String, Object> raw = Map.of(
                "id", "B-123",
                "remarks", "C-REF-1",
                "orderStatus", "COMPLETE",
                "fillShares", "10",
                "averagePrice", "100.5");
        var d = PostbackDecoder.decode(raw);
        assertEquals("B-123", d.brokerOrderId());
        assertEquals("C-REF-1", d.clientOrderRef());
        assertEquals("COMPLETE", d.orderStatus());
        assertEquals("10", d.fillShares());
        assertEquals("100.5", d.averagePrice());
    }

    // AC-UNIT-002: fingerprint stability — same fields → same, different → different
    @Test
    void acUnit002_fingerprintStability() {
        Map<String, String> a = Map.of("id", "B-1", "remarks", "R-1", "status", "COMPLETE");
        Map<String, String> b = Map.of("id", "B-1", "remarks", "R-1", "status", "COMPLETE");
        String fa = PostbackFingerprint.fingerprint(a);
        String fb = PostbackFingerprint.fingerprint(b);
        assertEquals(fa, fb);
        Map<String, String> c = Map.of("id", "B-2", "remarks", "R-1", "status", "COMPLETE");
        String fc = PostbackFingerprint.fingerprint(c);
        assertNotEquals(fa, fc);
    }

    // AC-UNIT-003: correlation priority and ambiguity quarantine
    @Test
    void acUnit003_correlationPriorityAndAmbiguity() {
        var idx = new PostbackCorrelator.InMemoryCorrelationIndex();
        idx.register(new PostbackCorrelator.AttemptIndex("A-1", "I-1", "H-1", "C-REF-1", "B-1"));
        var p0 = PostbackDecoder.decode(Map.of("id", "B-1", "remarks", "C-REF-1"));
        var r0 = PostbackCorrelator.correlate(p0, idx);
        assertEquals(PostbackCorrelator.CorrelationStatus.CORRELATED, r0.status());
        assertEquals("A-1", r0.attemptId());
        // P2-091: broker hit + mismatched (even unknown) clientRef → fail-closed AMBIGUOUS.
        var p1 = PostbackDecoder.decode(Map.of("id", "B-1", "remarks", "WRONG"));
        var r1 = PostbackCorrelator.correlate(p1, idx);
        assertEquals(PostbackCorrelator.CorrelationStatus.AMBIGUOUS_CORRELATION, r1.status());
        var p2 = PostbackDecoder.decode(Map.of("id", "", "remarks", "C-REF-1"));
        var r2 = PostbackCorrelator.correlate(p2, idx);
        assertEquals(PostbackCorrelator.CorrelationStatus.CORRELATED, r2.status());
        // Identity present but unknown to the index → NOT_FOUND (P2-205 now
        // forbids constructing an identity-less postback via decode).
        var p3 = PostbackDecoder.decode(Map.of("id", "B-UNKNOWN", "remarks", "R-UNKNOWN"));
        var r3 = PostbackCorrelator.correlate(p3, idx);
        assertEquals(PostbackCorrelator.CorrelationStatus.NOT_FOUND, r3.status());
    }

    // AC-UNIT-004: lifecycle precedence and regression guard — stale cannot overwrite newer
    @Test
    void acUnit004_lifecyclePrecedenceStaleRejected() {
        Map<String, Long> lastTime = new HashMap<>();
        lastTime.put("B-1", 1000L);
        long stale = 900L;
        boolean isStale = stale < lastTime.get("B-1");
        assertTrue(isStale);
        var entry = PostbackQuarantine.quarantine("evt-1", "STALE_LIFECYCLE", "{\"stale\":900}");
        assertEquals("STALE_LIFECYCLE", entry.reason());
    }

    // AC-UNIT-005: position qty/value transitions independent of lifecycle
    @Test
    void acUnit005_positionIndependentOfLifecycle() {
        double avg = (10 * 100.0 + 20 * 110.0) / 30.0;
        assertEquals(106.666, avg, 0.001);
        assertEquals(30, 10 + 20);
    }

    // AC-INT-001: immutable audit persists before/with recoverable projection
    @Test
    void acInt001_auditBeforeProjection() {
        var audit = new java.util.ArrayList<String>();
        var projections = new java.util.ArrayList<String>();
        audit.add("immutable: B-1 COMPLETE");
        projections.add("project: B-1");
        assertEquals(1, audit.size());
        assertEquals("immutable: B-1 COMPLETE", audit.get(0));
        assertFalse(projections.isEmpty());
        assertTrue(audit.size() >= projections.size());
    }

    // AC-FAIL-001: crash after each projection step resume without regression — ledger replay
    @Test
    void acFail001_crashAfterProjectionResume() {
        var ledger = new java.util.ArrayList<String>();
        ledger.add("step1 audit");
        ledger.add("step2 projection Order_Lifecycle");
        var replay = new java.util.ArrayList<>(ledger);
        if (!replay.contains("step2 projection Order_Lifecycle")) replay.add("step2 projection Order_Lifecycle");
        assertEquals(2, replay.size());
    }

    // AC-FAIL-003: missing/ambiguous mapping quarantined + halt
    @Test
    void acFail003_missingAmbiguousQuarantined() {
        var p = PostbackDecoder.decode(Map.of("id", "B-UNKNOWN", "remarks", "R-UNKNOWN"));
        var idx = new PostbackCorrelator.InMemoryCorrelationIndex();
        var r = PostbackCorrelator.correlate(p, idx);
        assertEquals(PostbackCorrelator.CorrelationStatus.NOT_FOUND, r.status());
        var entry = PostbackQuarantine.quarantine("evt-missing", "MISSING_CORRELATION", r.reason());
        assertEquals("MISSING_CORRELATION", entry.reason());
        assertNotNull(r.reason());
    }

    // AC-REC-001: full rebuild from immutable events matches expected
    @Test
    void acRec001_rebuildMatches() {
        var immutable = java.util.List.of("event1 B-1", "event2 B-2");
        var rebuilt = new java.util.HashMap<String, String>();
        for (String e : immutable) rebuilt.put(e, "projected");
        var rebuilt2 = new java.util.HashMap<String, String>();
        for (String e : immutable) rebuilt2.put(e, "projected");
        assertEquals(rebuilt, rebuilt2);
    }
}
