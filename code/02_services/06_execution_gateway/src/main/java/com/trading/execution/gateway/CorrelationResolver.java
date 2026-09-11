package com.trading.execution.gateway;

import java.util.Map;
import java.util.Objects;

/**
 * Tier 0 #6 bijective correlation guard.
 *
 * <p>Enforces one-to-one mapping between {@code execution_attempt_id} and
 * {@code broker_order_id} (and optionally {@code client_order_ref}). The
 * underlying {@code existingCorrelation} map is typically
 * {@code attemptId -> brokerOrderId} (or contains both directions);
 * this resolver checks both directions to detect hijacked or reused ids.
 *
 * <p>Fail-closed contract: if {@link #resolve} returns {@link Resolution#AMBIGUOUS},
 * the caller must quarantine the postback via {@link PostbackQuarantineStore#quarantine}
 * (or {@link FlussProjectionWriter#writeQuarantine}) and halt the affected scope —
 * no further fills/lifecycle/position writes for that event.
 */
public final class CorrelationResolver {

    public enum Resolution { CORRELATED, AMBIGUOUS, MISSING }

    /**
     * Bijective check against the durable correlation registry.
     *
     * @param attemptId the local execution attempt identity (may be null/blank if unknown)
     * @param brokerOrderId the broker-assigned order id (may be null/blank)
     * @param clientRef the echoed client order ref (may be null/blank)
     * @param existingCorrelation durable map of already-verified correlations.
     *        Expected shape is {@code attemptId -> brokerOrderId} but the method also
     *        handles reverse ({@code brokerOrderId -> attemptId}) and
     *        {@code clientRef -> attemptId} entries so callers can pass a combined view.
     * @return CORRELATED if a consistent mapping exists, AMBIGUOUS if the same attempt maps
     *         to a different broker (or the same broker maps to a different attempt/clientRef),
     *         MISSING if no mapping exists for the supplied keys
     */
    public Resolution resolve(String attemptId, String brokerOrderId, String clientRef,
                              Map<String, String> existingCorrelation) {
        String a = normalize(attemptId);
        String b = normalize(brokerOrderId);
        String c = normalize(clientRef);

        if (existingCorrelation == null || existingCorrelation.isEmpty()) {
            return Resolution.MISSING;
        }
        // Nothing to correlate at all — treat as missing, caller will quarantine as MISSING_BROKER_ID.
        if (a == null && b == null && c == null) {
            return Resolution.MISSING;
        }

        // --- 1) Direct key checks (attemptId as key) ---
        if (a != null && existingCorrelation.containsKey(a)) {
            String mappedBroker = existingCorrelation.get(a);
            // P3-056: broker-less partial key must not fall through to
            // CORRELATED without verification — fail closed to MISSING.
            if (b == null) {
                return Resolution.MISSING;
            }
            // null-valued mapping is inconsistent if caller supplied a broker
            if (!Objects.equals(b, mappedBroker)) {
                return Resolution.AMBIGUOUS;
            }
            // Bijective: no other attempt may map to the same broker
            for (Map.Entry<String, String> e : existingCorrelation.entrySet()) {
                // P3-274: null-safe — a null map key must not NPE the guard.
                if (!Objects.equals(e.getKey(), a) && b.equals(e.getValue())) {
                    return Resolution.AMBIGUOUS;
                }
            }
            // Also check clientRef bijective if present: clientRef as key -> attempt
            if (c != null && existingCorrelation.containsKey(c)) {
                String mappedAttempt = existingCorrelation.get(c);
                if (!a.equals(mappedAttempt)) {
                    return Resolution.AMBIGUOUS;
                }
            }
            // P3-057: clientRef bound elsewhere slips past the key check above
            // (early return skips sections 4-6) — scan values for hijack.
            if (c != null) {
                for (Map.Entry<String, String> e : existingCorrelation.entrySet()) {
                    if (c.equals(e.getValue()) && !a.equals(e.getKey())) {
                        return Resolution.AMBIGUOUS;
                    }
                }
            }
            return Resolution.CORRELATED;
        }

        // --- 2) Direct key checks (brokerOrderId as key -> attemptId) ---
        if (b != null && existingCorrelation.containsKey(b)) {
            String mappedAttempt = existingCorrelation.get(b);
            if (a != null && !a.equals(mappedAttempt)) {
                return Resolution.AMBIGUOUS;
            }
            // Bijective reverse: broker already mapped from different attempt key
            // (handled above). Ambiguity on this path is decided by the
            // mappedAttempt mismatch above — no attempt-as-value scan here:
            // P3-058 the combined view legitimately stores b1->a1 alongside
            // c1->a1/a1->b1, so scanning for attempt-as-value under other keys
            // false-AMBIGUOUSes healthy rows into quarantine+halt.
            return Resolution.CORRELATED;
        }

        // --- 3) Direct key checks (clientRef as key -> attemptId) ---
        if (c != null && existingCorrelation.containsKey(c)) {
            String mappedAttempt = existingCorrelation.get(c);
            if (a != null && !a.equals(mappedAttempt)) {
                return Resolution.AMBIGUOUS;
            }
            // Bijective: no other clientRef may map to same attempt with different value
            if (a != null) {
                for (Map.Entry<String, String> e : existingCorrelation.entrySet()) {
                    // P3-274: null-safe key comparison.
                    if (!Objects.equals(e.getKey(), c) && a.equals(e.getValue())) {
                        return Resolution.AMBIGUOUS;
                    }
                }
            }
            return Resolution.CORRELATED;
        }

        // --- 4) Value scans: brokerOrderId as value (attempt -> broker) ---
        // Reaching here implies b is not a key (section 2 returned otherwise),
        // so the old b-as-key branch here was unreachable (P3-477) — removed.
        if (b != null) {
            for (Map.Entry<String, String> e : existingCorrelation.entrySet()) {
                if (b.equals(e.getValue())) {
                    // broker found as value
                    if (a != null && !a.equals(e.getKey())) {
                        return Resolution.AMBIGUOUS;
                    }
                    return Resolution.CORRELATED;
                }
            }
        }

        // --- 5) Value scans: attemptId as value (broker -> attempt) ---
        if (a != null) {
            for (Map.Entry<String, String> e : existingCorrelation.entrySet()) {
                if (a.equals(e.getValue())) {
                    if (b != null && !b.equals(e.getKey())) {
                        return Resolution.AMBIGUOUS;
                    }
                    // Also check broker mismatch: if this entry's key is broker, but caller broker != key
                    // already handled above in b != null scan
                    return Resolution.CORRELATED;
                }
            }
        }

        // --- 6) Value scans: clientRef as value ---
        // Reaching here implies c is not a key (section 3 returned otherwise),
        // so the old c-as-key branch here was unreachable (P3-478) — removed.
        // The c-as-value scan below stands: clientRef-as-value is a supported shape.
        if (c != null) {
            for (Map.Entry<String, String> e : existingCorrelation.entrySet()) {
                if (c.equals(e.getValue())) {
                    if (a != null && !a.equals(e.getKey())) {
                        return Resolution.AMBIGUOUS;
                    }
                    return Resolution.CORRELATED;
                }
            }
        }

        // No mapping found for any supplied key/value
        return Resolution.MISSING;
    }

    private static String normalize(String s) {
        if (s == null) return null;
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }
}
