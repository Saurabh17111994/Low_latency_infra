package com.trading.capture;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Pure static correlator mirroring the execution-core dossier §Capture-path identity and
 * correlation (3-step resolution + bijective invariant, CORR-008).
 *
 * <p>Resolution order (pure, no Fluss):
 * <ol>
 *   <li>verified {@code broker_order_id} present in Order_Correlation → correlated via that row</li>
 *   <li>otherwise verified {@code client_order_ref} (remarks) → correlated via attempt index</li>
 *   <li>otherwise reconciliation query (caller-supplied) → correlated if single match</li>
 * </ol>
 * Generic {@code order_id} is never used. Bijective invariant: one attempt ↔ ≤1 broker order,
 * one client_ref ↔ 1 attempt, one broker_order ↔ 1 attempt. Violation → AMBIGUOUS_CORRELATION →
 * quarantine + halt signal.
 *
 * <p>Pure logic — no Fluss, no broker dependency. In-memory index mirrors
 * {@code FlussCorrelationIndex}/{@code InMemoryCorrelationIndex}.
 */
public final class PostbackCorrelator {

    private PostbackCorrelator() {}

    public enum CorrelationStatus {
        CORRELATED,
        NOT_FOUND,
        AMBIGUOUS_CORRELATION
    }

    public record CorrelationResult(
            CorrelationStatus status,
            String attemptId,
            String instructionId,
            String reason) {}

    /**
     * Attempt index entry (pure). Mirrors Execution_Attempts row subset.
     */
    public record AttemptIndex(
            String attemptId,
            String instructionId,
            String requestHash,
            String clientOrderRef,
            String brokerOrderId) {}

    /**
     * In-memory correlation index (pure) for deterministic tests.
     * Two maps: brokerOrderId→attempt, clientRef→attempt. Enforces bijective checks on insert.
     */
    public static final class InMemoryCorrelationIndex {
        private final Map<String, AttemptIndex> byBroker = new HashMap<>();
        private final Map<String, AttemptIndex> byClientRef = new HashMap<>();
        private final Map<String, AttemptIndex> byAttemptId = new HashMap<>();

        /**
         * Register attempt. Returns false if bijective violation would occur.
         * Violation cases: same brokerOrderId already bound to different attempt,
         * same clientRef already bound to different attempt.
         */
        public boolean register(AttemptIndex attempt) {
            Objects.requireNonNull(attempt, "attempt");
            Objects.requireNonNull(attempt.attemptId(), "attemptId");
            // broker bijective: if brokerOrderId present, it must be unique
            if (attempt.brokerOrderId() != null && !attempt.brokerOrderId().isEmpty()) {
                AttemptIndex existing = byBroker.get(attempt.brokerOrderId());
                if (existing != null && !Objects.equals(existing.attemptId(), attempt.attemptId())) {
                    return false;
                }
            }
            // clientRef bijective: clientRef must be unique per attempt
            if (attempt.clientOrderRef() != null && !attempt.clientOrderRef().isEmpty()) {
                AttemptIndex existing = byClientRef.get(attempt.clientOrderRef());
                if (existing != null && !Objects.equals(existing.attemptId(), attempt.attemptId())) {
                    return false;
                }
            }
            // Re-register of the same attempt: drop stale secondary keys before repointing.
            AttemptIndex prev = byAttemptId.get(attempt.attemptId());
            if (prev != null) {
                if (prev.brokerOrderId() != null && !prev.brokerOrderId().equals(attempt.brokerOrderId())) {
                    byBroker.remove(prev.brokerOrderId());
                }
                if (prev.clientOrderRef() != null && !prev.clientOrderRef().equals(attempt.clientOrderRef())) {
                    byClientRef.remove(prev.clientOrderRef());
                }
            }
            byAttemptId.put(attempt.attemptId(), attempt);
            if (attempt.brokerOrderId() != null && !attempt.brokerOrderId().isEmpty()) {
                byBroker.put(attempt.brokerOrderId(), attempt);
            }
            if (attempt.clientOrderRef() != null && !attempt.clientOrderRef().isEmpty()) {
                byClientRef.put(attempt.clientOrderRef(), attempt);
            }
            return true;
        }

        public AttemptIndex lookupByBroker(String brokerOrderId) {
            if (brokerOrderId == null || brokerOrderId.isEmpty()) return null;
            return byBroker.get(brokerOrderId);
        }

        public AttemptIndex lookupByClientRef(String clientRef) {
            if (clientRef == null || clientRef.isEmpty()) return null;
            return byClientRef.get(clientRef);
        }

        public AttemptIndex lookupByAttemptId(String attemptId) {
            if (attemptId == null || attemptId.isEmpty()) return null;
            return byAttemptId.get(attemptId);
        }

        public int size() { return byAttemptId.size(); }
    }

    /**
     * Correlate a decoded postback against the index.
     *
     * @param postback decoded postback (from PostbackDecoder)
     * @param index correlation index (pre-populated with attempts)
     * @param reconciliationAttempt optional reconciliation-discovered attempt (may be null) — step 3 fallback
     * @return correlation result; AMBIGUOUS_CORRELATION when bijective check fails
     */
    public static CorrelationResult correlate(
            PostbackDecoder.DecodedPostback postback,
            InMemoryCorrelationIndex index,
            AttemptIndex reconciliationAttempt) {

        if (postback == null || index == null) {
            return new CorrelationResult(CorrelationStatus.NOT_FOUND, null, null, "null input");
        }

        String brokerOrderId = postback.brokerOrderId();
        String clientRef = postback.clientOrderRef();

        // Step 1: verified broker_order_id in Order_Correlation
        if (brokerOrderId != null && !brokerOrderId.isEmpty()) {
            AttemptIndex byBroker = index.lookupByBroker(brokerOrderId);
            if (byBroker != null) {
                if (clientRef != null && !clientRef.isEmpty()
                        && !clientRef.equals(byBroker.clientOrderRef())) {
                    return new CorrelationResult(CorrelationStatus.AMBIGUOUS_CORRELATION,
                            null, null, "broker_order_id and client_ref mismatch");
                }
                return new CorrelationResult(CorrelationStatus.CORRELATED,
                        byBroker.attemptId(), byBroker.instructionId(), "via broker_order_id");
            }
        }

        // Step 2: verified client_order_ref → attempt
        if (clientRef != null && !clientRef.isEmpty()) {
            AttemptIndex byClient = index.lookupByClientRef(clientRef);
            if (byClient != null) {
                return new CorrelationResult(CorrelationStatus.CORRELATED,
                        byClient.attemptId(), byClient.instructionId(), "via client_order_ref");
            }
        }

        // Step 3: reconciliation query (caller must have resolved to single attempt)
        if (reconciliationAttempt != null) {
            if (reconciliationAttempt.attemptId() == null || reconciliationAttempt.instructionId() == null
                    || index.lookupByAttemptId(reconciliationAttempt.attemptId()) == null) {
                return new CorrelationResult(CorrelationStatus.NOT_FOUND, null, null, "invalid reconciliation attempt");
            }
            return new CorrelationResult(CorrelationStatus.CORRELATED,
                    reconciliationAttempt.attemptId(), reconciliationAttempt.instructionId(),
                    "via reconciliation");
        }

        return new CorrelationResult(CorrelationStatus.NOT_FOUND, null, null, "no correlation");
    }

    /**
     * Convenience overload without reconciliation fallback.
     */
    public static CorrelationResult correlate(
            PostbackDecoder.DecodedPostback postback,
            InMemoryCorrelationIndex index) {
        return correlate(postback, index, null);
    }
}
