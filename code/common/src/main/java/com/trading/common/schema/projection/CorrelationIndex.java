package com.trading.common.schema.projection;

import java.util.Optional;

/**
 * Correlation lookup registry (T6, CHG-045). Unifies the three precedence
 * sources: broker-order-id, echoed client-order-ref, and the approved
 * reconciliation result (05-execution-core.md correlation contract). A single
 * source is authoritative; multiple matches are ambiguous and go to
 * {@code Postback_Quarantine} (see {@link PostbackCorrelator}).
 */
public interface CorrelationIndex {

    /** Lookup by broker-assigned order id (highest precedence). */
    Optional<AttemptRef> byBrokerOrderId(String brokerOrderId);

    /** Lookup by the echoed client order reference (second precedence). */
    Optional<AttemptRef> byEchoedClientOrderRef(String clientOrderRef);

    /**
     * Approved reconciliation result (lowest precedence) — only consulted when
     * broker_order_id is present but has no direct match in
     * {@link #byBrokerOrderId} (P4-314: the old "no broker id" wording
     * contradicted the sole caller — PostbackCorrelator.correlate calls this
     * only on the present-but-unmatched path). Both arguments are required
     * and must be non-blank.
     */
    Optional<AttemptRef> approvedReconciliation(String accountScopeId, String brokerOrderId);
}
