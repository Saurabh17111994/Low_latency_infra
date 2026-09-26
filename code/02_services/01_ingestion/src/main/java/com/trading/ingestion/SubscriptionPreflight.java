package com.trading.ingestion;

import java.util.Optional;

/**
 * Startup subscription preflight (CHG-323): the effective manifest and the HFT
 * connection policy must be subscribable before the Go bridge starts.
 *
 * <p>The Go bridge enforces the same rules as the last line of defense:
 * {@code BuildSubscriptionPlan} rejects a token count above
 * {@code slots x connectionLimit}, and {@code multiSocketAllowedByPolicy}
 * (CHG-320) allows more than one socket only with the explicit approval flag in
 * a non-production deployment. When a configuration violates either rule, the
 * bridge fails after startup with its own log lines, which Java records as a
 * bridge crash. This preflight surfaces the same violation at Java startup with
 * the effective numbers and the fix, before any bridge or subscription is
 * attempted.
 *
 * <p>Pure and unit-tested ({@code SubscriptionPreflightTest}); no cluster.
 */
public final class SubscriptionPreflight {

    private SubscriptionPreflight() {}

    /**
     * Returns the violation message when the effective token count and
     * connection policy cannot be subscribed; empty when valid. Policy
     * violations are reported before capacity violations so the operator fixes
     * one thing at a time.
     */
    static Optional<String> violation(int tokenCount, int connections, int maxTokensPerConnection,
                                      boolean production, boolean multiConnectionApproved) {
        int capacity = connections * maxTokensPerConnection;
        if (connections > 1) {
            if (production) {
                return Optional.of("subscription policy violation: ARROW_HFT_CONNECTIONS=" + connections
                        + " is not allowed in production — production deployments stay single-socket");
            }
            if (!multiConnectionApproved) {
                return Optional.of("subscription policy violation: ARROW_HFT_CONNECTIONS=" + connections
                        + " requires ARROW_HFT_MULTI_CONNECTION_APPROVED=true in a non-production"
                        + " deployment (the extra-socket approval)");
            }
        }
        if (tokenCount > capacity) {
            String fix = production
                    ? "use a manifest within capacity"
                    : "raise ARROW_HFT_CONNECTIONS (up to 3) with"
                            + " ARROW_HFT_MULTI_CONNECTION_APPROVED=true, or use a manifest"
                            + " within capacity";
            return Optional.of("subscription capacity violation: manifest tokens=" + tokenCount
                    + " exceed capacity=" + capacity + " (ARROW_HFT_CONNECTIONS=" + connections
                    + " x ARROW_HFT_MAX_TOKENS_PER_CONNECTION=" + maxTokensPerConnection
                    + ") — " + fix);
        }
        return Optional.empty();
    }
}
