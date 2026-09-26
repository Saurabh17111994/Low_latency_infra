package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * CHG-323: startup subscription preflight — capacity and multi-socket policy.
 *
 * <p>Mirrors the Go bridge gates ({@code subscription_plan.go} capacity,
 * {@code multiSocketAllowedByPolicy} CHG-320) so a configuration the bridge
 * would refuse fails at Java startup with the effective numbers named, before
 * the bridge starts. Failing-first red under the CHG-323 evidence; this class
 * pins:
 *
 * <ul>
 *   <li>single socket within capacity passes (including exactly at capacity),</li>
 *   <li>over capacity fails naming tokens, capacity, connections, and the fix,</li>
 *   <li>multi-socket requires the approval flag in a non-production deployment,</li>
 *   <li>production stays single-socket even when approved,</li>
 *   <li>policy violations take precedence over capacity messages.</li>
 * </ul>
 */
@DisplayName("CHG-323: subscription capacity / multi-socket startup preflight")
class SubscriptionPreflightTest {

    private static final int PER_CONNECTION = 1024; // pinned ARROW_HFT_MAX_TOKENS_PER_CONNECTION

    private static Optional<String> check(int tokens, int connections,
                                          boolean production, boolean approved) {
        return SubscriptionPreflight.violation(
                tokens, connections, PER_CONNECTION, production, approved);
    }

    @Test
    @DisplayName("single socket within capacity passes (including exactly at capacity)")
    void withinCapacityPasses() {
        assertTrue(check(500, 1, false, false).isEmpty());
        assertTrue(check(1024, 1, false, false).isEmpty());
    }

    @Test
    @DisplayName("one token over capacity fails and names the numbers and the fix")
    void overCapacityNamesNumbers() {
        Optional<String> v = check(1025, 1, false, false);
        assertTrue(v.isPresent());
        assertTrue(v.get().contains("1025"), v.get());
        assertTrue(v.get().contains("1024"), v.get());
        assertTrue(v.get().contains("ARROW_HFT_CONNECTIONS"), v.get());
        assertTrue(v.get().contains("ARROW_HFT_MULTI_CONNECTION_APPROVED"), v.get());
    }

    @Test
    @DisplayName("full universe on one socket fails with the effective numbers")
    void fullUniverseOnOneSocketFails() {
        Optional<String> v = check(2433, 1, false, false);
        assertTrue(v.isPresent());
        assertTrue(v.get().contains("2433"), v.get());
        assertTrue(v.get().contains("1024"), v.get());
    }

    @Test
    @DisplayName("full universe on three approved dev sockets passes (2 433 <= 3 072)")
    void fullUniverseThreeSocketsPasses() {
        assertTrue(check(2433, 3, false, true).isEmpty());
        assertTrue(check(3072, 3, false, true).isEmpty());
    }

    @Test
    @DisplayName("three sockets without approval fail naming the approval flag")
    void threeSocketsWithoutApprovalFail() {
        Optional<String> v = check(2433, 3, false, false);
        assertTrue(v.isPresent());
        assertTrue(v.get().contains("ARROW_HFT_MULTI_CONNECTION_APPROVED"), v.get());
    }

    @Test
    @DisplayName("three sockets in production fail: production stays single-socket")
    void threeSocketsInProductionFail() {
        Optional<String> v = check(2433, 3, true, false);
        assertTrue(v.isPresent());
        assertTrue(v.get().contains("production"), v.get());
        assertTrue(v.get().contains("single-socket"), v.get());
        assertFalse(v.get().contains("exceed"), v.get());
    }

    @Test
    @DisplayName("approved three sockets in production still fail (policy before capacity)")
    void approvedThreeSocketsInProductionFail() {
        Optional<String> v = check(2433, 3, true, true);
        assertTrue(v.isPresent());
        assertTrue(v.get().contains("single-socket"), v.get());
    }

    @Test
    @DisplayName("over-capacity three-socket dev config fails on capacity after the policy gate")
    void overCapacityThreeSocketsFail() {
        Optional<String> v = check(3073, 3, false, true);
        assertTrue(v.isPresent());
        assertTrue(v.get().contains("3073"), v.get());
        assertTrue(v.get().contains("3072"), v.get());
    }
}
