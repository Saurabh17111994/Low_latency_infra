package com.trading.common.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.common.ownership.OwnershipMatrix.Component;
import com.trading.common.ownership.OwnershipMatrix.Rule;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Pins the 12-row ownership matrix (docs/08_implementation/01-foundation.md &rarr;
 * "Ownership matrix", orig L921-934) so doc-vs-code drift fails loudly.
 */
class OwnershipMatrixTest {

    private Rule rule(String target) {
        return OwnershipMatrix.RULES.stream()
                .filter(r -> r.target.equals(target))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no rule for target: " + target));
    }

    @Test
    void matrixHasExactlyTheDocumentedTwelveRows() {
        assertEquals(12, OwnershipMatrix.RULES.size(), "matrix must contain the 12 doc rows");
        List<String> targets = OwnershipMatrix.RULES.stream()
                .map(r -> r.target)
                .sorted()
                .collect(Collectors.toList());
        assertEquals(
                List.of(
                        "broker REST call",
                        "candidates/ranking/decisions",
                        "candle/forming-bar state",
                        "order gate/attempt/mapping/audit",
                        "order lifecycle",
                        "portfolio reservations",
                        "position actions",
                        "position aggregate",
                        "postback audit/lifecycle",
                        "projection ledger",
                        "raw packet/decode",
                        "safety halt requests"),
                targets);
        // No duplicate targets — a second rule for the same data would be ambiguous.
        assertEquals(12, targets.stream().distinct().count());
    }

    @Test
    void soleOwnersMatchTheDocTable() {
        assertEquals(Component.INGESTION, rule("raw packet/decode").soleOwner);
        assertEquals(Component.SIGNAL_JOB, rule("candle/forming-bar state").soleOwner);
        assertEquals(Component.SIGNAL_JOB, rule("candidates/ranking/decisions").soleOwner);
        assertEquals(Component.ACTION_CAPTURE, rule("order lifecycle").soleOwner);
        assertEquals(Component.POSITION_PROJECTOR, rule("position aggregate").soleOwner);
        assertEquals(Component.EXECUTOR, rule("order gate/attempt/mapping/audit").soleOwner);
        assertEquals(Component.ACTION_CAPTURE, rule("postback audit/lifecycle").soleOwner);
        assertEquals(Component.SIGNAL_JOB, rule("portfolio reservations").soleOwner);
        assertEquals(Component.ACTION_CAPTURE, rule("projection ledger").soleOwner);
        assertEquals(null, rule("safety halt requests").soleOwner); // "Authorized components"
        assertEquals(Component.EXECUTOR, rule("broker REST call").soleOwner);
        assertEquals(Component.BABYSITTER, rule("position actions").soleOwner);
    }

    @Test
    void noRuleProhibitsItsOwnSoleOwner() {
        // Catches self-contradictory rows (a prohibited owner can never write).
        for (Rule r : OwnershipMatrix.RULES) {
            if (r.soleOwner == null) {
                continue; // authorized-components row: open owner set
            }
            assertFalse(r.prohibitedOwners.contains(r.soleOwner),
                    "rule '" + r.target + "' must not prohibit its own sole owner");
        }
    }

    @Test
    void writeAccessFollowsTheMatrix() {
        // raw packet: Ingestion writes; Strategy and Executor are prohibited.
        assertTrue(OwnershipMatrix.canWrite(rule("raw packet/decode"), Component.INGESTION));
        assertFalse(OwnershipMatrix.canWrite(rule("raw packet/decode"), Component.EXECUTOR));
        assertFalse(OwnershipMatrix.canWrite(rule("raw packet/decode"), Component.STRATEGY));
        // candles: Signal job writes; Ingestion/Executor cannot.
        assertTrue(OwnershipMatrix.canWrite(rule("candle/forming-bar state"), Component.SIGNAL_JOB));
        assertFalse(OwnershipMatrix.canWrite(rule("candle/forming-bar state"), Component.INGESTION));
        assertFalse(OwnershipMatrix.canWrite(rule("candle/forming-bar state"), Component.EXECUTOR));
        // candidates: Signal job writes; Action Capture prohibited.
        assertTrue(OwnershipMatrix.canWrite(rule("candidates/ranking/decisions"), Component.SIGNAL_JOB));
        assertFalse(OwnershipMatrix.canWrite(rule("candidates/ranking/decisions"), Component.ACTION_CAPTURE));
        // gate/attempt: Executor writes; Signal + Babysitter prohibited (doc "Signal, Executor"
        // interpreted as Signal + Babysitter — see OwnershipMatrix javadoc).
        assertTrue(OwnershipMatrix.canWrite(rule("order gate/attempt/mapping/audit"), Component.EXECUTOR));
        assertFalse(OwnershipMatrix.canWrite(rule("order gate/attempt/mapping/audit"), Component.SIGNAL_JOB));
        assertFalse(OwnershipMatrix.canWrite(rule("order gate/attempt/mapping/audit"), Component.BABYSITTER));
        // broker REST: only Executor writes.
        assertTrue(OwnershipMatrix.canWrite(rule("broker REST call"), Component.EXECUTOR));
        assertFalse(OwnershipMatrix.canWrite(rule("broker REST call"), Component.INGESTION));
        assertFalse(OwnershipMatrix.canWrite(rule("broker REST call"), Component.BROKER));
        // position actions: only Babysitter writes (post-MVP).
        assertTrue(OwnershipMatrix.canWrite(rule("position actions"), Component.BABYSITTER));
        assertFalse(OwnershipMatrix.canWrite(rule("position actions"), Component.EXECUTOR));
    }

    @Test
    void openOwnerRowIsNotDecidedByThisHelper() {
        // "Safety halt requests | Authorized components": the owner set is open, so
        // this class cannot authorize a writer — canWrite denies and the caller
        // checks isOpenOwnerRow and applies its own authorization.
        Rule r = rule("safety halt requests");
        assertTrue(OwnershipMatrix.isOpenOwnerRow(r));
        assertFalse(OwnershipMatrix.canWrite(r, Component.INGESTION));
        assertFalse(OwnershipMatrix.canWrite(r, Component.EXECUTOR));
        assertFalse(OwnershipMatrix.canWrite(r, Component.BABYSITTER));
        assertTrue(OwnershipMatrix.canRead(r, Component.EXECUTOR));
        assertFalse(OwnershipMatrix.canRead(r, Component.SIGNAL_JOB));
        // Only that row is open.
        for (Rule other : OwnershipMatrix.RULES) {
            if (other != r) {
                assertFalse(OwnershipMatrix.isOpenOwnerRow(other), other.target);
            }
        }
    }

    @Test
    void readAccessFollowsTheMatrix() {
        assertTrue(OwnershipMatrix.canRead(rule("order lifecycle"), Component.EXECUTOR));
        assertTrue(OwnershipMatrix.canRead(rule("order lifecycle"), Component.PLATFORM_HEALTH)); // operations
        assertFalse(OwnershipMatrix.canRead(rule("order lifecycle"), Component.SIGNAL_JOB));
        assertTrue(OwnershipMatrix.canRead(rule("projection ledger"), Component.RECOVERY_SCANNER));
        assertFalse(OwnershipMatrix.canRead(rule("projection ledger"), Component.EXECUTOR));
        assertTrue(OwnershipMatrix.canRead(rule("broker REST call"), Component.BROKER));
        assertFalse(OwnershipMatrix.canRead(rule("broker REST call"), Component.ACTION_CAPTURE));
    }

    @Test
    void everyRuleHasNonNullReadableMetadata() {
        for (Rule r : OwnershipMatrix.RULES) {
            assertNotNull(r.target);
            assertNotNull(r.readers);
            assertNotNull(r.prohibitedOwners);
            assertTrue(!r.readers.isEmpty() || r.soleOwner != null,
                    "rule '" + r.target + "' must have at least an owner or readers");
        }
    }

    @Test
    void nullRuleOrActorDeniesInsteadOfThrowing() {
        Rule r = rule("raw packet/decode");
        assertFalse(OwnershipMatrix.canWrite(null, Component.INGESTION));
        assertFalse(OwnershipMatrix.canWrite(r, null));
        assertFalse(OwnershipMatrix.canRead(null, Component.INGESTION));
        assertFalse(OwnershipMatrix.canRead(r, null));
        assertFalse(OwnershipMatrix.isOpenOwnerRow(null));
    }

    @Test
    void ruleSetsAreImmutableCopies() {
        // The "every other component" rows are built from mutable EnumSets; before
        // the copy, the builder of a rule kept a handle on global enforcement.
        for (Rule r : OwnershipMatrix.RULES) {
            assertThrows(UnsupportedOperationException.class, () -> r.readers.add(Component.BROKER));
            assertThrows(UnsupportedOperationException.class,
                    () -> r.prohibitedOwners.add(Component.BROKER));
        }
        Set<Component> readers = new HashSet<>();
        readers.add(Component.EXECUTOR);
        Rule local = new Rule("local", Component.INGESTION, readers, Set.of());
        readers.clear();
        assertTrue(local.readers.contains(Component.EXECUTOR),
                "the rule must own a copy, not the caller's set");
    }

    @Test
    void malformedRulesAreRejectedAtConstruction() {
        assertThrows(IllegalArgumentException.class,
                () -> new Rule("  ", Component.INGESTION, Set.of(), Set.of()));
        assertThrows(NullPointerException.class,
                () -> new Rule("t", Component.INGESTION, null, Set.of()));
        assertThrows(NullPointerException.class,
                () -> new Rule("t", Component.INGESTION, Set.of(), null));
    }

    @Test
    void logRuleIsLeastPrivilegeAndValidated() {
        Rule log = OwnershipMatrix.logRule("trading_logs", Component.SIGNAL_JOB);
        assertTrue(OwnershipMatrix.canWrite(log, Component.SIGNAL_JOB));
        assertFalse(OwnershipMatrix.canWrite(log, Component.STORAGE), "a reader is not a writer");
        assertFalse(OwnershipMatrix.canWrite(log, Component.BROKER));
        assertTrue(OwnershipMatrix.canRead(log, Component.PLATFORM_HEALTH)); // operations
        assertTrue(OwnershipMatrix.canRead(log, Component.STORAGE));         // lake/audit
        assertFalse(OwnershipMatrix.canRead(log, Component.BROKER));
        assertThrows(IllegalArgumentException.class,
                () -> OwnershipMatrix.logRule("  ", Component.SIGNAL_JOB));
        assertThrows(IllegalArgumentException.class, () -> OwnershipMatrix.logRule("t", null));
    }
}
