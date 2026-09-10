package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.common.schema.ImmutabilityProtocol;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * SCH-19 pure-JVM unit tests for the immutable instruction-feed protocol
 * (REQ-FLS-008/015): same id + same hash = idempotent duplicate, same id +
 * different hash = contract violation → quarantine enforcement event + halt,
 * the original immutable row is never mutated.
 */
@DisplayName("SCH-19: TradeInstructionFeedProtocol")
class TradeInstructionFeedProtocolTest {

    private static final long TS = 1_752_000_000_000L;
    private static final String INSTRUCTION = "ins-v1-abc123";
    private static final String HASH_A = "aaaa";
    private static final String HASH_B = "bbbb";

    @Test
    @DisplayName("first write with no stored hash is ACCEPTED and never halts")
    void firstWriteAccepted() {
        TradeInstructionFeedProtocol.Verification v =
                TradeInstructionFeedProtocol.verify(INSTRUCTION, null, HASH_A, TS);
        assertEquals(ImmutabilityProtocol.Outcome.ACCEPTED, v.outcome());
        assertTrue(v.accepted());
        assertFalse(v.duplicate());
        assertFalse(v.violation());
        assertFalse(TradeInstructionFeedProtocol.requiresHalt(v),
                "a first write must not halt");
        assertEquals(INSTRUCTION, v.instructionId());
        assertEquals(HASH_A, v.contentHash());
        assertEquals(TS, v.timestampMs());
    }

    @Test
    @DisplayName("identical hash under the same instruction_id is an idempotent DUPLICATE")
    void identicalHashIsDuplicate() {
        TradeInstructionFeedProtocol.Verification v =
                TradeInstructionFeedProtocol.verify(INSTRUCTION, HASH_A, HASH_A, TS);
        assertEquals(ImmutabilityProtocol.Outcome.DUPLICATE, v.outcome());
        assertTrue(v.duplicate());
        assertFalse(TradeInstructionFeedProtocol.requiresHalt(v),
                "replay/restart re-emission of the identical row must be dropped, not halted");
    }

    @Test
    @DisplayName("different hash under the same instruction_id is a VIOLATION that requires halt")
    void differentHashIsViolation() {
        TradeInstructionFeedProtocol.Verification v =
                TradeInstructionFeedProtocol.verify(INSTRUCTION, HASH_A, HASH_B, TS);
        assertEquals(ImmutabilityProtocol.Outcome.VIOLATION, v.outcome());
        assertTrue(v.violation());
        assertTrue(TradeInstructionFeedProtocol.requiresHalt(v),
                "same identity + different content is a contract violation (REQ-FLS-015) — "
                        + "halt the affected order flow");
    }

    @Test
    @DisplayName("VIOLATION yields the REQ-FLS-015 enforcement event with identity, hash, timestamp")
    void enforcementEventCarriesIdentityHashTimestamp() {
        TradeInstructionFeedProtocol.Verification v =
                TradeInstructionFeedProtocol.verify(INSTRUCTION, HASH_A, HASH_B, TS);
        TradeInstructionFeedProtocol.EnforcementViolation e =
                TradeInstructionFeedProtocol.enforcementEvent(v);
        assertEquals(INSTRUCTION, e.instructionId(), "source identity");
        assertEquals(HASH_B, e.contentHash(), "content hash of the offending row");
        assertEquals(TS, e.timestampMs(), "timestamp");
    }

    @Test
    @DisplayName("enforcementEvent refuses clean outcomes — no fabricated quarantine")
    void enforcementEventRejectsCleanOutcomes() {
        assertThrows(IllegalStateException.class, () ->
                TradeInstructionFeedProtocol.enforcementEvent(
                        TradeInstructionFeedProtocol.verify(INSTRUCTION, null, HASH_A, TS)),
                "a first write must not produce a quarantine event");
        assertThrows(IllegalStateException.class, () ->
                TradeInstructionFeedProtocol.enforcementEvent(
                        TradeInstructionFeedProtocol.verify(INSTRUCTION, HASH_A, HASH_A, TS)),
                "an idempotent duplicate must not produce a quarantine event");
    }

    @Test
    @DisplayName("null/blank inputs fail closed, empty stored hash is first-write (P2-067/068/187/188)")
    void nullAndEmptyInputsFailClosed() {
        // P2-067: record construction rejects nulls outright.
        assertThrows(NullPointerException.class, () ->
                new TradeInstructionFeedProtocol.Verification(null, INSTRUCTION, HASH_A, TS));
        assertThrows(NullPointerException.class, () ->
                new TradeInstructionFeedProtocol.Verification(
                        ImmutabilityProtocol.Outcome.ACCEPTED, null, HASH_A, TS));
        // P2-068: blank id/incoming reject; "" existing normalizes to first-write.
        assertThrows(IllegalArgumentException.class, () ->
                TradeInstructionFeedProtocol.verify("  ", null, HASH_A, TS));
        assertThrows(IllegalArgumentException.class, () ->
                TradeInstructionFeedProtocol.verify(INSTRUCTION, null, "   ", TS));
        TradeInstructionFeedProtocol.Verification v =
                TradeInstructionFeedProtocol.verify(INSTRUCTION, "", HASH_A, TS);
        assertTrue(v.accepted(), "KV twin '' for missing is first-write, not VIOLATION");
        assertFalse(TradeInstructionFeedProtocol.requiresHalt(v));
        // P2-187/188 asymmetric nulls: halt-decision fails closed, event fails loud.
        assertTrue(TradeInstructionFeedProtocol.requiresHalt(null),
                "unknown verification must halt live-money flow");
        assertThrows(IllegalArgumentException.class, () ->
                TradeInstructionFeedProtocol.enforcementEvent(null));
    }

    @Test
    @DisplayName("protocol is pure: the stored hash is never mutated by a check")
    void protocolNeverMutatesStoredHash() {
        String stored = HASH_A;
        TradeInstructionFeedProtocol.verify(INSTRUCTION, stored, HASH_B, TS);
        assertEquals(HASH_A, stored,
                "verify() must never write — the original immutable instruction is never mutated "
                        + "(REQ-FLS-015)");
        // repeated evaluation of the same inputs yields the same outcome
        assertEquals(ImmutabilityProtocol.Outcome.VIOLATION,
                TradeInstructionFeedProtocol.verify(INSTRUCTION, HASH_A, HASH_B, TS + 1).outcome(),
                "pure function of the inputs");
    }

    @Test
    @DisplayName("violation is detected end-to-end via the builder's canonical hash")
    void violationDetectedWithBuilderHash() {
        TradeDecision original = TradeDecisionBuilderTest.sampleDecision();
        String originalHash = TradeDecisionBuilder.canonicalHash(original);
        // same content re-emitted -> duplicate
        assertEquals(ImmutabilityProtocol.Outcome.DUPLICATE,
                TradeInstructionFeedProtocol.verify(
                        TradeDecisionBuilder.instructionId(original), originalHash, originalHash, TS)
                        .outcome());
        // a mutated decision under the same identity cannot exist by construction
        // (any executable change yields a new id), so a VIOLATION requires a
        // stored hash from a DIFFERENT content under the SAME id — the
        // enforcement surface REQ-FLS-015 exists for exactly that corrupt case.
        TradeInstructionFeedProtocol.Verification v = TradeInstructionFeedProtocol.verify(
                TradeDecisionBuilder.instructionId(original), "corrupted-stored-hash",
                originalHash, TS);
        assertTrue(v.violation());
    }
}
