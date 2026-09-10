package com.trading.compute.signaljob;

import com.trading.common.schema.ImmutabilityProtocol;

/**
 * Immutable instruction-feed protocol (SCH-19; REQ-FLS-008/015, REQ-SS-004,
 * {@code docs/04_contracts/07-executor.md}).
 *
 * <p>Per {@code instruction_id} the writer persists the canonical content hash
 * (see {@link TradeDecisionBuilder#canonicalHash}) in the instruction index
 * (the Fluss KV twin, DEC-038 — Fluss is the authoritative durable index; the
 * LOG itself is the rebuild source). Every emission is checked against that
 * stored hash before the LOG append:
 *
 * <pre>{@code
 * no stored hash        → ACCEPTED  (first write: append LOG + record hash)
 * stored hash == new    → DUPLICATE (idempotent replay/restart re-emission: drop)
 * stored hash != new    → VIOLATION (contract violation: quarantine + halt,
 *                                    original row is never mutated)
 * }</pre>
 *
 * <p>This is the LOG-table realization of the canonical immutability protocol
 * ({@link ImmutabilityProtocol}: same id + same hash = duplicate evidence;
 * same id + different hash = contract violation). LOG tables have no
 * point-lookup by key, so the writer MUST persist/query enough state to detect
 * mutation — LOG comments or {@code NOT ENFORCED} keys do not enforce it.
 *
 * <p>Pure JVM: no state, no side effects. On {@code VIOLATION} the caller
 * (the future decision operator) raises the {@link EnforcementViolation}
 * quarantine event and the halt path ({@code Safety_Halt_Requests} + live-money
 * stop condition), per REQ-FLS-015 — the original immutable row is never
 * touched.
 */
public final class TradeInstructionFeedProtocol {

    private TradeInstructionFeedProtocol() {}

    /** Result of checking one emission against the stored instruction hash. */
    public record Verification(
            ImmutabilityProtocol.Outcome outcome,
            String instructionId,
            String contentHash,
            long timestampMs) {
        // P2-067: construction-time fail-closed — a null outcome would make
        // accepted()/duplicate()/violation() all false and requiresHalt()
        // silently return false (fail-open on the live-money halt path).
        public Verification {
            java.util.Objects.requireNonNull(outcome, "outcome");
            java.util.Objects.requireNonNull(instructionId, "instructionId");
            java.util.Objects.requireNonNull(contentHash, "contentHash");
        }

        public boolean accepted() {
            return outcome == ImmutabilityProtocol.Outcome.ACCEPTED;
        }

        public boolean duplicate() {
            return outcome == ImmutabilityProtocol.Outcome.DUPLICATE;
        }

        public boolean violation() {
            return outcome == ImmutabilityProtocol.Outcome.VIOLATION;
        }
    }

    /**
     * REQ-FLS-015 quarantine enforcement event: a separate, identifiable
     * record of a violation carrying source identity, content hash, and
     * timestamp — never a mutation of the original instruction row.
     */
    public record EnforcementViolation(String instructionId, String contentHash,
                                       long timestampMs) {}

    /**
     * Evaluate one emission against the stored hash for its
     * {@code instructionId}. {@code existingHash} is {@code null} on first
     * write (never stored). Pure: the caller decides what to persist from the
     * outcome — this method never writes.
     */
    public static Verification verify(String instructionId, String existingHash,
                                      String incomingHash, long timestampMs) {
        // P2-068: upstream evaluate() already requireNonNulls incomingHash —
        // the hole here is the EMPTY string: a KV twin returning "" for
        // missing must normalize to first-write (ACCEPTED), and blank
        // id/incoming must fail loud instead of a silent misclassify.
        if (instructionId == null || instructionId.isBlank()) {
            throw new IllegalArgumentException("instructionId must be non-blank");
        }
        if (incomingHash == null || incomingHash.isBlank()) {
            throw new IllegalArgumentException("incomingHash must be non-blank");
        }
        String normalizedExisting =
                (existingHash != null && existingHash.isEmpty()) ? null : existingHash;
        ImmutabilityProtocol.Outcome outcome =
                ImmutabilityProtocol.evaluate(normalizedExisting, incomingHash);
        return new Verification(outcome, instructionId, incomingHash, timestampMs);
    }

    /**
     * The quarantine + halt signal: only a VIOLATION requires it — an
     * accepted first write and an idempotent duplicate are both clean.
     */
    public static boolean requiresHalt(Verification verification) {
        // P2-187: fail-closed — unknown verification must halt live-money
        // flow, never skip via a swallowed NPE.
        if (verification == null) {
            return true;
        }
        return verification.violation();
    }

    /**
     * Build the REQ-FLS-015 enforcement event for a VIOLATION. Refuses to
     * fabricate an event for a clean outcome — the caller must not quarantine
     * a duplicate or a first write.
     */
    public static EnforcementViolation enforcementEvent(Verification verification) {
        // P2-188: fail LOUD (asymmetric with P2-187 by design) — event
        // construction on the quarantine path must name its cause, not NPE.
        if (verification == null) {
            throw new IllegalArgumentException(
                    "verification must not be null on quarantine path");
        }
        if (!verification.violation()) {
            throw new IllegalStateException(
                    "enforcement event only for VIOLATION, got " + verification.outcome()
                            + " for instruction " + verification.instructionId());
        }
        return new EnforcementViolation(
                verification.instructionId(), verification.contentHash(), verification.timestampMs());
    }
}
