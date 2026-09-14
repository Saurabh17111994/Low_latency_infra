package com.trading.common.audit;

import com.trading.common.schema.ImmutabilityProtocol;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Approved audit-retention deletion governance
 * (docs/02_requirements/03-non-functional.md &sect;3.4.1 "Deletion";
 * docs/02_requirements/04-data.md: deletion of audit records before the approved retention period
 * is prohibited unless an approved retention-policy change, a legal-hold release,
 * and single-operator (Saurabh) authorization are recorded as immutable deletion-evidence events).
 * <p>DEC-044 (2026-08-21): single-operator Saurabh (was two-person).
 */
public final class AuditDeletionControl {

    private AuditDeletionControl() {}

    public enum Decision {
        APPROVED,
        REJECTED_NO_POLICY_CHANGE,
        REJECTED_LEGAL_HOLD,
        REJECTED_REQUIRES_SINGLE_AUTHORIZER,
        REJECTED_UNAUTHORIZED_OPERATOR,
        REJECTED_MISSING_EVIDENCE
    }

    /** A deletion attempt against the policy-controlled audit store. */
    public record DeletionRequest(
            String requestId,
            String scope,
            boolean withinRetentionWindow,
            String retentionPolicyChangeId,
            String legalHoldReleaseId,
            List<String> authorizers) {
        public DeletionRequest {
            // Null-tolerant copy (P6-260): a malformed authorizer list must still
            // reach evaluate() and leave a governed REJECTED_* event rather than a
            // NullPointerException from List.copyOf.
            authorizers = authorizers == null
                    ? List.of()
                    : Collections.unmodifiableList(new ArrayList<>(authorizers));
        }
    }

    /** What the caller knows about approved policy changes, legal holds, and operators. */
    public record DeletionContext(
            Set<String> approvedPolicyChangeIds,
            Set<String> legalHoldReleaseIds,
            Set<String> authorizedOperators) {
        public DeletionContext {
            // Null-tolerant copies (P6-259): Set.copyOf throws on a null element,
            // which would turn malformed (e.g. deserialized) input into an NPE
            // before evaluate() could reject it with evidence.
            approvedPolicyChangeIds = nullTolerantSet(approvedPolicyChangeIds);
            legalHoldReleaseIds = nullTolerantSet(legalHoldReleaseIds);
            authorizedOperators = nullTolerantSet(authorizedOperators);
        }
    }

    /**
     * Immutable evidence of one deletion attempt. Approved and rejected attempts
     * both produce an event; each event feeds the immutable audit hash chain
     * via {@link #asAuditEvent()}.
     *
     * <p>{@link #canonical()} is the hashed preimage ({@code deletion-evidence-v2}
     * since 2026-09-14: field values are escaped so no value can forge a canonical
     * line). The preimage includes {@code timestampMs}, so an idempotent replay
     * must reuse the same timestamp — a re-stamped attempt is deliberately a
     * distinct, separately recorded event (P6-650).
     */
    public record DeletionEvidenceEvent(
            String eventId,
            String requestId,
            String scope,
            boolean approved,
            boolean withinRetentionWindow,
            String retentionPolicyChangeId,
            String legalHoldReleaseId,
            String authorizerOne,
            String authorizerTwo,
            long timestampMs,
            String contentHash) {

        public DeletionEvidenceEvent {
            if (eventId == null || eventId.isBlank()) {
                throw new IllegalArgumentException("eventId must be non-blank");
            }
            if (contentHash == null || contentHash.isBlank()) {
                throw new IllegalArgumentException("contentHash must be non-blank");
            }
        }

        public String canonical() {
            return canonicalForm(requestId, scope, approved, withinRetentionWindow,
                    retentionPolicyChangeId, legalHoldReleaseId,
                    authorizerOne, authorizerTwo, timestampMs);
        }

        /** The evidence identity feeds the immutable audit hash chain. */
        public AuditHashChain.AuditEvent asAuditEvent() {
            return new AuditHashChain.AuditEvent(eventId, contentHash);
        }
    }

    public record DeletionDecision(Decision decision, DeletionEvidenceEvent evidenceEvent) {}

    /**
     * Evaluates a deletion request against the known governance state. Approval
     * requires a single authorized operator (Saurabh, DEC-044). Deletion of records
     * still inside the approved retention window additionally requires an
     * approved retention-policy change and a legal-hold release. Every attempt —
     * approved or rejected — produces an immutable deletion-evidence event.
     */
    public static DeletionDecision evaluate(DeletionRequest request,
                                            DeletionContext context,
                                            long timestampMs) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(context, "context");
        if (request.requestId() == null || request.requestId().isBlank()
                || request.scope() == null || request.scope().isBlank()) {
            return new DeletionDecision(Decision.REJECTED_MISSING_EVIDENCE,
                    buildEvent(request, false, null, null, timestampMs));
        }
        Set<String> distinctAuthorizers = new LinkedHashSet<>(request.authorizers());
        // Who attempted the deletion is recorded on rejection too: an intruder, or
        // an over-authorized pair, is exactly what the forensic trail needs
        // (P6-647, P6-648).
        List<String> attempted = new ArrayList<>(distinctAuthorizers);
        String attemptedOne = attempted.isEmpty() ? null : attempted.get(0);
        String attemptedTwo = attempted.size() > 1 ? attempted.get(1) : null;
        if (distinctAuthorizers.size() != 1) {
            return new DeletionDecision(Decision.REJECTED_REQUIRES_SINGLE_AUTHORIZER,
                    buildEvent(request, false, attemptedOne, attemptedTwo, timestampMs));
        }
        String single = attempted.get(0);
        if (single == null || !context.authorizedOperators().contains(single)) {
            // A null element (P6-259/P6-260) is an unauthorized attempt, not a crash:
            // contains(null) would itself throw on the immutable operator set.
            return new DeletionDecision(Decision.REJECTED_UNAUTHORIZED_OPERATOR,
                    buildEvent(request, false, single, null, timestampMs));
        }
        if (request.withinRetentionWindow()) {
            if (request.retentionPolicyChangeId() == null
                    || !context.approvedPolicyChangeIds().contains(request.retentionPolicyChangeId())) {
                return new DeletionDecision(Decision.REJECTED_NO_POLICY_CHANGE,
                        buildEvent(request, false, single, null, timestampMs));
            }
            if (request.legalHoldReleaseId() == null
                    || !context.legalHoldReleaseIds().contains(request.legalHoldReleaseId())) {
                return new DeletionDecision(Decision.REJECTED_LEGAL_HOLD,
                        buildEvent(request, false, single, null, timestampMs));
            }
        }
        // DEC-044 single-operator governance: authorizerTwo is legacy-reserved
        // schema, always null for new evidence, kept so the deletion-evidence-v2
        // canonical bytes stay stable (P6-849).
        DeletionEvidenceEvent evidence =
                buildEvent(request, true, single, null, timestampMs);
        return new DeletionDecision(Decision.APPROVED, evidence);
    }

    /**
     * Same eventId + same contentHash &rarr; DUPLICATE (idempotent replay of the
     * same evidence); same eventId + different contentHash &rarr; VIOLATION
     * (mutation of immutable evidence). Different eventIds &rarr; ACCEPTED (a
     * distinct deletion event).
     *
     * <p>The incoming event must first prove its own integrity: its stored
     * contentHash is recomputed from {@link DeletionEvidenceEvent#canonical()}, so
     * a record whose fields were altered while its stored hashes were preserved is
     * a VIOLATION instead of a DUPLICATE (P6-261).
     */
    public static ImmutabilityProtocol.Outcome classify(DeletionEvidenceEvent existing,
                                                        DeletionEvidenceEvent incoming) {
        Objects.requireNonNull(existing, "existing");
        Objects.requireNonNull(incoming, "incoming");
        if (!ImmutabilityProtocol.canonicalHash(incoming.canonical())
                .equals(incoming.contentHash())) {
            return ImmutabilityProtocol.Outcome.VIOLATION;
        }
        if (existing.eventId().equals(incoming.eventId())) {
            return existing.contentHash().equals(incoming.contentHash())
                    ? ImmutabilityProtocol.Outcome.DUPLICATE
                    : ImmutabilityProtocol.Outcome.VIOLATION;
        }
        return ImmutabilityProtocol.Outcome.ACCEPTED;
    }

    private static DeletionEvidenceEvent buildEvent(DeletionRequest request, boolean approved,
                                                    String authorizerOne, String authorizerTwo,
                                                    long timestampMs) {
        String canonical = canonicalForm(request.requestId(), request.scope(), approved,
                request.withinRetentionWindow(), request.retentionPolicyChangeId(),
                request.legalHoldReleaseId(), authorizerOne, authorizerTwo, timestampMs);
        String contentHash = ImmutabilityProtocol.canonicalHash(canonical);
        // Identity is the whole canonical record, timestamp included: replaying the
        // same evidence (same fields and the same timestampMs) is a DUPLICATE,
        // while an attempt re-stamped with a fresh clock is a new, separately
        // recorded attempt. Retry callers must reuse their timestampMs for
        // idempotent replay; a timestamp-free id would collapse distinct attempts
        // and make a benign retry compare as VIOLATION (P6-650).
        String eventId = ImmutabilityProtocol.canonicalHash("deletion-evidence-v2|" + canonical);
        return new DeletionEvidenceEvent(eventId, request.requestId(), request.scope(), approved,
                request.withinRetentionWindow(), request.retentionPolicyChangeId(),
                request.legalHoldReleaseId(), authorizerOne, authorizerTwo, timestampMs, contentHash);
    }

    private static String canonicalForm(String requestId, String scope, boolean approved,
                                        boolean withinWindow, String policyChange,
                                        String legalHoldRelease, String authorizerOne,
                                        String authorizerTwo, long timestampMs) {
        // Values are escaped (P6-651): an unescaped newline in scope could forge
        // extra "key=value" lines, so two logically different requests shared one
        // canonical string (and therefore one content hash, one event id).
        return "deletion-evidence-v2\n"
                + "requestId=" + escape(requestId) + "\n"
                + "scope=" + escape(scope) + "\n"
                + "approved=" + approved + "\n"
                + "withinRetentionWindow=" + withinWindow + "\n"
                + "policyChange=" + escape(policyChange) + "\n"
                + "legalHoldRelease=" + escape(legalHoldRelease) + "\n"
                + "authorizerOne=" + escape(authorizerOne) + "\n"
                + "authorizerTwo=" + escape(authorizerTwo) + "\n"
                + "timestampMs=" + timestampMs;
    }

    /** Escapes the escape character and line breaks; null renders as empty. */
    private static String escape(String value) {
        return value == null
                ? ""
                : value.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r");
    }

    /** Null-tolerant immutable copy — null elements are kept for evaluate() to judge. */
    private static Set<String> nullTolerantSet(Set<String> values) {
        return values == null
                ? Set.of()
                : Collections.unmodifiableSet(new LinkedHashSet<>(values));
    }
}
