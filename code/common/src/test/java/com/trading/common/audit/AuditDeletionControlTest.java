package com.trading.common.audit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.trading.common.schema.ImmutabilityProtocol;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

/** Unit tests for the single-operator (Saurabh) audit deletion governance path (DEC-044). */
class AuditDeletionControlTest {

    private static final long TS = 1_700_000_000_000L;

    private static AuditDeletionControl.DeletionContext context() {
        return new AuditDeletionControl.DeletionContext(
                Set.of("RPC-42"), Set.of("LHR-7"), Set.of("saurabh"));
    }

    private static AuditDeletionControl.DeletionRequest request(boolean withinWindow,
                                                                String policyChange,
                                                                String legalHold,
                                                                List<String> authorizers) {
        return new AuditDeletionControl.DeletionRequest("del-1", "Execution_Audit:2024-06-01",
                withinWindow, policyChange, legalHold, authorizers);
    }

    private static AuditDeletionControl.DeletionRequest fullRequest() {
        return request(true, "RPC-42", "LHR-7", List.of("saurabh"));
    }

    @Test
    void approvedWithFullEvidence() {
        AuditDeletionControl.DeletionDecision d =
                AuditDeletionControl.evaluate(fullRequest(), context(), TS);
        assertThat(d.decision()).isEqualTo(AuditDeletionControl.Decision.APPROVED);
        assertThat(d.evidenceEvent()).isNotNull();
        assertThat(d.evidenceEvent().approved()).isTrue();
        assertThat(d.evidenceEvent().authorizerOne()).isNotBlank();
        assertThat(d.evidenceEvent().authorizerOne()).isEqualTo("saurabh");
    }

    @Test
    void rejectedWithoutApprovedPolicyChange() {
        AuditDeletionControl.DeletionDecision d = AuditDeletionControl.evaluate(
                request(true, "RPC-999", "LHR-7", List.of("saurabh")), context(), TS);
        assertThat(d.decision()).isEqualTo(AuditDeletionControl.Decision.REJECTED_NO_POLICY_CHANGE);
    }

    @Test
    void rejectedWhileLegalHoldActive() {
        AuditDeletionControl.DeletionDecision d = AuditDeletionControl.evaluate(
                request(true, "RPC-42", "LHR-0", List.of("saurabh")), context(), TS);
        assertThat(d.decision()).isEqualTo(AuditDeletionControl.Decision.REJECTED_LEGAL_HOLD);
    }

    @Test
    void rejectedWithNoAuthorizer() {
        AuditDeletionControl.DeletionDecision d = AuditDeletionControl.evaluate(
                request(true, "RPC-42", "LHR-7", List.of()), context(), TS);
        assertThat(d.decision())
                .isEqualTo(AuditDeletionControl.Decision.REJECTED_REQUIRES_SINGLE_AUTHORIZER);
    }

    @Test
    void rejectedWithTwoAuthorizersWhenOneRequired() {
        AuditDeletionControl.DeletionDecision d = AuditDeletionControl.evaluate(
                request(true, "RPC-42", "LHR-7", List.of("saurabh", "ops-2")), context(), TS);
        assertThat(d.decision())
                .isEqualTo(AuditDeletionControl.Decision.REJECTED_REQUIRES_SINGLE_AUTHORIZER);
    }

    @Test
    void rejectedWhenOperatorNotAuthorized() {
        AuditDeletionControl.DeletionDecision d = AuditDeletionControl.evaluate(
                request(true, "RPC-42", "LHR-7", List.of("intruder")), context(), TS);
        assertThat(d.decision())
                .isEqualTo(AuditDeletionControl.Decision.REJECTED_UNAUTHORIZED_OPERATOR);
    }

    @Test
    void rejectedWhenRequestMissingEvidence() {
        AuditDeletionControl.DeletionDecision d = AuditDeletionControl.evaluate(
                new AuditDeletionControl.DeletionRequest("", "Execution_Audit:2024-06-01",
                        true, "RPC-42", "LHR-7", List.of("saurabh")),
                context(), TS);
        assertThat(d.decision())
                .isEqualTo(AuditDeletionControl.Decision.REJECTED_MISSING_EVIDENCE);
    }

    @Test
    void approvedOutsideRetentionWindowWithoutPolicyChange() {
        // Records past the approved retention window: normal expiry, no policy-change or
        // legal-hold evidence required — single-operator authorization still applies.
        AuditDeletionControl.DeletionDecision d = AuditDeletionControl.evaluate(
                request(false, null, null, List.of("saurabh")), context(), TS);
        assertThat(d.decision()).isEqualTo(AuditDeletionControl.Decision.APPROVED);
        assertThat(d.evidenceEvent().withinRetentionWindow()).isFalse();
    }

    @Test
    void evidenceIsDeterministicAndImmutable() {
        AuditDeletionControl.DeletionEvidenceEvent a =
                AuditDeletionControl.evaluate(fullRequest(), context(), TS).evidenceEvent();
        AuditDeletionControl.DeletionEvidenceEvent b =
                AuditDeletionControl.evaluate(fullRequest(), context(), TS).evidenceEvent();
        assertThat(a.eventId()).isEqualTo(b.eventId());
        assertThat(a.contentHash()).isEqualTo(b.contentHash());
        assertThat(a.eventId()).hasSize(AuditHashChain.HASH_HEX_LENGTH);
        assertThat(a.contentHash()).hasSize(AuditHashChain.HASH_HEX_LENGTH);
    }

    @Test
    void replayOfApprovedEventIsDuplicate() {
        AuditDeletionControl.DeletionEvidenceEvent ev =
                AuditDeletionControl.evaluate(fullRequest(), context(), TS).evidenceEvent();
        assertThat(AuditDeletionControl.classify(ev, ev))
                .isEqualTo(ImmutabilityProtocol.Outcome.DUPLICATE);
    }

    @Test
    void mutatedEvidenceIsViolation() {
        AuditDeletionControl.DeletionEvidenceEvent ev =
                AuditDeletionControl.evaluate(fullRequest(), context(), TS).evidenceEvent();
        // Same event identity, different content hash — mutation of immutable evidence.
        AuditDeletionControl.DeletionEvidenceEvent mutated =
                new AuditDeletionControl.DeletionEvidenceEvent(
                        ev.eventId(), ev.requestId(), ev.scope(), ev.approved(),
                        ev.withinRetentionWindow(), ev.retentionPolicyChangeId(),
                        ev.legalHoldReleaseId(), ev.authorizerOne(), ev.authorizerTwo(),
                        ev.timestampMs(), "0".repeat(AuditHashChain.HASH_HEX_LENGTH));
        assertThat(AuditDeletionControl.classify(ev, mutated))
                .isEqualTo(ImmutabilityProtocol.Outcome.VIOLATION);
    }

    @Test
    void distinctRequestIsAcceptedNotDuplicate() {
        AuditDeletionControl.DeletionEvidenceEvent a =
                AuditDeletionControl.evaluate(fullRequest(), context(), TS).evidenceEvent();
        AuditDeletionControl.DeletionEvidenceEvent b =
                AuditDeletionControl.evaluate(
                        new AuditDeletionControl.DeletionRequest("del-2", "Execution_Audit:2024-06-01",
                                true, "RPC-42", "LHR-7", List.of("saurabh")),
                        context(), TS).evidenceEvent();
        assertThat(AuditDeletionControl.classify(a, b))
                .isEqualTo(ImmutabilityProtocol.Outcome.ACCEPTED);
    }

    @Test
    void evidenceFeedsAuditHashChain() {
        AuditDeletionControl.DeletionEvidenceEvent ev =
                AuditDeletionControl.evaluate(fullRequest(), context(), TS).evidenceEvent();
        AuditHashChain.Manifest manifest =
                new AuditHashChain.ManifestBuilder("2025-01-01", "Execution_Audit", "1")
                        .addEvent(ev.asAuditEvent().eventId(), ev.asAuditEvent().contentHash())
                        .build();
        assertThat(AuditHashChain.verifyManifestAgainstSource(manifest, manifest.events()))
                .isEqualTo(AuditHashChain.Verification.VALID);
    }

    // --- P6-259 / P6-260: malformed input becomes a governed decision, not an NPE ---

    @Test
    void nullAuthorizerElementIsRejectedWithEvidenceNotNpe() {
        AuditDeletionControl.DeletionRequest request = new AuditDeletionControl.DeletionRequest(
                "del-1", "Execution_Audit:2024-06-01", true, "RPC-42", "LHR-7",
                java.util.Arrays.asList("saurabh", null));
        AuditDeletionControl.DeletionDecision d =
                AuditDeletionControl.evaluate(request, context(), TS);
        assertThat(d.decision())
                .isEqualTo(AuditDeletionControl.Decision.REJECTED_REQUIRES_SINGLE_AUTHORIZER);
        assertThat(d.evidenceEvent().authorizerOne()).isEqualTo("saurabh");
        assertThat(d.evidenceEvent().contentHash()).hasSize(AuditHashChain.HASH_HEX_LENGTH);
    }

    @Test
    void nullOnlyAuthorizerIsUnauthorizedNotNpe() {
        AuditDeletionControl.DeletionDecision d = AuditDeletionControl.evaluate(
                request(false, null, null, java.util.Arrays.asList((String) null)), context(), TS);
        assertThat(d.decision())
                .isEqualTo(AuditDeletionControl.Decision.REJECTED_UNAUTHORIZED_OPERATOR);
        assertThat(d.evidenceEvent()).isNotNull();
    }

    @Test
    void nullElementsInContextAreToleratedNotNpe() {
        java.util.Set<String> policy = new java.util.LinkedHashSet<>();
        policy.add(null);
        policy.add("RPC-42");
        java.util.Set<String> hold = new java.util.LinkedHashSet<>();
        hold.add(null);
        hold.add("LHR-7");
        java.util.Set<String> operators = new java.util.LinkedHashSet<>();
        operators.add(null);
        operators.add("saurabh");
        AuditDeletionControl.DeletionContext context =
                new AuditDeletionControl.DeletionContext(policy, hold, operators);
        assertThat(AuditDeletionControl.evaluate(fullRequest(), context, TS).decision())
                .isEqualTo(AuditDeletionControl.Decision.APPROVED);
    }

    // --- P6-646 / P6-647 / P6-648: rejected evidence still names the attempt ---

    @Test
    void unauthorizedOperatorIsRecordedInEvidence() {
        AuditDeletionControl.DeletionDecision d = AuditDeletionControl.evaluate(
                request(false, null, null, List.of("intruder")), context(), TS);
        assertThat(d.decision())
                .isEqualTo(AuditDeletionControl.Decision.REJECTED_UNAUTHORIZED_OPERATOR);
        assertThat(d.evidenceEvent().authorizerOne()).isEqualTo("intruder");
    }

    @Test
    void rejectedPolicyAndHoldEvidenceRecordTheAttemptingOperator() {
        AuditDeletionControl.DeletionDecision noPolicy = AuditDeletionControl.evaluate(
                request(true, "RPC-999", "LHR-7", List.of("saurabh")), context(), TS);
        assertThat(noPolicy.decision())
                .isEqualTo(AuditDeletionControl.Decision.REJECTED_NO_POLICY_CHANGE);
        assertThat(noPolicy.evidenceEvent().authorizerOne()).isEqualTo("saurabh");

        AuditDeletionControl.DeletionDecision hold = AuditDeletionControl.evaluate(
                request(true, "RPC-42", "LHR-0", List.of("saurabh")), context(), TS);
        assertThat(hold.decision()).isEqualTo(AuditDeletionControl.Decision.REJECTED_LEGAL_HOLD);
        assertThat(hold.evidenceEvent().authorizerOne()).isEqualTo("saurabh");
    }

    @Test
    void twoAuthorizerRejectionRecordsBothAttempts() {
        AuditDeletionControl.DeletionDecision d = AuditDeletionControl.evaluate(
                request(true, "RPC-42", "LHR-7", List.of("saurabh", "ops-2")), context(), TS);
        assertThat(d.decision())
                .isEqualTo(AuditDeletionControl.Decision.REJECTED_REQUIRES_SINGLE_AUTHORIZER);
        assertThat(d.evidenceEvent().authorizerOne()).isEqualTo("saurabh");
        assertThat(d.evidenceEvent().authorizerTwo()).isEqualTo("ops-2");
    }

    // --- P6-261: classify() recomputes the canonical hash before trusting it ---

    @Test
    void tamperedFieldWithPreservedHashesIsViolation() {
        AuditDeletionControl.DeletionEvidenceEvent ev =
                AuditDeletionControl.evaluate(fullRequest(), context(), TS).evidenceEvent();
        AuditDeletionControl.DeletionEvidenceEvent tampered =
                new AuditDeletionControl.DeletionEvidenceEvent(
                        ev.eventId(), ev.requestId(), "Execution_Audit:2024-06-02",
                        ev.approved(), ev.withinRetentionWindow(), ev.retentionPolicyChangeId(),
                        ev.legalHoldReleaseId(), ev.authorizerOne(), ev.authorizerTwo(),
                        ev.timestampMs(), ev.contentHash()); // stored id + hash preserved
        assertThat(AuditDeletionControl.classify(ev, tampered))
                .isEqualTo(ImmutabilityProtocol.Outcome.VIOLATION);
    }

    @Test
    void classifyRejectsNullArguments() {
        AuditDeletionControl.DeletionEvidenceEvent ev =
                AuditDeletionControl.evaluate(fullRequest(), context(), TS).evidenceEvent();
        assertThatNullPointerException()
                .isThrownBy(() -> AuditDeletionControl.classify(null, ev));
        assertThatNullPointerException()
                .isThrownBy(() -> AuditDeletionControl.classify(ev, null));
    }

    // --- P6-650: the timestamp is part of the evidence identity (documented contract) ---

    @Test
    void freshTimestampIsANewAttemptNotADuplicate() {
        AuditDeletionControl.DeletionEvidenceEvent first =
                AuditDeletionControl.evaluate(fullRequest(), context(), TS).evidenceEvent();
        AuditDeletionControl.DeletionEvidenceEvent retry =
                AuditDeletionControl.evaluate(fullRequest(), context(), TS + 5_000).evidenceEvent();
        assertThat(retry.eventId()).isNotEqualTo(first.eventId());
        assertThat(retry.timestampMs()).isEqualTo(TS + 5_000);
        assertThat(AuditDeletionControl.classify(first, retry))
                .isEqualTo(ImmutabilityProtocol.Outcome.ACCEPTED);
        // Same fields AND same timestamp is the idempotent replay case.
        assertThat(AuditDeletionControl.classify(first,
                AuditDeletionControl.evaluate(fullRequest(), context(), TS).evidenceEvent()))
                .isEqualTo(ImmutabilityProtocol.Outcome.DUPLICATE);
    }

    // --- P6-651: escaping keeps distinct requests distinct in canonical bytes ---

    @Test
    void injectedNewlineNoLongerCollidesWithAnotherRequest() {
        AuditDeletionControl.DeletionEvidenceEvent a = AuditDeletionControl.evaluate(
                new AuditDeletionControl.DeletionRequest("r1", "a\nscope=b", true,
                        "RPC-42", "LHR-7", List.of("saurabh")), context(), TS).evidenceEvent();
        AuditDeletionControl.DeletionEvidenceEvent b = AuditDeletionControl.evaluate(
                new AuditDeletionControl.DeletionRequest("r1\nscope=a", "b", true,
                        "RPC-42", "LHR-7", List.of("saurabh")), context(), TS).evidenceEvent();
        assertThat(a.contentHash()).isNotEqualTo(b.contentHash());
        assertThat(a.eventId()).isNotEqualTo(b.eventId());
    }

    // --- P6-849 / P6-650: versioned canonical form, legacy authorizerTwo ---

    @Test
    void canonicalIsVersionedV2AndAuthorizerTwoIsLegacyNull() {
        AuditDeletionControl.DeletionEvidenceEvent ev =
                AuditDeletionControl.evaluate(fullRequest(), context(), TS).evidenceEvent();
        assertThat(ev.canonical()).startsWith("deletion-evidence-v2\n");
        assertThat(ev.canonical()).contains("authorizerTwo=\n");
        assertThat(ev.authorizerTwo()).isNull();
    }

    @Test
    void rejectedAttemptStillEmitsEvidenceEvent() {
        AuditDeletionControl.DeletionDecision d = AuditDeletionControl.evaluate(
                request(true, "RPC-999", "LHR-7", List.of("saurabh")), context(), TS);
        assertThat(d.decision()).isEqualTo(AuditDeletionControl.Decision.REJECTED_NO_POLICY_CHANGE);
        assertThat(d.evidenceEvent()).isNotNull();
        assertThat(d.evidenceEvent().approved()).isFalse();
        assertThat(d.evidenceEvent().eventId()).isNotBlank();
    }
}
