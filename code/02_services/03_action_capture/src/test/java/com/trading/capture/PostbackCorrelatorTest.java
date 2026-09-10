package com.trading.capture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.trading.capture.PostbackCorrelator.AttemptIndex;
import com.trading.capture.PostbackCorrelator.CorrelationResult;
import com.trading.capture.PostbackCorrelator.CorrelationStatus;
import com.trading.capture.PostbackCorrelator.InMemoryCorrelationIndex;
import com.trading.capture.PostbackDecoder.DecodedPostback;
import org.junit.jupiter.api.Test;

/** P2 8.2: correlator fail-closed invariants (089/090/091/092). */
class PostbackCorrelatorTest {

    private static AttemptIndex attempt(String id, String instructionId, String clientRef, String broker) {
        return new AttemptIndex(id, instructionId, "H", clientRef, broker);
    }

    private static DecodedPostback postback(String broker, String clientRef) {
        return new DecodedPostback(broker, "EX-0", clientRef, "COMPLETE", "Fill",
                "1", "100", "100", "1", "T", "TK", "T", 0L);
    }

    @Test
    void registerRejectsNullAttemptId() {
        InMemoryCorrelationIndex index = new InMemoryCorrelationIndex();
        // P2-089: null attemptId must fail closed, not insert a null-key row.
        assertThatThrownBy(() -> index.register(attempt(null, "I-1", "R-1", "B-1")))
                .isInstanceOf(NullPointerException.class);
        assertThat(index.size()).isZero();
    }

    @Test
    void registerRejectsBijectiveViolationEvenAfterUpdate() {
        InMemoryCorrelationIndex index = new InMemoryCorrelationIndex();
        assertThat(index.register(attempt("A-1", "I-1", "R-1", "B-1"))).isTrue();
        // P2-090: same attemptId re-registered with a different brokerOrderId is an update.
        assertThat(index.register(attempt("A-1", "I-1", "R-1", "B-2"))).isTrue();
        assertThat(index.lookupByBroker("B-1")).isNull(); // stale key removed
        assertThat(index.lookupByBroker("B-2")).isNotNull();
        assertThat(index.size()).isEqualTo(1);
        // Clearing the clientRef removes the stale secondary key too.
        assertThat(index.register(attempt("A-1", "I-1", "", "B-2"))).isTrue();
        assertThat(index.lookupByClientRef("R-1")).isNull();
        // A *different* attempt binding an already-taken brokerOrderId still fails.
        assertThat(index.register(attempt("A-2", "I-2", "R-2", "B-2"))).isFalse();
    }

    @Test
    void brokerHitWithMismatchedClientRefIsAmbiguousEvenWhenRefUnknown() {
        InMemoryCorrelationIndex index = new InMemoryCorrelationIndex();
        index.register(attempt("A-1", "I-1", "R-1", "B-1"));
        // P2-091: clientRef differs and is NOT in the index — the old code fell
        // through to CORRELATED via broker (the hole). Must be AMBIGUOUS.
        CorrelationResult r = PostbackCorrelator.correlate(postback("B-1", "CORRUPTED-REF"), index);
        assertThat(r.status()).isEqualTo(CorrelationStatus.AMBIGUOUS_CORRELATION);
        assertThat(r.reason()).contains("mismatch");
        // Known-different ref stays AMBIGUOUS as before.
        index.register(attempt("A-2", "I-2", "R-2", "B-2"));
        CorrelationResult r2 = PostbackCorrelator.correlate(postback("B-2", "R-1"), index);
        assertThat(r2.status()).isEqualTo(CorrelationStatus.AMBIGUOUS_CORRELATION);
        // Matching ref still correlates via broker.
        assertThat(PostbackCorrelator.correlate(postback("B-1", "R-1"), index).status())
                .isEqualTo(CorrelationStatus.CORRELATED);
    }

    @Test
    void reconciliationRequiresIndexMembershipAndNonNullIds() {
        InMemoryCorrelationIndex index = new InMemoryCorrelationIndex();
        index.register(attempt("A-1", "I-1", "R-1", "B-1"));
        // clientRef below is deliberately absent from the index so steps 1-2 miss
        // and the reconciliation fallback (step 3) is actually exercised.
        // P2-092: null attemptId → NOT_FOUND, not CORRELATED with nulls.
        assertThat(PostbackCorrelator.correlate(postback("", "R-NOPE"), index, attempt(null, "I-1", null, null)).status())
                .isEqualTo(CorrelationStatus.NOT_FOUND);
        // null instructionId → NOT_FOUND.
        assertThat(PostbackCorrelator.correlate(postback("", "R-NOPE"), index, attempt("A-1", null, null, null)).status())
                .isEqualTo(CorrelationStatus.NOT_FOUND);
        // Not a member of the index → NOT_FOUND (arbitrary attempt must not correlate).
        assertThat(PostbackCorrelator.correlate(postback("", "R-NOPE"), index, attempt("A-99", "I-99", null, null)).status())
                .isEqualTo(CorrelationStatus.NOT_FOUND);
        // Valid member → CORRELATED via reconciliation.
        CorrelationResult ok = PostbackCorrelator.correlate(postback("", "R-NOPE"), index, attempt("A-1", "I-1", "R-1", "B-1"));
        assertThat(ok.status()).isEqualTo(CorrelationStatus.CORRELATED);
        assertThat(ok.attemptId()).isEqualTo("A-1");
        assertThat(ok.reason()).contains("reconciliation");
    }

    @Test
    void lookupByAttemptIdMirrorsOtherLookups() {
        InMemoryCorrelationIndex index = new InMemoryCorrelationIndex();
        index.register(attempt("A-1", "I-1", "R-1", "B-1"));
        assertThat(index.lookupByAttemptId("A-1").attemptId()).isEqualTo("A-1");
        assertThat(index.lookupByAttemptId("missing")).isNull();
        assertThat(index.lookupByAttemptId("")).isNull();
        assertThat(index.lookupByAttemptId(null)).isNull();
    }
}
