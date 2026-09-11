package com.trading.common.schema.projection;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PostbackFingerprintTest {

    @Test
    void computeIsDeterministicAndVersioned() {
        String f1 = PostbackFingerprint.compute("1", "a", "b");
        String f2 = PostbackFingerprint.compute("1", "a", "b");
        String f3 = PostbackFingerprint.compute("2", "a", "b");
        assertThat(f1).isEqualTo(f2);
        assertThat(f1).startsWith("fp-1-");
        assertThat(f3).startsWith("fp-2-");
        assertThat(f1).isNotEqualTo(f3);
    }

    @Test
    void validFixtureFingerprintMatches() {
        NormalizedPostback p = TestPostbacks.fill(1L, "b-1", "BUY", 10, 0, 10, 1000L, 1000L);
        assertThat(PostbackFingerprint.matches(p)).isTrue();
    }

    @Test
    void versionSkewNoLongerMatches() {
        // P3-407: fingerprint tracks the fingerprint algorithm version — a
        // mapping bump without a scheme change must not validate.
        NormalizedPostback p = TestPostbacks.fill(1L, "b-1", "BUY", 10, 0, 10, 1000L, 1000L);
        NormalizedPostback skewed = new NormalizedPostback(
                p.postbackEventId(), p.sourceEventId(), p.sourceSequence(), p.fingerprint(),
                p.fingerprintVersion(), p.brokerOrderId(), p.echoedClientOrderRef(),
                p.accountScopeId(), p.instrumentToken(), p.exchange(), p.symbol(), p.side(),
                p.orderStatus(), p.cumulativeQty(), p.pendingQty(), p.fillQty(),
                p.fillPricePaise(), p.eventTimeMs(), p.receiveTimeMs(), "2",
                p.originalPayloadHash(), p.tradeContextId());
        assertThat(PostbackFingerprint.matches(skewed)).isFalse();
    }

    @Test
    void tamperedContentNoLongerMatches() {
        NormalizedPostback p = TestPostbacks.fill(1L, "b-1", "BUY", 10, 0, 10, 1000L, 1000L);
        NormalizedPostback tampered = new NormalizedPostback(
                p.postbackEventId(), p.sourceEventId(), p.sourceSequence(), p.fingerprint(),
                p.fingerprintVersion(), p.brokerOrderId(), p.echoedClientOrderRef(),
                p.accountScopeId(), p.instrumentToken(), p.exchange(), p.symbol(), p.side(),
                p.orderStatus(), p.cumulativeQty() + 1, p.pendingQty(), p.fillQty(),
                p.fillPricePaise(), p.eventTimeMs(), p.receiveTimeMs(), p.mappingVersion(),
                p.originalPayloadHash(), p.tradeContextId());
        assertThat(PostbackFingerprint.matches(tampered)).isFalse();
    }
}
