package com.trading.common.schema.projection;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Compact-ctor invariants of the capture-path envelope (P4-029, FOLLOW-3):
 * fill-present-requires-price, non-fill-must-not-carry-price, and the
 * heartbeat-vs-partial semantic.
 */
class NormalizedPostbackTest {

    private static final long NOW = 1_000L;

    @Test
    void fillWithoutPriceIsRejected() {
        NormalizedPostback valid = TestPostbacks.fill(1L, "b-1", "BUY", 10, 0, 10, 1000L, NOW);
        assertThatThrownBy(() -> withFillPrice(valid, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive fill price");
    }

    @Test
    void fillWithNegativePriceIsRejected() {
        NormalizedPostback valid = TestPostbacks.fill(1L, "b-1", "BUY", 10, 0, 10, 1000L, NOW);
        assertThatThrownBy(() -> withFillPrice(valid, -5L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive fill price");
    }

    @Test
    void nonFillCarryingPriceIsRejected() {
        NormalizedPostback valid = TestPostbacks.status(1L, "b-1", "PARTIAL", 10, 5, NOW);
        assertThatThrownBy(() -> withFillPrice(valid, 1000L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-fill row must not carry a fill price");
    }

    @Test
    void heartbeatStatusPostbackIsValidAndNotAFill() {
        NormalizedPostback heartbeat = TestPostbacks.status(1L, "b-1", "PARTIAL", 10, 5, NOW);
        assertThat(heartbeat.isFill()).isFalse();
        assertThat(heartbeat.fillQty()).isZero();
        assertThat(heartbeat.fillPricePaise()).isZero();
    }

    @Test
    void partialFillCarriesPriceAndSourceIdentity() {
        NormalizedPostback fill = TestPostbacks.fill(1L, "b-1", "BUY", 10, 5, 10, 1000L, NOW);
        assertThat(fill.isFill()).isTrue();
        assertThat(fill.fillPricePaise()).isPositive();
        // The fill identity for duplicate/conflict checks is always present.
        assertThat(fill.sourceEventId()).isNotBlank();
        assertThat(fill.postbackEventId()).isNotBlank();
    }

    private static NormalizedPostback withFillPrice(NormalizedPostback p, long fillPricePaise) {
        return new NormalizedPostback(
                p.postbackEventId(), p.sourceEventId(), p.sourceSequence(), p.fingerprint(),
                p.fingerprintVersion(), p.brokerOrderId(), p.echoedClientOrderRef(),
                p.accountScopeId(), p.instrumentToken(), p.exchange(), p.symbol(), p.side(),
                p.orderStatus(), p.cumulativeQty(), p.pendingQty(), p.fillQty(),
                fillPricePaise, p.eventTimeMs(), p.receiveTimeMs(), p.mappingVersion(),
                p.originalPayloadHash(), p.tradeContextId());
    }
}
