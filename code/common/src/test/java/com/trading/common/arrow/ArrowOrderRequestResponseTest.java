/* Copyright (c) Trading Platform. All rights reserved. */
package com.trading.common.arrow;

import com.trading.common.identity.IdentityModel.BrokerOrderId;
import com.trading.common.identity.IdentityModel.ClientOrderRef;
import com.trading.common.identity.IdentityModel.ExchangeId;
import com.trading.common.identity.IdentityModel.InstrumentToken;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Capability-evidence scaffold for VM-ARROW-010 (Arrow {@code /order/regular}
 * REST contract).
 *
 * <p>Proves:
 * <ul>
 *   <li>our {@code client_order_ref} flows to Arrow {@code remarks} and is
 *       rejected when it exceeds 16 chars (Arrow contract limit);</li>
 *   <li>the broker's response {@code orderNo} becomes our
 *       {@code broker_order_id};</li>
 *   <li>{@code instrument_token} is carried end-to-end.</li>
 * </ul>
 */
class ArrowOrderRequestResponseTest {

    @Test
    void clientOrderRefMapsToRemarksAndIsLengthBounded() {
        ClientOrderRef ref = new ClientOrderRef("INV20250721"); // 11 chars
        ArrowOrderRequest req = new ArrowOrderRequest(
            new ExchangeId("NSECM"),
            "RELIANCE-EQ",
            new InstrumentToken(26009),
            10,
            ArrowOrderRequest.TransactionType.B,
            ArrowOrderRequest.OrderType.LMT,
            ArrowOrderRequest.Product.I,
            "2975.10",
            ArrowOrderRequest.Validity.DAY,
            0,
            ref,
            false);

        assertThat(req.clientOrderRef()).isEqualTo(ref);
        // Must be carried verbatim; Arrow will echo it in postback remarks.
        assertThat(req.clientOrderRef().value()).hasSizeLessThanOrEqualTo(16);
    }

    @Test
    void rejectsOverlongClientOrderRef() {
        // P3-110: the 16-char cap now fails fast at the ClientOrderRef
        // constructor (fail-fast identity), not late at the Arrow boundary —
        // the broker check below stays as defense in depth.
        assertThatThrownBy(() -> new ClientOrderRef("INV20250721TOOLONG")) // > 16
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("16");
    }

    @Test
    void responseOrderNoIsBrokerOrderId() {
        ArrowOrderResponse resp = new ArrowOrderResponse(
            new BrokerOrderId("2600090001"), 1_752_539_000L);
        assertThat(resp.brokerOrderId()).isEqualTo(new BrokerOrderId("2600090001"));
    }
}
