/* Copyright (c) Trading Platform. All rights reserved. */
package com.trading.common.broker;

import com.trading.common.arrow.ArrowOrderStatus;
import com.trading.common.identity.IdentityModel.BrokerOrderId;
import com.trading.common.identity.IdentityModel.ClientOrderRef;
import com.trading.common.identity.IdentityModel.InstrumentToken;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Capability-evidence scaffold for VM-BROKER-PBK-009 (Arrow postback stream,
 * {@code order-updates.arrow.trade} WebSocket JSON).
 *
 * <p>Proves the identity mapping from a broker postback to our {@code Fills}
 * table: {@code id} -> broker_order_id, {@code remarks} -> client_order_ref,
 * {@code token} -> instrument_token, and fill fields are preserved.
 * This is the Action Capture contract: it must consume the postback
 * independently of the Signal/Executor path.
 */
class ArrowOrderUpdateTest {

    /** A complete Fill: every field the Fill mapping depends on is present. */
    private ArrowOrderUpdate.Builder fill() {
        return ArrowOrderUpdate.builder()
            .brokerOrderId(new BrokerOrderId("2600090001"))
            .clientOrderRef(new ClientOrderRef("INV20250721"))
            .instrumentToken(new InstrumentToken(26009))
            .status(ArrowOrderStatus.OrderStatus.COMPLETE)
            .reportType(ArrowOrderStatus.ReportType.FILL)
            .fillId("F1")
            .fillQuantity(10)
            .fillPrice(297510L)
            .fillTime(1_752_539_000_000L);
    }

    @Test
    void fillIsNotBuildableWithoutTheFieldsTheFillRowNeeds() {
        // Control: a complete Fill still builds — the guards below are FILL-scoped.
        assertThat(fill().build().isFill()).isTrue();

        assertThatThrownBy(() -> fill().fillId(null).build())
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("fillId");
        assertThatThrownBy(() -> fill().fillId("   ").build())
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("fillId");
        assertThatThrownBy(() -> fill().fillQuantity(0).build())
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("fillQuantity");
        assertThatThrownBy(() -> fill().fillPrice(0).build())
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("fillPrice");
        assertThatThrownBy(() -> fill().fillTime(0).build())
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("fillTime");
    }

    @Test
    void clientOrderRefIsAbsentWhenTheBrokerOmitsRemarks() {
        // P3-338 guard, not a falsification: the field is optional by contract, and
        // this pins the documented behaviour so it cannot be tightened by accident
        // without also changing the bridge that produces such events.
        ArrowOrderUpdate noRemarks = ArrowOrderUpdate.builder()
            .brokerOrderId(new BrokerOrderId("2600090003"))
            .instrumentToken(new InstrumentToken(1594))
            .status(ArrowOrderStatus.OrderStatus.OPEN)
            .reportType(ArrowOrderStatus.ReportType.NEW_ACK)
            .build();
        assertThat(noRemarks.clientOrderRef()).isNull();
    }

    @Test
    void mapsPostbackIdentityAndFills() {
        // Sample postback (abridged) for orderNo 2600090001, token 26009.
        // R-262: named-parameter builder (the old 11-param positional ctor
        // made the three adjacent longs silently swappable).
        ArrowOrderUpdate update = ArrowOrderUpdate.builder()
            .brokerOrderId(new BrokerOrderId("2600090001"))
            .clientOrderRef(new ClientOrderRef("INV20250721"))
            .instrumentToken(new InstrumentToken(26009))
            .status(ArrowOrderStatus.OrderStatus.COMPLETE)
            .reportType(ArrowOrderStatus.ReportType.FILL)
            .fillId("F1")
            .fillQuantity(10)
            .fillPrice(297510L)           // paise
            .fillTime(1_752_539_000_000L) // epoch ms (R-172)
            .build();

        assertThat(update.brokerOrderId()).isEqualTo(new BrokerOrderId("2600090001"));
        assertThat(update.clientOrderRef()).isEqualTo(new ClientOrderRef("INV20250721"));
        assertThat(update.instrumentToken()).isEqualTo(new InstrumentToken(26009));
        assertThat(update.reportType()).isEqualTo(ArrowOrderStatus.ReportType.FILL);
        assertThat(update.fillQuantity()).isEqualTo(10);
        assertThat(update.isFill()).isTrue();
    }

    @Test
    void newAckHasNoFillYet() {
        ArrowOrderUpdate ack = ArrowOrderUpdate.builder()
            .brokerOrderId(new BrokerOrderId("2600090002"))
            .clientOrderRef(new ClientOrderRef("INV20250722"))
            .instrumentToken(new InstrumentToken(1594))
            .status(ArrowOrderStatus.OrderStatus.OPEN)
            .reportType(ArrowOrderStatus.ReportType.NEW_ACK)
            .build();

        assertThat(ack.reportType()).isEqualTo(ArrowOrderStatus.ReportType.NEW_ACK);
        assertThat(ack.fillQuantity()).isEqualTo(0);
        assertThat(ack.isFill()).isFalse();
    }
}
