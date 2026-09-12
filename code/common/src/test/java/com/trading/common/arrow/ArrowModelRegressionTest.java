package com.trading.common.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.common.arrow.ArrowOrderStatus.OrderStatus;
import com.trading.common.arrow.ArrowOrderStatus.ReportType;
import com.trading.common.identity.IdentityModel.BrokerOrderId;
import com.trading.common.identity.IdentityModel.ClientOrderRef;
import com.trading.common.identity.IdentityModel.ExchangeId;
import com.trading.common.identity.IdentityModel.InstrumentToken;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * R-121..R-126, R-197, R-198 — Arrow order models fail fast and parse leniently.
 */
@DisplayName("Phase 5 arrow-model regression tests")
class ArrowModelRegressionTest {

    private ArrowOrderRequest limit(double price) {
        return new ArrowOrderRequest(
                new ExchangeId("NSECM"), "RELIANCE-EQ", new InstrumentToken(26009), 10,
                ArrowOrderRequest.TransactionType.B, ArrowOrderRequest.OrderType.LMT,
                ArrowOrderRequest.Product.I, String.valueOf(price),
                ArrowOrderRequest.Validity.DAY, 0, new ClientOrderRef("REF"), false);
    }

    @Test
    @DisplayName("null ClientOrderRef NPEs with a message, not a raw NPE (R-121)")
    void nullClientOrderRef() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new ArrowOrderRequest(
                        new ExchangeId("NSECM"), "SYM", new InstrumentToken(1), 1,
                        ArrowOrderRequest.TransactionType.B, ArrowOrderRequest.OrderType.LMT,
                        ArrowOrderRequest.Product.I, "100", ArrowOrderRequest.Validity.DAY,
                        0, null, false));
        assertTrue(e.getMessage().contains("clientOrderRef"));
    }

    @Test
    @DisplayName("price validated per order type (R-122)")
    void priceValidation() {
        assertThrows(IllegalArgumentException.class, () -> limit(0));
        assertThrows(IllegalArgumentException.class, () -> limit(-1));
        assertThrows(IllegalArgumentException.class, () -> limit(Double.NaN));
        // Market orders must carry "0".
        assertThrows(IllegalArgumentException.class,
                () -> new ArrowOrderRequest(
                        new ExchangeId("NSECM"), "SYM", new InstrumentToken(1), 1,
                        ArrowOrderRequest.TransactionType.B, ArrowOrderRequest.OrderType.MKT,
                        ArrowOrderRequest.Product.I, "100", ArrowOrderRequest.Validity.DAY,
                        0, new ClientOrderRef("REF"), false));
    }

    @Test
    @DisplayName("mandatory fields null-checked (R-123)")
    void mandatoryFieldsNullChecked() {
        assertThrows(NullPointerException.class,
                () -> new ArrowOrderRequest(
                        null, "SYM", new InstrumentToken(1), 1,
                        ArrowOrderRequest.TransactionType.B, ArrowOrderRequest.OrderType.LMT,
                        ArrowOrderRequest.Product.I, "100", ArrowOrderRequest.Validity.DAY,
                        0, new ClientOrderRef("REF"), false));
        assertThrows(NullPointerException.class,
                () -> new ArrowOrderRequest(
                        new ExchangeId("NSE"), "SYM", new InstrumentToken(1), 1,
                        null, ArrowOrderRequest.OrderType.LMT,
                        ArrowOrderRequest.Product.I, "100", ArrowOrderRequest.Validity.DAY,
                        0, new ClientOrderRef("REF"), false));
    }

    @Test
    @DisplayName("negative disclosedQty rejected (R-197)")
    void disclosedQtyNonNegative() {
        assertThrows(IllegalArgumentException.class,
                () -> new ArrowOrderRequest(
                        new ExchangeId("NSECM"), "SYM", new InstrumentToken(1), 1,
                        ArrowOrderRequest.TransactionType.B, ArrowOrderRequest.OrderType.LMT,
                        ArrowOrderRequest.Product.I, "100", ArrowOrderRequest.Validity.DAY,
                        -1, new ClientOrderRef("REF"), false));
    }

    @Test
    @DisplayName("fromJson guards null data and missing/non-numeric requestTime (R-124/198)")
    void fromJsonGuards() {
        assertThrows(IllegalArgumentException.class,
                () -> ArrowOrderResponse.fromJson(null));
        assertThrows(IllegalArgumentException.class,
                () -> ArrowOrderResponse.fromJson(Map.of("orderNo", "123")));
        assertThrows(IllegalArgumentException.class,
                () -> ArrowOrderResponse.fromJson(
                        Map.of("orderNo", "123", "requestTime", "1_752_539_000")));
        ArrowOrderResponse ok = ArrowOrderResponse.fromJson(
                Map.of("orderNo", "123", "requestTime", 1_752_539_000L));
        assertEquals(new BrokerOrderId("123"), ok.brokerOrderId());
        assertEquals(1_752_539_000L, ok.requestTime());
    }

    @Test
    @DisplayName("OrderStatus parses broker variants leniently (R-125)")
    void orderStatusLenient() {
        assertEquals(OrderStatus.COMPLETE, OrderStatus.from("COMPLETE"));
        assertEquals(OrderStatus.COMPLETE, OrderStatus.from("FILLED"));
        assertEquals(OrderStatus.CANCELLED, OrderStatus.from("CANCELED"));
        assertEquals(OrderStatus.CANCELLED, OrderStatus.from("cancelled"));
        // P3-107: "FILL" is not the broker's terminal spelling.
        assertThrows(IllegalArgumentException.class, () -> OrderStatus.from("FILL"));
        // P3-108: a stop order that has not triggered yet is its own state.
        assertEquals(OrderStatus.TRIGGER_PENDING, OrderStatus.from("TRIGGER_PENDING"));
        assertEquals(OrderStatus.TRIGGER_PENDING, OrderStatus.from("trigger_pending"));
        assertThrows(IllegalArgumentException.class, () -> OrderStatus.from("NOT_A_STATUS"));
        assertThrows(IllegalArgumentException.class, () -> OrderStatus.from(null));
    }

    @Test
    @DisplayName("ReportType maps unrecognized wire values to UNKNOWN (R-126)")
    void reportTypeUnknown() {
        assertEquals(ReportType.FILL, ReportType.from("Fill"));
        assertEquals(ReportType.UNKNOWN, ReportType.from("SomeFutureEventKind"));
        assertEquals(ReportType.UNKNOWN, ReportType.from(null));
        assertEquals(ReportType.UNKNOWN, ReportType.from(""));
    }
}
