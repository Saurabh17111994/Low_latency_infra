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
    @DisplayName("whitespace-only symbol is not a TradingSymbol (P3-328)")
    void symbolMustNotBeBlank() {
        assertThrows(IllegalArgumentException.class,
                () -> new ArrowOrderRequest(
                        new ExchangeId("NSECM"), "   ", new InstrumentToken(1), 1,
                        ArrowOrderRequest.TransactionType.B, ArrowOrderRequest.OrderType.LMT,
                        ArrowOrderRequest.Product.I, "100", ArrowOrderRequest.Validity.DAY,
                        0, new ClientOrderRef("REF"), false));
    }

    @Test
    @DisplayName("disclosedQty may not exceed quantity (P3-329)")
    void disclosedQtyWithinQuantity() {
        assertThrows(IllegalArgumentException.class,
                () -> new ArrowOrderRequest(
                        new ExchangeId("NSECM"), "SYM", new InstrumentToken(1), 10,
                        ArrowOrderRequest.TransactionType.B, ArrowOrderRequest.OrderType.LMT,
                        ArrowOrderRequest.Product.I, "100", ArrowOrderRequest.Validity.DAY,
                        11, new ClientOrderRef("REF"), false));
    }

    @Test
    @DisplayName("the stored price is the validated, trimmed price (P3-331)")
    void priceIsStoredTrimmed() {
        ArrowOrderRequest padded = new ArrowOrderRequest(
                new ExchangeId("NSECM"), "SYM", new InstrumentToken(1), 1,
                ArrowOrderRequest.TransactionType.B, ArrowOrderRequest.OrderType.LMT,
                ArrowOrderRequest.Product.I, " 100 ", ArrowOrderRequest.Validity.DAY,
                0, new ClientOrderRef("REF"), false);
        assertEquals("100", padded.price());
    }

    @Test
    @DisplayName("a limit price must be finite, not merely > 0 (P3-332)")
    void limitPriceMustBeFinite() {
        assertThrows(IllegalArgumentException.class,
                () -> new ArrowOrderRequest(
                        new ExchangeId("NSECM"), "SYM", new InstrumentToken(1), 1,
                        ArrowOrderRequest.TransactionType.B, ArrowOrderRequest.OrderType.LMT,
                        ArrowOrderRequest.Product.I, "1e309", ArrowOrderRequest.Validity.DAY,
                        0, new ClientOrderRef("REF"), false));
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
        // Same branch, spelled out: the string is non-numeric, not merely unparseable
        // because of its underscores (P3-334 makes numeric strings valid).
        assertThrows(IllegalArgumentException.class,
                () -> ArrowOrderResponse.fromJson(
                        Map.of("orderNo", "123", "requestTime", "not-a-number")));
        ArrowOrderResponse ok = ArrowOrderResponse.fromJson(
                Map.of("orderNo", "123", "requestTime", 1_752_539_000L));
        assertEquals(new BrokerOrderId("123"), ok.brokerOrderId());
        assertEquals(1_752_539_000L, ok.requestTime());
    }

    @Test
    @DisplayName("orderNo must be a non-blank String or Number, never coerced (P3-335)")
    void orderNoTypeGuards() {
        // A boolean has no order number: String.valueOf turned it into "true" and
        // BrokerOrderId accepted that as a broker identity.
        assertThrows(IllegalArgumentException.class,
                () -> ArrowOrderResponse.fromJson(
                        Map.of("orderNo", true, "requestTime", 1_752_539_000L)));
        // A Double orderNo's String.valueOf is "1.75253900075E9" — not an order number.
        ArrowOrderResponse numeric = ArrowOrderResponse.fromJson(
                Map.of("orderNo", 1752539000.75, "requestTime", 1_752_539_000L));
        assertEquals(new BrokerOrderId("1752539000"), numeric.brokerOrderId());
        ArrowOrderResponse padded = ArrowOrderResponse.fromJson(
                Map.of("orderNo", " 123 ", "requestTime", 1_752_539_000L));
        assertEquals(new BrokerOrderId("123"), padded.brokerOrderId());
    }

    @Test
    @DisplayName("a quoted numeric requestTime is accepted (P3-334)")
    void requestTimeAcceptsNumericStrings() {
        ArrowOrderResponse fromString = ArrowOrderResponse.fromJson(
                Map.of("orderNo", "123", "requestTime", "1752539000"));
        assertEquals(1_752_539_000L, fromString.requestTime());
    }

    @Test
    @DisplayName("a fractional requestTime is rejected, not truncated (P3-489)")
    void requestTimeRejectsFractionalNumbers() {
        assertThrows(IllegalArgumentException.class,
                () -> ArrowOrderResponse.fromJson(
                        Map.of("orderNo", "123", "requestTime", 1_752_539_000.9)));
        // The float must be small enough for a fraction to survive float precision:
        // at 1.7e9 a float's precision is about 128, so 1_752_539_000.5f is already
        // integral before any code here sees it. 1234567.5f is exactly representable.
        assertThrows(IllegalArgumentException.class,
                () -> ArrowOrderResponse.fromJson(
                        Map.of("orderNo", "123", "requestTime", 1_234_567.5f)));
    }

    @Test
    @DisplayName("the response constructor enforces what fromJson enforces (P3-333)")
    void constructorGuards() {
        assertThrows(NullPointerException.class,
                () -> new ArrowOrderResponse(null, 1_752_539_000L));
        assertThrows(IllegalArgumentException.class,
                () -> new ArrowOrderResponse(new BrokerOrderId("123"), 0));
        assertThrows(IllegalArgumentException.class,
                () -> new ArrowOrderResponse(new BrokerOrderId("123"), -1));
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
        // P3-336: the broker also spells it with a double L.
        assertEquals(ReportType.CANCELED, ReportType.from("Cancelled"));
        assertEquals(ReportType.CANCELED, ReportType.from("CANCELLED"));
        assertEquals(ReportType.CANCELED, ReportType.from("cancelled"));
        assertEquals(ReportType.UNKNOWN, ReportType.from("SomeFutureEventKind"));
        assertEquals(ReportType.UNKNOWN, ReportType.from(null));
        assertEquals(ReportType.UNKNOWN, ReportType.from(""));
    }
}
