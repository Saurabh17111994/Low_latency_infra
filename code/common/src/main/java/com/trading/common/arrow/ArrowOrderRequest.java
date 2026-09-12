package com.trading.common.arrow;

import com.trading.common.identity.IdentityModel.ClientOrderRef;
import com.trading.common.identity.IdentityModel.ExchangeId;
import com.trading.common.identity.IdentityModel.InstrumentToken;
import java.util.Objects;

/**
 * Arrow {@code POST /order/regular} request, typed to the platform identity model.
 *
 * Identity rule: the platform {@link ClientOrderRef} is sent in Arrow's
 * {@code remarks} field, which is capped at 16 characters. The constructor
 * enforces that cap so an over-long ref can never be submitted.
 */
public final class ArrowOrderRequest {

    public static final int REMARKS_MAX = 16;

    public enum TransactionType { B, S }
    public enum OrderType { LMT, MKT, SL_LMT, SL_MKT }
    public enum Product { I, C, M } // intraday, delivery, F&O
    public enum Validity { DAY, IOC }

    private final ExchangeId exchange;
    private final String symbol; // Arrow TradingSymbol, e.g. "ITC-EQ"
    private final InstrumentToken instrumentToken;
    private final long quantity;
    private final TransactionType transactionType;
    private final OrderType order;
    private final Product product;
    private final String price; // Arrow expects string; "0" for MKT
    private final Validity validity;
    private final long disclosedQty;
    private final ClientOrderRef clientOrderRef; // -> remarks (<=16)
    private final boolean mpp; // mimic-market; MKT disabled by default

    public ArrowOrderRequest(ExchangeId exchange, String symbol, InstrumentToken instrumentToken,
                             long quantity, TransactionType transactionType, OrderType order,
                             Product product, String price, Validity validity, long disclosedQty,
                             ClientOrderRef clientOrderRef, boolean mpp) {
        // R-123: mandatory Arrow fields are null-checked at construction so a
        // broken request fails fast instead of NPE-ing at the broker boundary.
        Objects.requireNonNull(exchange, "exchange");
        Objects.requireNonNull(instrumentToken, "instrumentToken");
        Objects.requireNonNull(transactionType, "transactionType");
        Objects.requireNonNull(order, "order");
        Objects.requireNonNull(product, "product");
        Objects.requireNonNull(validity, "validity");
        // P3-328: isBlank, not isEmpty — "   " is not a TradingSymbol.
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("symbol required");
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be > 0");
        }
        // R-197: a negative disclosed quantity is never valid for Arrow.
        if (disclosedQty < 0) {
            throw new IllegalArgumentException("disclosedQty must be >= 0");
        }
        // P3-329: disclosing more than the order size is not expressible.
        if (disclosedQty > quantity) {
            throw new IllegalArgumentException(
                    "disclosedQty must be <= quantity, got: " + disclosedQty + " > " + quantity);
        }
        // R-121: null-check the ref BEFORE calling .value() — the old code
        // dereferenced clientOrderRef.value() first, NPE-ing on a null ref.
        if (clientOrderRef == null) {
            throw new IllegalArgumentException("clientOrderRef is required");
        }
        String ref = clientOrderRef.value();
        if (ref == null || ref.length() > REMARKS_MAX) {
            throw new IllegalArgumentException(
                "clientOrderRef (remarks) must be <= " + REMARKS_MAX + " chars, got: " + ref);
        }
        // R-122: price format must match the order type — LMT/SL_LMT require a
        // positive numeric price; MKT/SL_MKT must be "0".
        validatePrice(order, price);
        this.exchange = exchange;
        this.symbol = symbol;
        this.instrumentToken = instrumentToken;
        this.quantity = quantity;
        this.transactionType = transactionType;
        this.order = order;
        this.product = product;
        this.price = price.trim(); // P3-331: store what was validated, not the raw string
        this.validity = validity;
        this.disclosedQty = disclosedQty;
        this.clientOrderRef = clientOrderRef;
        this.mpp = mpp;
    }

    private static void validatePrice(OrderType order, String price) {
        boolean limit = order == OrderType.LMT || order == OrderType.SL_LMT;
        if (price == null || price.isBlank()) {
            throw new IllegalArgumentException("price required");
        }
        if (limit) {
            double v;
            try {
                v = Double.parseDouble(price.trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "limit order price must be numeric, got: " + price);
            }
            // P3-332: NaN and +Infinity both satisfy "v > 0" in some way (Infinity
            // does outright: "1e309" parses to Inf), so finiteness is checked first.
            if (!Double.isFinite(v) || v <= 0) {
                throw new IllegalArgumentException(
                        "limit order price must be a finite value > 0, got: " + price);
            }
        } else {
            // MKT / SL_MKT — the broker expects "0".
            if (!"0".equals(price.trim())) {
                throw new IllegalArgumentException(
                        "market order price must be \"0\", got: " + price);
            }
        }
    }

    public ExchangeId exchange() { return exchange; }
    public String symbol() { return symbol; }
    public InstrumentToken instrumentToken() { return instrumentToken; }
    public long quantity() { return quantity; }
    public TransactionType transactionType() { return transactionType; }
    public OrderType order() { return order; }
    public Product product() { return product; }
    public String price() { return price; }
    public Validity validity() { return validity; }
    public long disclosedQty() { return disclosedQty; }
    public ClientOrderRef clientOrderRef() { return clientOrderRef; }
    public boolean mpp() { return mpp; }
}
