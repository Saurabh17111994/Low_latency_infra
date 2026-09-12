package com.trading.common.arrow;

import com.trading.common.identity.IdentityModel.BrokerOrderId;
import com.trading.common.identity.IdentityModel.ClientOrderRef;
import com.trading.common.identity.IdentityModel.InstrumentToken;
import java.util.Locale;

/** Arrow order-status vocabulary from rest-api/orders (order book / trade book). */
public final class ArrowOrderStatus {

    private ArrowOrderStatus() {}

    /** orderStatus values returned by GET /user/orders and the postback stream. */
    public enum OrderStatus {
        PENDING,
        OPEN,
        COMPLETE,
        CANCELLED,
        REJECTED;

        /**
         * R-125: lenient, case-insensitive parse with common broker variants —
         * the old strict {@code valueOf()} threw an unchecked exception for any
         * spelling the broker actually emits (e.g. "FILLED" or "CANCELED").
         * Truly unknown values still fail, but with a descriptive message.
         */
        public static OrderStatus from(String s) {
            if (s == null || s.isBlank()) {
                throw new IllegalArgumentException("orderStatus is null or blank");
            }
            String v = s.trim().toUpperCase(Locale.ROOT);
            switch (v) {
                case "PENDING": return PENDING;
                case "OPEN": return OPEN;
                case "COMPLETE":
                case "FILLED": return COMPLETE;
                // P3-107: "FILL" is not a status the broker sends — the Go bridge's
                // knownOrderStatus whitelist (postback.go:220) has no such arm, and a
                // partial fill arrives as reportType=Fill with status OPEN. Mapping it
                // to the terminal COMPLETE hid a live position behind a finished one.
                case "CANCELLED":
                case "CANCELED": return CANCELLED;
                case "REJECTED": return REJECTED;
                default:
                    throw new IllegalArgumentException(
                            "unknown orderStatus: " + s
                            + " (expected one of PENDING, OPEN, COMPLETE, CANCELLED, REJECTED)");
            }
        }
    }

    /** reportType values describing the lifecycle event. */
    public enum ReportType {
        NEW_ACK("NewAck"),
        PENDING_NEW("PendingNew"),
        FILL("Fill"),
        CANCELED("Canceled"),
        REJECTED("Rejected"),
        /**
         * R-126: the broker's wire vocabulary is not exhaustively known — an
         * unrecognized reportType must not throw (that would break the
         * fill-detection path on any new event kind). Map it to UNKNOWN.
         */
        UNKNOWN("");

        private final String wire;
        ReportType(String wire) { this.wire = wire; }

        public String wire() { return wire; }

        public static ReportType from(String s) {
            if (s == null || s.isBlank()) {
                return UNKNOWN;
            }
            String v = s.trim();
            for (ReportType r : values()) {
                if (r == UNKNOWN) continue;
                if (r.wire.equalsIgnoreCase(v) || r.name().equalsIgnoreCase(v)) {
                    return r;
                }
            }
            return UNKNOWN;
        }
    }

    /** Lifecycle event from the postback stream or order book row. */
    public static final class OrderUpdate {
        public final BrokerOrderId brokerOrderId;
        public final ClientOrderRef clientOrderRef; // Arrow "remarks"
        public final InstrumentToken instrumentToken; // Arrow "token"
        public final OrderStatus status;
        public final ReportType reportType;
        public final long fillQuantity;
        public final long averagePrice; // paise
        public final String exchangeOrderId;
        public final String rejectReason;

        public OrderUpdate(BrokerOrderId brokerOrderId, ClientOrderRef clientOrderRef,
                           InstrumentToken instrumentToken, OrderStatus status, ReportType reportType,
                           long fillQuantity, long averagePrice, String exchangeOrderId, String rejectReason) {
            this.brokerOrderId = brokerOrderId;
            this.clientOrderRef = clientOrderRef;
            this.instrumentToken = instrumentToken;
            this.status = status;
            this.reportType = reportType;
            this.fillQuantity = fillQuantity;
            this.averagePrice = averagePrice;
            this.exchangeOrderId = exchangeOrderId;
            this.rejectReason = rejectReason;
        }
    }
}
