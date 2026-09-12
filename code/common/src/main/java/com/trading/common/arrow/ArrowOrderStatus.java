package com.trading.common.arrow;

import java.util.Locale;

/** Arrow order-status vocabulary from rest-api/orders (order book / trade book). */
public final class ArrowOrderStatus {

    private ArrowOrderStatus() {}

    /** orderStatus values returned by GET /user/orders and the postback stream. */
    public enum OrderStatus {
        PENDING,
        /**
         * P3-108: accepted but not yet triggered (stop orders). The Go bridge
         * whitelists this value (postback.go:220) and the executor contract names
         * it, so it must not collapse into PENDING — that would lose the broker's
         * distinction exactly as mapping FILL onto COMPLETE did (P3-107).
         */
        TRIGGER_PENDING,
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
                case "TRIGGER_PENDING": return TRIGGER_PENDING;
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
                            + " (expected one of " + java.util.Arrays.toString(values()) + ")");
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
            // P3-336: the broker writes "Canceled" in some payloads and "Cancelled"
            // in others; a double L matches neither the wire string nor the enum name,
            // so it silently became UNKNOWN and the cancel was never classified.
            if (v.equalsIgnoreCase("cancelled")) {
                return CANCELED;
            }
            for (ReportType r : values()) {
                if (r == UNKNOWN) continue;
                if (r.wire.equalsIgnoreCase(v) || r.name().equalsIgnoreCase(v)) {
                    return r;
                }
            }
            return UNKNOWN;
        }
    }

}
