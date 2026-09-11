package com.trading.common.identity;

/**
 * Canonical platform identity model.
 *
 * Every domain uses its own identity. An overloaded "order_id" is prohibited.
 * See docs/02_requirements/04-data.md §4.2 and DEC-007.
 *
 * <p>R-043/R-074: every identity class now implements value semantics
 * (equals/hashCode over the wrapped value) and rejects null/blank values at
 * construction so a bad identity fails fast instead of NPE-ing later.
 */
public final class IdentityModel {

    private IdentityModel() {}

    /** Immutable platform execution request created by the Signal job. */
    public static final class InstructionId {
        private final String value;
        public InstructionId(String value) {
            this.value = requireValue("InstructionId", value);
        }
        public String value() { return value; }
        @Override public String toString() { return value; }
        @Override public boolean equals(Object o) {
            return o instanceof InstructionId that && value.equals(that.value);
        }
        @Override public int hashCode() { return value.hashCode(); }
    }

    /** Deterministic broker-facing attempt reference (max 16 chars for Arrow remarks). */
    public static final class ClientOrderRef {
        // P3-110: the cap is the contract — ArrowOrderRequest re-checks at the
        // broker boundary, but fail-fast belongs here (every non-Arrow path
        // that mints a ref gets the same guarantee).
        public static final int MAX_LENGTH = 16;
        private final String value;
        public ClientOrderRef(String value) {
            String v = requireValue("ClientOrderRef", value);
            if (v.length() > MAX_LENGTH) {
                throw new IllegalArgumentException(
                        "ClientOrderRef value must be <= " + MAX_LENGTH + " chars, got length " + v.length());
            }
            this.value = v;
        }
        public String value() { return value; }
        @Override public String toString() { return value; }
        @Override public boolean equals(Object o) {
            return o instanceof ClientOrderRef that && value.equals(that.value);
        }
        @Override public int hashCode() { return value.hashCode(); }
    }

    /** Broker-authoritative order identity (returned by broker). */
    public static final class BrokerOrderId {
        private final String value;
        public BrokerOrderId(String value) {
            this.value = requireValue("BrokerOrderId", value);
        }
        public String value() { return value; }
        @Override public String toString() { return value; }
        @Override public boolean equals(Object o) {
            return o instanceof BrokerOrderId that && value.equals(that.value);
        }
        @Override public int hashCode() { return value.hashCode(); }
    }

    /** Stable instrument identity; the join key across market/postback/order books.
     *  Equals Arrow {@code Token} (int32) and {@code 01_ticks_raw.bucket.key}. */
    public static final class InstrumentToken {
        private final int value;
        public InstrumentToken(int value) {
            // R-075: tokens are Arrow int32 identities and the routing/join key —
            // zero or negative values would silently corrupt routing. Fail fast.
            if (value <= 0) {
                throw new IllegalArgumentException(
                        "InstrumentToken must be positive, got " + value);
            }
            this.value = value;
        }
        public int value() { return value; }
        @Override public String toString() { return Integer.toString(value); }
        @Override public boolean equals(Object o) {
            return o instanceof InstrumentToken that && value == that.value;
        }
        @Override public int hashCode() { return Integer.hashCode(value); }
    }

    /** Exchange/segment qualifier (NSE, BSE, NFO, BFO, MCX, ...). */
    public static final class ExchangeId {
        private final String value;
        public ExchangeId(String value) {
            this.value = requireValue("ExchangeId", value);
        }
        public String value() { return value; }
        @Override public String toString() { return value; }
        @Override public boolean equals(Object o) {
            return o instanceof ExchangeId that && value.equals(that.value);
        }
        @Override public int hashCode() { return value.hashCode(); }
    }

    /** One detected setup/audit record. */
    public static final class CandidateId {
        private final String value;
        public CandidateId(String value) { this.value = requireValue("CandidateId", value); }
        public String value() { return value; }
        @Override public String toString() { return value; }
        @Override public boolean equals(Object o) {
            return o instanceof CandidateId that && value.equals(that.value);
        }
        @Override public int hashCode() { return value.hashCode(); }
    }

    /** One submission attempt. */
    public static final class ExecutionAttemptId {
        private final String value;
        public ExecutionAttemptId(String value) { this.value = requireValue("ExecutionAttemptId", value); }
        public String value() { return value; }
        @Override public String toString() { return value; }
        @Override public boolean equals(Object o) {
            return o instanceof ExecutionAttemptId that && value.equals(that.value);
        }
        @Override public int hashCode() { return value.hashCode(); }
    }

    /** Groups entry and related position-management orders. */
    public static final class TradeContextId {
        private final String value;
        public TradeContextId(String value) { this.value = requireValue("TradeContextId", value); }
        public String value() { return value; }
        @Override public String toString() { return value; }
        @Override public boolean equals(Object o) {
            return o instanceof TradeContextId that && value.equals(that.value);
        }
        @Override public int hashCode() { return value.hashCode(); }
    }

    /** Stable position aggregate for fills, trim, exit, re-entry. */
    public static final class PositionId {
        private final String value;
        public PositionId(String value) { this.value = requireValue("PositionId", value); }
        public String value() { return value; }
        @Override public String toString() { return value; }
        @Override public boolean equals(Object o) {
            return o instanceof PositionId that && value.equals(that.value);
        }
        @Override public int hashCode() { return value.hashCode(); }
    }

    /** One platform-captured postback delivery. */
    public static final class PostbackEventId {
        private final String value;
        public PostbackEventId(String value) { this.value = requireValue("PostbackEventId", value); }
        public String value() { return value; }
        @Override public String toString() { return value; }
        @Override public boolean equals(Object o) {
            return o instanceof PostbackEventId that && value.equals(that.value);
        }
        @Override public int hashCode() { return value.hashCode(); }
    }

    /** Broker/account isolation boundary. */
    public static final class AccountScopeId {
        private final String value;
        public AccountScopeId(String value) { this.value = requireValue("AccountScopeId", value); }
        public String value() { return value; }
        @Override public String toString() { return value; }
        @Override public boolean equals(Object o) {
            return o instanceof AccountScopeId that && value.equals(that.value);
        }
        @Override public int hashCode() { return value.hashCode(); }
    }

    /** Ranking and capacity boundary. */
    public static final class PortfolioId {
        private final String value;
        public PortfolioId(String value) { this.value = requireValue("PortfolioId", value); }
        public String value() { return value; }
        @Override public String toString() { return value; }
        @Override public boolean equals(Object o) {
            return o instanceof PortfolioId that && value.equals(that.value);
        }
        @Override public int hashCode() { return value.hashCode(); }
    }

    /** Fenced Executor ownership boundary. */
    public static final class ExecutionPartitionId {
        private final String value;
        public ExecutionPartitionId(String value) { this.value = requireValue("ExecutionPartitionId", value); }
        public String value() { return value; }
        @Override public String toString() { return value; }
        @Override public boolean equals(Object o) {
            return o instanceof ExecutionPartitionId that && value.equals(that.value);
        }
        @Override public int hashCode() { return value.hashCode(); }
    }

    /** One portfolio capacity reservation. */
    public static final class ReservationId {
        private final String value;
        public ReservationId(String value) { this.value = requireValue("ReservationId", value); }
        public String value() { return value; }
        @Override public String toString() { return value; }
        @Override public boolean equals(Object o) {
            return o instanceof ReservationId that && value.equals(that.value);
        }
        @Override public int hashCode() { return value.hashCode(); }
    }

    /** One durable safety-halt request. */
    public static final class HaltRequestId {
        private final String value;
        public HaltRequestId(String value) { this.value = requireValue("HaltRequestId", value); }
        public String value() { return value; }
        @Override public String toString() { return value; }
        @Override public boolean equals(Object o) {
            return o instanceof HaltRequestId that && value.equals(that.value);
        }
        @Override public int hashCode() { return value.hashCode(); }
    }

    /** One immutable structured position action (post-MVP). */
    public static final class ActionId {
        private final String value;
        public ActionId(String value) { this.value = requireValue("ActionId", value); }
        public String value() { return value; }
        @Override public String toString() { return value; }
        @Override public boolean equals(Object o) {
            return o instanceof ActionId that && value.equals(that.value);
        }
        @Override public int hashCode() { return value.hashCode(); }
    }

    /** R-074: identity values are mandatory and must not be blank. */
    private static String requireValue(String kind, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(kind + " value must not be null or blank");
        }
        // P3-339: join/idempotency keys compare by value — 'ABC' vs ' ABC '
        // must be the same identity, never two map keys / two submissions.
        return value.strip();
    }
}
