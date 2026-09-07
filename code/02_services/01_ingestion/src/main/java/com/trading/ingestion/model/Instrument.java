package com.trading.ingestion.model;

import java.util.Objects;

/**
 * One entry from the versioned instrument manifest.
 * Every field must be validated, and required routing fields must be non-null.
 */
public final class Instrument {
    private final long instrumentToken;
    private final String tradingSymbol;
    private final String exchange;
    private final String segment;
    private final int lotSize;
    private final long manifestVersion;

    private Instrument(Builder builder) {
        // R-115: instrumentToken is the identity/join key — a defaulted 0 or
        // negative token must fail fast instead of silently corrupting routing.
        if (builder.instrumentToken <= 0) {
            throw new IllegalArgumentException(
                    "instrumentToken must be positive, got " + builder.instrumentToken);
        }
        // R-193: required routing fields must be non-blank, not just non-null.
        if (isBlank(builder.tradingSymbol)) {
            throw new IllegalArgumentException("tradingSymbol must not be blank");
        }
        if (isBlank(builder.exchange)) {
            throw new IllegalArgumentException("exchange must not be blank");
        }
        this.instrumentToken = builder.instrumentToken;
        this.tradingSymbol = builder.tradingSymbol;
        this.exchange = builder.exchange;
        this.segment = builder.segment != null ? builder.segment : "";
        // R-116: a non-positive lot size must not be silently coerced to 1 —
        // that masks invalid manifest data. Require an explicit positive value.
        if (builder.lotSize <= 0) {
            throw new IllegalArgumentException(
                    "lotSize must be positive, got " + builder.lotSize);
        }
        this.lotSize = builder.lotSize;
        if (builder.manifestVersion <= 0) {
            throw new IllegalArgumentException(
                    "manifestVersion must be positive, got " + builder.manifestVersion);
        }
        this.manifestVersion = builder.manifestVersion;
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    public long instrumentToken() { return instrumentToken; }
    public String tradingSymbol() { return tradingSymbol; }
    public String exchange() { return exchange; }
    public String segment() { return segment; }
    public int lotSize() { return lotSize; }
    public long manifestVersion() { return manifestVersion; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Instrument that)) return false;
        return instrumentToken == that.instrumentToken;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(instrumentToken);
    }

    @Override
    public String toString() {
        return "Instrument{" + instrumentToken + "=" + tradingSymbol + "(" + exchange + ")}";
    }

    public static class Builder {
        private long instrumentToken;
        private String tradingSymbol;
        private String exchange;
        private String segment;
        // P1-086: NO default — an omitted lotSize must fail the R-116 positive
        // check (leave it 1 and omission silently passes as quantity 1).
        private int lotSize;
        private long manifestVersion;

        public Builder instrumentToken(long v) { this.instrumentToken = v; return this; }
        public Builder tradingSymbol(String v) { this.tradingSymbol = v; return this; }
        public Builder exchange(String v) { this.exchange = v; return this; }
        public Builder segment(String v) { this.segment = v; return this; }
        public Builder lotSize(int v) { this.lotSize = v; return this; }
        public Builder manifestVersion(long v) { this.manifestVersion = v; return this; }

        public Instrument build() {
            return new Instrument(this);
        }
    }
}
