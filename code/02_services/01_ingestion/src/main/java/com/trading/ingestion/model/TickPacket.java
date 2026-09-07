package com.trading.ingestion.model;

import com.trading.common.config.PlatformConfig;
import java.time.Instant;

/**
 * Fully decoded + normalized + validated tick packet ready for Fluss append.
 * All monetary values in integer paise (₹1 = 100 paise).
 * All typed fields are verified and normalized before construction.
 */
public final class TickPacket {
    // --- provenance ---
    private final RawTick raw;
    private final ValidityClassification validity;
    private final String validityReason;

    // --- routing ---
    private final long instrumentToken;
    private final String tradingSymbol;
    private final String exchange;

    // --- event time ---
    private final Instant eventTime;       // verified UTC broker timestamp
    private final Instant ingestTs;        // local time before append
    private final Instant appendAckTs;     // local time of append ack (set post-append)

    // --- trade data (verified/normalized; prices in paise) ---
    private final long lastPricePaise;
    private final long volume;
    private final double change;            // pct change — not monetary
    private final long ohlcOpenPaise;
    private final long ohlcHighPaise;
    private final long ohlcLowPaise;
    private final long ohlcClosePaise;
    private final long averagePricePaise;
    private final long openInterest;

    // --- fingerprint ---
    private final String eventFingerprint;
    private final int fingerprintVersion;

    // --- connection identity ---
    private final String connectionId;
    private final long connectionEpoch;
    private final String instanceId;

    // --- schema ---
    private final int schemaVersion;

    private TickPacket(Builder b) {
        // R-227: "All typed fields are verified and normalized before
        // construction" was a lie — build() validated nothing. Fail fast on
        // the required fields so an uninitialized packet can never reach
        // the append path.
        if (b.raw == null) throw new IllegalArgumentException("raw is required");
        if (b.validity == null) throw new IllegalArgumentException("validity is required");
        if (b.instrumentToken <= 0) {
            throw new IllegalArgumentException(
                    "instrumentToken must be positive, got " + b.instrumentToken);
        }
        // R-209: eventTime/ingestTs must be explicitly provided — the old
        // Instant.EPOCH default made the `eventTime != null` guard dead code
        // and silently fabricated 1970 timestamps.
        if (b.eventTime == null || b.eventTime.equals(Instant.EPOCH)) {
            throw new IllegalArgumentException("eventTime must be set (not EPOCH)");
        }
        if (b.ingestTs == null || b.ingestTs.equals(Instant.EPOCH)) {
            throw new IllegalArgumentException("ingestTs must be set (not EPOCH)");
        }
        if (isBlank(b.tradingSymbol)) throw new IllegalArgumentException("tradingSymbol must not be blank");
        if (isBlank(b.exchange)) throw new IllegalArgumentException("exchange must not be blank");
        if (isBlank(b.eventFingerprint)) throw new IllegalArgumentException("eventFingerprint must not be blank");
        if (isBlank(b.connectionId)) throw new IllegalArgumentException("connectionId must not be blank");
        if (b.fingerprintVersion <= 0) {
            throw new IllegalArgumentException(
                    "fingerprintVersion must be positive, got " + b.fingerprintVersion);
        }
        // P1-089: monetary invariants hold for EVERY classification — quotes
        // may be 0 (VALID_NON_TRADE), never negative; change is a pct, always
        // finite. Corrupt values previously reached the Fluss append path.
        if (b.lastPricePaise < 0 || b.ohlcOpenPaise < 0 || b.ohlcHighPaise < 0
                || b.ohlcLowPaise < 0 || b.ohlcClosePaise < 0 || b.averagePricePaise < 0) {
            throw new IllegalArgumentException("price fields must be >= 0");
        }
        if (b.volume < 0 || b.openInterest < 0) {
            throw new IllegalArgumentException("volume/openInterest must be >= 0");
        }
        if (!Double.isFinite(b.change)) {
            throw new IllegalArgumentException("change must be finite, got " + b.change);
        }
        if (b.validity == ValidityClassification.VALID_TRADE && b.lastPricePaise <= 0) {
            throw new IllegalArgumentException("VALID_TRADE requires lastPricePaise > 0");
        }

        this.raw = b.raw;
        this.validity = b.validity;
        this.validityReason = b.validityReason != null ? b.validityReason : "";
        this.instrumentToken = b.instrumentToken;
        this.tradingSymbol = b.tradingSymbol;
        this.exchange = b.exchange;
        this.eventTime = b.eventTime;
        this.ingestTs = b.ingestTs;
        this.appendAckTs = b.appendAckTs; // R-160: no longer hardcoded to EPOCH
        this.lastPricePaise = b.lastPricePaise;
        this.volume = b.volume;
        this.change = b.change;
        this.ohlcOpenPaise = b.ohlcOpenPaise;
        this.ohlcHighPaise = b.ohlcHighPaise;
        this.ohlcLowPaise = b.ohlcLowPaise;
        this.ohlcClosePaise = b.ohlcClosePaise;
        this.averagePricePaise = b.averagePricePaise;
        this.openInterest = b.openInterest;
        this.eventFingerprint = b.eventFingerprint;
        this.fingerprintVersion = b.fingerprintVersion;
        this.connectionId = b.connectionId;
        this.connectionEpoch = b.connectionEpoch;
        this.instanceId = b.instanceId != null ? b.instanceId : "";
        this.schemaVersion = b.schemaVersion;
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    // --- accessors ---
    public RawTick raw() { return raw; }
    public ValidityClassification validity() { return validity; }
    public String validityReason() { return validityReason; }
    public long instrumentToken() { return instrumentToken; }
    public String tradingSymbol() { return tradingSymbol; }
    public String exchange() { return exchange; }
    public Instant eventTime() { return eventTime; }
    public Instant ingestTs() { return ingestTs; }
    public Instant appendAckTs() { return appendAckTs; }
    public long lastPricePaise() { return lastPricePaise; }
    public long volume() { return volume; }
    public double change() { return change; }
    public long ohlcOpenPaise() { return ohlcOpenPaise; }
    public long ohlcHighPaise() { return ohlcHighPaise; }
    public long ohlcLowPaise() { return ohlcLowPaise; }
    public long ohlcClosePaise() { return ohlcClosePaise; }
    public long averagePricePaise() { return averagePricePaise; }
    public long openInterest() { return openInterest; }
    public String eventFingerprint() { return eventFingerprint; }
    public int fingerprintVersion() { return fingerprintVersion; }
    public String connectionId() { return connectionId; }
    public long connectionEpoch() { return connectionEpoch; }
    public String instanceId() { return instanceId; }
    public int schemaVersion() { return schemaVersion; }

    public boolean isTradeEligible() {
        return validity == ValidityClassification.VALID_TRADE && eventTime != null;
    }

    @Override
    public String toString() {
        return "TickPacket{token=" + instrumentToken + ", sym=" + tradingSymbol
                + ", pricePaise=" + lastPricePaise + ", vol=" + volume
                + ", validity=" + validity + ", fp="
                + eventFingerprint.substring(0, Math.min(12, eventFingerprint.length())) + "}";
    }

    public static class Builder {
        RawTick raw;
        ValidityClassification validity = ValidityClassification.INVALID_VALUES;
        String validityReason;
        long instrumentToken;
        String tradingSymbol;
        String exchange;
        Instant eventTime;
        Instant ingestTs;
        Instant appendAckTs; // R-160: settable via builder; null until append ack
        long lastPricePaise;
        long volume;
        double change;
        long ohlcOpenPaise, ohlcHighPaise, ohlcLowPaise, ohlcClosePaise;
        long averagePricePaise;
        long openInterest;
        String eventFingerprint;
        int fingerprintVersion = 1;
        String connectionId;
        long connectionEpoch;
        String instanceId;
        /**
         * Default = shared raw_table_1 contract version; IngestionService no longer
         * overrides it, so the persisted label cannot drift from the consumer default.
         */
        int schemaVersion = Integer.parseInt(PlatformConfig.RAW_TABLE_1_SCHEMA_VERSION);

        public Builder raw(RawTick v) { this.raw = v; return this; }
        public Builder validity(ValidityClassification v) { this.validity = v; return this; }
        public Builder validityReason(String v) { this.validityReason = v; return this; }
        public Builder instrumentToken(long v) { this.instrumentToken = v; return this; }
        public Builder tradingSymbol(String v) { this.tradingSymbol = v; return this; }
        public Builder exchange(String v) { this.exchange = v; return this; }
        public Builder eventTime(Instant v) { this.eventTime = v; return this; }
        public Builder ingestTs(Instant v) { this.ingestTs = v; return this; }
        public Builder appendAckTs(Instant v) { this.appendAckTs = v; return this; }
        public Builder lastPricePaise(long v) { this.lastPricePaise = v; return this; }
        public Builder volume(long v) { this.volume = v; return this; }
        public Builder change(double v) { this.change = v; return this; }
        public Builder ohlcOpenPaise(long v) { this.ohlcOpenPaise = v; return this; }
        public Builder ohlcHighPaise(long v) { this.ohlcHighPaise = v; return this; }
        public Builder ohlcLowPaise(long v) { this.ohlcLowPaise = v; return this; }
        public Builder ohlcClosePaise(long v) { this.ohlcClosePaise = v; return this; }
        public Builder averagePricePaise(long v) { this.averagePricePaise = v; return this; }
        public Builder openInterest(long v) { this.openInterest = v; return this; }
        public Builder eventFingerprint(String v) { this.eventFingerprint = v; return this; }
        public Builder fingerprintVersion(int v) { this.fingerprintVersion = v; return this; }
        public Builder connectionId(String v) { this.connectionId = v; return this; }
        public Builder connectionEpoch(long v) { this.connectionEpoch = v; return this; }
        public Builder instanceId(String v) { this.instanceId = v; return this; }
        public Builder schemaVersion(int v) { this.schemaVersion = v; return this; }

        public TickPacket build() { return new TickPacket(this); }
    }
}
