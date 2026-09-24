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
    private final long lastQty;
    private final long volume;
    private final double change;            // pct change — not monetary
    private final long ohlcOpenPaise;
    private final long ohlcHighPaise;
    private final long ohlcLowPaise;
    private final long ohlcClosePaise;
    private final long averagePricePaise;
    private final long openInterest;

    // --- v4 full-mode fields (raw_table_1 indexes 21-71) ---
    // Feed-agnostic: the standard (free-plan) and HFT feeds both report these.
    private final long totalBuyQty;
    private final long totalSellQty;
    /**
     * Traded qty since the previous tick for this token. {@code null} = baseline
     * unknown (this epoch's first tick for the token); 0 = nothing traded. A candle
     * sums THIS — {@link #lastQty} is a single trade's size, so summing it
     * under-counts when several trades land between two snapshots.
     */
    private final Long volumeDelta;
    // Feed-specific: null when the active feed does not report the field at all.
    private final Long atv;                 // HFT only
    private final Long btv;                 // HFT only
    private final Long changeFlag;          // standard feed only
    private final Long oiDayHigh;           // standard feed only
    private final Long oiDayLow;            // standard feed only
    private final Long imbalanceQty;        // standard feed, closing-auction window only
    private final Long indicativeClosePaise;
    private final Long refPricePaise;
    // Both feeds report a price band; 0 paise is not a price, so 0 = not published.
    private final Long lowerLimitPaise;
    private final Long upperLimitPaise;
    /** Epoch ms; null = unknown (the bridge sends 0 when the unit was implausible). */
    private final Long lastTradedTimeMs;
    // Depth ladder, level 1..5 at index 0..4. A level the book does not have is 0,
    // matching the explicit zero ladders the LTP path already emits.
    private final long[] bidPx;
    private final long[] bidQty;
    private final long[] bidOrd;
    private final long[] askPx;
    private final long[] askQty;
    private final long[] askOrd;

    /** Depth levels carried on the wire, fixed-width (proto Q-O2 / DDL _1.._5). */
    public static final int DEPTH_LEVELS = 5;

    // --- fingerprint ---
    private final String eventFingerprint;
    private final int fingerprintVersion;

    // --- connection identity ---
    private final String connectionId;
    private final long connectionEpoch;
    private final String instanceId;

    // --- schema ---
    private final int schemaVersion;

    // P1-251: parsed once (not per-Builder on the hot path); a malformed
    // constant fails fast with context instead of a bare NumberFormatException
    // during unrelated packet construction.
    private static final int DEFAULT_SCHEMA_VERSION = parseSchemaVersion();

    private static int parseSchemaVersion() {
        try {
            return Integer.parseInt(PlatformConfig.RAW_TABLE_1_SCHEMA_VERSION);
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    "PlatformConfig.RAW_TABLE_1_SCHEMA_VERSION is not an int: "
                            + PlatformConfig.RAW_TABLE_1_SCHEMA_VERSION, e);
        }
    }

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
        // P1-251: schema label must be positive — 0/-1 would persist corrupt.
        if (b.schemaVersion <= 0) {
            throw new IllegalArgumentException(
                    "schemaVersion must be positive, got " + b.schemaVersion);
        }
        // P1-089: monetary invariants hold for EVERY classification — quotes
        // may be 0 (VALID_NON_TRADE), never negative; change is a pct, always
        // finite. Corrupt values previously reached the Fluss append path.
        if (b.lastPricePaise < 0 || b.ohlcOpenPaise < 0 || b.ohlcHighPaise < 0
                || b.ohlcLowPaise < 0 || b.ohlcClosePaise < 0 || b.averagePricePaise < 0) {
            throw new IllegalArgumentException("price fields must be >= 0");
        }
        if (b.lastQty < 0 || b.volume < 0 || b.openInterest < 0) {
            throw new IllegalArgumentException("lastQty/volume/openInterest must be >= 0");
        }
        if (!Double.isFinite(b.change)) {
            throw new IllegalArgumentException("change must be finite, got " + b.change);
        }
        if (b.validity == ValidityClassification.VALID_TRADE && b.lastPricePaise <= 0) {
            throw new IllegalArgumentException("VALID_TRADE requires lastPricePaise > 0");
        }
        // v4: the broker is a trust boundary, so the new feed-sourced values are
        // validated like the money fields above. Absent (null) values are fine;
        // present ones may not be negative — except changeFlag and imbalanceQty,
        // which are signed by nature (a flag with unknown semantics; a buy/sell
        // imbalance where negative means more sell than buy).
        if (b.totalBuyQty < 0 || b.totalSellQty < 0) {
            throw new IllegalArgumentException("totalBuyQty/totalSellQty must be >= 0");
        }
        requireNonNegative(b.volumeDelta, "volumeDelta");
        requireNonNegative(b.atv, "atv");
        requireNonNegative(b.btv, "btv");
        requireNonNegative(b.oiDayHigh, "oiDayHigh");
        requireNonNegative(b.oiDayLow, "oiDayLow");
        requireNonNegative(b.lowerLimitPaise, "lowerLimitPaise");
        requireNonNegative(b.upperLimitPaise, "upperLimitPaise");
        requireNonNegative(b.indicativeClosePaise, "indicativeClosePaise");
        requireNonNegative(b.refPricePaise, "refPricePaise");
        requireNonNegative(b.lastTradedTimeMs, "lastTradedTimeMs");
        requireDepth(b.bidPx, "bidPx");
        requireDepth(b.bidQty, "bidQty");
        requireDepth(b.bidOrd, "bidOrd");
        requireDepth(b.askPx, "askPx");
        requireDepth(b.askQty, "askQty");
        requireDepth(b.askOrd, "askOrd");

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
        this.lastQty = b.lastQty;
        this.volume = b.volume;
        this.change = b.change;
        this.ohlcOpenPaise = b.ohlcOpenPaise;
        this.ohlcHighPaise = b.ohlcHighPaise;
        this.ohlcLowPaise = b.ohlcLowPaise;
        this.ohlcClosePaise = b.ohlcClosePaise;
        this.averagePricePaise = b.averagePricePaise;
        this.openInterest = b.openInterest;
        this.totalBuyQty = b.totalBuyQty;
        this.totalSellQty = b.totalSellQty;
        this.volumeDelta = b.volumeDelta;
        this.atv = b.atv;
        this.btv = b.btv;
        this.changeFlag = b.changeFlag;
        this.oiDayHigh = b.oiDayHigh;
        this.oiDayLow = b.oiDayLow;
        this.imbalanceQty = b.imbalanceQty;
        this.indicativeClosePaise = b.indicativeClosePaise;
        this.refPricePaise = b.refPricePaise;
        this.lowerLimitPaise = b.lowerLimitPaise;
        this.upperLimitPaise = b.upperLimitPaise;
        this.lastTradedTimeMs = b.lastTradedTimeMs;
        this.bidPx = b.bidPx;
        this.bidQty = b.bidQty;
        this.bidOrd = b.bidOrd;
        this.askPx = b.askPx;
        this.askQty = b.askQty;
        this.askOrd = b.askOrd;
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

    private static void requireNonNegative(Long v, String name) {
        if (v != null && v < 0) {
            throw new IllegalArgumentException(name + " must be >= 0, got " + v);
        }
    }

    /** The ladder is fixed-width on the wire, so anything else is a decode bug. */
    private static void requireDepth(long[] v, String name) {
        if (v == null || v.length != DEPTH_LEVELS) {
            throw new IllegalArgumentException(
                    name + " must have " + DEPTH_LEVELS + " levels, got "
                            + (v == null ? "null" : String.valueOf(v.length)));
        }
        for (long level : v) {
            if (level < 0) {
                throw new IllegalArgumentException(name + " levels must be >= 0, got " + level);
            }
        }
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
    /** Local time of append ack, or {@code null} until the Fluss ack completes. */
    public Instant appendAckTs() { return appendAckTs; }
    public long lastPricePaise() { return lastPricePaise; }
    public long lastQty() { return lastQty; }
    public long volume() { return volume; }
    public double change() { return change; }
    public long ohlcOpenPaise() { return ohlcOpenPaise; }
    public long ohlcHighPaise() { return ohlcHighPaise; }
    public long ohlcLowPaise() { return ohlcLowPaise; }
    public long ohlcClosePaise() { return ohlcClosePaise; }
    public long averagePricePaise() { return averagePricePaise; }
    public long openInterest() { return openInterest; }
    public long totalBuyQty() { return totalBuyQty; }
    public long totalSellQty() { return totalSellQty; }
    /** @return traded qty since the previous tick, or null when the baseline is unknown. */
    public Long volumeDelta() { return volumeDelta; }
    public Long atv() { return atv; }
    public Long btv() { return btv; }
    public Long changeFlag() { return changeFlag; }
    public Long oiDayHigh() { return oiDayHigh; }
    public Long oiDayLow() { return oiDayLow; }
    public Long imbalanceQty() { return imbalanceQty; }
    public Long indicativeClosePaise() { return indicativeClosePaise; }
    public Long refPricePaise() { return refPricePaise; }
    public Long lowerLimitPaise() { return lowerLimitPaise; }
    public Long upperLimitPaise() { return upperLimitPaise; }
    public Long lastTradedTimeMs() { return lastTradedTimeMs; }
    /**
     * Depth ladders, index 0 = level 1. Returned by reference: this runs once per
     * tick and the arrays are never mutated after construction (same reason
     * {@link com.trading.common.schema.RawTableSchema#COLUMNS} is immutable).
     */
    public long[] bidPx() { return bidPx; }
    public long[] bidQty() { return bidQty; }
    public long[] bidOrd() { return bidOrd; }
    public long[] askPx() { return askPx; }
    public long[] askQty() { return askQty; }
    public long[] askOrd() { return askOrd; }
    public String eventFingerprint() { return eventFingerprint; }
    public int fingerprintVersion() { return fingerprintVersion; }
    public String connectionId() { return connectionId; }
    public long connectionEpoch() { return connectionEpoch; }
    public String instanceId() { return instanceId; }
    public int schemaVersion() { return schemaVersion; }

    public boolean isTradeEligible() {
        return validity == ValidityClassification.VALID_TRADE;
    }

    @Override
    public String toString() {
        return "TickPacket{token=" + instrumentToken + ", sym=" + tradingSymbol
                + ", pricePaise=" + lastPricePaise + ", ltq=" + lastQty + ", vol=" + volume
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
        long lastQty;
        long volume;
        double change;
        long ohlcOpenPaise, ohlcHighPaise, ohlcLowPaise, ohlcClosePaise;
        long averagePricePaise;
        long openInterest;
        // v4. Zeroed ladders by default, so a packet built without depth (the LTP
        // path, and every pre-v4 test) carries an explicit empty ladder rather than
        // failing construction.
        long totalBuyQty, totalSellQty;
        Long volumeDelta, atv, btv, changeFlag, oiDayHigh, oiDayLow;
        Long imbalanceQty, indicativeClosePaise, refPricePaise;
        Long lowerLimitPaise, upperLimitPaise, lastTradedTimeMs;
        long[] bidPx = new long[DEPTH_LEVELS];
        long[] bidQty = new long[DEPTH_LEVELS];
        long[] bidOrd = new long[DEPTH_LEVELS];
        long[] askPx = new long[DEPTH_LEVELS];
        long[] askQty = new long[DEPTH_LEVELS];
        long[] askOrd = new long[DEPTH_LEVELS];
        String eventFingerprint;
        int fingerprintVersion = 1;
        String connectionId;
        long connectionEpoch;
        String instanceId;
        /**
         * Default = shared raw_table_1 contract version; IngestionService no longer
         * overrides it, so the persisted label cannot drift from the consumer default.
         */
        int schemaVersion = DEFAULT_SCHEMA_VERSION;

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
        public Builder lastQty(long v) { this.lastQty = v; return this; }
        public Builder change(double v) { this.change = v; return this; }
        public Builder ohlcOpenPaise(long v) { this.ohlcOpenPaise = v; return this; }
        public Builder ohlcHighPaise(long v) { this.ohlcHighPaise = v; return this; }
        public Builder ohlcLowPaise(long v) { this.ohlcLowPaise = v; return this; }
        public Builder ohlcClosePaise(long v) { this.ohlcClosePaise = v; return this; }
        public Builder averagePricePaise(long v) { this.averagePricePaise = v; return this; }
        public Builder openInterest(long v) { this.openInterest = v; return this; }
        public Builder totalBuyQty(long v) { this.totalBuyQty = v; return this; }
        public Builder totalSellQty(long v) { this.totalSellQty = v; return this; }
        public Builder volumeDelta(Long v) { this.volumeDelta = v; return this; }
        public Builder atv(Long v) { this.atv = v; return this; }
        public Builder btv(Long v) { this.btv = v; return this; }
        public Builder changeFlag(Long v) { this.changeFlag = v; return this; }
        public Builder oiDayHigh(Long v) { this.oiDayHigh = v; return this; }
        public Builder oiDayLow(Long v) { this.oiDayLow = v; return this; }
        public Builder imbalanceQty(Long v) { this.imbalanceQty = v; return this; }
        public Builder indicativeClosePaise(Long v) { this.indicativeClosePaise = v; return this; }
        public Builder refPricePaise(Long v) { this.refPricePaise = v; return this; }
        /** 0 cannot be a price: the band is either published or absent. */
        public Builder lowerLimitPaise(long v) { this.lowerLimitPaise = v == 0 ? null : v; return this; }
        public Builder upperLimitPaise(long v) { this.upperLimitPaise = v == 0 ? null : v; return this; }
        /** 0 cannot be an epoch ms: the bridge sends 0 when the unit was implausible. */
        public Builder lastTradedTimeMs(long v) { this.lastTradedTimeMs = v == 0 ? null : v; return this; }
        /**
         * Sets all six ladders at once (level 1 at index 0) so a caller cannot
         * set the bid price ladder and forget the bid size ladder.
         */
        public Builder depth(long[] bidPx, long[] bidQty, long[] bidOrd,
                            long[] askPx, long[] askQty, long[] askOrd) {
            this.bidPx = bidPx;
            this.bidQty = bidQty;
            this.bidOrd = bidOrd;
            this.askPx = askPx;
            this.askQty = askQty;
            this.askOrd = askOrd;
            return this;
        }
        public Builder eventFingerprint(String v) { this.eventFingerprint = v; return this; }
        public Builder fingerprintVersion(int v) { this.fingerprintVersion = v; return this; }
        public Builder connectionId(String v) { this.connectionId = v; return this; }
        public Builder connectionEpoch(long v) { this.connectionEpoch = v; return this; }
        public Builder instanceId(String v) { this.instanceId = v; return this; }
        public Builder schemaVersion(int v) { this.schemaVersion = v; return this; }

        public TickPacket build() { return new TickPacket(this); }
    }
}
