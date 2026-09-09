package com.trading.common.model;

import java.util.Arrays;
import java.util.Objects;

/**
 * Normalized market tick — what the Ingestion writes to raw_table_1.
 * All prices in integer paise (₹1 = 100 paise).
 * Original broker bytes preserved in rawPayload.
 */
public record MarketTick(
    String eventFingerprint,
    String fingerprintVersion,
    String connectionId,
    long connectionEpoch,
    long instrumentToken,
    String exchange,
    String symbol,
    String instrumentType,
    Long strikePaise,            // null for non-options
    Long expiry,
    String optionType,
    long eventTime,
    long ingestTs,
    long ackTs,
    String tickType,
    Long lastPricePaise,
    Long lastQty,
    Long bidPricePaise,
    Long bidQty,
    Long askPricePaise,
    Long askQty,
    byte[] rawPayload,
    String payloadHash,
    String decoderVersion,
    String protocolVersion,
    String validityState,
    String validityReason,
    String schemaVersion
) {
    /**
     * Compact constructor — defensive-copies the mutable array component
     * (R-163) so records compare by value, not by array reference identity.
     */
    public MarketTick {
        if (rawPayload != null) {
            rawPayload = rawPayload.clone();
        }
    }

    /**
     * R-163: expose the raw payload via a defensive copy so callers cannot
     * mutate the record's internal array.
     */
    @Override
    public byte[] rawPayload() {
        return rawPayload == null ? null : rawPayload.clone();
    }

    /** Zero-copy view for internal hot paths (Fluss/quarantine) — caller must not mutate. */
    public byte[] rawPayloadUnsafe() {
        return rawPayload;
    }

    public boolean isTrade() { return "TRADE".equals(tickType); }

    /**
     * R-128: the ingestion pipeline writes the {@code ValidityClassification}
     * enum name (VALID_TRADE / VALID_NON_TRADE) into {@code validity_state};
     * the literal "VALID" never occurs. Fail closed on any other value —
     * a future VALID-prefixed state must not leak into signal logic.
     */
    public boolean isValid() {
        return "VALID_TRADE".equals(validityState) || "VALID_NON_TRADE".equals(validityState);
    }

    public boolean isValidTrade() { return isTrade() && isValid(); }

    // R-163: value semantics over every field including the payload bytes.

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MarketTick that)) return false;
        return connectionEpoch == that.connectionEpoch
                && instrumentToken == that.instrumentToken
                && eventTime == that.eventTime
                && ingestTs == that.ingestTs
                && ackTs == that.ackTs
                && Objects.equals(eventFingerprint, that.eventFingerprint)
                && Objects.equals(fingerprintVersion, that.fingerprintVersion)
                && Objects.equals(connectionId, that.connectionId)
                && Objects.equals(exchange, that.exchange)
                && Objects.equals(symbol, that.symbol)
                && Objects.equals(instrumentType, that.instrumentType)
                && Objects.equals(strikePaise, that.strikePaise)
                && Objects.equals(expiry, that.expiry)
                && Objects.equals(optionType, that.optionType)
                && Objects.equals(tickType, that.tickType)
                && Objects.equals(lastPricePaise, that.lastPricePaise)
                && Objects.equals(lastQty, that.lastQty)
                && Objects.equals(bidPricePaise, that.bidPricePaise)
                && Objects.equals(bidQty, that.bidQty)
                && Objects.equals(askPricePaise, that.askPricePaise)
                && Objects.equals(askQty, that.askQty)
                && Objects.equals(payloadHash, that.payloadHash)
                && Objects.equals(decoderVersion, that.decoderVersion)
                && Objects.equals(protocolVersion, that.protocolVersion)
                && Objects.equals(validityState, that.validityState)
                && Objects.equals(validityReason, that.validityReason)
                && Objects.equals(schemaVersion, that.schemaVersion)
                && Arrays.equals(rawPayload, that.rawPayload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventFingerprint, fingerprintVersion, connectionId,
                connectionEpoch, instrumentToken, exchange, symbol, instrumentType,
                strikePaise, expiry, optionType, eventTime, ingestTs, ackTs, tickType,
                lastPricePaise, lastQty, bidPricePaise, bidQty, askPricePaise, askQty,
                payloadHash, decoderVersion, protocolVersion, validityState,
                validityReason, schemaVersion)
                ^ Arrays.hashCode(rawPayload);
    }

    @Override
    public String toString() {
        return "MarketTick{fp=" + eventFingerprint + ", token=" + instrumentToken
                + ", ts=" + eventTime + ", state=" + validityState + "}";
    }
}
