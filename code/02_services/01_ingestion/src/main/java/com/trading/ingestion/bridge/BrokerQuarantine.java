package com.trading.ingestion.bridge;

import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Set;

/** Immutable, validated broker-data quarantine record from the bridge. */
public record BrokerQuarantine(
        int contractVersion,
        String slotId,
        String connectionId,
        long connectionEpoch,
        long token,
        String reason,
        byte[] rawPayload,
        String payloadHash,
        long detectedTsMs) {

    public static final int CONTRACT_VERSION = 2;
    private static final Set<String> REASONS = Set.of(
            "MALFORMED_JSON", "INVALID_SCHEMA", "INVALID_VALUES", "HASH_MISMATCH",
            "FUTURE_BROKER_TIMESTAMP", "STALE_BROKER_TIMESTAMP");

    public BrokerQuarantine {
        // R-207: the record is documented Immutable — the byte[] component
        // must be defensive-copied so a caller cannot mutate it after the
        // hash was verified.
        if (rawPayload != null) {
            rawPayload = rawPayload.clone();
        }
        if (contractVersion != CONTRACT_VERSION) throw new IllegalArgumentException("unsupported contract version");
        if (slotId == null || slotId.isBlank()) throw new IllegalArgumentException("slot_id is required");
        if (connectionId == null || connectionId.isBlank()) throw new IllegalArgumentException("connection_id is required");
        if (connectionEpoch <= 0) throw new IllegalArgumentException("connection_epoch must be positive");
        // P1-064: 0 = unknown token (R-010 convention, like ack_ts 0 = unknown).
        // Bridge-quarantine ticks are undecodable by definition - no token exists.
        // The old mapping stuffed receivedTsMs here, corrupting the column.
        if (token < 0) throw new IllegalArgumentException("token must be non-negative");
        if (reason == null || !REASONS.contains(reason)) throw new IllegalArgumentException("unknown quarantine reason");
        if (rawPayload == null || rawPayload.length == 0) throw new IllegalArgumentException("raw_payload is required");
        if (payloadHash == null || !payloadHash.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("payload_hash must be lowercase SHA-256");
        String expected = sha256(rawPayload);
        if (!expected.equals(payloadHash)) throw new IllegalArgumentException("payload hash mismatch");
        if (detectedTsMs <= 0) throw new IllegalArgumentException("detected_ts_ms must be positive");
    }

    /** R-207: expose the payload via a fresh copy. */
    @Override
    public byte[] rawPayload() {
        return rawPayload == null ? null : rawPayload.clone();
    }

    private static String sha256(byte[] payload) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload));
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
