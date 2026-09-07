package com.trading.ingestion.bridge;

import java.security.MessageDigest;
import java.util.Base64;
import java.util.HexFormat;
import java.util.regex.Pattern;

/**
 * Validates a tick's original-bytes hash (plan §Tick / §Data Flow).
 *
 * <p>The Go bridge emits {@code raw_payload} = Base64 of the exact decompressed
 * broker packet bytes and {@code payload_hash} = SHA-256 hex of those bytes.
 * Decoded JSON must not replace those bytes. This helper decodes and verifies;
 * the caller decides quarantine/append side effects.
 *
 * <p>Pure logic — no Fluss or OTLP dependency, unit-testable in isolation.
 */
public final class PayloadHashValidator {

    /** Outcome of validation. */
    public enum Result {
        /** Packet bytes are valid — caller may append. */
        VALID,
        /** raw_payload missing, not valid Base64, or empty. */
        MALFORMED_PAYLOAD,
        /** payload_hash missing or not a lowercase SHA-256 hex digest. */
        MALFORMED_HASH,
        /** SHA-256 of decoded bytes does not match payload_hash. */
        HASH_MISMATCH
    }

    /**
     * R-215: precompiled once — {@code String.matches} compiled a fresh regex
     * Pattern on every tick of the HFT hot path.
     */
    private static final Pattern SHA256_HEX = Pattern.compile("[0-9a-f]{64}");

    // P1-076: reject oversized inputs BEFORE the Base64 decode + SHA-256 pass
    // (a MiB-scale raw_payload forced a large contiguous allocation + hash on
    // the hot path). Bound = the largest packet this broker emits:
    // hftSizeResponse = 540 (go-bridge/third_party/go-arrow/arrow/hft_stream.go:35;
    // tick frames are smaller — LTP 40 / full 196). Base64 ceiling =
    // ceil(540/3)*4 = 720 chars. NOTE: this validator currently has no
    // production caller (only tests) — the cap hardens the API before it is
    // wired into the append path (Batch-3 #25 ruling).
    static final int MAX_PACKET_BYTES = 540;
    static final int MAX_RAW_PAYLOAD_B64_LEN = 720;

    private PayloadHashValidator() {}

    /**
     * Decode and validate. On {@link Result#VALID} returns the exact packet
     * bytes; otherwise returns {@code null} (caller handles quarantine).
     *
     * <p>R-248: the previous signature took a caller-owned {@code Result[] out}
     * and dereferenced {@code out[0]} in every branch without a precondition —
     * a null/zero-length array would NPE/ArrayIndexOutOfBounds on the hot path.
     * The result is now returned directly.
     */
    public static Result validate(String rawPayloadB64, String payloadHash) {
        if (payloadHash == null || !SHA256_HEX.matcher(payloadHash).matches()) {
            return Result.MALFORMED_HASH;
        }
        if (rawPayloadB64 == null || rawPayloadB64.isBlank()) {
            return Result.MALFORMED_PAYLOAD;
        }
        if (rawPayloadB64.length() > MAX_RAW_PAYLOAD_B64_LEN) {
            return Result.MALFORMED_PAYLOAD;
        }
        byte[] packet;
        try {
            packet = Base64.getDecoder().decode(rawPayloadB64);
        } catch (IllegalArgumentException e) {
            return Result.MALFORMED_PAYLOAD;
        }
        if (packet.length == 0 || packet.length > MAX_PACKET_BYTES) {
            return Result.MALFORMED_PAYLOAD;
        }
        String actual = HexFormat.of().formatHex(sha256(packet));
        if (!actual.equals(payloadHash)) {
            return Result.HASH_MISMATCH;
        }
        return Result.VALID;
    }

    /**
     * Decode-and-validate returning the packet bytes on {@link Result#VALID}.
     * Kept for callers that need the decoded payload.
     *
     * <p>P1-238: single-pass — the old body called validate() (which decodes
     * internally) then decoded a SECOND time: 2x Base64 on every valid tick.
     * This body decodes once and returns the same buffer. Zero live gain
     * today (no production caller — test-only) but correct for the 60k gate.
     */
    public static byte[] decodeValid(String rawPayloadB64, String payloadHash) {
        if (payloadHash == null || !SHA256_HEX.matcher(payloadHash).matches()) {
            return null;
        }
        if (rawPayloadB64 == null || rawPayloadB64.isBlank()) {
            return null;
        }
        if (rawPayloadB64.length() > MAX_RAW_PAYLOAD_B64_LEN) {
            return null;
        }
        byte[] packet;
        try {
            packet = Base64.getDecoder().decode(rawPayloadB64);
        } catch (IllegalArgumentException e) {
            return null;
        }
        if (packet.length == 0 || packet.length > MAX_PACKET_BYTES) {
            return null;
        }
        String actual = HexFormat.of().formatHex(sha256(packet));
        if (!actual.equals(payloadHash)) {
            return null;
        }
        return packet;
    }

    /**
     * P1-239: per-thread digest — MessageDigest is not thread-safe but
     * getInstance does a provider lookup + alloc per call; at 50k ticks/s
     * that is 50k wasted allocs/s. ThreadLocal pays it once per thread.
     */
    private static final ThreadLocal<MessageDigest> SHA256_DIGEST = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    });

    private static byte[] sha256(byte[] data) {
        MessageDigest md = SHA256_DIGEST.get();
        md.reset();
        return md.digest(data);
    }
}
