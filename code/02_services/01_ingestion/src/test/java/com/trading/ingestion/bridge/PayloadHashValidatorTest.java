package com.trading.ingestion.bridge;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.MessageDigest;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Phase 2 — packet-byte preservation (plan §Tick / §Data Flow).
 *
 * <p>Verifies {@code PayloadHashValidator}: a tick whose Base64
 * {@code raw_payload} does not hash to the bridge-provided {@code payload_hash}
 * must be rejected, while a valid packet passes through untouched.
 *
 * <p>R-248: the result is now returned directly (no caller-owned out-array).
 */
@DisplayName("ING-UNIT-010: raw_payload hash validation")
class PayloadHashValidatorTest {

    private static String b64(byte[] packet) {
        return Base64.getEncoder().encodeToString(packet);
    }

    private static String sha256Hex(byte[] data) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(data));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    @DisplayName("P1-076: oversized raw_payload rejected BEFORE decode+hash (MALFORMED, not HASH_MISMATCH)")
    void rejectsOversizedPayloadBeforeHash() {
        byte[] big = new byte[4096];
        new Random(7).nextBytes(big);
        // Well-formed hex hash of OTHER content: passes the hash gate, so the
        // old code decoded all 4KiB, hashed, and returned HASH_MISMATCH.
        assertEquals(PayloadHashValidator.Result.MALFORMED_PAYLOAD,
                PayloadHashValidator.validate(b64(big), sha256Hex(new byte[]{1})));
    }

    @Test
    @DisplayName("P1-076: largest broker packet (540B response frame) still accepted")
    void acceptsLargestBrokerPacket() {
        byte[] packet = new byte[PayloadHashValidator.MAX_PACKET_BYTES];
        new Random(11).nextBytes(packet);
        assertEquals(PayloadHashValidator.Result.VALID,
                PayloadHashValidator.validate(b64(packet), sha256Hex(packet)));
        assertArrayEquals(packet, PayloadHashValidator.decodeValid(b64(packet), sha256Hex(packet)),
                "decoded bytes must equal the original packet exactly");
    }

    @Test
    @DisplayName("P1-076: one byte over the broker max is rejected even with a matching hash")
    void rejectsJustOverBrokerMax() {
        byte[] packet = new byte[PayloadHashValidator.MAX_PACKET_BYTES + 1];
        new Random(13).nextBytes(packet);
        // The hash MATCHES — the old code returned VALID (unbounded accept).
        assertEquals(PayloadHashValidator.Result.MALFORMED_PAYLOAD,
                PayloadHashValidator.validate(b64(packet), sha256Hex(packet)));
    }

    @Test
    @DisplayName("valid packet bytes pass")
    void validPayloadPasses() {
        byte[] packet = {1, 2, 3, 4, 5, 40, 0, 2, 0, 0};
        assertEquals(PayloadHashValidator.Result.VALID,
                PayloadHashValidator.validate(b64(packet), sha256Hex(packet)));
        assertArrayEquals(packet, PayloadHashValidator.decodeValid(
                b64(packet), sha256Hex(packet)));
    }

    @Test
    @DisplayName("hash mismatch is rejected")
    void hashMismatchRejected() {
        byte[] packet = {1, 2, 3, 4, 5};
        assertEquals(PayloadHashValidator.Result.HASH_MISMATCH,
                PayloadHashValidator.validate(b64(packet), sha256Hex(new byte[]{9, 9, 9})));
        assertNull(PayloadHashValidator.decodeValid(
                b64(packet), sha256Hex(new byte[]{9, 9, 9})));
    }

    @Test
    @DisplayName("missing payload_hash is rejected")
    void missingHashRejected() {
        assertEquals(PayloadHashValidator.Result.MALFORMED_HASH,
                PayloadHashValidator.validate(b64(new byte[]{1, 2, 3}), null));
    }

    @Test
    @DisplayName("non-hex payload_hash is rejected")
    void nonHexHashRejected() {
        assertEquals(PayloadHashValidator.Result.MALFORMED_HASH,
                PayloadHashValidator.validate(b64(new byte[]{1, 2, 3}), "not-a-hash"));
    }

    @Test
    @DisplayName("invalid Base64 is rejected")
    void invalidBase64Rejected() {
        assertEquals(PayloadHashValidator.Result.MALFORMED_PAYLOAD,
                PayloadHashValidator.validate("!!!not-base64!!!", sha256Hex(new byte[]{1})));
    }

    @Test
    @DisplayName("empty payload is rejected")
    void emptyPayloadRejected() {
        assertEquals(PayloadHashValidator.Result.MALFORMED_PAYLOAD,
                PayloadHashValidator.validate(b64(new byte[0]), sha256Hex(new byte[0])));
    }

    @Test
    @DisplayName("missing raw_payload is rejected")
    void missingPayloadRejected() {
        assertEquals(PayloadHashValidator.Result.MALFORMED_PAYLOAD,
                PayloadHashValidator.validate(null, sha256Hex(new byte[]{1})));
    }

    // ---- ING-UNIT-015: base64 form edge cases ----

    @Test
    @DisplayName("ING-UNIT-015: unpadded base64 is accepted (Java's basic decoder is padding-lenient)")
    void unpaddedBase64Accepted() {
        byte[] packet = {1, 2, 3, 4, 5, 6, 7, 8};
        String padded = b64(packet);
        String unpadded = padded.replaceAll("=+$", "");
        assertTrue(unpadded.length() < padded.length(), "unpadded form must actually drop the padding");
        assertEquals(PayloadHashValidator.Result.VALID,
                PayloadHashValidator.validate(unpadded, sha256Hex(packet)),
                "the padding-lenient decoder must accept the unpadded form");
        assertArrayEquals(packet, PayloadHashValidator.decodeValid(unpadded, sha256Hex(packet)));
    }

    @Test
    @DisplayName("ING-UNIT-015: URL-safe base64 is rejected (bridge emits the basic alphabet)")
    void urlSafeBase64Rejected() {
        byte[] packet = {(byte) 0xFB, 0x00, 0x01}; // decodes to '-' / '_' under URL-safe encoding
        String urlSafe = Base64.getUrlEncoder().encodeToString(packet);
        assertTrue(urlSafe.contains("-") || urlSafe.contains("_"),
                "URL-safe alphabet uses -/_ — the fixture must exercise it");
        assertEquals(PayloadHashValidator.Result.MALFORMED_PAYLOAD,
                PayloadHashValidator.validate(urlSafe, sha256Hex(packet)),
                "a URL-safe payload must be quarantined as MALFORMED_PAYLOAD, never silently dropped");
    }

    @Test
    @DisplayName("P1-076: 64KiB payload rejected even with a matching hash (no producer emits this)")
    void largeMultiFramePayloadRejected() {
        // P1-076 contract change (Batch-3 #25): raw_payload is EXACTLY one
        // broker packet (proto contract, market_data.pb.go; producer
        // ProtoEmitter.EmitTick passes one frame per tick; largest known
        // packet is the 540B response frame). A 64KiB "multi-frame packet"
        // has no producer — the old VALID verdict was unbounded acceptance.
        byte[] packet = new byte[65_536];
        new Random(42L).nextBytes(packet);
        assertEquals(PayloadHashValidator.Result.MALFORMED_PAYLOAD,
                PayloadHashValidator.validate(b64(packet), sha256Hex(packet)));
        assertNull(PayloadHashValidator.decodeValid(b64(packet), sha256Hex(packet)));
    }

    @Test
    @DisplayName("ING-UNIT-015: SHA-256 of the empty payload is a real digest — empty is a typed rejection, never dropped (R-186)")
    void emptyPayloadHashIsRealDigest() {
        // sha256("") is a real, well-known digest — an empty payload is NOT
        // a missing hash: the digest exists, the payload is still rejected
        // with a typed result (R-186: never silently dropped).
        assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                sha256Hex(new byte[0]), "SHA-256 of the empty input is a real digest");
        assertEquals(PayloadHashValidator.Result.MALFORMED_PAYLOAD,
                PayloadHashValidator.validate(b64(new byte[0]), sha256Hex(new byte[0])));
    }
}
