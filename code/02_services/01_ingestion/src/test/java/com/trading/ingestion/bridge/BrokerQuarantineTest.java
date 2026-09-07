package com.trading.ingestion.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ING-UNIT-009: the {@link BrokerQuarantine} record contract (R-207 immutability,
 * field validation).
 *
 * <p>Migrated off the removed NDJSON {@code BridgeEventParser}: the proto transport
 * builds the same record in {@code IngestionService.handleControlRecord}, so the
 * record's own validation is asserted directly here.
 */
@DisplayName("ING-UNIT-009: BrokerQuarantine record contract")
class BrokerQuarantineTest {

    private static BrokerQuarantine record(String reason, byte[] payload, String payloadHash) {
        return new BrokerQuarantine(
                BrokerQuarantine.CONTRACT_VERSION,
                "hft-0", "hft-0", 1L, 3045L, reason, payload, payloadHash, 1700000000000L);
    }

    @Test
    @DisplayName("valid record keeps token + hash and the exact payload bytes")
    void keepsTokenHashAndPayload() throws Exception {
        byte[] payload = "bad-frame".getBytes(StandardCharsets.UTF_8);
        String hash = sha256Hex(payload);
        BrokerQuarantine parsed = record("HASH_MISMATCH", payload, hash);
        assertEquals(3045, parsed.token());
        assertEquals(hash, parsed.payloadHash());
        assertArrayEquals(payload, parsed.rawPayload());
    }

    @Test
    @DisplayName("payload hash mismatch is rejected (fail-closed)")
    void rejectsHashMismatch() {
        byte[] payload = "bad-frame".getBytes(StandardCharsets.UTF_8);
        assertThrows(IllegalArgumentException.class,
                () -> record("HASH_MISMATCH", payload, "0".repeat(64)));
    }

    @Test
    @DisplayName("required-field validation: reason vocabulary, ids, epoch, token, payload, version")
    void rejectsInvalidFields() throws Exception {
        byte[] payload = "bad-frame".getBytes(StandardCharsets.UTF_8);
        String hash = sha256Hex(payload);
        // Unknown quarantine reason (the vocabulary is the bridge↔Java contract).
        assertThrows(IllegalArgumentException.class, () -> record("NOT_A_REASON", payload, hash));
        // Missing identity fields.
        assertThrows(IllegalArgumentException.class,
                () -> new BrokerQuarantine(BrokerQuarantine.CONTRACT_VERSION, "", "hft-0",
                        1L, 3045L, "HASH_MISMATCH", payload, hash, 1700000000000L));
        // Non-positive epoch / token.
        assertThrows(IllegalArgumentException.class,
                () -> new BrokerQuarantine(BrokerQuarantine.CONTRACT_VERSION, "hft-0", "hft-0",
                        0L, 3045L, "HASH_MISMATCH", payload, hash, 1700000000000L));
        // P1-064: token 0 is now legal (unknown/undecodable, R-010 convention) -
        // negatives are still rejected.
        assertThrows(IllegalArgumentException.class,
                () -> new BrokerQuarantine(BrokerQuarantine.CONTRACT_VERSION, "hft-0", "hft-0",
                        1L, -1L, "HASH_MISMATCH", payload, hash, 1700000000000L));
        // Empty payload.
        assertThrows(IllegalArgumentException.class, () -> record("HASH_MISMATCH", new byte[0], hash));
        // Unsupported contract version.
        assertThrows(IllegalArgumentException.class,
                () -> new BrokerQuarantine(1, "hft-0", "hft-0",
                        1L, 3045L, "HASH_MISMATCH", payload, hash, 1700000000000L));
    }

    @Test
    @DisplayName("R-207: rawPayload is defensively copied — a caller cannot mutate the record")
    void rawPayloadIsDefensivelyCopied() throws Exception {
        byte[] payload = "bad-frame".getBytes(StandardCharsets.UTF_8);
        String hash = sha256Hex(payload);
        BrokerQuarantine parsed = record("HASH_MISMATCH", payload, hash);
        // Mutating the caller's original array must not change the record.
        payload[0] = 'X';
        assertArrayEquals("bad-frame".getBytes(StandardCharsets.UTF_8), parsed.rawPayload(),
                "record must be immutable with respect to the payload bytes");
        // Mutating the returned array must not change subsequent reads either.
        parsed.rawPayload()[1] = 'Y';
        assertArrayEquals("bad-frame".getBytes(StandardCharsets.UTF_8), parsed.rawPayload(),
                "rawPayload() must return a fresh copy");
    }

    private static String sha256Hex(byte[] data) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
    }
}
