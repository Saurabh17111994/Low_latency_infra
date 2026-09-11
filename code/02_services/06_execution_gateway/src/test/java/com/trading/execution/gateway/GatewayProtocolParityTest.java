package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

/**
 * Cross-language HMAC envelope parity (Java ↔ Rust).
 *
 * <p>Fixed deterministic fixture mirrors Rust gateway_protocol::tests::PARITY_*.
 * The payload key order zulu,alpha,qty is intentionally non-alphabetical — sorted
 * order would be alpha,qty,zulu. A sorting divergence (BTreeMap, Jackson
 * ORDER_MAP_ENTRIES_BY_KEYS) would produce a different payload_json, payload_hash,
 * canonical string and HMAC, breaking byte equality.
 */
class GatewayProtocolParityTest {

    private static final String FIXED_SECRET = "parity-test-secret-v1-2026-08-24";
    private static final String PROTOCOL_VERSION = "execution-gateway.v1";
    private static final String MESSAGE_TYPE = "EXECUTION_INTENT";
    private static final String REQUEST_ID = "parity-req-0001";
    private static final String ACCOUNT_SCOPE_ID = "parity-acct-001";
    private static final String EXECUTION_PARTITION_ID = "parity-partition-1";
    private static final long GATE_EPOCH = 42L;
    private static final String FENCE_TOKEN = "parity-fence-token-xyz";
    private static final long DEADLINE_EPOCH_MS = 2_000_000_000_000L;
    private static final long NOW_MS = 1_000_000_000_000L;
    // Canonical payload bytes — must be byte-identical to Rust PARITY_PAYLOAD_JSON.
    static final String FIXED_PAYLOAD_JSON = "{\"zulu\":\"z\",\"alpha\":\"a\",\"qty\":100}";
    static final String FIXED_PAYLOAD_HASH = "bceb5c2c5139f412f53bf7d27178ea551f68d333566d52485fc5d705e3d06e71";
    static final String FIXED_AUTH = "ae9003e44be67518aafd38be98d9cb6132120890925bc1dc713fc2708fa0e9ba";
    // Java-produced token == Rust-produced token (byte-identical). This is the shared fixture.
    static final String EXPECTED_ENVELOPE_JSON =
            "{\"protocol_version\":\"execution-gateway.v1\",\"message_type\":\"EXECUTION_INTENT\",\"request_id\":\"parity-req-0001\",\"account_scope_id\":\"parity-acct-001\",\"execution_partition_id\":\"parity-partition-1\",\"payload_hash\":\"bceb5c2c5139f412f53bf7d27178ea551f68d333566d52485fc5d705e3d06e71\",\"gate_epoch\":42,\"fence_token\":\"parity-fence-token-xyz\",\"deadline_epoch_ms\":2000000000000,\"payload\":{\"zulu\":\"z\",\"alpha\":\"a\",\"qty\":100},\"authentication\":\"ae9003e44be67518aafd38be98d9cb6132120890925bc1dc713fc2708fa0e9ba\"}";

    private ObjectNode fixedPayload(ObjectMapper m) {
        ObjectNode p = m.createObjectNode();
        p.put("zulu", "z");
        p.put("alpha", "a");
        p.put("qty", 100);
        return p;
    }

    private GatewayProtocol.Envelope fixedEnvelope(ObjectMapper m) throws Exception {
        ObjectNode payload = fixedPayload(m);
        // payload_hash must be computed from the exact canonical payload bytes
        String hash = GatewayProtocol.sha256(m.writeValueAsBytes(payload));
        return new GatewayProtocol.Envelope(
                PROTOCOL_VERSION,
                MESSAGE_TYPE,
                REQUEST_ID,
                ACCOUNT_SCOPE_ID,
                EXECUTION_PARTITION_ID,
                hash,
                GATE_EPOCH,
                FENCE_TOKEN,
                DEADLINE_EPOCH_MS,
                payload,
                null);
    }

    @Test
    void payloadSerializationIsByteIdenticalToRust() throws Exception {
        ObjectMapper m = new ObjectMapper();
        ObjectNode payload = fixedPayload(m);
        String json = m.writeValueAsString(payload);
        assertThat(json)
                .as("payload JSON bytes must be byte-identical to Rust fixture (Jackson preserves insertion order)")
                .isEqualTo(FIXED_PAYLOAD_JSON);
        // also verify hash
        String hash = GatewayProtocol.sha256(m.writeValueAsBytes(payload));
        assertThat(hash).isEqualTo(FIXED_PAYLOAD_HASH);
        // sorted would differ — proof that order matters
        String sorted = "{\"alpha\":\"a\",\"qty\":100,\"zulu\":\"z\"}";
        assertThat(json).isNotEqualTo(sorted);
    }

    @Test
    void encodeProducesByteIdenticalEnvelopeToRustFixture() throws Exception {
        ObjectMapper m = new ObjectMapper();
        GatewayProtocol p = new GatewayProtocol(FIXED_SECRET);
        GatewayProtocol.Envelope e = fixedEnvelope(m);
        String encoded = p.encode(e);
        assertThat(encoded)
                .as("Java encode must be byte-identical to Rust fixture (canonical + HMAC match)")
                .isEqualTo(EXPECTED_ENVELOPE_JSON);
    }

    @Test
    void verifyRustProducedTokenAccepted() {
        // EXPECTED_ENVELOPE_JSON is documented as both Java and Rust output; verifying it
        // proves cross-language acceptance. This simulates Rust-produced token verified by Java.
        GatewayProtocol p = new GatewayProtocol(FIXED_SECRET);
        GatewayProtocol.Verification v = p.verify(EXPECTED_ENVELOPE_JSON, PROTOCOL_VERSION, NOW_MS);
        assertThat(v.accepted()).as("Java must accept Rust-signed parity fixture, reason: " + v.reason()).isTrue();
        assertThat(v.envelope().requestId()).isEqualTo(REQUEST_ID);
        assertThat(v.envelope().payloadHash()).isEqualTo(FIXED_PAYLOAD_HASH);
        // payload round-trip bytes must remain identical
        try {
            ObjectMapper m = new ObjectMapper();
            String payloadJson = m.writeValueAsString(v.envelope().payload());
            assertThat(payloadJson).isEqualTo(FIXED_PAYLOAD_JSON);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    void verifyTamperedPayloadRejected() throws Exception {
        GatewayProtocol p = new GatewayProtocol(FIXED_SECRET);
        // 1) naive payload tamper without updating auth/hash -> authentication failed
        ObjectMapper m = new ObjectMapper();
        com.fasterxml.jackson.databind.JsonNode node = m.readTree(EXPECTED_ENVELOPE_JSON);
        ((ObjectNode) node).set("payload", m.createObjectNode().put("zulu", "tampered").put("alpha", "a").put("qty", 100));
        String tampered = m.writeValueAsString(node);
        GatewayProtocol.Verification v = p.verify(tampered, PROTOCOL_VERSION, NOW_MS);
        assertThat(v.accepted()).as("tampered payload must be rejected").isFalse();
        assertThat(v.reason()).isIn("authentication failed", "payload hash mismatch");

        // 2) hash-mismatch path: valid auth for new payload but stale payload_hash -> payload hash mismatch
        ObjectNode tamperedPayload = m.createObjectNode().put("zulu", "tampered").put("alpha", "a").put("qty", 100);
        String tamperedJson = m.writeValueAsString(tamperedPayload);
        String tamperedHash = GatewayProtocol.sha256(m.writeValueAsBytes(tamperedPayload));
        // encode with correct hash, then overwrite hash with old value and re-sign with stale canonical
        GatewayProtocol.Envelope correct = new GatewayProtocol.Envelope(
                PROTOCOL_VERSION, MESSAGE_TYPE, REQUEST_ID, ACCOUNT_SCOPE_ID, EXECUTION_PARTITION_ID,
                tamperedHash, GATE_EPOCH, FENCE_TOKEN, DEADLINE_EPOCH_MS, tamperedPayload, null);
        String correctEncoded = p.encode(correct);
        com.fasterxml.jackson.databind.JsonNode correctNode = m.readTree(correctEncoded);
        // 2) hash-mismatch path: stale payload_hash + stale auth over the new
        // payload_json (P3-296: encode refuses to sign a mismatched pair, so the
        // stale-auth bytes are crafted via raw HMAC here, not via encode).
        // canonical layout: version\ntype\nrequest\nscope\npartition\nhash\nepoch\nfence\ndeadline\npayload_json
        String tamperedPayloadJson = m.writeValueAsString(tamperedPayload);
        String staleCanonical = String.join("\n", PROTOCOL_VERSION, MESSAGE_TYPE, REQUEST_ID,
                ACCOUNT_SCOPE_ID, EXECUTION_PARTITION_ID, FIXED_PAYLOAD_HASH,
                Long.toString(GATE_EPOCH), FENCE_TOKEN, Long.toString(DEADLINE_EPOCH_MS),
                tamperedPayloadJson);
        javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
        mac.init(new javax.crypto.spec.SecretKeySpec(
                FIXED_SECRET.getBytes(java.nio.charset.StandardCharsets.UTF_8), "HmacSHA256"));
        String staleAuth = java.util.HexFormat.of().formatHex(
                mac.doFinal(staleCanonical.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        ((ObjectNode) correctNode).put("payload_hash", FIXED_PAYLOAD_HASH);
        ((ObjectNode) correctNode).put("authentication", staleAuth);
        String mismatchJson = m.writeValueAsString(correctNode);
        GatewayProtocol.Verification v2 = p.verify(mismatchJson, PROTOCOL_VERSION, NOW_MS);
        assertThat(v2.accepted()).isFalse();
        assertThat(v2.reason()).isEqualTo("payload hash mismatch");
    }

    @Test
    void selfEncodeThenVerifyRoundTrip() throws Exception {
        ObjectMapper m = new ObjectMapper();
        GatewayProtocol p = new GatewayProtocol(FIXED_SECRET);
        GatewayProtocol.Envelope e = fixedEnvelope(m);
        String encoded = p.encode(e);
        GatewayProtocol.Verification v = p.verify(encoded, PROTOCOL_VERSION, NOW_MS);
        assertThat(v.accepted()).as("self-encoded fixture must verify, reason: " + v.reason()).isTrue();
        assertThat(v.envelope().protocolVersion()).isEqualTo(PROTOCOL_VERSION);
        assertThat(v.envelope().messageType()).isEqualTo(MESSAGE_TYPE);
        assertThat(v.envelope().accountScopeId()).isEqualTo(ACCOUNT_SCOPE_ID);
        assertThat(v.envelope().executionPartitionId()).isEqualTo(EXECUTION_PARTITION_ID);
        assertThat(v.envelope().gateEpoch()).isEqualTo(GATE_EPOCH);
        assertThat(v.envelope().fenceToken()).isEqualTo(FENCE_TOKEN);
        assertThat(v.envelope().deadlineEpochMs()).isEqualTo(DEADLINE_EPOCH_MS);
        // auth must equal pre-computed fixture
        com.fasterxml.jackson.databind.JsonNode parsed = m.readTree(encoded);
        assertThat(parsed.get("authentication").asText()).isEqualTo(FIXED_AUTH);
    }
}
