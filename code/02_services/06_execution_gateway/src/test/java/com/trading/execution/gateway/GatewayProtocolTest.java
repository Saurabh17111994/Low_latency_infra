package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class GatewayProtocolTest {
    @Test void roundTripAuthenticatesAndChecksPayloadHash() throws Exception {
        GatewayProtocol p = new GatewayProtocol("test-secret");
        var payload = new ObjectMapper().createObjectNode().put("instruction_id", "i-1");
        var e = new GatewayProtocol.Envelope("execution-gateway.v1", "EXECUTION_EVENT", "r-1",
                "acct", "part", GatewayProtocol.sha256(new ObjectMapper().writeValueAsBytes(payload)),
                4, "fence-4", System.currentTimeMillis() + 10_000, payload, null);
        String json = p.encode(e);
        assertThat(p.verify(json, "execution-gateway.v1", System.currentTimeMillis()).accepted()).isTrue();
        assertThat(p.verify(json.replace("fence-4", "fence-5"), "execution-gateway.v1",
                System.currentTimeMillis()).reason()).isEqualTo("authentication failed");
    }

    @Test void rejectsExpiredAndWrongVersionEnvelopes() throws Exception {
        GatewayProtocol p = new GatewayProtocol("secret");
        var payload = new ObjectMapper().createObjectNode().put("x", 1);
        var e = new GatewayProtocol.Envelope("v1", "EXECUTION_INTENT", "r", "a", "p",
                GatewayProtocol.sha256(new ObjectMapper().writeValueAsBytes(payload)), 1, "f", 10, payload, null);
        String json = p.encode(e);
        assertThat(p.verify(json, "v2", 0).reason()).isEqualTo("unsupported version");
        assertThat(p.verify(json, "v1", 11).reason()).isEqualTo("deadline expired");
    }

    @Test void freshnessGatePresenceTypeTtlAndReplay() throws Exception {
        GatewayProtocol p = new GatewayProtocol("secret");
        ObjectMapper m = new ObjectMapper();
        var payload = m.createObjectNode().put("x", 1);
        long now = 1_000_000L;
        var base = new GatewayProtocol.Envelope("v1", "EXECUTION_INTENT", "fresh-1", "a", "p",
                GatewayProtocol.sha256(m.writeValueAsBytes(payload)), now, "f", now + 60_000, payload, null);
        String json = p.encode(base);
        // First accept.
        assertThat(p.verify(json, "v1", now).accepted()).isTrue();
        // P3-078 replay: same bytes again must not accept.
        assertThat(p.verify(json, "v1", now).reason()).isEqualTo("duplicate request");
        // Unknown message type.
        var bad = m.readTree(json);
        ((com.fasterxml.jackson.databind.node.ObjectNode) bad).put("message_type", "WIPE");
        // Re-sign is impossible without the secret path — any type change breaks auth first,
        // so craft via encode with the bad type directly.
        var badEnv = new GatewayProtocol.Envelope("v1", "WIPE", "fresh-2", "a", "p",
                GatewayProtocol.sha256(m.writeValueAsBytes(payload)), now, "f", now + 60_000, payload, null);
        assertThat(p.verify(p.encode(badEnv), "v1", now).reason()).isEqualTo("unsupported message type");
        // Missing gate_epoch / deadline presence.
        var missing = (com.fasterxml.jackson.databind.node.ObjectNode) m.readTree(json);
        missing.remove("gate_epoch");
        assertThat(p.verify(m.writeValueAsString(missing), "v1", now).reason()).isEqualTo("missing freshness");
        // Far-future deadline (beyond the 200-year offline cap).
        var far = new GatewayProtocol.Envelope("v1", "EXECUTION_INTENT", "far-1", "a", "p",
                GatewayProtocol.sha256(m.writeValueAsBytes(payload)), now, "f", now + 800L * 365 * 24 * 60 * 60 * 1000, payload, null);
        assertThat(p.verify(p.encode(far), "v1", now).reason()).isEqualTo("deadline too far in future");
        // Missing payload.
        var nopayload = (com.fasterxml.jackson.databind.node.ObjectNode) m.readTree(json);
        nopayload.remove("payload");
        assertThat(p.verify(m.writeValueAsString(nopayload), "v1", now).reason()).isEqualTo("missing payload");
    }

    @Test void newlineIdentityRejectedBothPaths() throws Exception {
        // P3-079: cross-field shift must fail at sign time, not verify time.
        GatewayProtocol p = new GatewayProtocol("secret");
        ObjectMapper m = new ObjectMapper();
        var payload = m.createObjectNode().put("x", 1);
        var evil = new GatewayProtocol.Envelope("v1", "EXECUTION_INTENT", "a\nb", "c", "p",
                GatewayProtocol.sha256(m.writeValueAsBytes(payload)), 1, "f", 60_000, payload, null);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> p.encode(evil))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void numericIdentityIsMissingNotCoerced() throws Exception {
        // P3-480: {"request_id": 123} must not verify as "123".
        GatewayProtocol p = new GatewayProtocol("secret");
        ObjectMapper m = new ObjectMapper();
        var payload = m.createObjectNode().put("x", 1);
        var e = new GatewayProtocol.Envelope("v1", "EXECUTION_INTENT", "num-1", "a", "p",
                GatewayProtocol.sha256(m.writeValueAsBytes(payload)), 1, "f", 60_000, payload, null);
        var node = (com.fasterxml.jackson.databind.node.ObjectNode) m.readTree(p.encode(e));
        node.put("request_id", 123);
        assertThat(p.verify(m.writeValueAsString(node), "v1", 0).reason()).isEqualTo("missing identity");
    }

    @Test void encodeDerivesHashAndValidatesNulls() throws Exception {
        // P3-296: blank hash in → derived hash out, and it verifies.
        GatewayProtocol p = new GatewayProtocol("secret");
        ObjectMapper m = new ObjectMapper();
        var payload = m.createObjectNode().put("x", 1);
        var e = new GatewayProtocol.Envelope("v1", "EXECUTION_INTENT", "derive-1", "a", "p",
                null, 1, "f", 60_000, payload, null);
        String json = p.encode(e);
        assertThat(p.verify(json, "v1", 0).accepted()).isTrue();
        // P3-296: wrong hash → fail fast at encode.
        var wrong = new GatewayProtocol.Envelope("v1", "EXECUTION_INTENT", "derive-2", "a", "p",
                "deadbeef", 1, "f", 60_000, payload, null);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> p.encode(wrong))
                .isInstanceOf(IllegalArgumentException.class);
        // P3-297: null identity → named field error, not raw NPE.
        var noid = new GatewayProtocol.Envelope("v1", "EXECUTION_INTENT", null, "a", "p",
                null, 1, "f", 60_000, payload, null);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> p.encode(noid))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("requestId");
    }

    @Test void bearerCheckIsConstantTimeNullSafe() {
        GatewayProtocol p = new GatewayProtocol("s3cr3t");
        assertThat(p.authorizedBearer(null)).isFalse();
        assertThat(p.authorizedBearer("Basic abc")).isFalse();
        assertThat(p.authorizedBearer("Bearer s3cr3t")).isTrue();
        assertThat(p.authorizedBearer("Bearer wrong")).isFalse();
    }
}
