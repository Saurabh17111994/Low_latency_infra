package com.trading.common.observability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * R-045/R-077/R-218 (Json), R-129 (SafetyHaltRequest), R-130/R-266
 * (StructuredLogEvent), R-134 (AuditLogger).
 */
@DisplayName("Phase 5 observability regression tests")
class ObservabilityRegressionTest {

    // ---- R-045: nested structures must not corrupt the outer separator ----

    @Test
    void nestedContainersProduceValidJson() throws Exception {
        String json = Json.buildObj(o -> {
            o.kv("a", "1");
            o.obj("nested", o2 -> o2.kv("b", "2"));
            o.kv("c", "3"); // after a nested obj, the separator must still be correct
            o.arr("list", a -> {
                a.value("x");
                a.value(7L);
                a.obj(e -> e.kv("d", "4"));
            });
        });
        assertEquals("{\"a\":\"1\",\"nested\":{\"b\":\"2\"},\"c\":\"3\","
                + "\"list\":[\"x\",7,{\"d\":\"4\"}]}", json);
        // The separator logic is only correct if the document actually parses.
        JsonNode root = new ObjectMapper().readTree(json);
        assertEquals("1", root.get("a").asText());
        assertEquals("2", root.get("nested").get("b").asText());
        assertEquals("4", root.get("list").get(2).get("d").asText());
        // Outer obj must end with the kv, not a dangling comma.
        assertFalse(json.endsWith(","));
        assertFalse(json.contains(",}"));
    }

    @Test
    void mixedMemberSyntaxIsRejectedInsteadOfSerialized() {
        // The old API emitted {"a":"1",{"b":"2"}} for this — invalid JSON.
        assertThrows(IllegalStateException.class,
                () -> Json.buildObj(o -> o.obj(o2 -> o2.kv("b", "2"))));
        assertThrows(IllegalStateException.class,
                () -> Json.buildObj(o -> o.arr(a -> a.value("x"))));
        // Mirror case: a keyed member inside an array.
        assertThrows(IllegalStateException.class,
                () -> Json.buildArr(a -> a.kv("k", "v")));
    }

    @Test
    void nullOrBlankKeyIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> Json.buildObj(o -> o.kv(null, "v")));
        assertThrows(IllegalArgumentException.class, () -> Json.buildObj(o -> o.kv("  ", "v")));
        assertThrows(IllegalArgumentException.class,
                () -> Json.buildObj(o -> o.obj(null, inner -> inner.kv("a", "1"))));
    }

    @Test
    void emptyContainersAndNullElementsSerializeLegally() {
        assertEquals("[]", Json.buildArr(a -> { }));
        assertEquals("{}", Json.buildObj(o -> { }));
        assertEquals("[\"a\",null]", Json.buildArr(a -> {
            a.value("a");
            a.value((String) null);
        }));
    }

    // ---- R-077: null values serialize as JSON null ----

    @Test
    void nullValueSerializesAsNull() {
        String json = Json.buildObj(o -> o.kv("k", (String) null));
        assertEquals("{\"k\":null}", json);
    }

    // ---- R-218: control chars escaped without String.format ----

    @Test
    void controlCharsEscaped() {
        String escaped = Json.escape("a\u0001b\u001fc");
        assertTrue(escaped.contains("\\u0001"));
        assertTrue(escaped.contains("\\u001f"));
        assertEquals("a\\u0001b\\u001fc", escaped);
    }

    // ---- R-129: idempotency only for terminal-success results ----

    @Test
    void idempotencyOnlyForTerminalSuccess() {
        // Repeated APPLIED / ALREADY_HALTED are idempotent no-ops.
        assertTrue(SafetyHaltRequest.isIdempotentDuplicate(
                SafetyHaltRequest.Result.APPLIED, SafetyHaltRequest.Result.APPLIED));
        assertTrue(SafetyHaltRequest.isIdempotentDuplicate(
                SafetyHaltRequest.Result.ALREADY_HALTED,
                SafetyHaltRequest.Result.ALREADY_HALTED));
        // FAILED means never applied; PENDING is non-terminal — NOT idempotent.
        assertFalse(SafetyHaltRequest.isIdempotentDuplicate(
                SafetyHaltRequest.Result.FAILED, SafetyHaltRequest.Result.FAILED));
        assertFalse(SafetyHaltRequest.isIdempotentDuplicate(
                SafetyHaltRequest.Result.PENDING, SafetyHaltRequest.Result.PENDING));
        assertFalse(SafetyHaltRequest.isIdempotentDuplicate(
                SafetyHaltRequest.Result.APPLIED, SafetyHaltRequest.Result.FAILED));
    }

    // ---- R-130: required fields enforced at build ----

    @Test
    void requiredFieldsEnforced() {
        StructuredLogEvent.Builder b = StructuredLogEvent.builder(
                1L, null, "svc", "comp", "sub", "host", "vm", "prod",
                "cid", "trace", "span", "msg");
        IllegalStateException e = assertThrows(IllegalStateException.class, b::build);
        assertTrue(e.getMessage().contains("level"), e.getMessage());

        // A full sample still builds.
        StructuredLogEvent.builder(
                1L, "INFO", "svc", "comp", "sub", "host", "vm", "prod",
                "cid", "trace", "span", "msg").build();
    }

    // ---- R-266: equals covers identity/correlation fields ----

    @Test
    void equalsCoversIdentityFields() {
        StructuredLogEvent a = StructuredLogEvent.builder(
                1L, "INFO", "svc", "comp", "sub", "host-1", "vm1", "prod",
                "cid", "trace-1", "span-1", "msg").build();
        StructuredLogEvent b = StructuredLogEvent.builder(
                1L, "INFO", "svc", "comp", "sub", "host-2", "vm1", "prod",
                "cid", "trace-1", "span-1", "msg").build();
        assertFalse(a.equals(b), "different host must make records unequal (R-266)");

        StructuredLogEvent c = StructuredLogEvent.builder(
                1L, "INFO", "svc", "comp", "sub", "host-1", "vm1", "prod",
                "cid", "trace-2", "span-1", "msg").build();
        assertFalse(a.equals(c), "different trace_id must make records unequal (R-266)");
        assertTrue(a.equals(StructuredLogEvent.builder(
                1L, "INFO", "svc", "comp", "sub", "host-1", "vm1", "prod",
                "cid", "trace-1", "span-1", "msg").build()));
    }

    // ---- R-134: case-insensitive, token-contained redaction ----

    @Test
    void redactionIsCaseInsensitiveAndTokenContained() {
        assertEquals("***REDACTED***", AuditLogger.redact("api_key", "abc"));
        assertEquals("***REDACTED***", AuditLogger.redact("apiKey", "abc"));
        assertEquals("***REDACTED***", AuditLogger.redact("API_KEY", "abc"));
        assertEquals("***REDACTED***", AuditLogger.redact("authToken", "abc"));
        assertEquals("***REDACTED***", AuditLogger.redact("my-secret-field", "abc"));
        assertEquals("***REDACTED***", AuditLogger.redact("password", "abc"));
        assertEquals("plain", AuditLogger.redact("symbol", "plain"));
        assertEquals(null, AuditLogger.redact("symbol", null));
    }

    @Test
    void genericCredentialNamesAreRedacted() {
        // The concrete names normalize to "authtoken"/"apikey", so a field named
        // exactly `token`/`key` matched nothing and leaked verbatim.
        assertEquals("***REDACTED***", AuditLogger.redact("token", "abc"));
        assertEquals("***REDACTED***", AuditLogger.redact("key", "abc"));
        assertEquals("***REDACTED***", AuditLogger.redact("clientToken", "abc"));
        assertEquals("***REDACTED***", AuditLogger.redact("accessKey", "abc"));
        // Names that merely look similar stay readable.
        assertEquals("plain", AuditLogger.redact("id", "plain"));
        assertEquals("plain", AuditLogger.redact("symbol_count", "plain"));
    }

    @Test
    void nullFieldNameFailsClosed() {
        // Mandatory redaction: an unknown field name must not pass the value through.
        assertEquals("***REDACTED***", AuditLogger.redact(null, "abc"));
        assertEquals(null, AuditLogger.redact(null, null));
    }
}
