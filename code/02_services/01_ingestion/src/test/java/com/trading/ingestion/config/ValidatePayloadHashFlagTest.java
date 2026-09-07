// T5-H2 — INGEST_VALIDATE_PAYLOAD_HASH config flag (A2 decision):
// true (default) = per-tick SHA-256 validation; false = decode-only.

package com.trading.ingestion.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** T5-H2: hash-validation flag parses both ways and defaults to true. */
@DisplayName("T5-H2: INGEST_VALIDATE_PAYLOAD_HASH flag")
class ValidatePayloadHashFlagTest {

    private static Map<String, String> baseEnv() {
        Map<String, String> env = new LinkedHashMap<>();
        env.put("DEPLOYMENT_ENV", "dev");
        env.put("ARROW_APP_ID", "test-app");
        env.put("ARROW_APP_SECRET", "test-secret");
        env.put("ARROW_USER_ID", "test-user");
        env.put("ARROW_PASSWORD", "test-pass");
        env.put("ARROW_TOTP_KEY", "JBSWY3DPEHPK3PXP");
        env.put("FLUSS_BOOTSTRAP", "localhost:9123");
        env.put("RAW_TABLE_NAME", "raw_table_1");
        env.put("ARROW_MAX_EVENT_AGE_MS", "5000");
        env.put("ARROW_MAX_FUTURE_EVENT_SKEW_MS", "2000");
        return env;
    }

    @Test
    @DisplayName("default is true (safety) when env unset")
    void defaultTrue() {
        IngestionConfig cfg = IngestionConfig.validateFrom(baseEnv());
        assertTrue(cfg.validatePayloadHash, "default true (A2 safety)");
    }

    @Test
    @DisplayName("false explicitly disables validation")
    void falseDisables() {
        Map<String, String> env = baseEnv();
        env.put("INGEST_VALIDATE_PAYLOAD_HASH", "false");
        IngestionConfig cfg = IngestionConfig.validateFrom(env);
        assertFalse(cfg.validatePayloadHash, "false → validation off");
    }

    @Test
    @DisplayName("true explicitly enables validation")
    void trueEnables() {
        Map<String, String> env = baseEnv();
        env.put("INGEST_VALIDATE_PAYLOAD_HASH", "true");
        IngestionConfig cfg = IngestionConfig.validateFrom(env);
        assertTrue(cfg.validatePayloadHash, "true → validation on");
    }

    @Test
    @DisplayName("case-insensitive: TRUE/FALSE accepted")
    void caseInsensitive() {
        Map<String, String> env = baseEnv();
        env.put("INGEST_VALIDATE_PAYLOAD_HASH", "TRUE");
        assertTrue(IngestionConfig.validateFrom(env).validatePayloadHash, "TRUE");

        env.put("INGEST_VALIDATE_PAYLOAD_HASH", "FALSE");
        assertFalse(IngestionConfig.validateFrom(env).validatePayloadHash, "FALSE");
    }

    @Test
    @DisplayName("garbage value is a config error (fail-closed Q18)")
    void garbageFailsClosed() {
        Map<String, String> env = baseEnv();
        env.put("INGEST_VALIDATE_PAYLOAD_HASH", "maybe");
        var thrown = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalStateException.class, () -> IngestionConfig.validateFrom(env));
        assertTrue(thrown.getMessage().contains("INGEST_VALIDATE_PAYLOAD_HASH"),
                "error names the flag: " + thrown.getMessage());
    }

    @Test
    @DisplayName("metrics map exposes the flag")
    void metricsExposed() {
        IngestionConfig cfg = IngestionConfig.validateFrom(baseEnv());
        assertEquals(true, cfg.toMap().get("INGEST_VALIDATE_PAYLOAD_HASH"),
                "config map carries the flag");
    }
}
