package com.trading.ingestion.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.trading.common.observability.AlertThresholds;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ING-UNIT-002: IngestionConfig — verifies constants, defaults, and
 * internal consistency. Full validation against env vars is integration-tested.
 */
@DisplayName("ING-UNIT-002: IngestionConfig Validation")
class IngestionConfigTest {

    @Test
    @DisplayName("MAX_PENDING_RECORDS = 150,000 — T2 streaming-3000 3k default (50k→150k)")
    void maxPendingRecords() {
        assertEquals(150_000L, IngestionConfig.MAX_PENDING_RECORDS);
    }

    @Test
    @DisplayName("MAX_PENDING_BYTES = 192 MiB — T2 streaming-3000 3k default (64M→192M)")
    void maxPendingBytes() {
        assertEquals(201_326_592L, IngestionConfig.MAX_PENDING_BYTES);
    }

    @Test
    @DisplayName("WARNING_PERCENT = 80%")
    void warningPercent() {
        assertEquals(0.80, IngestionConfig.WARNING_PERCENT, 0.001);
        // The 80% default and the PENDING_APPEND_* alert rows read one number.
        assertEquals(AlertThresholds.PENDING_APPEND_WARNING_PERCENT / 100.0,
                IngestionConfig.WARNING_PERCENT);
    }

    @Test
    @DisplayName("DRAIN_DEADLINE_SECONDS default = 2s — B130 single total close budget")
    void drainDeadlineDefaultTwoSeconds() {
        java.util.Map<String, String> env = new java.util.LinkedHashMap<>();
        env.put("ARROW_APP_ID", "test-app");
        env.put("DEPLOYMENT_ENV", "dev");
        env.put("ARROW_APP_SECRET", "test-secret");
        env.put("ARROW_USER_ID", "test-user");
        env.put("ARROW_PASSWORD", "test-pass");
        env.put("ARROW_TOTP_KEY", "JBSWY3DPEHPK3PXP");
        env.put("FLUSS_BOOTSTRAP", "localhost:9123");
        env.put("RAW_TABLE_NAME", "raw_table_1");
        env.put("ARROW_MAX_EVENT_AGE_MS", "5000");
        env.put("ARROW_MAX_FUTURE_EVENT_SKEW_MS", "2000");
        IngestionConfig cfg = IngestionConfig.validateFrom(env);
        assertEquals(2, cfg.drainDeadline.getSeconds(),
                "default close budget is 2s (operators can raise via env)");
    }

    @Test
    @DisplayName("CLOCK_OFFSET_LIMIT_MS = 2000ms (T10 2s gate)")
    void clockOffsetLimit() {
        assertEquals(2000L, IngestionConfig.CLOCK_OFFSET_LIMIT_MS);
    }

    @Test
    @DisplayName("Constants are within safe ranges")
    void constantsInSafeRanges() {
        assertTrue(IngestionConfig.MAX_PENDING_RECORDS > 0, "Records limit > 0");
        assertTrue(IngestionConfig.MAX_PENDING_RECORDS <= 1_000_000, "Records limit ≤ 1M");

        assertTrue(IngestionConfig.MAX_PENDING_BYTES >= 1_048_576L, "Bytes limit ≥ 1 MiB");
        assertTrue(IngestionConfig.MAX_PENDING_BYTES <= Long.MAX_VALUE, "Bytes limit finite");

        assertTrue(IngestionConfig.WARNING_PERCENT > 0.0, "Warning percent > 0%");
        assertTrue(IngestionConfig.WARNING_PERCENT < 1.0, "Warning percent < 100%");

        assertTrue(IngestionConfig.CLOCK_OFFSET_LIMIT_MS >= 10L, "Clock offset ≥ 10ms");
        assertTrue(IngestionConfig.CLOCK_OFFSET_LIMIT_MS <= 60_000L, "Clock offset ≤ 60s");
    }

    @Test
    @DisplayName("HFT connection policy defaults match plan §IngestionConfig")
    void hftPolicyDefaultsMatchPlan() {
        java.util.Map<String, String> env = new java.util.LinkedHashMap<>();
        env.put("ARROW_APP_ID", "test-app");
        env.put("DEPLOYMENT_ENV", "dev");
        env.put("ARROW_APP_SECRET", "test-secret");
        env.put("ARROW_USER_ID", "test-user");
        env.put("ARROW_PASSWORD", "test-pass");
        env.put("ARROW_TOTP_KEY", "JBSWY3DPEHPK3PXP");
        env.put("FLUSS_BOOTSTRAP", "localhost:9123");
        env.put("RAW_TABLE_NAME", "raw_table_1");
        env.put("ARROW_MAX_EVENT_AGE_MS", "5000");
        env.put("ARROW_MAX_FUTURE_EVENT_SKEW_MS", "2000");

        IngestionConfig cfg = IngestionConfig.validateFrom(env);

        assertEquals(1, cfg.arrowHftConnections, "ARROW_HFT_CONNECTIONS default = 1");
        assertEquals(1024, cfg.arrowHftMaxTokensPerConnection, "max tokens/connection = 1024");
        assertEquals(512, cfg.arrowHftMaxTokensPerRequest, "max tokens/request = 512");
        assertEquals(3, cfg.arrowHftHeartbeatSeconds, "heartbeat = 3s");
        assertEquals(15, cfg.arrowHftStallTimeoutSeconds, "stall timeout default = 15s");
        assertEquals(10, cfg.arrowHftSubscriptionResponseTimeoutSeconds, "response timeout default = 10s");
        assertEquals(1, cfg.arrowHftReconnectBaseSeconds, "reconnect base = 1s");
        assertEquals(30, cfg.arrowHftReconnectMaxSeconds, "reconnect max = 30s");
        assertEquals(3, cfg.arrowHftAuthRefreshAttempts, "auth refresh attempts = 3");
        assertEquals(1, cfg.arrowHftMinActiveSlots, "min active slots = 1");
        assertEquals(false, cfg.arrowHftMultiConnectionApproved, "multi-connection unapproved by default");
        assertEquals(false, cfg.ingestionAllowDegraded, "degraded mode disabled by default");
        assertEquals(false, cfg.allowRuntimeDdl, "runtime DDL disabled by default (read-only verifyTables)");

        // Redacted config map exposes the values but never secrets.
        Map<String, Object> map = cfg.toMap();
        assertEquals(1, map.get("ARROW_HFT_CONNECTIONS"));
        assertTrue(map.containsKey("ARROW_HFT_MAX_TOKENS_PER_CONNECTION"));
        assertTrue(!map.get("ARROW_APP_SECRET").toString().contains("test-secret"));
    }

    @Test
    @DisplayName("production rejects degraded mode and unapproved multi-connection")
    void productionRejectsDegradedAndUnapprovedMulti() {
        java.util.Map<String, String> env = new java.util.LinkedHashMap<>();
        env.put("ARROW_APP_ID", "test-app");
        env.put("ARROW_APP_SECRET", "test-secret");
        env.put("ARROW_USER_ID", "test-user");
        env.put("ARROW_PASSWORD", "test-pass");
        env.put("ARROW_TOTP_KEY", "JBSWY3DPEHPK3PXP");
        env.put("FLUSS_BOOTSTRAP", "localhost:9123");
        env.put("RAW_TABLE_NAME", "raw_table_1");
        env.put("ARROW_MAX_EVENT_AGE_MS", "5000");
        env.put("ARROW_MAX_FUTURE_EVENT_SKEW_MS", "2000");
        env.put("DEPLOY_ENV", "prod");
        env.put("INGESTION_ALLOW_DEGRADED", "true");

        try {
            IngestionConfig.validateFrom(env);
            org.junit.jupiter.api.Assertions.fail("production must reject INGESTION_ALLOW_DEGRADED=true");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("INGESTION_ALLOW_DEGRADED"));
        }
    }

    @Test
    @DisplayName("ARROW_MAX_EVENT_AGE_MS / SKEW reject 0 (R-114)")
    void ageAndSkewMustBePositive() {
        java.util.Map<String, String> env = new java.util.LinkedHashMap<>();
        env.put("ARROW_APP_ID", "test-app");
        env.put("DEPLOYMENT_ENV", "dev");
        env.put("ARROW_APP_SECRET", "test-secret");
        env.put("ARROW_USER_ID", "test-user");
        env.put("ARROW_PASSWORD", "test-pass");
        env.put("ARROW_TOTP_KEY", "JBSWY3DPEHPK3PXP");
        env.put("FLUSS_BOOTSTRAP", "localhost:9123");
        env.put("RAW_TABLE_NAME", "raw_table_1");
        env.put("ARROW_MAX_EVENT_AGE_MS", "0");
        env.put("ARROW_MAX_FUTURE_EVENT_SKEW_MS", "2000");

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> IngestionConfig.validateFrom(env),
                "ARROW_MAX_EVENT_AGE_MS=0 must be rejected (R-114)");
        assertTrue(e.getMessage().contains("ARROW_MAX_EVENT_AGE_MS"), e.getMessage());

        env.put("ARROW_MAX_EVENT_AGE_MS", "5000");
        env.put("ARROW_MAX_FUTURE_EVENT_SKEW_MS", "0");
        IllegalStateException e2 = assertThrows(IllegalStateException.class,
                () -> IngestionConfig.validateFrom(env),
                "ARROW_MAX_FUTURE_EVENT_SKEW_MS=0 must be rejected (R-114)");
        assertTrue(e2.getMessage().contains("ARROW_MAX_FUTURE_EVENT_SKEW_MS"), e2.getMessage());
    }

    @Test
    @DisplayName("MAX_PENDING_APPEND_BYTES below the 1 MiB floor is rejected once (R-156)")
    void pendingBytesFloorEnforced() {
        java.util.Map<String, String> env = new java.util.LinkedHashMap<>();
        env.put("ARROW_APP_ID", "test-app");
        env.put("DEPLOYMENT_ENV", "dev");
        env.put("ARROW_APP_SECRET", "test-secret");
        env.put("ARROW_USER_ID", "test-user");
        env.put("ARROW_PASSWORD", "test-pass");
        env.put("ARROW_TOTP_KEY", "JBSWY3DPEHPK3PXP");
        env.put("FLUSS_BOOTSTRAP", "localhost:9123");
        env.put("RAW_TABLE_NAME", "raw_table_1");
        env.put("ARROW_MAX_EVENT_AGE_MS", "5000");
        env.put("ARROW_MAX_FUTURE_EVENT_SKEW_MS", "2000");
        env.put("MAX_PENDING_APPEND_BYTES", "1024"); // below 1 MiB floor

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> IngestionConfig.validateFrom(env),
                "MAX_PENDING_APPEND_BYTES below the 1 MiB floor must be rejected (R-156)");
        assertTrue(e.getMessage().contains("MAX_PENDING_APPEND_BYTES"), e.getMessage());
    }

    @Test
    @DisplayName("T2 tunable backpressure — env overrides 80%/100%, alias PENDING_MAX_*")
    void tunableBackpressureEnvOverrides() {
        java.util.Map<String, String> env = new java.util.LinkedHashMap<>();
        env.put("ARROW_APP_ID", "test-app");
        env.put("DEPLOYMENT_ENV", "dev");
        env.put("ARROW_APP_SECRET", "test-secret");
        env.put("ARROW_USER_ID", "test-user");
        env.put("ARROW_PASSWORD", "test-pass");
        env.put("ARROW_TOTP_KEY", "JBSWY3DPEHPK3PXP");
        env.put("FLUSS_BOOTSTRAP", "localhost:9123");
        env.put("RAW_TABLE_NAME", "raw_table_1");
        env.put("ARROW_MAX_EVENT_AGE_MS", "5000");
        env.put("ARROW_MAX_FUTURE_EVENT_SKEW_MS", "2000");
        // Default is 150k/192M (3k) — no env → defaults.
        IngestionConfig def = IngestionConfig.validateFrom(env);
        assertEquals(150_000, def.maxPendingRecords, "default 150k for 3k");
        assertEquals(201_326_592L, def.maxPendingBytes, "default 192M for 3k");
        assertEquals(0.80, def.pendingWarningPercent, 0.001, "default 80% warn");

        // Override back to 1k values via primary keys.
        env.put("MAX_PENDING_APPEND_RECORDS", "50000");
        env.put("MAX_PENDING_APPEND_BYTES", "67108864");
        env.put("PENDING_APPEND_WARNING_PERCENT", "0.80");
        IngestionConfig v1k = IngestionConfig.validateFrom(env);
        assertEquals(50_000, v1k.maxPendingRecords, "1k override 50k via primary");
        assertEquals(67_108_864L, v1k.maxPendingBytes, "1k override 64M via primary");

        // Alias PENDING_MAX_* works when primary absent.
        env.remove("MAX_PENDING_APPEND_RECORDS");
        env.remove("MAX_PENDING_APPEND_BYTES");
        env.remove("PENDING_APPEND_WARNING_PERCENT");
        env.put("PENDING_MAX_RECORDS", "50000");
        env.put("PENDING_MAX_BYTES", "67108864");
        env.put("PENDING_WARNING_PERCENT", "0.85");
        IngestionConfig alias = IngestionConfig.validateFrom(env);
        assertEquals(50_000, alias.maxPendingRecords, "alias PENDING_MAX_RECORDS");
        assertEquals(67_108_864L, alias.maxPendingBytes, "alias PENDING_MAX_BYTES");
        assertEquals(0.85, alias.pendingWarningPercent, 0.001, "alias warning 85%");

        // Primary wins over alias when both set.
        env.put("MAX_PENDING_APPEND_RECORDS", "150000");
        env.put("PENDING_MAX_RECORDS", "50000");
        IngestionConfig priWins = IngestionConfig.validateFrom(env);
        assertEquals(150_000, priWins.maxPendingRecords, "primary wins over alias");

        // Custom 3k tunable — env overrides still enforce range (100..1M, 1MiB..MAX).
        env.put("MAX_PENDING_APPEND_RECORDS", "150000");
        env.put("PENDING_MAX_BYTES", "201326592");
        IngestionConfig c3k = IngestionConfig.validateFrom(env);
        assertEquals(150_000, c3k.maxPendingRecords);
        assertEquals(201_326_592L, c3k.maxPendingBytes);
    }

    @Test
    @DisplayName("P1-077: blank deploy env refuses startup (fail-closed, no silent dev)")
    void blankDeployEnvRefusesStartup() {
        java.util.Map<String, String> env = new java.util.LinkedHashMap<>();
        env.put("ARROW_APP_ID", "test-app");
        env.put("ARROW_APP_SECRET", "test-secret");
        env.put("ARROW_USER_ID", "test-user");
        env.put("ARROW_PASSWORD", "test-pass");
        env.put("ARROW_TOTP_KEY", "JBSWY3DPEHPK3PXP");
        env.put("FLUSS_BOOTSTRAP", "localhost:9123");
        env.put("RAW_TABLE_NAME", "raw_table_1");
        env.put("ARROW_MAX_EVENT_AGE_MS", "5000");
        env.put("ARROW_MAX_FUTURE_EVENT_SKEW_MS", "2000");
        try {
            IngestionConfig.validateFrom(env);
            org.junit.jupiter.api.Assertions.fail("blank deploy env must refuse startup");
        } catch (IllegalStateException e) {
            org.junit.jupiter.api.Assertions.assertTrue(
                    e.getMessage().contains("DEPLOYMENT_ENV/DEPLOY_ENV"), e.getMessage());
        }
    }

    @Test
    @DisplayName("P1-077: canonical DEPLOYMENT_ENV=production engages the prod gates")
    void canonicalKeyEngagesProdGates() {
        java.util.Map<String, String> env = new java.util.LinkedHashMap<>();
        env.put("DEPLOYMENT_ENV", "production");
        env.put("ARROW_APP_ID", "test-app");
        env.put("ARROW_APP_SECRET", "test-secret");
        env.put("ARROW_USER_ID", "test-user");
        env.put("ARROW_PASSWORD", "test-pass");
        env.put("ARROW_TOTP_KEY", "JBSWY3DPEHPK3PXP");
        env.put("FLUSS_BOOTSTRAP", "localhost:9123");
        env.put("RAW_TABLE_NAME", "raw_table_1");
        env.put("ARROW_MAX_EVENT_AGE_MS", "5000");
        env.put("ARROW_MAX_FUTURE_EVENT_SKEW_MS", "2000");
        env.put("INGESTION_ALLOW_DEGRADED", "true");
        try {
            IngestionConfig.validateFrom(env);
            org.junit.jupiter.api.Assertions.fail(
                    "DEPLOYMENT_ENV=production must reject INGESTION_ALLOW_DEGRADED=true");
        } catch (IllegalStateException e) {
            org.junit.jupiter.api.Assertions.assertTrue(
                    e.getMessage().contains("INGESTION_ALLOW_DEGRADED"), e.getMessage());
        }
    }

    @Test
    @DisplayName("P1-240: boolean typos fail closed instead of coercing to false")
    void booleanTyposFailClosed() {
        String[] keys = {
            "ARROW_HFT_MULTI_CONNECTION_APPROVED",
            "INGESTION_ALLOW_DEGRADED",
            "ALLOW_RUNTIME_DDL",
            "CLOCK_CHECK_REQUIRED"
        };
        for (String key : keys) {
            java.util.Map<String, String> env = new java.util.LinkedHashMap<>();
            env.put("ARROW_APP_ID", "test-app");
            env.put("DEPLOYMENT_ENV", "dev");
            env.put("ARROW_APP_SECRET", "test-secret");
            env.put("ARROW_USER_ID", "test-user");
            env.put("ARROW_PASSWORD", "test-pass");
            env.put("ARROW_TOTP_KEY", "JBSWY3DPEHPK3PXP");
            env.put("FLUSS_BOOTSTRAP", "localhost:9123");
            env.put("RAW_TABLE_NAME", "raw_table_1");
            env.put("ARROW_MAX_EVENT_AGE_MS", "5000");
            env.put("ARROW_MAX_FUTURE_EVENT_SKEW_MS", "2000");
            env.put(key, "ture");
            IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> IngestionConfig.validateFrom(env),
                    key + "=ture must fail startup (fail-closed)");
            assertTrue(e.getMessage().contains(key), e.getMessage());
        }
    }

    @Test
    @DisplayName("P1-241: toMap exposes the validated FLUSS_WRITER_* bench keys")
    void toMapExposesWriterBenchKeys() {
        java.util.Map<String, String> env = new java.util.LinkedHashMap<>();
        env.put("ARROW_APP_ID", "test-app");
        env.put("DEPLOYMENT_ENV", "dev");
        env.put("ARROW_APP_SECRET", "test-secret");
        env.put("ARROW_USER_ID", "test-user");
        env.put("ARROW_PASSWORD", "test-pass");
        env.put("ARROW_TOTP_KEY", "JBSWY3DPEHPK3PXP");
        env.put("FLUSS_BOOTSTRAP", "localhost:9123");
        env.put("RAW_TABLE_NAME", "raw_table_1");
        env.put("ARROW_MAX_EVENT_AGE_MS", "5000");
        env.put("ARROW_MAX_FUTURE_EVENT_SKEW_MS", "2000");
        env.put("FLUSS_WRITER_MODE", "typed");
        env.put("FLUSS_WRITERS", "4");
        env.put("FLUSS_WRITER_BATCH_SIZE_BYTES", "65536");
        IngestionConfig cfg = IngestionConfig.validateFrom(env);
        Map<String, Object> map = cfg.toMap();
        assertEquals("typed", map.get("FLUSS_WRITER_MODE"));
        assertEquals(4, map.get("FLUSS_WRITERS"));
        assertEquals(65536, map.get("FLUSS_WRITER_BATCH_SIZE_BYTES"));
    }
}
