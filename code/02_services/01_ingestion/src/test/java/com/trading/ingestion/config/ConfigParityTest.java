package com.trading.ingestion.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ING-UNIT-018: Java↔Go config parity for the {@code ARROW_HFT_*} policy keys.
 *
 * <p>This table is the Java mirror of {@code hft_policy_test.go} (Go side):
 * the same accepted / rejected values are fed to Java's {@code exactInt} /
 * {@code intRange} here and to Go's {@code hftPin} / {@code hftRange} there,
 * so a drift on either side (wrong default, loosened bound, silently-ignored
 * value) fails the suite that owns it. Pinned keys must equal the pin exactly;
 * tunable keys must be within {@code [min, max]}.
 */
@DisplayName("ING-UNIT-018: Java↔Go ARROW_HFT_* config parity")
class ConfigParityTest {

    /** One {@code ARROW_HFT_*} key's validation contract (pin == -1 = tunable). */
    private record Policy(String key, int pin, int def, int min, int max) {}

    /** Must stay in lockstep with the {@code hftPolicyTable} in hft_policy_test.go. */
    private static final List<Policy> TABLE = List.of(
            new Policy("ARROW_HFT_CONNECTIONS", -1, 1, 1, 3),
            new Policy("ARROW_HFT_LATENCY_MS", -1, 50, 50, 60_000),
            new Policy("ARROW_HFT_MAX_TOKENS_PER_CONNECTION", 1024, 1024, 1024, 1024),
            new Policy("ARROW_HFT_MAX_TOKENS_PER_REQUEST", 512, 512, 512, 512),
            new Policy("ARROW_HFT_HEARTBEAT_SECONDS", 3, 3, 3, 3),
            new Policy("ARROW_HFT_STALL_TIMEOUT_SECONDS", -1, 15, 5, 60),
            new Policy("ARROW_HFT_SUBSCRIPTION_RESPONSE_TIMEOUT_SECONDS", -1, 10, 1, 60),
            new Policy("ARROW_HFT_RECONNECT_BASE_SECONDS", 1, 1, 1, 1),
            new Policy("ARROW_HFT_RECONNECT_MAX_SECONDS", 30, 30, 30, 30),
            new Policy("ARROW_HFT_AUTH_REFRESH_ATTEMPTS", 3, 3, 3, 3),
            new Policy("ARROW_HFT_MIN_ACTIVE_SLOTS", 1, 1, 1, 1));

    private static int defaultOf(Policy p) {
        return p.pin() >= 0 ? p.pin() : p.def();
    }

    /** A value every validator of this shape must accept. */
    private static int acceptedValue(Policy p) {
        return p.pin() >= 0 ? p.pin() : (p.min() + p.max()) / 2;
    }

    /** A value every validator of this shape must reject. */
    private static int rejectedValue(Policy p) {
        return p.pin() >= 0 ? p.pin() + 1 : p.min() - 1;
    }

    /** Minimal valid base env — the parity keys are layered on top. */
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
    @DisplayName("unset keys fall back to the shared defaults")
    void defaultsMatchTable() {
        IngestionConfig cfg = IngestionConfig.validateFrom(baseEnv());
        for (Policy p : TABLE) {
            assertEquals(defaultOf(p), cfg.toMap().get(p.key()),
                    p.key() + " default");
        }
    }

    @Test
    @DisplayName("every accepted value (pin / in-range) is accepted")
    void acceptedValuesAccepted() {
        for (Policy p : TABLE) {
            Map<String, String> env = baseEnv();
            env.put(p.key(), String.valueOf(acceptedValue(p)));
            IngestionConfig cfg = IngestionConfig.validateFrom(env);
            assertEquals(acceptedValue(p), cfg.toMap().get(p.key()),
                    p.key() + " accepted value");
        }
    }

    @Test
    @DisplayName("every rejected value (non-pin / out-of-range) fails startup validation")
    void rejectedValuesRejected() {
        for (Policy p : TABLE) {
            Map<String, String> env = baseEnv();
            env.put(p.key(), String.valueOf(rejectedValue(p)));
            IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> IngestionConfig.validateFrom(env),
                    p.key() + "=" + rejectedValue(p) + " must be rejected");
            assertTrue(e.getMessage().contains(p.key()), e.getMessage());
        }
    }

    @Test
    @DisplayName("non-integer values fail startup validation for every key")
    void nonIntegerValuesRejected() {
        for (Policy p : TABLE) {
            Map<String, String> env = baseEnv();
            env.put(p.key(), "abc");
            IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> IngestionConfig.validateFrom(env),
                    p.key() + "=abc must be rejected");
            assertTrue(e.getMessage().contains(p.key()), e.getMessage());
        }
    }
}
