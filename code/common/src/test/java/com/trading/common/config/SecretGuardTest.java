package com.trading.common.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** S3 (2026-08-29): SecretGuard fails closed when a secret is in the main env map. */
class SecretGuardTest {

    private static Map<String, String> map(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void emptyMapPasses() {
        assertDoesNotThrow(() -> SecretGuard.assertNoSecrets(Map.of()));
    }

    /** P6-854: the documented contract is IllegalStateException, not a NullPointerException. */
    @Test
    void nullEnvFailsClosedWithTheDocumentedException() {
        assertThrows(IllegalStateException.class, () -> SecretGuard.assertNoSecrets(null),
                "a null env map must fail closed as an IllegalStateException");
    }

    @Test
    void nonSecretKeysPass() {
        assertDoesNotThrow(() -> SecretGuard.assertNoSecrets(
                map("ARROW_APP_ID", "app-1", "FLUSS_BOOTSTRAP", "localhost:9123")));
    }

    @Test
    void secretInMainMapFails() {
        for (String key : SecretGuard.SECRET_KEYS) {
            assertThrows(IllegalStateException.class,
                    () -> SecretGuard.assertNoSecrets(map(key, "some-value")),
                    "secret key '" + key + "' must be rejected in the main env map");
        }
    }

    @Test
    void blankSecretValuePasses() {
        // A key present-but-blank (e.g. a commented placeholder) is not a leak.
        assertDoesNotThrow(() -> SecretGuard.assertNoSecrets(map("ARROW_APP_SECRET", "")));
    }

    /** S3: the guarded validation path (production) rejects a secret in the main map. */
    @Test
    void guardedValidationRejectsSecretInMainMap() {
        Map<String, String> env = new HashMap<>();
        env.put("ARROW_APP_ID", "app");
        env.put("ARROW_APP_SECRET", "fake-secret");
        assertThrows(IllegalStateException.class, () -> SecretGuard.assertNoSecrets(env));
    }

    /** 2026-08-29 decision A: compose env-file marker skips the fail-closed check. */
    @Test
    void envFileMarkerSkipsSecretCheck() {
        Map<String, String> env = new HashMap<>();
        env.put(SecretGuard.SECRETS_VIA_ENV_FILE, "1");
        env.put("ARROW_APP_SECRET", "whatever");
        env.put("EOD_MASTER_KEY", "whatever");
        assertDoesNotThrow(() -> SecretGuard.assertNoSecrets(env),
                "SECRETS_VIA_ENV_FILE=1 must allow secrets delivered via env_file");
    }

    /** 2026-08-29 decision A: a marker value other than "1" does NOT skip. */
    @Test
    void envFileMarkerOnlySkipsWhenExactlyOne() {
        Map<String, String> env = new HashMap<>();
        env.put(SecretGuard.SECRETS_VIA_ENV_FILE, "yes");
        env.put("ARROW_APP_SECRET", "fake-secret");
        assertThrows(IllegalStateException.class, () -> SecretGuard.assertNoSecrets(env),
                "a non-'1' marker value must not disable the guard");
    }
}
