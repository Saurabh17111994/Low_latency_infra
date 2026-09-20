package com.trading.ingestion.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * CHG-251: Docker and Swarm secrets arrive as FILES. Mirrors the gateway's
 * {@code _FILE} contract (plain JUnit assertions — this module has no AssertJ):
 * the file wins when both forms are set, a named-but-unusable file fails LOUD,
 * and the plain variable keeps working when no file is named.
 */
class IngestionConfigFileTest {
    static Map<String, String> values() {
        Map<String, String> m = new HashMap<>();
        m.put("ARROW_APP_ID", "app-id");
        m.put("ARROW_APP_SECRET", "plain-secret");
        m.put("ARROW_USER_ID", "user");
        m.put("ARROW_PASSWORD", "plain-password");
        m.put("ARROW_TOTP_KEY", "plain-totp");
        m.put("FLUSS_BOOTSTRAP", "fluss-coordinator:9123");
        m.put("RAW_TABLE_NAME", "raw_ticks");
        m.put("DEPLOYMENT_ENV", "dev");
        m.put("ARROW_MAX_EVENT_AGE_MS", "5000");
        m.put("ARROW_MAX_FUTURE_EVENT_SKEW_MS", "2000");
        return m;
    }

    /**
     * The stack mounts the three Swarm secrets and leaves the plain variables unset.
     * The guard stays ON here to prove the file form and SecretGuard coexist.
     */
    @Test void secretsCanComeFromFiles(@TempDir Path dir) throws Exception {
        Path secret = dir.resolve("arrow_app_secret");
        Files.writeString(secret, "from-file-secret\n");
        Path password = dir.resolve("arrow_password");
        Files.writeString(password, "from-file-password");
        Path totp = dir.resolve("arrow_totp_key");
        Files.writeString(totp, "from-file-totp\n");
        Map<String, String> m = values();
        m.remove("ARROW_APP_SECRET");
        m.remove("ARROW_PASSWORD");
        m.remove("ARROW_TOTP_KEY");
        m.put("ARROW_APP_SECRET_FILE", secret.toString());
        m.put("ARROW_PASSWORD_FILE", password.toString());
        m.put("ARROW_TOTP_KEY_FILE", totp.toString());
        IngestionConfig cfg = IngestionConfig.validateFrom(m, true);
        assertEquals("from-file-secret", cfg.arrowAppSecret);
        assertEquals("from-file-password", cfg.arrowPassword);
        assertEquals("from-file-totp", cfg.arrowTotpKey);
    }

    @Test void secretFileWinsOverThePlainVariable(@TempDir Path dir) throws Exception {
        Path secret = dir.resolve("arrow_app_secret");
        Files.writeString(secret, "from-file");
        Map<String, String> m = values();   // still carries ARROW_APP_SECRET=plain-secret
        m.put("ARROW_APP_SECRET_FILE", secret.toString());
        assertEquals("from-file", IngestionConfig.validateFrom(m, false).arrowAppSecret);
    }

    @Test void plainVariablesStillWorkWhenNoFileIsNamed() {
        IngestionConfig cfg = IngestionConfig.validateFrom(values(), false);
        assertEquals("plain-secret", cfg.arrowAppSecret);
        assertEquals("plain-password", cfg.arrowPassword);
        assertEquals("plain-totp", cfg.arrowTotpKey);
    }

    /**
     * A named-but-unusable file must fail LOUD rather than silently continue with an unset
     * secret — the same fail-closed contract the Go execution bridge applies.
     */
    @Test void unreadableOrEmptySecretFileFailsLoud(@TempDir Path dir) throws Exception {
        Map<String, String> missing = values();
        missing.put("ARROW_APP_SECRET_FILE", dir.resolve("does-not-exist").toString());
        IllegalArgumentException unreadable =
                assertThrows(IllegalArgumentException.class, () -> IngestionConfig.validateFrom(missing, false));
        assertTrue(unreadable.getMessage().contains("ARROW_APP_SECRET_FILE"));
        assertTrue(unreadable.getMessage().contains("unreadable"));

        Path empty = dir.resolve("empty");
        Files.writeString(empty, "   \n");
        Map<String, String> blank = values();
        blank.put("ARROW_PASSWORD_FILE", empty.toString());
        IllegalArgumentException emptyFile =
                assertThrows(IllegalArgumentException.class, () -> IngestionConfig.validateFrom(blank, false));
        assertTrue(emptyFile.getMessage().contains("ARROW_PASSWORD_FILE"));
        assertTrue(emptyFile.getMessage().contains("empty"));
    }
}
