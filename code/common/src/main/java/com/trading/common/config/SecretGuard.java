package com.trading.common.config;

import java.util.List;
import java.util.Map;

/**
 * Startup guard (configuration-driven plan S3, 2026-08-29): secrets must live
 * in the git-ignored {@code secrets.env}, never in the main {@code .env} or
 * process environment that feeds regular configuration.
 *
 * <p>Every config validator that reads the main env map must call
 * {@link #assertNoSecrets(Map)} first. A secret key present with a non-blank
 * value in the main map means someone re-added a secret to the wrong file —
 * fail closed rather than risk shipping credentials in a committed config.
 */
public final class SecretGuard {

    /** Secret keys that must NOT appear (non-blank) in the main env map. */
    public static final List<String> SECRET_KEYS = List.of(
            "ARROW_APP_SECRET",
            "ARROW_PASSWORD",
            "ARROW_TOTP_KEY",
            "EOD_MASTER_KEY",
            "O2_PASSWORD",
            "O2_AUTH_BASIC",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY");

    private SecretGuard() {}

    /**
     * Fail closed if any secret key has a non-blank value in the provided env map.
     *
     * @param env the main configuration environment map (never the secrets file)
     * @throws IllegalStateException if a secret is present in the main map
     */
    public static void assertNoSecrets(Map<String, String> env) {
        for (String key : SECRET_KEYS) {
            String v = env.get(key);
            if (v != null && !v.isBlank()) {
                throw new IllegalStateException(
                        "Secret key '" + key + "' must live in secrets.env, not the main "
                                + "config/env. Refusing to start (configuration-driven plan S3).");
            }
        }
    }
}
