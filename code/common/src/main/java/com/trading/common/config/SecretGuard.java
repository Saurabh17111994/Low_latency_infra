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
 *
 * <p><b>Env-file deployments (docker compose):</b> compose delivers secrets
 * into the container env via {@code env_file: [.env, secrets.env]} and
 * {@code ${VAR}} interpolation — the JVM sees the merged map and cannot tell
 * a secret came from {@code secrets.env} vs {@code .env}. The compose
 * deployment therefore sets {@code SECRETS_VIA_ENV_FILE=1} (a non-secret
 * marker) to declare "secrets are delivered via the env-file mechanism";
 * when the marker is present the guard skips the fail-closed check.
 *
 * <p><b>What the marker is and is not (P6-271):</b> it travels in the same map
 * as the secrets it excuses, so it is a deployment declaration, not a boundary —
 * anything able to set the process environment can set it too. The threat this
 * guard closes is accidental: a real credential that reached the committed
 * {@code .env} or the exported host env by mistake, which is how the audit found
 * fake secrets in the harness scripts. The local harnesses
 * ({@code loadtest-run.sh}, {@code pipeline-lib.sh}, {@code tiering-smoke.sh},
 * {@code stage-soak-e2e.sh}) and {@code docker-compose.yml} therefore set the
 * marker alongside their fake secrets; that is the intended use, not a bypass.
 * Hardening the marker onto an out-of-band channel would also reject those
 * harnesses, so the honest contract is this comment plus a test, not an API split.
 */
public final class SecretGuard {

    /**
     * Marker env var set by docker-compose deployments that deliver secrets
     * via {@code env_file}. Presence = "secrets legitimately came through the
     * env-file mechanism; skip the fail-closed check"; absence = the check
     * applies (2026-08-29 decision A). It is convenience, not a trust boundary
     * — see the class comment.
     */
    public static final String SECRETS_VIA_ENV_FILE = "SECRETS_VIA_ENV_FILE";

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
        if (env == null) {
            throw new IllegalStateException(
                    "No configuration env map was supplied to the secret guard; refusing to "
                            + "continue unguarded (configuration-driven plan S3).");
        }
        if ("1".equals(env.get(SECRETS_VIA_ENV_FILE))) {
            return; // compose env-file deployment — secrets arrive via env_file
        }
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
