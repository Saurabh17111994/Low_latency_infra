package com.trading.compute.babysitter;

/**
 * Babysitter runtime configuration (Task 7 of
 * docs/08_implementation/19-nautilus-execution-service-implementation-plan.md).
 *
 * <p>Read from the process environment and fail closed: every production
 * value is either required (non-blank) or has a safe default, and
 * {@code POSITION_ACTIONS_ENABLED} must be unset or {@code false} — any
 * attempt to enable position actions throws before a job is submitted
 * (R-286 / DEC-017). Missing or malformed source configuration must surface
 * as a startup failure, never as a silent default to a live path.
 */
public record BabysitterConfig(
        String bootstrapServers,
        String database,
        String table,
        String checkpointDir,
        long checkpointIntervalMs,
        long freshnessThresholdMs,
        boolean actionEnabled,
        String stateRecoveryPath) {

    public static final String FLUSS_BOOTSTRAP_SERVERS = "FLUSS_BOOTSTRAP_SERVERS";
    public static final String FLUSS_DATABASE = "FLUSS_DATABASE";
    public static final String FLUSS_TABLE = "FLUSS_TABLE";
    public static final String CHECKPOINT_DIR = "BABYSITTER_CHECKPOINT_DIR";
    /** Optional {@code file://}/{@code s3a://} path of a completed checkpoint to
     * restore observation state from on startup (mirrors SignalJob
     * {@code STATE_RECOVERY_PATH}); {@code null} means start fresh. */
    public static final String STATE_RECOVERY_PATH = "BABYSITTER_STATE_RECOVERY_PATH";
    public static final String CHECKPOINT_INTERVAL_MS = "BABYSITTER_CHECKPOINT_INTERVAL_MS";
    public static final String FRESHNESS_THRESHOLD_MS = "BABYSITTER_FRESHNESS_THRESHOLD_MS";
    public static final String POSITION_ACTIONS_ENABLED = "POSITION_ACTIONS_ENABLED";

    public static final String DEFAULT_DATABASE = "default";
    public static final String DEFAULT_TABLE = "Positions";

    public BabysitterConfig {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalStateException(
                    FLUSS_BOOTSTRAP_SERVERS + " is required; the Babysitter must fail "
                            + "closed rather than default to a live source");
        }
        // P2-113: fail closed at construction, not just in fromEnv() — blank
        // database/table are rejected, blank optional paths normalize to null.
        if (database == null || database.isBlank()) {
            throw new IllegalStateException(FLUSS_DATABASE + " must be non-blank");
        }
        if (table == null || table.isBlank()) {
            throw new IllegalStateException(FLUSS_TABLE + " must be non-blank");
        }
        database = database.trim();
        table = table.trim();
        checkpointDir = (checkpointDir == null || checkpointDir.isBlank())
                ? null : checkpointDir.trim();
        stateRecoveryPath = (stateRecoveryPath == null || stateRecoveryPath.isBlank())
                ? null : stateRecoveryPath.trim();
        if (checkpointIntervalMs <= 0) {
            throw new IllegalStateException(CHECKPOINT_INTERVAL_MS + " must be positive, got "
                    + checkpointIntervalMs);
        }
        if (freshnessThresholdMs <= 0) {
            throw new IllegalStateException(FRESHNESS_THRESHOLD_MS + " must be positive, got "
                    + freshnessThresholdMs);
        }
        if (actionEnabled) {
            throw new IllegalStateException(POSITION_ACTIONS_ENABLED
                    + " must be false in MVP; got 'true'");
        }
    }

    /** Builds a config from env vars, failing closed on anything invalid. */
    public static BabysitterConfig fromEnv() {
        // G1 (2026-08-29): every declared config key must actually be read.
        com.trading.common.config.ConfigGuard.assertAllKeysRead();
        return new BabysitterConfig(
                envOrNull(FLUSS_BOOTSTRAP_SERVERS),
                envOrDefault(FLUSS_DATABASE, DEFAULT_DATABASE),
                envOrDefault(FLUSS_TABLE, DEFAULT_TABLE),
                envOrNull(CHECKPOINT_DIR),
                positiveLong(CHECKPOINT_INTERVAL_MS, envOrNull(CHECKPOINT_INTERVAL_MS), 60_000L),
                positiveLong(FRESHNESS_THRESHOLD_MS, envOrNull(FRESHNESS_THRESHOLD_MS), 60_000L),
                parseActionEnabled(System.getenv(POSITION_ACTIONS_ENABLED)),
                envOrNull(STATE_RECOVERY_PATH));
    }

    /**
     * Fail closed on the action flag: any value other than unset or
     * {@code false} (case-insensitive, trimmed — R-286, config-file exports
     * may carry padding or newlines) throws.
     */
    public static boolean parseActionEnabled(String envValue) {
        if (envValue != null && !"false".equalsIgnoreCase(envValue.trim())) {
            throw new IllegalStateException(
                    POSITION_ACTIONS_ENABLED + " must be false in MVP; got '" + envValue + "'");
        }
        return false;
    }

    private static long positiveLong(String name, String value, long def) {
        if (value == null || value.isBlank()) {
            return def;
        }
        try {
            long parsed = Long.parseLong(value.trim());
            if (parsed <= 0) {
                throw new NumberFormatException("must be positive");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalStateException(name + " must be a positive number, got '" + value + "'",
                    e);
        }
    }

    private static String envOrNull(String name) {
        String value = System.getenv(name);
        return (value == null || value.isBlank()) ? null : value.trim();
    }

    private static String envOrDefault(String name, String def) {
        String value = envOrNull(name);
        return value == null ? def : value;
    }
}
