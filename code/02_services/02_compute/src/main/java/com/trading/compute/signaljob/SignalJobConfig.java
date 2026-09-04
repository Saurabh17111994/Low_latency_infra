package com.trading.compute.signaljob;

import com.trading.common.config.PlatformConfig;
import com.trading.common.schema.CandlePreviewTableSchema;
import com.trading.common.schema.CandleTableSchema;
import java.io.Serializable;
import java.util.Map;

/**
 * Validated runtime configuration for the Signal job (compute path).
 *
 * <p><b>Load-bearing values are pinned, not tuned.</b> The fixed-scope contract
 * (docs/08_implementation/01-foundation.md, PlatformConfig javadoc) requires the
 * two correctness-critical constants — {@code DEDUP_WINDOW_ENTRIES} and
 * {@code CANDLE_WINDOW_MS} — plus the checkpoint contract
 * (REQ-FC-006) to be exactly pinned or the job MUST fail at startup. A missing
 * required value also aborts: "no unsafe default may be substituted"
 * (PlatformConfig.requirePresent). All other keys are tuning parameters with
 * documented defaults from the Signal-job dossier.
 *
 * <p>The fixed-delay restart strategy
 * ({@code RESTART_MAX_ATTEMPTS=3}, {@code RESTART_DELAY_MS=30000}) is
 * production-pinned: in {@code DEPLOYMENT_ENV=production} the two keys must be
 * present and exactly equal the PlatformConfig pins or startup fails — a
 * deployment cannot silently raise the retry budget. Dev keeps them as
 * overridable tuning defaults (failure-injection integration tests rely on
 * low attempts).
 *
 * <p>Build via {@link #fromEnv()} in production; {@link #from(Map)} is exposed
 * so the rejection rules are unit-testable without touching {@code System.getenv}.
 *
 * <p><b>Signal dual-sink (DEC-035, tracker 14 re-scoped P2).</b>
 * {@code SIGNAL_CANDIDATES_TABLE} (default {@code Signal_Candidates}) is the
 * append-only LOG; {@code SIGNAL_CURRENT_TABLE} (default
 * {@code Signal_Candidates_current}) is the KV current-state projection. The
 * canonical identity — {@code SIGNAL_STRATEGY_ID},
 * {@code SIGNAL_STRATEGY_VERSION}, {@code SIGNAL_RULE_ID} — defaults to the
 * pinned {@link SignalCandidatesTableColumns} canonical constants, so the
 * default config emits rows the KV filter accepts; an override changes the
 * identity and those rows are filtered from the KV sink (LOG keeps them) and
 * counted via {@code compute.signal.kv.filtered.noncanonical}.
 *
 * <p><b>Fail-closed startup mode (CANDLE-KV-REPLAY-001 A3.3).</b> A restart
 * must either restore from a checkpoint ({@code STATE_RECOVERY_PATH}, mode
 * {@link StartupMode#RESTORE}) or explicitly accept an offset-0 full replay
 * ({@code ALLOW_FULL_REPLAY=true}, mode {@link StartupMode#FULL_REPLAY}).
 * Neither, both, a blank restore path, or an invalid boolean all FAIL startup
 * — a silent offset-0 replay is forbidden (it re-emits the whole backlog,
 * balloons dedup state past the pinned checkpoint contract, and appends
 * duplicate candle rows to the immutable LOG; observed 2026-08-10).
 *
 * <p>Serializable: the config is a field of every operator function and must
 * survive Flink's closure cleaning and serialization to task managers.
 */
public record SignalJobConfig(
        String bootstrapServers,
        String database,
        String rawTable,
        String candleTable,
        String previewTable,
        String quarantineTable,
        String rawSchemaVersion,
        String algorithmVersion,
        String configurationVersion,
        String candleSchemaVersion,
        int dedupWindowEntries,
        long candleWindowMs,
        long outOfOrderMs,
        long allowedLatenessMs,
        long sourceIdleMs,
        long sourceIdleAlertMs,
        long checkpointIntervalMs,
        long checkpointTimeoutMs,
        int maxConcurrentCheckpoints,
        int restartMaxAttempts,
        long restartDelayMs,
        String checkpointDir,
        String signalCandidatesTable,
        String signalCurrentTable,
        String signalStrategyId,
        String signalStrategyVersion,
        String signalRuleId,
        int signalLookbackCandles,
        long signalQuantity,
        String formingRuleId,
        int formingLookbackCandles,
        boolean previewEnabled,
        long previewIntervalMs,
        long previewTtlMs,
        boolean earlySignalEnabled,
        String earlySignalRuleId,
        long earlySignalConfirmAfterMs,
        String formingBarTable,
        long formingBarWriteBatchMs,
        String tentativeMarkersTable,
        String positionStateTable,
        String otelCollectorHost,
        String stateRecoveryPath,
        boolean allowFullReplay,
        String deploymentEnv,
        String stateBackend,
        String savepointDir,
        String stateBackendLocalDirs,
        boolean stateBackendManagedMemory,
        String taskManagerMemoryManagedSize,
        String taskManagerNetworkMemoryMax,
        int parallelism,
        String s3Endpoint,
        String s3AccessKey,
        String s3SecretKey,
        String s3Region,
        boolean s3PathStyle,
        long sinkWriteStallTimeoutMs,
        int writerRetries,
        String tradeDecisionsTable,
        String tradeInstructionStateTable,
        boolean tradeDecisionsEnabled,
        String executionIntentTable,
        String executionAccountScopeId,
        String executionPartitionId,
        String executionProductType,
        String executionTimeInForce,
        boolean executionIntentEnabled,
        boolean multiTfEnabled,
        long liveSnapshotIntervalMs,
        boolean multiTfSessionBypass,
        boolean multiTfSignalContextEnabled,
        String candleLiveTable,
        String candleClosedTable,
        StartupMode startupMode) implements Serializable {

    public static SignalJobConfig fromEnv() {
        // G1 (2026-08-29): every declared config key must actually be read.
        com.trading.common.config.ConfigGuard.assertAllKeysRead();
        return from(System.getenv());
    }

    /** Builds a validated config from an environment map (production: {@code System.getenv()}). */
    public static SignalJobConfig from(Map<String, String> env) {
        StartupMode mode = validateStartupMode(env);
        boolean executionIntentEnabled = booleanValue(env, "EXECUTION_INTENT_ENABLED", false);
        String configurationVersion = requireCanonicalVersion(env, "CONFIGURATION_VERSION",
                CandleTableSchema.CANONICAL_CONFIGURATION_VERSION);
        if (executionIntentEnabled && !env.containsKey("CONFIGURATION_VERSION")) {
            throw new IllegalStateException("Config CONFIGURATION_VERSION must be explicit when "
                    + "EXECUTION_INTENT_ENABLED=true — no implicit execution contract version");
        }
        boolean earlySignalEnabled = booleanValue(env, "EARLY_SIGNAL_ENABLED", true);
        boolean previewEnabled = booleanValue(env, "PREVIEW_ENABLED", true);
        if (earlySignalEnabled && !previewEnabled) {
            throw new IllegalStateException("Config EARLY_SIGNAL_ENABLED=true requires "
                    + "PREVIEW_ENABLED=true — the early-signal path consumes preview rows");
        }
        boolean multiTfEnabled = booleanValue(env, "MULTITF_ENABLED", false);
        long liveSnapshotIntervalMs = positiveLong(env, "MULTITF_LIVE_SNAPSHOT_INTERVAL_MS", 1_000L);
        // Phase 5 soak mode A (2026-09-05): lets the multi-TF aggregator run
        // against the 15s fake-broker feed OUTSIDE market hours (weekend /
        // after close). Default false — production never bypasses the
        // 09:15-15:30 IST session filter. The soak harness sets it ONLY for
        // the dedicated soak run; it does not touch the old 15s path.
        boolean multiTfSessionBypass = booleanValue(env, "MULTITF_SESSION_BYPASS", false);
        // Candle-phase soak switch (2026-09-05): when false the multi-TF
        // aggregator skips the per-tick SIGNAL_TAG snapshot build (signal-job
        // work, unused while the focus is ticks -> candles). Default true —
        // production behavior unchanged; the candle soak sets it false.
        boolean multiTfSignalContextEnabled =
                booleanValue(env, "MULTITF_SIGNAL_CONTEXT_ENABLED", true);
        String candleLiveTable = stringEnv(env, "CANDLE_LIVE_TABLE", "candle_live");
        String candleClosedTable = stringEnv(env, "CANDLE_CLOSED_TABLE", "candle_closed");
        return new SignalJobConfig(
                bootstrapServers(env),
                env.getOrDefault("FLUSS_DATABASE", "default"),
                env.getOrDefault("RAW_TABLE", "raw_table_1"),
                env.getOrDefault("CANDLE_TABLE", "feature_candles_15s"),
                previewTable(env),
                env.getOrDefault("QUARANTINE_TABLE", "ingestion_quarantine"),
                env.getOrDefault("RAW_SCHEMA_VERSION", PlatformConfig.RAW_TABLE_1_SCHEMA_VERSION),
                requireCanonicalVersion(env, "ALGORITHM_VERSION",
                        CandleTableSchema.CANONICAL_ALGORITHM_VERSION),
                configurationVersion,
                env.getOrDefault("CANDLE_SCHEMA_VERSION", "2"),
                dedupWindowEntries(env),
                candleWindowMs(env),
                // Single-timeline rule (decision 2026-08-30): the SAME wait
                // bounds both the preview path and the final candle path, so
                // live decisions and backtests always see the same tick set.
                // 500ms: the fake broker delivers in order; revisit ONLY with
                // measured real-feed lateness percentiles (set just above the
                // observed p99.9 arrival lag) — never guess upward, every ms
                // here is added preview latency on every window tail
                // (measured 2026-08-30: 5s default produced p95=5.7s e2e,
                // with the last-preview p50=6.5s ≈ the wait itself).
                longValue(env, "WATERMARK_OUT_OF_ORDER_MS", 500L),
                longValue(env, "ALLOWED_LATENESS_MS", 5_000L),
                longValue(env, "SOURCE_IDLE_MS", 15_000L),
                sourceIdleAlertMs(env),
                checkpointIntervalMs(env),
                checkpointTimeoutMs(env),
                maxConcurrentCheckpoints(env),
                restartMaxAttempts(env),
                restartDelayMs(env),
                checkpointDir(env),
                env.getOrDefault("SIGNAL_CANDIDATES_TABLE", "Signal_Candidates"),
                env.getOrDefault("SIGNAL_CURRENT_TABLE", "Signal_Candidates_current"),
                env.getOrDefault("SIGNAL_STRATEGY_ID",
                        SignalCandidatesTableColumns.CANONICAL_STRATEGY_ID),
                env.getOrDefault("SIGNAL_STRATEGY_VERSION",
                        SignalCandidatesTableColumns.CANONICAL_STRATEGY_VERSION),
                env.getOrDefault("SIGNAL_RULE_ID",
                        SignalCandidatesTableColumns.CANONICAL_RULE_ID),
                signalLookbackCandles(env),
                signalQuantity(env),
                env.getOrDefault("FORMING_RULE_ID",
                        SignalCandidatesTableColumns.CANONICAL_FORMING_RULE_ID),
                formingLookbackCandles(env),
                previewEnabled,
                positiveLong(env, "PREVIEW_INTERVAL_MS", 1_000L),
                positiveLong(env, "PREVIEW_TTL_MS", 60_000L),
                earlySignalEnabled,
                env.getOrDefault("EARLY_SIGNAL_RULE",
                        SignalCandidatesTableColumns.CANONICAL_RULE_ID),
                positiveLong(env, "EARLY_SIGNAL_CONFIRM_AFTER_MS", 4_000L),
                env.getOrDefault("FORMING_BAR_TABLE", "forming_bar"),
                positiveLong(env, "FORMING_BAR_WRITE_BATCH_MS", 250L),
                // CHG-121 (2026-09-01): durable tentative-marker table for F4
                // crash reconciliation. Empty string DISABLES markers (the
                // pre-CHG-121 behavior — embedded tests, legacy runs).
                env.getOrDefault("SIGNAL_TENTATIVE_MARKERS_TABLE",
                        "Signal_Tentative_Markers"),
                env.getOrDefault("POSITION_STATE_TABLE", "Position_State"),
                env.getOrDefault("OTEL_COLLECTOR_HOST", "otel-collector:4318"),
                stateRecoveryPath(env),
                mode == StartupMode.FULL_REPLAY,
                deploymentEnv(env),
                stateBackend(env),
                savepointDir(env),
                stateBackendLocalDirs(env),
                stateBackendManagedMemory(env),
                taskManagerMemoryManagedSize(env),
                taskManagerNetworkMemoryMax(env),
                parallelism(env),
                s3Endpoint(env),
                s3AccessKey(env),
                s3SecretKey(env),
                s3Region(env),
                s3PathStyle(env),
                sinkWriteStallTimeoutMs(env),
                parseWriterRetries(env),
                env.getOrDefault("TRADE_DECISIONS_TABLE", "Trade_Decisions"),
                env.getOrDefault("TRADE_INSTRUCTION_STATE_TABLE", "trade_instruction_state"),
                booleanValue(env, "TRADE_DECISIONS_ENABLED", false),
                executionIntentTable(env, executionIntentEnabled),
                executionAccountScopeId(env, executionIntentEnabled),
                executionPartitionId(env, executionIntentEnabled),
                executionProductType(env, executionIntentEnabled),
                executionTimeInForce(env, executionIntentEnabled),
                executionIntentEnabled,
                multiTfEnabled,
                liveSnapshotIntervalMs,
                multiTfSessionBypass,
                multiTfSignalContextEnabled,
                candleLiveTable,
                candleClosedTable,
                mode);
    }

    /**
     * Bootstrap servers (prod wiring): accepts both {@code FLUSS_BOOTSTRAP_SERVERS}
     * (SignalJob contract) and {@code FLUSS_BOOTSTRAP} (compose/stack Fluss alias).
     * The alias lets the Flink job resolve {@code fluss-coordinator:9123} via compose
     * DNS without requiring a duplicate env key; explicit SERVERS wins.
     */
    private static String bootstrapServers(Map<String, String> env) {
        String v = env.get("FLUSS_BOOTSTRAP_SERVERS");
        if (v != null && !v.isBlank()) return v.trim();
        v = env.get("FLUSS_BOOTSTRAP");
        if (v != null && !v.isBlank()) return v.trim();
        return "localhost:9123";
    }

    /**
     * The validated startup mode (record component, set by {@link #from(Map)}):
     * {@link StartupMode#RESTORE} resumes from STATE_RECOVERY_PATH;
     * {@link StartupMode#FULL_REPLAY} explicitly accepts the offset-0 replay;
     * {@link StartupMode#LATEST} starts from the current offset, skipping the
     * accumulated LOG backlog (2026-08-29 — clean/loadtest runs).
     */
    @Override
    public StartupMode startupMode() {
        return startupMode;
    }

    /** Preview visibility enabled (low-latency candles Phase 1). */
    public boolean previewEnabled() {
        return previewEnabled;
    }

    /** Preview emission cadence (default 1s). */
    public long previewIntervalMs() {
        return previewIntervalMs;
    }

    /** Preview row TTL in the KV table (default 60s — auto-expiry). */
    public long previewTtlMs() {
        return previewTtlMs;
    }

    /** Early-signal path enabled (low-latency candles Phase 2 — requires previews). */
    public boolean earlySignalEnabled() {
        return earlySignalEnabled;
    }

    /** Early-signal rule identity (defaults to the canonical breakout rule —
     *  consistent with detection; the supersession chain stays coherent). */
    public String earlySignalRuleId() {
        return earlySignalRuleId;
    }

    /** Confirm-window shortening threshold (Phase 3): after this much
     *  sustained preview hold, the tentative is confirmed early (default 4s —
     *  4 consecutive 1s previews). */
    public long earlySignalConfirmAfterMs() {
        return earlySignalConfirmAfterMs;
    }

    /** Preview table schema version (pinned from the shared contract). */
    public String previewSchemaVersion() {
        return CandlePreviewTableSchema.ROW_SCHEMA_VERSION;
    }

    /** Multi-timeframe aggregator enabled (Phase 4, default false). */
    public boolean multiTfEnabled() {
        return multiTfEnabled;
    }

    /** Live snapshot interval for the multi-TF aggregator (default 1000ms). */
    public long liveSnapshotIntervalMs() {
        return liveSnapshotIntervalMs;
    }

    /** Fluss table for candle_live (default candle_live). */
    public String candleLiveTable() {
        return candleLiveTable;
    }

    /** Fluss table for candle_closed (default candle_closed). */
    public String candleClosedTable() {
        return candleClosedTable;
    }

    /** Startup-mode gate (CANDLE-KV-REPLAY-001 A3.3). */
    public enum StartupMode {
        /** Resume from STATE_RECOVERY_PATH (previous run's last checkpoint). */
        RESTORE,
        /** Explicit offset-0 replay (ALLOW_FULL_REPLAY=true, no restore path). */
        FULL_REPLAY,
        /** No restore and no replay: start from LATEST, skip the accumulated
         *  LOG backlog (2026-08-29 — added so clean/loadtest runs measure only
         *  live data instead of replaying the whole table at max speed). */
        LATEST
    }

    /**
     * Fail-closed startup-mode validation: a restart must either restore from
     * a checkpoint or explicitly accept a full replay — a silent offset-0
     * replay is forbidden (it re-emits the whole backlog, balloons the dedup
     * state past the pinned checkpoint contract, and appends duplicate candle
     * rows to the immutable LOG — observed 2026-08-10).
     *
     * <p>T7 strict gate (G1 Safety): the missing-mode path MUST fail with
     * explicit code {@code F005} — no silent offset-0 replay. Every
     * validation branch is fail-closed; the missing-mode message carries
     * {@code [F005]} so operators and tests can assert the gate without
     * string-fragile substring checks on generic text.
     */
    private static StartupMode validateStartupMode(Map<String, String> env) {
        String path = env.get("STATE_RECOVERY_PATH");
        boolean hasPath;
        if (path == null) {
            hasPath = false;
        } else {
            String trimmed = path.trim();
            if (trimmed.isEmpty()) {
                throw new IllegalStateException(
                        "[F005] Config STATE_RECOVERY_PATH is present but blank — a restore path must be a real path "
                                + "(CANDLE-KV-REPLAY-001 A3.3)");
            }
            hasPath = true;
        }

        String replayRaw = env.get("ALLOW_FULL_REPLAY");
        boolean replay;
        if (replayRaw == null) {
            replay = false;
        } else {
            String trimmed = replayRaw.trim();
            if (trimmed.isEmpty()) {
                throw new IllegalStateException(
                        "[F005] Config ALLOW_FULL_REPLAY is present but blank — use 'true' or 'false' "
                                + "(CANDLE-KV-REPLAY-001 A3.3)");
            }
            if (!trimmed.equalsIgnoreCase("true") && !trimmed.equalsIgnoreCase("false")) {
                throw new IllegalStateException(
                        "[F005] Config ALLOW_FULL_REPLAY must be 'true' or 'false' (case-insensitive), got '"
                                + trimmed + "' — no unsafe default may be substituted (CANDLE-KV-REPLAY-001 A3.3)");
            }
            replay = Boolean.parseBoolean(trimmed);
        }

        if (hasPath && replay) {
            throw new IllegalStateException(
                    "[F005] Config STATE_RECOVERY_PATH and ALLOW_FULL_REPLAY=true are both set — a restore and a "
                            + "full replay cannot be combined (CANDLE-KV-REPLAY-001 A3.3); unset one of them");
        }
        if (!hasPath && !replay) {
            // No restore, no explicit replay: start from LATEST. This is NOT a
            // silent offset-0 replay (the original F005 concern) — the source
            // skips the accumulated LOG backlog entirely. Safe for clean runs;
            // production restarts still require STATE_RECOVERY_PATH (RESTORE).
            return StartupMode.LATEST;
        }
        return hasPath ? StartupMode.RESTORE : StartupMode.FULL_REPLAY;
    }

    /** Trims the restore path (validation already rejected blank values). */
    private static String stateRecoveryPath(Map<String, String> env) {
        String raw = env.get("STATE_RECOVERY_PATH");
        return raw == null ? null : raw.trim();
    }

    private static int signalLookbackCandles(Map<String, String> env) {
        int value = intValue(env, "SIGNAL_LOOKBACK_CANDLES", 20);
        if (value < 2) {
            throw new IllegalStateException("Config SIGNAL_LOOKBACK_CANDLES must be >= 2 "
                    + "(the rule compares against the previous completed candles), got " + value);
        }
        return value;
    }

    private static long signalQuantity(Map<String, String> env) {
        long value = longValue(env, "SIGNAL_QUANTITY", 1L);
        if (value <= 0) {
            throw new IllegalStateException("Config SIGNAL_QUANTITY must be > 0, got " + value);
        }
        return value;
    }

    /**
     * Forming-bar placeholder detector lookback (Slice 2.2, Phase C). The
     * mirrored breakout rule compares the live forming bar against the
     * {@code high}s/{@code close}s of the previous completed candles; the
     * lookback must be &ge; 2 (a one-candle lookback would compare against
     * the candle still forming's predecessor only, and the warm-up gate needs
     * at least one completed candle before any comparison). Default 5 = 75 s
     * of completed history. PLACEHOLDER tuning key — the real strategy
     * replaces the rule without changing the pipeline.
     */
    private static int formingLookbackCandles(Map<String, String> env) {
        int value = intValue(env, "FORMING_LOOKBACK_CANDLES", 5);
        if (value < 2) {
            throw new IllegalStateException("Config FORMING_LOOKBACK_CANDLES must be >= 2 "
                    + "(the rule compares against the previous completed candles), got " + value);
        }
        return value;
    }

    /**
     * Checkpoint interval (P2, 2026-08-29): production-pinned to
     * {@link PlatformConfig#CHECKPOINT_INTERVAL_MS}; dev tunable in
     * 1000..600000 ms (low intervals speed up dev checkpoint cycling).
     */
    private static long checkpointIntervalMs(Map<String, String> env) {
        if (isProduction(env)) {
            return requirePinnedLong(env, "CHECKPOINT_INTERVAL_MS",
                    PlatformConfig.CHECKPOINT_INTERVAL_MS);
        }
        long v = longValue(env, "CHECKPOINT_INTERVAL_MS", PlatformConfig.CHECKPOINT_INTERVAL_MS);
        if (v < 1_000 || v > 600_000) {
            throw new IllegalStateException("Config CHECKPOINT_INTERVAL_MS must be in "
                    + "1000..600000 (dev), got " + v);
        }
        return v;
    }

    /**
     * Checkpoint timeout (P2, 2026-08-29): production-pinned to
     * {@link PlatformConfig#CHECKPOINT_TIMEOUT_MS}; dev tunable in
     * 1000..600000 ms.
     */
    private static long checkpointTimeoutMs(Map<String, String> env) {
        if (isProduction(env)) {
            return requirePinnedLong(env, "CHECKPOINT_TIMEOUT_MS",
                    PlatformConfig.CHECKPOINT_TIMEOUT_MS);
        }
        long v = longValue(env, "CHECKPOINT_TIMEOUT_MS", PlatformConfig.CHECKPOINT_TIMEOUT_MS);
        if (v < 1_000 || v > 600_000) {
            throw new IllegalStateException("Config CHECKPOINT_TIMEOUT_MS must be in "
                    + "1000..600000 (dev), got " + v);
        }
        return v;
    }

    /**
     * Max concurrent checkpoints (P2, 2026-08-29): production-pinned to
     * {@link PlatformConfig#MAX_CONCURRENT_CHECKPOINTS}; dev tunable in 1..4.
     */
    private static int maxConcurrentCheckpoints(Map<String, String> env) {
        if (isProduction(env)) {
            return requirePinnedInt(env, "MAX_CONCURRENT_CHECKPOINTS",
                    PlatformConfig.MAX_CONCURRENT_CHECKPOINTS);
        }
        int v = intValue(env, "MAX_CONCURRENT_CHECKPOINTS",
                PlatformConfig.MAX_CONCURRENT_CHECKPOINTS);
        if (v < 1 || v > 4) {
            throw new IllegalStateException("Config MAX_CONCURRENT_CHECKPOINTS must be in "
                    + "1..4 (dev), got " + v);
        }
        return v;
    }

    /**
     * Fixed-delay restart strategy (CHECKPOINT_RESTART_STRATEGY, dossier
     * config contract). Dev defaults {@code RESTART_MAX_ATTEMPTS} to
     * {@link PlatformConfig#RESTART_MAX_ATTEMPTS} and allows overrides
     * (failure-injection integration tests use low attempts to fail fast);
     * {@code DEPLOYMENT_ENV=production} pins the exact value and requires it
     * explicit — a deployment cannot silently raise the retry budget or widen
     * the delay (bounded retry only, never unbounded).
     */
    private static int restartMaxAttempts(Map<String, String> env) {
        if (isProduction(env)) {
            return requirePinnedInt(env, "RESTART_MAX_ATTEMPTS",
                    PlatformConfig.RESTART_MAX_ATTEMPTS);
        }
        return intValue(env, "RESTART_MAX_ATTEMPTS", PlatformConfig.RESTART_MAX_ATTEMPTS);
    }

    /**
     * Fixed-delay restart delay (see {@link #restartMaxAttempts(Map)}): dev
     * tuning default 30 s, production-pinned to
     * {@link PlatformConfig#RESTART_DELAY_MS} and required explicit.
     */
    private static long restartDelayMs(Map<String, String> env) {
        if (isProduction(env)) {
            return requirePinnedLong(env, "RESTART_DELAY_MS", PlatformConfig.RESTART_DELAY_MS);
        }
        return longValue(env, "RESTART_DELAY_MS", PlatformConfig.RESTART_DELAY_MS);
    }

    private static long requirePinnedLong(Map<String, String> env, String key, long pinned) {
        String raw = env.get(key);
        if (raw == null) {
            throw new IllegalStateException("Missing required config " + key
                    + " (pinned to " + pinned + ") — no unsafe default may be substituted");
        }
        long value = Long.parseLong(raw.trim());
        if (value != pinned) {
            throw new IllegalStateException("Config " + key + " must equal " + pinned
                    + " (fixed scope), got " + value);
        }
        return value;
    }

    private static int requirePinnedInt(Map<String, String> env, String key, int pinned) {
        return (int) requirePinnedLong(env, key, pinned);
    }

    /**
     * Canonical version-column gate (tracker 14 P2 — CANDLE-CANONICAL-001).
     * The emitted candle KV rows carry the algorithm/configuration pair; a
     * <em>deviating</em> pair must fail startup rather than silently change
     * row identity for replay evidence. A missing key falls back to the
     * canonical default (documented in the record javadoc — the default IS
     * the canonical pair); a blank or non-canonical value is fatal. Changing
     * the pair is a governed change in {@link CandleTableSchema}, not a
     * tuning knob.
     */
    private static String requireCanonicalVersion(Map<String, String> env, String key,
            String canonical) {
        String raw = env.get(key);
        if (raw == null) {
            return canonical; // documented default — the default IS the canonical pair
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalStateException("Config " + key + " is present but blank — the "
                    + "canonical pair must be explicit (tracker 14 P2, CANDLE-CANONICAL-001)");
        }
        if (!canonical.equals(trimmed)) {
            throw new IllegalStateException("Config " + key + " must equal the canonical value '"
                    + canonical + "' (tracker 14 P2, CANDLE-CANONICAL-001), got '" + trimmed
                    + "' — a deviating version column would change emitted row identity "
                    + "and corrupt replay evidence");
        }
        return trimmed;
    }

    /**
     * Deployment environment (tracker 14 P4.1 / G4 T9). Defaults to {@code dev} —
     * the fail-closed direction is toward dev-local, never silently toward
     * production: a production launch must set {@code DEPLOYMENT_ENV=production}
     * explicitly (alias {@code PROFILE=prod} accepted, also normalizes
     * {@code PROFILE=prod} → {@code production} and case-insensitive), which
     * then enforces the RocksDB backend and durable encrypted S3
     * checkpoint/savepoint URIs (P4.2 / T9). The default keeps the live dev run
     * (HashMap state, local checkpoints on the flink-checkpoints volume,
     * {@code file:///checkpoints}) restart-compatible.
     *
     * <p>G4 Durability T9: production requires {@code CHECKPOINT_DIR=s3://} or
     * {@code s3a://} with bucket encryption at rest (AES256 via R2/S3 bucket
     * policy); a missing or local path in production fails fast with an
     * actionable message so a mis-configured swarm deploy never silently loses
     * checkpoints on container loss.
     */
    private static String deploymentEnv(Map<String, String> env) {
        String raw = env.get("DEPLOYMENT_ENV");
        if (raw == null) {
            raw = env.get("PROFILE");
        }
        if (raw == null) {
            return "dev";
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalStateException("Config DEPLOYMENT_ENV is present but blank — "
                    + "use 'dev' or 'production' (PROFILE=prod accepted as alias)");
        }
        String normalized = trimmed.toLowerCase();
        if ("prod".equals(normalized)) {
            normalized = "production";
        }
        if (!"dev".equals(normalized) && !"production".equals(normalized)) {
            throw new IllegalStateException("Config DEPLOYMENT_ENV must be 'dev' or 'production' "
                    + "(PROFILE=prod accepted as alias), got '" + trimmed
                    + "' — no unsafe default may be substituted");
        }
        return normalized;
    }

    /** Returns true when the deployment environment resolves to production (via DEPLOYMENT_ENV or PROFILE alias). */
    private static boolean isProduction(Map<String, String> env) {
        return "production".equals(deploymentEnv(env));
    }

    /**
     * State backend (tracker 14 P4.1). {@code rocksdb} is the pinned production
     * backend; {@code hashmap} is dev-only and FAILS in production (heap state
     * blew past the pinned checkpoint contract on 2026-08-10). The dev default
     * is {@code hashmap} to keep the live dev run's checkpoints
     * (HashMapStateBackend) restorable on the next operator restart; dev may
     * choose {@code rocksdb} explicitly.
     */
    private static String stateBackend(Map<String, String> env) {
        String envRaw = env.get("STATE_BACKEND");
        String backend;
        if (envRaw == null) {
            backend = isProduction(env) ? "rocksdb" : "hashmap";
        } else {
            backend = envRaw.trim();
        }
        if (backend.isEmpty()) {
            throw new IllegalStateException("Config STATE_BACKEND is present but blank — "
                    + "use 'rocksdb' or 'hashmap'");
        }
        if (!"rocksdb".equals(backend) && !"hashmap".equals(backend)) {
            throw new IllegalStateException("Config STATE_BACKEND must be 'rocksdb' (production "
                    + "pin) or 'hashmap' (dev only), got '" + backend + "' — no unsafe default "
                    + "may be substituted");
        }
        String envName = deploymentEnv(env);
        if ("production".equals(envName) && "hashmap".equals(backend)) {
            throw new IllegalStateException("Config STATE_BACKEND=hashmap is forbidden in "
                    + "DEPLOYMENT_ENV=production — heap/HashMap state blew past the pinned "
                    + "checkpoint contract on 2026-08-10 (tracker 14 P4.1); use rocksdb");
        }
        return backend;
    }

    /**
     * Durable checkpoint URI (tracker 14 P4.2 / G4 T9). Production REQUIRES an
     * encrypted S3 object-store URI ({@code s3://} or {@code s3a://}) — local
     * {@code file://}, {@code /tmp}, or {@code /checkpoints} checkpoints
     * silently evaporate on container loss and must never be substituted in
     * production. Dev accepts any path and keeps the local volume default
     * ({@code file:///checkpoints} on the {@code flink-checkpoints} Docker
     * volume, or {@code /tmp/signaljob-checkpoints} for host runs). The
     * production bucket must have at-rest encryption (R2 default AES256 or S3
     * SSE-S3/SSE-KMS) — enforced via bucket policy and wired in Flink as
     * {@code fs.s3a.server-side-encryption-algorithm=AES256} in
     * {@code SignalJob.applyRuntimeOptions}.
     *
     * <p>G4 Durability: prod without s3:// fails fast with an actionable message;
     * dev never requires S3 — the local volume is intentional for single-host
     * development.
     */
    private static String checkpointDir(Map<String, String> env) {
        String raw = env.get("CHECKPOINT_DIR");
        String dir = raw == null ? null : raw.trim();
        if (isProduction(env)) {
            if (dir == null || dir.isEmpty()) {
                throw new IllegalStateException("Config CHECKPOINT_DIR is required in "
                        + "DEPLOYMENT_ENV=production (or PROFILE=prod) — durable encrypted S3 checkpoint storage is mandatory "
                        + "(s3:// or s3a://, bucket encryption AES256) (tracker 14 P4.2 / G4 T9). "
                        + "Set CHECKPOINT_DIR=s3://<bucket>/flink-checkpoints (or s3a://) and provide "
                        + "S3_ENDPOINT (or R2_ENDPOINT) + AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY via Swarm secrets/env. "
                        + "Dev keeps local volume without S3: file:///checkpoints via the flink-checkpoints volume — no action needed for dev.");
            }
            if (!dir.startsWith("s3://") && !dir.startsWith("s3a://")) {
                throw new IllegalStateException("Config CHECKPOINT_DIR must be an encrypted S3 object-store "
                        + "URI (s3:// or s3a://) in DEPLOYMENT_ENV=production (or PROFILE=prod), got '" + dir
                        + "' — a local path (file://, /tmp, /checkpoints) silently loses checkpoints on container/VM loss "
                        + "and violates G4 durability (encrypted S3 mandatory). "
                        + "Use s3://<bucket>/flink-checkpoints with bucket encryption AES256 (R2 default, S3 SSE-S3) "
                        + "(tracker 14 P4.2 / T9). Dev may use file:///checkpoints (local volume) — no S3 required.");
            }
        }
        return dir;
    }

    /**
     * Durable savepoint URI (tracker 14 P4.2 / G4 T9), kept separate from the
     * checkpoint directory. Production requires encrypted S3 when set; dev accepts any.
     * Alias PROFILE=prod is honored alongside DEPLOYMENT_ENV=production.
     */
    private static String savepointDir(Map<String, String> env) {
        String raw = env.get("SAVEPOINT_DIR");
        String dir = raw == null ? null : raw.trim();
        // T9-errata 2026-08-22: compose injects SAVEPOINT_DIR="" (present-but-
        // blank) — treat blank as unset so the deployment config stays
        // authoritative and `execution.checkpointing.savepoint-dir` is never
        // set to an empty Path (Flink rejects it at JobMaster init).
        if (dir != null && dir.isEmpty()) {
            dir = null;
        }
        if (isProduction(env) && dir != null && !dir.isEmpty()
                && !dir.startsWith("s3://") && !dir.startsWith("s3a://")) {
            throw new IllegalStateException("Config SAVEPOINT_DIR must be an encrypted S3 object-store URI "
                    + "(s3:// or s3a://) in DEPLOYMENT_ENV=production (or PROFILE=prod), got '" + dir
                    + "' (tracker 14 P4.2 / G4 T9) — a local savepoint path violates encrypted S3 durability");
        }
        return dir;
    }

    /** RocksDB local state directory (fast disk), optional (tracker 14 P4.1). */
    private static String stateBackendLocalDirs(Map<String, String> env) {
        String raw = env.get("STATE_BACKEND_LOCAL_DIRS");
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalStateException("Config STATE_BACKEND_LOCAL_DIRS is present but blank "
                    + "(tracker 14 P4.1)");
        }
        return trimmed;
    }

    /**
     * Optional RocksDB managed-memory budget for local/embedded execution
     * (2026-08-17, E2E root cause). Under a real deployment the TM memory is
     * set by flink-conf.yaml (dev compose: {@code taskmanager.memory.process.size}
     * 3g → managed fraction ≈ 1.2 GB); under LOCAL execution (no conf file)
     * Flink falls back to 128 MB TOTAL managed memory, which starves the
     * RocksDB block cache for the Design-B dedup envelope (~628 MB at
     * 20 480 t/s × 300 s) and collapses throughput to ≈ the feed rate.
     *
     * <p>This key is a passthrough for exactly that case: when set, the value
     * (a Flink memory string, e.g. {@code 2048m} / {@code 2g}) is written to
     * {@code taskmanager.memory.managed.size} in
     * {@code SignalJob.applyRuntimeOptions}. When unset the key stays absent
     * and the deployment (flink-conf.yaml) stays authoritative — production is
     * never affected by a default. Validated to be a non-blank string; the
     * actual memory format is enforced by Flink's own parser at startup.
     */
    private static String taskManagerMemoryManagedSize(Map<String, String> env) {
        String raw = env.get("TASK_MANAGER_MEMORY_MANAGED_SIZE");
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalStateException("Config TASK_MANAGER_MEMORY_MANAGED_SIZE must be a "
                    + "Flink memory string (e.g. '2048m' or '2g'), got empty");
        }
        return trimmed;
    }

    /**
     * Optional taskmanager.memory.network.max override for embedded/local runs
     * (E2E). Local MiniCluster execution defaults network memory to 64 MB —
     * at higher parallelism the connected forming-bar branch needs more
     * buffers (measured 2026-08-17: p16 deploy failed with "required 17, but
     * only 13 available"). Unset → the deployment stays authoritative.
     */
    private static String taskManagerNetworkMemoryMax(Map<String, String> env) {
        String raw = env.get("TASK_MANAGER_NETWORK_MEMORY_MAX");
        if (raw == null) {
            return null;
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalStateException("Config TASK_MANAGER_NETWORK_MEMORY_MAX must be a "
                    + "Flink memory string (e.g. '512m' or '1g'), got empty");
        }
        return trimmed;
    }

    /**
     * RocksDB managed-memory toggle (tracker 14 P4.1); explicit boolean, no
     * unsafe default substitution when the key is present but unparsable.
     */
    private static boolean stateBackendManagedMemory(Map<String, String> env) {
        String raw = env.get("STATE_BACKEND_MANAGED_MEMORY");
        if (raw == null) {
            return true;
        }
        String trimmed = raw.trim();
        if (!trimmed.equalsIgnoreCase("true") && !trimmed.equalsIgnoreCase("false")) {
            throw new IllegalStateException("Config STATE_BACKEND_MANAGED_MEMORY must be 'true' "
                    + "or 'false' (case-insensitive), got '" + trimmed + "'");
        }
        return Boolean.parseBoolean(trimmed);
    }

    /**
     * Explicit task parallelism — streaming-3000 T3 G3 Compute (p=8).
     * Default 8 matches the 16 Fluss buckets → 8 TaskSlots mapping (2:1,
     * hash(token) rebalance) and the 3000-instrument envelope (1024 tokens
     * per slot at design cap). Single-host dev keeps 8 slots (taskmanager
     * .numberOfTaskSlots=8) so p=8 runs without slot starvation; tests may
     * override with explicit PARALLELISM=1/2/4 for focused unit coverage.
     */
    private static int parallelism(Map<String, String> env) {
        int value = intValue(env, "PARALLELISM", 8);
        if (value <= 0) {
            throw new IllegalStateException("Config PARALLELISM must be > 0, got " + value);
        }
        return value;
    }

    /**
     * Object-store checkpoint/savepoint endpoint (tracker 14 P4.2). Non-null
     * ONLY when CHECKPOINT_DIR or SAVEPOINT_DIR is an S3 object-store URI
     * ({@code s3://}/{@code s3a://}). Fail-closed: an object-store URI without
     * an endpoint + credentials is rejected at startup — credentials come from
     * secret injection via env (never committed files, never logged). The
     * endpoint comes from {@code S3_ENDPOINT}, with {@code R2_ENDPOINT} as the
     * Cloudflare-R2 fallback (R2 speaks the S3 API; the endpoint is the
     * jurisdiction URL, e.g. {@code https://<account>.r2.cloudflarestorage.com}).
     */
    private static String s3Endpoint(Map<String, String> env) {
        String cp = env.get("CHECKPOINT_DIR");
        String sp = env.get("SAVEPOINT_DIR");
        boolean objectStore = (cp != null && (cp.startsWith("s3://") || cp.startsWith("s3a://")))
                || (sp != null && (sp.startsWith("s3://") || sp.startsWith("s3a://")));
        if (!objectStore) {
            return null;
        }
        String endpoint = env.get("S3_ENDPOINT");
        if (endpoint == null || endpoint.trim().isEmpty()) {
            endpoint = env.get("R2_ENDPOINT");
        }
        // G4 T9 / Swarm secret support: credentials may arrive via *_FILE (Docker Swarm secrets
        // mounted at /run/secrets/...). Prefer direct env, fall back to file content.
        String access = resolveSecret(env, "AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID_FILE");
        String secret = resolveSecret(env, "AWS_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY_FILE");
        if (endpoint == null || endpoint.trim().isEmpty()
                || access == null || access.trim().isEmpty()
                || secret == null || secret.trim().isEmpty()) {
            throw new IllegalStateException("Config CHECKPOINT_DIR/SAVEPOINT_DIR uses an S3 "
                    + "object-store URI but S3_ENDPOINT (or R2_ENDPOINT), AWS_ACCESS_KEY_ID and "
                    + "AWS_SECRET_ACCESS_KEY are not all set — checkpoint credentials must come "
                    + "from secret injection, never committed files (tracker 14 P4.2). "
                    + "Provide via env or Swarm secrets (AWS_ACCESS_KEY_ID_FILE=/run/secrets/aws_access_key_id, AWS_SECRET_ACCESS_KEY_FILE=/run/secrets/aws_secret_access_key)");
        }
        return endpoint.trim();
    }

    /**
     * Resolve a secret value: direct env first, then {@code *_FILE} mounted secret file
     * (Swarm secrets model). Returns trimmed content or null.
     */
    private static String resolveSecret(Map<String, String> env, String directKey, String fileKey) {
        String direct = env.get(directKey);
        if (direct != null && !direct.trim().isEmpty()) {
            return direct.trim();
        }
        String filePath = env.get(fileKey);
        if (filePath != null && !filePath.trim().isEmpty()) {
            try {
                String content = java.nio.file.Files.readString(java.nio.file.Path.of(filePath.trim())).trim();
                if (!content.isEmpty()) {
                    return content;
                }
            } catch (java.io.IOException e) {
                // Fall through to null — caller will emit actionable missing-credential message
            }
        }
        return null;
    }

    /** Static access key, present only when {@link #s3Endpoint(Map)} returned non-null. */
    private static String s3AccessKey(Map<String, String> env) {
        String resolved = resolveSecret(env, "AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID_FILE");
        return resolved;
    }

    /** Static secret key, present only when {@link #s3Endpoint(Map)} returned non-null. */
    private static String s3SecretKey(Map<String, String> env) {
        String resolved = resolveSecret(env, "AWS_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY_FILE");
        return resolved;
    }

    /**
     * S3 signing region (tracker 14 P4.2). Defaults to {@code auto} — the
     * correct choice for Cloudflare R2 (signing is per-endpoint, not
     * region-scoped). Explicit AWS regions (e.g. {@code us-east-1}) override.
     */
    private static String s3Region(Map<String, String> env) {
        String raw = env.get("AWS_REGION");
        return raw == null || raw.trim().isEmpty() ? "auto" : raw.trim();
    }

    /**
     * Path-style S3 addressing (tracker 14 P4.2). Defaults to {@code true} —
     * required for R2 (no virtual-hosted-style buckets); explicit
     * {@code false} opts into virtual-hosted style for real AWS.
     */
    private static boolean s3PathStyle(Map<String, String> env) {
        String raw = env.get("S3_PATH_STYLE");
        if (raw == null) {
            return true;
        }
        String trimmed = raw.trim();
        if (!trimmed.equalsIgnoreCase("true") && !trimmed.equalsIgnoreCase("false")) {
            throw new IllegalStateException("Config S3_PATH_STYLE must be 'true' or 'false' "
                    + "(case-insensitive), got '" + trimmed + "'");
        }
        return Boolean.parseBoolean(trimmed);
    }

    /**
     * Sink write-path stall bound (tracker 14 box 682/116, governed pin in
     * {@link PlatformConfig#SINK_WRITE_STALL_TIMEOUT_MS}). Defaults to the
     * pinned 15000 ms; a present value must parse and be strictly positive —
     * a non-positive bound would let a stalled sink hang the job forever
     * (the exact failure the guard exists to bound).
     */
    private static long sinkWriteStallTimeoutMs(Map<String, String> env) {
        long value = longValue(env, "SINK_WRITE_STALL_TIMEOUT_MS",
                PlatformConfig.SINK_WRITE_STALL_TIMEOUT_MS);
        if (value <= 0) {
            throw new IllegalStateException("Config SINK_WRITE_STALL_TIMEOUT_MS must be > 0 "
                    + "(pinned default " + PlatformConfig.SINK_WRITE_STALL_TIMEOUT_MS
                    + "), got " + value);
        }
        return value;
    }

    /**
     * Dedup window (G-DEDUP-4, 2026-09-03 redesign): per-token recent-print
     * bound, pinned to {@link PlatformConfig#DEDUP_WINDOW_ENTRIES} in
     * production (a deployment cannot silently change repeat semantics); in
     * dev tunable within 100..100000 (the speed-test prototype uses a large
     * bound to measure the hot path, not trimming).
     */
    private static int dedupWindowEntries(Map<String, String> env) {
        if (isProduction(env)) {
            return (int) requirePinnedLong(env, "DEDUP_WINDOW_ENTRIES",
                    PlatformConfig.DEDUP_WINDOW_ENTRIES);
        }
        long v = longValue(env, "DEDUP_WINDOW_ENTRIES", PlatformConfig.DEDUP_WINDOW_ENTRIES);
        if (v < 100 || v > 100_000) {
            throw new IllegalStateException("Config DEDUP_WINDOW_ENTRIES must be in 100..100000 (dev), got "
                    + v + " (production pins " + PlatformConfig.DEDUP_WINDOW_ENTRIES + ")");
        }
        return (int) v;
    }

    /**
     * Candle window (P1, 2026-08-29): pinned to
     * {@link PlatformConfig#CANDLE_WINDOW_MS} in production; in dev it is
     * tunable within 1000..60000 ms.
     */
    private static long candleWindowMs(Map<String, String> env) {
        if (isProduction(env)) {
            return requirePinnedLong(env, "CANDLE_WINDOW_MS", PlatformConfig.CANDLE_WINDOW_MS);
        }
        long v = longValue(env, "CANDLE_WINDOW_MS", PlatformConfig.CANDLE_WINDOW_MS);
        if (v < 1_000 || v > 60_000) {
            throw new IllegalStateException("Config CANDLE_WINDOW_MS must be in 1000..60000 (dev), got "
                    + v + " (production pins " + PlatformConfig.CANDLE_WINDOW_MS + ")");
        }
        return v;
    }

    /**
     * Candle-preview table (K3, 2026-08-29). Default
     * {@link CandlePreviewTableSchema#TABLE}; an override must be a
     * non-blank identifier.
     */
    private static String previewTable(Map<String, String> env) {
        String v = env.get("PREVIEW_TABLE");
        if (v == null || v.isBlank()) {
            return CandlePreviewTableSchema.TABLE;
        }
        return v.trim();
    }

    /**
     * Fluss client writer retry budget (K2, 2026-08-29). Default 2; a present
     * value must parse and be >= 1 — a zero retry budget would make every
     * transient writer failure fatal.
     */
    private static int parseWriterRetries(Map<String, String> env) {
        String v = env.get("FLUSS_WRITER_RETRIES");
        if (v == null || v.isBlank()) {
            return 2;
        }
        int value;
        try {
            value = Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Config FLUSS_WRITER_RETRIES must be an integer, got '" + v + "'");
        }
        if (value < 1) {
            throw new IllegalStateException("Config FLUSS_WRITER_RETRIES must be >= 1, got " + value);
        }
        return value;
    }

    /**
     * Source idle-at-tail alert threshold (tracker 14 P7/P10, 2026-08-13).
     * Defaults to the governed default in {@link PlatformConfig#SOURCE_IDLE_ALERT_MS}
     * (60000 ms); a present value must parse and be strictly positive — a
     * non-positive threshold would alert on every scheduler pause, and a
     * threshold below SOURCE_IDLE_MS would alert during normal quiet-split
     * watermark idleness.
     */
    private static long sourceIdleAlertMs(Map<String, String> env) {
        long value = longValue(env, "SOURCE_IDLE_ALERT_MS", PlatformConfig.SOURCE_IDLE_ALERT_MS);
        if (value <= 0) {
            throw new IllegalStateException("Config SOURCE_IDLE_ALERT_MS must be > 0 "
                    + "(pinned default " + PlatformConfig.SOURCE_IDLE_ALERT_MS
                    + "), got " + value);
        }
        return value;
    }

    /**
     * Strict boolean (no unsafe default substitution when the key is present
     * but unparsable) — the pattern of {@link #stateBackendManagedMemory}.
     * Used for the SCH-19 decision-sink gate {@code TRADE_DECISIONS_ENABLED}
     * (default {@code false}: the ranking feed does not exist yet; the
     * machinery stays off until wired).
     */
    private static boolean booleanValue(Map<String, String> env, String key,
            boolean defaultValue) {
        String raw = env.get(key);
        if (raw == null) {
            return defaultValue;
        }
        String trimmed = raw.trim();
        if (!trimmed.equalsIgnoreCase("true") && !trimmed.equalsIgnoreCase("false")) {
            throw new IllegalStateException("Config " + key + " must be 'true' or 'false' "
                    + "(case-insensitive), got '" + trimmed + "'");
        }
        return Boolean.parseBoolean(trimmed);
    }

    private static String executionAccountScopeId(Map<String, String> env, boolean enabled) {
        String value = env.get("ACCOUNT_SCOPE_ID");
        if (!enabled) {
            return value == null ? null : value.trim();
        }
        return requiredExecutionText(value, "ACCOUNT_SCOPE_ID");
    }

    private static String executionIntentTable(Map<String, String> env, boolean enabled) {
        String value = env.get("EXECUTION_INTENT_TABLE");
        if (!enabled) {
            return value == null ? "Execution_Intent" : value.trim();
        }
        return requiredExecutionText(value == null ? "Execution_Intent" : value,
                "EXECUTION_INTENT_TABLE");
    }

    private static String executionPartitionId(Map<String, String> env, boolean enabled) {
        String value = env.get("EXECUTION_PARTITION_ID");
        if (!enabled) {
            return value == null ? "partition-0" : value.trim();
        }
        return requiredExecutionText(value, "EXECUTION_PARTITION_ID");
    }

    private static String executionProductType(Map<String, String> env, boolean enabled) {
        String value = env.get("EXECUTION_PRODUCT_TYPE");
        if (!enabled) {
            return value == null ? "CNC" : value.trim();
        }
        return requiredExecutionText(value, "EXECUTION_PRODUCT_TYPE");
    }

    private static String executionTimeInForce(Map<String, String> env, boolean enabled) {
        String value = env.get("EXECUTION_TIME_IN_FORCE");
        if (!enabled) {
            return value == null ? "DAY" : value.trim();
        }
        return requiredExecutionText(value, "EXECUTION_TIME_IN_FORCE");
    }

    private static String requiredExecutionText(String value, String key) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException("Config " + key + " must be non-blank when "
                    + "EXECUTION_INTENT_ENABLED=true");
        }
        return value.trim();
    }

    private static String stringEnv(Map<String, String> env, String key, String defaultValue) {
        String v = env.get(key);
        if (v == null || v.isBlank()) {
            return defaultValue;
        }
        return v.trim();
    }

    /**
     * Positive tuning keys (e.g. FORMING_BAR_WRITE_BATCH_MS): defaulted and
     * validated at startup — a missing key falls back to the documented
     * starting value, a present non-positive value is fatal (a zero cadence
     * would break the write contract). The DEC-038 dedup keys this helper
     * used to serve (DEDUP_CACHE_* / DEDUP_WRITE_* / DEDUP_CLEANUP_*) were
     * retired 2026-08-16 with the bounded-cache externalization contract
     * (design B: the dedup set is authoritative Flink keyed state).
     */
    private static long positiveLong(Map<String, String> env, String key, long defaultValue) {
        long value = longValue(env, key, defaultValue);
        if (value <= 0) {
            throw new IllegalStateException("Config " + key + " must be > 0 "
                    + "(default " + defaultValue + "), got " + value);
        }
        return value;
    }

    private static long longValue(Map<String, String> env, String key, long defaultValue) {
        String raw = env.get(key);
        return raw == null ? defaultValue : Long.parseLong(raw.trim());
    }

    private static int intValue(Map<String, String> env, String key, int defaultValue) {
        String raw = env.get(key);
        return raw == null ? defaultValue : Integer.parseInt(raw.trim());
    }
}
