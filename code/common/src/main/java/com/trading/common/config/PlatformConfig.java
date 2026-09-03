package com.trading.common.config;

import java.util.Map;
import java.util.Set;

/**
 * Centralized, versioned runtime configuration constants for the trading platform.
 *
 * <p>No numeric literals for these keys may be scattered through source files. Startup must
 * reject two values outright (see {@link #validateStartup()}):
 * <ul>
 *   <li>{@code DEDUP_WINDOW_ENTRIES} must equal 2000</li>
 *   <li>{@code CANDLE_WINDOW_MS} must equal 15000</li>
 * </ul>
 *
 * <p>Source: docs/08_implementation/01-foundation.md &rarr; "Required configuration constants"
 * (orig L37) and "Fixed scope" (orig L23).
 */
public final class PlatformConfig {

    private PlatformConfig() {}

    // ---- ingestion / workload profile ----
    public static final int BROKER_BASELINE_TICKS_PER_INSTRUMENT_PER_SEC = 20;
    /** R-263: single source of truth — delegates to FixedScope, no duplicate literal. */
    public static final int BROKER_MAX_TICKS_PER_INSTRUMENT_PER_SEC =
            FixedScope.MAX_TICKS_PER_INSTRUMENT_PER_SEC;
    public static final int INGESTION_MAX_BATCH_RECORDS = 1;
    public static final int INGESTION_MAX_BATCH_WAIT_MS = 0;
    // Note: MAX_PENDING_APPEND_RECORDS is owned by IngestionConfig (env default
    // 50,000, range 100..1,000,000) — no duplicate constant here. A stale 10,000
    // literal was removed 2026-08-13 (it was unused and contradicted the runtime).

    // ---- raw_table_1 schema contract ----
    /**
     * Authoritative {@code raw_table_1.schema_version} value — DDL 02_raw_table_1.sql
     * "Schema version: 3" (the 21-column v3 layout with event_day partitioning).
     * Ingestion writes
     * this value and SignalJob's {@code RAW_SCHEMA_VERSION} default derives from it,
     * so the producer label and the consumer default cannot drift apart.
     */
    public static final String RAW_TABLE_1_SCHEMA_VERSION = "3";

    // ---- dedup / candles (reject-startup values) ----
    // G-DEDUP-4 (2026-09-03 redesign): per-token recent-fingerprint bound for
    // the heap-window repeat filter (replaces the 60 s TTL pin).
    public static final int DEDUP_WINDOW_ENTRIES = 2000;
    public static final long CANDLE_WINDOW_MS = 15_000L;

    // ---- checkpointing ----
    public static final long CHECKPOINT_INTERVAL_MS = 10_000L;
    public static final long CHECKPOINT_TIMEOUT_MS = 30_000L;
    public static final int MAX_CONCURRENT_CHECKPOINTS = 1;

    // ---- fixed-delay restart strategy (CHECKPOINT_RESTART_STRATEGY) ----
    /**
     * Governed pins for the fixed-delay restart strategy (dossier config
     * contract): any production deployment SHALL use exactly 3 attempts with a
     * 30 s delay — bounded retry, never unbounded (REQ-FC-006 checkpoint
     * contract). Enforced by SignalJobConfig in
     * {@code DEPLOYMENT_ENV=production}: missing or deviating values fail
     * startup so a deployment cannot silently raise the retry budget or widen
     * the delay. Dev keeps them as tuning defaults (failure-injection
     * integration tests use low attempts to fail fast).
     */
    public static final int RESTART_MAX_ATTEMPTS = 3;
    public static final long RESTART_DELAY_MS = 30_000L;

    // ---- sink write-path (tracker 14 box 682/116, 2026-08-12; CHG-023 item 4, 2026-08-17) ----
    /**
     * Governed pin: the sink write-path stall bound. Passed to every Fluss
     * sink as the Fluss client's own {@code client.request-timeout} (CHG-023
     * item 4 removed the StallGuardedSink watchdog — the native client
     * timeout is the stall bound; a stalled write fails within this window
     * and the configured restart policy drives the job to terminal FAILED
     * rather than hanging it). Healthy-path writes complete in
     * milliseconds; 15000 ms is ~10x headroom. Change via the
     * PlatformConfig/tracker governed-change process, not a tuning knob.
     */
    public static final long SINK_WRITE_STALL_TIMEOUT_MS = 15_000L;

    /**
     * Source idle-at-tail alert threshold (tracker 14 P7/P10, 2026-08-13):
     * a restored source that sits at a frozen feed tail consumes ZERO records
     * for as long as the feed is stopped — correct idle-tail behavior, not a
     * stall (probe-verified 2026-08-13). This tuning default bounds how long
     * that silence must last before the Signal job's watermark-level watchdog
     * logs a WARN and ships a {@code compute.source.idle.at.tail} delta so the
     * idle tail is observable instead of being misread as a hang. Deliberately
     * larger than SOURCE_IDLE_MS (15000, watermark idleness) so normal quiet-
     * split behavior never alerts.
     */
    public static final long SOURCE_IDLE_ALERT_MS = 60_000L;

    // ---- JVM / container memory ----
    public static final int JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT = 65;
    public static final int NON_HEAP_MEMORY_RESERVE_PERCENT = 35;
    public static final int CONTAINER_MEMORY_ALERT_PERCENT = 85;

    // ---- signal job ----
    public static final int MAX_ACTIVE_CANDIDATES_PER_INSTRUMENT = 1;

    /**
     * {@code MAX_PENDING_APPEND_BYTES = min(201326592, floor(container_memory_limit_bytes * 0.10))}.
     * Capped at 192 MiB (T2 streaming-3000: 64M → 192M) so very large container limits do not over-buffer
     * beyond the 3k tunable bound. Env overrides via MAX_PENDING_APPEND_BYTES / PENDING_MAX_BYTES.
     *
     * <p>R-199: a non-positive container limit (unreadable cgroup surfaced as 0,
     * or a misconfigured value) previously produced a <= 0 result from
     * {@code Math.min} — silently disabling the byte ceiling. Fail fast instead.
     */
    public static long maxPendingAppendBytes(long containerMemoryLimitBytes) {
        if (containerMemoryLimitBytes <= 0) {
            throw new IllegalArgumentException(
                    "containerMemoryLimitBytes must be positive, got: "
                    + containerMemoryLimitBytes);
        }
        long derived = (long) Math.floor(containerMemoryLimitBytes * 0.10);
        return Math.min(201_326_592L, derived);
    }

    /**
     * The two constants whose value is load-bearing for correctness; any other value must abort
     * startup rather than degrade silently.
     *
     * <p>R-127: the old checks compared compile-time constants against their own
     * literals — dead code that could never trigger. This now validates the
     * <em>runtime</em> values supplied via environment (the actual override
     * vector a deploy could use), so the guard is real.
     */
    public static void validateStartup() {
        // G1 (2026-08-29): every declared config key must actually be read —
        // a config-defined-but-ignored key is a silent lie.
        ConfigGuard.assertAllKeysRead();
        // P1 (2026-08-29): the load-bearing pins apply in production only;
        // dev is tunable (SignalJobConfig enforces the dev ranges).
        if (isProductionEnv()) {
            validateLoadBearing("DEDUP_WINDOW_ENTRIES", envLong("DEDUP_WINDOW_ENTRIES"),
                    DEDUP_WINDOW_ENTRIES);
            validateLoadBearing("CANDLE_WINDOW_MS", envLong("CANDLE_WINDOW_MS"), CANDLE_WINDOW_MS);
        }
        // 09-production-swarm § JVM and memory configuration: inside a real
        // container, enforce the 65/35 contract so a mis-sized container aborts
        // startup instead of OOM-ing at runtime. Non-fatal on a bare JVM (no
        // bounded cgroup budget) — keeps dev/test unaffected.
        ContainerMemoryGuard.assertContainerMemoryContract();
    }

    private static boolean isProductionEnv() {
        String env = System.getenv("DEPLOYMENT_ENV");
        if (env == null) {
            env = System.getenv("DEPLOY_ENV");
        }
        return env != null && ("production".equalsIgnoreCase(env.trim())
                || "prod".equalsIgnoreCase(env.trim()));
    }

    private static void validateLoadBearing(String key, Long runtimeValue, long pinned) {
        if (runtimeValue != null && runtimeValue != pinned) {
            throw new IllegalStateException(
                key + " must be " + pinned + "; found " + runtimeValue
                    + ". Refusing to start (docs/08_implementation/01-foundation.md L37).");
        }
    }

    private static Long envLong(String key) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException(key + " must be a number, got: " + v);
        }
    }

    /**
     * Configuration invariant (orig L716): a required config value missing at load time means the
     * component is NOT ready. No unsafe default may be substituted.
     */
    public static void requirePresent(Map<String, String> loaded, Set<String> requiredKeys) {
        for (String key : requiredKeys) {
            String v = loaded.get(key);
            if (v == null || v.isBlank()) {
                throw new IllegalStateException(
                    "Required configuration '" + key + "' is missing or blank; component is not ready.");
            }
        }
    }
}
