package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.fluss.config.ConfigOptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 2026-09-26 S5→S6 latency workstream: the stage profile identified two native
 * cadence knobs behind the 316 ms S6 p50 — the Fluss scanner's fetch chunk
 * (~260 ms waiting in Fluss; client defaults 16 MiB request / 1 MiB per
 * bucket / 500 ms wait) and Flink's output-buffer flush timeout (~45 ms per
 * shuffle hop; default 100 ms), with every operator measured 88-95 % idle.
 * The tuned values are the new defaults (the change is the fix, not an
 * experiment flag); the env seam stays for A/B runs.
 */
@DisplayName("Raw-source fetch / buffer-flush latency tuning (2026-09-26)")
class SourceFetchLatencyTuningTest {

    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_WINDOW_ENTRIES", "200");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        return env;
    }

    @Test
    @DisplayName("defaults: 512 KiB request / 128 KiB per bucket / 20 ms wait; 10 ms buffer timeout")
    void tunedDefaults() {
        SignalJobConfig config = SignalJobConfig.from(env());

        assertEquals(512L * 1024, config.flussScannerFetchMaxBytes());
        assertEquals(128L * 1024, config.flussScannerFetchMaxBytesForBucket());
        assertEquals(20L, config.flussScannerFetchWaitMaxTimeMs());
        assertEquals(10L, config.bufferTimeoutMs());
    }

    @Test
    @DisplayName("env overrides win (A/B seam)")
    void envOverridesHonored() {
        Map<String, String> env = env();
        env.put("FLUSS_SCANNER_FETCH_MAX_BYTES", "1048576");
        env.put("FLUSS_SCANNER_FETCH_MAX_BYTES_FOR_BUCKET", "262144");
        env.put("FLUSS_SCANNER_FETCH_WAIT_MAX_TIME_MS", "50");
        env.put("BUFFER_TIMEOUT_MS", "25");
        SignalJobConfig config = SignalJobConfig.from(env);

        assertEquals(1048576L, config.flussScannerFetchMaxBytes());
        assertEquals(262144L, config.flussScannerFetchMaxBytesForBucket());
        assertEquals(50L, config.flussScannerFetchWaitMaxTimeMs());
        assertEquals(25L, config.bufferTimeoutMs());
    }

    @Test
    @DisplayName("non-positive buffer timeout fails closed with the key named")
    void nonPositiveBufferTimeoutRejected() {
        Map<String, String> env = env();
        env.put("BUFFER_TIMEOUT_MS", "0");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SignalJobConfig.from(env));
        assertTrue(e.getMessage().contains("BUFFER_TIMEOUT_MS"), e.getMessage());
    }

    @Test
    @DisplayName("per-bucket cap above the request cap fails closed (Fluss pair rule)")
    void perBucketAboveRequestCapRejected() {
        Map<String, String> env = env();
        env.put("FLUSS_SCANNER_FETCH_MAX_BYTES", "1024");
        env.put("FLUSS_SCANNER_FETCH_MAX_BYTES_FOR_BUCKET", "2048");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SignalJobConfig.from(env));
        assertTrue(e.getMessage().contains("FLUSS_SCANNER_FETCH_MAX_BYTES_FOR_BUCKET"),
                e.getMessage());
    }

    @Test
    @DisplayName("source Configuration carries the native Fluss scanner keys")
    void sourceConfigurationCarriesScannerChunks() {
        SignalJobConfig config = SignalJobConfig.from(env());
        org.apache.fluss.config.Configuration sourceConf =
                SignalJob.flussSourceConfiguration(config);

        assertEquals(512L * 1024,
                sourceConf.get(ConfigOptions.CLIENT_SCANNER_LOG_FETCH_MAX_BYTES).getBytes());
        assertEquals(128L * 1024,
                sourceConf.get(ConfigOptions.CLIENT_SCANNER_LOG_FETCH_MAX_BYTES_FOR_BUCKET)
                        .getBytes());
        assertEquals(Duration.ofMillis(20),
                sourceConf.get(ConfigOptions.CLIENT_SCANNER_LOG_FETCH_WAIT_MAX_TIME));
    }

    @Test
    @DisplayName("execution.buffer-timeout reaches the Flink Configuration")
    void bufferTimeoutReachesFlinkConfig() {
        SignalJobConfig config = SignalJobConfig.from(env());
        org.apache.flink.configuration.Configuration flinkConfig =
                new org.apache.flink.configuration.Configuration();
        SignalJob.applyRuntimeOptions(config, flinkConfig);

        assertEquals(Duration.ofMillis(10), flinkConfig.get(ExecutionOptions.BUFFER_TIMEOUT));
    }
}
