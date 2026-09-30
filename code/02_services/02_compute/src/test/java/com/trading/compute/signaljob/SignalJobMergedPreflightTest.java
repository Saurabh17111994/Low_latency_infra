package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Wave C W-C3 (docs/plans/2026-09-30-wave-c-merged-cutover.md): pins when the
 * merged candle_features table joins the startup contract preflight — the
 * merged writer persists to it or the sealed-only context reader reads it.
 * Every other combination keeps the pre-Wave-C validation set unchanged.
 */
class SignalJobMergedPreflightTest {

    @Test
    @DisplayName("merged writer on -> candle_features validated")
    void mergedWriterEnablesValidation() {
        Map<String, String> env = env();
        env.put("MERGED_CANDLE_FEATURES_ENABLED", "true");
        assertTrue(SignalJob.mergedCandleTableInUse(SignalJobConfig.from(env)));
    }

    @Test
    @DisplayName("writer off, context reading candle_features -> validated")
    void contextOnMergedEnablesValidation() {
        Map<String, String> env = env();
        env.put("CANDLE_CONTEXT_TABLE", "candle_features");
        assertTrue(SignalJob.mergedCandleTableInUse(SignalJobConfig.from(env)));
    }

    @Test
    @DisplayName("writer off, renamed merged table read by the context -> validated")
    void contextOnRenamedMergedEnablesValidation() {
        Map<String, String> env = env();
        env.put("MERGED_CANDLE_TABLE", "candle_features_v2");
        env.put("CANDLE_CONTEXT_TABLE", "candle_features_v2");
        assertTrue(SignalJob.mergedCandleTableInUse(SignalJobConfig.from(env)));
    }

    @Test
    @DisplayName("defaults (legacy writer + legacy context) -> merged table untouched")
    void legacyDefaultsLeaveMergedOut() {
        assertFalse(SignalJob.mergedCandleTableInUse(SignalJobConfig.from(env())));
    }

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
}
