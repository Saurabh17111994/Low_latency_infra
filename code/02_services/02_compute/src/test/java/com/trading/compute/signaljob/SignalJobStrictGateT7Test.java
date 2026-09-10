package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * T7 strict gate (G1 Safety): fail-closed on missing STATE_RECOVERY_PATH and
 * ALLOW_FULL_REPLAY not true — job must fail with explicit F005 if no recovery
 * path. No silent offset-0 replay.
 *
 * <p>Minimal build-phase unit test only — does not run 3000-tick soak.
 */
class SignalJobStrictGateT7Test {

    private static Map<String, String> baseEnv() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_TTL_MS", "60000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        return env;
    }

    @Test
    void missingBothYieldsLatestMode() {
        Map<String, String> env = baseEnv();
        // neither STATE_RECOVERY_PATH nor ALLOW_FULL_REPLAY=true: the source
        // starts from LATEST (2026-08-29) — skips the LOG backlog, no F005.
        assertEquals(SignalJobConfig.StartupMode.LATEST,
                SignalJobConfig.from(env).startupMode());
    }

    @Test
    void missingModeWithExplicitFalseYieldsLatest() {
        Map<String, String> env = baseEnv();
        env.put("ALLOW_FULL_REPLAY", "false");
        assertEquals(SignalJobConfig.StartupMode.LATEST,
                SignalJobConfig.from(env).startupMode());
    }

    @Test
    void blankRestorePathFailsWithF005() {
        Map<String, String> env = baseEnv();
        env.put("STATE_RECOVERY_PATH", "   ");
        env.put("ALLOW_FULL_REPLAY", "false");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SignalJobConfig.from(env));
        assertTrue(e.getMessage().contains("F005"), e.getMessage());
        assertTrue(e.getMessage().contains("STATE_RECOVERY_PATH"), e.getMessage());
    }

    @Test
    void blankReplayFailsWithF005() {
        Map<String, String> env = baseEnv();
        env.put("ALLOW_FULL_REPLAY", "   ");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SignalJobConfig.from(env));
        assertTrue(e.getMessage().contains("F005"), e.getMessage());
    }

    @Test
    void invalidReplayFailsWithF005() {
        Map<String, String> env = baseEnv();
        env.put("ALLOW_FULL_REPLAY", "yes");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SignalJobConfig.from(env));
        assertTrue(e.getMessage().contains("F005"), e.getMessage());
    }

    @Test
    void bothRestoreAndReplayFailsWithF005() {
        Map<String, String> env = baseEnv();
        env.put("STATE_RECOVERY_PATH", "file:///tmp/chk");
        env.put("ALLOW_FULL_REPLAY", "true");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SignalJobConfig.from(env));
        assertTrue(e.getMessage().contains("F005"), e.getMessage());
    }

    @Test
    void allowFullReplayTruePassesAsFullReplay() {
        Map<String, String> env = baseEnv();
        env.put("ALLOW_FULL_REPLAY", "true");
        SignalJobConfig cfg = SignalJobConfig.from(env);
        assertEquals(SignalJobConfig.StartupMode.FULL_REPLAY, cfg.startupMode());
        assertTrue(cfg.allowFullReplay());
    }

    @Test
    void restorePathPassesAsRestore() {
        Map<String, String> env = baseEnv();
        env.put("STATE_RECOVERY_PATH", "file:///tmp/chk");
        SignalJobConfig cfg = SignalJobConfig.from(env);
        assertEquals(SignalJobConfig.StartupMode.RESTORE, cfg.startupMode());
        assertEquals("file:///tmp/chk", cfg.stateRecoveryPath());
    }

    @Test
    void restorePathWithFalseReplayIsRestore() {
        Map<String, String> env = baseEnv();
        env.put("STATE_RECOVERY_PATH", "file:///tmp/chk");
        env.put("ALLOW_FULL_REPLAY", "false");
        SignalJobConfig cfg = SignalJobConfig.from(env);
        assertEquals(SignalJobConfig.StartupMode.RESTORE, cfg.startupMode());
    }

    @Test
    void trimsRestorePath() {
        Map<String, String> env = baseEnv();
        env.put("STATE_RECOVERY_PATH", "  file:///tmp/chk  ");
        SignalJobConfig cfg = SignalJobConfig.from(env);
        assertEquals("file:///tmp/chk", cfg.stateRecoveryPath());
    }

    @Test
    void productionWithoutRestoreOrReplayFailsWithF005() {
        // P2-052: prod must restore or explicitly break glass — a silent
        // LATEST backlog skip in production is forbidden (🔥 KILL).
        Map<String, String> env = baseEnv();
        env.put("DEPLOYMENT_ENV", "production");
        env.put("DEDUP_WINDOW_ENTRIES", "200");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("RESTART_MAX_ATTEMPTS", "3");
        env.put("RESTART_DELAY_MS", "30000");
        env.put("CHECKPOINT_DIR", "s3://signal-checkpoints/prod");
        env.put("S3_ENDPOINT", "https://signal-test.r2.cloudflarestorage.com");
        env.put("AWS_ACCESS_KEY_ID", "r2accesskey000000000000");
        env.put("AWS_SECRET_ACCESS_KEY", "r2s3cr3tvalue000000000000");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SignalJobConfig.from(env));
        assertTrue(e.getMessage().contains("F005"), e.getMessage());
    }

    @Test
    void productionWithExplicitReplayPasses() {
        Map<String, String> env = baseEnv();
        env.put("DEPLOYMENT_ENV", "production");
        // Production pins (must equal PlatformConfig values, like SignalJobConfigTest base env).
        env.put("DEDUP_WINDOW_ENTRIES", "200");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("RESTART_MAX_ATTEMPTS", "3");
        env.put("RESTART_DELAY_MS", "30000");
        env.put("CHECKPOINT_DIR", "s3://signal-checkpoints/prod");
        env.put("S3_ENDPOINT", "https://signal-test.r2.cloudflarestorage.com");
        env.put("AWS_ACCESS_KEY_ID", "r2accesskey000000000000");
        env.put("AWS_SECRET_ACCESS_KEY", "r2s3cr3tvalue000000000000");
        env.put("ALLOW_FULL_REPLAY", "true");
        assertEquals(SignalJobConfig.StartupMode.FULL_REPLAY,
                SignalJobConfig.from(env).startupMode());
    }

    @Test
    void schemeOfHandlesSchemeLessNullAndBlank() {
        // P2-051: local paths and blank must map to "none", never throw.
        assertEquals("none", SignalJob.schemeOf(null));
        assertEquals("none", SignalJob.schemeOf(""));
        assertEquals("none", SignalJob.schemeOf("   "));
        assertEquals("none", SignalJob.schemeOf("/tmp/checkpoints"));
        assertEquals("s3", SignalJob.schemeOf("s3://bucket/checkpoints"));
        assertEquals("file", SignalJob.schemeOf("file:///tmp/chk"));
    }
}
