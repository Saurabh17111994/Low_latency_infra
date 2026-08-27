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
    void missingBothFailsWithF005() {
        Map<String, String> env = baseEnv();
        // neither STATE_RECOVERY_PATH nor ALLOW_FULL_REPLAY=true
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SignalJobConfig.from(env));
        assertTrue(e.getMessage().contains("F005"),
                "gate must surface explicit F005, got: " + e.getMessage());
        assertTrue(e.getMessage().contains("Missing startup mode")
                        || e.getMessage().contains("STATE_RECOVERY_PATH"),
                "message must name startup mode, got: " + e.getMessage());
    }

    @Test
    void missingModeWithExplicitFalseStillF005() {
        Map<String, String> env = baseEnv();
        env.put("ALLOW_FULL_REPLAY", "false");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SignalJobConfig.from(env));
        assertTrue(e.getMessage().contains("F005"), e.getMessage());
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
}
