package com.trading.common.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * G2 (2026-08-29): ConfigGuard itself validated — the real inventory passes
 * (every declared key is read somewhere in code), and a fabricated unread key
 * is detected.
 */
class ConfigGuardTest {

    @Test
    void declaredKeysAreNonEmpty() {
        assertFalse(ConfigGuard.declaredKeys().isEmpty(),
                "ConfigKeys inventory must not be empty");
    }

    @Test
    void realInventoryPasses() {
        // Every key in ConfigKeys must be read by config code — if this fails,
        // a declared key is a silent lie and must be wired in or removed.
        assertDoesNotThrow(ConfigGuard::assertAllKeysRead);
    }

    @Test
    void keyReadInCodeDetectsRealAndMissingKeys() {
        // DEDUP_TTL_MS is read by SignalJobConfig — must be found.
        assertTrue(ConfigGuard.keyReadInCode("DEDUP_TTL_MS"),
                "DEDUP_TTL_MS is read by SignalJobConfig");
        // A fabricated key must NOT be found — proves the scan isn't vacuous.
        // (Split so this test file itself doesn't contain the literal.)
        String fake = "THIS_KEY_DOES_NOT_" + "EXIST_ANYWHERE";
        assertFalse(ConfigGuard.keyReadInCode(fake),
                "fabricated key must not be reported as read");
    }

    @Test
    void ignoredKeysDoNotBlockStartup() {
        // The IGNORED list is private; just assert the guard passes with them present.
        assertDoesNotThrow(ConfigGuard::assertAllKeysRead);
    }
}
