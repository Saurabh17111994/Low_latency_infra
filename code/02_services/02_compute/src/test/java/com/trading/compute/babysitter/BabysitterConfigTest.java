package com.trading.compute.babysitter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * BAB-CONFIG-001 (Task 7): Babysitter configuration fails closed. Missing or
 * blank {@code FLUSS_BOOTSTRAP_SERVERS}, a non-positive checkpoint/freshness
 * interval, or an action-enable attempt must reject at construction time rather
 * than default to a live path.
 */
@DisplayName("BAB-CONFIG-001: Babysitter configuration fails closed")
class BabysitterConfigTest {

    @Test
    @DisplayName("action enable attempts are rejected by both the config and the flag guard")
    void actionEnabledRejected() {
        assertThrows(IllegalStateException.class, () -> new BabysitterConfig(
                "localhost:9123", "default", "Positions", null, 60_000L, 60_000L, true, null));
        assertThrows(IllegalStateException.class,
                () -> BabysitterConfig.parseActionEnabled("true"));
    }

    @Test
    @DisplayName("missing/blank bootstrap must fail closed, never default to a live path")
    void missingBootstrapRejected() {
        assertThrows(IllegalStateException.class, () -> new BabysitterConfig(
                null, "default", "Positions", null, 60_000L, 60_000L, false, null));
        assertThrows(IllegalStateException.class, () -> new BabysitterConfig(
                "   ", "default", "Positions", null, 60_000L, 60_000L, false, null));
    }

    @Test
    @DisplayName("non-positive checkpoint/freshness intervals are rejected")
    void nonPositiveIntervalsRejected() {
        assertThrows(IllegalStateException.class, () -> new BabysitterConfig(
                "localhost:9123", "default", "Positions", null, 0L, 60_000L, false, null));
        assertThrows(IllegalStateException.class, () -> new BabysitterConfig(
                "localhost:9123", "default", "Positions", null, 60_000L, -1L, false, null));
    }

    @Test
    @DisplayName("a fully valid config is accepted with the explicit non-live-action contract")
    void validConfigAccepted() {
        BabysitterConfig c = new BabysitterConfig(
                "localhost:9123", "default", "Positions", null, 60_000L, 60_000L, false, null);
        assertEquals("localhost:9123", c.bootstrapServers());
        assertEquals("Positions", c.table());
        assertEquals(60_000L, c.checkpointIntervalMs());
        assertEquals(false, c.actionEnabled());
    }

    @Test
    @DisplayName("P2-113: blank database/table rejected, blank paths normalize to null")
    void blankDbAndPathsHandledInCtor() {
        assertThrows(IllegalStateException.class, () -> new BabysitterConfig(
                "localhost:9123", "  ", "Positions", null, 60_000L, 60_000L, false, null));
        assertThrows(IllegalStateException.class, () -> new BabysitterConfig(
                "localhost:9123", "default", "", null, 60_000L, 60_000L, false, null));
        BabysitterConfig c = new BabysitterConfig(
                "localhost:9123", "default", "Positions", "  ", 60_000L, 60_000L, false, "\t");
        assertEquals(null, c.checkpointDir());
        assertEquals(null, c.stateRecoveryPath());
    }
}
