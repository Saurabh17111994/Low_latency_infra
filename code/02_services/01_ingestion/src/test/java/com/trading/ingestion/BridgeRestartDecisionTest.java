package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.ingestion.IngestionService.BridgeRestartDecision;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ING-UNIT-012: bridge restart policy (plan §IngestionService).
 *
 * <p>An unexpected exit is restarted once; a second unexpected exit in the same
 * process is terminal; an explicit teardown is never restarted. H2-3: exit 0
 * without an explicit teardown is unexpected too — the Go bridge exits 3 on
 * terminal runtime failures and 0 only for requested/completed runs.
 */
@DisplayName("ING-UNIT-012: bridge restart policy")
class BridgeRestartDecisionTest {

    @Test
    @DisplayName("unexpected exit restarts once")
    void unexpectedExitRestartsOnce() {
        assertEquals(BridgeRestartDecision.RESTART,
                IngestionService.bridgeRestartDecision(true, false, 1, 0),
                "first unexpected exit (restartCount=0) must restart");
    }

    @Test
    @DisplayName("second unexpected exit is terminal")
    void secondUnexpectedExitIsTerminal() {
        assertEquals(BridgeRestartDecision.TERMINAL,
                IngestionService.bridgeRestartDecision(true, false, 1, 1),
                "second unexpected exit (restartCount=1) must be terminal");
    }

    @Test
    @DisplayName("H2-3: an unrequested exit 0 is unexpected, not a clean shutdown")
    void unrequestedZeroExitIsUnexpected() {
        assertEquals(BridgeRestartDecision.RESTART,
                IngestionService.bridgeRestartDecision(true, false, 0, 0),
                "exit 0 without an explicit teardown is a silent bridge death");
        assertEquals(BridgeRestartDecision.TERMINAL,
                IngestionService.bridgeRestartDecision(true, false, 0, 1),
                "the second unrequested exit 0 is terminal");
    }

    @Test
    @DisplayName("the bridge's terminal exit code 3 restarts once, then goes terminal")
    void terminalRuntimeExitCodeIsUnexpected() {
        assertEquals(BridgeRestartDecision.RESTART,
                IngestionService.bridgeRestartDecision(true, false, 3, 0),
                "the Go terminal-runtime exit must trigger the one restart");
        assertEquals(BridgeRestartDecision.TERMINAL,
                IngestionService.bridgeRestartDecision(true, false, 3, 1),
                "a second terminal exit is fatal");
    }

    @Test
    @DisplayName("shutdown in progress never restarts")
    void shutdownNeverRestarts() {
        assertEquals(BridgeRestartDecision.NO_RESTART,
                IngestionService.bridgeRestartDecision(false, false, 1, 0),
                "not running must never restart");
        // CHG-015: the hook tears the bridge down while `running` is still
        // true (it stays true until the bridge exits), so a non-zero exit in
        // that window — e.g. the forced-kill fallback (137) — must also never
        // restart, or a fresh bridge would be spawned mid-shutdown and
        // orphaned when the JVM halts.
        assertEquals(BridgeRestartDecision.NO_RESTART,
                IngestionService.bridgeRestartDecision(true, true, 137, 0),
                "shutdown in progress must never restart even while running");
    }
}
