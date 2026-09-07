package com.trading.ingestion.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Transition grid for the sustained container-memory readiness gate.
 *
 * <p>Uses {@code limit=10_000} bytes for exact integer percentages: block at
 * 85% (= 8500 used), clear at 75% (= 7500 used), sustain window 1000 ms.
 */
class JvmHeapReadinessGateTest {

    private static final long LIMIT = 10_000L;
    private static final long WINDOW = 1_000L;
    private static final long BLOCK = 8_500L;   // 85%
    private static final long CLEAR = 7_500L;   // 75%

    private JvmHeapReadinessGate gate() {
        return new JvmHeapReadinessGate(85, 75, WINDOW);
    }

    @Test
    void transient_spike_below_window_never_blocks() {
        JvmHeapReadinessGate g = gate();
        assertEquals(JvmHeapReadinessGate.Signal.NONE, g.observe(LIMIT, BLOCK, 0));
        assertEquals(JvmHeapReadinessGate.Signal.NONE, g.observe(LIMIT, BLOCK, 500)); // not yet 1000ms
        assertFalse(g.isBlocked(), "sub-window breach must not block");
        // breach aborts before it sustains
        assertEquals(JvmHeapReadinessGate.Signal.NONE, g.observe(LIMIT, CLEAR, 600));
        assertFalse(g.isBlocked());
    }

    @Test
    void sustained_breach_blocks_and_emits_warn_once() {
        JvmHeapReadinessGate g = gate();
        assertEquals(JvmHeapReadinessGate.Signal.NONE, g.observe(LIMIT, BLOCK, 0));
        assertEquals(JvmHeapReadinessGate.Signal.WARN_HEAP_HIGH, g.observe(LIMIT, BLOCK, WINDOW));
        assertTrue(g.isBlocked(), "sustained breach must block");
        // still blocked, still above -> no repeat WARN same episode
        assertEquals(JvmHeapReadinessGate.Signal.NONE, g.observe(LIMIT, 9_500L, WINDOW + 200));
        assertTrue(g.isBlocked());
    }

    @Test
    void deadband_holds_blocked_state() {
        JvmHeapReadinessGate g = gate();
        g.observe(LIMIT, BLOCK, 0);
        g.observe(LIMIT, BLOCK, WINDOW); // -> blocked
        // usage in (75,85) is the deadband: holds blocked, no flapping
        assertEquals(JvmHeapReadinessGate.Signal.NONE, g.observe(LIMIT, 8_000L, WINDOW + 400));
        assertTrue(g.isBlocked(), "deadband must hold the blocked state");
    }

    @Test
    void sustained_recovery_clears_and_emits_info() {
        JvmHeapReadinessGate g = gate();
        g.observe(LIMIT, BLOCK, 0);
        g.observe(LIMIT, BLOCK, WINDOW);          // -> blocked
        g.observe(LIMIT, 8_000L, WINDOW + 400);   // deadband hold
        // drop below clear setpoint; must be sustained to clear
        assertEquals(JvmHeapReadinessGate.Signal.NONE, g.observe(LIMIT, CLEAR, WINDOW + 600));
        assertTrue(g.isBlocked(), "sub-window recovery must NOT clear yet");
        assertEquals(JvmHeapReadinessGate.Signal.INFO_HEAP_RECOVERED,
                g.observe(LIMIT, CLEAR, WINDOW + 600 + WINDOW));
        assertFalse(g.isBlocked(), "sustained recovery must clear the block");
    }

    @Test
    void recovered_then_rebreach_rearms_and_reemits() {
        JvmHeapReadinessGate g = gate();
        g.observe(LIMIT, BLOCK, 0);
        g.observe(LIMIT, BLOCK, WINDOW);                    // WARN, blocked
        g.observe(LIMIT, CLEAR, WINDOW + 1100);             // accumulation
        g.observe(LIMIT, CLEAR, WINDOW + 1100 + WINDOW);    // INFO, cleared
        assertFalse(g.isBlocked());
        // a fresh episode: rebreach must rearm and re-emit WARN
        g.observe(LIMIT, BLOCK, WINDOW + 2100 + WINDOW);
        assertEquals(JvmHeapReadinessGate.Signal.WARN_HEAP_HIGH,
                g.observe(LIMIT, BLOCK, WINDOW + 2100 + WINDOW + WINDOW));
        assertTrue(g.isBlocked());
    }

    @Test
    void clock_regression_rearms_recovery_timer_p1_247() {
        // P1-247: nowMs stepping backward after clearSinceMs is armed must
        // re-arm the recovery timer instead of stalling the clear forever
        // (nowMs - clearSinceMs stays negative on the old code).
        JvmHeapReadinessGate g = gate();
        g.observe(LIMIT, BLOCK, 10_000);
        assertEquals(JvmHeapReadinessGate.Signal.WARN_HEAP_HIGH,
                g.observe(LIMIT, BLOCK, 10_000 + WINDOW));
        assertTrue(g.isBlocked());
        assertEquals(JvmHeapReadinessGate.Signal.NONE,
                g.observe(LIMIT, CLEAR, 20_000)); // arm recovery timer
        assertEquals(JvmHeapReadinessGate.Signal.NONE,
                g.observe(LIMIT, CLEAR, 19_000)); // clock steps back: re-arm, no early clear
        assertTrue(g.isBlocked(), "regression must re-arm, never clear early");
        assertEquals(JvmHeapReadinessGate.Signal.INFO_HEAP_RECOVERED,
                g.observe(LIMIT, CLEAR, 19_000 + WINDOW)); // sustained since re-armed stamp
        assertFalse(g.isBlocked());
    }

    @Test
    void constructor_rejects_missing_hysteresis_and_nonpositive_window() {
        assertThrows(IllegalArgumentException.class, () -> new JvmHeapReadinessGate(85, 85, WINDOW));
        assertThrows(IllegalArgumentException.class, () -> new JvmHeapReadinessGate(85, 75, 0));
        assertThrows(IllegalArgumentException.class, () -> new JvmHeapReadinessGate(70, 75, WINDOW));
    }
}
