package com.trading.ingestion.health;

import com.trading.common.config.ContainerMemoryGuard;
import com.trading.common.observability.AlertThresholds;

/**
 * Sustained JVM/container memory readiness gate (09-production-swarm § JVM and
 * memory configuration; {@link AlertThresholds#CONTAINER_MEMORY}).
 *
 * <p>Pure, injectable-clock state machine: it turns a stream of
 * {@code (containerLimitBytes, usedBytes)} samples — taken e.g. every few
 * seconds by the ingestion memory monitor — into a change of readiness and a
 * {@code SIGNAL-warn-jvm-heap-high} / recovery signal. It is deliberately
 * <em>transient-spike immune</em>: a breach must be sustained for a full
 * {@code sustainWindowMs} before the gate blocks, and a recovery must be
 * sustained below a lower (hysteresis) setpoint before it clears, so brief
 * allocation bursts never flap readiness. The deadband between the clear and
 * block setpoints holds the current state.
 *
 * <p>This class holds no wall-clock reference and performs no I/O, so the whole
 * transition grid is unit-testable with explicit {@code nowMs} stamps.
 */
public final class JvmHeapReadinessGate {

    /**
     * Default block setpoint: 85% of the container limit — the same figure as
     * {@code AlertThresholds.Alert.CONTAINER_MEMORY} ("container_memory_pct >= 85").
     */
    public static final int DEFAULT_BLOCK_AT_PERCENT = 85;
    /** Hysteresis: recovery only clears once usage drops to/below 75%. */
    public static final int DEFAULT_CLEAR_AT_PERCENT = 75;
    /** Sustained window: reuse the observability invariant's 60 s consecutive breach bound. */
    public static final long DEFAULT_SUSTAIN_WINDOW_MS =
            AlertThresholds.CONSECUTIVE_BREACH_SECONDS * 1_000L;

    /** Direction of the readiness transition requested by a sample. */
    public enum Signal {
        /** No state change. */
        NONE,
        /** Breach became sustained → gate should block readiness and emit WARN. */
        WARN_HEAP_HIGH,
        /** Recovery became sustained → gate should unblock readiness and emit a clear. */
        INFO_HEAP_RECOVERED;
    }

    private final int blockAtPercent;
    private final int clearAtPercent;
    private final long sustainWindowMs;

    // state
    private boolean blocked;
    private Long breachSinceMs;   // monotonic start of the current sustained-breach timer
    private Long clearSinceMs;    // monotonic start of the current recovery timer

    public JvmHeapReadinessGate() {
        this(DEFAULT_BLOCK_AT_PERCENT, DEFAULT_CLEAR_AT_PERCENT, DEFAULT_SUSTAIN_WINDOW_MS);
    }

    public JvmHeapReadinessGate(int blockAtPercent, int clearAtPercent, long sustainWindowMs) {
        if (blockAtPercent <= clearAtPercent) {
            throw new IllegalArgumentException("blockAtPercent must exceed clearAtPercent (hysteresis)");
        }
        if (sustainWindowMs <= 0) {
            throw new IllegalArgumentException("sustainWindowMs must be positive");
        }
        this.blockAtPercent = blockAtPercent;
        this.clearAtPercent = clearAtPercent;
        this.sustainWindowMs = sustainWindowMs;
    }

    /**
     * Feed one usage sample. Returns the readiness transition (if any) the
     * sample triggers; the caller applies the corresponding
     * {@code health.setMemoryBlocked(isBlocked())} and emits the matching alert.
     */
    public Signal observe(long containerLimitBytes, long usedBytes, long nowMs) {
        long pct = ContainerMemoryGuard.utilizedPercent(containerLimitBytes, usedBytes);
        if (pct >= blockAtPercent) {
            // entering/existing breach accumulation
            clearSinceMs = null;
            if (breachSinceMs == null) {
                breachSinceMs = nowMs;
            }
            if (!blocked && nowMs - breachSinceMs >= sustainWindowMs) {
                blocked = true;
                return Signal.WARN_HEAP_HIGH;
            }
            return Signal.NONE;
        }
        if (pct <= clearAtPercent) {
            // entering/existing recovery accumulation
            breachSinceMs = null;
            // P1-247: re-arm on clock regression — a backward nowMs step
            // leaves nowMs - clearSinceMs negative indefinitely, stalling
            // the sustained-recovery clear (symmetric to the breach timer).
            if (clearSinceMs == null || nowMs < clearSinceMs) {
                clearSinceMs = nowMs;
            }
            if (blocked && nowMs - clearSinceMs >= sustainWindowMs) {
                blocked = false;
                return Signal.INFO_HEAP_RECOVERED;
            }
            return Signal.NONE;
        }
        // deadband (clearAt < pct < blockAt): hold current state, reset timers
        breachSinceMs = null;
        clearSinceMs = null;
        return Signal.NONE;
    }

    /** Whether the gate is currently blocking readiness (sustained breach). */
    public boolean isBlocked() {
        return blocked;
    }
}
