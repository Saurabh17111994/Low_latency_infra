package com.trading.common.schema.position;

import com.trading.common.model.PositionState;
import com.trading.common.schema.KvStateUpdateProtocol;
import java.util.Objects;

/**
 * Pure-JVM core of the SCH-20 position projector: projects a {@link FillEvent}
 * onto the current {@link PositionSnapshot} with (1) version-gating via
 * {@link KvStateUpdateProtocol} (stale/regressive writes rejected — the
 * projector half of the KV state-update protocol, SCH-09) and (2) lifecycle
 * validation via {@link PositionLifecycle} (illegal transitions → UNKNOWN +
 * halt signal). Deterministic and side-effect free; the operator integration
 * (Action Capture, Phase 4) drives it and persists the snapshot to Positions.
 *
 * <p><b>Projection rules</b> (the pinned contract):
 * <ul>
 *   <li>BUY fill adds {@code fill_qty} to {@code open_quantity} (weighted
 *       average entry).</li>
 *   <li>SELL fill adds {@code fill_qty} to {@code closed_quantity} (weighted
 *       average exit); a sell that exceeds the open quantity is a violation
 *       (UNKNOWN + halt) — overselling a flat position is never allowed.</li>
 *   <li>State is derived from the quantities ({@link PositionLifecycle#derive})
 *       and the transition from the prior state is validated.</li>
 *   <li>{@code source_version} is the monotone sequence: older than the
 *       current → STALE (rejected + halt); equal with identical content →
 *       DUPLICATE (no-op); equal with different content → CONFLICT
 *       (UNKNOWN + halt); newer → APPLIED.</li>
 * </ul>
 */
public final class PositionProjector {

    private PositionProjector() {}

    /** Outcome of one projection step. */
    public enum Outcome {
        /** Applied — the returned snapshot replaces the current one. */
        APPLIED,
        /** Same version + same event already reflected — no-op. */
        DUPLICATE,
        /** Older than current — rejected. */
        STALE,
        /** Quantity/lifecycle violation — rejected, requires quarantine + halt. */
        VIOLATION
    }

    /** Result of {@link #apply}; {@code snapshot()} is null unless APPLIED/DUPLICATE. */
    public record ProjectionResult(Outcome outcome, PositionSnapshot snapshot, String reason) {

        public static ProjectionResult applied(PositionSnapshot s) {
            return new ProjectionResult(Outcome.APPLIED, s, null);
        }

        public static ProjectionResult duplicate(PositionSnapshot s) {
            return new ProjectionResult(Outcome.DUPLICATE, s, null);
        }

        public static ProjectionResult stale(PositionSnapshot current) {
            return new ProjectionResult(Outcome.STALE, current,
                    "stale fill version " + current.sourceVersion());
        }

        public static ProjectionResult violation(String reason) {
            return new ProjectionResult(Outcome.VIOLATION, null, reason);
        }
    }

    /**
     * Project {@code fill} onto {@code current} (null = no position yet).
     * Never mutates {@code current} — returns a new snapshot or a rejection.
     */
    public static ProjectionResult apply(PositionSnapshot current, FillEvent fill, long nowMs) {
        Objects.requireNonNull(fill, "fill");
        long currentVersion = current == null ? 0L : current.sourceVersion();

        // Version gate (SCH-09 KvStateUpdateProtocol semantics).
        // P3-165: `fill.sourceEventId().equals(current.sourceEventId())` NPEs when the snapshot's
        // event id is null — PositionSnapshot does not reject one, so a snapshot hydrated from a
        // corrupt store broke the never-throws contract instead of yielding a controlled outcome.
        boolean contentMatches = current != null
                && Objects.equals(fill.sourceEventId(), current.sourceEventId());
        KvStateUpdateProtocol.Outcome v = KvStateUpdateProtocol.evaluate(
                currentVersion, fill.sourceVersion(), contentMatches);
        switch (v) {
            case DUPLICATE -> {
                return ProjectionResult.duplicate(current);
            }
            case STALE -> {
                return ProjectionResult.stale(current);
            }
            case REGRESSION, CONFLICT, UNKNOWN -> {
                return ProjectionResult.violation("version check " + v
                        + " for fill " + fill.sourceEventId());
            }
            case APPLIED -> {
                // fall through
            }
        }

        long open = current == null ? 0L : current.openQuantity();
        long closed = current == null ? 0L : current.closedQuantity();
        long avgEntry = current == null ? 0L : current.averageEntryPaise();
        long avgExit = current == null ? 0L : current.averageExitPaise();

        if (FillEvent.SIDE_BUY.equals(fill.side())) {
            long currentOpenBefore = open - closed;
            open += fill.fillQty();
            // Average entry weights over the CURRENT open units (open - closed),
            // not the cumulative buys: units exited are no longer part of the
            // position, so a fresh re-entry after a full close starts a new
            // average.
            avgEntry = weightedAverage(avgEntry, currentOpenBefore,
                    fill.fillPricePaise(), fill.fillQty());
        } else {
            long nextClosed = closed + fill.fillQty();
            if (nextClosed > open) {
                return ProjectionResult.violation("sell overshoots open quantity: open="
                        + open + " sell=" + fill.fillQty() + " (would close to "
                        + (open - nextClosed) + ")");
            }
            closed = nextClosed;
            avgExit = weightedAverage(avgExit, closed - fill.fillQty(),
                    fill.fillPricePaise(), fill.fillQty());
        }

        PositionState priorState = current == null
                ? PositionState.FLAT
                : current.state();
        // Cycle-aware state: a fresh BUY after a FULL close starts a new open
        // cycle (CLOSED -> OPEN) even though the cumulative closed_quantity is
        // still positive — the quantity-only derive() cannot see the cycle
        // boundary, so the projector tracks it via the prior state.
        PositionState nextState = nextState(priorState, open, closed, fill.side());
        if (!PositionLifecycle.isLegalTransition(priorState, nextState)) {
            return ProjectionResult.violation("illegal transition " + priorState
                    + " -> " + nextState + " for fill " + fill.sourceEventId());
        }

        PositionSnapshot next = new PositionSnapshot(
                fill.positionId(),
                fill.tradeContextId(),
                fill.accountScopeId(),
                fill.instrumentToken(),
                fill.exchange(),
                fill.symbol(),
                fill.side(),
                nextState,
                open,
                closed,
                avgEntry,
                avgExit,
                fill.sourceEventId(),
                fill.sourceVersion(),
                current == null ? nowMs : current.createdTs(),
                nowMs,
                PositionsColumns.SCHEMA_VERSION_V2);
        return ProjectionResult.applied(next);
    }

    /**
     * Cycle-aware next state: fully exited -> CLOSED; never exited -> OPEN;
     * a fresh BUY after a full close re-opens the cycle; otherwise a position
     * with cumulative exits and remaining quantity is REDUCING.
     */
    private static PositionState nextState(PositionState prior, long open, long closed,
            String side) {
        if (open == closed) {
            return PositionState.CLOSED;
        }
        if (closed == 0) {
            return PositionState.OPEN;
        }
        if (prior == PositionState.CLOSED && FillEvent.SIDE_BUY.equals(side)) {
            return PositionState.OPEN;
        }
        return PositionState.REDUCING;
    }

    /** Weighted average across the existing notional and the new fill. */
    private static long weightedAverage(long existingAvg, long existingQty,
            long newPrice, long newQty) {
        if (existingQty + newQty == 0) {
            return 0L;
        }
        long numerator = existingAvg * existingQty + newPrice * newQty;
        return numerator / (existingQty + newQty);
    }
}
