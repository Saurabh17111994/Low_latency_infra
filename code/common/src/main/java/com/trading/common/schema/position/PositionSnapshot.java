package com.trading.common.schema.position;

import com.trading.common.model.PositionState;

/**
 * Immutable projection snapshot mirroring the Positions KV layout
 * (10_positions.sql v2). Quantity invariants: {@code open_quantity} and
 * {@code closed_quantity} are never negative and never cross
 * ({@code open >= closed}); the current quantity is derived
 * ({@code open - closed}, never persisted — v2 removed the derived column).
 *
 * <p>Average-price encoding (L5-3): {@code averageEntryPaise} is 0 iff
 * {@code openQuantity} is 0, and {@code averageExitPaise} is 0 iff
 * {@code closedQuantity} is 0 — the constructor enforces both directions, so a
 * live quantity can never carry a zero price and a flat side can never carry a
 * stale one. Legacy SQL NULL rows read as 0 at the store boundary.
 */
public record PositionSnapshot(
        String positionId,
        String tradeContextId,
        String accountScopeId,
        long instrumentToken,
        String exchange,
        String symbol,
        String side,
        PositionState state,
        long openQuantity,
        long closedQuantity,
        long averageEntryPaise,
        long averageExitPaise,
        String sourceEventId,
        long sourceVersion,
        long createdTs,
        long lastUpdateTs,
        String schemaVersion) {

    public PositionSnapshot {
        if (positionId == null || positionId.isBlank()) {
            throw new IllegalArgumentException("position_id is required");
        }
        if (openQuantity < 0 || closedQuantity < 0 || openQuantity < closedQuantity) {
            throw new IllegalArgumentException("quantity invariant violated: open="
                    + openQuantity + " closed=" + closedQuantity);
        }
        // L5-3: average_*_paise = 0 iff the matching quantity is 0 (10_positions.sql).
        if ((openQuantity == 0) != (averageEntryPaise == 0)) {
            throw new IllegalArgumentException("average_entry_paise must be 0 iff open_quantity is 0:"
                    + " open=" + openQuantity + " averageEntry=" + averageEntryPaise);
        }
        if ((closedQuantity == 0) != (averageExitPaise == 0)) {
            throw new IllegalArgumentException("average_exit_paise must be 0 iff closed_quantity is 0:"
                    + " closed=" + closedQuantity + " averageExit=" + averageExitPaise);
        }
    }

    /** Current position size — derived, never persisted. */
    public long currentQuantity() {
        return openQuantity - closedQuantity;
    }
}
