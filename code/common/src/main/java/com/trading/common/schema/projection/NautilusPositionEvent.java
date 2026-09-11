package com.trading.common.schema.projection;

import com.trading.common.model.PositionState;

/**
 * The authoritative Nautilus-computed position update (T6, CHG-045). Nautilus
 * is the only production position/PnL calculator; the projection layer
 * serializes this event into a Positions KV row and never recomputes
 * arithmetic. Carries the stable {@link #sourceSequence} that the version gate
 * compares against {@code Positions.source_version}.
 *
 * <p>Quantity invariants are validated here (never negative, open >= closed)
 * so an impossible Nautilus event is rejected as
 * {@link QuarantineReason#POSITION_VIOLATION} before it can reach the store.
 *
 * <p>P3-399: the compact constructor throws IllegalArgumentException, but the
 * production ingestion path (PostbackProjectionDriver → positionAuthority →
 * this constructor) routes that failure into the same quarantine path — the
 * driver catches IAE from the authority and returns a POSITION_VIOLATION
 * quarantine + scope halt instead of letting it escape.
 */
public record NautilusPositionEvent(
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
        long sourceSequence,
        long lastUpdateTs) {

    public NautilusPositionEvent {
        if (positionId == null || positionId.isBlank()) {
            throw new IllegalArgumentException("positionId is required");
        }
        // P3-400: identity fields are persisted verbatim into Positions —
        // null/blank here would silently poison downstream matching/queries.
        if (tradeContextId == null || tradeContextId.isBlank()) {
            throw new IllegalArgumentException("tradeContextId is required");
        }
        if (accountScopeId == null || accountScopeId.isBlank()) {
            throw new IllegalArgumentException("accountScopeId is required");
        }
        if (exchange == null || exchange.isBlank()) {
            throw new IllegalArgumentException("exchange is required");
        }
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("symbol is required");
        }
        if (side == null || side.isBlank()) {
            throw new IllegalArgumentException("side is required");
        }
        if (sourceEventId == null || sourceEventId.isBlank()) {
            throw new IllegalArgumentException("sourceEventId is required");
        }
        if (state == null) {
            throw new IllegalArgumentException("state is required");
        }
        if (openQuantity < 0 || closedQuantity < 0 || openQuantity < closedQuantity) {
            throw new IllegalArgumentException(
                    "quantity invariant violated: open=" + openQuantity
                    + " closed=" + closedQuantity);
        }
        if (sourceSequence < 0) {
            throw new IllegalArgumentException("sourceSequence must be >= 0");
        }
        // P3-503: authoritative store of record — reject meaningless prices/ts
        // here rather than persisting them into Positions for PnL/UI.
        // 0 average is legitimate only while the position is open (no fills yet).
        if (averageEntryPaise < 0 || averageExitPaise < 0) {
            throw new IllegalArgumentException("average prices must be >= 0");
        }
        if (lastUpdateTs <= 0) {
            throw new IllegalArgumentException("lastUpdateTs must be > 0");
        }
    }
}
