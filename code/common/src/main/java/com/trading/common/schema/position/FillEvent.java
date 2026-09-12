package com.trading.common.schema.position;

/**
 * A fill as the position projector consumes it — the caller-resolved subset
 * of the Fills LOG (08_fills.sql v2). The LOG itself has NO side column
 * (verified 2026-08-15): {@code side} is resolved by the caller (Action
 * Capture) from the correlated instruction/attempt before projection — the
 * projector never guesses direction. {@code sourceVersion} is the monotone
 * sequence the projection versioning compares ({@code Positions.source_version});
 * {@code sourceEventId} is the fill identity for duplicate/conflict content
 * checks ({@code Positions.source_event_id}).
 */
public record FillEvent(
        String positionId,
        String tradeContextId,
        String accountScopeId,
        long instrumentToken,
        String exchange,
        String symbol,
        String side,            // "BUY" | "SELL" (caller-resolved)
        long fillQty,
        long fillPricePaise,
        String sourceEventId,
        long sourceVersion,
        long eventTimeMs) {

    public static final String SIDE_BUY = "BUY";
    public static final String SIDE_SELL = "SELL";

    public FillEvent {
        if (positionId == null || positionId.isBlank()) {
            throw new IllegalArgumentException("position_id is required");
        }
        if (fillQty <= 0) {
            throw new IllegalArgumentException("fill_qty must be positive, got " + fillQty);
        }
        if (fillPricePaise < 0) {
            throw new IllegalArgumentException("fill_price_paise must be >= 0, got "
                    + fillPricePaise);
        }
        if (!SIDE_BUY.equals(side) && !SIDE_SELL.equals(side)) {
            throw new IllegalArgumentException("side must be BUY or SELL, got " + side);
        }
    }

    /**
     * A copy of this fill bound to {@code positionId}.
     *
     * <p>The driver maps a row before it can resolve the position id — the
     * re-entry decision needs the fill's {@code sourceVersion}/{@code sourceEventId}
     * (P3-166) — so the id is bound on afterwards rather than guessed up front.
     */
    public FillEvent withPositionId(String positionId) {
        return new FillEvent(positionId, tradeContextId, accountScopeId, instrumentToken,
                exchange, symbol, side, fillQty, fillPricePaise, sourceEventId, sourceVersion,
                eventTimeMs);
    }
}
