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
        // P3-390: only `< 0` was rejected while fillQty required > 0, so a zero-price fill flowed
        // through FillEventMapper into weightedAverage and diluted averageEntryPaise /
        // averageExitPaise — corrupting cost basis. Zero is not a traded price in paise. Kept in
        // parity with FillEventMapper.isFill and the Rust port's `validate`.
        if (fillPricePaise <= 0) {
            throw new IllegalArgumentException("fill_price_paise must be positive, got "
                    + fillPricePaise);
        }
        if (!SIDE_BUY.equals(side) && !SIDE_SELL.equals(side)) {
            throw new IllegalArgumentException("side must be BUY or SELL, got " + side);
        }
        // P3-159: sourceEventId is the fill identity for the duplicate/conflict content check,
        // and it is dereferenced by PositionProjector and by FlussPositionsStateStore.upsert
        // (BinaryString.fromString). Failing fast here keeps a null identity from surfacing as an
        // NPE deep in the projection, and keeps blank from silently defeating DUPLICATE vs
        // CONFLICT discrimination.
        if (sourceEventId == null || sourceEventId.isBlank()) {
            throw new IllegalArgumentException("source_event_id is required");
        }
        // sourceVersion is deliberately NOT range-checked here: a negative version must flow to
        // PositionProjector / KvStateUpdateProtocol, which maps it to UNKNOWN -> VIOLATION
        // (pinned by PositionProjectorTest.negativeVersionIsUnknownViolation).
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
