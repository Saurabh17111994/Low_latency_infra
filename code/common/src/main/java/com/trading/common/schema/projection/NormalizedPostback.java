package com.trading.common.schema.projection;


/**
 * The normalized Nautilus postback envelope (T6, CHG-045) — the single agreed
 * shape Rust/Nautilus emits for an order/fill update before it is projected
 * into {@code Fills} / {@code Order_Lifecycle} / {@code Positions}.
 *
 * <p>This envelope closes the T6 "fill context gap" (plan T6 audit): the Fills
 * LOG carries no {@code side} / instrument / exchange / symbol, so this
 * envelope carries them explicitly — the projection never infers {@code side}
 * from broker proximity or a missing field. The stable {@link #sourceSequence}
 * (event-store/event sequence) is the authoritative monotone version; the
 * plan explicitly disallows wall-clock {@code receiveTimeMs} as the only
 * version for live replay.
 *
 * <p>Identity: {@code postbackEventId} is the unique platform postback
 * identity; {@code sourceEventId} is the underlying fill/order event identity
 * used for duplicate/conflict content checks. {@code fingerprint} is the
 * versioned content fingerprint (see {@link PostbackFingerprint}); a
 * mismatch is quarantined as {@link QuarantineReason#FINGERPRINT_MISMATCH}.
 *
 * <p>Fill semantics (P4-029): a postback with {@code fillQty == 0} is a
 * status/heartbeat update — pure order bookkeeping that must not carry a
 * price. A postback with {@code fillQty > 0} is a fill event (partial while
 * {@code pendingQty > 0}, complete at {@code pendingQty == 0}) and must
 * carry a strictly positive {@code fillPricePaise}. The fill's identity for
 * duplicate/conflict checks is {@code sourceEventId}, which this envelope
 * always requires — this envelope does not model a separate broker fill id.
 *
 * @param mappingVersion fingerprint/mapping release, e.g. {@code "1"}
 */
public record NormalizedPostback(
        String postbackEventId,
        String sourceEventId,
        long sourceSequence,
        String fingerprint,
        String fingerprintVersion,
        String brokerOrderId,
        String echoedClientOrderRef,
        String accountScopeId,
        long instrumentToken,
        String exchange,
        String symbol,
        String side,
        String orderStatus,
        long cumulativeQty,
        long pendingQty,
        long fillQty,
        long fillPricePaise,
        long eventTimeMs,
        long receiveTimeMs,
        String mappingVersion,
        String originalPayloadHash,
        String tradeContextId) {

    public static final String SIDE_BUY = "BUY";
    public static final String SIDE_SELL = "SELL";

    public NormalizedPostback {
        require(postbackEventId, "postbackEventId");
        require(sourceEventId, "sourceEventId");
        require(fingerprint, "fingerprint");
        require(fingerprintVersion, "fingerprintVersion");
        if (mappingVersion == null || mappingVersion.isBlank()) {
            throw new IllegalArgumentException("mappingVersion is required");
        }
        if (sourceSequence < 0) {
            throw new IllegalArgumentException("sourceSequence must be >= 0, got " + sourceSequence);
        }
        if (cumulativeQty < 0 || pendingQty < 0 || fillQty < 0) {
            throw new IllegalArgumentException("quantities must be >= 0");
        }
        if (fillQty == 0 && fillPricePaise != 0) {
            throw new IllegalArgumentException("non-fill row must not carry a fill price");
        }
        // P4-029 FOLLOW-3: fill-present-requires-price. A fill without a
        // positive price would corrupt average_fill_price_paise downstream
        // (0 is this envelope's sentinel for "no price").
        if (fillQty > 0 && fillPricePaise <= 0) {
            throw new IllegalArgumentException("fill row must carry a positive fill price");
        }
        if (side != null && !SIDE_BUY.equals(side) && !SIDE_SELL.equals(side)) {
            throw new IllegalArgumentException("side must be BUY or SELL, got " + side);
        }
        if (instrumentToken < 0) {
            throw new IllegalArgumentException("instrumentToken must be positive when present");
        }
    }

    /** True when this postback carries a fill (a quantity moved at a price). */
    public boolean isFill() {
        return fillQty > 0;
    }

    /** True when a broker order id is present (correlation can use it directly). */
    public boolean hasBrokerOrderId() {
        return brokerOrderId != null && !brokerOrderId.isBlank();
    }

    private static void require(String s, String name) {
        if (s == null || s.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
    }
}
