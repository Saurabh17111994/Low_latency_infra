package com.trading.common.schema.position;

import java.util.Objects;
import java.util.Optional;
import org.apache.fluss.row.GenericRow;

/**
 * Maps a Fills LOG row (08_fills.sql v2, pinned by {@link FillsColumns}) into
 * the projector's {@link FillEvent} (SCH-20 operator wiring). The caller
 * supplies the fields the LOG cannot carry via {@link FillContext}; everything
 * else is read from the row by pinned column index.
 *
 * <p>Mapping decisions (documented, pinned by {@code FillEventMapperTest}):
 * <ul>
 *   <li>{@code sourceEventId} = {@code postback_event_id} — the unique platform
 *       fill identity used for duplicate/conflict content checks;</li>
 *   <li>{@code sourceVersion} = {@code receive_time} — platform receive time as
 *       the documented non-authoritative monotone sequence
 *       (05-execution-core.md: "platform receive time as non-authoritative
 *       evidence"); a future reading layer may supply log offsets instead, but
 *       the pure core pins receive_time;</li>
 *   <li>{@code eventTimeMs} = {@code broker_event_time} when present, else
 *       {@code receive_time};</li>
 *   <li>a row whose {@code fill_qty} is null/&le;0 (a status-only postback, not
 *       a fill) or whose {@code fill_price_paise} is null maps to
 *       {@link Optional#empty()} — it never reaches the projector.</li>
 * </ul>
 */
public final class FillEventMapper {

    private FillEventMapper() {}

    /**
     * Is this row a fill at all? A status-only postback (missing/non-positive
     * {@code fill_qty}, or missing/negative {@code fill_price_paise}) is not.
     *
     * <p>Split out of {@link #mapIfFill} so the driver can decide fill-ness
     * BEFORE resolving a position id (P3-395): minting for a status-only row
     * created an id with no snapshot and displaced {@code active} off a closed
     * position.
     */
    public static boolean isFill(GenericRow row) {
        Objects.requireNonNull(row, "row");
        if (row.isNullAt(FillsColumns.FILL_QTY)
                || row.getLong(FillsColumns.FILL_QTY) <= 0) {
            return false;
        }
        return !row.isNullAt(FillsColumns.FILL_PRICE_PAISE)
                && row.getLong(FillsColumns.FILL_PRICE_PAISE) >= 0;
    }

    /**
     * @return the {@link FillEvent}, or empty when the row is not a fill
     *         (fill_qty missing/non-positive or price missing/negative)
     */
    public static Optional<FillEvent> mapIfFill(GenericRow row, String positionId,
            FillContext ctx) {
        Objects.requireNonNull(row, "row");
        Objects.requireNonNull(positionId, "positionId");
        Objects.requireNonNull(ctx, "ctx");
        if (!isFill(row)) {
            return Optional.empty();
        }
        // P3-160: receive_time seeds BOTH sourceVersion and the event-time fallback. It was read
        // with a bare getLong while broker_event_time was guarded — an absent value either threw a
        // raw NPE from the unboxing or, if an implementation returned 0, silently pinned version 0
        // and corrupted STALE/DUPLICATE gating.
        if (row.isNullAt(FillsColumns.RECEIVE_TIME)) {
            throw new IllegalArgumentException("receive_time is required");
        }
        long receiveTime = row.getLong(FillsColumns.RECEIVE_TIME);
        long eventTime = row.isNullAt(FillsColumns.BROKER_EVENT_TIME)
                ? receiveTime
                : row.getLong(FillsColumns.BROKER_EVENT_TIME);
        String tradeContextId = row.isNullAt(FillsColumns.TRADE_CONTEXT_ID)
                ? null
                : row.getString(FillsColumns.TRADE_CONTEXT_ID).toString();
        // P3-161/P3-162: identity fields are required. Dereferencing them via getString(...).toString()
        // threw a raw NPE that bypassed the driver's quarantine path, instead of rejecting the row
        // with the column identity. The DDL's NOT NULL does not protect a GenericRow built from an
        // external postback — the mapper already guards the nullable TRADE_CONTEXT_ID the same way.
        String accountScopeId = requiredString(row, FillsColumns.ACCOUNT_SCOPE_ID,
                "account_scope_id");
        String sourceEventId = requiredString(row, FillsColumns.POSTBACK_EVENT_ID,
                "postback_event_id");
        return Optional.of(new FillEvent(
                positionId,
                tradeContextId,
                accountScopeId,
                ctx.instrumentToken(),
                ctx.exchange(),
                ctx.symbol(),
                ctx.side(),
                row.getLong(FillsColumns.FILL_QTY),
                row.getLong(FillsColumns.FILL_PRICE_PAISE),
                sourceEventId,
                receiveTime,
                eventTime));
    }

    /**
     * Reads a required STRING column, failing with the column identity rather than a raw NPE.
     */
    private static String requiredString(GenericRow row, int index, String field) {
        if (row.isNullAt(index)) {
            throw new IllegalArgumentException(field + " is required");
        }
        return row.getString(index).toString();
    }
}
