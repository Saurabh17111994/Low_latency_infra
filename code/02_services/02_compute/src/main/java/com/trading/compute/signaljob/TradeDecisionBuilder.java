package com.trading.compute.signaljob;

import com.trading.common.schema.ImmutabilityProtocol;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

/**
 * Pure builder for one immutable {@code Trade_Decisions} LOG row (SCH-19,
 * REQ-FLS-008 / REQ-SS-004).
 *
 * <p>Deterministic by construction: {@link #build(TradeDecision)} is a pure
 * function of its input — the same input always yields the same
 * {@code instruction_id}, the same canonical content hash, and the same row.
 * Replay/restart re-emission therefore produces an identical row (the
 * instruction-feed protocol's {@code DUPLICATE} path drops it), and any
 * change to the executable identity yields a NEW {@code instruction_id} with
 * a supersession relation (REQ-SS-004, {@code 10-ranking.md} REQ-RNK-004).
 *
 * <p><b>Instruction identity ({@code instruction_id}):</b> versioned
 * deterministic encoding of the executable identity — {@code ins-v1-} +
 * SHA-256 hex of the identity serialization. Identity fields are exactly the
 * REQ-SS-004 set: instrument, symbol, trade context, side, quantity, price,
 * order type, product type, strategy version. Provenance fields
 * ({@code candidate_id}, {@code evaluation_id}, timestamps, …) never affect
 * the identity, so a same-winner unchanged-parameter re-evaluation produces
 * the same id and stays audit-only.
 *
 * <p><b>Canonical content hash:</b> SHA-256 of a versioned serialization of
 * the COMPLETE execution request (REQ-FLS-008) — the full row content minus
 * the derived identity/version columns. The instruction-feed protocol
 * compares this hash per {@code instruction_id}: same id + same hash =
 * duplicate evidence; same id + different hash = contract violation
 * (REQ-FLS-015).
 */
public final class TradeDecisionBuilder {

    /** Version tag of the instruction_id encoding — bump on any encoding change. */
    static final String INSTRUCTION_ID_PREFIX = "ins-v1-";

    /** Version tag of the executable-identity serialization. */
    static final String IDENTITY_SERIALIZATION_VERSION = "tdi-v1";

    /** Version tag of the canonical-content serialization. */
    static final String CONTENT_SERIALIZATION_VERSION = "tdc-v1";

    private static final String NULL_TOKEN = "null";

    private TradeDecisionBuilder() {}

    /**
     * Build the 25-column {@code Trade_Decisions} row in DDL order (pinned by
     * {@link TradeDecisionsTableColumns}). Throws
     * {@link IllegalArgumentException} on any input that would produce an
     * unroutable or executable-invalid instruction (non-positive instrument /
     * quantity / createdTs, blank required identity, non-finite score, invalid
     * price).
     */
    public static RowData build(TradeDecision d) {
        requireValid(d);
        GenericRowData row = new GenericRowData(TradeDecisionsTableColumns.FIELD_COUNT);
        row.setField(TradeDecisionsTableColumns.INSTRUCTION_ID,
                StringData.fromString(instructionId(d)));
        row.setField(TradeDecisionsTableColumns.CANDIDATE_ID,
                StringData.fromString(d.candidateId()));
        row.setField(TradeDecisionsTableColumns.TRADE_CONTEXT_ID,
                StringData.fromString(d.tradeContextId()));
        row.setField(TradeDecisionsTableColumns.INSTRUMENT_TOKEN, d.instrumentToken());
        row.setField(TradeDecisionsTableColumns.EXCHANGE, StringData.fromString(d.exchange()));
        row.setField(TradeDecisionsTableColumns.SYMBOL, StringData.fromString(d.symbol()));
        row.setField(TradeDecisionsTableColumns.SIDE, StringData.fromString(d.side()));
        row.setField(TradeDecisionsTableColumns.QUANTITY, d.quantity());
        row.setField(TradeDecisionsTableColumns.ORDER_TYPE, StringData.fromString(d.orderType()));
        row.setField(TradeDecisionsTableColumns.PRODUCT_TYPE,
                StringData.fromString(d.productType()));
        row.setField(TradeDecisionsTableColumns.LIMIT_PRICE_PAISE, d.limitPricePaise());
        row.setField(TradeDecisionsTableColumns.PORTFOLIO_ID,
                StringData.fromString(d.portfolioId()));
        row.setField(TradeDecisionsTableColumns.ACCOUNT_SCOPE_ID,
                StringData.fromString(d.accountScopeId()));
        row.setField(TradeDecisionsTableColumns.STRATEGY_ID,
                StringData.fromString(d.strategyId()));
        row.setField(TradeDecisionsTableColumns.STRATEGY_VERSION,
                StringData.fromString(d.strategyVersion()));
        row.setField(TradeDecisionsTableColumns.CONFIGURATION_VERSION,
                StringData.fromString(d.configurationVersion()));
        row.setField(TradeDecisionsTableColumns.EVALUATION_ID,
                StringData.fromString(d.evaluationId()));
        row.setField(TradeDecisionsTableColumns.COMPOSITE_SCORE, d.compositeScore());
        row.setField(TradeDecisionsTableColumns.RESERVATION_ID,
                StringData.fromString(d.reservationId()));
        row.setField(TradeDecisionsTableColumns.RESERVATION_VERSION,
                StringData.fromString(d.reservationVersion()));
        row.setField(TradeDecisionsTableColumns.CREATED_TS, d.createdTs());
        row.setField(TradeDecisionsTableColumns.EXPIRY_TS, d.expiryTs());
        row.setField(TradeDecisionsTableColumns.SUPERSEDES_INSTRUCTION_ID,
                d.supersedesInstructionId() == null ? null
                        : StringData.fromString(d.supersedesInstructionId()));
        row.setField(TradeDecisionsTableColumns.SUPERSEDED_BY_INSTRUCTION_ID,
                d.supersededByInstructionId() == null ? null
                        : StringData.fromString(d.supersededByInstructionId()));
        row.setField(TradeDecisionsTableColumns.SCHEMA_VERSION,
                StringData.fromString(TradeDecisionsTableColumns.SCHEMA_VERSION_V2));
        return row;
    }

    /**
     * Deterministic, versioned {@code instruction_id} for the executable
     * identity: {@code ins-v1-<sha256hex(identity)>}. Identical content →
     * identical id; any REQ-SS-004 field change → a different id (never a
     * mutated reuse of the old identity).
     */
    public static String instructionId(TradeDecision d) {
        requireValid(d);
        return INSTRUCTION_ID_PREFIX + sha256Hex(executableIdentity(d));
    }

    /**
     * Canonical content hash of the COMPLETE execution request (REQ-FLS-008):
     * SHA-256 over a versioned serialization of every non-derived column
     * (identity + provenance + reservation + lifecycle). Compared per
     * {@code instruction_id} by
     * {@link TradeInstructionFeedProtocol#verify} to separate idempotent
     * duplicates from contract violations (REQ-FLS-015).
     */
    public static String canonicalHash(TradeDecision d) {
        requireValid(d);
        return ImmutabilityProtocol.canonicalHash(canonicalContent(d));
    }

    /**
     * Versioned executable-identity serialization — the ONLY input to
     * {@code instruction_id}. Fields: instrument, symbol, trade context, side,
     * quantity, price, order type, product type, strategy version (the exact
     * REQ-SS-004 set). Provenance never enters the identity.
     */
    static String executableIdentity(TradeDecision d) {
        return IDENTITY_SERIALIZATION_VERSION + "|"
                + d.instrumentToken() + "|"
                + d.symbol() + "|"
                + d.tradeContextId() + "|"
                + d.side() + "|"
                + d.quantity() + "|"
                + token(d.limitPricePaise()) + "|"
                + d.orderType() + "|"
                + d.productType() + "|"
                + d.strategyVersion();
    }

    /**
     * Versioned canonical-content serialization — the input to
     * {@link #canonicalHash}. Covers the complete execution request: the
     * executable identity plus provenance (candidate, evaluation, strategy,
     * configuration, scope) and reservation evidence (reservation id/version).
     */
    private static String canonicalContent(TradeDecision d) {
        return CONTENT_SERIALIZATION_VERSION + "|"
                + d.instrumentToken() + "|"
                + d.symbol() + "|"
                + d.tradeContextId() + "|"
                + d.side() + "|"
                + d.quantity() + "|"
                + token(d.limitPricePaise()) + "|"
                + d.orderType() + "|"
                + d.productType() + "|"
                + d.strategyVersion() + "|"
                + d.candidateId() + "|"
                + d.evaluationId() + "|"
                + d.exchange() + "|"
                + d.strategyId() + "|"
                + d.configurationVersion() + "|"
                + token(d.compositeScore()) + "|"
                + d.portfolioId() + "|"
                + d.accountScopeId() + "|"
                + d.reservationId() + "|"
                + d.reservationVersion() + "|"
                + d.createdTs() + "|"
                + token(d.expiryTs()) + "|"
                + token(d.supersedesInstructionId());
    }

    /** Validation that fails closed before ANY row is produced. */
    private static void requireValid(TradeDecision d) {
        requireNonBlank(d.candidateId(), "candidate_id");
        requireNonBlank(d.tradeContextId(), "trade_context_id");
        requireNonBlank(d.exchange(), "exchange");
        requireNonBlank(d.symbol(), "symbol");
        // P4-025: closed enums — a decision with an unknown side/order_type is
        // unroutable and must fail here, never reach the gateway (mirror of
        // IntentValidator's closure).
        if (!TradeDecisionsTableColumns.SIDE_BUY.equals(d.side())
                && !TradeDecisionsTableColumns.SIDE_SELL.equals(d.side())) {
            throw new IllegalArgumentException("side must be BUY or SELL, got "
                    + d.side());
        }
        if (!TradeDecisionsTableColumns.ORDER_TYPE_MARKET.equals(d.orderType())
                && !TradeDecisionsTableColumns.ORDER_TYPE_LIMIT.equals(d.orderType())) {
            throw new IllegalArgumentException("order_type must be MARKET or LIMIT, got "
                    + d.orderType());
        }
        requireNonBlank(d.productType(), "product_type");
        requireNonBlank(d.portfolioId(), "portfolio_id");
        requireNonBlank(d.accountScopeId(), "account_scope_id");
        requireNonBlank(d.strategyId(), "strategy_id");
        requireNonBlank(d.strategyVersion(), "strategy_version");
        requireNonBlank(d.configurationVersion(), "configuration_version");
        requireNonBlank(d.evaluationId(), "evaluation_id");
        requireNonBlank(d.reservationId(), "reservation_id");
        requireNonBlank(d.reservationVersion(), "reservation_version");
        if (d.instrumentToken() <= 0) {
            throw new IllegalArgumentException("instrument_token must be positive, got "
                    + d.instrumentToken());
        }
        if (d.quantity() <= 0) {
            throw new IllegalArgumentException("quantity must be positive, got " + d.quantity());
        }
        if (d.createdTs() <= 0) {
            throw new IllegalArgumentException("created_ts must be a positive epoch millis, got "
                    + d.createdTs());
        }
        if (d.compositeScore() != null && !Double.isFinite(d.compositeScore())) {
            throw new IllegalArgumentException("composite_score must be null or finite, got "
                    + d.compositeScore());
        }
        // P4-025: LIMIT/MARKET-price coupling (mirror of IntentValidator) —
        // LIMIT requires a positive limit price; MARKET must not carry one.
        boolean limit = TradeDecisionsTableColumns.ORDER_TYPE_LIMIT.equals(d.orderType());
        boolean market = TradeDecisionsTableColumns.ORDER_TYPE_MARKET.equals(d.orderType());
        if (limit && (d.limitPricePaise() == null || d.limitPricePaise() <= 0)) {
            throw new IllegalArgumentException(
                    "limit order requires positive limit_price_paise, got " + d.limitPricePaise());
        }
        if (market && d.limitPricePaise() != null) {
            throw new IllegalArgumentException(
                    "market order must not carry limit_price_paise, got "
                            + d.limitPricePaise());
        }
        if (d.expiryTs() != null && d.expiryTs() <= 0) {
            throw new IllegalArgumentException("expiry_ts must be null or positive, got "
                    + d.expiryTs());
        }
    }

    private static void requireNonBlank(String value, String column) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(column + " must be a non-blank value, got "
                    + (value == null ? "null" : "'" + value + "'"));
        }
    }

    private static String token(Long value) {
        return value == null ? NULL_TOKEN : String.valueOf(value);
    }

    private static String token(Double value) {
        return value == null ? NULL_TOKEN : String.valueOf(value);
    }

    private static String token(String value) {
        return value == null ? NULL_TOKEN : value;
    }

    private static String sha256Hex(String content) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(content.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
