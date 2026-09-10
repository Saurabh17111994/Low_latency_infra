package com.trading.compute.signaljob;

/**
 * Typed executable request the Signal-job decision builder turns into one
 * immutable {@code Trade_Decisions} LOG row (REQ-FLS-008 / REQ-SS-004 /
 * {@code docs/04_contracts/10-ranking.md}).
 *
 * <p>{@code instruction_id} is deliberately NOT an input: it is derived
 * deterministically from the executable identity by
 * {@link TradeDecisionBuilder} (REQ-SS-004 — the Signal job SHALL not reuse
 * an {@code instruction_id} for different instrumentToken, symbol,
 * tradeContextId, side, quantity, limitPricePaise, orderType, productType,
 * or strategyVersion; identical content MUST yield the same id so a
 * same-winner unchanged-parameter re-evaluation is audit-only). This list
 * is authoritative — {@link TradeDecisionBuilder#executableIdentity} hashes
 * exactly this set, nothing more (P2-179).
 *
 * <p>Contains no Executor-assigned fields by construction (no
 * {@code client_order_ref}, {@code broker_order_id}, no execution status).
 *
 * @param candidateId immutable candidate identity that won the evaluation
 * @param tradeContextId stable trade-context identity
 * @param instrumentToken routing/execution instrument
 * @param exchange exchange the instrument trades on
 * @param symbol instrument symbol
 * @param side BUY or SELL ({@link TradeDecisionsTableColumns#SIDE_BUY})
 * @param quantity non-positive rejected
 * @param orderType MARKET or LIMIT
 * @param productType product class
 * @param limitPricePaise null for market orders; non-null (> 0) for limit
 * @param portfolioId ranking/capacity scope
 * @param accountScopeId account scope
 * @param strategyId strategy provenance
 * @param strategyVersion strategy provenance (part of the executable identity)
 * @param configurationVersion configuration provenance
 * @param evaluationId ranking evaluation that produced this winner
 * @param compositeScore null or finite score from the evaluation
 * @param reservationId reservation backing this instruction
 * @param reservationVersion reservation transition version
 * @param createdTs emission timestamp (epoch millis; must be positive)
 * @param expiryTs null unless the instruction carries an explicit expiry
 * @param supersedesInstructionId set when this instruction replaces a prior one
 * @param supersededByInstructionId informational only — the LOG row is
 *        append-only, so this is practically null at emission time
 */
public record TradeDecision(
        String candidateId,
        String tradeContextId,
        long instrumentToken,
        String exchange,
        String symbol,
        String side,
        long quantity,
        String orderType,
        String productType,
        Long limitPricePaise,
        String portfolioId,
        String accountScopeId,
        String strategyId,
        String strategyVersion,
        String configurationVersion,
        String evaluationId,
        Double compositeScore,
        String reservationId,
        String reservationVersion,
        long createdTs,
        Long expiryTs,
        String supersedesInstructionId,
        String supersededByInstructionId) {
    public TradeDecision {
    // P2-061/P2-180: fail fast at construction — an invalid instance must
    // never circulate to (or past) the builder. Same rules as the builder's
    // requireValid, minus the null-record guard (a record component cannot
    // be the record itself). No allocation on the happy path beyond the
    // checks themselves.
    if (candidateId == null || candidateId.isBlank()) {
        throw new IllegalArgumentException("candidate_id must be non-blank");
    }
    if (tradeContextId == null || tradeContextId.isBlank()) {
        throw new IllegalArgumentException("trade_context_id must be non-blank");
    }
    if (instrumentToken <= 0) {
        throw new IllegalArgumentException(
                "instrument_token must be positive, got " + instrumentToken);
    }
    if (exchange == null || exchange.isBlank()) {
        throw new IllegalArgumentException("exchange must be non-blank");
    }
    if (symbol == null || symbol.isBlank()) {
        throw new IllegalArgumentException("symbol must be non-blank");
    }
    if (!TradeDecisionsTableColumns.SIDE_BUY.equals(side)
            && !TradeDecisionsTableColumns.SIDE_SELL.equals(side)) {
        throw new IllegalArgumentException("side must be BUY or SELL, got " + side);
    }
    if (quantity <= 0) {
        throw new IllegalArgumentException("quantity must be positive, got " + quantity);
    }
    if (!TradeDecisionsTableColumns.ORDER_TYPE_MARKET.equals(orderType)
            && !TradeDecisionsTableColumns.ORDER_TYPE_LIMIT.equals(orderType)) {
        throw new IllegalArgumentException(
                "order_type must be MARKET or LIMIT, got " + orderType);
    }
    if (productType == null || productType.isBlank()) {
        throw new IllegalArgumentException("product_type must be non-blank");
    }
    boolean limit = TradeDecisionsTableColumns.ORDER_TYPE_LIMIT.equals(orderType);
    if (limit && (limitPricePaise == null || limitPricePaise <= 0)) {
        throw new IllegalArgumentException(
                "limit order requires positive limit_price_paise, got " + limitPricePaise);
    }
    if (!limit && limitPricePaise != null) {
        throw new IllegalArgumentException(
                "market order must not carry limit_price_paise, got " + limitPricePaise);
    }
    if (portfolioId == null || portfolioId.isBlank()) {
        throw new IllegalArgumentException("portfolio_id must be non-blank");
    }
    if (accountScopeId == null || accountScopeId.isBlank()) {
        throw new IllegalArgumentException("account_scope_id must be non-blank");
    }
    if (strategyId == null || strategyId.isBlank()) {
        throw new IllegalArgumentException("strategy_id must be non-blank");
    }
    if (strategyVersion == null || strategyVersion.isBlank()) {
        throw new IllegalArgumentException("strategy_version must be non-blank");
    }
    if (configurationVersion == null || configurationVersion.isBlank()) {
        throw new IllegalArgumentException("configuration_version must be non-blank");
    }
    if (evaluationId == null || evaluationId.isBlank()) {
        throw new IllegalArgumentException("evaluation_id must be non-blank");
    }
    if (compositeScore != null && !Double.isFinite(compositeScore)) {
        throw new IllegalArgumentException(
                "composite_score must be null or finite, got " + compositeScore);
    }
    if (reservationId == null || reservationId.isBlank()) {
        throw new IllegalArgumentException("reservation_id must be non-blank");
    }
    if (reservationVersion == null || reservationVersion.isBlank()) {
        throw new IllegalArgumentException("reservation_version must be non-blank");
    }
    if (createdTs <= 0) {
        throw new IllegalArgumentException(
                "created_ts must be a positive epoch millis, got " + createdTs);
    }
    if (expiryTs != null && expiryTs <= 0) {
        throw new IllegalArgumentException(
                "expiry_ts must be null or positive, got " + expiryTs);
    }
    if (expiryTs != null && expiryTs <= createdTs) {
        throw new IllegalArgumentException(
                "expiry_ts must be > created_ts, got " + expiryTs + " <= " + createdTs);
    }
    if (supersedesInstructionId != null && supersedesInstructionId.isBlank()) {
        throw new IllegalArgumentException("supersedes_instruction_id must be null or non-blank");
    }
    // P2-182 strict (mirrors the builder): the LOG is append-only, so
    // superseded_by is resolved read-side and must be null at emission.
    if (supersededByInstructionId != null) {
        throw new IllegalArgumentException(
                "superseded_by_instruction_id must be null at emission (append-only LOG), got '"
                        + supersededByInstructionId + "'");
    }
    }
}
