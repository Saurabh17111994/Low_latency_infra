package com.trading.compute.signaljob;

/**
 * Immutable execution request before any broker-facing processing exists.
 *
 * <p>Nullable by lifecycle: {@code instructionId} is derived (not an input —
 * {@link ExecutionIntentBuilder#instructionId} computes it, so it is null
 * pre-derivation); {@code limitPricePaise} / {@code expiryTs} /
 * {@code supersedesInstructionId} are optional. Every other component is
 * required.
 */
public record ExecutionIntent(
        String instructionId,
        String candidateId,
        String tradeContextId,
        String accountScopeId,
        String executionPartitionId,
        long instrumentToken,
        String exchange,
        String symbol,
        String side,
        long quantity,
        String orderType,
        Long limitPricePaise,
        String productType,
        String timeInForce,
        String strategyId,
        String strategyVersion,
        String configurationVersion,
        long createdTs,
        Long expiryTs,
        String supersedesInstructionId) {
    public ExecutionIntent {
        // P2-029: fail fast at construction — an invalid instance must never
        // circulate to (or past) the builder. Same rules as the builder's
        // validate, so direct construction and build() enforce one contract.
        // No allocation on the happy path beyond the checks themselves.
        if (instructionId != null && instructionId.isBlank()) {
            throw new IllegalArgumentException("instruction_id must be null or non-blank");
        }
        if (candidateId == null || candidateId.isBlank()) {
            throw new IllegalArgumentException("candidate_id must be non-blank");
        }
        if (tradeContextId == null || tradeContextId.isBlank()) {
            throw new IllegalArgumentException("trade_context_id must be non-blank");
        }
        if (accountScopeId == null || accountScopeId.isBlank()) {
            throw new IllegalArgumentException("account_scope_id must be non-blank");
        }
        if (executionPartitionId == null || executionPartitionId.isBlank()) {
            throw new IllegalArgumentException("execution_partition_id must be non-blank");
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
        // P2-030/134 (Option A constant checks, NOT enums): closed
        // BUY/SELL + MARKET/LIMIT — an unknown side/order_type must fail
        // here, never reach broker-facing code or the immutable log.
        if (!ExecutionIntentTableColumns.SIDE_BUY.equals(side)
                && !ExecutionIntentTableColumns.SIDE_SELL.equals(side)) {
            throw new IllegalArgumentException("side must be BUY or SELL, got " + side);
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be positive, got " + quantity);
        }
        if (!ExecutionIntentTableColumns.ORDER_TYPE_MARKET.equals(orderType)
                && !ExecutionIntentTableColumns.ORDER_TYPE_LIMIT.equals(orderType)) {
            throw new IllegalArgumentException(
                    "order_type must be MARKET or LIMIT, got " + orderType);
        }
        if (productType == null || productType.isBlank()) {
            throw new IllegalArgumentException("product_type must be non-blank");
        }
        if (timeInForce == null || timeInForce.isBlank()) {
            throw new IllegalArgumentException("time_in_force must be non-blank");
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
        if (createdTs <= 0) {
            throw new IllegalArgumentException(
                    "created_ts must be a positive epoch millis, got " + createdTs);
        }
        boolean limit = ExecutionIntentTableColumns.ORDER_TYPE_LIMIT.equals(orderType);
        if (limit && (limitPricePaise == null || limitPricePaise <= 0)) {
            throw new IllegalArgumentException(
                    "LIMIT intent requires positive limit_price_paise, got " + limitPricePaise);
        }
        if (!limit && limitPricePaise != null) {
            throw new IllegalArgumentException(
                    "MARKET intent must not carry limit_price_paise, got " + limitPricePaise);
        }
        if (expiryTs != null && expiryTs <= createdTs) {
            throw new IllegalArgumentException(
                    "expiry_ts must be after created_ts, got " + expiryTs + " <= " + createdTs);
        }
        if (supersedesInstructionId != null && supersedesInstructionId.isBlank()) {
            throw new IllegalArgumentException(
                    "supersedes_instruction_id must be null or non-blank");
        }
    }
}
