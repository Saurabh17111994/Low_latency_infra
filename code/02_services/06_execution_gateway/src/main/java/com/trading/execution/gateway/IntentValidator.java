package com.trading.execution.gateway;

import java.time.Clock;
import java.util.Objects;

/** Fail-closed validation performed before an intent can cross the private boundary. */
public final class IntentValidator {
    private IntentValidator() {}
    // P3-485: precompiled — String.matches() recompiles per intent on this hot path.
    private static final java.util.regex.Pattern REQUEST_HASH =
            java.util.regex.Pattern.compile("[0-9a-fA-F]{64}");
    private static final java.util.regex.Pattern INSTRUCTION_ID =
            java.util.regex.Pattern.compile("^[A-Za-z0-9_-]{1,64}$");

    public static void validate(IntentRecord i, String accountScope, String partition, long nowMs) {
        Objects.requireNonNull(i, "intent");
        required(i.instructionId(), "instruction_id"); required(i.requestHash(), "request_hash");
        // P3-094: instruction_id was non-blank-only while supersedes enforced
        // the allowlist — malformed ids break dedup/idempotency downstream.
        if (!INSTRUCTION_ID.matcher(i.instructionId()).matches()) throw invalid("invalid instruction_id");
        required(i.accountScopeId(), "account_scope_id"); required(i.executionPartitionId(), "execution_partition_id");
        required(i.tradeContextId(), "trade_context_id"); required(i.candidateId(), "candidate_id");
        required(i.exchange(), "exchange"); required(i.symbol(), "symbol"); required(i.side(), "side");
        required(i.orderType(), "order_type"); required(i.productType(), "product_type");
        required(i.timeInForce(), "time_in_force"); required(i.strategyId(), "strategy_id");
        required(i.strategyVersion(), "strategy_version"); required(i.configurationVersion(), "configuration_version");
        if (!Objects.equals(i.schemaVersion(), "1")) throw invalid("unsupported schema_version");
        if (!Objects.equals(i.accountScopeId(), accountScope)) throw invalid("account scope mismatch");
        if (!Objects.equals(i.executionPartitionId(), partition)) throw invalid("partition mismatch");
        if (i.quantity() <= 0 || i.instrumentToken() <= 0) throw invalid("quantity/instrument must be positive");
        if (!i.side().equals("BUY") && !i.side().equals("SELL")) throw invalid("unsupported side");
        if (!i.orderType().equals("MARKET") && !i.orderType().equals("LIMIT")) throw invalid("unsupported order type");
        if (i.orderType().equals("LIMIT") && (i.limitPricePaise() == null || i.limitPricePaise() <= 0)) {
            throw invalid("limit order requires positive limit_price_paise");
        }
        if (i.expiryTs() != null && i.expiryTs() <= nowMs) throw invalid("intent expired");
        if (!REQUEST_HASH.matcher(i.requestHash()).matches()) throw invalid("request_hash must be hex sha256");
        // P3-313: whitespace-only supersedes failed the !isBlank guard and was
        // silently treated as absent — reject explicitly instead.
        if (i.supersedesInstructionId() != null) {
            if (i.supersedesInstructionId().isBlank()) throw invalid("invalid supersedes_instruction_id");
            if (i.supersedesInstructionId().equals(i.instructionId())) throw invalid("self-supersede not allowed");
            if (!INSTRUCTION_ID.matcher(i.supersedesInstructionId()).matches()) throw invalid("invalid supersedes_instruction_id");
        }
        if (i.orderType().equals("MARKET") && i.limitPricePaise() != null) throw invalid("market order must not have limit_price_paise");
        // P3-486: required() above already guarantees non-blank (record is
        // immutable, cannot become blank later) — dead check removed.
    }

    public static void validate(IntentRecord i, String accountScope, String partition) {
        validate(i, accountScope, partition, Clock.systemUTC().millis());
    }

    private static void required(String value, String name) {
        if (value == null || value.isBlank()) throw invalid(name + " is required");
    }
    private static IllegalArgumentException invalid(String reason) { return new IllegalArgumentException(reason); }
}
