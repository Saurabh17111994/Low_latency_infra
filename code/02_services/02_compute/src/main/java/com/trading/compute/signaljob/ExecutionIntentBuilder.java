package com.trading.compute.signaljob;

import com.trading.common.schema.ImmutabilityProtocol;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

/** Pure deterministic builder for the v1 {@code Execution_Intent} contract. */
public final class ExecutionIntentBuilder {

    private static final String IDENTITY_VERSION = "ei-id-v2";
    private static final String REQUEST_VERSION = "ei-request-v1";

    private ExecutionIntentBuilder() {}

    /**
     * Maps one validated signal candidate into an execution intent without
     * performing I/O or assigning any broker/executor identity.
     *
     * <p>MVP: every intent from this path carries null {@code expiry_ts} and
     * null {@code supersedes_instruction_id} — i.e. never-expiring intents
     * (P2-133). There is no candidate validity window to derive a TTL from
     * ({@code VALIDITY_REASON} is status, not a window); TTL enforcement
     * belongs to the Java execution gateway (T2), which owns durable
     * quarantine. If a bounded TTL is ever required, derive
     * {@code expiryTs} here and thread {@code supersedesInstructionId}
     * through instead of hardcoding null.
     *
     * <p>This overload deliberately requires the candidate to carry a
     * non-null {@code trade_context_id}. The MVP producer uses the overload
     * below after {@link ExecutionIntentContextResolver} assigns a deterministic
     * entry context; future reduce/exit producers must pass the durable context
     * from position/trade state rather than minting one independently.
     */
    public static ExecutionIntent fromCandidate(RowData candidate, String accountScopeId,
            String executionPartitionId, String productType, String timeInForce,
            String configurationVersion) {
        if (candidate == null || candidate.getArity() != SignalCandidatesTableColumns.FIELD_COUNT) {
            throw new IllegalArgumentException("candidate must match the Signal_Candidates v2 layout");
        }
        String tradeContextId = required(candidate, SignalCandidatesTableColumns.TRADE_CONTEXT_ID,
                "trade_context_id");
        return fromCandidate(candidate, accountScopeId, executionPartitionId, productType,
                timeInForce, configurationVersion, tradeContextId);
    }

    /** Maps a candidate after an approved context resolver assigned its trade context. */
    public static ExecutionIntent fromCandidate(RowData candidate, String accountScopeId,
            String executionPartitionId, String productType, String timeInForce,
            String configurationVersion, String tradeContextId) {
        if (candidate == null || candidate.getArity() != SignalCandidatesTableColumns.FIELD_COUNT) {
            throw new IllegalArgumentException("candidate must match the Signal_Candidates v2 layout");
        }
        String candidateId = required(candidate, SignalCandidatesTableColumns.CANDIDATE_ID,
                "candidate_id");
        if (tradeContextId == null || tradeContextId.isBlank()) {
            throw new IllegalArgumentException("trade_context_id must be present");
        }
        String validity = required(candidate, SignalCandidatesTableColumns.VALIDITY_REASON,
                "validity_reason");
        if (!SignalCandidatesTableColumns.VALIDITY_REASON_VALID.equals(validity)) {
            throw new IllegalArgumentException("candidate is not valid: " + validity);
        }
        String action = required(candidate, SignalCandidatesTableColumns.ACTION, "action");
        if (!SignalCandidatesTableColumns.ACTION_ENTRY.equals(action)) {
            throw new IllegalArgumentException("only ENTRY candidates can become intents: " + action);
        }
        // P2-031: unguarded getLong auto-unboxes null → NPE escapes flatMap
        // (only IAE is caught) → task failover on one malformed row. Fail as
        // a counted reject instead.
        if (candidate.isNullAt(SignalCandidatesTableColumns.DETECTION_TS)
                || candidate.isNullAt(SignalCandidatesTableColumns.INSTRUMENT_TOKEN)
                || candidate.isNullAt(SignalCandidatesTableColumns.QUANTITY)) {
            throw new IllegalArgumentException(
                    "detection_ts, instrument_token and quantity must be present");
        }
        long createdTs = candidate.getLong(SignalCandidatesTableColumns.DETECTION_TS);
        return new ExecutionIntent(
                null,
                candidateId,
                tradeContextId,
                accountScopeId,
                executionPartitionId,
                candidate.getLong(SignalCandidatesTableColumns.INSTRUMENT_TOKEN),
                required(candidate, SignalCandidatesTableColumns.EXCHANGE, "exchange"),
                required(candidate, SignalCandidatesTableColumns.SYMBOL, "symbol"),
                required(candidate, SignalCandidatesTableColumns.SIDE, "side"),
                candidate.getLong(SignalCandidatesTableColumns.QUANTITY),
                required(candidate, SignalCandidatesTableColumns.ORDER_TYPE, "order_type"),
                candidate.isNullAt(SignalCandidatesTableColumns.LIMIT_PRICE_PAISE)
                        ? null : candidate.getLong(SignalCandidatesTableColumns.LIMIT_PRICE_PAISE),
                productType,
                timeInForce,
                required(candidate, SignalCandidatesTableColumns.STRATEGY_ID, "strategy_id"),
                required(candidate, SignalCandidatesTableColumns.STRATEGY_VERSION,
                        "strategy_version"),
                configurationVersion,
                createdTs,
                null,
                null);
    }

    /** Builds one DDL-ordered row, including its derived identity and request hash. */
    public static RowData build(ExecutionIntent intent) {
        validate(intent);
        GenericRowData row = new GenericRowData(ExecutionIntentTableColumns.FIELD_COUNT);
        row.setField(ExecutionIntentTableColumns.INSTRUCTION_ID,
                StringData.fromString(instructionId(intent)));
        row.setField(ExecutionIntentTableColumns.CANDIDATE_ID, text(intent.candidateId()));
        row.setField(ExecutionIntentTableColumns.TRADE_CONTEXT_ID, text(intent.tradeContextId()));
        row.setField(ExecutionIntentTableColumns.ACCOUNT_SCOPE_ID, text(intent.accountScopeId()));
        row.setField(ExecutionIntentTableColumns.EXECUTION_PARTITION_ID,
                text(intent.executionPartitionId()));
        row.setField(ExecutionIntentTableColumns.INSTRUMENT_TOKEN, intent.instrumentToken());
        row.setField(ExecutionIntentTableColumns.EXCHANGE, text(intent.exchange()));
        row.setField(ExecutionIntentTableColumns.SYMBOL, text(intent.symbol()));
        row.setField(ExecutionIntentTableColumns.SIDE, text(intent.side()));
        row.setField(ExecutionIntentTableColumns.QUANTITY, intent.quantity());
        row.setField(ExecutionIntentTableColumns.ORDER_TYPE, text(intent.orderType()));
        row.setField(ExecutionIntentTableColumns.LIMIT_PRICE_PAISE, intent.limitPricePaise());
        row.setField(ExecutionIntentTableColumns.PRODUCT_TYPE, text(intent.productType()));
        row.setField(ExecutionIntentTableColumns.TIME_IN_FORCE, text(intent.timeInForce()));
        row.setField(ExecutionIntentTableColumns.STRATEGY_ID, text(intent.strategyId()));
        row.setField(ExecutionIntentTableColumns.STRATEGY_VERSION, text(intent.strategyVersion()));
        row.setField(ExecutionIntentTableColumns.CONFIGURATION_VERSION,
                text(intent.configurationVersion()));
        row.setField(ExecutionIntentTableColumns.CREATED_TS, intent.createdTs());
        row.setField(ExecutionIntentTableColumns.EXPIRY_TS, intent.expiryTs());
        row.setField(ExecutionIntentTableColumns.REQUEST_HASH,
                StringData.fromString(requestHash(intent)));
        row.setField(ExecutionIntentTableColumns.SUPERSEDES_INSTRUCTION_ID,
                text(intent.supersedesInstructionId()));
        row.setField(ExecutionIntentTableColumns.SCHEMA_VERSION,
                StringData.fromString(ExecutionIntentTableColumns.SCHEMA_VERSION_V1));
        return row;
    }

    /** Deterministic identity for executable fields and scope. */
    public static String instructionId(ExecutionIntent intent) {
        validate(intent);
        return "ei-v1-" + sha256(identityContent(intent));
    }

    /** SHA-256 over every executable, scope, version, expiry, and supersession field. */
    public static String requestHash(ExecutionIntent intent) {
        validate(intent);
        return ImmutabilityProtocol.canonicalHash(requestContent(intent));
    }

    static String identityContent(ExecutionIntent intent) {
        // P2-032: venue is executable routing scope — it MUST be in the
        // identity. Without it, two venues sharing token/symbol mint the same
        // instructionId with different requestHashes → false VIOLATION.
        return join(IDENTITY_VERSION, intent.accountScopeId(), intent.executionPartitionId(),
                intent.instrumentToken(), intent.exchange(), intent.symbol(),
                intent.tradeContextId(), intent.side(),
                intent.quantity(), token(intent.limitPricePaise()), intent.orderType(),
                intent.productType(), intent.timeInForce(), intent.strategyId(),
                intent.strategyVersion());
    }

    static String requestContent(ExecutionIntent intent) {
        return join(REQUEST_VERSION, intent.candidateId(), intent.tradeContextId(),
                intent.accountScopeId(), intent.executionPartitionId(), intent.instrumentToken(),
                intent.exchange(), intent.symbol(), intent.side(), intent.quantity(),
                intent.orderType(), token(intent.limitPricePaise()), intent.productType(),
                intent.timeInForce(), intent.strategyId(), intent.strategyVersion(),
                intent.configurationVersion(), intent.createdTs(), token(intent.expiryTs()),
                token(intent.supersedesInstructionId()));
    }

    private static String join(Object... values) {
        StringBuilder out = new StringBuilder();
        for (Object value : values) {
            if (out.length() > 0) {
                out.append('|');
            }
            String text = value == null ? "null" : String.valueOf(value);
            out.append(text.length()).append(':').append(text);
        }
        return out.toString();
    }

    private static String token(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static StringData text(String value) {
        return value == null ? null : StringData.fromString(value);
    }

    private static String required(RowData row, int index, String field) {
        if (row.isNullAt(index) || row.getString(index).toString().isBlank()) {
            throw new IllegalArgumentException(field + " must be present in the candidate");
        }
        return row.getString(index).toString();
    }

    private static String sha256(String content) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(
                    digest.digest(content.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static void validate(ExecutionIntent intent) {
        // P2-134: single enforcement point is the P2-029 compact ctor — the
        // builder delegates (mirrors the P2-063/P2-181 fold), keeping only
        // the null-record guard the record itself cannot express.
        if (intent == null) {
            throw new IllegalArgumentException("execution intent must not be null");
        }
    }
}
