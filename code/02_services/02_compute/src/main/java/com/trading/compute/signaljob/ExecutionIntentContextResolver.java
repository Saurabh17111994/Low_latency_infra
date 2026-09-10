package com.trading.compute.signaljob;

import com.trading.common.schema.ImmutabilityProtocol;
import org.apache.flink.table.data.RowData;

/**
 * Deterministically assigns the MVP trade context for an entry candidate.
 *
 * <p>The context is stable for a candidate and account scope, but it is not a
 * broker order ID. Later reduce/exit actions must carry the same context from
 * the durable position/trade state; they must not independently mint a new
 * context. A new entry candidate after closure naturally receives a new
 * candidate-derived context.
 */
public final class ExecutionIntentContextResolver {

    private static final String CONTEXT_VERSION = "trade-context-v1";

    private ExecutionIntentContextResolver() {}

    public static String resolveEntry(RowData candidate, String accountScopeId) {
        if (candidate == null || candidate.getArity() != SignalCandidatesTableColumns.FIELD_COUNT) {
            throw new IllegalArgumentException("candidate must match the Signal_Candidates v2 layout");
        }
        if (accountScopeId == null || accountScopeId.isBlank()) {
            throw new IllegalArgumentException("account_scope_id must be configured");
        }
        String candidateId = required(candidate, SignalCandidatesTableColumns.CANDIDATE_ID,
                "candidate_id");
        // P2-033: same NPE-escapes-IAE-catch as P2-031 — resolveEntry runs
        // inside flatMap's IAE-only catch, so a null token must reject, not NPE.
        if (candidate.isNullAt(SignalCandidatesTableColumns.INSTRUMENT_TOKEN)) {
            throw new IllegalArgumentException("instrument_token must be present in the candidate");
        }
        long instrumentToken = candidate.getLong(SignalCandidatesTableColumns.INSTRUMENT_TOKEN);
        if (instrumentToken <= 0) {
            throw new IllegalArgumentException("instrument_token must be positive");
        }
        String action = required(candidate, SignalCandidatesTableColumns.ACTION, "action");
        if (!SignalCandidatesTableColumns.ACTION_ENTRY.equals(action)) {
            throw new IllegalArgumentException("only ENTRY candidates can mint an MVP trade context: "
                    + action);
        }
        String identity = join(CONTEXT_VERSION, accountScopeId.trim(), instrumentToken, candidateId);
        return "trade-v1-" + ImmutabilityProtocol.canonicalHash(identity);
    }

    private static String required(RowData row, int index, String field) {
        if (row.isNullAt(index) || row.getString(index).toString().isBlank()) {
            throw new IllegalArgumentException(field + " must be present in the candidate");
        }
        return row.getString(index).toString();
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
}
