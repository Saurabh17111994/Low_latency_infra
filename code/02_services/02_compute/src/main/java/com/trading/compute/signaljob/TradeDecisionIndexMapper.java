package com.trading.compute.signaljob;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

/**
 * Maps one immutable {@code Trade_Decisions} LOG row (25 columns, see
 * {@link TradeDecisionsTableColumns}) to its {@code trade_instruction_state}
 * KV index row (4 columns, see {@link TradeInstructionStateColumns}) — the
 * dual-sink twin the instruction-feed protocol checks before every append
 * (SCH-19, REQ-FLS-015).
 *
 * <p>The canonical content hash is recomputed from the row's executable
 * request via {@link TradeDecisionBuilder#canonicalHash} (the row round-trips
 * back to the typed decision), so the index always stores exactly the hash
 * the protocol compares — a mismatch means a bug, not a drift between the two
 * sinks. The routing identity ({@code instruction_id}) is copied through
 * unchanged; {@code first_written_ts} = the decision's {@code created_ts}.
 *
 * <p>Stateless and pure — unit-testable without a cluster.
 */
public final class TradeDecisionIndexMapper implements MapFunction<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    @Override
    public RowData map(RowData decision) {
        // P2-184: arity check + stored-vs-recomputed id equality — a column
        // reorder/addition or a toDecision/hash-input omission must surface
        // loud instead of an orphan index entry defeating REQ-FLS-015.
        if (decision == null) {
            throw new IllegalArgumentException("Trade_Decisions row must not be null");
        }
        if (decision.getArity() != TradeDecisionsTableColumns.FIELD_COUNT) {
            throw new IllegalArgumentException("Trade_Decisions row arity " + decision.getArity()
                    + " != " + TradeDecisionsTableColumns.FIELD_COUNT);
        }
        TradeDecision d = toDecision(decision);
        String storedId = decision.isNullAt(TradeDecisionsTableColumns.INSTRUCTION_ID) ? null
                : decision.getString(TradeDecisionsTableColumns.INSTRUCTION_ID).toString();
        String recomputedId = TradeDecisionBuilder.instructionId(d);
        if (!recomputedId.equals(storedId)) {
            throw new IllegalStateException("recomputed instruction_id " + recomputedId
                    + " != LOG row " + storedId + " — toDecision/hash drift");
        }
        GenericRowData index = new GenericRowData(TradeInstructionStateColumns.FIELD_COUNT);
        index.setField(TradeInstructionStateColumns.INSTRUCTION_ID,
                StringData.fromString(TradeDecisionBuilder.instructionId(d)));
        index.setField(TradeInstructionStateColumns.CANONICAL_HASH,
                StringData.fromString(TradeDecisionBuilder.canonicalHash(d)));
        index.setField(TradeInstructionStateColumns.FIRST_WRITTEN_TS, d.createdTs());
        index.setField(TradeInstructionStateColumns.SCHEMA_VERSION,
                StringData.fromString(TradeInstructionStateColumns.SCHEMA_VERSION_V1));
        return index;
    }

    /** Rebuild the typed decision from the 25-column row (all inputs are in the row). */
    static TradeDecision toDecision(RowData row) {
        // P2-064: column-named IAE on every NOT NULL column — a corrupt
        // backfill / schema-evolution null must name its column instead of
        // an NPE deep in the index-map branch (P2-009 convention).
        return new TradeDecision(
                requireNonNullString(row, TradeDecisionsTableColumns.CANDIDATE_ID, "candidate_id"),
                requireNonNullString(
                        row, TradeDecisionsTableColumns.TRADE_CONTEXT_ID, "trade_context_id"),
                requireNonNullLong(
                        row, TradeDecisionsTableColumns.INSTRUMENT_TOKEN, "instrument_token"),
                requireNonNullString(row, TradeDecisionsTableColumns.EXCHANGE, "exchange"),
                requireNonNullString(row, TradeDecisionsTableColumns.SYMBOL, "symbol"),
                requireNonNullString(row, TradeDecisionsTableColumns.SIDE, "side"),
                requireNonNullLong(row, TradeDecisionsTableColumns.QUANTITY, "quantity"),
                requireNonNullString(row, TradeDecisionsTableColumns.ORDER_TYPE, "order_type"),
                requireNonNullString(
                        row, TradeDecisionsTableColumns.PRODUCT_TYPE, "product_type"),
                row.isNullAt(TradeDecisionsTableColumns.LIMIT_PRICE_PAISE) ? null
                        : row.getLong(TradeDecisionsTableColumns.LIMIT_PRICE_PAISE),
                requireNonNullString(
                        row, TradeDecisionsTableColumns.PORTFOLIO_ID, "portfolio_id"),
                requireNonNullString(
                        row, TradeDecisionsTableColumns.ACCOUNT_SCOPE_ID, "account_scope_id"),
                requireNonNullString(
                        row, TradeDecisionsTableColumns.STRATEGY_ID, "strategy_id"),
                requireNonNullString(
                        row, TradeDecisionsTableColumns.STRATEGY_VERSION, "strategy_version"),
                requireNonNullString(
                        row, TradeDecisionsTableColumns.CONFIGURATION_VERSION,
                        "configuration_version"),
                requireNonNullString(
                        row, TradeDecisionsTableColumns.EVALUATION_ID, "evaluation_id"),
                row.isNullAt(TradeDecisionsTableColumns.COMPOSITE_SCORE) ? null
                        : row.getDouble(TradeDecisionsTableColumns.COMPOSITE_SCORE),
                requireNonNullString(
                        row, TradeDecisionsTableColumns.RESERVATION_ID, "reservation_id"),
                requireNonNullString(
                        row, TradeDecisionsTableColumns.RESERVATION_VERSION,
                        "reservation_version"),
                requireNonNullLong(row, TradeDecisionsTableColumns.CREATED_TS, "created_ts"),
                row.isNullAt(TradeDecisionsTableColumns.EXPIRY_TS) ? null
                        : row.getLong(TradeDecisionsTableColumns.EXPIRY_TS),
                row.isNullAt(TradeDecisionsTableColumns.SUPERSEDES_INSTRUCTION_ID) ? null
                        : row.getString(TradeDecisionsTableColumns.SUPERSEDES_INSTRUCTION_ID).toString(),
                row.isNullAt(TradeDecisionsTableColumns.SUPERSEDED_BY_INSTRUCTION_ID) ? null
                        : row.getString(TradeDecisionsTableColumns.SUPERSEDED_BY_INSTRUCTION_ID).toString());
    }

    private static String requireNonNullString(RowData row, int pos, String column) {
        if (row.isNullAt(pos)) {
            throw new IllegalArgumentException(column + " must not be null at pos " + pos);
        }
        return row.getString(pos).toString();
    }

    private static long requireNonNullLong(RowData row, int pos, String column) {
        if (row.isNullAt(pos)) {
            throw new IllegalArgumentException(column + " must not be null at pos " + pos);
        }
        return row.getLong(pos);
    }
}
