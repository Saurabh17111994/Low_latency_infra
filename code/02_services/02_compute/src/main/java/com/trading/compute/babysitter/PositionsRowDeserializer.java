package com.trading.compute.babysitter;

import com.trading.common.model.PositionState;
import com.trading.common.schema.position.PositionSnapshot;
import com.trading.common.schema.position.PositionsColumns;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deserializes a current-value {@code Positions} changelog row
 * ({@code RowData} full image) into a validated {@link PositionSnapshot}.
 *
 * <p>Only the deserializer parses and validates rows; it never computes
 * position/PnL arithmetic (Nautilus is the only authority) and never emits a
 * {@code Position_Actions} record. A row that fails schema/version/quantity
 * validation is counted as malformed and skipped — under no circumstance is
 * it routed to an action or broker path (Task 7 failure posture:
 * malformed/missing source means not-ready/no-action, not silent skip into a
 * live path; but a single bad changelog row is a non-fatal, counted skip).
 */
public final class PositionsRowDeserializer
        extends RichFlatMapFunction<RowData, PositionSnapshot> {

    private static final Logger LOG = LoggerFactory.getLogger(PositionsRowDeserializer.class);
    private static final long serialVersionUID = 1L;

    private transient Counter observed;
    private transient Counter malformed;

    @Override
    public void open(OpenContext ctx) {
        observed = getRuntimeContext().getMetricGroup()
                .counter("babysitter.positions.rows.observed");
        malformed = getRuntimeContext().getMetricGroup()
                .counter("babysitter.positions.rows.malformed");
    }

    @Override
    public void flatMap(RowData row, Collector<PositionSnapshot> out) {
        observed.inc();
        try {
            out.collect(toSnapshot(row));
        } catch (RuntimeException ex) {
            // P2-008: any structural failure (arity, cast, NPE, validation)
            // is a non-fatal counted skip, never a task-killing poison pill.
            malformed.inc();
            LOG.warn("babysitter: malformed Positions row skipped (no action): {}", ex.getMessage());
        }
    }

    /**
     * Parses and validates a full {@code Positions} row image into a
     * {@link PositionSnapshot}. Throws on missing required column, unsupported
     * {@code schema_version}, unknown {@code state}, or a quantity invariant
     * violation (validated by the {@link PositionSnapshot} constructor).
     */
    static PositionSnapshot toSnapshot(RowData row) {
        // P2-008: explicit arity gate — a short row must fail as a counted
        // malformed skip, not an uncaught IndexOutOfBounds restart loop.
        if (row.getArity() < PositionsColumns.FIELD_COUNT) {
            throw new IllegalArgumentException(
                    "Positions row arity " + row.getArity()
                            + ", expected " + PositionsColumns.FIELD_COUNT);
        }
        String schemaVersion = requireText(row, PositionsColumns.SCHEMA_VERSION);
        if (!PositionsColumns.SCHEMA_VERSION_V2.equals(schemaVersion)) {
            throw new IllegalArgumentException(
                    "unsupported Positions schema_version '" + schemaVersion + "'");
        }
        PositionState state;
        try {
            state = PositionState.valueOf(requireText(row, PositionsColumns.STATE));
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("unknown Positions state", ex);
        }
        long avgEntry = row.isNullAt(PositionsColumns.AVERAGE_ENTRY_PAISE)
                ? 0L : row.getLong(PositionsColumns.AVERAGE_ENTRY_PAISE);
        long avgExit = row.isNullAt(PositionsColumns.AVERAGE_EXIT_PAISE)
                ? 0L : row.getLong(PositionsColumns.AVERAGE_EXIT_PAISE);
        // Constructor validates position_id non-blank + open >= closed >= 0.
        // P2-009: every non-nullable long/string is guarded — BinaryRowData
        // reads null as silent 0, so unguarded reads invent valid snapshots.
        return new PositionSnapshot(
                requireText(row, PositionsColumns.POSITION_ID),
                requireText(row, PositionsColumns.TRADE_CONTEXT_ID),
                requireText(row, PositionsColumns.ACCOUNT_SCOPE_ID),
                requireLong(row, PositionsColumns.INSTRUMENT_TOKEN),
                requireText(row, PositionsColumns.EXCHANGE),
                requireText(row, PositionsColumns.SYMBOL),
                requireText(row, PositionsColumns.SIDE),
                state,
                requireLong(row, PositionsColumns.OPEN_QUANTITY),
                requireLong(row, PositionsColumns.CLOSED_QUANTITY),
                avgEntry,
                avgExit,
                requireText(row, PositionsColumns.SOURCE_EVENT_ID),
                requireLong(row, PositionsColumns.SOURCE_VERSION),
                requireLong(row, PositionsColumns.CREATED_TS),
                requireLong(row, PositionsColumns.LAST_UPDATE_TS),
                schemaVersion);
    }

    /** P2-009: null becomes a named IllegalArgumentException, never silent 0/NPE. */
    private static long requireLong(RowData row, int idx) {
        if (row.isNullAt(idx)) {
            throw new IllegalArgumentException(
                    "Positions column null: " + PositionsColumns.NAMES.get(idx));
        }
        return row.getLong(idx);
    }

    /** P2-009: same for strings — explicit name instead of bare NPE. */
    private static String requireText(RowData row, int idx) {
        if (row.isNullAt(idx)) {
            throw new IllegalArgumentException(
                    "Positions column null: " + PositionsColumns.NAMES.get(idx));
        }
        return row.getString(idx).toString();
    }
}
