package com.trading.common.schema.position;

import com.trading.common.model.PositionState;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

/**
 * {@link PositionsStateStore} backed by the Positions KV table via the Fluss
 * raw client (SCH-20). {@code position_id} is a single-field PK with
 * {@code bucket.key = position_id}, so the raw client upsert works directly
 * (COMPAT-FLUSS-005 — the composite-PK limitation does not apply). Writes are
 * full row images via {@link UpsertWriter}; reads are point lookups via
 * {@link Lookuper}. Owns its {@link Connection} — {@link #close()} releases it.
 */
public final class FlussPositionsStateStore implements PositionsStateStore, AutoCloseable {

    private final Connection connection;
    private final Table table;
    private final long timeoutMs;

    private FlussPositionsStateStore(Connection connection, Table table, long timeoutMs) {
        this.connection = connection;
        this.table = table;
        this.timeoutMs = timeoutMs;
    }

    /** Open the store against {@code database.tableName} on the cluster. */
    public static FlussPositionsStateStore open(String bootstrap, String database,
            String tableName, Duration timeout) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        Connection connection = ConnectionFactory.createConnection(conf);
        try {
            TableInfo info = connection.getAdmin().getTableInfo(TablePath.of(database, tableName))
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            Table table = connection.getTable(TablePath.of(database, tableName));
            return new FlussPositionsStateStore(connection, table, timeout.toMillis());
        } catch (Exception e) {
            connection.close();
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    @Override
    public PositionSnapshot lookup(String positionId) throws Exception {
        // P4-311 note: Lookuper is a bare interface in Fluss 0.9.1 (no
        // AutoCloseable, no close()) — per-lookup creation holds no
        // releasable handle; nothing to close. Documented so the next audit
        // does not re-raise the leak.
        Lookuper lookuper = table.newLookup().createLookuper();
        InternalRow found = lookuper.lookup(GenericRow.of(BinaryString.fromString(positionId)))
                .get(timeoutMs, TimeUnit.MILLISECONDS).getSingletonRow();
        return found == null ? null : toSnapshot(found);
    }

    @Override
    public void upsert(PositionSnapshot s) throws Exception {
        Objects.requireNonNull(s, "snapshot");
        Object[] values = new Object[PositionsColumns.FIELD_COUNT];
        values[PositionsColumns.POSITION_ID] = BinaryString.fromString(s.positionId());
        values[PositionsColumns.TRADE_CONTEXT_ID] = bs(s.tradeContextId());
        values[PositionsColumns.ACCOUNT_SCOPE_ID] = BinaryString.fromString(s.accountScopeId());
        values[PositionsColumns.INSTRUMENT_TOKEN] = s.instrumentToken();
        values[PositionsColumns.EXCHANGE] = BinaryString.fromString(s.exchange());
        values[PositionsColumns.SYMBOL] = BinaryString.fromString(s.symbol());
        values[PositionsColumns.SIDE] = BinaryString.fromString(s.side());
        values[PositionsColumns.STATE] = BinaryString.fromString(s.state().name());
        values[PositionsColumns.OPEN_QUANTITY] = s.openQuantity();
        values[PositionsColumns.CLOSED_QUANTITY] = s.closedQuantity();
        values[PositionsColumns.AVERAGE_ENTRY_PAISE] = s.averageEntryPaise();
        values[PositionsColumns.AVERAGE_EXIT_PAISE] = s.averageExitPaise();
        values[PositionsColumns.SOURCE_EVENT_ID] = BinaryString.fromString(s.sourceEventId());
        values[PositionsColumns.SOURCE_VERSION] = s.sourceVersion();
        values[PositionsColumns.CREATED_TS] = s.createdTs();
        values[PositionsColumns.LAST_UPDATE_TS] = s.lastUpdateTs();
        values[PositionsColumns.SCHEMA_VERSION] = BinaryString.fromString(s.schemaVersion());
        UpsertWriter writer = table.newUpsert().createWriter();
        try {
            writer.upsert(GenericRow.of(values)).get(timeoutMs, TimeUnit.MILLISECONDS);
        } finally {
            // P4-150 note: UpsertWriter/TableWriter is flush()-only in Fluss
            // 0.9.1 (no AutoCloseable, no close()) — same shape as the
            // gateway's FlussProjectionLedgerStore. Nothing to close beyond
            // flush; documented so the next audit does not re-raise the leak.
            writer.flush();
        }
    }

    private static BinaryString bs(String s) {
        return s == null ? BinaryString.EMPTY_UTF8 : BinaryString.fromString(s);
    }

    private static PositionSnapshot toSnapshot(InternalRow r) {
        // P4-352: avg prices are nullable in DDL (NULL iff matching qty is 0
        // per the 10_positions header contract) — decode NULL as 0L, the
        // record's absent-value, instead of crashing on getLong.
        long avgEntry = r.isNullAt(PositionsColumns.AVERAGE_ENTRY_PAISE)
                ? 0L : r.getLong(PositionsColumns.AVERAGE_ENTRY_PAISE);
        long avgExit = r.isNullAt(PositionsColumns.AVERAGE_EXIT_PAISE)
                ? 0L : r.getLong(PositionsColumns.AVERAGE_EXIT_PAISE);
        return new PositionSnapshot(
                r.getString(PositionsColumns.POSITION_ID).toString(),
                r.getString(PositionsColumns.TRADE_CONTEXT_ID).toString(),
                r.getString(PositionsColumns.ACCOUNT_SCOPE_ID).toString(),
                r.getLong(PositionsColumns.INSTRUMENT_TOKEN),
                r.getString(PositionsColumns.EXCHANGE).toString(),
                r.getString(PositionsColumns.SYMBOL).toString(),
                r.getString(PositionsColumns.SIDE).toString(),
                PositionState.valueOf(r.getString(PositionsColumns.STATE).toString()),
                r.getLong(PositionsColumns.OPEN_QUANTITY),
                r.getLong(PositionsColumns.CLOSED_QUANTITY),
                avgEntry,
                avgExit,
                r.getString(PositionsColumns.SOURCE_EVENT_ID).toString(),
                r.getLong(PositionsColumns.SOURCE_VERSION),
                r.getLong(PositionsColumns.CREATED_TS),
                r.getLong(PositionsColumns.LAST_UPDATE_TS),
                r.getString(PositionsColumns.SCHEMA_VERSION).toString());
    }
}
