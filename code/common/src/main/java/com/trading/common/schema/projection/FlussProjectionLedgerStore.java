package com.trading.common.schema.projection;

import java.time.Duration;
import java.util.Optional;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;

/** Offline/test-only {@link ProjectionLedgerStore} for Postback_Projection_Ledger (T6).
 *
 * <p>P4-010 honesty note: despite the name, lookup/put touch ONLY the
 * in-memory delegate — {@code table} is never read or written, so state is
 * volatile and lost on restart. It is NOT opened against production Fluss
 * anywhere (driver tests pass InMemoryProjectionLedgerStore). The production
 * durable path is the gateway's FlussProjectionLedgerStore
 * (Lookuper + UpsertWriter, proven by the durable-replay integration tests).
 * Wiring a second writer to the same KV table risks dual-writer corruption —
 * a single-writer decision for the owning team, not a drive-by. Until then,
 * treat as in-memory-only. The {@code timeout} parameter is accepted for API
 * symmetry but inapplicable offline (P4-155 — no guessed config keys). */
public final class FlussProjectionLedgerStore implements ProjectionLedgerStore, AutoCloseable {
    private final Connection connection; private final Table table;
    private final InMemoryProjectionLedgerStore delegate=new InMemoryProjectionLedgerStore();
    private FlussProjectionLedgerStore(Connection c, Table t){ this.connection=c; this.table=t; }
    public static FlussProjectionLedgerStore open(String bootstrap,String db,String tbl,Duration timeout) throws Exception{
        Configuration conf=new Configuration(); conf.setString("bootstrap.servers", bootstrap);
        Connection conn=ConnectionFactory.createConnection(conf);
        try{ Table table=conn.getTable(TablePath.of(db,tbl)); return new FlussProjectionLedgerStore(conn,table); }
        catch(Exception e){ try { conn.close(); } catch (Exception closeEx) { e.addSuppressed(closeEx); } throw e; }
    }
    @Override public void close() throws Exception{ try { table.close(); } finally { connection.close(); } }
    @Override public Optional<ProjectionLedgerEntry> lookup(String id) throws Exception{ return delegate.lookup(id); }
    @Override public void put(ProjectionLedgerEntry e) throws Exception{ delegate.put(e); }
}
