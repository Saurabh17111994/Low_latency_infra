package com.trading.common.schema.projection;

import java.time.Duration;
import java.util.Optional;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;

/** Offline/test-only {@link LifecycleStore} for Order_Lifecycle (T6).
 *
 * <p>P4-154 honesty note: despite the name, lookup/upsert touch ONLY the
 * in-memory delegate — {@code table} is never read or written, so state is
 * volatile and lost on restart. It is NOT opened against production Fluss
 * anywhere. The production durable path is the postback projection driver
 * with its gateway stores. Wiring a second writer to the same KV table
 * risks dual-writer corruption — a single-writer decision for the owning
 * team, not a drive-by (same ruling as P4-009/P4-010). Until then, treat
 * as in-memory-only. The {@code timeout} parameter is accepted for API
 * symmetry but inapplicable offline (P4-315 — no guessed config keys). */
public final class FlussLifecycleStore implements LifecycleStore, AutoCloseable {
    private final Connection connection; private final Table table;
    private final InMemoryLifecycleStore delegate = new InMemoryLifecycleStore();
    private FlussLifecycleStore(Connection c, Table t){ this.connection=c; this.table=t; }
    public static FlussLifecycleStore open(String bootstrap, String db, String tbl, Duration timeout) throws Exception{
        Configuration conf=new Configuration(); conf.setString("bootstrap.servers", bootstrap);
        Connection conn=ConnectionFactory.createConnection(conf);
        try{ Table table=conn.getTable(TablePath.of(db,tbl)); return new FlussLifecycleStore(conn,table); }
        catch(Exception e){ try { conn.close(); } catch (Exception closeEx) { e.addSuppressed(closeEx); } throw e; }
    }
    @Override public void close() throws Exception{ try { table.close(); } finally { connection.close(); } }
    @Override public Optional<OrderLifecycleSnapshot> lookup(String a, String b) throws Exception { return delegate.lookup(a,b); }
    @Override public void upsert(OrderLifecycleSnapshot s) throws Exception { delegate.upsert(s); /* Fluss upsert would write via UpsertWriter using OrderLifecycleColumns */ }
}
