package com.trading.compute.signaljob;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;

/**
 * Raw-client access to the {@code Signal_Tentative_Markers} KV table — the
 * F4 crash-reconciliation primitive (CHG-121, 2026-09-01).
 *
 * <p>WHY: a TENTATIVE is decided on PARTIAL preview state and appended to the
 * immutable {@code Signal_Candidates} LOG before the tracking Flink state is
 * checkpointed. A TM crash in that gap rolls the state back while the LOG row
 * survives; on replay the bursty watermark delivers full-window previews, so
 * the partial-data decision is not re-derivable and the tentative settles
 * nothing (drill tm-kill-full-load-20260901-234448: 84/84 F4 orphans had
 * exactly one pre-kill tentative row). The durable marker written HERE at
 * tentative-emit time survives the crash and lets the final-candle path
 * reconcile: marker present → settle CONFIRM/CANCEL by the final rule;
 * marker absent → no tentative existed — silent as today.
 *
 * <p>Raw-client shape: PK {@code [candidate_id]}, bucket key
 * {@code candidate_id}, {@code table.kv.format-version=2} — the
 * COMPAT-FLUSS-005 combo (same machinery as fingerprint_dedup / instruments
 * raw-client writes). Owns its {@link Connection}; {@link #close()} releases
 * it. {@code candidate_id} is deterministic
 * ({@code ruleId-token-windowEnd-TENTATIVE}), so replayed re-emits converge
 * on the same marker row (idempotent upsert).
 */
public final class FlussTentativeMarkerStore implements AutoCloseable {

    private final Connection connection;
    private final Table table;
    private final long timeoutMs;
    /**
     * Reused lookuper (2026-09-02): a fresh Lookuper per exists() call put
     * per-call setup cost on every reconcile lookup (~68/s at 10Hz drill);
     * drill 20260902-022843 lost 4 settles to 5s lookup timeouts. The
     * store is used ONLY from the hook's single background thread, so
     * reuse is race-free (Lookuper is not AutoCloseable in 0.9.1 — same
 * as the forming-bar store; the client reclaims it with the table).
     */
    private final Lookuper lookuper;

    private FlussTentativeMarkerStore(Connection connection, Table table,
            Lookuper lookuper, long timeoutMs) {
        this.connection = connection;
        this.table = table;
        this.lookuper = lookuper;
        this.timeoutMs = timeoutMs;
    }

    /** Open the store against {@code database.table} on the cluster. */
    public static FlussTentativeMarkerStore open(String bootstrap, String database,
            String tableName, Duration timeout) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        Connection connection = ConnectionFactory.createConnection(conf);
        try {
            TablePath path = TablePath.of(database, tableName);
            connection.getAdmin().getTableInfo(path)
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            Table table = connection.getTable(path);
            Lookuper lookuper = table.newLookup().createLookuper();
            return new FlussTentativeMarkerStore(connection, table, lookuper,
                    timeout.toMillis());
        } catch (Exception e) {
            connection.close();
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    /**
     * Durable tentative marker (idempotent — the candidate_id is deterministic,
     * so a replayed re-emit upserts the same row). Written BEFORE the
     * tentative LOG row leaves the operator.
     */
    public void mark(String candidateId, long instrumentToken, long windowEnd, long markedTs)
            throws Exception {
        UpsertWriter writer = table.newUpsert().createWriter();
        try {
            GenericRow row = GenericRow.of(
                    BinaryString.fromString(candidateId),
                    instrumentToken,
                    windowEnd,
                    markedTs,
                    BinaryString.fromString("1"));
            writer.upsert(row).get(timeoutMs, TimeUnit.MILLISECONDS);
        } finally {
            writer.flush();
        }
    }

    /**
     * Does a marker exist for this candidate? The post-crash reconciliation
     * check on the final-candle path (pending == null).
     */
    public boolean exists(String candidateId) throws Exception {
        org.apache.fluss.row.InternalRow found = lookuper
                .lookup(GenericRow.of(BinaryString.fromString(candidateId)))
                .get(timeoutMs, TimeUnit.MILLISECONDS)
                .getSingletonRow();
        return found != null;
    }

    /**
     * Remove the marker after settlement (idempotent — deleting a missing key
     * is a no-op). Failure to delete only leaves a stale marker that the
     * 2d table TTL eventually reclaims.
     */
    public void clear(String candidateId) throws Exception {
        UpsertWriter writer = table.newUpsert().createWriter();
        try {
            // The PK column is a STRING → the key row must carry a
            // BinaryString, not a java String (a String key silently
            // targets a different key and the delete becomes a no-op).
            writer.delete(GenericRow.of(BinaryString.fromString(candidateId)))
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
        } finally {
            writer.flush();
        }
    }
}
