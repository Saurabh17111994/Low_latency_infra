package com.trading.execution.gateway;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;

/**
 * Quarantine contract for Tier 0 #6 bijective correlation guard.
 *
 * <p>Writes to {@code Postback_Quarantine} LOG table (16_postback_quarantine.sql, 13 cols).
 * Implementations must use the same Fluss append pattern as {@link FlussProjectionWriter}
 * (BinaryString fields, AppendWriter, observed future). A simple in-memory stub is
 * acceptable for offline tests but the Fluss-backed path must be durable.
 *
 * <p>Fail-closed: if {@link CorrelationResolver} returns {@code AMBIGUOUS}, callers must
 * {@link #quarantine} and halt the affected scope — no further lifecycle/position writes.
 */
public interface PostbackQuarantineStore {

    /**
     * Durably quarantine a postback that failed bijective correlation.
     *
     * @param postbackEventId immutable postback identity ({@code postback_event_id})
     * @param reason machine-readable reason, e.g. {@code AMBIGUOUS_CORRELATION}, {@code MISSING_BROKER_ID}
     * @param evidenceSummary human-readable detail for audit/disposition_reason
     * @param rawPayload original broker bytes (null stored as empty BYTES)
     */
    void quarantine(String postbackEventId, String reason, String evidenceSummary, byte[] rawPayload) throws Exception;

    /**
     * Fluss-backed durable implementation — same append pattern as FlussProjectionWriter.
     * Row layout matches 16_postback_quarantine.sql (13 cols).
     */
    final class Fluss implements PostbackQuarantineStore, AutoCloseable {
        private final Connection connection;
        private final Table table;
        private final long timeoutMs;

        public Fluss(Connection connection, Table table, Duration timeout) {
            this.connection = connection;
            this.table = table;
            this.timeoutMs = timeout.toMillis();
        }

        public static Fluss open(String bootstrap, String database, Duration timeout) throws Exception {
            return open(bootstrap, database, "Postback_Quarantine", timeout);
        }

        public static Fluss open(String bootstrap, String database, String tableName, Duration timeout) throws Exception {
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", bootstrap);
            Connection conn = ConnectionFactory.createConnection(conf);
            try {
                Table t = conn.getTable(TablePath.of(database, tableName));
                return new Fluss(conn, t, timeout);
            } catch (Exception e) {
                conn.close();
                throw e;
            }
        }

        @Override public void close() throws Exception { try { table.close(); } finally { connection.close(); } }

        @Override
        public void quarantine(String postbackEventId, String reason, String evidenceSummary, byte[] rawPayload) throws Exception {
            // P3-096: fail fast — "q-null" masks null as a colliding key while
            // v[1] stays null, and null reason violates reason NOT NULL only at
            // the server. P3-317: deterministic "q-"+id collides on retry
            // (same bucket key, confusing disposition) — UUID suffix.
            if (postbackEventId == null || postbackEventId.isBlank())
                throw new IllegalArgumentException("postbackEventId required");
            if (reason == null || reason.isBlank()) throw new IllegalArgumentException("reason required");
            String quarantineId = "q-" + postbackEventId + "-" + java.util.UUID.randomUUID();
            long now = System.currentTimeMillis();
            Object[] v = new Object[13];
            v[0] = bs(quarantineId);
            v[1] = bs(postbackEventId);
            v[2] = bs(reason);
            v[3] = rawPayload == null ? new byte[0] : rawPayload;
            v[4] = bs("unknown");
            v[5] = null;
            v[6] = null;
            v[7] = null;
            v[8] = BinaryString.fromString("OPEN");
            v[9] = bs(evidenceSummary);
            v[10] = now;
            v[11] = null;
            v[12] = BinaryString.fromString("2");
            AppendWriter writer = table.newAppend().createWriter();
            try {
                // P3-318: restore the interrupt flag — get() clears it, and a
                // swallowed interrupt stalls shutdown of the quarantine path.
                try {
                    writer.append(GenericRow.of(v)).get(timeoutMs, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw ie;
                }
                writer.flush();
            } catch (Exception e) {
                // Never let flush mask the append outcome (fail-closed diagnosis
                // needs the original Timeout/ExecutionException).
                try { writer.flush(); } catch (Exception suppressed) { e.addSuppressed(suppressed); }
                throw e;
            }
        }

        private static BinaryString bs(String s) { return s == null ? null : BinaryString.fromString(s); }
    }
}
