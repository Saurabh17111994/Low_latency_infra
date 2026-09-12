package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * WP-4 (T6) live projection-write drill, env-gated by {@code FLUSS_BOOTSTRAP}: drives a normalized
 * execution event (audit/fill/lifecycle/position/correlation) through the REAL
 * {@link FlussProjectionWriter} into Fluss scratch tables and reads the KV projections back.
 * This closes the step-1 gap: the existing durable-replay test uses a fake writer; here the actual
 * FlussProjectionWriter is exercised end-to-end against a live cluster (no arithmetic in JVM).
 */
@Tag("fluss")
class FlussProjectionWriterIntegrationTest {

    private static final String ACCOUNT = "acct-live-proj";
    private static final String PARTITION = "part-live-proj";
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    @Test
    @DisplayName("WP-4: postback -> normalized envelope -> real FlussProjectionWriter lands projection rows")
    void projectionWritesReachFlussAndKvUpsertIsIdempotent() throws Exception {
        String bootstrap = System.getenv("FLUSS_BOOTSTRAP");
        Assumptions.assumeTrue(bootstrap != null && !bootstrap.isBlank(),
                "set FLUSS_BOOTSTRAP for live WP-4 projection-write evidence");

        String db = "proj_" + Long.toHexString(System.nanoTime());
        Connection conn = null;
        Admin admin = null;
        try {
            Configuration c = new Configuration();
            c.setString("bootstrap.servers", bootstrap);
            conn = ConnectionFactory.createConnection(c);
            admin = conn.getAdmin();
            admin.createDatabase(db, DatabaseDescriptor.EMPTY, false)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            createFills(admin, db);
            createOrderLifecycle(admin, db);
            createPositions(admin, db);
            createPositionState(admin, db);
            createOrderCorrelation(admin, db);
            createExecutionAudit(admin, db);

            NormalizedExecutionEvent e = event("pb-proj-1");
            try (FlussProjectionWriter writer = FlussProjectionWriter.open(config(bootstrap, db))) {
                writer.writeAudit(e);
                writer.writeLifecycle(e);
                writer.writePosition(e);
                // Re-drive the same event: upsert-by-key must stay idempotent (single rows).
                writer.writePosition(e);
                writer.writeLifecycle(e);
            }

            // KV read-back via Lookuper on the live cluster.
            // Positions PK = position_id; col 0 = position_id, col 12 = source_event_id.
            assertThat(readString(conn, db, "Positions",
                    new String[]{"pos-proj-1"}, 0)).isEqualTo("pos-proj-1");
            assertThat(readString(conn, db, "Positions",
                    new String[]{"pos-proj-1"}, 12)).isEqualTo("pb-proj-1");
            // Position_State handshake (Option B): per-instrument OPEN/CLOSED
            // Positions OPEN -> Position_State status OPEN, PK (account, token).
            assertThat(readPositionStateLong(conn, db, 12345L, 1)).isEqualTo(12345L);
            assertThat(readPositionStateString(conn, db, 12345L, 2)).isEqualTo("OPEN");
            // Order_Lifecycle PK = (account_scope_id, broker_order_id); col 1 = broker_order_id.
            assertThat(readString(conn, db, "Order_Lifecycle",
                    new String[]{ACCOUNT, "broker-proj-1"}, 1)).isEqualTo("broker-proj-1");
            // Order_Correlation PK = (instruction_id, execution_attempt_id); col 7 = verification_state.
            assertThat(readString(conn, db, "Order_Correlation",
                    new String[]{"instr-proj-1", "att-proj-1"}, 7)).isEqualTo("VERIFIED");
            // Idempotency: after double write, Positions still exactly the one source event (no dup).
            assertThat(readString(conn, db, "Positions",
                    new String[]{"pos-proj-1"}, 12)).isEqualTo("pb-proj-1");
        } finally {
            if (admin != null) {
                // cascade=true: the scratch DB holds tables, and a non-cascading
                // drop throws DatabaseNotEmptyException — swallowed below, which
                // leaked the database on every run.
                try { admin.dropDatabase(db, false, true).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS); }
                catch (Exception ignored) { }
            }
            if (conn != null) conn.close();
        }
    }

    static GatewayConfig config(String bootstrap, String db) {
        Map<String, String> m = new HashMap<>();
        m.put("FLUSS_BOOTSTRAP", bootstrap);
        m.put("FLUSS_DATABASE", db);
        m.put("EXECUTION_GATE_TABLE", "Execution_Gate");
        m.put("EXECUTION_ATTEMPTS_TABLE", "Execution_Attempts");
        m.put("EXECUTION_INTENT_TABLE", "Execution_Intent");
        m.put("ORDER_CORRELATION_TABLE", "Order_Correlation");
        m.put("PROJECTION_LEDGER_TABLE", "Postback_Projection_Ledger");
        m.put("SAFETY_HALT_TABLE", "Safety_Halt_Requests");
        m.put("GATEWAY_BIND_HOST", "127.0.0.1");
        m.put("GATEWAY_BIND_PORT", "9180");
        m.put("NAUTILUS_PRIVATE_ENDPOINT", "http://127.0.0.1:9190/v1/intents");
        m.put("GATEWAY_PROTOCOL_VERSION", "execution-gateway.v1");
        m.put("GATEWAY_SHARED_SECRET", "private");
        m.put("GATEWAY_REQUEST_TIMEOUT_MS", "2000");
        m.put("GATEWAY_POLL_TIMEOUT_MS", "250");
        m.put("ACCOUNT_SCOPE_ID", ACCOUNT);
        m.put("EXECUTION_PARTITION_ID", PARTITION);
        return GatewayConfig.from(m);
    }

    private static String readString(Connection conn, String db, String tableName,
                                       String[] pk, int colIndex) throws Exception {
        Table t = conn.getTable(TablePath.of(db, tableName));
        BinaryString[] key = new BinaryString[pk.length];
        for (int i = 0; i < pk.length; i++) key[i] = BinaryString.fromString(pk[i]);
        Lookuper lookuper = t.newLookup().createLookuper();
        InternalRow r = lookuper.lookup(GenericRow.of((Object[]) key))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).getSingletonRow();
        if (r == null) return null;
        return r.isNullAt(colIndex) ? null : r.getString(colIndex).toString();
    }

    private static Long readLong(Connection conn, String db, String tableName,
                                 String[] pk, int colIndex) throws Exception {
        Table t = conn.getTable(TablePath.of(db, tableName));
        BinaryString[] key = new BinaryString[pk.length];
        for (int i = 0; i < pk.length; i++) key[i] = BinaryString.fromString(pk[i]);
        Lookuper lookuper = t.newLookup().createLookuper();
        InternalRow r = lookuper.lookup(GenericRow.of((Object[]) key))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).getSingletonRow();
        if (r == null || r.isNullAt(colIndex)) return null;
        return r.getLong(colIndex);
    }

    private static Long readPositionStateLong(Connection conn, String db, long token, int colIndex) throws Exception {
        Table t = conn.getTable(TablePath.of(db, "Position_State"));
        Lookuper lookuper = t.newLookup().createLookuper();
        InternalRow r = lookuper.lookup(GenericRow.of(BinaryString.fromString(ACCOUNT), token))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).getSingletonRow();
        if (r == null || r.isNullAt(colIndex)) return null;
        return r.getLong(colIndex);
    }

    private static String readPositionStateString(Connection conn, String db, long token, int colIndex) throws Exception {
        Table t = conn.getTable(TablePath.of(db, "Position_State"));
        Lookuper lookuper = t.newLookup().createLookuper();
        InternalRow r = lookuper.lookup(GenericRow.of(BinaryString.fromString(ACCOUNT), token))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).getSingletonRow();
        if (r == null || r.isNullAt(colIndex)) return null;
        return r.getString(colIndex).toString();
    }

    static NormalizedExecutionEvent event(String eventId) {
        long now = System.currentTimeMillis();
        return new NormalizedExecutionEvent(eventId, ACCOUNT, PARTITION, 0L, "actor-1",
                "FILL", now,
                new NormalizedExecutionEvent.Audit("audit-proj-1", "evh", "summary"),
                new NormalizedExecutionEvent.Fill("fp-proj-1", "v1", "broker-proj-1",
                        "instr-proj-1", "att-proj-1", "tc-proj-1", "EXECUTED",
                        100L, 0L, 100L, 150_00L, "fill-proj-1", now, now, now,
                        new byte[]{1, 2, 3}, "ph-proj-1", "CORRELATED", null, "v1"),
                new NormalizedExecutionEvent.Lifecycle("broker-proj-1", "instr-proj-1",
                        "att-proj-1", "tc-proj-1", "FILLED", 100L, 0L, 150_00L, 1L, now, now, "CORRELATED"),
                new NormalizedExecutionEvent.Position("pos-proj-1", "tc-proj-1", 12345L,
                        "NSE", "RELIANCE", "BUY", "OPEN", 100L, 0L, 150_00L, null, 1L, now, now),
                new NormalizedExecutionEvent.Correlation("instr-proj-1", "att-proj-1",
                        "clref-proj-1", "broker-proj-1", "tc-proj-1", "pos-proj-1",
                        "VERIFIED", "ev", now));
    }

    /* ---- scratch DDL mirrors (column order == FlussProjectionWriter row order) ---- */

    static void createFills(Admin admin, String db) throws Exception {
        Schema s = Schema.newBuilder()
                .column("postback_event_id", DataTypes.STRING())
                .column("postback_fingerprint", DataTypes.STRING())
                .column("fingerprint_version", DataTypes.STRING())
                .column("account_scope_id", DataTypes.STRING())
                .column("broker_order_id", DataTypes.STRING())
                .column("instruction_id", DataTypes.STRING())
                .column("execution_attempt_id", DataTypes.STRING())
                .column("trade_context_id", DataTypes.STRING())
                .column("order_status", DataTypes.STRING())
                .column("cumulative_qty", DataTypes.BIGINT())
                .column("pending_qty", DataTypes.BIGINT())
                .column("fill_qty", DataTypes.BIGINT())
                .column("fill_price_paise", DataTypes.BIGINT())
                .column("fill_id", DataTypes.STRING())
                .column("broker_event_time", DataTypes.BIGINT())
                .column("receive_time", DataTypes.BIGINT())
                .column("ingest_ts", DataTypes.BIGINT())
                .column("original_payload", DataTypes.BYTES())
                .column("payload_hash", DataTypes.STRING())
                .column("correlation_state", DataTypes.STRING())
                .column("correlation_reason", DataTypes.STRING())
                .column("decoder_version", DataTypes.STRING())
                .column("schema_version", DataTypes.STRING())
                .build();
        admin.createTable(TablePath.of(db, "Fills"),
                TableDescriptor.builder().schema(s).distributedBy(8, "postback_event_id").build(), false)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    static void createOrderLifecycle(Admin admin, String db) throws Exception {
        Schema s = Schema.newBuilder()
                .column("account_scope_id", DataTypes.STRING())
                .column("broker_order_id", DataTypes.STRING())
                .column("instruction_id", DataTypes.STRING())
                .column("execution_attempt_id", DataTypes.STRING())
                .column("trade_context_id", DataTypes.STRING())
                .column("normalized_state", DataTypes.STRING())
                .column("cumulative_qty", DataTypes.BIGINT())
                .column("pending_qty", DataTypes.BIGINT())
                .column("average_fill_price_paise", DataTypes.BIGINT())
                .column("source_event_id", DataTypes.STRING())
                .column("source_version", DataTypes.BIGINT())
                .column("source_event_time", DataTypes.BIGINT())
                .column("last_receive_time", DataTypes.BIGINT())
                .column("correlation_state", DataTypes.STRING())
                .column("schema_version", DataTypes.STRING())
                .primaryKey("account_scope_id", "broker_order_id")
                .build();
        admin.createTable(TablePath.of(db, "Order_Lifecycle"),
                TableDescriptor.builder().schema(s).distributedBy(8, "account_scope_id").build(), false)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    static void createPositions(Admin admin, String db) throws Exception {
        Schema s = Schema.newBuilder()
                .column("position_id", DataTypes.STRING())
                .column("trade_context_id", DataTypes.STRING())
                .column("account_scope_id", DataTypes.STRING())
                .column("instrument_token", DataTypes.BIGINT())
                .column("exchange", DataTypes.STRING())
                .column("symbol", DataTypes.STRING())
                .column("side", DataTypes.STRING())
                .column("state", DataTypes.STRING())
                .column("open_quantity", DataTypes.BIGINT())
                .column("closed_quantity", DataTypes.BIGINT())
                .column("average_entry_paise", DataTypes.BIGINT())
                .column("average_exit_paise", DataTypes.BIGINT())
                .column("source_event_id", DataTypes.STRING())
                .column("source_version", DataTypes.BIGINT())
                .column("created_ts", DataTypes.BIGINT())
                .column("last_update_ts", DataTypes.BIGINT())
                .column("schema_version", DataTypes.STRING())
                .primaryKey("position_id")
                .build();
        admin.createTable(TablePath.of(db, "Positions"),
                TableDescriptor.builder().schema(s).distributedBy(8, "position_id").build(), false)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    static void createPositionState(Admin admin, String db) throws Exception {
        Schema s = Schema.newBuilder()
                .column("account_scope_id", DataTypes.STRING())
                .column("instrument_token", DataTypes.BIGINT())
                .column("status", DataTypes.STRING())
                .column("position_id", DataTypes.STRING())
                .column("updated_ts", DataTypes.BIGINT())
                .column("closed_ts", DataTypes.BIGINT())
                .column("closed_reason", DataTypes.STRING())
                .column("source_version", DataTypes.BIGINT())
                .column("schema_version", DataTypes.STRING())
                .primaryKey("account_scope_id", "instrument_token")
                .build();
        admin.createTable(TablePath.of(db, "Position_State"),
                TableDescriptor.builder().schema(s).distributedBy(16, "instrument_token").build(), false)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    static void createOrderCorrelation(Admin admin, String db) throws Exception {
        Schema s = Schema.newBuilder()
                .column("instruction_id", DataTypes.STRING())
                .column("execution_attempt_id", DataTypes.STRING())
                .column("account_scope_id", DataTypes.STRING())
                .column("client_order_ref", DataTypes.STRING())
                .column("broker_order_id", DataTypes.STRING())
                .column("trade_context_id", DataTypes.STRING())
                .column("position_id", DataTypes.STRING())
                .column("verification_state", DataTypes.STRING())
                .column("verification_evidence", DataTypes.STRING())
                .column("correlated_ts", DataTypes.BIGINT())
                .column("schema_version", DataTypes.STRING())
                .primaryKey("instruction_id", "execution_attempt_id")
                .build();
        admin.createTable(TablePath.of(db, "Order_Correlation"),
                TableDescriptor.builder().schema(s).distributedBy(8, "instruction_id").build(), false)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    static void createExecutionAudit(Admin admin, String db) throws Exception {
        Schema s = Schema.newBuilder()
                .column("audit_event_id", DataTypes.STRING())
                .column("event_type", DataTypes.STRING())
                .column("instruction_id", DataTypes.STRING())
                .column("execution_attempt_id", DataTypes.STRING())
                .column("execution_partition_id", DataTypes.STRING())
                .column("account_scope_id", DataTypes.STRING())
                .column("gate_epoch", DataTypes.BIGINT())
                .column("actor_id", DataTypes.STRING())
                .column("evidence_hash", DataTypes.STRING())
                .column("evidence_summary", DataTypes.STRING())
                .column("event_ts", DataTypes.BIGINT())
                .column("schema_version", DataTypes.STRING())
                .build();
        admin.createTable(TablePath.of(db, "Execution_Audit"),
                TableDescriptor.builder().schema(s).distributedBy(8, "audit_event_id").build(), false)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }
}
