package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.trading.common.schema.ownership.ExecutionGateColumns;
import com.trading.common.schema.fluss.WriteAwait;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** B4.2 HALTED-path E2E (non-market half): an immutable Execution_Intent reaches
 *  the real gateway reader and is DEFERRED — never executed — while the gate is
 *  not ENABLED and no durable fence token exists. This is the consumer-side
 *  wall of the B4.2 chain: intent exists in Fluss, the real IntentReader +
 *  DurableIntentDispatcher + NautilusIntentClient path runs, and the outcome is
 *  fail-closed (Result.DEFERRED, readiness not protocol/fluss-ready, zero
 *  Execution_Attempts / Order_Lifecycle rows, intent NOT committed to
 *  Execution_Intent_Processed so it stays replayable). No broker, no market
 *  hours, no sandbox credentials — runs whenever FLUSS_BOOTSTRAP is set (the
 *  same env gate as the T2 durable-replay evidence). Scratch DB, dropped at end. */
@Tag("integration")
class B4HaltedIntentConsumeDeferE2ETest {
    /**
     * Write/await budget. Measured 2026-09-18 in the full drill: under the churn's
     * teardown backlog the cluster answers NotLeader while a fresh replica finds its
     * leader, so a write to a table created seconds earlier can take 20-60s to ack —
     * other writes in that same run resolved on their second wait. {@link
     * com.trading.common.schema.fluss.WriteAwait} allows exactly one more wait for the
     * SAME future, so the intent append gets 2x this budget. The assertions are
     * unchanged: a write that never resolves still fails the run, it just no longer
     * fails at 40s while the cluster is merely slow.
     */
    private static final Duration TIMEOUT = Duration.ofSeconds(45);

    /**
     * Fixture readiness bound: the five tables are created milliseconds before the
     * append, and the coordinator places a fresh replica only after the previous
     * churn's teardown backlog drains (see
     * {@link FlussProjectionWriterIntegrationTest#awaitReady}).
     */
    private static final long FIXTURE_READY_BUDGET_MS = 240_000L;
    private static final String ACCOUNT = "b4halt-acct";
    private static final String PARTITION = "b4halt-part";
    private static final String HALTED_REASON_FENCE =
            "durable fence token is not available";

    @Test
    void haltedGateDefersIntentWithoutSideEffects() {
        String bootstrap = System.getenv("FLUSS_BOOTSTRAP");
        org.junit.jupiter.api.Assumptions.assumeTrue(
                bootstrap != null && !bootstrap.isBlank(),
                "set FLUSS_BOOTSTRAP for live B4.2 HALTED-path evidence");
        // Envelope above the worst legitimate case, not a budget for the step under
        // test: fixture readiness (FIXTURE_READY_BUDGET_MS) + a write that needs both
        // of its waits (2x TIMEOUT) + the reader's poll and the scratch cleanup.
        assertTimeoutPreemptively(Duration.ofSeconds(420), () -> {
            String db = "b4_halted_" + System.nanoTime();
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", bootstrap);
            // Set when the intent append never resolved. The scratch DB must then be KEPT:
            // dropping it leaves the writer's Sender retrying a record whose table no longer
            // exists — the busy loop that produced 115k metadata errors on 2026-09-18,
            // starved every other request on the cluster, and then blocked this test in
            // Connection.close() until its preemptive guard fired.
            boolean keepScratch = false;
            try (Connection conn = ConnectionFactory.createConnection(conf);
                 Admin admin = conn.getAdmin()) {
                admin.createDatabase(db, DatabaseDescriptor.EMPTY, false)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                try {
                    createIntentLog(admin, conn, db);
                    createIntentProcessedKv(admin, conn, db);
                    createExecutionGate(admin, conn, db);   // EMPTY — gate not ENABLED
                    createAttempts(admin, conn, db);        // EMPTY — nothing may land here
                    createOrderLifecycle(admin, conn, db);  // EMPTY
                    // Fixture readiness, not the step under test: the five tables were created
                    // milliseconds ago, and the coordinator places a fresh replica only once
                    // the previous churn's teardown backlog has drained (see awaitReady).
                    FlussProjectionWriterIntegrationTest.awaitReady(
                            "b4 halted fixture " + db, FIXTURE_READY_BUDGET_MS,
                            () -> tableRowCount(conn, db, "Execution_Intent"));
                    GatewayConfig config = config(db);
                    GatewayReadiness readiness = new GatewayReadiness();

                    try (FlussControlStateStore controls = FlussControlStateStore.open(config)) {
                        NautilusIntentClient sink = new NautilusIntentClient(config, controls, readiness);

                        // 1. An immutable intent row is already in the LOG (as the signal job writes it).
                        String id = "halt-instr-0001";
                        String hash = "aabbccdd00112233445566778899aabbccddeeff00112233445566778899aabb";
                        // The table handle is closed before the scratch DB is dropped, and the
                        // append is required to resolve (see keepScratch). Fluss 0.9.1's
                        // TableWriter has no close() (FlussHandlePool javadoc), so an abandoned
                        // writer whose record never resolved keeps a Sender retrying against a
                        // table that no longer exists — the storm this guard exists to prevent.
                        try (Table table = conn.getTable(TablePath.of(db, "Execution_Intent"))) {
                            AppendWriter w = table.newAppend().createWriter();
                            WriteAwait.State append = WriteAwait.await(
                                    w.append(intentRow(id, hash)), "intent append to " + db, TIMEOUT);
                            keepScratch = append == WriteAwait.State.UNRESOLVED;
                            assertThat(append)
                                    .as("the intent append must resolve; when it cannot, the"
                                            + " scratch DB is kept instead of being dropped"
                                            + " under a pending write")
                                    .isEqualTo(WriteAwait.State.RESOLVED);

                            // 2. The real reader runs the real consume path.
                            List<String> violations = new ArrayList<>();
                            try (IntentReader reader = IntentReader.open(
                                    config, sink, violations::add)) {
                                reader.subscribeFromBeginning();
                                int accepted = 0;
                                for (int i = 0; i < 40; i++) {
                                    accepted += reader.poll(Duration.ofMillis(250));
                                    if (accepted > 0) break;
                                }
                                // HALTED wall: the intent is consumed but NEVER handed off.
                                assertThat(accepted)
                                        .as("a HALTED gate must hand off nothing")
                                        .isZero();
                                assertThat(violations).as("valid intent must not violate")
                                        .isEmpty();
                            }

                            // 3. The fail-closed observable state (readiness contract).
                            GatewayReadiness.Snapshot snap = readiness.snapshot();
                            assertThat(snap.protocolReady())
                                    .as("no durable fence token -> protocol not ready")
                                    .isFalse();
                            assertThat(snap.flussReady())
                                    .as("gate lookup did not find an ENABLED row")
                                    .isFalse();
                            assertThat(snap.executionReady())
                                    .as("gate HALTED means execution not ready")
                                    .isFalse();

                            // 4. Forward() itself must answer DEFERRED (direct contract).
                            assertThat(sink.forward(IntentReader.decode(intentRow(id, hash), 0L)))
                                    .isEqualTo(IntentSink.Result.DEFERRED);

                            // 5. No execution side effects anywhere:
                            //    Execution_Attempts empty, Order_Lifecycle empty,
                            //    intent NOT marked processed (stays replayable).
                            assertThat(tableRowCount(conn, db, "Execution_Attempts"))
                                    .as("no Execution_Attempts while HALTED").isZero();
                            assertThat(tableRowCount(conn, db, "Order_Lifecycle"))
                                    .as("no Order_Lifecycle while HALTED").isZero();
                            assertThat(durableRecord(conn, db, id))
                                    .as("DEFERRED intents must not be committed as processed")
                                    .isNull();
                        }
                    }
                } finally {
                    if (keepScratch) {
                        System.out.println("b4-halted: KEPT scratch DB " + db
                                + " — its intent append never resolved; dropping it would spin"
                                + " the writer's Sender against a table that no longer exists");
                    } else {
                        try {
                            // cascade=true: the scratch DB holds tables, and a
                            // non-cascading drop throws DatabaseNotEmptyException —
                            // swallowed below, which leaked the DB on every run.
                            admin.dropDatabase(db, false, true)
                                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                        } catch (Exception ignored) {
                            // best-effort scratch cleanup
                        }
                    }
                }
            }
        });
    }

    private static GatewayConfig config(String db) {
        Map<String, String> m = new HashMap<>();
        m.put("FLUSS_BOOTSTRAP", System.getenv("FLUSS_BOOTSTRAP"));
        m.put("FLUSS_DATABASE", db);
        m.put("EXECUTION_INTENT_TABLE", "Execution_Intent");
        m.put("EXECUTION_GATE_TABLE", "Execution_Gate");
        m.put("EXECUTION_ATTEMPTS_TABLE", "Execution_Attempts");
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

    /** Execution_Intent LOG — 22 columns, mirror of 27_execution_intent.sql. */
    private static void createIntentLog(Admin admin, Connection conn, String db) throws Exception {
        Schema schema = Schema.newBuilder()
                .column("instruction_id", DataTypes.STRING())
                .column("candidate_id", DataTypes.STRING())
                .column("trade_context_id", DataTypes.STRING())
                .column("account_scope_id", DataTypes.STRING())
                .column("execution_partition_id", DataTypes.STRING())
                .column("instrument_token", DataTypes.BIGINT())
                .column("exchange", DataTypes.STRING())
                .column("symbol", DataTypes.STRING())
                .column("side", DataTypes.STRING())
                .column("quantity", DataTypes.BIGINT())
                .column("order_type", DataTypes.STRING())
                .column("limit_price_paise", DataTypes.BIGINT())
                .column("product_type", DataTypes.STRING())
                .column("time_in_force", DataTypes.STRING())
                .column("strategy_id", DataTypes.STRING())
                .column("strategy_version", DataTypes.STRING())
                .column("configuration_version", DataTypes.STRING())
                .column("created_ts", DataTypes.BIGINT())
                .column("expiry_ts", DataTypes.BIGINT())
                .column("request_hash", DataTypes.STRING())
                .column("supersedes_instruction_id", DataTypes.STRING())
                .column("schema_version", DataTypes.STRING())
                .build();
        TableDescriptor td = TableDescriptor.builder()
                .schema(schema).distributedBy(8, "instruction_id").build();
        admin.createTable(TablePath.of(db, "Execution_Intent"), td, false)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Execution_Intent_Processed KV — the durable dedup index (28_ DDL). */
    private static void createIntentProcessedKv(Admin admin, Connection conn, String db) throws Exception {
        Schema schema = Schema.newBuilder()
                .column("instruction_id", DataTypes.STRING())
                .column("request_hash", DataTypes.STRING())
                .column("handed_off_ts", DataTypes.BIGINT())
                .column("source_log_offset", DataTypes.BIGINT())
                .column("schema_version", DataTypes.STRING())
                .primaryKey("instruction_id")
                .build();
        TableDescriptor td = TableDescriptor.builder()
                .schema(schema).distributedBy(8, "instruction_id").build();
        admin.createTable(TablePath.of(db, "Execution_Intent_Processed"), td, false)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Execution_Gate KV — created EMPTY (no row): the HALTED default.
     *
     * <p>The schema is derived from {@link ExecutionGateColumns} rather than hand-written. It used
     * to be a hand-written 15-column list with {@code transition_ts} at index 7 and no
     * {@code approval_1}/{@code approval_2} at all — so this test created a table that does not
     * match the production Execution_Gate shape (17 columns), and would have kept passing while
     * the real schema moved underneath it. Deriving it makes a column add/rename/reorder fail
     * here loudly instead of silently testing a table nobody deploys.
     */
    private static void createExecutionGate(Admin admin, Connection conn, String db) throws Exception {
        Schema schema = executionGateSchema();
        TableDescriptor td = TableDescriptor.builder()
                .schema(schema).distributedBy(8, "execution_partition_id").build();
        admin.createTable(TablePath.of(db, "Execution_Gate"), td, false)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** The production Execution_Gate shape, built from the single canonical column definition. */
    private static Schema executionGateSchema() {
        Schema.Builder builder = Schema.newBuilder();
        for (int i = 0; i < ExecutionGateColumns.FIELD_COUNT; i++) {
            builder.column(ExecutionGateColumns.NAMES.get(i),
                    dataTypeFor(ExecutionGateColumns.TYPE_ROOTS.get(i)));
        }
        return builder
                .primaryKey(ExecutionGateColumns.NAMES.get(ExecutionGateColumns.EXECUTION_PARTITION_ID))
                .build();
    }

    private static DataType dataTypeFor(String typeRoot) {
        switch (typeRoot) {
            case "STRING":
                return DataTypes.STRING();
            case "BIGINT":
                return DataTypes.BIGINT();
            default:
                // Fail loud: an unmapped new column type must not silently become something else.
                throw new IllegalArgumentException(
                        "unmapped Execution_Gate type root: " + typeRoot);
        }
    }

    /** Execution_Attempts KV — EMPTY; nothing may be written while HALTED. */
    private static void createAttempts(Admin admin, Connection conn, String db) throws Exception {
        Schema schema = Schema.newBuilder()
                .column("execution_attempt_id", DataTypes.STRING())
                .column("prepared_ts", DataTypes.BIGINT())
                .column("submitted_ts", DataTypes.BIGINT())
                .primaryKey("execution_attempt_id")
                .build();
        TableDescriptor td = TableDescriptor.builder()
                .schema(schema).distributedBy(8, "execution_attempt_id").build();
        admin.createTable(TablePath.of(db, "Execution_Attempts"), td, false)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Order_Lifecycle KV — EMPTY; nothing may be written while HALTED. */
    private static void createOrderLifecycle(Admin admin, Connection conn, String db) throws Exception {
        Schema schema = Schema.newBuilder()
                .column("account_scope_id", DataTypes.STRING())
                .column("broker_order_id", DataTypes.STRING())
                .column("normalized_state", DataTypes.STRING())
                .primaryKey("account_scope_id", "broker_order_id")
                .build();
        TableDescriptor td = TableDescriptor.builder()
                .schema(schema).distributedBy(8, "account_scope_id").build();
        admin.createTable(TablePath.of(db, "Order_Lifecycle"), td, false)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** A canonical, validator-passing intent row (mirror of the signal-job write). */
    private static GenericRow intentRow(String id, String hash) {
        long now = System.currentTimeMillis();
        return GenericRow.of(bs(id), bs("cand-b4-1"), bs("tc-b4-1"), bs(ACCOUNT), bs(PARTITION),
                762583L, bs("NSE"), bs("BI-EQ"), bs("BUY"), 1L, bs("MARKET"),
                null, bs("CNC"), bs("DAY"), bs("strat-b4"), bs("v1"), bs("cfg-b4-20260821"),
                now, null, bs(hash), null, bs("1"));
    }

    private static BinaryString bs(String s) {
        return s == null ? null : BinaryString.fromString(s);
    }

    private static int tableRowCount(Connection conn, String db, String tableName) throws Exception {
        Table table = conn.getTable(TablePath.of(db, tableName));
        var info = table.getTableInfo();
        int n = 0;
        for (int bucket = 0; bucket < info.getNumBuckets(); bucket++) {
            try (BatchScanner scanner = table.newScan().limit(Integer.MAX_VALUE)
                    .createBatchScanner(new TableBucket(info.getTableId(), bucket));
                 CloseableIterator<InternalRow> it = scanner.pollBatch(TIMEOUT)) {
                while (it.hasNext()) {
                    it.next();
                    n++;
                }
            }
        }
        return n;
    }

    /** Returns the stored request_hash for an instruction_id, or null (not processed). */
    private static String durableRecord(Connection conn, String db, String id) throws Exception {
        Table table = conn.getTable(TablePath.of(db, "Execution_Intent_Processed"));
        var info = table.getTableInfo();
        for (int bucket = 0; bucket < info.getNumBuckets(); bucket++) {
            try (BatchScanner scanner = table.newScan().limit(Integer.MAX_VALUE)
                    .createBatchScanner(new TableBucket(info.getTableId(), bucket));
                 CloseableIterator<InternalRow> it = scanner.pollBatch(TIMEOUT)) {
                while (it.hasNext()) {
                    InternalRow r = it.next();
                    if (id.equals(r.getString(0).toString())) {
                        return r.getString(1).toString();
                    }
                }
            }
        }
        return null;
    }
}
