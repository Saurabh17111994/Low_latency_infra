package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

/** Live T2 durable-replay proof (env-gated by FLUSS_BOOTSTRAP): append one
 *  Execution_Intent, hand it off once, then restart the reader and prove the
 *  same (instruction_id, request_hash) replayed from offset 0 is NOT handed off
 *  a second time — the durable Execution_Intent_Processed index replaces the
 *  process-local duplicate guard (plan T2 line 714). Runs against a scratch
 *  database and drops it on completion. */
@Tag("integration")
class GatewayFlussDurableReplayIntegrationTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    private static final String ACCOUNT = "gr-acct";
    private static final String PARTITION = "gr-part";

    @Test void replayAfterRestartDoesNotHandoffTwice() {
        String bootstrap = System.getenv("FLUSS_BOOTSTRAP");
        org.junit.jupiter.api.Assumptions.assumeTrue(
                bootstrap != null && !bootstrap.isBlank(),
                "set FLUSS_BOOTSTRAP for live T2 durable-replay evidence");
        assertTimeoutPreemptively(Duration.ofSeconds(90), () -> {
            String db = "gateway_replay_" + System.nanoTime();
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", bootstrap);
            try (Connection conn = ConnectionFactory.createConnection(conf);
                 Admin admin = conn.getAdmin()) {
                admin.createDatabase(db, DatabaseDescriptor.EMPTY, false)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                try {
                    createIntentLog(admin, conn, db);
                    createIntentProcessedKv(admin, conn, db);
                    GatewayConfig config = config(db);

                    // Feed one source Execution_Intent.
                    String id = "instr-R-1";
                    // request_hash must be a 64-hex sha256 (IntentValidator:37); a
                    // human-readable placeholder here is rejected as an invalid
                    // Execution_Intent, so the reader would never hand it off.
                    String hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
                    Table intent = conn.getTable(TablePath.of(db, "Execution_Intent"));
                    AppendWriter w = intent.newAppend().createWriter();
                    try {
                        w.append(intentRow(id, hash)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    } finally {
                        w.flush();
                    }

                    // Reader #1 — first process: hands off once, durably records.
                    RecordingSink sink1 = new RecordingSink();
                    try (IntentReader reader1 = IntentReader.open(config, sink1, reason -> {})) {
                        reader1.subscribeFromBeginning();
                        awaitHandoff(reader1, sink1, id);
                        assertThat(sink1.handoffs(id)).isEqualTo(1);
                    }
                    // Reader #1 closed = in-memory guard lost (crash-simulated).

                    // Reader #2 — restart: same intent replayed from offset 0, but
                    // the durable index says it was already handed off -> no second
                    // handoff, and still exactly one durable record.
                    RecordingSink sink2 = new RecordingSink();
                    try (IntentReader reader2 = IntentReader.open(config, sink2, reason -> {})) {
                        reader2.subscribeFromBeginning();
                        // Drain a bounded window so any replayed record is observed.
                        for (int i = 0; i < 8; i++) {
                            reader2.poll(Duration.ofMillis(500));
                        }
                        assertThat(sink2.handoffs(id)).as("no duplicate handoff after restart")
                                .isZero();
                    }
                    assertThat(durableRecord(conn, admin, db, id)).contains(hash);
                } finally {
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
        });
    }

    /** Recoverable-ledger restart proof (plan line 718): a crash mid-apply leaves
     *  an incomplete ledger row; a fresh applier on the SAME durable store resumes
     *  without repeating completed steps to a terminal COMPLETE, and a re-apply is
     *  a duplicate no-op. Uses the real FlussProjectionLedgerStore in a scratch DB
     *  with a writing boundary that fails exactly once. */
    @Test void incompleteLedgerResumesAfterRestart() {
        String bootstrap = System.getenv("FLUSS_BOOTSTRAP");
        org.junit.jupiter.api.Assumptions.assumeTrue(
                bootstrap != null && !bootstrap.isBlank(),
                "set FLUSS_BOOTSTRAP for live T2 recoverable-ledger evidence");
        assertTimeoutPreemptively(Duration.ofSeconds(90), () -> {
            String db = "gateway_ledger_" + System.nanoTime();
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", bootstrap);
            try (Connection conn = ConnectionFactory.createConnection(conf);
                 Admin admin = conn.getAdmin()) {
                admin.createDatabase(db, DatabaseDescriptor.EMPTY, false)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                try {
                    createProjectionLedgerKv(admin, conn, db);
                    GatewayConfig config = config(db);
                    NormalizedExecutionEvent event = event("pe-recover-1");

                    // Crash mid-apply: the writer fails on the lifecycle write, so
                    // the ledger stops at LIFECYCLE_APPLIED (incomplete), not COMPLETE.
                    CountWriter failing = new CountWriter(true);
                    try (FlussProjectionLedgerStore ledger = FlussProjectionLedgerStore.open(config)) {
                        ProjectionApplier applier1 = new ProjectionApplier(failing, ledger);
                        org.assertj.core.api.Assertions.assertThatThrownBy(
                                () -> applier1.apply(event)).isInstanceOf(Exception.class);
                        ProjectionLedgerStore.Entry eased = ledger.lookup(event.postbackEventId());
                        assertThat(eased.state()).isNotEqualTo(ProjectionLedger.State.COMPLETE);
                    }
                    // "Restart": a fresh applier over the same durable store resumes.
                    CountWriter good = new CountWriter(false);
                    try (FlussProjectionLedgerStore ledger = FlussProjectionLedgerStore.open(config)) {
                        ProjectionApplier applier2 = new ProjectionApplier(good, ledger);
                        assertThat(applier2.apply(event)).isTrue();
                        assertThat(ledger.lookup(event.postbackEventId()).state())
                                .isEqualTo(ProjectionLedger.State.COMPLETE);
                        // Recovery reached COMPLETE WITHOUT repeating the already-
                        // completed audit step (audits stayed 0), while the failed
                        // lifecycle step WAS retried exactly once to completion.
                        assertThat(good.audits()).isZero();
                        assertThat(good.lifecycles()).isEqualTo(1);
                        // Re-applying the terminal event is a duplicate no-op.
                        ProjectionApplier applier3 = new ProjectionApplier(good, ledger);
                        assertThat(applier3.apply(event)).isFalse();
                        assertThat(good.audits()).isZero();
                        assertThat(good.lifecycles()).isEqualTo(1);
                    }
                } finally {
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
        });
    }

    /**
     * Live P3-102 proof: the recovery read is a BOUNDED page and its truncation flag is exact.
     *
     * <p>The flag is the safety-critical part. This read drives recovery, so a page that quietly
     * omitted work would skip it until TTL — which is why truncation is reported rather than
     * inferred, and why the interesting case is a FULL page that is not truncated (exactly as much
     * work as the limit, nothing beyond). A naive "page is full ⇒ truncated" implementation passes
     * every other case here and fails only that one.
     */
    @Test void incompletePageIsBoundedAndReportsTruncationExactly() {
        String bootstrap = System.getenv("FLUSS_BOOTSTRAP");
        org.junit.jupiter.api.Assumptions.assumeTrue(
                bootstrap != null && !bootstrap.isBlank(),
                "set FLUSS_BOOTSTRAP for live P3-102 bounded-recovery-page evidence");
        assertTimeoutPreemptively(Duration.ofSeconds(90), () -> {
            String db = "gateway_page_" + System.nanoTime();
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", bootstrap);
            try (Connection conn = ConnectionFactory.createConnection(conf);
                 Admin admin = conn.getAdmin()) {
                admin.createDatabase(db, DatabaseDescriptor.EMPTY, false)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                try {
                    createProjectionLedgerKv(admin, conn, db);
                    try (FlussProjectionLedgerStore ledger =
                                 FlussProjectionLedgerStore.open(config(db))) {
                        long now = System.currentTimeMillis();
                        for (int i = 0; i < 3; i++) {
                            ledger.put(new ProjectionLedgerStore.Entry("pg-" + i,
                                    ProjectionLedger.State.RECEIVED, null, 0, null, null, now, null));
                        }
                        // Terminal work is not recoverable and must not occupy the page.
                        ledger.put(new ProjectionLedgerStore.Entry("pg-done",
                                ProjectionLedger.State.COMPLETE, null, 0, null, null, now, now));

                        // Bounded: fewer than the limit available.
                        ProjectionLedgerStore.IncompletePage small = ledger.incomplete(2);
                        assertThat(small.entries()).as("page size").hasSize(2);
                        assertThat(small.truncated()).as("3 recoverable exist, page holds 2").isTrue();

                        // EXACTNESS: a full page with no further work is NOT truncated.
                        ProjectionLedgerStore.IncompletePage exact = ledger.incomplete(3);
                        assertThat(exact.entries()).hasSize(3);
                        assertThat(exact.truncated())
                                .as("a full page that exhausts the work must not claim truncation")
                                .isFalse();

                        // Room to spare: everything, untruncated, terminal row excluded.
                        ProjectionLedgerStore.IncompletePage all = ledger.incomplete(50);
                        assertThat(all.entries()).hasSize(3);
                        assertThat(all.entries()).allMatch(
                                e -> ProjectionLedger.recoverable(e.state()));
                        assertThat(all.entries()).noneMatch(e -> "pg-done".equals(e.eventId()));
                        assertThat(all.truncated()).isFalse();

                        // No unbounded escape hatch: a non-positive limit is rejected.
                        org.assertj.core.api.Assertions.assertThatThrownBy(() -> ledger.incomplete(0))
                                .isInstanceOf(IllegalArgumentException.class);
                    }
                } finally {
                    try {
                        admin.dropDatabase(db, false, true)
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    } catch (Exception ignored) {
                        // best-effort scratch cleanup
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

    private static void createProjectionLedgerKv(Admin admin, Connection conn, String db) throws Exception {
        Schema schema = Schema.newBuilder()
                .column("postback_event_id", DataTypes.STRING())
                .column("projection_state", DataTypes.STRING())
                .column("expected_prior_state", DataTypes.STRING())
                .column("retry_count", DataTypes.INT())
                .column("last_error", DataTypes.STRING())
                .column("disposition", DataTypes.STRING())
                .column("step_ts", DataTypes.BIGINT())
                .column("completed_ts", DataTypes.BIGINT())
                .column("schema_version", DataTypes.STRING())
                .primaryKey("postback_event_id")
                .build();
        TableDescriptor td = TableDescriptor.builder()
                .schema(schema).distributedBy(8, "postback_event_id").build();
        admin.createTable(TablePath.of(db, "Postback_Projection_Ledger"), td, false)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static NormalizedExecutionEvent event(String eventId) {
        long now = System.currentTimeMillis();
        return new NormalizedExecutionEvent(eventId, ACCOUNT, PARTITION, 0L, "actor-1",
                "FILL", now,
                new NormalizedExecutionEvent.Audit("audit-" + eventId, "evh", "summary"),
                null,
                new NormalizedExecutionEvent.Lifecycle("broker-1", "instr-L", "att-1",
                        "tc-1", "FILLED", 100L, 0L, 150L, 1L, now, now, "CORRELATED"),
                null, null);
    }

    private static GenericRow intentRow(String id, String hash) {
        long now = System.currentTimeMillis();
        return GenericRow.of(bs(id), bs("cand-1"), bs("tc-1"), bs(ACCOUNT), bs(PARTITION),
                12345L, bs("NSE"), bs("RELIANCE"), bs("BUY"), 100L, bs("MARKET"),
                null, bs("DELIVERY"), bs("DAY"), bs("strat-1"), bs("v1"), bs("cfg-1"),
                now, null, bs(hash), null, bs("1"));
    }

    private static BinaryString bs(String s) { return s == null ? null : BinaryString.fromString(s); }

    private static void awaitHandoff(IntentReader reader, RecordingSink sink, String id) throws Exception {
        for (int i = 0; i < 40; i++) {
            reader.poll(Duration.ofMillis(250));
            if (sink.handoffs(id) > 0) return;
        }
    }

    private static String durableRecord(Connection conn, Admin admin, String db, String id)
            throws Exception {
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

    static final class RecordingSink implements IntentSink {
        final List<String> handed = new ArrayList<>();
        @Override public Result forward(IntentRecord intent) {
            handed.add(intent.instructionId());
            return Result.FORWARDED;
        }
        long handoffs(String id) { return handed.stream().filter(id::equals).count(); }
    }

    static final class CountWriter implements ProjectionWriter {
        private final boolean failOnLifecycle;
        private int audits;
        private int lifecycles;
        CountWriter(boolean failOnLifecycle) { this.failOnLifecycle = failOnLifecycle; }
        @Override public void writeAudit(NormalizedExecutionEvent event) { audits++; }
        @Override public void writeLifecycle(NormalizedExecutionEvent event) throws Exception {
            if (failOnLifecycle) throw new IllegalStateException("simulated lifecycle write failure");
            lifecycles++;
        }
        @Override public void writePosition(NormalizedExecutionEvent event) { }
        @Override public void close() { }
        int audits() { return audits; }
        int lifecycles() { return lifecycles; }
    }
}
