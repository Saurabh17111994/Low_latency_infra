package com.trading.common.schema.eod;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Live drill for the EOD shadow-copy write path — {@link
 * EodControllerTool#copyBucketsParallel} and the per-bucket {@code copyBucket}
 * it drives (SCH-24 / CHG-098/099, the retention-extend rewrite).
 *
 * <p>Why this exists: the copy path had no executed coverage. The
 * {@code eod-controller-test.sh} guards never copy — no day is ever due on a
 * guard run — and {@code EodControllerToolTest} covers only CLI parsing. That
 * left one change unverified by execution: the flush-in-{@code finally} removed
 * from {@code copyBucket} (2026-09-11, see {@code FlussWriteProfiles} for why a
 * per-record {@code flush()} is unbounded and how it masks a timeout). This
 * drill pins the property that flush was believed to protect — <b>once
 * {@code copyBucketsParallel} returns, every row is durable and readable by a
 * separate connection</b> — and crosses the {@link
 * EodControllerTool#COPY_WRITE_BATCH} boundary on the way, so both the
 * mid-loop {@code awaitBatch} and the final one are exercised.
 *
 * <p>Gated on {@code FLUSS_BOOTSTRAP} (e.g. {@code localhost:9123}) and tagged
 * {@code integration} like the other live-Fluss common tests. Creates its own
 * {@code compat_test_eodcopy_*} scratch tables and drops them; platform tables
 * are never touched.
 */
@Tag("integration")
class EodBucketCopyIntegrationTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(EodBucketCopyIntegrationTest.class);

    private static final String PREFIX = "compat_test_eodcopy_";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /** Two buckets, so the copy really uses its per-bucket worker pool. */
    private static final int BUCKETS = 2;

    private static final Schema COPY_SCHEMA = Schema.newBuilder()
            .column("key", DataTypes.STRING())
            .column("value", DataTypes.STRING())
            .column("status", DataTypes.STRING())
            .primaryKey("key")
            .build();

    private static String bootstrap;
    private static Connection connection;
    private static final List<String> CREATED_TABLES = new ArrayList<>();

    @BeforeAll
    static void connect() throws Exception {
        bootstrap = System.getenv("FLUSS_BOOTSTRAP");
        assumeTrue(bootstrap != null && !bootstrap.isBlank(),
                "set FLUSS_BOOTSTRAP to run the EOD bucket-copy drill");
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        connection = ConnectionFactory.createConnection(conf);
        LOG.info("eod-copy drill: connected to {}", bootstrap);
    }

    @AfterAll
    static void cleanup() throws Exception {
        if (connection != null) {
            for (String table : CREATED_TABLES) {
                try {
                    connection.getAdmin().dropTable(TablePath.of("default", table), false)
                            .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    LOG.info("eod-copy drill: dropped {}", table);
                } catch (Exception e) {
                    LOG.warn("eod-copy drill: drop {} failed: {}", table, e.getMessage());
                }
            }
            connection.close();
        }
    }

    private static void createTable(String name) throws Exception {
        TableDescriptor td = TableDescriptor.builder()
                .schema(COPY_SCHEMA)
                .distributedBy(BUCKETS, "key")
                .build();
        connection.getAdmin().createTable(TablePath.of("default", name), td, false)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        CREATED_TABLES.add(name);
    }

    private static BinaryString bs(String s) {
        return s == null ? null : BinaryString.fromString(s);
    }

    /**
     * Read a scratch KV table as key→value through its <b>own</b> connection — a
     * different client from the one that wrote, so a hit here means the rows were
     * durably acked, not merely buffered in the writer.
     *
     * <p>Scans the same way production does: an explicit {@code limit()} (Fluss
     * 0.9.1 refuses a BatchScanner without one) and drain-until-empty, since one
     * {@code pollBatch} is one time-bounded batch, not the bucket.
     */
    private static Map<String, String> readAllOverFreshConnection(String table) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        Map<String, String> out = new LinkedHashMap<>();
        try (Connection reader = ConnectionFactory.createConnection(conf)) {
            TableInfo info = reader.getAdmin().getTableInfo(TablePath.of("default", table))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            try (Table t = reader.getTable(TablePath.of("default", table))) {
                for (int b = 0; b < info.getNumBuckets(); b++) {
                    try (BatchScanner scanner = t.newScan().limit(Integer.MAX_VALUE)
                            .createBatchScanner(new TableBucket(info.getTableId(), b))) {
                        while (true) {
                            CloseableIterator<InternalRow> batch =
                                    scanner.pollBatch(Duration.ofMillis(250));
                            if (batch == null) {
                                break;
                            }
                            boolean any = false;
                            try (CloseableIterator<InternalRow> it = batch) {
                                while (it.hasNext()) {
                                    InternalRow row = it.next();
                                    out.put(row.getString(0).toString(),
                                            row.getString(1).toString());
                                    any = true;
                                }
                            }
                            if (!any) {
                                break;
                            }
                        }
                    }
                }
            }
        }
        return out;
    }

    /**
     * Load {@code rows} keys into the live table exactly the way the copy writes
     * them: async upserts with one bounded {@code allOf} await per batch and no
     * per-record flush.
     */
    private static void loadLive(Table live, int rows) throws Exception {
        UpsertWriter writer = live.newUpsert().createWriter();
        List<CompletableFuture<?>> batch = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            batch.add(writer.upsert(GenericRow.of(bs("k" + i), bs("v" + i), bs("ACTIVE"))));
            if (batch.size() == EodControllerTool.COPY_WRITE_BATCH) {
                await(batch);
            }
        }
        await(batch);
    }

    private static void await(List<CompletableFuture<?>> batch) throws Exception {
        try {
            CompletableFuture.allOf(batch.toArray(CompletableFuture[]::new))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            batch.clear();
        }
    }

    @Test
    @DisplayName("EOD shadow copy: every row is durable without a per-record flush")
    void shadowCopyIsDurableWithoutPerRecordFlush() throws Exception {
        String liveName = PREFIX + "live_" + System.nanoTime();
        String shadowName = PREFIX + "shadow_" + System.nanoTime();
        createTable(liveName);
        createTable(shadowName);

        // Several COPY_WRITE_BATCHes, so the mid-loop awaitBatch fires in each
        // bucket and the final one still has a remainder to flush out.
        int rows = EodControllerTool.COPY_WRITE_BATCH * 4;

        TablePath livePath = TablePath.of("default", liveName);
        TablePath shadowPath = TablePath.of("default", shadowName);
        TableInfo liveInfo = connection.getAdmin().getTableInfo(livePath)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Schema schema = liveInfo.getSchema();

        try (Table liveTable = connection.getTable(livePath);
             Table shadowTable = connection.getTable(shadowPath)) {
            loadLive(liveTable, rows);

            long copied = EodControllerTool.copyBucketsParallel(
                    liveTable, shadowTable, liveInfo, schema, TIMEOUT.toMillis());
            assertThat(copied).as("copyBucketsParallel must count every source row")
                    .isEqualTo(rows);
        }

        // The property the removed flush was believed to protect: the copy method
        // has returned, nothing was flushed explicitly, and an independent reader
        // must already see every row. First/middle/last pins content, not just a
        // count, so a truncated or aliased copy cannot pass.
        Map<String, String> shadowRows = readAllOverFreshConnection(shadowName);
        assertThat(shadowRows).as("every row durable in the shadow once the copy returns")
                .hasSize(rows);
        assertThat(shadowRows).containsEntry("k0", "v0");
        assertThat(shadowRows).containsEntry("k" + (rows / 2), "v" + (rows / 2));
        assertThat(shadowRows).containsEntry("k" + (rows - 1), "v" + (rows - 1));

        // The parity contract performRewrite enforces before it reports success.
        assertThat(readAllOverFreshConnection(liveName))
                .as("live and shadow must reconcile")
                .hasSameSizeAs(shadowRows);

        LOG.info("eod-copy drill: {} rows durable in {} with no per-record flush",
                rows, shadowName);
    }
}
