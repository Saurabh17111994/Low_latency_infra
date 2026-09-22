package com.trading.common.schema.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.trading.common.schema.fluss.FlussPlacementAwait;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * WP-4 step 3 (T1) live quarantine-write drill, env-gated by {@code FLUSS_BOOTSTRAP}: appends a
 * {@link QuarantinedPostback} through the REAL {@link FlussPostbackQuarantineStore} into a scratch
 * Postback_Quarantine LOG table, then reads the log back via {@link LogScanner} to prove the row
 * durably landed in Fluss.
 */
@Tag("fluss")
class FlussPostbackQuarantineStoreIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final int BUCKETS = 8;

    /**
     * Fixture-readiness budget for the pair to the LOG table: bounded and logged, asserts nothing.
     * The same value the KV-side readiness gate uses (CHG-212, CHG-213).
     */
    private static final Duration READY_BUDGET = Duration.ofSeconds(240);

    @Test
    @DisplayName("WP-4 T1: FlussPostbackQuarantineStore appends a durable quarantine LOG row, read back via log scan")
    void appendedQuarantineRowSurvivesAndIsReadableFromFlussLog() throws Exception {
        String bootstrap = System.getenv("FLUSS_BOOTSTRAP");
        Assumptions.assumeTrue(bootstrap != null && !bootstrap.isBlank(),
                "set FLUSS_BOOTSTRAP for live WP-4 T1 quarantine-store evidence");

        String db = "quar_" + Long.toHexString(System.nanoTime());
        Connection conn = null;
        Admin admin = null;
        try {
            Configuration c = new Configuration();
            c.setString("bootstrap.servers", bootstrap);
            conn = ConnectionFactory.createConnection(c);
            admin = conn.getAdmin();
            admin.createDatabase(db, DatabaseDescriptor.EMPTY, false)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            createPostbackQuarantine(admin, db);
            // createTable returns in ~10 ms, but that reports "metadata written", not "writable":
            // the first write waits for the bucket leader, measured 2026-09-22 at 1-13 s under
            // teardown load against this fixture's 5 s budget. Without this gate the test fails on a
            // cold cluster for a reason it does not assert. Postback_Quarantine is a LOG table, so
            // there is no primary key to probe -- see FlussPlacementAwait.
            FlussPlacementAwait.awaitPlacement(conn, admin, "Postback_Quarantine", READY_BUDGET);

            QuarantinedPostback q = new QuarantinedPostback(
                    "quar-1", "pb-1", QuarantineReason.AMBIGUOUS_CORRELATION,
                    new byte[]{4, 5, 6}, "ph-1", "broker-1", "instr-1", "attempt-1",
                    "OPEN", null, 1_700_000_000_000L, null, "2");
            try (FlussPostbackQuarantineStore store =
                         FlussPostbackQuarantineStore.open(bootstrap, db, "Postback_Quarantine", TIMEOUT)) {
                store.append(q);

                // P3-410/P3-169: read back THROUGH the store's own bounded scan rather than a
                // hand-rolled scanner, so this covers the real read path. Comparing the whole
                // record also pins that decode() is the exact inverse of append() — a column
                // written in one order and read in another would fail here, not in production.
                List<QuarantinedPostback> found = List.of();
                long deadline = System.currentTimeMillis() + 15_000;
                while (System.currentTimeMillis() < deadline && found.isEmpty()) {
                    found = store.scan(10).stream()
                            .filter(r -> "quar-1".equals(r.quarantineId()))
                            .toList();
                    if (found.isEmpty()) Thread.sleep(200);
                }

                assertThat(found).as("quarantine row 'quar-1' readable via store.scan").isNotEmpty();
                assertThat(found.get(0))
                        .as("decode() must invert append() exactly, field for field")
                        .isEqualTo(q);
                assertThatThrownBy(() -> store.scan(0))
                        .as("limit is bounded by contract — zero is rejected, not silently empty")
                        .isInstanceOf(IllegalArgumentException.class);

                // P3-410: the read is BOUNDED — the whole point, since the LOG grows without
                // limit while a caller's memory budget does not. Append two more and prove the
                // page never exceeds the limit even when more rows are durable.
                store.append(new QuarantinedPostback("quar-2", "pb-2",
                        QuarantineReason.MISSING_BROKER_ID, new byte[]{7}, "ph-2", null, "instr-2",
                        null, "OPEN", null, 1_700_000_000_001L, null, "2"));
                store.append(new QuarantinedPostback("quar-3", "pb-3",
                        QuarantineReason.MISSING_BROKER_ID, new byte[]{8}, "ph-3", null, "instr-3",
                        null, "OPEN", null, 1_700_000_000_002L, null, "2"));

                // Wait for all three to be visible before asserting the bound, so the small page
                // is genuinely smaller than what is readable rather than merely what is visible.
                long visibleDeadline = System.currentTimeMillis() + 15_000;
                while (System.currentTimeMillis() < visibleDeadline && store.scan(10).size() < 3) {
                    Thread.sleep(200);
                }
                assertThat(store.scan(10)).as("three appended rows").hasSize(3);
                assertThat(store.scan(2))
                        .as("a bounded read must never exceed the caller's limit")
                        .hasSize(2);
            }
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

    private static void createPostbackQuarantine(Admin admin, String db) throws Exception {
        Schema s = Schema.newBuilder()
                .column("quarantine_id", DataTypes.STRING())
                .column("postback_event_id", DataTypes.STRING())
                .column("reason", DataTypes.STRING())
                .column("original_payload", DataTypes.BYTES())
                .column("payload_hash", DataTypes.STRING())
                .column("broker_order_id", DataTypes.STRING())
                .column("instruction_id", DataTypes.STRING())
                .column("correlation_attempt", DataTypes.STRING())
                .column("disposition", DataTypes.STRING())
                .column("disposition_reason", DataTypes.STRING())
                .column("quarantined_ts", DataTypes.BIGINT())
                .column("disposition_ts", DataTypes.BIGINT())
                .column("schema_version", DataTypes.STRING())
                .build();
        TableDescriptor td = TableDescriptor.builder()
                .schema(s)
                .distributedBy(BUCKETS, "quarantine_id")
                .build();
        admin.createTable(TablePath.of(db, "Postback_Quarantine"), td, false)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }
}
