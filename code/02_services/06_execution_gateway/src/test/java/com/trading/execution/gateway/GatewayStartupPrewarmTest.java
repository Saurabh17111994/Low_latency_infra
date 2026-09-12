package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * C1: the first write after table creation must not land on a user request.
 *
 * <p>It does today. Creating the projection tables opens a window in which the FIRST write
 * pays post-CREATE work: measured 2.2-3.7s under churn against the caller's 2s bound, versus
 * ~120ms in steady state and 112-251ms once that first write happens >=5s after CREATE. The
 * request path is the wrong place to pay it, and the retry that currently absorbs it
 * (BoundedRetry) is load-bearing on every cold table as a result.
 *
 * <p>This drives the canary's exact six-table DDL shape, runs the startup pre-warm, then times
 * the first REAL projection write through {@link FlussProjectionWriter} at the production 2s
 * bound. It fails before the pre-warm exists, which is the point: the assertion is the
 * production requirement (no request-path write exceeds the caller's bound), not a tautology.
 *
 * <p>Read-only by construction: pre-warm may not write probe rows into authoritative tables.
 */
@Tag("fluss")
class GatewayStartupPrewarmTest {

    private static final int ITERATIONS = 8;

    @Test
    @DisplayName("after startup pre-warm the first projection write fits the caller's 2s bound")
    void firstWriteAfterPrewarmStaysWithinRequestBudget() throws Exception {
        String bootstrap = System.getenv("FLUSS_BOOTSTRAP");
        Assumptions.assumeTrue(bootstrap != null && !bootstrap.isBlank(),
                "set FLUSS_BOOTSTRAP for live pre-warm evidence");

        List<Long> firstWriteMillis = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            String db = "prewarm_" + Long.toHexString(System.nanoTime()) + "_" + i;
            Connection conn = null;
            Admin admin = null;
            try {
                Configuration c = new Configuration();
                c.setString("bootstrap.servers", bootstrap);
                conn = ConnectionFactory.createConnection(c);
                admin = conn.getAdmin();
                admin.createDatabase(db, org.apache.fluss.metadata.DatabaseDescriptor.EMPTY, false)
                        .get(60, TimeUnit.SECONDS);
                FlussProjectionWriterIntegrationTest.createFills(admin, db);
                FlussProjectionWriterIntegrationTest.createOrderLifecycle(admin, db);
                FlussProjectionWriterIntegrationTest.createPositions(admin, db);
                FlussProjectionWriterIntegrationTest.createPositionState(admin, db);
                FlussProjectionWriterIntegrationTest.createOrderCorrelation(admin, db);
                FlussProjectionWriterIntegrationTest.createExecutionAudit(admin, db);

                GatewayConfig cfg = FlussProjectionWriterIntegrationTest.config(bootstrap, db);
                assertThat(cfg.requestTimeout().toMillis())
                        .as("this test is only meaningful at the production 2s bound")
                        .isEqualTo(2000L);

                // The step under test: everything a request would otherwise pay for.
                GatewayStartup.prewarmTables(cfg);

                try (FlussProjectionWriter writer = FlussProjectionWriter.open(cfg)) {
                    NormalizedExecutionEvent e =
                            FlussProjectionWriterIntegrationTest.event("pb-prewarm-" + i);
                    long t0 = System.nanoTime();
                    writer.writeAudit(e); // the request path's first write
                    long elapsed = (System.nanoTime() - t0) / 1_000_000L;
                    firstWriteMillis.add(elapsed);
                    System.out.printf("[prewarm-t1] iter%d first write after pre-warm = %dms%n",
                            i, elapsed);
                }
            } finally {
                if (admin != null) {
                    try {
                        admin.dropDatabase(db, false, true).get(60, TimeUnit.SECONDS);
                    } catch (Exception ignored) {
                        // scratch db, best effort
                    }
                }
                if (conn != null) {
                    conn.close();
                }
            }
        }

        List<Long> sorted = new ArrayList<>(firstWriteMillis);
        sorted.sort(null);
        long over = firstWriteMillis.stream().filter(v -> v >= 2000).count();
        System.out.printf("[prewarm-t1] first write after pre-warm: min=%dms p50=%dms max=%dms "
                        + "at-or-over-2s=%d/%d%n",
                sorted.get(0), sorted.get(sorted.size() / 2), sorted.get(sorted.size() - 1),
                over, firstWriteMillis.size());

        assertThat(over)
                .as("pre-warm must leave no request-path write paying the post-CREATE window; "
                        + "samples (ms): " + firstWriteMillis)
                .isZero();
    }

    /**
     * The check that would have stopped the A5 misdiagnosis. Needs no cluster: the kind is read off
     * the table handle, and a stub carrying a PRIMARY KEY stands in for the shape that matters - a
     * table whose deployed kind contradicts the list this code keeps.
     */
    @Test
    @DisplayName("a table of the wrong kind fails pre-warm by name and kind, not at the first write")
    void wrongTableKindFailsPrewarmWithTheTableNamed() {
        Schema primaryKeySchema = Schema.newBuilder()
                .column("id", DataTypes.STRING())
                .primaryKey("id")
                .build();
        TableInfo primaryKeyInfo = new TableInfo(
                TablePath.of("kind_check_db", "Fills"), 1L, 1, primaryKeySchema,
                List.of(), List.of(), 1, new Configuration(), new Configuration(), null, 0L, 0L);

        // Fills is the first APPEND_TABLES entry, so the contradiction is hit before any writer is
        // minted - which is why nothing here needs closing.
        FlussProjectionWriter writer = new FlussProjectionWriter(
                new FaultFlussStubs.FaultConnection(
                        new FaultFlussStubs.FaultTable(new FaultFlussStubs.FaultWriter(),
                                primaryKeyInfo)),
                FlussProjectionWriterIntegrationTest.config("localhost:9123", "kind_check_db"),
                Duration.ofMillis(250));

        assertThatThrownBy(writer::prewarm)
                .as("an operator must be told which table contradicts which call site")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Fills")
                .hasMessageContaining("PRIMARY KEY (KV)")
                .hasMessageContaining("newAppend()");
    }
}
