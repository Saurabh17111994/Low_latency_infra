package com.trading.ingestion;

import com.trading.ingestion.config.IngestionConfig;
import com.trading.ingestion.write.AppendTracker;
import com.trading.ingestion.write.FlussRowConverter;
import com.trading.ingestion.write.RawTickWriter;
import com.trading.ingestion.model.TickPacket;
import java.time.Duration;

/**
 * Smoke test — validates ingestion → Fluss end-to-end.
 * Connects to localhost:9123, runs DDL bootstrap, appends 10 synthetic ticks.
 *
 * <p>Run:
 * <pre>{@code
 *   FLUSS_BOOTSTRAP=localhost:9123  ARROW_APP_ID=smoke  ARROW_APP_SECRET=sec  ARROW_TOKEN=tok  RAW_TABLE_NAME=raw_table_1
 *   java --add-opens=java.base/java.nio=ALL-UNNAMED -cp ingestion.jar:test-classes com.trading.ingestion.SmokeTest
 * }</pre>
 */
public final class SmokeTest {

    public static void main(String[] args) throws Exception {
        System.out.println("=== Ingestion Smoke Test ===");
        System.out.println("FLUSS=" + System.getenv("FLUSS_BOOTSTRAP"));
        System.out.println("JAVA=" + System.getProperty("java.version"));

        // Step 1: Validate config
        IngestionConfig config = IngestionConfig.validate();
        System.out.println("✓ config validated: " + config.rawTableName);

        // Step 2: Read-only schema verification (plan: runtime never mutates
        // DDL). Apply the offline `make ddl` gate first so all tables exist.
        boolean ddlOk = DdlBootstrap.verifyTables(config.flussBootstrap);
        if (!ddlOk) {
            System.err.println("✗ DDL verification failed — run the offline `make ddl` gate first, "
                    + "then confirm Fluss is reachable at " + config.flussBootstrap);
            System.exit(1);
        }
        System.out.println("✓ DDL verified (read-only)");

        // Step 3: Connect to Fluss
        FlussRowConverter converter = FlussClientAdapter.connect(
                config.flussBootstrap, config.rawTableName,
                config.flussWriterBatchTimeoutMs);
        System.out.println("✓ Fluss connected: " + config.rawTableName);

        // Step 4: Append 10 ticks
        AppendTracker tracker = new AppendTracker();
        RawTickWriter writer = new RawTickWriter(
                converter, tracker, config.rawTableName,
                Duration.ofSeconds(5), Duration.ofSeconds(30));

        for (int i = 0; i < 10; i++) {
            writer.write(TickPacketFixtures.validTrade(i));
        }
        writer.drain();
        int ok = (int) writer.appendCount();

        System.out.println("✓ appended " + ok + "/10 ticks");
        System.out.println("  total=" + writer.appendCount()
                + " err=" + writer.errorCount());
        writer.close();
        System.out.println("=== PASSED ===");
    }
}
