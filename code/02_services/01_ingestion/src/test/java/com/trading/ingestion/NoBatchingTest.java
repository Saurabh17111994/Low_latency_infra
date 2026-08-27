package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.trading.ingestion.model.TickPacket;
import com.trading.ingestion.write.AppendTracker;
import com.trading.ingestion.write.FlussRowConverter;
import com.trading.ingestion.write.RawTickWriter;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ING-INT-003: No application batching — one append per tick.
 *
 * <p>Set {@code INGESTION_INT_TEST_FLUSS=true} to run.
 */
@DisplayName("ING-INT-003: No Application Batching")
class NoBatchingTest {

    private static final Logger LOG = LoggerFactory.getLogger(NoBatchingTest.class);

    @Test
    @DisplayName("1000 ticks → 1000 individual appends, no grouping")
    void noBatchingProof() throws Exception {
        assumeTrue("true".equalsIgnoreCase(
                System.getenv().getOrDefault("INGESTION_INT_TEST_FLUSS", "false")),
                "Skipping — set INGESTION_INT_TEST_FLUSS=true");

        String bootstrap = System.getenv().getOrDefault(
                "FLUSS_BOOTSTRAP_SERVERS", "localhost:9123");

        // Read-only schema verification (plan: runtime startup must verify,
        // never create/drop/alter). Tables must exist via the offline DDL gate.
        if (!DdlBootstrap.verifyTables(bootstrap)) {
            LOG.warn("Fluss schema not present at {} — skipping integration test "
                    + "(apply DDL via the offline `make ddl` gate first)", bootstrap);
            assumeTrue(false, "Fluss cluster not available at " + bootstrap);
            return;
        }

        FlussRowConverter converter;
        try {
            converter = FlussClientAdapter.connect(bootstrap, "default.raw_table_1", 1);
        } catch (Exception e) {
            LOG.warn("Fluss not reachable at {} — skipping integration test", bootstrap);
            assumeTrue(false, "Fluss cluster not available at " + bootstrap);
            return;
        }

        AppendTracker tracker = new AppendTracker();
        RawTickWriter writer = new RawTickWriter(
                converter,
                tracker,
                "default.raw_table_1",
                Duration.ofSeconds(5),
                Duration.ofSeconds(30));

        final int TICK_COUNT = 1000;
        for (int i = 0; i < TICK_COUNT; i++) {
            TickPacket packet = TickPacketFixtures.validTrade(i);
            RawTickWriter.AppendOutcome outcome = writer.write(packet);
            assertNotEquals(RawTickWriter.Status.FAILED, outcome.status());
            assertNotEquals(RawTickWriter.Status.FATAL, outcome.status());
        }

        // Phase 2: write() is async — wait for all acks before counting.
        writer.drain();

        LOG.info("no-batching: {} ticks → {} accepted",
                TICK_COUNT, writer.appendCount());
        assertEquals(TICK_COUNT, writer.appendCount(),
                "every tick individually appended");

        writer.close();
    }
}
