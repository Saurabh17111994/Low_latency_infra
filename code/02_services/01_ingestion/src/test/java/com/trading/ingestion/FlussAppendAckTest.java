package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
 * ING-INT-002/003: Fluss append with real cluster.
 *
 * <p>Set {@code INGESTION_INT_TEST_FLUSS=true} to run.
 * Requires {@code FLUSS_BOOTSTRAP_SERVERS=localhost:9123}.
 */
@DisplayName("ING-INT-002: Fluss Append + Ack")
class FlussAppendAckTest {

    private static final Logger LOG = LoggerFactory.getLogger(FlussAppendAckTest.class);

    @Test
    @DisplayName("Append 100 ticks via real Fluss — all accepted, no uncertainty")
    void append100Ticks() throws Exception {
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
            LOG.warn("Fluss not reachable at {} — skipping integration test: {}",
                    bootstrap, e.getMessage());
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

        LOG.info("append-100: writing to {}", bootstrap);

        for (int i = 0; i < 100; i++) {
            TickPacket packet = TickPacketFixtures.validTrade(i);
            RawTickWriter.AppendOutcome outcome = writer.write(packet);
            assertNotNull(outcome, "outcome must not be null");
        }

        // Phase 2: write() is async — wait for all acks before counting.
        writer.drain();

        LOG.info("append-100: accepted={}", writer.appendCount());
        assertEquals(100, writer.appendCount(),
                "all ticks submitted");
        assertEquals(0, writer.uncertainCount(), "no uncertain outcomes");

        writer.close();
    }
}
