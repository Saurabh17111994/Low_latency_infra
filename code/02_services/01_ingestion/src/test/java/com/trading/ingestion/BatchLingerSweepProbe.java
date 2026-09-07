package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.common.schema.EventDay;
import com.trading.ingestion.model.RawTick;
import com.trading.ingestion.model.TickPacket;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendResult;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * THR-PROBE-002: sweep Fluss client.writer.batch-timeout (linger) to resolve
 * batch-param Decision #2. Measures rows/s + p50/p99 for linger ∈ {0,1,5,10,20,50}ms
 * against a live cluster, single AppendWriter, non-blocking submission.
 *
 * <p>Env-gated: INGESTION_INT_TEST_PERF=true, FLUSS_BOOTSTRAP_SERVERS.
 * Not part of any default gate. Mirrors FlussClientAdapter's real append path
 * (AppendWriter + GenericRow) with a configurable linger.
 */
@DisplayName("THR-PROBE-002: batch-timeout (linger) sweep")
class BatchLingerSweepProbe {

    private static final Logger LOG = LoggerFactory.getLogger(BatchLingerSweepProbe.class);
    private static final int ROWS_PER_RUN = 20_480;

    @Test
    @DisplayName("sweep linger 0..50ms; report rows/s, p50, p99 per linger")
    void sweepLinger() throws Exception {
        assumePerfEnv();
        String bootstrap = System.getenv().getOrDefault("FLUSS_BOOTSTRAP_SERVERS", "127.0.0.1:9123");
        int[] lingersMs = {0, 1, 5, 10, 20, 50};

        System.out.println("=== linger_sweep bootstrap=" + bootstrap + " rows=" + ROWS_PER_RUN);
        for (int lingerMs : lingersMs) {
            runLinger(bootstrap, lingerMs);
        }
        assertTrue(true, "sweep complete — see stdout for numbers");
    }

    private void runLinger(String bootstrap, int lingerMs) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        conf.setString("client.writer.batch-timeout", lingerMs + "ms");
        conf.setString("client.writer.buffer.wait-timeout", "30s");

        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TablePath.of("default", "raw_table_1"));
        AppendWriter writer = table.newAppend().createWriter();

        long submitStart = System.nanoTime();
        long[] latenciesNs = new long[ROWS_PER_RUN];
        CompletableFuture<?>[] futures = new CompletableFuture<?>[ROWS_PER_RUN];
        AtomicInteger failed = new AtomicInteger(0);

        for (int i = 0; i < ROWS_PER_RUN; i++) {
            final int idx = i;
            TickPacket p = TickPacketFixtures.validTrade(i);
            long submittedAt = System.nanoTime();
            futures[idx] = writer.append(toRow(p)).handle((r, ex) -> {
                if (ex != null) { failed.incrementAndGet(); return null; }
                latenciesNs[idx] = System.nanoTime() - submittedAt;
                return null;
            });
        }
        long submitEnd = System.nanoTime();
        CompletableFuture.allOf(futures).get(60, TimeUnit.SECONDS);
        long doneAt = System.nanoTime();

        long[] sorted = latenciesNs.clone();
        Arrays.sort(sorted);
        double rowsPerSec = (double) ROWS_PER_RUN / ((doneAt - submitStart) / 1_000_000_000.0);
        double avgMs = avg(sorted) / 1_000_000.0;
        double p50Ms = sorted[ROWS_PER_RUN / 2] / 1_000_000.0;
        double p99Ms = sorted[(int) (ROWS_PER_RUN * 0.99)] / 1_000_000.0;

        String line = String.format(
                "linger_ms=%-3d rows_s=%.0f avg_ms=%.2f p50_ms=%.2f p99_ms=%.2f failed=%d",
                lingerMs, rowsPerSec, avgMs, p50Ms, p99Ms, failed.get());
        System.out.println(line);
        LOG.info(line);

        conn.close();
    }

    /** Row mirroring raw_table_1's 20 columns, using GenericRow.of exactly like production. */
    private static InternalRow toRow(TickPacket p) {
        RawTick raw = p.raw();
        return GenericRow.of(
                BinaryString.fromString(EventDay.of(p.eventTime())),   // event_day (partition key)
                BinaryString.fromString("fp_" + p.instrumentToken()),  // event_fingerprint
                BinaryString.fromString("1"),                          // fingerprint_version
                BinaryString.fromString(p.connectionId()),             // connection_id
                p.connectionEpoch(),                                   // connection_epoch
                p.instrumentToken(),                                   // instrument_token
                BinaryString.fromString(p.exchange()),                 // exchange
                BinaryString.fromString(p.tradingSymbol()),            // symbol
                p.eventTime().toEpochMilli(),                          // event_time
                p.ingestTs().toEpochMilli(),                           // ingest_ts
                0L,                                                    // ack_ts
                BinaryString.fromString("TRADE"),                      // tick_type
                p.lastPricePaise(),                                    // last_price_paise
                p.volume(),                                            // last_qty
                raw != null ? raw.rawPayload() : new byte[0],            // raw_payload BYTES (P1-087)
                BinaryString.fromString(raw != null ? raw.payloadHash() : ""), // payload_hash
                BinaryString.fromString("probe"),                      // decoder_version
                BinaryString.fromString("probe"),                      // protocol_version
                BinaryString.fromString("VALID_TRADE"),                // validity_state
                BinaryString.fromString(""),                           // validity_reason
                BinaryString.fromString("2")                           // schema_version
        );
    }

    private static void assumePerfEnv() {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                "true".equalsIgnoreCase(System.getenv().getOrDefault("INGESTION_INT_TEST_PERF", "false")),
                "Skipping — set INGESTION_INT_TEST_PERF=true");
    }

    private static double avg(long[] ns) {
        long sum = 0;
        for (long v : ns) sum += v;
        return (double) sum / ns.length;
    }
}
