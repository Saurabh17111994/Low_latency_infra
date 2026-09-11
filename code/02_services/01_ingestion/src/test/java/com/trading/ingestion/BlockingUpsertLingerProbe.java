package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * D1 follow-up probe: <b>blocking, serialized</b> upsert latency vs {@code client.writer.batch-timeout}
 * (linger), on a live cluster.
 *
 * <p>Why this exists separately from {@link BatchLingerSweepProbe}: that probe submits 20,480 appends
 * non-blocking and then awaits them all, so the accumulator deque fills and batches are sent when
 * {@code full = dequeSize > 1}. That measures the <i>concurrent/batched</i> regime — but every gateway
 * store that D1 touched is <b>serialized and awaits each ack</b> (the gate/attempt stores are
 * {@code synchronized}; intent-dedup commits come from the single intent-reader thread; projection
 * writes run on a serial HTTP handler). With one writer and no overlap the deque never exceeds one
 * entry, {@code full} is never true, and {@code RecordAccumulator.batchReady()} sends only once
 * {@code waitedTimeMs >= batchTimeoutMs} — so the linger is expected to be <b>purely latency-additive</b>
 * here, with no batching benefit to offset it.
 *
 * <p>It also settles P4-134, which asserts that {@code get()} <i>before</i> {@code flush()} "risks
 * hanging to timeout". Variant A awaits the ack with no explicit flush (the post-D1 shape); variant B
 * flushes before awaiting (the pre-D1 shape). If P4-134 is right, variant A times out; if the sender's
 * polling loop is what releases the ack, the two are equivalent and the pre-D1 flush only added work.
 *
 * <p>Env-gated exactly like the other probes: {@code INGESTION_INT_TEST_PERF=true},
 * {@code FLUSS_BOOTSTRAP_SERVERS}. Uses a uniquely-named scratch KV table and drops it afterwards, so
 * no production table is touched. Not part of any default gate.
 */
@DisplayName("D1 probe: blocking serialized upsert latency vs linger")
class BlockingUpsertLingerProbe {

    private static final Logger LOG = LoggerFactory.getLogger(BlockingUpsertLingerProbe.class);
    private static final int WRITES_PER_RUN = 200;
    private static final Duration ACK_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration ADMIN_TIMEOUT = Duration.ofSeconds(30);
    /** Includes 0 to exercise the no-linger poll behaviour, and 100 = the Fluss default. */
    private static final int[] LINGERS_MS = {0, 1, 5, 20, 100};

    @Test
    @DisplayName("per-write latency for serialized blocking upserts, with and without pre-ack flush")
    void serializedBlockingUpserts() throws Exception {
        assumePerfEnv();
        String bootstrap = System.getenv().getOrDefault("FLUSS_BOOTSTRAP_SERVERS", "127.0.0.1:9123");
        String scratch = "gateway_probe_" + System.nanoTime();

        Configuration adminConf = new Configuration();
        adminConf.setString("bootstrap.servers", bootstrap);

        try (Connection adminConn = ConnectionFactory.createConnection(adminConf)) {
            Admin admin = adminConn.getAdmin();
            TablePath path = TablePath.of("default", scratch);
            Schema schema = Schema.newBuilder()
                    .column("probe_key", DataTypes.STRING())
                    .column("probe_value", DataTypes.BIGINT())
                    .primaryKey("probe_key")
                    .build();
            TableDescriptor td = TableDescriptor.builder()
                    .schema(schema)
                    .distributedBy(1, "probe_key")
                    .build();
            admin.createTable(path, td, false).get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            try {
                System.out.println("=== blocking_serialized_probe bootstrap=" + bootstrap
                        + " writes=" + WRITES_PER_RUN + " table=" + scratch);
                for (int lingerMs : LINGERS_MS) {
                    runVariant(bootstrap, scratch, lingerMs, false);
                    runVariant(bootstrap, scratch, lingerMs, true);
                }
                assertTrue(true, "probe complete — see stdout for numbers");
            } finally {
                admin.dropTable(path, false).get(ADMIN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * One measuring pass: {@value #WRITES_PER_RUN} strictly sequential writes, each awaited before the
     * next. {@code flushBeforeAck} selects the pre-D1 shape (flush() then get()) over the post-D1 shape
     * (get() only).
     */
    private void runVariant(String bootstrap, String tableName, int lingerMs, boolean flushBeforeAck)
            throws Exception {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        conf.setString("client.writer.batch-timeout", lingerMs + "ms");
        conf.setString("client.writer.buffer.wait-timeout", "30s");

        long[] latenciesNs = new long[WRITES_PER_RUN];
        int failures = 0;

        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            Table table = conn.getTable(TablePath.of("default", tableName));
            UpsertWriter writer = table.newUpsert().createWriter();

            for (int i = 0; i < WRITES_PER_RUN; i++) {
                GenericRow row = GenericRow.of(
                        BinaryString.fromString("k-" + lingerMs + "-" + flushBeforeAck + "-" + i),
                        (long) i);
                long startNs = System.nanoTime();
                try {
                    if (flushBeforeAck) {
                        // Pre-D1 shape: submit, flush() to make the batch sendable, then await the
                        // SAME future (one write, not two).
                        java.util.concurrent.CompletableFuture<?> f = writer.upsert(row);
                        writer.flush();
                        f.get(ACK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    } else {
                        // Post-D1 shape: no flush — the sender's poll loop must release the ack.
                        writer.upsert(row).get(ACK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    }
                    latenciesNs[i] = System.nanoTime() - startNs;
                } catch (Exception e) {
                    failures++;
                    latenciesNs[i] = ACK_TIMEOUT.toNanos();
                }
            }

            long[] sorted = latenciesNs.clone();
            Arrays.sort(sorted);
            String line = String.format(
                    "linger_ms=%-4d flush_before_ack=%-5s avg_ms=%.2f p50_ms=%.2f p99_ms=%.2f "
                            + "max_ms=%.2f failures=%d",
                    lingerMs, flushBeforeAck, avg(sorted) / 1_000_000.0,
                    sorted[WRITES_PER_RUN / 2] / 1_000_000.0,
                    sorted[(int) (WRITES_PER_RUN * 0.99)] / 1_000_000.0,
                    sorted[WRITES_PER_RUN - 1] / 1_000_000.0, failures);
            System.out.println(line);
            LOG.info(line);
        }
    }

    private static double avg(long[] ns) {
        long sum = 0;
        for (long v : ns) {
            sum += v;
        }
        return (double) sum / ns.length;
    }

    private static void assumePerfEnv() {
        assumeTrue("true".equalsIgnoreCase(
                        System.getenv().getOrDefault("INGESTION_INT_TEST_PERF", "false")),
                "Skipping — set INGESTION_INT_TEST_PERF=true");
    }
}
