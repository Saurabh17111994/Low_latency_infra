package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.trading.ingestion.model.TickPacket;
import com.trading.ingestion.write.FlussRowConverter;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client-capacity probe (throughput build plan, Phase 1): how fast can the
 * Fluss client alone ingest rows when submissions do NOT block on acks?
 *
 * <p>Env-gated with {@code INGESTION_INT_TEST_PERF=true}. Requires a live
 * Fluss cluster ({@code FLUSS_BOOTSTRAP_SERVERS}, default {@code localhost:9123})
 * with the offline-DDL-gated {@code default.raw_table_1} present.
 *
 * <p>This exercises the exact production append path
 * ({@link FlussClientAdapter} → {@link RealFlussRowConverter}) but submits
 * the bench worst case (1024 instruments × 20 ticks/s = 20,480 rows) without
 * waiting per row. The current production code waits on each ack
 * (~105ms = 100ms client linger + ~5ms RPC), which caps throughput at ~10/s.
 * The probe answers: what is the client/server capacity when the per-row
 * wait is removed? Output feeds the Phase 1 stop/go gate for the async-append
 * redesign.
 *
 * <p>Not part of any default gate — run explicitly:
 * {@code mvn -o test -pl 02_services/01_ingestion -Dtest=FlussThroughputProbeTest
 * -DINGESTION_INT_TEST_PERF=true}
 */
@DisplayName("THR-PROBE-001: client capacity without per-row blocking")
class FlussThroughputProbeTest {

    private static final Logger LOG = LoggerFactory.getLogger(FlussThroughputProbeTest.class);

    /** Bench worst case: 1024 instruments × 20 ticks/s. */
    private static final int ROW_COUNT = 20_480;

    @Test
    @DisplayName("submit 20,480 rows non-blocking; report rows/s, avg, p50, p99")
    void probeClientCapacity() throws Exception {
        assumeTrue("true".equalsIgnoreCase(
                System.getenv().getOrDefault("INGESTION_INT_TEST_PERF", "false")),
                "Skipping — set INGESTION_INT_TEST_PERF=true");

        String bootstrap = System.getenv().getOrDefault(
                "FLUSS_BOOTSTRAP_SERVERS", "localhost:9123");

        FlussRowConverter converter;
        try {
            converter = FlussClientAdapter.connect(bootstrap, "default.raw_table_1", 1);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Fluss not reachable at " + bootstrap
                            + " — start the docker stack before running the probe", e);
        }

        try {
            long submitStart = System.nanoTime();
            long[] latenciesNs = new long[ROW_COUNT];
            CompletableFuture<?>[] futures = new CompletableFuture<?>[ROW_COUNT];
            AtomicInteger failed = new AtomicInteger(0);

            for (int i = 0; i < ROW_COUNT; i++) {
                final int idx = i;
                TickPacket packet = TickPacketFixtures.validTrade(i);
                long submittedAt = System.nanoTime();
                // handle(): swallow completion so allOf() only waits — failures
                // are counted and reported, not thrown mid-probe.
                futures[idx] = converter.append(packet).handle((r, ex) -> {
                    if (ex != null) {
                        failed.incrementAndGet();
                        return null;
                    }
                    latenciesNs[idx] = System.nanoTime() - submittedAt;
                    return null;
                });
            }
            long submitEnd = System.nanoTime();

            CompletableFuture.allOf(futures).get(60, TimeUnit.SECONDS);
            long doneAt = System.nanoTime();

            int failedCount = failed.get();
            assertTrue(failedCount == 0,
                    "all rows must append; failures=" + failedCount);

            long[] sorted = latenciesNs.clone();
            Arrays.sort(sorted);

            double rowsPerSec = (double) ROW_COUNT
                    / ((doneAt - submitStart) / 1_000_000_000.0);
            double avgMs = average(sorted) / 1_000_000.0;
            double p50Ms = sorted[ROW_COUNT / 2] / 1_000_000.0;
            double p99Ms = sorted[(int) (ROW_COUNT * 0.99)] / 1_000_000.0;

            String summary = String.format(
                    "probe: rows=%d submit_ms=%d total_ms=%d rows_s=%.0f avg_ms=%.3f p50_ms=%.3f p99_ms=%.3f",
                    ROW_COUNT,
                    (submitEnd - submitStart) / 1_000_000,
                    (doneAt - submitStart) / 1_000_000,
                    rowsPerSec, avgMs, p50Ms, p99Ms);
            // LOG may be dropped in the test JVM (broken JSON_FILE appender) —
            // echo to stdout so surefire always captures the numbers.
            System.out.println(summary);
            LOG.info(summary);

            // Pathological guard only — the stop/go decision (>=15k rows/s
            // expected) is made by the operator from the printed numbers.
            assertTrue(rowsPerSec >= 5_000,
                    "capacity must be >= 5k rows/s; got " + String.format("%.0f", rowsPerSec));
        } finally {
            converter.close();
        }
    }

    private static double average(long[] ns) {
        long sum = 0;
        for (long v : ns) {
            sum += v;
        }
        return (double) sum / ns.length;
    }
}
