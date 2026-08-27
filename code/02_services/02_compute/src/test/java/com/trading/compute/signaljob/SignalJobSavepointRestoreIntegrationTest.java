package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Foundation L477 / L850 — Flink savepoint/restore/rescale capability evidence
 * (version matrix VM-FLINK-SRV-003, VM-FLINK-API-004): the SignalJob's real
 * keyed dedup operator survives a stop-with-savepoint → start-from-savepoint
 * cycle on a fresh cluster, at DIFFERENT parallelism, with strict restore
 * (no {@code allowNonRestoredState}).
 *
 * <p>What this proves, with runtime artifacts (not config assertions):
 * <ul>
 *   <li><b>Savepoint production</b> — phase 1 (parallelism 2) runs the actual
 *       {@link FingerprintDedupFunction} (MapState fingerprint-dedup with
 *       native StateTtlConfig expiry — CHG-023 item 2 removed the expiry
 *       index + event-time timers) over real {@link TestRawRows} rows;
 *       {@code stopWithSavepoint} returns a path on the local filesystem and
 *       the job reaches CANCELED with that savepoint on disk.</li>
 *   <li><b>State continuity across a job upgrade</b> — phase 2 runs on a brand
 *       new MiniCluster (fresh TaskManagers, zero phase-1 memory) restored
 *       strictly from the savepoint: re-feeding the SAME fingerprints emits
 *       NOTHING (the dedup MapState survived — a zero-state re-run would emit
 *       every duplicate), while new fingerprints pass.</li>
 *   <li><b>Rescale</b> — phase 2 restores at parallelism 4 (2× phase 1):
 *       keyed state redistributes across the wider set of subtasks and
 *       continuity still holds. Restore is strict: a failed restore fails the
 *       job, so phase 2 producing exactly the NEW fingerprints is the negative
 *       proof.</li>
 * </ul>
 *
 * <p>Gate: {@code @EnabledIfEnvironmentVariable(COMPUTE_INT_TEST_SAVEPOINT=true)}
 * — skipped in the normal suite. Run:
 * {@code COMPUTE_INT_TEST_SAVEPOINT=true mvn -o -f code/02_services/02_compute/pom.xml test -Dtest=SignalJobSavepointRestoreIntegrationTest}
 * (MiniCluster runs inside the test JVM — no external cluster, no Fluss, no S3).
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "COMPUTE_INT_TEST_SAVEPOINT", matches = "true")
@DisplayName("foundation L477/L850: dedup state survives stop-with-savepoint -> strict restore at 2x parallelism")
class SignalJobSavepointRestoreIntegrationTest {

    private static final Duration POLL = Duration.ofMillis(250);
    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    /** Serializable row spec — GenericRowData is NOT serializable, so the
     *  source carries primitives only and the mapper builds rows at runtime. */
    private record RowSpec(long token, long eventTime, String fingerprint)
            implements java.io.Serializable {}

    /** Phase-1 fingerprints: every one must be swallowed by phase-2 re-feeds. */
    private static final List<RowSpec> PHASE1_ROWS = List.of(
            new RowSpec(1L, 1_700_000_000_000L, "fp-p1-a"),
            new RowSpec(1L, 1_700_000_001_000L, "fp-p1-b"),
            new RowSpec(1L, 1_700_000_002_000L, "fp-p1-c"),
            new RowSpec(2L, 1_700_000_000_000L, "fp-p1-d"),
            new RowSpec(2L, 1_700_000_001_000L, "fp-p1-e"),
            new RowSpec(2L, 1_700_000_002_000L, "fp-p1-f"));

    /** Phase-2 NEW fingerprints: the only rows a restored job may emit. */
    private static final List<RowSpec> PHASE2_NEW_ROWS = List.of(
            new RowSpec(1L, 1_700_000_003_000L, "fp-p2-a"),
            new RowSpec(2L, 1_700_000_003_000L, "fp-p2-b"));

    /** Collects emitted fingerprints across the phase-2 sink (parallel-safe). */
    private static final List<String> EMITTED =
            Collections.synchronizedList(new ArrayList<>());

    /** Config env identical to the unit-test baseline (tuning keys default). */
    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_TTL_MS", "60000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        return env;
    }

    /** Emits rows once, then parks so the job stays RUNNING for stopWithSavepoint. */
    private static final class EmitOnceThenPark implements SourceFunction<RowSpec> {
        private static final long serialVersionUID = 1L;
        private final List<RowSpec> rows;
        private volatile boolean cancelled;

        EmitOnceThenPark(List<RowSpec> rows) {
            this.rows = rows;
        }

        @Override
        public void run(SourceContext<RowSpec> ctx) {
            for (RowSpec row : rows) {
                ctx.collectWithTimestamp(row, row.eventTime);
            }
            while (!cancelled) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
        }
    }

    /** Builds the real raw row at runtime — nothing non-serializable in the graph. */
    private static RowData toRow(RowSpec spec) {
        return TestRawRows.row(spec.token, spec.eventTime, spec.fingerprint, "TRADE", 100, 1);
    }

    /** Static collect sink (sink2 API — SinkFunction was removed in Flink 2.x). */
    private static final class CollectingSink implements Sink<RowData> {
        private static final long serialVersionUID = 1L;

        @Override
        public SinkWriter<RowData> createWriter(WriterInitContext context) {
            return new SinkWriter<>() {
                @Override
                public void write(RowData element, Context context) {
                    EMITTED.add(element.getString(RawTableColumns.EVENT_FINGERPRINT).toString());
                }

                @Override
                public void flush(boolean endOfInput) {
                    // results are collected in-process
                }

                @Override
                public void close() {
                    // nothing to release
                }
            };
        }
    }

    private static void awaitTrue(String what, BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(POLL.toMillis());
        }
        assertTrue(condition.getAsBoolean(), "timed out waiting for " + what);
    }

    private static Configuration baseConfig(Path workDir) {
        Configuration config = new Configuration();
        // Local filesystem savepoints/checkpoints — no object store needed.
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file://" + workDir);
        config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, "file://" + workDir);
        return config;
    }

    private static MiniClusterWithClientResource cluster(int parallelism) {
        return new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberSlotsPerTaskManager(Math.max(4, parallelism))
                        .setNumberTaskManagers(1)
                        .build());
    }

    private static JobClient submit(StreamExecutionEnvironment env) throws Exception {
        return env.executeAsync();
    }

    @Test
    void dedupStateSurvivesSavepointRestoreAtTwoTimesParallelism() throws Exception {
        org.apache.logging.log4j.core.config.Configurator.setRootLevel(
                org.apache.logging.log4j.Level.INFO);
        Path workDir = Files.createTempDirectory("savepoint-l850-");
        try {
            String savepointPath = phase1ProduceSavepoint(workDir);
            assertFalse(savepointPath.isEmpty(), "stopWithSavepoint must return a savepoint path");

            // Phase 2: fresh cluster, 2x parallelism, strict restore.
            phase2RestoreAndVerify(workDir, savepointPath, 4);
        } finally {
            deleteRecursively(workDir);
        }
    }

    /** Phase 1 (parallelism 2): run dedup over PHASE1_ROWS, then stopWithSavepoint. */
    private String phase1ProduceSavepoint(Path workDir) throws Exception {
        MiniClusterWithClientResource cluster = cluster(2);
        cluster.before();
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(
                    baseConfig(workDir));
            env.setParallelism(2);
            env.addSource(new EmitOnceThenPark(PHASE1_ROWS)).uid("src")
                    .map(SignalJobSavepointRestoreIntegrationTest::toRow).uid("map")
                    .keyBy(row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN), Types.LONG)
                    .process(new FingerprintDedupFunction(SignalJobConfig.from(env())))
                    .uid("dedup")
                    .sinkTo(new CollectingSink());

            JobClient client = submit(env);
            // Wait until all 6 phase-1 fingerprints have been emitted (state built).
            awaitTrue("phase-1 fingerprints to pass dedup", () -> EMITTED.size() == 6);

            CompletableFuture<String> future = client.stopWithSavepoint(
                    false, "file://" + workDir,
                    org.apache.flink.core.execution.SavepointFormatType.CANONICAL);
            String savepoint = future.get(60, TimeUnit.SECONDS);
            assertTrue(savepoint != null && !savepoint.isEmpty(),
                    "stopWithSavepoint returned a real savepoint path: " + savepoint);
            return savepoint;
        } finally {
            cluster.after();
        }
    }

    /** Phase 2 (parallelism 4): strict restore from savepoint, re-feed dups + new rows. */
    private void phase2RestoreAndVerify(Path workDir, String savepointPath, int parallelism)
            throws Exception {
        EMITTED.clear();
        MiniClusterWithClientResource cluster = cluster(parallelism);
        cluster.before();
        try {
            Configuration config = baseConfig(workDir);
            // Strict restore: any graph/state mismatch fails the job.
            config.set(StateRecoveryOptions.SAVEPOINT_PATH, savepointPath);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
            env.setParallelism(parallelism);
            List<RowSpec> phase2Rows = new ArrayList<>(PHASE1_ROWS);
            phase2Rows.addAll(PHASE2_NEW_ROWS);
            env.addSource(new EmitOnceThenPark(phase2Rows)).uid("src")
                    .map(SignalJobSavepointRestoreIntegrationTest::toRow).uid("map")
                    .keyBy(row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN), Types.LONG)
                    .process(new FingerprintDedupFunction(SignalJobConfig.from(env())))
                    .uid("dedup")
                    .sinkTo(new CollectingSink());

            JobClient client = submit(env);
            awaitTrue("phase-2 NEW fingerprints to pass dedup", () -> EMITTED.size() == 2);

            // Continuity: exactly the two NEW fingerprints — a zero-state re-run
            // would have emitted all 8 (the 6 phase-1 duplicates included).
            List<String> emitted = new ArrayList<>(EMITTED);
            List<String> expected = PHASE2_NEW_ROWS.stream()
                    .map(RowSpec::fingerprint)
                    .sorted()
                    .collect(Collectors.toList());
            List<String> actual = emitted.stream().sorted().collect(Collectors.toList());
            assertEquals(expected, actual,
                    "restored dedup state must swallow every phase-1 fingerprint "
                            + "and pass exactly the new ones (rescale 2 -> " + parallelism + ")");

            // The job must still be RUNNING (no state-restore failure).
            assertTrue(client.getJobStatus().get(15, TimeUnit.SECONDS)
                            == org.apache.flink.api.common.JobStatus.RUNNING,
                    "restored job must run (strict restore did not fail)");
        } finally {
            cluster.after();
        }
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (java.util.stream.Stream<Path> walk = Files.walk(root)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    // best-effort temp cleanup
                }
            });
        }
    }
}
