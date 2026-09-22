package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Checkpoint-bounded + strict-restore validation for the heap-window dedup
 * (`fingerprint-dedup-v2`, 2026-09-03): the per-token repeat windows are PLAIN
 * FIELDS on the operator, intentionally not checkpointed ("intentional
 * amnesia"), so (a) the checkpoint must stay bounded however many fingerprints
 * are fed - the operator requests no managed state at all - and (b) a strict
 * restore at a 1 -> 2 rescale must still submit and run, with every restored
 * window starting EMPTY.
 *
 * <p><b>Why the re-fed rows all come back out.</b> A restored job starts with
 * empty windows, so a deliberate full replay (this test) re-accepts every
 * fingerprint - that is the design, not a defect. Production safety comes from
 * checkpointed SOURCE OFFSETS: a restore resumes where the checkpoint left off,
 * so already-emitted ticks are never replayed, and SignalJobConfig fails closed
 * on a replay unless ALLOW_FULL_REPLAY=true (which this test sets to run the
 * synthetic re-feed). This file asserted the RETIRED Design-B contract - "the
 * checkpoint MUST grow with the live set" and "a replay is never re-accepted" -
 * until 2026-09-23, when the 2026-09-03 rewrite (`c0c50ee6`) was reconciled
 * here; it had been red since that rewrite.
 *
 * <p><b>Phases.</b> Job A (parallelism 2, 2,000 fingerprints) and job B
 * (parallelism 1, 10,000 fingerprints - the full fed set on one key) each
 * emit, checkpoint once, and are cancelled with the checkpoint retained.
 * Phase 3 restores job B's latest checkpoint at 2x parallelism (1 -> 2
 * rescale) and re-feeds all 10,000 + 2 new fingerprints: every re-fed
 * fingerprint is re-accepted (empty windows), and the job stays RUNNING.
 * Restore-submit -> first-output duration must stay under the 30 s budget.
 *
 * <p>Gate: {@code @EnabledIfEnvironmentVariable(COMPUTE_INT_TEST_SIG_STATE_RESTORE=true)}
 * - skipped in the normal suite (MiniCluster). Host-runnable: embedded
 * MiniCluster + {@code file://} checkpoints - no external cluster, no Fluss,
 * no S3. Run:
 * {@code COMPUTE_INT_TEST_SIG_STATE_RESTORE=true mvn -o -f code/02_services/02_compute/pom.xml test -Dtest=SignalJobCompactCheckpointRestoreIntegrationTest}
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "COMPUTE_INT_TEST_SIG_STATE_RESTORE", matches = "true")
@DisplayName("heap-window dedup: checkpoint stays bounded at 5x fed cardinality; strict 1->2 restore runs with empty windows")
class SignalJobCompactCheckpointRestoreIntegrationTest {

    /** Collects main-output fingerprints (accepted first-seen), parallel-safe. */
    private static final List<String> EMITTED = Collections.synchronizedList(new ArrayList<>());

    private static final Duration POLL = Duration.ofMillis(250);
    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final int RUN_A_COUNT = 2_000;
    private static final int RUN_B_COUNT = 10_000;

    /**
     * Wall-clock base for event times. The dedup window is a per-token COUNT
     * window (DEDUP_WINDOW_ENTRIES, no TTL), so event-time spacing carries no
     * expiry meaning here; the base just puts the 2 "new" rows after the
     * re-fed block.
     */
    private static final long WALL_T0 = System.currentTimeMillis();

    /** Serializable row spec — GenericRowData is not serializable. */
    private record RowSpec(long token, long eventTime, String fingerprint)
            implements java.io.Serializable {}

    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        // No dedup-TTL key: unread since the 2026-09-03 heap rewrite - the
        // window is a per-token COUNT bound (DEDUP_WINDOW_ENTRIES).
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        return env;
    }

    private static List<RowSpec> rows(String prefix, int count, long token, long startEventTime) {
        List<RowSpec> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            out.add(new RowSpec(token, startEventTime + i * 1_000L, prefix + "-" + i));
        }
        return out;
    }

    /** Emits rows once, then parks so the job stays RUNNING for checkpoints. */
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

    private static RowData toRow(RowSpec spec) {
        return TestRawRows.row(spec.token, spec.eventTime, spec.fingerprint, "TRADE", 100, 1);
    }

    /** Static collect sink (sink2 API) for the main output. */
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

    private static Configuration baseConfig(Path workDir) {
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file://" + workDir);
        config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, "file://" + workDir);
        // Retention 1: the job root at any moment holds ONE full checkpoint's
        // state (latest chk-N + its shared/taskowned files) — the whole-root
        // byte walk IS the latest checkpoint size.
        config.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1);
        config.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        return config;
    }

    private static MiniClusterWithClientResource cluster(Configuration config, int parallelism) {
        return new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setConfiguration(config)
                        .setNumberSlotsPerTaskManager(Math.max(4, parallelism))
                        .setNumberTaskManagers(1)
                        .build());
    }

    /**
     * The real dedup sub-graph: source → toRow → keyBy(token) → dedup → main
     * output sink. No side output, no writer, no store — design B has no write
     * path. Checkpoint storage + restore go through the declarative
     * Configuration (the same route SignalJob uses — Flink 2.2.1 removed
     * {@code CheckpointConfig.setCheckpointStorage}; restore reads
     * {@code StateRecoveryOptions.SAVEPOINT_PATH}). The SAME configuration is
     * the MiniCluster's (JobManager resolves checkpoint storage from its own
     * config) AND the env's — the FS walk must find the checkpoints under the
     * phase's checkpoint root.
     */
    private static JobClient submit(Configuration config, List<RowSpec> feed, boolean restore,
            String restorePath, int parallelism) throws Exception {
        if (restore) {
            config.set(StateRecoveryOptions.SAVEPOINT_PATH, restorePath);
        }
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(parallelism);
        env.enableCheckpointing(10_000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<RowData> deduped =
                env.addSource(new EmitOnceThenPark(feed)).uid("src")
                        .map(SignalJobCompactCheckpointRestoreIntegrationTest::toRow).uid("map")
                        .keyBy(row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN), Types.LONG)
                        .process(new FingerprintDedupFunction(SignalJobConfig.from(env())))
                        .uid("dedup");
        deduped.sinkTo(new CollectingSink()).uid("out");
        return env.executeAsync();
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

    /** Completed chk-N dirs under the checkpoint root (any depth), sorted by N. */
    private static List<Path> completedCheckpoints(Path cpRoot) throws IOException {
        List<Path> chks = new ArrayList<>();
        if (!Files.exists(cpRoot)) {
            return chks;
        }
        java.nio.file.Files.walkFileTree(cpRoot, new java.nio.file.SimpleFileVisitor<>() {
            @Override
            public java.nio.file.FileVisitResult preVisitDirectory(Path dir,
                    BasicFileAttributes attrs) {
                String name = dir.getFileName().toString();
                if (name.startsWith("chk-") && Files.exists(dir.resolve("_metadata"))) {
                    chks.add(dir);
                    return java.nio.file.FileVisitResult.SKIP_SUBTREE;
                }
                return java.nio.file.FileVisitResult.CONTINUE;
            }
        });
        chks.sort(java.util.Comparator.comparingLong(
                p -> Long.parseLong(p.getFileName().toString().substring(4))));
        return chks;
    }

    /** Latest completed checkpoint path (highest N), or null. */
    private static String latestCompletedCheckpoint(Path cpRoot) throws IOException {
        List<Path> chks = completedCheckpoints(cpRoot);
        return chks.isEmpty() ? null : chks.get(chks.size() - 1).toString();
    }

    /** Total bytes under a path (recursive). */
    private static long bytesUnder(Path root) throws IOException {
        if (!Files.exists(root)) {
            return 0L;
        }
        final long[] total = {0L};
        java.nio.file.Files.walkFileTree(root, new java.nio.file.SimpleFileVisitor<>() {
            @Override
            public java.nio.file.FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                total[0] += attrs.size();
                return java.nio.file.FileVisitResult.CONTINUE;
            }
        });
        return total[0];
    }

    /** The job root holding the latest checkpoint (parent chain up to cpRoot). */
    private static Path checkpointJobRoot(Path cpRoot, Path chk) {
        Path p = chk.getParent();
        while (p != null && !p.equals(cpRoot) && !p.getParent().equals(cpRoot)) {
            p = p.getParent();
        }
        return p;
    }

    /** Latest checkpoint's full state size (whole job root). */
    private static long latestCheckpointBytes(Path cpRoot) throws IOException {
        String latest = latestCompletedCheckpoint(cpRoot);
        assertFalse(latest == null, "no completed checkpoint under " + cpRoot);
        return bytesUnder(checkpointJobRoot(cpRoot, Path.of(latest)));
    }

    private static void awaitCompletedCheckpoints(JobClient job, Path cpRoot, int want)
            throws Exception {
        long deadline = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < deadline) {
            JobStatus status = job.getJobStatus().get(10, TimeUnit.SECONDS);
            if (status == JobStatus.FAILED || status == JobStatus.CANCELED) {
                fail("job " + status + " before " + want + " completed checkpoint(s) — see logs");
            }
            if (completedCheckpoints(cpRoot).size() >= want) {
                return;
            }
            Thread.sleep(1_000);
        }
        fail("timed out waiting for " + want + " completed checkpoint(s) under " + cpRoot);
    }

    private static void cancelAndWait(JobClient job) throws Exception {
        if (job.getJobStatus().get(10, TimeUnit.SECONDS) == JobStatus.RUNNING) {
            job.cancel().get(20, TimeUnit.SECONDS);
        }
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            if (job.getJobStatus().get(5, TimeUnit.SECONDS) == JobStatus.CANCELED) {
                return;
            }
            Thread.sleep(500);
        }
        fail("phase job did not reach CANCELED");
    }

    @Test
    @DisplayName("checkpoint stays bounded at 5x fed cardinality; strict 1->2 restore runs with empty windows; < 30s")
    void boundedCheckpointRestoresAndRescalesWithEmptyWindows() throws Exception {
        org.apache.logging.log4j.core.config.Configurator.setRootLevel(
                org.apache.logging.log4j.Level.INFO);
        Path workDir = Files.createTempDirectory("sig-state-001-");
        try {
            // ---- Job A: 2,000 fingerprints at parallelism 2 ----------------
            long sA = runSizedJob(workDir, "jobA", "fp-a", RUN_A_COUNT, 2);
            // ---- Job B: 10,000 fingerprints (5x) at parallelism 1 ---------
            // The FULL live set on a single subtask — the restore below at
            // parallelism 2 is a genuine 1 -> 2 rescale.
            long sB = runSizedJob(workDir, "jobB", "fp-b", RUN_B_COUNT, 1);

            System.out.println("HEAP-WINDOW[checkpoint-size] S(2k)=" + sA
                    + " bytes, S(10k)=" + sB + " bytes, ratio=" + (sB / (double) sA));
            // Bounded-checkpoint guard (the DEC-038-era shape, now for the
            // heap-window design): the windows are plain fields that are never
            // checkpointed, so 5x the fed cardinality must not grow the
            // checkpoint. Growth would mean repeat state crept back into
            // managed state - exactly the regression this file guards.
            assertTrue(sB <= sA * 1.5,
                    "checkpoint must not scale with the fed cardinality: the dedup windows "
                            + "are intentionally not checkpointed, so " + RUN_B_COUNT
                            + " fingerprints must keep the checkpoint near S(" + RUN_A_COUNT
                            + ")=" + sA + " bytes (+50% headroom), got " + sB + " bytes");

            // ---- Phase 3: strict restore at 2x parallelism (1 -> 2 rescale) -
            String restore = latestCompletedCheckpoint(workDir.resolve("jobB"));
            assertFalse(restore == null, "latest jobB checkpoint to restore from");
            long restoreStart = System.nanoTime();
            runRestoreAndVerify(workDir, restore);
            long restoreMs = (System.nanoTime() - restoreStart) / 1_000_000L;
            System.out.println("HEAP-WINDOW[restore] restore-to-first-new-output="
                    + restoreMs + " ms (budget 30000)");
            assertTrue(restoreMs < 30_000L,
                    "restore must resume inside the 30 s budget, took " + restoreMs + " ms");
        } finally {
            deleteRecursively(workDir);
        }
    }

    /**
     * Run one sized job (fresh MiniCluster): await all `count` first-seen
     * emitted + one completed checkpoint ON TOP of that state (retention 1
     * keeps exactly the latest chk-N on disk), then return the checkpoint's
     * byte size. Those bytes are the graph's offset/watermark skeleton
     * only - the dedup windows are never checkpointed.
     */
    private long runSizedJob(Path workDir, String runId, String fpPrefix, int count,
            int parallelism) throws Exception {
        Path sub = workDir.resolve(runId);
        Files.createDirectories(sub);
        Configuration config = baseConfig(sub);
        MiniClusterWithClientResource cluster = cluster(config, parallelism);
        cluster.before();
        try {
            EMITTED.clear();
            JobClient job = submit(config, rows(fpPrefix, count, 1L, WALL_T0), false, null,
                    parallelism);
            // Fed set fully emitted FIRST (main output), then one completed
            // checkpoint ON TOP of it - the measured bytes must NOT carry the
            // dedup set (plain fields, fingerprint-dedup-v2).
            awaitTrue(fpPrefix + ": all " + count + " first-seen emitted",
                    () -> EMITTED.size() == count);
            awaitCompletedCheckpoints(job, sub, 1);
            long bytes = latestCheckpointBytes(sub);
            cancelAndWait(job);
            return bytes;
        } finally {
            cluster.after();
        }
    }

    /**
     * Fresh MiniCluster, strict restore at 2x parallelism (job B ran at 1),
     * re-feed all 10,000 + 2 new. The restored windows are empty by design, so
     * every re-fed fingerprint is re-accepted: the assertions are that the
     * strict 1 -> 2 restore submits, the job keeps RUNNING, and the re-fed set
     * comes back out (all rows share token 1, so key-group redistribution must
     * keep the whole set on exactly one subtask).
     */
    private void runRestoreAndVerify(Path workDir, String restorePath) throws Exception {
        EMITTED.clear();
        Path sub = workDir.resolve("jobB-restore");
        Files.createDirectories(sub);
        Configuration config = baseConfig(sub);
        MiniClusterWithClientResource cluster = cluster(config, 4); // 2x parallelism
        cluster.before();
        try {
            List<RowSpec> refeed = new ArrayList<>(rows("fp-b", 10_000, 1L, WALL_T0));
            refeed.addAll(rows("fp-new", 2, 1L, WALL_T0 + 20_000_000L));
            java.util.Set<String> expectedFingerprints =
                    refeed.stream().map(RowSpec::fingerprint).collect(Collectors.toSet());
            JobClient job = submit(config, refeed, true, restorePath, 2);
            awaitTrue("restored job to re-emit every re-fed fingerprint",
                    () -> EMITTED.containsAll(expectedFingerprints));
            // Containment, not an exact count: the legacy test source runs one
            // copy per subtask while the per-token window holds 200 entries, so
            // a lagging copy can legitimately re-accept a row it already sent -
            // and that re-acceptance is exactly what empty restored windows do.
            assertTrue(EMITTED.containsAll(expectedFingerprints),
                    "every re-fed fingerprint must be re-accepted after a restore: the restored "
                            + "windows are empty by design (intentional amnesia); safety rests on "
                            + "checkpointed source offsets, not restored windows. saw "
                            + EMITTED.size() + " emissions for " + refeed.size() + " re-fed rows");
            assertTrue(job.getJobStatus().get(15, TimeUnit.SECONDS) == JobStatus.RUNNING,
                    "restored job must run (strict restore at 1 -> 2 did not fail)");
            cancelAndWait(job);
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
