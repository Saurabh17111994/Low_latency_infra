package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
 * Design-B checkpoint/restore + rescale validation (2026-08-16; rules C + D of
 * the state-authoritative dedup change): the dedup set is authoritative Flink
 * keyed state, checkpointed atomically with the source offset, so (a) a strict
 * restore reconstructs the COMPLETE dedup set and a replay of already-accepted
 * fingerprints is never re-accepted, and (b) a rescale restore (1 → 2) cannot
 * split identical keys across subtasks — keyBy(instrument_token) + Flink
 * key-group redistribution keep every record of a token on exactly one
 * subtask at any parallelism.
 *
 * <p>This REPLACES the DEC-038 SIG-STATE-001 compact-checkpoint test: under
 * the old bounded-cache design the checkpoint had to stay flat across dedup
 * cardinality (the Fluss table was authoritative); under design B the
 * checkpoint carries the full live set by construction, so the measured bytes
 * are evidence that the state IS checkpointed (job B = 5x the live set of job
 * A must produce a larger checkpoint), and the restore phase is the
 * functional proof that the checkpointed set is complete and correct.
 *
 * <p><b>The post-checkpoint replay window.</b> Records accepted AFTER the last
 * completed checkpoint are replayed on restore; their entries are not in the
 * restored state, so they re-pass downstream. The window is bounded by the
 * checkpoint interval (10 s) and is inherent to at-least-once recovery — it
 * existed identically under DEC-038 (post-checkpoint first-seen rows had not
 * reached a completed Fluss checkpoint either). This test validates the
 * PRE-checkpoint window: every fingerprint whose entry was checkpointed is
 * swallowed on replay, never re-accepted.
 *
 * <p><b>Phases.</b> Job A (parallelism 2, 2,000 fingerprints) and job B
 * (parallelism 1, 10,000 fingerprints — the full live set on one subtask)
 * each emit, checkpoint once on top of the fully-built state, and are
 * cancelled with the checkpoint retained. Phase 3 restores job B's latest
 * checkpoint at 2x parallelism (1 → 2 rescale) and re-feeds all 10,000 + 2
 * new fingerprints: exactly the 2 NEW ones emit (a zero-state re-run would
 * emit all 10,002). Restore-submit → first-new-output duration must stay under
 * the 30 s checkpoint budget.
 *
 * <p>Gate: {@code @EnabledIfEnvironmentVariable(COMPUTE_INT_TEST_SIG_STATE_RESTORE=true)}
 * — skipped in the normal suite (MiniCluster). Host-runnable: embedded
 * MiniCluster + {@code file://} checkpoints — no external cluster, no Fluss,
 * no S3. Run:
 * {@code COMPUTE_INT_TEST_SIG_STATE_RESTORE=true mvn -o -f code/02_services/02_compute/pom.xml test -Dtest=SignalJobCompactCheckpointRestoreIntegrationTest}
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "COMPUTE_INT_TEST_SIG_STATE_RESTORE", matches = "true")
@DisplayName("Design B: checkpointed dedup set survives restore + 1->2 rescale; replay never re-accepts")
class SignalJobCompactCheckpointRestoreIntegrationTest {

    /** Collects main-output fingerprints (accepted first-seen), parallel-safe. */
    private static final List<String> EMITTED = Collections.synchronizedList(new ArrayList<>());

    private static final Duration POLL = Duration.ofMillis(250);
    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final int RUN_A_COUNT = 2_000;
    private static final int RUN_B_COUNT = 10_000;

    /**
     * Wall-clock base for event times: every re-fed row must still be inside
     * its 5-minute TTL at restore time, so the restored entries are still live
     * (expiry = first_seen + TTL, first_seen based on the real clock).
     */
    private static final long WALL_T0 = System.currentTimeMillis();

    /** Serializable row spec — GenericRowData is not serializable. */
    private record RowSpec(long token, long eventTime, String fingerprint)
            implements java.io.Serializable {}

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
    @DisplayName("checkpointed dedup set survives restore + 1->2 rescale; replay never re-accepts; < 30s")
    void checkpointedStateRestoresAndRescalesWithoutReacceptingReplay() throws Exception {
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

            System.out.println("DESIGN-B[checkpoint-size] S(2k)=" + sA
                    + " bytes, S(10k)=" + sB + " bytes, ratio=" + (sB / (double) sA));
            // Inverted DEC-038 invariant: the checkpoint MUST grow with the
            // live set — the full dedup set is checkpointed state now. A flat
            // checkpoint would mean the state is not being captured.
            assertTrue(sB > sA,
                    "checkpoint must carry the full live dedup set: 5x fingerprints "
                            + "(2,000 -> 10,000) must grow the checkpoint from " + sA
                            + " to more than " + sA + " bytes (got " + sB + ")");

            // ---- Phase 3: strict restore at 2x parallelism (1 -> 2 rescale) -
            String restore = latestCompletedCheckpoint(workDir.resolve("jobB"));
            assertFalse(restore == null, "latest jobB checkpoint to restore from");
            long restoreStart = System.nanoTime();
            runRestoreAndVerify(workDir, restore);
            long restoreMs = (System.nanoTime() - restoreStart) / 1_000_000L;
            System.out.println("DESIGN-B[restore] restore-to-first-new-output="
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
     * byte size.
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
            // State fully built FIRST (main output), then one completed
            // checkpoint ON TOP of it — the measured bytes are the checkpoint
            // that carries all `count` fingerprints' dedup state.
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
     * re-feed all 10,000 + 2 new. Every re-fed fingerprint whose entry was in
     * the checkpoint must be swallowed (never re-accepted); only the 2 new
     * pass. All rows share token 1 — the key-group redistribution across the
     * 1 -> 2 rescale must keep the whole set on exactly one subtask.
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
            JobClient job = submit(config, refeed, true, restorePath, 2);
            awaitTrue("restored job to pass exactly the 2 NEW fingerprints",
                    () -> EMITTED.size() == 2);
            List<String> emitted = new ArrayList<>(EMITTED);
            List<String> expected = rows("fp-new", 2, 1L, WALL_T0 + 20_000_000L).stream()
                    .map(RowSpec::fingerprint).sorted().collect(Collectors.toList());
            assertEquals(expected, emitted.stream().sorted().collect(Collectors.toList()),
                    "restored dedup state must swallow all 10,000 re-fed fingerprints "
                            + "(no full replay) and pass exactly the new ones — a zero-state "
                            + "re-run would have emitted all 10,002");
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
