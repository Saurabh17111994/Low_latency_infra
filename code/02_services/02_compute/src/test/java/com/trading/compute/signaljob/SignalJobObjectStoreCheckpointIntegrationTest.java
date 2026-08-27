package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracker 14 P4.2 (docs/08_implementation/04-signal-job.md §Absorbed documents) —
 * durable checkpoints on a real S3-compatible object store (Cloudflare R2),
 * proven at RUNTIME through the SAME config path the job uses:
 * {@link SignalJobConfig#from} → {@link SignalJob#applyRuntimeOptions} (which
 * wires {@code fs.s3a.*} endpoint/credentials/region/path-style) → MiniCluster.
 *
 * <p>What this proves, with runtime artifacts (not config assertions):
 * <ul>
 *   <li>The P4.2 runtime wiring actually lands: {@code fs.s3a.endpoint} /
 *       {@code fs.s3a.access.key} / {@code fs.s3a.secret.key} /
 *       {@code fs.s3a.path.style.access} are present in the Flink
 *       Configuration the job runs with (the "runtime-log verification" the
 *       tracker demands — credentials come from env injection, never files).</li>
 *   <li>RocksDB checkpoints WRITE to R2: phase 1 completes ≥2 checkpoints on
 *       the {@code s3a://} URI, the latest holds {@code _metadata} plus
 *       incremental-RocksDB artifacts under {@code shared/} (files on the
 *       object store, not the local disk).</li>
 *   <li>State RESTORES from R2 on a fresh worker: phase 2 runs on a brand-new
 *       MiniCluster (new TaskManager, new RocksDB instances — zero local
 *       knowledge of phase 1) and resumes the phase-1 keyed counts from the S3
 *       checkpoint, proven by a state-origin tag: keys carry
 *       {@code firstRun=phase-1} (the value persisted to R2), NOT
 *       {@code phase-2} (which a zero-state re-run would write), and the
 *       resumed counts are 900 = 600 checkpointed + 300 new, never 300.</li>
 *   <li>Restore is strict: a failed restore fails the job (no
 *       {@code allowNonRestoredState}), so phase 2 reaching FINISHED is itself
 *       the negative proof.</li>
 * </ul>
 *
 * <p>Gate: {@code @EnabledIfEnvironmentVariable(COMPUTE_INT_TEST_P42=true)} —
 * skipped (no MiniCluster, no S3 access) in the normal suite. Run:
 * {@code mvn -o test -Dtest=SignalJobObjectStoreCheckpointIntegrationTest}
 * with the gate set and an existing writable R2 (or S3) bucket configured via
 * the same env the production launcher uses:
 * <pre>
 *   CHECKPOINT_DIR=s3://&lt;bucket&gt;/fluss-p4-it   (or s3a://)
 *   S3_ENDPOINT=https://&lt;account&gt;.r2.cloudflarestorage.com   (or R2_ENDPOINT)
 *   AWS_ACCESS_KEY_ID=…  AWS_SECRET_ACCESS_KEY=…
 *   AWS_REGION=auto (default)   S3_PATH_STYLE=true (default, R2)
 * </pre>
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "COMPUTE_INT_TEST_P42", matches = "true")
@DisplayName("tracker 14 P4.2: RocksDB checkpoints write to and restore from R2 (S3-compatible)")
class SignalJobObjectStoreCheckpointIntegrationTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(SignalJobObjectStoreCheckpointIntegrationTest.class);

    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    private static final int KEYS = 4;
    private static final long PHASE1_PER_KEY = 600;
    private static final long PHASE2_PER_KEY = 300;

    private static Configuration flinkConfig;
    private static String cpBase; // s3a://<bucket>/fluss-p4-it/<runId>
    private static java.nio.file.Path rocksDir;
    private static String runId;

    // ── gate + P4.2 runtime wiring ─────────────────────────────────────────

    @org.junit.jupiter.api.BeforeAll
    static void setUp() {
        // Flink's -tests.jar log4j2-test.properties pins rootLogger.level=OFF
        // ("to not flood build logs"); the P4.2 proof needs the cluster's real
        // failure output, so raise the level in THIS test's fork only. All
        // other tests keep the OFF default (their logging behavior is
        // untouched).
        org.apache.logging.log4j.core.config.Configurator.setRootLevel(
                org.apache.logging.log4j.Level.INFO);

        runId = "run-" + System.currentTimeMillis();
        String cp = System.getenv("CHECKPOINT_DIR");
        assumeTrue(cp != null && (cp.startsWith("s3://") || cp.startsWith("s3a://")),
                "Skipping — set CHECKPOINT_DIR to an s3:// or s3a:// URI to run the P4.2 test");
        assumeTrue(System.getenv("S3_ENDPOINT") != null || System.getenv("R2_ENDPOINT") != null,
                "Skipping — set S3_ENDPOINT (or R2_ENDPOINT) to the R2 jurisdiction URL");
        assumeTrue(System.getenv("AWS_ACCESS_KEY_ID") != null
                        && System.getenv("AWS_SECRET_ACCESS_KEY") != null,
                "Skipping — set AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY (injected, never committed)");

        // The REAL config path: SignalJobConfig validates (fail-closed on
        // object-store URIs without creds) and applyRuntimeOptions wires
        // fs.s3a.* into the Flink Configuration the cluster will run with.
        rocksDir = java.nio.file.Path.of(System.getProperty("java.io.tmpdir"), "rocks-p42-" + runId);
        Map<String, String> env = envFor(cp, null);
        SignalJobConfig config = SignalJobConfig.from(env);
        flinkConfig = new Configuration();
        SignalJob.applyRuntimeOptions(config, flinkConfig);
        // Retention: with the default (1), completing chk-2 immediately deletes
        // chk-1, racing this test's walk of the job root and destroying the
        // phase-2 restore target mid-read. Retain the checkpoint lineage for
        // the whole proof instead (also matches production retention settings).
        flinkConfig.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 10);
        // Manual restore after cancel: by default Flink DELETES completed
        // checkpoints when a job is cancelled (verified: chk-2 existed on R2
        // at phase-1 time, FileNotFound 11 s after the cancel). This is the
        // production pattern too — RETAIN_ON_CANCELLATION is what makes a
        // cancelled job's checkpoints restorable from the object store.
        flinkConfig.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                org.apache.flink.configuration.ExternalizedCheckpointRetention
                        .RETAIN_ON_CANCELLATION);

        // Runtime-log verification (tracker 14 P4.2): the wired keys are
        // present in the configuration the job actually runs with.
        assertNotNull(flinkConfig.getString("fs.s3a.endpoint", null), "fs.s3a.endpoint wired");
        assertNotNull(flinkConfig.getString("fs.s3a.access.key", null), "fs.s3a.access.key wired");
        assertNotNull(flinkConfig.getString("fs.s3a.secret.key", null), "fs.s3a.secret.key wired");
        assertEquals("true", flinkConfig.getString("fs.s3a.path.style.access", null),
                "path-style addressing wired (R2)");
        LOG.info("p4.2: object-store runtime config — endpoint={}, region={}, pathStyle={}, "
                        + "checkpoint dir class={}",
                flinkConfig.getString("fs.s3a.endpoint", null),
                flinkConfig.getString("fs.s3a.endpoint.region", null),
                flinkConfig.getString("fs.s3a.path.style.access", null),
                config.checkpointDir().substring(0, config.checkpointDir().indexOf(':')));

        cpBase = cp.replaceFirst("^s3://", "s3a://") + "/" + runId;
        flinkConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, cpBase + "/");
        // Global FileSystem config: Flink's FileSystem.get(URI) resolves fs.s3a.*
        // from the initialized global configuration (no per-call config overload).
        FileSystem.initialize(flinkConfig);
    }

    private static Map<String, String> envFor(String cp, String recovery) {
        Map<String, String> e = new HashMap<>();
        e.put("DEDUP_TTL_MS", "60000");
        e.put("CANDLE_WINDOW_MS", "15000");
        e.put("CHECKPOINT_INTERVAL_MS", "10000");
        e.put("CHECKPOINT_TIMEOUT_MS", "30000");
        e.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        e.put("DEPLOYMENT_ENV", "dev");
        e.put("STATE_BACKEND", "rocksdb");
        e.put("STATE_BACKEND_LOCAL_DIRS", rocksDir.toAbsolutePath().toString());
        e.put("CHECKPOINT_DIR", cp);
        e.put("S3_ENDPOINT", System.getenv("S3_ENDPOINT") != null
                ? System.getenv("S3_ENDPOINT") : System.getenv("R2_ENDPOINT"));
        e.put("AWS_ACCESS_KEY_ID", System.getenv("AWS_ACCESS_KEY_ID"));
        e.put("AWS_SECRET_ACCESS_KEY", System.getenv("AWS_SECRET_ACCESS_KEY"));
        e.put("PARALLELISM", "1");
        if (recovery == null) {
            e.put("ALLOW_FULL_REPLAY", "true");
        } else {
            e.put("STATE_RECOVERY_PATH", recovery);
        }
        return e;
    }

    @Test
    @DisplayName("RocksDB checkpoints on R2 restore keyed state on a fresh worker")
    void rocksDbCheckpointsWriteToAndRestoreFromR2() throws Exception {
        // ── Phase 1: RocksDB job on worker A, checkpoints to R2 ───────────
        String restore;
        MiniClusterWithClientResource clusterA = newMiniCluster();
        clusterA.before();
        try {
            JobClient job1 = startCountJob("phase-1", PHASE1_PER_KEY, clusterA);
            awaitCompletedCheckpoints(job1);
            // Live RocksDB store on the pinned fast-disk dirs while the job runs.
            assertRocksDbStore(rocksDir, "state.backend.rocksdb.localdir");
            restore = latestCompletedCheckpoint();
            assertS3CheckpointHoldsRocksDbArtifacts(restore);
            LOG.info("p4.2: phase 1 done — checkpoint {} holds RocksDB artifacts on R2", restore);
            if (job1.getJobStatus().get(10, TimeUnit.SECONDS) == JobStatus.RUNNING) {
                job1.cancel().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                awaitTrue(() -> safe(() -> job1.getJobStatus().get(5, TimeUnit.SECONDS)
                        == JobStatus.CANCELED), "phase-1 job CANCELED", 60);
            }
            assertCounts("phase-1", PHASE1_PER_KEY, "phase-1");
        } finally {
            clusterA.after(); // phase 2 must run on a FRESH worker
        }

        // ── Phase 2: restore from the R2 checkpoint on worker B ───────────
        MiniClusterWithClientResource clusterB = newMiniCluster();
        clusterB.before();
        try {
            Configuration restoreConfig = new Configuration();
            restoreConfig.addAll(flinkConfig);
            restoreConfig.set(StateRecoveryOptions.SAVEPOINT_PATH, restore);
            JobClient job2 = startCountJobWithConfig("phase-2", PHASE2_PER_KEY, clusterB,
                    restoreConfig);
            JobStatus status = awaitTerminalStatus(job2, 120);
            assertEquals(JobStatus.FINISHED, status,
                    "phase-2 must FINISH — a failed strict restore would FAIL the job, "
                            + "never fall back to a full replay");
            // 600 from the R2 checkpoint + 300 new; firstRun=phase-1 proves the
            // state came from S3, not a zero-state re-run (which would tag phase-2).
            assertCounts("phase-2", PHASE1_PER_KEY + PHASE2_PER_KEY, "phase-1");
            LOG.info("p4.2: phase 2 done — restored from {} on a fresh worker", restore);
        } finally {
            clusterB.after();
        }

        // ── cleanup: remove this run's unique prefix from the bucket ──────
        try {
            FileSystem fs = new Path(cpBase).getFileSystem();
            fs.delete(new Path(cpBase), true);
            LOG.info("p4.2: removed run prefix {} from the bucket", cpBase);
        } catch (Exception e) {
            LOG.warn("p4.2: could not remove {} — bucket lifecycle policy must clean it: {}",
                    cpBase, e.getMessage());
        }
    }

    private static MiniClusterWithClientResource newMiniCluster() {
        return new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setConfiguration(flinkConfig)
                        .setNumberSlotsPerTaskManager(2)
                        .setNumberTaskManagers(1)
                        .build());
    }

    // ── the job: bounded keyed counter with a state-origin tag ────────────

    private static JobClient startCountJob(String firstRun, long perKey,
            MiniClusterWithClientResource cluster) throws Exception {
        // Phase 1 MUST keep running: an idle-but-open source (a) keeps the
        // counting task's RocksDB store live for the store assertion, (b) makes
        // EVERY completed checkpoint carry the full per-key counts (restore can
        // assert exact 600+300), and (c) lets us cancel the job — a FINISHED
        // bounded job DELETES its completed checkpoints (verified on R2), a
        // CANCELLED one keeps them long enough for phase 2 (the same cleanup
        // race P4.3 relies on).
        return startCountJobWithConfig(firstRun, perKey, cluster, flinkConfig, true);
    }

    private static JobClient startCountJobWithConfig(String firstRun, long perKey,
            MiniClusterWithClientResource cluster, Configuration config) throws Exception {
        return startCountJobWithConfig(firstRun, perKey, cluster, config, false);
    }

    private static JobClient startCountJobWithConfig(String firstRun, long perKey,
            MiniClusterWithClientResource cluster, Configuration config, boolean keepAlive)
            throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);
        env.enableCheckpointing(1_000);
        if (keepAlive) {
            env.addSource(new BoundedThenIdleSource(KEYS * perKey), "p4.2-" + firstRun + "-src")
                    .keyBy(v -> v % KEYS)
                    .process(new KeyedCountFunction(firstRun))
                    .sinkTo(new CollectSink());
        } else {
            env
                    .fromSequence(0L, KEYS * perKey - 1)
                    .keyBy(v -> v % KEYS)
                    .process(new KeyedCountFunction(firstRun))
                    .sinkTo(new CollectSink());
        }
        JobClient client = env.executeAsync("p4.2-" + firstRun);
        JobStatus status = awaitJobStatus(client, 90);
        assertEquals(JobStatus.RUNNING, status, "job " + firstRun + " must reach RUNNING");
        LOG.info("p4.2: job {} RUNNING ({} elements/key, checkpoint dir s3a://…/{})",
                firstRun, perKey, runId);
        return client;
    }

    /** Per-key counter; the {@code firstRun} tag is written ONLY into fresh state. */
    private static final class KeyedCountFunction
            extends KeyedProcessFunction<Long, Long, String> {
        private final String firstRunTag;
        private transient ValueState<Long> count;
        private transient ValueState<String> firstRun;

        KeyedCountFunction(String firstRunTag) {
            this.firstRunTag = firstRunTag;
        }

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext ctx) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Types.LONG));
            firstRun = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("firstRun", Types.STRING));
        }

        @Override
        public void processElement(Long value, Context ctx, Collector<String> out)
                throws Exception {
            Thread.sleep(1); // pace the drain; the source idles after emitting so the job stays RUNNING
            Long c = count.value();
            if (c == null) {
                c = 0L;
                firstRun.update(firstRunTag); // only fresh state tags itself
            }
            c += 1;
            count.update(c);
            out.collect(ctx.getCurrentKey() + "|" + c + "|" + firstRun.value());
        }
    }

    /** Static collect sink (sink2 API — SinkFunction was removed in Flink 2.x). */
    private static final class CollectSink implements Sink<String> {
        static final List<String> RESULTS = Collections.synchronizedList(new ArrayList<>());

        @Override
        public SinkWriter<String> createWriter(WriterInitContext context) {
            return new SinkWriter<>() {
                @Override
                public void write(String element, Context context) {
                    RESULTS.add(element);
                }

                @Override
                public void flush(boolean endOfInput) {
                    // nothing to flush — results are collected in-process
                }

                @Override
                public void close() {
                    // nothing to release
                }
            };
        }
    }

    /**
     * Emits {@code 0..total-1} then idles until cancelled — a source that is
     * never exhausted, so phase 1's job stays RUNNING with its state fully
     * materialized. Checkpoint barriers flow to the idle source task normally
     * (it snapshots empty state), so every completed checkpoint carries the
     * full counts.
     */
    private static final class BoundedThenIdleSource implements SourceFunction<Long> {
        private static final long serialVersionUID = 1L;
        private final long total;
        private volatile boolean running = true;

        BoundedThenIdleSource(long total) {
            this.total = total;
        }

        @Override
        public void run(SourceContext<Long> ctx) {
            for (long i = 0; i < total && running; i++) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(i);
                }
            }
            while (running) {
                try {
                    Thread.sleep(1_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    running = false;
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static void assertCounts(String phase, long expectedMax, String expectedFirstRun) {
        Map<Integer, Long> maxByKey = new HashMap<>();
        Map<Integer, String> firstRunOfMax = new HashMap<>();
        for (String row : CollectSink.RESULTS) {
            String[] p = row.split("\\|");
            int key = Integer.parseInt(p[0]);
            long c = Long.parseLong(p[1]);
            if (c > maxByKey.getOrDefault(key, 0L)) {
                maxByKey.put(key, c);
                firstRunOfMax.put(key, p[2]);
            }
        }
        for (int k = 0; k < KEYS; k++) {
            assertEquals(expectedMax, maxByKey.getOrDefault(k, -1L),
                    phase + " key " + k + " final count");
            assertEquals(expectedFirstRun, firstRunOfMax.get(k),
                    phase + " key " + k + " state-origin tag");
        }
        CollectSink.RESULTS.clear();
    }

    // ── S3 polling + artifact assertions ───────────────────────────────────

    /**
     * Polls until ≥2 completed checkpoints exist on R2, failing FAST with the
     * real cause: a job FAILED (checkpoint write errors surface here) or an
     * S3A probe error (bad endpoint/creds/region) — never a silent 120 s wait.
     */
    private static void awaitCompletedCheckpoints(JobClient job) throws Exception {
        long deadline = System.currentTimeMillis() + 120_000;
        boolean loggedFsError = false;
        while (System.currentTimeMillis() < deadline) {
            JobStatus status = job.getJobStatus().get(10, TimeUnit.SECONDS);
            if (status == JobStatus.FAILED || status == JobStatus.CANCELED) {
                fail("phase-1 job " + status + " before 2 completed checkpoints — checkpoint "
                        + "write to R2 failed; see the job logs above for the cause");
            }
            long count;
            try {
                count = completedCheckpointCount();
            } catch (Exception e) {
                if (!loggedFsError) {
                    LOG.warn("p4.2: S3A probe error while polling {} — first failure:",
                            cpBase, e);
                    loggedFsError = true;
                }
                count = -1;
            }
            if (count >= 2) {
                return;
            }
            if (status == JobStatus.FINISHED) {
                fail("phase-1 job FINISHED with only " + Math.max(count, 0)
                        + " completed checkpoint(s) on R2 — the bounded source ended before "
                        + "2 checkpoints; increase PHASE1_PER_KEY, not a storage failure "
                        + "(completed checkpoints exist: " + Math.max(count, 0) + ")");
            }
            Thread.sleep(1_000);
        }
        fail("timed out waiting for phase-1 ≥2 completed checkpoints on R2 under " + cpBase);
    }

    private static long completedCheckpointCount() throws Exception {
        return checkpointDirs().length;
    }

    /**
     * Completed checkpoints live at {@code <cpBase>/<jobId>/chk-N/_metadata}
     * (verified layout on R2: one jobId dir per run, with chk-N plus shared/
     * and taskowned/ as siblings). The jobId level is one deeper than cpBase.
     */
    private static FileStatus[] checkpointDirs() throws Exception {
        FileSystem fs = new Path(cpBase).getFileSystem();
        Path base = new Path(cpBase);
        if (!fs.exists(base)) {
            return new FileStatus[0];
        }
        List<FileStatus> out = new ArrayList<>();
        for (FileStatus top : fs.listStatus(base)) {
            if (!top.isDir() || "shared".equals(top.getPath().getName())
                    || "taskowned".equals(top.getPath().getName())) {
                continue;
            }
            for (FileStatus d : fs.listStatus(top.getPath())) {
                String name = d.getPath().getName();
                if (d.isDir() && name.startsWith("chk-")
                        && fs.exists(new Path(d.getPath(), "_metadata"))) {
                    out.add(d);
                }
            }
        }
        return out.toArray(new FileStatus[0]);
    }

    /** Latest completed chk-N path (highest N), or null. */
    private static String latestCompletedCheckpoint() throws Exception {
        long best = -1;
        String bestPath = null;
        for (FileStatus d : checkpointDirs()) {
            long n = Long.parseLong(d.getPath().getName().substring(4));
            if (n > best) {
                best = n;
                bestPath = d.getPath().toString();
            }
        }
        assertTrue(bestPath != null, "no completed checkpoint under " + cpBase);
        return bestPath;
    }

    /**
     * The completed checkpoint on R2 must hold {@code _metadata} (inside
     * chk-N) plus incremental-RocksDB artifacts under {@code shared/} — state
     * files that live on the object store, not the local disk. {@code shared/}
     * is a sibling of chk-N at the job root, so the walk starts there.
     */
    private static void assertS3CheckpointHoldsRocksDbArtifacts(String chkPath)
            throws Exception {
        Path chk = new Path(chkPath);
        FileSystem fs = chk.getFileSystem();
        assertTrue(fs.exists(new Path(chk, "_metadata")), "checkpoint _metadata on R2");
        // job root = parent of chk-N (holds shared/ + taskowned/ too)
        Path jobRoot = chk.getParent();
        Deque<Path> queue = new ArrayDeque<>();
        queue.add(jobRoot);
        boolean sharedFile = false;
        while (!queue.isEmpty()) {
            Path dir = queue.poll();
            final FileStatus[] entries;
            try {
                entries = fs.listStatus(dir);
            } catch (java.io.FileNotFoundException e) {
                // a checkpoint (or shared-file refcount) was deleted between
                // the outer listing and this walk — skip, do not fail
                continue;
            }
            for (FileStatus f : entries) {
                String p = f.getPath().toString();
                if (p.contains("/shared/") && !f.isDir()) {
                    sharedFile = true;
                }
                if (f.isDir()) {
                    queue.add(f.getPath());
                }
            }
        }
        assertTrue(sharedFile,
                "checkpoint on R2 must hold incremental-RocksDB files under shared/ "
                        + "(heap-state checkpoints never do): " + chkPath);
    }

    /** RocksDB store in the configured local directory (same walk as P4.3). */
    private static void assertRocksDbStore(java.nio.file.Path dir, String what) throws Exception {
        assertTrue(Files.isDirectory(dir), what + " dir missing: " + dir);
        List<String> all = new ArrayList<>();
        Files.walkFileTree(dir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(java.nio.file.Path file,
                    java.nio.file.attribute.BasicFileAttributes attrs) {
                all.add(file.getFileName().toString());
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(java.nio.file.Path file, IOException exc) {
                return FileVisitResult.CONTINUE;
            }
        });
        boolean manifest = all.stream().anyMatch(n -> n.startsWith("MANIFEST-"));
        boolean options = all.stream().anyMatch(n -> n.startsWith("OPTIONS-"));
        boolean sstOrLog = all.contains("LOG") || all.stream().anyMatch(n -> n.endsWith(".sst"));
        if (!(all.contains("CURRENT") && (manifest || options) && sstOrLog)) {
            fail(what + "=" + dir + " must hold a live RocksDB store (CURRENT/MANIFEST/OPTIONS/"
                    + "LOG/.sst per operator subdir); found: " + all);
        }
    }

    // ── small helpers ──────────────────────────────────────────────────────

    private static JobStatus awaitJobStatus(JobClient client, int timeoutSeconds)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        JobStatus last = null;
        while (System.currentTimeMillis() < deadline) {
            last = client.getJobStatus().get(10, TimeUnit.SECONDS);
            if (last == JobStatus.RUNNING || last == JobStatus.FINISHED
                    || last == JobStatus.FAILED || last == JobStatus.CANCELED) {
                return last;
            }
            Thread.sleep(2_000);
        }
        return last;
    }

    /** Wait for a terminal status (FINISHED/FAILED/CANCELED) and return it. */
    private static JobStatus awaitTerminalStatus(JobClient client, int timeoutSeconds)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        JobStatus last = null;
        while (System.currentTimeMillis() < deadline) {
            last = client.getJobStatus().get(10, TimeUnit.SECONDS);
            if (last == JobStatus.FINISHED || last == JobStatus.FAILED
                    || last == JobStatus.CANCELED) {
                return last;
            }
            Thread.sleep(1_000);
        }
        return last;
    }

    private static void awaitTrue(BooleanSupplier cond, String what, long timeoutSeconds)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return;
            }
            Thread.sleep(1_000);
        }
        fail("timed out waiting for " + what);
    }

    private interface SafeSupplier<T> {
        T get() throws Exception;
    }

    private static boolean safe(SafeSupplier<Boolean> s) {
        try {
            return s.get();
        } catch (Exception e) {
            return false;
        }
    }
}
