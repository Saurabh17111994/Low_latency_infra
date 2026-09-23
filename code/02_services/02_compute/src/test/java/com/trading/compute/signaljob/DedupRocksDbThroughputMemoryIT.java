package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.StateBackendOptions;
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
 * Throughput + memory validation of the RETIRED MapState dedup design (DEC-040) on the RocksDB state backend
 * (2026-08-16; rules E/F/G of the state-authoritative dedup change). Runs the
 * real dedup sub-graph (source → keyBy(token) → {@link FingerprintDedupFunction}
 * → collecting sink) on an embedded MiniCluster with the production backend —
 * RocksDB, incremental checkpoints, pinned local dirs — and measures:
 *
 * <ul>
 *   <li><b>E — throughput:</b> one wave of 20,480 unique fingerprints (the P7
 *       envelope) — the operator must drain it at ≥ 20,480 rec/s (the source
 *       is NOT backpressured by dedup; the old serial-Fluss-lookup design
 *       sustained ~10 rec/s).</li>
 *   <li><b>F — checkpoints:</b> the 1 s-interval checkpoint completes on top
 *       of each wave — never an expiry, duration measured dir-created →
 *       _metadata, asserted < 30 s (the pinned budget).</li>
 *   <li><b>G — memory:</b> three progressive waves (50k / 200k / 500k live
 *       fingerprints, parallelism 1 — the full set on one subtask) report
 *       state count, checkpoint bytes, the RocksDB local-dir footprint
 *       (native-memory proxy) and the JVM heap delta. The TM runs in-process
 *       in a MiniCluster, so the heap figure is the JVM-wide delta around the
 *       phase, not a per-TM split; the RocksDB native footprint is the
 *       on-disk store size (block cache + memtables live inside Flink managed
 *       memory, which is bounded by the configured fraction).</li>
 * </ul>
 *
 * <p>Checkpoint contract: this test uses a 1 s interval so phases complete
 * fast — the ASSERTED budget (30 s per checkpoint) is the production pin; the
 * job itself still pins 10 s / 30 s (REQ-FC-006, unchanged by design B).
 *
 * <p>Gate: {@code @EnabledIfEnvironmentVariable(COMPUTE_INT_TEST_DEDUP_ROCKSDB=true)}
 * — skipped in the normal suite (MiniCluster). Run:
 * {@code COMPUTE_INT_TEST_DEDUP_ROCKSDB=true mvn -o -f code/02_services/02_compute/pom.xml test -Dtest=DedupRocksDbThroughputMemoryIT}
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "COMPUTE_INT_TEST_DEDUP_ROCKSDB", matches = "true")
@DisplayName("RETIRED MapState dedup: RocksDB throughput >= 20480 rec/s; checkpoints < 30s; progressive state/memory growth")
class DedupRocksDbThroughputMemoryIT {

    /** Collects main-output fingerprints (accepted first-seen), parallel-safe. */
    private static final List<String> EMITTED = Collections.synchronizedList(new ArrayList<>());

    private static final Duration POLL = Duration.ofMillis(200);
    private static final Duration TIMEOUT = Duration.ofSeconds(120);
    private static final long WALL_T0 = System.currentTimeMillis();
    private static final long TTL_MS = 60_000L;

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

    /** One wave of `count` unique fingerprints for one token, 1 ms apart. */
    private static List<RowSpec> rows(String prefix, int count, long token, long startEventTime) {
        List<RowSpec> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            out.add(new RowSpec(token, startEventTime + i, prefix + "-" + i));
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

    /** Total bytes under a path (recursive). */
    private static long bytesUnder(Path root) throws IOException {
        if (!Files.exists(root)) {
            return 0L;
        }
        final long[] total = {0L};
        Files.walkFileTree(root, new java.nio.file.SimpleFileVisitor<>() {
            @Override
            public java.nio.file.FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                total[0] += attrs.size();
                return java.nio.file.FileVisitResult.CONTINUE;
            }
        });
        return total[0];
    }

    /** Latest completed chk-N dir, or null. */
    private static Path latestCompletedCheckpoint(Path cpRoot) throws IOException {
        final Path[] latest = {null};
        if (!Files.exists(cpRoot)) {
            return null;
        }
        Files.walkFileTree(cpRoot, new java.nio.file.SimpleFileVisitor<>() {
            @Override
            public java.nio.file.FileVisitResult preVisitDirectory(Path dir,
                    BasicFileAttributes attrs) {
                String name = dir.getFileName().toString();
                if (name.startsWith("chk-") && Files.exists(dir.resolve("_metadata"))) {
                    if (latest[0] == null
                            || Long.parseLong(name.substring(4)) > Long.parseLong(
                                    latest[0].getFileName().toString().substring(4))) {
                        latest[0] = dir;
                    }
                    return java.nio.file.FileVisitResult.SKIP_SUBTREE;
                }
                return java.nio.file.FileVisitResult.CONTINUE;
            }
        });
        return latest[0];
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
    @DisplayName("RocksDB: >=20480 rec/s steady-state, checkpoints < 30s, progressive state/memory growth")
    void rocksDbThroughputCheckpointAndMemory() throws Exception {
        org.apache.logging.log4j.core.config.Configurator.setRootLevel(
                org.apache.logging.log4j.Level.WARN);
        Path workDir = Files.createTempDirectory("dedup-rocksdb-");
        Path rocksDir = workDir.resolve("rocks-local");
        Files.createDirectories(rocksDir);
        MiniClusterWithClientResource cluster = cluster(rocksDir, workDir);
        cluster.before();
        try {
            // ── E + F: steady-state throughput on a warm job, then a
            // completed checkpoint on top of the envelope state ───────────
            measureThroughput(cluster, workDir, rocksDir);

            // ── G: progressive memory waves (parallelism 1, one subtask) ──
            measureMemoryWave(cluster, workDir, "w50k", 50_000, rocksDir);
            measureMemoryWave(cluster, workDir, "w200k", 200_000, rocksDir);
            measureMemoryWave(cluster, workDir, "w500k", 500_000, rocksDir);
        } finally {
            cluster.after();
            deleteRecursively(workDir);
        }
    }

    /**
     * One job with a warm-up feed followed by the 20,480-row envelope wave.
     * The drain rate is measured over the ENVELOPE SEGMENT ONLY (from the
     * first envelope row emitted to the last) — job startup, RocksDB open,
     * and JIT warm-up are outside the measured window, so the number is the
     * operator's steady-state drain rate, not pipeline bootstrap. A completed
     * checkpoint on top of the full state validates rule F.
     */
    private void measureThroughput(MiniClusterWithClientResource cluster, Path workDir,
            Path rocksDir) throws Exception {
        int warmup = 5_000;
        int envelope = 20_480;
        Path sub = workDir.resolve("throughput");
        Files.createDirectories(sub);
        Configuration config = baseConfig(sub, rocksDir);
        EMITTED.clear();
        long rocksBefore = bytesUnder(rocksDir);

        List<RowSpec> feed = new ArrayList<>(rows("warm", warmup, 1L, WALL_T0 + 10_000_000L));
        feed.addAll(rows("env", envelope, 1L, WALL_T0 + 20_000_000L));
        JobClient job = submit(config, feed);

        // Start the clock when the first envelope row is emitted (steady state).
        awaitTrue("warm-up emitted", () -> EMITTED.size() >= warmup);
        long start = System.nanoTime();
        awaitTrue("envelope fully emitted", () -> EMITTED.size() >= warmup + envelope);
        long ms = (System.nanoTime() - start) / 1_000_000L;
        double recsPerSec = envelope / (ms / 1000.0);

        awaitCompletedCheckpoint(job, sub);
        long checkpointBytes = checkpointBytesAt(sub);
        long checkpointMs = lastCheckpointDurationMs(sub);
        long rocksBytes = bytesUnder(rocksDir) - rocksBefore;

        System.out.println("DEDUP-ROCKSDB[envelope-steady] rows=" + envelope
                + " steadyMs=" + ms + " recsPerSec=" + String.format("%.0f", recsPerSec)
                + " state=" + (warmup + envelope)
                + " checkpointBytes=" + checkpointBytes + " checkpointMs=" + checkpointMs
                + " rocksDirMiB=" + String.format("%.1f", rocksBytes / 1048576.0));

        assertTrue(recsPerSec >= 20_480,
                "dedup must sustain the 20,480 rec/s envelope at steady state: measured "
                        + String.format("%.0f", recsPerSec) + " rec/s");
        assertCheckpointWithinBudget(checkpointMs,
                "checkpoint on top of the envelope state");
        cancelAndWait(job);
    }

    /**
     * One fresh job per memory wave: feed `count` unique fingerprints, await
     * all emitted, await one completed checkpoint ON TOP of the state, report
     * checkpoint size + duration, the JVM heap delta, and the RocksDB local
     * dir footprint (native-memory proxy). No rate assertion — the drain rate
     * is proven by the throughput job.
     */
    private void measureMemoryWave(MiniClusterWithClientResource cluster, Path workDir,
            String label, int count, Path rocksDir) throws Exception {
        Path sub = workDir.resolve(label);
        Files.createDirectories(sub);
        Configuration config = baseConfig(sub, rocksDir);
        EMITTED.clear();
        long heapBefore = heapUsed();
        long rocksBefore = bytesUnder(rocksDir);

        JobClient job = submit(config, rows(label, count, 1L, WALL_T0 + 30_000_000L));
        awaitTrue(label + ": all " + count + " emitted", () -> EMITTED.size() == count);
        awaitCompletedCheckpoint(job, sub);
        long checkpointBytes = checkpointBytesAt(sub);
        long checkpointMs = lastCheckpointDurationMs(sub);

        long heapDelta = heapUsed() - heapBefore;
        long rocksBytes = bytesUnder(rocksDir) - rocksBefore;

        System.out.println("DEDUP-ROCKSDB[" + label + "] rows=" + count
                + " state=" + count
                + " checkpointBytes=" + checkpointBytes + " checkpointMs=" + checkpointMs
                + " heapDeltaMiB=" + String.format("%.1f", heapDelta / 1048576.0)
                + " rocksDirDeltaMiB=" + String.format("%.1f", rocksBytes / 1048576.0));

        assertEquals(count, EMITTED.size(), "every distinct fingerprint passes");
        assertCheckpointWithinBudget(checkpointMs, "checkpoint on top of the " + label + " state");
        cancelAndWait(job);
    }

    /**
     * The checkpoint COMPLETED (awaitCompletedCheckpoint) and the job stayed
     * RUNNING — with checkpoint-timeout 30 s an expiring checkpoint fails the
     * job, so completion already proves the budget. The dir-window duration is
     * best-effort evidence (a ~40 ms checkpoint can slip between polls); when
     * observed it must be < 30 s.
     */
    private static void assertCheckpointWithinBudget(long checkpointMs, String what) {
        if (checkpointMs == -1L) {
            System.out.println("DEDUP-ROCKSDB[note] " + what
                    + ": in-flight dir not caught by the 20 ms poll — duration unmeasured, "
                    + "completion + RUNNING already prove the 30 s budget");
            return;
        }
        assertTrue(checkpointMs < 30_000L,
                what + " must complete inside the 30 s budget: " + checkpointMs + " ms");
    }

    private static long heapUsed() {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }

    private static MiniClusterWithClientResource cluster(Path rocksDir, Path workDir) {
        Configuration config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
        config.setString("state.backend.rocksdb.localdir", rocksDir.toAbsolutePath().toString());
        // Bounded managed memory (RocksDB block cache + memtables) — in-process
        // TM, so this sizes the memory manager rather than a separate heap.
        config.setString("taskmanager.memory.process.size", "1024m");
        config.setString("taskmanager.memory.managed.fraction", "0.4");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file://" + workDir);
        return new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setConfiguration(config)
                        .setNumberSlotsPerTaskManager(2)
                        .setNumberTaskManagers(1)
                        .build());
    }

    private static Configuration baseConfig(Path cpRoot, Path rocksDir) {
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file://" + cpRoot);
        config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, "file://" + cpRoot);
        config.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1);
        config.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
        config.setString("state.backend.rocksdb.localdir", rocksDir.toAbsolutePath().toString());
        return config;
    }

    private static JobClient submit(Configuration config, List<RowSpec> feed) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1); // the full live set on one subtask — worst case
        env.enableCheckpointing(1_000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30_000L);
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        env.addSource(new EmitOnceThenPark(feed)).uid("src")
                .map(DedupRocksDbThroughputMemoryIT::toRow).uid("map")
                .keyBy(row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN), Types.LONG)
                .process(new FingerprintDedupFunction(SignalJobConfig.from(env())))
                .uid("dedup")
                .sinkTo(new CollectingSink()).uid("out");
        return env.executeAsync();
    }

    private static void awaitCompletedCheckpoint(JobClient job, Path cpRoot) throws Exception {
        long deadline = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < deadline) {
            JobStatus status = job.getJobStatus().get(10, TimeUnit.SECONDS);
            if (status == JobStatus.FAILED || status == JobStatus.CANCELED) {
                fail("job " + status + " before a completed checkpoint — see logs");
            }
            if (latestCompletedCheckpoint(cpRoot) != null) {
                return;
            }
            Thread.sleep(500);
        }
        fail("timed out waiting for a completed checkpoint under " + cpRoot);
    }

    /**
     * Full footprint of the latest checkpoint: with incremental RocksDB, the
     * per-task state files live in the chk-N dir while the .sst state handles
     * live in the job root's shared/ dir — so the walk covers the whole job
     * root (chk-N's parent), exactly like the compact-checkpoint test.
     */
    private static long checkpointBytesAt(Path cpRoot) throws IOException {
        Path chk = latestCompletedCheckpoint(cpRoot);
        if (chk == null) {
            return 0L;
        }
        Path jobRoot = chk.getParent();
        while (jobRoot != null && !jobRoot.equals(cpRoot) && !jobRoot.getParent().equals(cpRoot)) {
            jobRoot = jobRoot.getParent();
        }
        return bytesUnder(jobRoot);
    }

    /** Dir-created → _metadata-complete wall time of the latest checkpoint. */
    private static long lastCheckpointDurationMs(Path cpRoot) throws Exception {
        // Poll for a dir without _metadata (in-flight), then time until
        // _metadata appears — a bounded proxy for checkpoint duration.
        long deadline = System.currentTimeMillis() + 10_000;
        Path inFlight = null;
        while (System.currentTimeMillis() < deadline) {
            for (Path p : listCheckpointDirs(cpRoot)) {
                if (!Files.exists(p.resolve("_metadata"))) {
                    inFlight = p;
                    break;
                }
            }
            if (inFlight != null) {
                break;
            }
            Thread.sleep(20);
        }
        if (inFlight == null) {
            return -1L; // no in-flight checkpoint observed — cannot measure
        }
        long start = System.nanoTime();
        while (System.currentTimeMillis() < deadline) {
            if (Files.exists(inFlight.resolve("_metadata"))) {
                return (System.nanoTime() - start) / 1_000_000L;
            }
            Thread.sleep(20);
        }
        return -1L;
    }

    private static List<Path> listCheckpointDirs(Path cpRoot) throws IOException {
        List<Path> out = new ArrayList<>();
        if (!Files.exists(cpRoot)) {
            return out;
        }
        try (java.util.stream.Stream<Path> walk = Files.walk(cpRoot, 6)) {
            walk.filter(p -> p.getFileName().toString().startsWith("chk-")).forEach(out::add);
        }
        return out;
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
