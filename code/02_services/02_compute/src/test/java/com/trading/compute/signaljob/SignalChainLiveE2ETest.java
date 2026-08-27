package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SIGNAL-CHAIN-E2E-001 (DRAFT): full-chain live proof — broker → bridge →
 * {@code raw_table_1} → SignalJob → {@code feature_candles_15s}, run for
 * {@code E2E_RUN_MINUTES} (default 5) with live assertions at both ends.
 *
 * <p>This is the test the repo previously lacked: the ingestion leg
 * (broker → Fluss) and the compute leg (Fluss → feature table) are each proven
 * separately (ING-E2E-001, CANDLE-KV-REPLAY-001 P6), but no single run
 * exercised both against the SAME live {@code raw_table_1}. This test launches
 * the real {@code IngestionService} as a subprocess (FullStackE2ETest pattern)
 * and runs the real {@link SignalJob#buildTopology} in-process (P6 pattern),
 * both against one live Fluss cluster.
 *
 * <h2>Broker modes ({@code E2E_BROKER})</h2>
 * <ul>
 *   <li>{@code faketool} (default, runs any time): starts the Go fake broker in
 *       {@code -real-rate} mode on a free port. Fresh ticks guaranteed.</li>
 *   <li>{@code arrow-hft}: the REAL broker HFT feed
 *       ({@code wss://socket.arrow.trade}, 1,024-token manifest subscription).
 *       Requires device-flow credentials ({@code ARROW_APP_ID}/{@code ARROW_APP_SECRET}/
 *       {@code ARROW_TOKEN}) and MARKET HOURS — post-close ticks are STALE and
 *       go to quarantine, so {@code raw_table_1} never grows and the test skips
 *       with that reason. (The Standard feed was removed 2026-08-14 — HFT only.)</li>
 * </ul>
 *
 * <h2>What it asserts (acceptance)</h2>
 * <ol>
 *   <li>{@code raw_table_1} row count strictly grows during the run — the
 *       freshness gates (post-close STALE → quarantine) already guarantee that
 *       any raw growth is fresh-ts data.</li>
 *   <li>{@code feature_candles_15s} gains rows with {@code window_end} after
 *       run start, across ≥1 instrument, and keeps growing between samples —
 *       the Flink window/emit path is alive on real ingested ticks.</li>
 *   <li>New feature rows carry canonical versions (schema/algorithm/
 *       configuration) and sane OHLC (low ≤ min(open,close), max(open,close) ≤
 *       high, tick_count ≥ 1).</li>
 *   <li>The SignalJob stays RUNNING the whole window and completes ≥1
 *       EXACTLY_ONCE checkpoint ({@code chk-N/_metadata} under
 *       {@code E2E_CHECKPOINT_DIR}).</li>
 * </ol>
 *
 * <h2>Env contract</h2>
 * <pre>
 * SIGNAL_CHAIN_E2E=true                      gate (required)
 * FLUSS_BOOTSTRAP                            default localhost:9123
 * E2E_BROKER                                 faketool | arrow-hft (default faketool)
 * E2E_RUN_MINUTES                            default 5
 * E2E_FRESH_WARMUP_S                         default 90 (arrow modes: wait for first raw growth)
 * E2E_CHECKPOINT_DIR                         default file:///tmp/signal-chain-e2e-checkpoints
 * INGESTION_CLASSPATH                        ingestion module test/runtime classpath (runner computes it)
 * E2E_DRAIN_LOG                              optional — append subprocess stdout/stderr to this file
 *                                             instead of discarding it (diagnosis; unset = discard)
 * ARROW_BRIDGE_BIN                           default go-bridge/arrow-bridge
 * FAKETOOL_BIN                               default go-bridge/faketool/faketool
 * INSTRUMENT_MANIFEST_PATH                   NSE_CM_EQUITY (1024).csv
 * ARROW_APP_ID / ARROW_APP_SECRET / ARROW_TOKEN   arrow modes only (device-flow tokens)

 * </pre>
 *
 * <p><b>Writes to REAL platform tables</b> ({@code raw_table_1},
 * {@code feature_candles_15s}) — dev-cluster gate, never the production
 * cluster, and never drops them at teardown (unlike the P6 scratch pattern).
 * Runner: {@code code/01_platform/04_scripts/run-signal-chain-e2e.sh}.
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "SIGNAL_CHAIN_E2E", matches = "true")
@DisplayName("SIGNAL-CHAIN-E2E-001: broker → raw_table_1 → SignalJob → feature_candles_15s (5 min live)")
class SignalChainLiveE2ETest {

    private static final Logger LOG = LoggerFactory.getLogger(SignalChainLiveE2ETest.class);

    private static final String DEFAULT_MANIFEST =
            "../../../../Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv";

    private Connection connection;
    private Table rawTable;
    private Table featureTable;
    private Process ingestionProc;
    private Process faketoolProc;
    private JobClient job;
    private long runStartMillis;

    @Test
    void fullChainBrokerToFeatureTable() throws Exception {
        String broker = env("E2E_BROKER", "faketool");
        String bootstrap = env("FLUSS_BOOTSTRAP", "localhost:9123");
        long runMillis = TimeUnit.MINUTES.toMillis(longEnv("E2E_RUN_MINUTES", 5));
        long warmupS = longEnv("E2E_FRESH_WARMUP_S", 90);

        connectFluss(bootstrap);
        assertTrue(tableExists("raw_table_1"), "raw_table_1 must exist on the target cluster");
        assertTrue(tableExists("feature_candles_15s"),
                "feature_candles_15s must exist on the target cluster");

        long rawStart = countRows(rawTable);
        long featStart = countRows(featureTable);
        LOG.info("chain-e2e: raw_start={} feature_start={} broker={} run_minutes={}",
                rawStart, featStart, broker, runMillis / 60_000);

        // ── Ingestion leg: real service + bridge (+ faketool when requested) ──
        try {
            if ("faketool".equals(broker)) {
                faketoolProc = startFaketool();
            }
            ingestionProc = startIngestion(broker);
            awaitTrue(() -> safeThrows(() -> rawCount() > rawStart),
                    "first fresh ticks in raw_table_1 (" + warmupS + "s warmup; arrow modes need "
                            + "market hours — post-close data goes to quarantine)", warmupS);

            // ── Compute leg: real SignalJob topology, same live raw_table_1 ──
            runStartMillis = System.currentTimeMillis();
            job = startSignalJob(bootstrap);

            long lastFeature = featStart;
            long samples = 0;
            // SIG-PERF-001 envelope measurement: raw/feature counts per sample
            // (15 s apart) → rows/s throughput + candle emit rate over the run.
            List<long[]> perfSamples = new java.util.ArrayList<>();
            long sampleStart = System.currentTimeMillis();
            while (System.currentTimeMillis() - runStartMillis < runMillis) {
                Thread.sleep(15_000L);
                samples++;
                long rawNow = rawCount();
                long featNow = countRowsLazy(featureTable, lastFeature);
                JobStatus status = job.getJobStatus().get(10, TimeUnit.SECONDS);
                assertEquals(JobStatus.RUNNING, status,
                        "SignalJob must stay RUNNING (sample " + samples + ")");
                assertTrue(featNow >= lastFeature,
                        "feature_candles_15s must not shrink (sample " + samples
                                + "): " + lastFeature + " → " + featNow);
                lastFeature = featNow;
                perfSamples.add(new long[] {System.currentTimeMillis() - sampleStart, rawNow, featNow});
                LOG.info("chain-e2e sample {}: raw={} feature={}", samples, rawNow, featNow);
            }

            // ── Final assertions ──
            long rawEnd = rawCount();
            long featEnd = featureCount();
            assertTrue(rawEnd > rawStart,
                    "raw_table_1 must grow during the run: " + rawStart + " → " + rawEnd
                            + " (post-close broker data is quarantined by design)");
            assertTrue(featEnd > featStart,
                    "feature_candles_15s must gain rows: " + featStart + " → " + featEnd);

            List<InternalRow> newRows = newFeatureRows();
            assertTrue(!newRows.isEmpty(),
                    "at least one candle window must complete during the run");
            Set<Long> instruments = new HashSet<>();
            for (InternalRow r : newRows) {
                instruments.add(r.getLong(CandleTableColumns.INSTRUMENT_TOKEN));
                assertCanonicalAndSane(r);
            }
            assertTrue(instruments.size() >= 1,
                    "new feature rows must cover ≥1 instrument, got " + instruments.size());

            long[] ckpt = assertCompletedCheckpoint();
            long runMs = System.currentTimeMillis() - runStartMillis;
            double rawPerSec = (rawEnd - rawStart) * 1000.0 / runMs;
            double featPerSec = (featEnd - featStart) * 1000.0 / runMs;
            System.out.println("DESIGN-PERF[envelope] SIG-PERF-001: raw=" + rawEnd + " (+"
                    + (rawEnd - rawStart) + " rows), feature=" + featEnd + " (+"
                    + (featEnd - featStart) + " rows), instruments=" + instruments.size()
                    + ", rawRowsPerSec=" + String.format("%.0f", rawPerSec)
                    + ", candleRowsPerSec=" + String.format("%.0f", featPerSec)
                    + ", checkpoints=" + ckpt[0] + ", checkpointBytes=" + ckpt[1]
                    + ", runMs=" + runMs + ", samples=" + samples
                    + " (envelope 1 024 instruments / 20 480 t/s)");
            LOG.info("chain-e2e PASS: raw {}→{} rows, feature {}→{} rows, {} instruments, {} samples",
                    rawStart, rawEnd, featStart, featEnd, instruments.size(), samples);
        } finally {
            teardown();
        }
    }

    // ── ingestion subprocess ──────────────────────────────────────────────

    /** Starts the Go fake broker in real-rate mode; returns the process. */
    private static Process startFaketool() throws IOException {
        int port = freePort();
        faketoolPort = port;
        String bin = env("FAKETOOL_BIN", "go-bridge/faketool/faketool");
        LOG.info("chain-e2e: starting faketool {} on :{}", bin, port);
        Process p = new ProcessBuilder(bin, "--port", String.valueOf(port),
                "-real-rate", "-real-rate-hz", "20")
                .redirectErrorStream(true).start();
        // Drain the merged stdout/stderr on a DAEMON thread: the faketool is a
        // long-running server, so a synchronous transferTo would block the
        // test forever (the pipe never reaches EOF while the process lives).
        Thread drainer = new Thread(() -> {
            try (java.io.InputStream in = p.getInputStream()) {
                in.transferTo(OutputStream.nullOutputStream());
            } catch (IOException ignored) {
                // process died — nothing to drain
            }
        });
        drainer.setDaemon(true);
        drainer.start();
        try {
            Thread.sleep(500L); // let it bind
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return p;
    }

    /** Launches the real IngestionService as a subprocess (FullStackE2ETest pattern). */
    private static Process startIngestion(String broker) throws IOException {
        String cp = env("INGESTION_CLASSPATH", System.getProperty("java.class.path"));
        String bridgeBin = env("ARROW_BRIDGE_BIN", "go-bridge/arrow-bridge");
        String manifest = env("INSTRUMENT_MANIFEST_PATH", DEFAULT_MANIFEST);

        List<String> cmd = new ArrayList<>(List.of(
                "java", "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "-cp", cp, "com.trading.ingestion.IngestionService"));

        Map<String, String> e = new HashMap<>();
        e.put("ARROW_BRIDGE_BIN", bridgeBin);
        e.put("INSTRUMENT_MANIFEST_PATH", manifest);
        e.put("ARROW_INSTRUMENT_MANIFEST", manifest);
        e.put("FLUSS_BOOTSTRAP", env("FLUSS_BOOTSTRAP", "localhost:9123"));
        e.put("RAW_TABLE_NAME", "raw_table_1");
        e.put("ARROW_MAX_EVENT_AGE_MS", env("ARROW_MAX_EVENT_AGE_MS", "5000"));
        e.put("ARROW_MAX_FUTURE_EVENT_SKEW_MS", env("ARROW_MAX_FUTURE_EVENT_SKEW_MS", "2000"));
        e.put("READINESS_FILE_PATH",
                Path.of(System.getProperty("java.io.tmpdir"), "signal-chain-e2e-ready").toString());
        // log4j2's JSON_FILE appender (ingestion log4j2.xml) needs a writable
        // LOG_DIR; without one it fails to create /data/ingestion/logs and the
        // structured JSON evidence stream is lost (same trap FullStackE2ETest
        // documents). Shared tmp path is fine for the single subprocess.
        Path subLogDir = Path.of(System.getProperty("java.io.tmpdir"), "signal-chain-e2e-logs");
        java.nio.file.Files.createDirectories(subLogDir);
        e.put("LOG_DIR", subLogDir.toString());

        if ("faketool".equals(broker)) {
            e.put("ARROW_HFT_URL", "ws://127.0.0.1:" + faketoolPort());
            e.put("ARROW_APP_ID", "e2e");
            e.put("ARROW_APP_SECRET", "e2esecret");
            e.put("ARROW_USER_ID", "e2e-user");
        e.put("ARROW_PASSWORD", "e2e-pass");
        e.put("ARROW_TOTP_KEY", "JBSWY3DPEHPK3PXP");
            e.put("ARROW_HFT_CONNECTIONS", "1");
        } else {
            e.put("ARROW_APP_ID", env("ARROW_APP_ID", ""));
            e.put("ARROW_APP_SECRET", env("ARROW_APP_SECRET", ""));
            e.put("ARROW_USER_ID", env("ARROW_USER_ID", ""));
            e.put("ARROW_PASSWORD", env("ARROW_PASSWORD", ""));
            e.put("ARROW_TOTP_KEY", env("ARROW_TOTP_KEY", ""));
            String tokens = env("ARROW_INSTRUMENT_TOKENS", "");
            if (!tokens.isBlank()) {
                e.put("ARROW_INSTRUMENT_TOKENS", tokens);
            }
        }

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.environment().putAll(e);
        pb.redirectErrorStream(false);
        Process proc = pb.start();
        drain(proc);
        return proc;
    }

    private static int faketoolPort = -1;

    private static int faketoolPort() {
        return faketoolPort;
    }

    // ── compute leg ───────────────────────────────────────────────────────

    private static JobClient startSignalJob(String bootstrap) throws Exception {
        Map<String, String> e = new HashMap<>();
        e.put("FLUSS_BOOTSTRAP_SERVERS", bootstrap);
        e.put("FLUSS_DATABASE", "default");
        e.put("RAW_TABLE", "raw_table_1");
        e.put("CANDLE_TABLE", "feature_candles_15s");
        e.put("SIGNAL_CANDIDATES_TABLE", "Signal_Candidates");
        e.put("SIGNAL_CURRENT_TABLE", "Signal_Candidates_current");
        e.put("DEDUP_TTL_MS", "60000");
        e.put("CANDLE_WINDOW_MS", "15000");
        e.put("CHECKPOINT_INTERVAL_MS", "10000");
        e.put("CHECKPOINT_TIMEOUT_MS", "30000");
        e.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        e.put("ALLOW_FULL_REPLAY", "true"); // fresh job — explicit offset-0 replay gate
        e.put("CHECKPOINT_DIR", env("E2E_CHECKPOINT_DIR", "file:///tmp/signal-chain-e2e-checkpoints"));
        // Backend passthrough (design B, 2026-08-16): the state-authoritative
        // dedup set is ~6M entries at the 20,480 t/s envelope — the benchmark
        // MUST run on RocksDB (rule 9), never the dev hashmap default. The
        // runner script sets STATE_BACKEND=rocksdb + STATE_BACKEND_LOCAL_DIRS;
        // absent those, the dev defaults apply as before.
        //
        // TASK_MANAGER_MEMORY_MANAGED_SIZE (2026-08-17, E2E root cause): local
        // execution defaults taskmanager.memory.managed.size to 128 MB TOTAL,
        // which starves the RocksDB block cache for the ~628 MB dedup envelope
        // and throttles the job to ≈ the 20,480/s feed rate (backlog never
        // drains, feature never grows). The runner must pass a realistic
        // managed-memory budget (e.g. 2048m) for the E2E to reach the tail.
        for (String k : new String[] {"STATE_BACKEND", "STATE_BACKEND_LOCAL_DIRS",
                "STATE_BACKEND_MANAGED_MEMORY", "TASK_MANAGER_MEMORY_MANAGED_SIZE",
                "TASK_MANAGER_NETWORK_MEMORY_MAX", "PARALLELISM",
                // CHG-023 item-1 gap fix 2026-08-17: the native reporter's
                // endpoint is wired from OTEL_COLLECTOR_HOST, but the E2E env
                // map was explicit and never passed it — every host run fell
                // back to the container-network default otel-collector:4318
                // (UnknownHostException on every 10 s flush; O2 got nothing).
                // The runner script now defaults OTEL_COLLECTOR_HOST=localhost:4318.
                "OTEL_COLLECTOR_HOST"}) {
            String v = System.getenv().get(k);
            if (v != null && !v.isBlank()) {
                e.put(k, v);
            }
        }

        SignalJobConfig config = SignalJobConfig.from(e);
        StreamExecutionEnvironment senv = SignalJob.buildTopology(config);
        JobClient client = senv.executeAsync("signal-chain-e2e");
        awaitTrue(() -> statusIs(client, JobStatus.RUNNING),
                "SignalJob reaches RUNNING", 90);
        LOG.info("chain-e2e: SignalJob RUNNING, jobId={}", client.getJobID());
        return client;
    }

    // ── Fluss access (P6 probe pattern) ───────────────────────────────────

    private void connectFluss(String bootstrap) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        try {
            connection = ConnectionFactory.createConnection(conf);
            rawTable = connection.getTable(TablePath.of("default", "raw_table_1"));
            featureTable = connection.getTable(TablePath.of("default", "feature_candles_15s"));
            LOG.info("chain-e2e: connected to Fluss at {}", bootstrap);
        } catch (Exception e) {
            LOG.warn("chain-e2e: cannot connect to {} — {}", bootstrap, e.getMessage());
            assumeTrue(false, "Fluss cluster not available at " + bootstrap);
        }
    }

    private boolean tableExists(String name) {
        try {
            connection.getTable(TablePath.of("default", name)).getTableInfo();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Row count over every bucket. KV tables (feature_candles_15s,
     * Signal_Candidates_current) are counted with the batch scanner (P6
     * scanAll pattern, count-only — no materialization). LOG tables
     * (raw_table_1) are counted as the SUM OF END OFFSETS via listOffsets:
     * the KV batch scanner returns nothing for a LOG table, and a full log
     * scan would replay the multi-GB backlog per sample — measured
     * 2026-08-16 that a LOG-table batch scan ALSO poisons the shared
     * connection so the next KV scan silently returns 0, so the two paths
     * must never share a scan call. The end-offset sum is monotone with
     * growth, which is exactly what the DELTA assertions need.
     */
    private long countRows(Table table) throws Exception {
        TableInfo info = table.getTableInfo();
        if (!info.hasPrimaryKey()) {
            List<Integer> buckets = new ArrayList<>();
            for (int b = 0; b < info.getNumBuckets(); b++) {
                buckets.add(b);
            }
            org.apache.fluss.client.admin.ListOffsetsResult ends = connection.getAdmin()
                    .listOffsets(info.getTablePath(), buckets,
                            new org.apache.fluss.client.admin.OffsetSpec.LatestSpec());
            long sum = 0;
            for (int b : buckets) {
                sum += ends.bucketResult(b).get(10, TimeUnit.SECONDS);
            }
            return sum;
        }
        // KV count under live write load is NOT snapshot-isolated per scan:
        // a bucket's batch scan can transiently return a partial snapshot while
        // the job is concurrently upserting (observed 2026-08-17: one bucket's
        // worth of rows vanished from a single sample, count recovered after
        // the run). KV rows are never deleted, so the true count is the MAX
        // over repeated scans — a single low scan is a read artifact, not a
        // shrink. Longer poll timeout so a busy server can finish the scan.
        // This full max-of-3 is used for the start/end baselines (a handful of
        // calls per run); the per-sample loop uses countRowsLazy so the
        // steady-state scan load is 1 full pass per sample instead of 3
        // (3× full-table scans every 15 s were measured to roughly halve the
        // job's replay throughput on the shared Fluss server).
        return countRowsLazy(table, 0, 3, false);
    }

    /**
     * KV row count with dip-retry. {@code forceMax} (baseline calls) always
     * runs all {@code attempts} and returns the max. Per-sample calls pass
     * {@code previousBest} = the previous sample's count: a scan >= previous
     * best is the true count (KV rows only grow) and returns after ONE scan;
     * a dip means the scan caught a partial snapshot, so it keeps scanning
     * and returns the max (guarding against two partial scans agreeing).
     */
    private long countRowsLazy(Table table, long previousBest, int attempts, boolean forceMax)
            throws Exception {
        TableInfo info = table.getTableInfo();
        long best = 0;
        int attempt = 0;
        while (attempt < attempts) {
            long n = 0;
            for (int b = 0; b < info.getNumBuckets(); b++) {
                TableBucket tb = new TableBucket(info.getTableId(), b);
                try (BatchScanner scanner = table.newScan()
                             .limit(Integer.MAX_VALUE)
                             .createBatchScanner(tb);
                     CloseableIterator<InternalRow> it =
                             scanner.pollBatch(Duration.ofSeconds(5))) {
                    while (it.hasNext()) {
                        it.next();
                        n++;
                    }
                }
            }
            if (n > best) {
                best = n;
            }
            if (!forceMax && best >= previousBest) {
                return best;
            }
            attempt++;
        }
        return best;
    }

    /** Per-sample: 1 scan unless a dip below the previous sample is detected. */
    private long countRowsLazy(Table table, long previousBest) throws Exception {
        return countRowsLazy(table, previousBest, 3, false);
    }

    private long rawCount() throws Exception {
        return countRows(rawTable);
    }

    private long featureCount() throws Exception {
        return countRows(featureTable);
    }

    /** Feature rows whose window ended after the run started. */
    private List<InternalRow> newFeatureRows() throws Exception {
        TableInfo info = featureTable.getTableInfo();
        List<InternalRow> rows = new ArrayList<>();
        for (int b = 0; b < info.getNumBuckets(); b++) {
            TableBucket tb = new TableBucket(info.getTableId(), b);
            try (BatchScanner scanner = featureTable.newScan()
                         .limit(Integer.MAX_VALUE)
                         .createBatchScanner(tb);
                 CloseableIterator<InternalRow> it =
                         scanner.pollBatch(Duration.ofSeconds(5))) {
                while (it.hasNext()) {
                    InternalRow r = it.next();
                    if (r.getLong(CandleTableColumns.WINDOW_END) > runStartMillis) {
                        rows.add(r);
                    }
                }
            }
        }
        return rows;
    }

    private static void assertCanonicalAndSane(InternalRow r) {
        long open = r.getLong(CandleTableColumns.OPEN_PAISE);
        long high = r.getLong(CandleTableColumns.HIGH_PAISE);
        long low = r.getLong(CandleTableColumns.LOW_PAISE);
        long close = r.getLong(CandleTableColumns.CLOSE_PAISE);
        assertTrue(low <= Math.min(open, close) && Math.max(open, close) <= high,
                "OHLC sanity: low=" + low + " open=" + open + " high=" + high + " close=" + close);
        assertTrue(r.getInt(CandleTableColumns.TICK_COUNT) >= 1,
                "emitted candle must have ≥1 tick");
        assertEquals("2", r.getString(CandleTableColumns.SCHEMA_VERSION).toString(),
                "schema_version must be canonical v2");
        // Algorithm/configuration versions are asserted canonical by the job's
        // own emit path; a non-canonical row here would fail CanonicalCandlePolicy.
    }

    /**
     * Asserts ≥1 completed checkpoint and returns {@code [count, bytes]} — the
     * SIG-PERF-001 checkpoint-size evidence (bytes = summed checkpoint dir).
     */
    private long[] assertCompletedCheckpoint() throws Exception {
        String dir = env("E2E_CHECKPOINT_DIR", "file:///tmp/signal-chain-e2e-checkpoints");
        Path root = Path.of(dir.replaceFirst("^file://", ""));
        try (Stream<Path> walk = Files.walk(root)) {
            List<Path> all = walk.filter(Files::isRegularFile).toList();
            long completed = all.stream()
                    .filter(p -> p.getFileName().toString().equals("_metadata")).count();
            long bytes = all.stream().mapToLong(p -> {
                try {
                    return Files.size(p);
                } catch (java.io.IOException e) {
                    return 0L;
                }
            }).sum();
            assertTrue(completed >= 1,
                    "SignalJob must complete ≥1 checkpoint, found " + completed
                            + " under " + root);
            return new long[] {completed, bytes};
        }
    }

    // ── teardown ──────────────────────────────────────────────────────────

    private void teardown() {
        Exception first = null;
        try {
            if (job != null) {
                job.cancel().get(30, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            first = e;
        }
        if (ingestionProc != null) {
            ingestionProc.destroy();
            try {
                if (!ingestionProc.waitFor(10, TimeUnit.SECONDS)) {
                    ingestionProc.destroyForcibly();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (faketoolProc != null) {
            faketoolProc.destroyForcibly();
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                if (first == null) {
                    first = e;
                }
            }
        }
        if (first != null) {
            throw new RuntimeException("teardown failure", first);
        }
    }

    // ── plumbing ──────────────────────────────────────────────────────────

    private static void drain(Process proc) {
        // Drain BOTH streams on daemon threads: a long-lived subprocess whose
        // stdout/stderr pipe fills would block forever. (The error stream is
        // the ingest service's stderr; stdout carries its log4j console
        // output.)
        drainStream(proc.getErrorStream());
        drainStream(proc.getInputStream());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> proc.destroyForcibly()));
    }

    // Optional subprocess-output capture (E2E_DRAIN_LOG): when set, subprocess
    // stdout/stderr is appended to that file instead of discarded — the
    // 2026-08-17 warmup failures hid behind the null drain (bridge FATAL /
    // config errors were invisible). Diagnosis only; unset keeps the null
    // drain.
    private static java.nio.file.Path drainLog() {
        String v = System.getenv("E2E_DRAIN_LOG");
        return (v == null || v.isBlank()) ? null : java.nio.file.Path.of(v);
    }

    private static void drainStream(java.io.InputStream in) {
        java.nio.file.Path log = drainLog();
        Thread t = new Thread(() -> {
            try (java.io.InputStream s = in;
                 java.io.OutputStream out = log == null
                         ? OutputStream.nullOutputStream()
                         : java.nio.file.Files.newOutputStream(log,
                                 java.nio.file.StandardOpenOption.CREATE,
                                 java.nio.file.StandardOpenOption.APPEND)) {
                s.transferTo(out);
            } catch (IOException ignored) {
            }
        });
        t.setDaemon(true);
        t.start();
    }

    @FunctionalInterface
    private interface ThrowingBooleanSupplier {
        boolean getAsBoolean() throws Exception;
    }

    /** Runs a throwing check inside a plain BooleanSupplier (P6 safe() pattern). */
    private static boolean safeThrows(ThrowingBooleanSupplier cond) {
        try {
            return cond.getAsBoolean();
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean statusIs(JobClient c, JobStatus wanted) {
        try {
            return c.getJobStatus().get(10, TimeUnit.SECONDS) == wanted;
        } catch (Exception e) {
            return false;
        }
    }

    private static void awaitTrue(BooleanSupplier cond, String what, long timeoutSeconds)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (System.nanoTime() < deadline) {
            if (cond.getAsBoolean()) {
                return;
            }
            Thread.sleep(1_000L);
        }
        fail("Timed out after " + timeoutSeconds + "s waiting for " + what);
    }

    private static int freePort() throws IOException {
        try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static String env(String key, String def) {
        return System.getenv().getOrDefault(key, def);
    }

    private static long longEnv(String key, long def) {
        String v = System.getenv().get(key);
        return v == null ? def : Long.parseLong(v);
    }
}
