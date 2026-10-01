import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLongArray;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetSpec.LatestSpec;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;

/**
 * FlussReadabilityProbe — per-timeframe readable-to-a-fresh-reader sampler and
 * per-timeframe state counter (approved 2026-09-30; "readable" = a fresh
 * reader sees the row, Option A).
 *
 * <p>Long-lived companion probe for the latency matrix: runs for
 * {@code duration_s} in the background of a stage capture and appends to
 * four TSVs next to the other probe evidence (headers written if absent):
 *
 * <pre>
 *   liveread.tsv    epoch_ms  token  tf  window_start  window_end  last_event_time  staleness_ms
 *   closeread.tsv   epoch_ms  token  tf  window_start  window_end  latency_ms
 *   featureread.tsv epoch_ms  token  tf  window_start  window_end  feature_ids  latency_ms
 *   state-tf.tsv    epoch_ms  table  tf  rows_cum  bytes_cum
 * </pre>
 *
 * <p>Latency legs (each on its OWN thread; the first smoke measured a 0-7.6 s
 * sawtooth when a slow live sweep shared the loop with the scanners,
 * 2026-09-30):
 * <ul>
 *   <li><b>live</b> — KV point lookups of {@code candle_features} for every
 *       timeframe of a small token sample. All current-window lookups are
 *       issued as one fan-out and then collected (one RTT round, P6-086
 *       discipline); only the misses get a second fan-out against the
 *       previous window. {@code staleness_ms = read_ts - last_event_time} =
 *       age of the newest sealed candle a reader can see. Since closed-only
 *       storage (CHG-485/486) there is no forming row, so this series is a
 *       sawtooth 0..TF by construction — informational (repointed 2026-10-01,
 *       CHG-489); the readability SLO is window close -> first read (the
 *       closed/features legs).
 *   <li><b>closed</b> — log tail of {@code candle_features} SEALED rows,
 *       subscribed to ALL buckets from the CURRENT log end (fallback: from
 *       beginning with an age filter when listOffsets(latest) fails); one row
 *       per {@code (token, tf, window_start)} first sighting **on the latency
 *       sample buckets** ({@code buckets_csv}, default 0,1);
 *       {@code latency_ms = read_ts - window_end} = window close -> readable.
 *   <li><b>features</b> — the same sealed-row tail with the feature-id list
 *       carried by the row so the report can break down per registered
 *       feature id (0 last_price, 1 sma_close_20, 2 rsi_close_14).
 * </ul>
 *
 * <p>State leg: the same two tails count EVERY record they see — per timeframe,
 * cumulative rows and bytes ({@code ScanRecord.getSizeInBytes()}) — regardless
 * of the latency bucket sample, and a snapshot is written to
 * {@code state-tf.tsv} every {@value #SNAPSHOT_MS} ms. That is the
 * per-timeframe state-growth meter: which timeframe is accumulating how many
 * stored rows/bytes as the run progresses. Numeric bytes are changelog payload
 * bytes (an upper-bound proxy for stored bytes); rows are exact for the tailed
 * tables because the tails subscribe all buckets.
 *
 * <p>Contract:
 * <ul>
 *   <li>columns are resolved BY NAME from the live TableInfo; a renamed or
 *       missing column is an input error, never a wrong-index read (P6-371
 *       discipline).
 *   <li>UPDATE_BEFORE/DELETE changelog records are skipped; a row counts when
 *       its UPDATE_AFTER/INSERT/APPEND_ONLY record is first readable.
 *   <li>replayed rows older than AGE_FILTER_MS before start (only possible on
 *       the subscribe-from-beginning fallback) are skipped and counted, never
 *       printed as huge fake latencies and never added to the state counters.
 *   <li>one failing lookup/table is reported on stderr and the other legs keep
 *       sampling; a broken schema is fail-fast input (exit 2).
 *   <li>the feature leg reads the merged candle_features sealed rows (DEC-059,
 *       Wave C W-C5a): every sealed row carries its feature map, so an empty
 *       featureread.tsv means no sealed row was visible — data, not a probe
 *       fault.
 * </ul>
 *
 * <p>Exit codes: 0 all three latency legs emitted rows, 3 partial (some leg
 * empty), 2 unusable input, 1 no rows at all.
 *
 * <p>Usage: java -cp &lt;cp&gt; FlussReadabilityProbe &lt;bootstrap&gt;
 * &lt;tokens_csv&gt; &lt;out_dir&gt; &lt;duration_s&gt; [buckets_csv]
 * [live_interval_ms]
 */
public final class FlussReadabilityProbe {

    static class InputException extends RuntimeException {
        InputException(String msg) {
            super(msg);
        }
    }

    /** Timeframe discriminator -> window length ms (multi-TF contract). */
    static final Map<String, Long> TF_WINDOW_MS = new LinkedHashMap<>();

    static {
        TF_WINDOW_MS.put("FIFTEEN_S", 15_000L);
        TF_WINDOW_MS.put("THIRTY_S", 30_000L);
        TF_WINDOW_MS.put("ONE_M", 60_000L);
        TF_WINDOW_MS.put("THREE_M", 180_000L);
        TF_WINDOW_MS.put("FIVE_M", 300_000L);
        TF_WINDOW_MS.put("FIFTEEN_M", 900_000L);
    }

    static final List<String> TF_ORDER = new ArrayList<>(TF_WINDOW_MS.keySet());
    static final Map<String, Integer> TF_INDEX = new HashMap<>();

    static {
        for (int i = 0; i < TF_ORDER.size(); i++) {
            TF_INDEX.put(TF_ORDER.get(i), i);
        }
    }

    private static final long LOOKUP_TIMEOUT_MS = 2_000L;
    private static final long POLL_MS = 200L;
    /** Fallback-replay guard: windows that ended this long before start are history. */
    private static final long AGE_FILTER_MS = 300_000L;
    /** Per-timeframe state snapshot cadence (ms). */
    static final long SNAPSHOT_MS = 5_000L;

    public static void main(String[] args) {
        int code = 1;
        try {
            code = run(args);
        } catch (InputException e) {
            System.err.println("FlussReadabilityProbe: " + e);
            System.exit(2);
        } catch (Throwable t) {
            System.err.println("FlussReadabilityProbe failed: " + t);
            t.printStackTrace();
            System.exit(1);
        }
        System.out.flush();
        System.exit(code);
    }

    static int run(String[] args) throws Exception {
        if (args.length < 4) {
            throw new InputException("usage: <bootstrap> <tokens_csv> <out_dir> <duration_s>"
                    + " [buckets_csv] [live_interval_ms]");
        }
        String bootstrap = args[0];
        List<Long> tokens = parseTokens(args[1]);
        if (tokens.isEmpty()) {
            throw new InputException("no usable token in tokens_csv '" + args[1] + "'");
        }
        Path outDir = Paths.get(args[2]);
        Files.createDirectories(outDir);
        long durationS = parsePositive(args[3], "duration_s");
        String bucketsRaw = args.length > 4 ? args[4] : "0,1";
        long liveIntervalMs = args.length > 5 ? parsePositive(args[5], "live_interval_ms") : 1000L;

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        long startedMs = System.currentTimeMillis();
        long deadlineMs = startedMs + durationS * 1000L;

        Writer liveOut = new Writer(outDir.resolve("liveread.tsv"),
                "epoch_ms\ttoken\ttf\twindow_start\twindow_end\tlast_event_time\tstaleness_ms");
        Writer closedOut = new Writer(outDir.resolve("closeread.tsv"),
                "epoch_ms\ttoken\ttf\twindow_start\twindow_end\tlatency_ms");
        Writer featureOut = new Writer(outDir.resolve("featureread.tsv"),
                "epoch_ms\ttoken\ttf\twindow_start\twindow_end\tfeature_ids\tlatency_ms");
        Writer stateOut = new Writer(outDir.resolve("state-tf.tsv"),
                "epoch_ms\ttable\ttf\trows\tbytes");

        System.out.println("FLUSS-READABILITY start bootstrap=" + bootstrap + " tokens=" + tokens
                + " buckets=" + bucketsRaw + " live_interval_ms=" + liveIntervalMs
                + " duration_s=" + durationS);
        try (Connection conn = ConnectionFactory.createConnection(conf);
                Admin admin = conn.getAdmin()) {
            LiveSampler live = LiveSampler.open(conn, tokens, liveIntervalMs, liveOut);
            Tail closed = Tail.open(conn, admin, "candle_features", bucketsRaw, startedMs, closedOut, false);
            Tail features = Tail.open(conn, admin, "candle_features", bucketsRaw, startedMs, featureOut, true);
            TfSnapshotter snapshotter = new TfSnapshotter(
                    stateOut, List.of(features), startedMs);

            // One thread per leg: a record's read timestamp must never wait on
            // another leg's RPC, and the state snapshot must never stall the
            // scanner tails. Each thread owns its writers; stateOut is owned
            // by the live thread through the snapshotter and closed below.
            Thread closedThread = tailThread("closed-tail", closed, deadlineMs);
            Thread featureThread = tailThread("feature-tail", features, deadlineMs);
            Thread liveThread = new Thread(() -> {
                try {
                    live.runUntil(deadlineMs, snapshotter::maybeSnapshot);
                } catch (Throwable t) {
                    System.err.println("FlussReadabilityProbe live thread died: " + t);
                    t.printStackTrace();
                } finally {
                    closeQuietly(liveOut);
                    closeQuietly(stateOut);
                }
            }, "live-sweep");

            closedThread.start();
            featureThread.start();
            liveThread.start();
            closedThread.join();
            featureThread.join();
            liveThread.join();

            live.close();
            closed.close();
            features.close();
            long liveRows = live.emitted();
            long closedRows = closed.emitted;
            long featureRows = features.emitted;
            System.out.println("FLUSS-READABILITY done live_rows=" + liveRows
                    + " closed_rows=" + closedRows + " feature_rows=" + featureRows);
            System.out.flush();
            return liveRows > 0 && closedRows > 0 && featureRows > 0 ? 0 : 3;
        }
    }

    private static Thread tailThread(String name, Tail tail, long deadlineMs) {
        return new Thread(() -> {
            try {
                tail.runUntil(deadlineMs);
            } catch (Throwable t) {
                System.err.println("FlussReadabilityProbe " + name + " died: " + t);
                t.printStackTrace();
            }
        }, name);
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            System.err.println("FlussReadabilityProbe: close failed: " + e);
        }
    }

    /** A per-loop hook that may do checked I/O (the state snapshot writer). */
    @FunctionalInterface
    interface LoopHook {
        void run() throws Exception;
    }

    // ── live leg: one fan-out of KV samples per sweep, per timeframe ─────────

    static final class LiveSampler {
        private final Lookuper lookuper;
        private final int windowEndIdx;
        private final int lastEventIdx;
        private final List<Long> tokens;
        private final long intervalMs;
        private final Writer out;
        private volatile long emitted;

        private LiveSampler(Lookuper lookuper, int windowEndIdx, int lastEventIdx,
                List<Long> tokens, long intervalMs, Writer out) {
            this.lookuper = lookuper;
            this.windowEndIdx = windowEndIdx;
            this.lastEventIdx = lastEventIdx;
            this.tokens = tokens;
            this.intervalMs = intervalMs;
            this.out = out;
        }

        static LiveSampler open(Connection conn, List<Long> tokens, long intervalMs, Writer out)
                throws Exception {
            Table table = conn.getTable(TablePath.of("default", "candle_features"));
            List<String> names = table.getTableInfo().getSchema().getRowType().getFieldNames();
            int windowEndIdx = names.indexOf("window_end");
            int lastEventIdx = names.indexOf("last_event_time");
            if (windowEndIdx < 0 || lastEventIdx < 0) {
                throw new InputException("candle_features has no window_end/last_event_time column"
                        + " (columns: " + names + ")");
            }
            return new LiveSampler(table.newLookup().createLookuper(), windowEndIdx, lastEventIdx,
                    tokens, intervalMs, out);
        }

        void runUntil(long deadlineMs, LoopHook eachLoop) throws InterruptedException {
            while (System.currentTimeMillis() < deadlineMs) {
                long sweepStart = System.currentTimeMillis();
                try {
                    sweep(sweepStart);
                    out.flush();
                } catch (InterruptedException ie) {
                    throw ie;
                } catch (Exception e) {
                    System.err.println("FlussReadabilityProbe live sweep failed: " + e);
                }
                try {
                    eachLoop.run();
                } catch (Exception e) {
                    System.err.println("FlussReadabilityProbe state snapshot failed: " + e);
                }
                long sleepMs = intervalMs - (System.currentTimeMillis() - sweepStart);
                if (sleepMs > 0) {
                    Thread.sleep(Math.min(sleepMs, Math.max(1, deadlineMs - System.currentTimeMillis())));
                }
            }
        }

        private void sweep(long now) throws Exception {
            List<Pending> round1 = new ArrayList<>(TF_ORDER.size() * tokens.size());
            for (String tf : TF_ORDER) {
                long winMs = TF_WINDOW_MS.get(tf);
                long current = (now / winMs) * winMs;
                for (long token : tokens) {
                    round1.add(new Pending(tf, token, current));
                }
            }
            Set<String> found = collect(round1);

            List<Pending> round2 = new ArrayList<>();
            for (Pending p : round1) {
                if (!found.contains(p.key())) {
                    round2.add(new Pending(p.tf, p.token, p.windowStart - TF_WINDOW_MS.get(p.tf)));
                }
            }
            Set<String> found2 = collect(round2);
            if (!round2.isEmpty()) {
                int misses = round2.size() - found2.size();
                if (misses > 0) {
                    System.err.println("FlussReadabilityProbe live sample: " + misses
                            + " of " + round1.size()
                            + " token/tf lookups found no current/previous window row");
                }
            }
        }

        /** Issue every lookup of the round first, then collect (one RTT round). */
        private Set<String> collect(List<Pending> pendings) throws IOException {
            if (pendings.isEmpty()) {
                return Set.of();
            }
            Map<Pending, CompletableFuture<LookupResult>> futures = new LinkedHashMap<>();
            for (Pending p : pendings) {
                futures.put(p, lookuper.lookup(GenericRow.of(
                        p.token, BinaryString.fromString(p.tf), p.windowStart)));
            }
            Set<String> found = new HashSet<>();
            int failed = 0;
            for (Map.Entry<Pending, CompletableFuture<LookupResult>> entry : futures.entrySet()) {
                Pending p = entry.getKey();
                try {
                    InternalRow row = entry.getValue()
                            .get(LOOKUP_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                            .getSingletonRow();
                    if (row == null) {
                        continue;
                    }
                    // Timestamp per emitted row (P6-372): earlier lookups in
                    // this fan-out completed at different instants.
                    long ts = System.currentTimeMillis();
                    long windowEnd = row.getLong(windowEndIdx);
                    long lastEvent = row.isNullAt(lastEventIdx) ? -1L : row.getLong(lastEventIdx);
                    long staleness = lastEvent < 0 ? -1L : ts - lastEvent;
                    out.row(ts + "\t" + p.token + "\t" + p.tf + "\t" + p.windowStart + "\t"
                            + windowEnd + "\t" + lastEvent + "\t" + staleness);
                    emitted++;
                    found.add(p.key());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while collecting live lookups", ie);
                } catch (TimeoutException | ExecutionException e) {
                    failed++;
                } catch (IOException e) {
                    throw e;
                } catch (Exception e) {
                    failed++;
                }
            }
            if (failed > 0) {
                System.err.println("FlussReadabilityProbe live sample: " + failed
                        + " lookup(s) failed (timeout/execution)");
            }
            return found;
        }

        long emitted() {
            return emitted;
        }

        void close() {
            // Lookuper holds no independent resource beyond the Table (never closed here).
        }
    }

    /** One (tf, token, window) lookup: the unit of a live fan-out round. */
    static final class Pending {
        final String tf;
        final long token;
        final long windowStart;

        Pending(String tf, long token, long windowStart) {
            this.tf = tf;
            this.token = token;
            this.windowStart = windowStart;
        }

        String key() {
            return tf + "|" + token;
        }
    }

    // ── per-timeframe state snapshots ─────────────────────────────────────────

    /**
     * Writes one {@code state-tf.tsv} line per (table, timeframe) per snapshot:
     * cumulative rows and changelog bytes counted by the tails, which subscribe
     * every bucket — the counts are exact for the tailed tables, unlike the
     * per-timeframe latency log which stays bucket-sampled.
     */
    static final class TfSnapshotter {
        private final Writer out;
        private final List<Tail> tails;
        private long nextMs;
        private final long intervalMs;

        TfSnapshotter(Writer out, List<Tail> tails, long startedMs) {
            this(out, tails, startedMs, SNAPSHOT_MS);
        }

        TfSnapshotter(Writer out, List<Tail> tails, long startedMs, long intervalMs) {
            this.out = out;
            this.tails = tails;
            this.intervalMs = intervalMs;
            this.nextMs = startedMs + intervalMs;
        }

        void maybeSnapshot() throws IOException {
            long now = System.currentTimeMillis();
            if (now < nextMs) {
                return;
            }
            nextMs = now + intervalMs;
            long ts = System.currentTimeMillis();
            for (Tail tail : tails) {
                for (int i = 0; i < TF_ORDER.size(); i++) {
                    out.row(ts + "\t" + tail.name + "\t" + TF_ORDER.get(i) + "\t"
                            + tail.rowsByTf.get(i) + "\t" + tail.bytesByTf.get(i));
                }
            }
            out.flush();
        }
    }

    // ── closed/feature legs: log tail, first sighting per key ─────────────────

    static final class Tail implements AutoCloseable {
        final String name;
        private final Table table;
        private final LogScanner scanner;
        private final Writer out;
        private final boolean featureTable;
        private final int tokenIdx;
        private final int tfIdx;
        private final int windowStartIdx;
        private final int featuresIdx;
        private final int sealedIdx;
        private final long startedMs;
        private final Set<Integer> latencyBuckets;
        private final Set<String> seen = new HashSet<>();
        private volatile long emitted;
        private long skippedOld;
        private long skippedForming;
        /** Per-timeframe state counters (index = TF_ORDER position). */
        final AtomicLongArray rowsByTf = new AtomicLongArray(TF_ORDER.size());
        final AtomicLongArray bytesByTf = new AtomicLongArray(TF_ORDER.size());

        private Tail(String name, Table table, LogScanner scanner, Writer out, boolean featureTable,
                int tokenIdx, int tfIdx, int windowStartIdx, int featuresIdx, int sealedIdx,
                long startedMs, Set<Integer> latencyBuckets) {
            this.name = name;
            this.table = table;
            this.scanner = scanner;
            this.out = out;
            this.featureTable = featureTable;
            this.tokenIdx = tokenIdx;
            this.tfIdx = tfIdx;
            this.windowStartIdx = windowStartIdx;
            this.featuresIdx = featuresIdx;
            this.sealedIdx = sealedIdx;
            this.startedMs = startedMs;
            this.latencyBuckets = latencyBuckets;
        }

        static Tail open(Connection conn, Admin admin, String tableName, String bucketsRaw,
                long startedMs, Writer out, boolean featureTable) throws Exception {
            TablePath path = TablePath.of("default", tableName);
            Table table = conn.getTable(path);
            TableInfo info = table.getTableInfo();
            List<String> names = info.getSchema().getRowType().getFieldNames();
            int tokenIdx = names.indexOf("instrument_token");
            int tfIdx = names.indexOf("tf");
            int windowStartIdx = names.indexOf("window_start");
            int featuresIdx = featureTable ? names.indexOf("features") : -1;
            int sealedIdx = names.indexOf("sealed");
            if (tokenIdx < 0 || tfIdx < 0 || windowStartIdx < 0 || (featureTable && featuresIdx < 0)) {
                throw new InputException(tableName + " schema lacks expected columns (columns: "
                        + names + ")");
            }
            // Latency stays bucket-sampled (rows/s cost); the state counters and
            // the reported counts cover every bucket, so per-TF state is exact.
            List<Integer> latency = parseBuckets(bucketsRaw, info.getNumBuckets());
            if (latency.isEmpty()) {
                throw new InputException(tableName + " has no requested buckets (have "
                        + info.getNumBuckets() + ", requested " + bucketsRaw + ")");
            }
            List<Integer> all = new ArrayList<>();
            for (int b = 0; b < info.getNumBuckets(); b++) {
                all.add(b);
            }
            LogScanner scanner = table.newScan().createLogScanner();
            try {
                ListOffsetsResult res = admin.listOffsets(path, all, new LatestSpec());
                Map<Integer, Long> offsets = res.all().get(5, TimeUnit.SECONDS);
                for (Map.Entry<Integer, Long> e : offsets.entrySet()) {
                    scanner.subscribe(e.getKey(), e.getValue());
                }
            } catch (Exception e) {
                System.err.println("FlussReadabilityProbe: " + tableName
                        + " listOffsets(latest) failed (" + e + ") — subscribing from beginning with a"
                        + " " + AGE_FILTER_MS + "ms age filter");
                for (int b : all) {
                    scanner.subscribeFromBeginning(b);
                }
            }
            System.out.println("FLUSS-READABILITY tail " + tableName + " buckets=ALL"
                    + " latency_sample=" + latency);
            return new Tail(tableName, table, scanner, out, featureTable,
                    tokenIdx, tfIdx, windowStartIdx, featuresIdx, sealedIdx, startedMs,
                    new HashSet<>(latency));
        }

        void runUntil(long deadlineMs) {
            while (System.currentTimeMillis() < deadlineMs) {
                try {
                    poll(Duration.ofMillis(POLL_MS));
                    out.flush();
                } catch (Exception e) {
                    System.err.println("FlussReadabilityProbe " + name + " poll failed: " + e);
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }

        long poll(Duration timeout) throws IOException {
            ScanRecords records = scanner.poll(timeout);
            if (records == null || records.isEmpty()) {
                return 0;
            }
            long n = 0;
            for (TableBucket bucket : records.buckets()) {
                for (ScanRecord rec : records.records(bucket)) {
                    ChangeType ct = rec.getChangeType();
                    if (ct == ChangeType.UPDATE_BEFORE || ct == ChangeType.DELETE) {
                        continue;
                    }
                    InternalRow row = rec.getRow();
                    if (row != null) {
                        n += handle(rec, row, bucket.getBucket());
                    }
                }
            }
            return n;
        }

        private long handle(ScanRecord rec, InternalRow row, int bucket) throws IOException {
            long token = row.getLong(tokenIdx);
            String tf = row.getString(tfIdx).toString();
            long windowStart = row.getLong(windowStartIdx);
            Integer tfIndex = TF_INDEX.get(tf);
            if (tfIndex == null) {
                System.err.println("FlussReadabilityProbe: " + name + " unknown tf '" + tf + "'");
                return 0;
            }
            long windowEnd = windowStart + TF_WINDOW_MS.get(tf);
            long ts = System.currentTimeMillis();
            if (windowEnd + AGE_FILTER_MS < startedMs) {
                skippedOld++;
                return 0;
            }
            // State counters: every record, every bucket, before any sampling.
            rowsByTf.incrementAndGet(tfIndex);
            bytesByTf.addAndGet(tfIndex, rec.getSizeInBytes());
            // Wave C W-C5a: candle_features carries forming rows too — the
            // close-read latency is a SEALED-row measure only.
            if (sealedIdx >= 0 && !row.getBoolean(sealedIdx)) {
                skippedForming++;
                return 0;
            }
            if (!latencyBuckets.contains(bucket)) {
                return 0;
            }
            if (!seen.add(token + "|" + tf + "|" + windowStart)) {
                return 0;
            }
            emitted++;
            if (featureTable) {
                InternalMap map = row.getMap(featuresIdx);
                StringBuilder ids = new StringBuilder();
                if (map != null) {
                    InternalArray keys = map.keyArray();
                    for (int i = 0; i < map.size(); i++) {
                        if (i > 0) {
                            ids.append(',');
                        }
                        ids.append(keys.getInt(i));
                    }
                }
                out.row(ts + "\t" + token + "\t" + tf + "\t" + windowStart + "\t" + windowEnd
                        + "\t" + ids + "\t" + (ts - windowEnd));
            } else {
                out.row(ts + "\t" + token + "\t" + tf + "\t" + windowStart + "\t" + windowEnd
                        + "\t" + (ts - windowEnd));
            }
            return 1;
        }

        @Override
        public void close() throws Exception {
            if (skippedOld > 0) {
                System.err.println("FlussReadabilityProbe: " + name + " skipped " + skippedOld
                        + " replayed row(s) older than the age filter");
            }
            if (skippedForming > 0) {
                System.err.println("FlussReadabilityProbe: " + name + " skipped " + skippedForming
                        + " forming (unsealed) row(s)");
            }
            scanner.close();
            table.close();
            closeQuietly(out);
            System.out.println("FLUSS-READABILITY tail " + name + " emitted=" + emitted);
        }
    }

    // ── shared plumbing ───────────────────────────────────────────────────────

    /** Appends TSV rows; writes the header only into a fresh (missing/empty) file. */
    static final class Writer implements AutoCloseable {
        private final BufferedWriter writer;

        Writer(Path path, String header) throws IOException {
            boolean fresh = !Files.exists(path) || Files.size(path) == 0;
            this.writer = new BufferedWriter(new FileWriter(path.toFile(), true));
            if (fresh) {
                writer.write(header);
                writer.newLine();
                writer.flush();
            }
        }

        void row(String line) throws IOException {
            writer.write(line);
            writer.newLine();
        }

        void flush() throws IOException {
            writer.flush();
        }

        @Override
        public void close() throws IOException {
            writer.flush();
            writer.close();
        }
    }

    /** P6-370 discipline: a positive numeric argument, or an input error. */
    static long parsePositive(String raw, String name) {
        final long v;
        try {
            v = Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            throw new InputException(name + " must be a number, got '" + raw + "'");
        }
        if (v <= 0) {
            throw new InputException(name + " must be > 0, got " + raw);
        }
        return v;
    }

    static List<Long> parseTokens(String raw) {
        List<Long> out = new ArrayList<>();
        if (raw == null) {
            return out;
        }
        for (String part : raw.split(",")) {
            String s = part.trim();
            if (s.isEmpty()) {
                continue;
            }
            try {
                out.add(Long.parseLong(s));
            } catch (NumberFormatException e) {
                System.err.println("FlussReadabilityProbe: skipping bad token '" + s + "'");
            }
        }
        return out;
    }

    /** Requested latency-sample buckets filtered to the table's range; none is fatal. */
    static List<Integer> parseBuckets(String raw, int bucketCount) {
        List<Integer> out = new ArrayList<>();
        for (String part : raw.split(",")) {
            String s = part.trim();
            if (s.isEmpty()) {
                continue;
            }
            final int b;
            try {
                b = Integer.parseInt(s);
            } catch (NumberFormatException e) {
                throw new InputException("bucket must be a number, got '" + s + "'");
            }
            if (b < 0 || b >= bucketCount) {
                throw new InputException("bucket " + b + " out of range [0," + bucketCount + ")");
            }
            if (!out.contains(b)) {
                out.add(b);
            }
        }
        return out;
    }

    private FlussReadabilityProbe() {}
}
