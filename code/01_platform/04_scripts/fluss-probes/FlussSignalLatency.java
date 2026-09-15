import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metadata.TableStats;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

/**
 * Offline signal/intent latency + correctness census (perf report, 2026-09-05).
 *
 * <p>Mode "signals": scans the Signal_Candidates LOG, keeps rows for one
 * rule_id, and reports the evaluation_ts - detection_ts population (tick age
 * at signal time: detection holds the breaking tick's event time). Prints
 * count, min, p50, p95, p99, max, and the number of negative rows (clock
 * skew markers, excluded from percentiles).
 *
 * <p>Mode "intents": scans the Execution_Intent LOG and reports total rows,
 * distinct candidate_ids, and duplicate intent rows (same candidate twice).
 * Read-only in both modes.
 *
 * <p>Mode "orphans": prints the candidate_ids present in the intent table but
 * absent from the signal LOG table (phantom-intent check). Both tables are
 * required arguments: comparing a table against itself always yields zero
 * orphans, so a defaulted pair would report a false all-clear every time
 * (P6-003, P6-090). An unknown mode is an input error, never a silent
 * fall-through to "signals" (P6-732).
 *
 * <p>This is a dev census tool, not a production reader: it retains ids in
 * memory, so it refuses rather than OOMs past {@code MAX_RETAINED} entries
 * (P6-087, P6-089). A poll timeout is not treated as end-of-bucket (P6-088).
 *
 * <p>Wave 35x — how rows are counted, and why:
 *
 * <ul>
 *   <li><b>Reads are paged and offset-based for LOG tables</b> ({@code LogScanner}), so they are
 *       uncapped by construction. The batch path is not used for LOG tables any more: a batch
 *       scan of a bucket returns its first stored SEGMENT, not the bucket
 *       ({@code Replica.limitLogScan} → {@code LogTablet.read} → {@code LocalLog.read} exits as
 *       soon as one segment yields data), and {@code LimitBatchScanner.pollBatch} sets
 *       end-of-input after a single RPC. Measured on dev: 25 rows by batch, 41 by paged read, 48
 *       by the server's own count.
 *   <li><b>Each read is checked against the server's own row count</b>
 *       ({@code Admin.getTableStats}), and a disagreement fails loudly (exit 1) with no census
 *       printed. The server count is the only figure that stays right as retention deletes rows
 *       and as tiering moves segments out; a client read that cannot open a tiered segment logs
 *       the failure and then SKIPS it (P6-379's saturation guard could never fire — the read never
 *       reaches the cap).
 *   <li><b>No single number is trusted twice:</b> a mismatch is reported as a mismatch, never
 *       adjusted, and the census is withheld rather than printed short.
 * </ul>
 *
 * <p>Usage: FlussSignalLatency signals &lt;table&gt; [rule] [bootstrap]
 *                    | intents &lt;table&gt; [bootstrap]
 *                    | orphans &lt;intent_table&gt; &lt;signal_table&gt; [bootstrap]
 */
public class FlussSignalLatency {
    private static final String USAGE =
            "usage: FlussSignalLatency signals <table> [rule] [bootstrap]"
            + " | intents <table> [bootstrap]"
            + " | orphans <intent_table> <signal_table> [bootstrap]";
    private static final String DEFAULT_RULE = "n7-range-breakout-v1";
    private static final String DEFAULT_BOOTSTRAP = "localhost:9123";
    /** P6-734: how many orphan ids are listed before the count is summarised. */
    private static final int ORPHANS_SHOWN = 20;

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("missing mode. " + USAGE);
        }
        String mode = args[0];
        if (!"signals".equals(mode) && !"intents".equals(mode) && !"orphans".equals(mode)) {
            throw new IllegalArgumentException(
                    "unknown mode '" + mode + "'. " + USAGE);
        }
        Configuration conf = new Configuration();
        if ("orphans".equals(mode)) {
            // Both tables are explicit: a single-table run compares a table
            // against itself and always prints orphan_intents=0.
            if (args.length < 3) {
                throw new IllegalArgumentException(
                        "orphans needs both tables. " + USAGE);
            }
            String intentTable = args[1];
            String signalTable = args[2];
            if (intentTable.equals(signalTable)) {
                throw new IllegalArgumentException("orphans: intent and signal table are both '"
                        + intentTable + "' — a self-comparison always reports zero orphans");
            }
            conf.setString("bootstrap.servers", arg(args, 3, DEFAULT_BOOTSTRAP));
            try (Connection conn = ConnectionFactory.createConnection(conf);
                 Admin admin = conn.getAdmin()) {
                TablePath intentPath = TablePath.of("default", intentTable);
                TableInfo intentInfo = admin.getTableInfo(intentPath).get();
                TablePath signalPath = TablePath.of("default", signalTable);
                TableInfo signalInfo = admin.getTableInfo(signalPath).get();
                reportOrphans(conn, admin, intentInfo, intentPath, signalInfo, signalPath);
            }
            return;
        }

        String tableName = arg(args, 1,
                "intents".equals(mode) ? "Execution_Intent" : "Signal_Candidates");
        conf.setString("bootstrap.servers",
                arg(args, "intents".equals(mode) ? 2 : 3, DEFAULT_BOOTSTRAP));
        try (Connection conn = ConnectionFactory.createConnection(conf);
             Admin admin = conn.getAdmin()) {
            TablePath path = TablePath.of("default", tableName);
            TableInfo info = admin.getTableInfo(path).get();
            Schema schema = info.getSchema();
            if ("intents".equals(mode)) {
                reportIntents(conn, admin, info, path, schema);
            } else {
                reportSignals(conn, admin, info, path, schema,
                        arg(args, 2, DEFAULT_RULE));
            }
        }
    }

    /** Positional arg with a default; keeps each mode's arity explicit. */
    private static String arg(String[] args, int i, String fallback) {
        return args.length > i ? args[i] : fallback;
    }

    private static int col(Schema schema, String name) {
        int i = schema.getColumnNames().indexOf(name);
        if (i < 0) {
            throw new IllegalStateException("no " + name + " column");
        }
        return i;
    }

    /** Consumes one row of a bucket scan. */
    private interface RowSink {
        void accept(InternalRow row);
    }

    /**
     * Row cap for ONE bucket of a KV snapshot. {@code createBatchScanner}
     * rejects a null limit, and the limit scan returns the first stored
     * segment of a bucket rather than the bucket, so this cap applies ONLY to
     * KV tables — where a snapshot read is one RPC. Reaching it is a hard
     * failure, not a quiet partial count, and it is overridable so the failure
     * is reachable in dev.
     */
    private static final int BUCKET_ROW_LIMIT = bucketRowLimit();
    /**
     * P6-088: consecutive empty polls that declare the read drained.
     * {@code poll} is time-bounded, so a single empty poll is not proof of
     * exhaustion — and for a LOG read it is not proof that the offset advanced.
     */
    private static final int EMPTY_POLLS_TO_DRAIN = 3;
    private static final Duration FIRST_POLL_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration DRAIN_POLL_TIMEOUT = Duration.ofMillis(500);

    /**
     * P6-087/P6-089: memory is bounded, and exceeding the bound is an explicit
     * failure rather than an OOM kill. An exact census needs a retained set per
     * distinct id (and a retained latency per kept row), so it cannot be made
     * both exact and unbounded on a multi-billion-row LOG table. A killed JVM
     * loses the whole answer silently; this fails with a message that names the
     * limit and the way out (aggregate in Flink instead).
     */
    private static final int MAX_RETAINED = 5_000_000;

    private static void checkRetained(int size, String what) {
        if (size > MAX_RETAINED) {
            throw new IllegalStateException(what + " exceeded " + MAX_RETAINED
                    + " retained entries — this census cannot run in one JVM's heap;"
                    + " aggregate the table in Flink/Fluss SQL instead");
        }
    }

    /**
     * The per-bucket cap is overridable so the saturation failure is reachable
     * in dev instead of only at a scale nobody runs.
     */
    private static int bucketRowLimit() {
        int v = Integer.getInteger("fluss.probe.bucket.row.limit", 10_000_000);
        if (v < 1) {
            throw new IllegalArgumentException(
                    "fluss.probe.bucket.row.limit must be >= 1, got " + v);
        }
        return v;
    }

    /**
     * Reads every row of {@code table} into {@code sink}, and returns the number read.
     *
     * <p>LOG tables are read with the offset-paged {@code LogScanner}: offsets advance
     * monotonically, so the read is uncapped and resumable. The batch path is NOT used for
     * LOG tables — a batch scan of a bucket returns the bucket's first stored SEGMENT, and
     * {@code LimitBatchScanner.pollBatch} reports end-of-input after a single RPC.
     *
     * <p>KV tables are read as a per-bucket snapshot, because a KV log is a changelog
     * carrying UPDATE_BEFORE/UPDATE_AFTER and DELETE records whose count is not the row count.
     *
     * <p>A partitioned table is refused: {@code subscribe(bucket, offset)} reads only the
     * default partition, so the census would cover a fraction of the rows silently.
     */
    private static long readAll(Table table, TableInfo info, RowSink sink) throws Exception {
        return info.hasPrimaryKey() ? readKvSnapshot(table, info, sink) : readLog(table, info, sink);
    }

    private static long readLog(Table table, TableInfo info, RowSink sink) throws Exception {
        if (info.isPartitioned()) {
            throw new IllegalStateException("cannot census partitioned table "
                    + info.getTablePath() + " — subscribe per partition is not implemented");
        }
        long read = 0;
        try (LogScanner scanner = table.newScan().createLogScanner()) {
            for (int b = 0; b < info.getNumBuckets(); b++) {
                scanner.subscribeFromBeginning(b);
            }
            int emptyPolls = 0;
            while (true) {
                ScanRecords records = scanner.poll(FIRST_POLL_TIMEOUT);
                if (records == null || records.isEmpty()) {
                    // The scanner keeps its offsets, so a later poll resumes; only
                    // consecutive empties prove the current end was reached.
                    if (++emptyPolls >= EMPTY_POLLS_TO_DRAIN) {
                        break;
                    }
                    continue;
                }
                emptyPolls = 0;
                for (ScanRecord rec : records) {
                    InternalRow row = rec.getRow();
                    if (row != null) {
                        read++;
                        sink.accept(row);
                    }
                }
            }
        }
        return read;
    }

    private static long readKvSnapshot(Table table, TableInfo info, RowSink sink) throws Exception {
        long total = 0;
        for (int b = 0; b < info.getNumBuckets(); b++) {
            TableBucket tb = new TableBucket(info.getTableId(), b);
            long read = 0;
            try (BatchScanner scanner = table.newScan()
                         .limit(BUCKET_ROW_LIMIT)
                         .createBatchScanner(tb)) {
                int emptyPolls = 0;
                while (true) {
                    Duration timeout = emptyPolls == 0 ? FIRST_POLL_TIMEOUT : DRAIN_POLL_TIMEOUT;
                    try (CloseableIterator<InternalRow> batch = scanner.pollBatch(timeout)) {
                        if (batch == null) {
                            break; // end of input, per the BatchScanner contract
                        }
                        boolean any = false;
                        while (batch.hasNext()) {
                            InternalRow row = batch.next();
                            any = true;
                            read++;
                            sink.accept(row);
                        }
                        if (!any && ++emptyPolls >= EMPTY_POLLS_TO_DRAIN) {
                            break;
                        }
                        if (any) {
                            emptyPolls = 0;
                        }
                    }
                }
            }
            if (read >= BUCKET_ROW_LIMIT) {
                throw new IllegalStateException("bucket " + b + " of " + info.getTablePath()
                        + " reached the " + BUCKET_ROW_LIMIT + "-row snapshot cap — the census"
                        + " would under-report; raise it with"
                        + " -Dfluss.probe.bucket.row.limit=<n> or aggregate in Flink SQL");
            }
            total += read;
        }
        return total;
    }

    /**
     * The number of rows Fluss itself reports for the table.
     *
     * <p>The server's count ({@code Replica.getRowCount}: a KV tablet's row count, or for a LOG
     * table {@code highWatermark - logStartOffset}) is the only figure that stays correct as
     * retention deletes rows and as tiering moves segments out of local storage.
     */
    private static long tableRowCount(Admin admin, TablePath path) throws Exception {
        try {
            TableStats stats =
                    admin.getTableStats(path).get(60, java.util.concurrent.TimeUnit.SECONDS);
            return stats.getRowCount();
        } catch (java.util.concurrent.ExecutionException e) {
            // Silently skipping the statistic would disable the check that makes
            // the numbers below trustworthy, so it fails the census instead.
            throw new IllegalStateException("could not read the server's row count for " + path
                    + " — this census compares its read against that count, so it does not run"
                    + " without one: " + e.getCause(), e);
        }
    }

    /**
     * Reconcile a read against the server's count, refusing to print a census that disagrees.
     *
     * <p>The dangerous direction is a SHORT read: rows are missing, and the usual cause is
     * segments this client cannot open (it logs the failure and then skips them), which is
     * exactly what a partial census used to hide.
     */
    private static void checkAgainstServer(long read, long expected, TableInfo info) {
        if (read == expected) {
            return;
        }
        String kind = info.hasPrimaryKey() ? "KV snapshot" : "paged log read";
        String common = "census read " + read + " rows but Fluss reports " + expected + " — the "
                + kind + " of " + info.getTablePath() + " and the server's own row count disagree,"
                + " so this total is not trustworthy and is not printed.";
        if (read < expected) {
            throw new IllegalStateException(common + " The read is SHORT: rows are missing. Check"
                    + " that this client can read tiered segments — when it cannot, the client"
                    + " logs the failure and then SKIPS those rows, and only this comparison makes"
                    + " that visible.");
        }
        throw new IllegalStateException(common + " The read is LONG: rows were counted twice or"
                + " appended while the census ran. Re-run against a quiescent table.");
    }

    private static void reportSignals(Connection conn, Admin admin, TableInfo info,
            TablePath path, Schema schema, String rule) throws Exception {
        int ruleIdx = col(schema, "rule_id");
        int detIdx = col(schema, "detection_ts");
        int evalIdx = col(schema, "evaluation_ts");
        List<Long> lat = new ArrayList<>();
        long[] counters = new long[3]; // 0=kept, 1=negatives, 2=null-timestamp rows
        long expected = tableRowCount(admin, path);
        long rows;
        try (Table table = conn.getTable(path)) {
            rows = readAll(table, info, row -> {
                // Wave 34's refactor dropped this `continue` while converting the
                // loop to a lambda, so every row counted as a match for the
                // requested rule. A rule that matched nothing still reported
                // kept=rows and a full latency population (measured: asking for
                // a non-existent rule returned kept=25 of 25 rows).
                if (row.isNullAt(ruleIdx)
                        || !rule.equals(row.getString(ruleIdx).toString())) {
                    return;
                }
                counters[0]++;
                // Likewise: without this, null timestamps were subtracted as 0
                // and entered the population as real measurements.
                if (row.isNullAt(detIdx) || row.isNullAt(evalIdx)) {
                    counters[2]++;
                    return;
                }
                long d = row.getLong(evalIdx) - row.getLong(detIdx);
                if (d < 0) {
                    counters[1]++;
                } else {
                    lat.add(d);
                    checkRetained(lat.size(), "latency samples");
                }
            });
        }
        checkAgainstServer(rows, expected, info);
        Collections.sort(lat);
        System.out.println("table=" + path.getTableName() + " rule=" + rule
                + " rows=" + rows + " kept=" + counters[0]);
        System.out.println("latency_ms n=" + lat.size() + " negatives=" + counters[1]
                + " null_ts=" + counters[2]
                + " min=" + pct(lat, 0) + " p50=" + pct(lat, 50)
                + " p95=" + pct(lat, 95) + " p99=" + pct(lat, 99)
                + " max=" + pct(lat, 100));
    }

    private static void reportIntents(Connection conn, Admin admin, TableInfo info,
            TablePath path, Schema schema) throws Exception {
        int candIdx = col(schema, "candidate_id");
        Set<String> seen = new HashSet<>();
        long[] counter = new long[1]; // dups
        long expected = tableRowCount(admin, path);
        long rows;
        try (Table table = conn.getTable(path)) {
            rows = readAll(table, info, row -> {
                String c = row.isNullAt(candIdx)
                        ? "<null>" : row.getString(candIdx).toString();
                if (!seen.add(c)) {
                    counter[0]++;
                }
                checkRetained(seen.size(), "distinct candidate ids");
            });
        }
        checkAgainstServer(rows, expected, info);
        System.out.println("table=" + path.getTableName() + " rows=" + rows
                + " distinct_candidates=" + seen.size() + " duplicate_rows=" + counter[0]);
    }

    private static void reportOrphans(Connection conn, Admin admin, TableInfo intentInfo,
            TablePath intentPath, TableInfo signalInfo, TablePath signalPath) throws Exception {
        Schema intentSchema = intentInfo.getSchema();
        int candIdx = col(intentSchema, "candidate_id");
        long intentExpected = tableRowCount(admin, intentPath);
        Set<String> intentIds = new HashSet<>();
        long intentRows;
        try (Table table = conn.getTable(intentPath)) {
            intentRows = readAll(table, intentInfo, row -> {
                if (!row.isNullAt(candIdx)) {
                    intentIds.add(row.getString(candIdx).toString());
                    checkRetained(intentIds.size(), "intent candidate ids");
                }
            });
        }
        checkAgainstServer(intentRows, intentExpected, intentInfo);
        // The signal table is the caller's, not a hardcoded default: the old
        // literal compared Signal_Candidates against itself (P6-090).
        int sigCandIdx = col(signalInfo.getSchema(), "candidate_id");
        long signalExpected = tableRowCount(admin, signalPath);
        Set<String> signalIds = new HashSet<>();
        long signalRows;
        try (Table table = conn.getTable(signalPath)) {
            signalRows = readAll(table, signalInfo, row -> {
                if (!row.isNullAt(sigCandIdx)) {
                    signalIds.add(row.getString(sigCandIdx).toString());
                    checkRetained(signalIds.size(), "signal candidate ids");
                }
            });
        }
        checkAgainstServer(signalRows, signalExpected, signalInfo);
        // Snapshot the intent cardinality before the join destroys it.
        int intentCount = intentIds.size();
        intentIds.removeAll(signalIds);
        // Name both sides: a bare count cannot be told apart from a
        // self-comparison's guaranteed zero.
        System.out.println("intent_table=" + intentPath.getTableName()
                + " signal_table=" + signalPath.getTableName()
                + " intent_candidates=" + intentCount
                + " signal_candidates=" + signalIds.size()
                + " orphan_intents=" + intentIds.size());
        // P6-734: only the lowest 20 are printed, so keep a bounded max-heap
        // instead of sorting every orphan (O(N) instead of O(N log N)).
        java.util.PriorityQueue<String> lowest = new java.util.PriorityQueue<>(
                java.util.Collections.reverseOrder());
        for (String id : intentIds) {
            lowest.add(id);
            if (lowest.size() > ORPHANS_SHOWN) {
                lowest.poll();
            }
        }
        java.util.List<String> sorted = new java.util.ArrayList<>(lowest);
        java.util.Collections.sort(sorted);
        for (String id : sorted) {
            System.out.println("orphan=" + id);
        }
        if (intentIds.size() > sorted.size()) {
            // Never let a truncated list read as the complete set.
            System.out.println("orphan_truncated=" + (intentIds.size() - sorted.size()));
        }
    }

    private static long pct(List<Long> sorted, int p) {
        if (sorted.isEmpty()) {
            return -1;
        }
        if (p <= 0) {
            return sorted.get(0);
        }
        if (p >= 100) {
            return sorted.get(sorted.size() - 1);
        }
        return sorted.get((int) Math.ceil(p / 100.0 * sorted.size()) - 1);
    }
}
