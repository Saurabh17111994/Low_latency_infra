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
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
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
 * (P6-087, P6-089). A poll timeout is not treated as end-of-bucket, and a
 * saturated per-bucket {@code limit} fails loudly instead of under-reporting
 * (P6-088, P6-379).
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
                reportOrphans(conn, intentInfo, intentPath, signalInfo, signalPath);
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
                reportIntents(conn, info, path, schema);
            } else {
                reportSignals(conn, info, path, schema,
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

    private static final int BUCKET_ROW_LIMIT = 1_000_000_000;
    /**
     * P6-088: consecutive empty polls that declare a bucket drained.
     * {@code pollBatch} is time-bounded and its timeout returns an EMPTY
     * iterator (verified in LimitBatchScanner), not the null that means
     * end-of-input; a single empty poll is therefore not proof of exhaustion.
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
     * Scans one bucket, feeding every row to {@code sink}.
     *
     * <p>Two defects lived in each of the four hand-rolled loops this replaces:
     * a timeout was treated as end-of-bucket, silently truncating the census
     * (P6-088), and a saturated {@code .limit()} was invisible, so a truncated
     * census still printed a confident total (P6-379). Both now surface here
     * once instead of in four places.
     *
     * @return the number of rows read from this bucket
     */
    private static long drainBucket(Table table, TableInfo info, int bucket, RowSink sink)
            throws Exception {
        long read = 0;
        TableBucket tb = new TableBucket(info.getTableId(), bucket);
        // The cap cannot simply be dropped: createBatchScanner rejects a null
        // limit. Keep it and detect saturation instead.
        try (BatchScanner scanner = table.newScan()
                     .limit(BUCKET_ROW_LIMIT)
                     .createBatchScanner(tb)) {
            int emptyPolls = 0;
            while (true) {
                // The first poll waits for data; the drain-confirmation polls
                // only need to prove emptiness, so they use a short window
                // rather than adding minutes to a multi-bucket census.
                Duration timeout = emptyPolls == 0 ? FIRST_POLL_TIMEOUT : DRAIN_POLL_TIMEOUT;
                try (CloseableIterator<InternalRow> batch = scanner.pollBatch(timeout)) {
                    if (batch == null) {
                        break; // end of input, confirmed by the API contract
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
            // Fail loudly: a census that describes itself as complete must not
            // silently under-report because it hit the per-bucket cap.
            throw new IllegalStateException("bucket " + bucket + " of "
                    + info.getTablePath() + " reached the " + BUCKET_ROW_LIMIT
                    + "-row per-bucket cap — counts below would be truncated");
        }
        return read;
    }

    private static void reportSignals(Connection conn, TableInfo info, TablePath path,
            Schema schema, String rule) throws Exception {
        int ruleIdx = col(schema, "rule_id");
        int detIdx = col(schema, "detection_ts");
        int evalIdx = col(schema, "evaluation_ts");
        List<Long> lat = new ArrayList<>();
        long rows = 0;
        long kept = 0;
        long negatives = 0;
        try (Table table = conn.getTable(path)) {
            for (int b = 0; b < info.getNumBuckets(); b++) {
                long[] counters = new long[2]; // 0=kept, 1=negatives
                rows += drainBucket(table, info, b, row -> {
                    if (row.isNullAt(ruleIdx)
                            || !rule.equals(row.getString(ruleIdx).toString())) {
                        }
                    counters[0]++;
                    if (row.isNullAt(detIdx) || row.isNullAt(evalIdx)) {
                        }
                    long d = row.getLong(evalIdx) - row.getLong(detIdx);
                    if (d < 0) {
                        counters[1]++;
                    } else {
                        lat.add(d);
                        checkRetained(lat.size(), "latency samples");
                    }
                });
                kept += counters[0];
                negatives += counters[1];
            }
        }
        Collections.sort(lat);
        System.out.println("table=" + path.getTableName() + " rule=" + rule
                + " rows=" + rows + " kept=" + kept);
        System.out.println("latency_ms n=" + lat.size() + " negatives=" + negatives
                + " min=" + pct(lat, 0) + " p50=" + pct(lat, 50)
                + " p95=" + pct(lat, 95) + " p99=" + pct(lat, 99)
                + " max=" + pct(lat, 100));
    }

    private static void reportIntents(Connection conn, TableInfo info, TablePath path,
            Schema schema) throws Exception {
        int candIdx = col(schema, "candidate_id");
        long rows = 0;
        Set<String> seen = new HashSet<>();
        long dups = 0;
        try (Table table = conn.getTable(path)) {
            for (int b = 0; b < info.getNumBuckets(); b++) {
                long[] counter = new long[1]; // dups
                rows += drainBucket(table, info, b, row -> {
                    String c = row.isNullAt(candIdx)
                            ? "<null>" : row.getString(candIdx).toString();
                    if (!seen.add(c)) {
                        counter[0]++;
                    }
                    checkRetained(seen.size(), "distinct candidate ids");
                });
                dups += counter[0];
            }
        }
        System.out.println("table=" + path.getTableName() + " rows=" + rows
                + " distinct_candidates=" + seen.size() + " duplicate_rows=" + dups);
    }

    private static void reportOrphans(Connection conn, TableInfo intentInfo, TablePath intentPath,
            TableInfo signalInfo, TablePath signalPath) throws Exception {
        Schema intentSchema = intentInfo.getSchema();
        int candIdx = col(intentSchema, "candidate_id");
        Set<String> intentIds = new HashSet<>();
        try (Table table = conn.getTable(intentPath)) {
            for (int b = 0; b < intentInfo.getNumBuckets(); b++) {
                drainBucket(table, intentInfo, b, row -> {
                    if (!row.isNullAt(candIdx)) {
                        intentIds.add(row.getString(candIdx).toString());
                        checkRetained(intentIds.size(), "intent candidate ids");
                    }
                });
            }
        }
        // The signal table is the caller's, not a hardcoded default: the old
        // literal compared Signal_Candidates against itself (P6-090).
        int sigCandIdx = col(signalInfo.getSchema(), "candidate_id");
        Set<String> signalIds = new HashSet<>();
        try (Table table = conn.getTable(signalPath)) {
            for (int b = 0; b < signalInfo.getNumBuckets(); b++) {
                drainBucket(table, signalInfo, b, row -> {
                    if (!row.isNullAt(sigCandIdx)) {
                        signalIds.add(row.getString(sigCandIdx).toString());
                        checkRetained(signalIds.size(), "signal candidate ids");
                    }
                });
            }
        }
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
