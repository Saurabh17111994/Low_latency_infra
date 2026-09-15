import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;
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
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.utils.CloseableIterator;

/**
 * Full-bucket rule-id census for a signal table (strategy-host smoke,
 * 2026-09-05). Prints total rows + per-rule_id counts. Read-only.
 *
 * <p>Contract (wave 35x — see CHG-178):
 *
 * <ul>
 *   <li><b>The row total comes from Fluss, not from this probe.</b> {@code Admin.getTableStats}
 *       returns the server's own row count for the table, which subtracts rows that retention has
 *       removed and counts rows that were moved to tiered (remote) storage. Counting rows by
 *       reading them cannot do either: a reader that cannot open tiered segments loses those rows
 *       silently, so its total is a lower bound, never a truth.
 *   <li><b>The per-rule breakdown comes from a real read,</b> and the read is checked against the
 *       server's total before anything is printed. A mismatch fails loudly
 *       ({@code IllegalStateException}, exit 1) instead of printing a partial census — that is the
 *       whole point of the pairing.
 *   <li><b>A LOG table (no primary key) is read with the offset-paged {@code LogScanner},</b>
 *       which advances by offset, so the read is genuinely uncapped and wave 13's removed
 *       {@code .limit(1_000_000_000)} cannot truncate it. ({@code createBatchScanner} rejects a
 *       null limit, and the limit scan returns the first stored SEGMENT of a bucket rather than
 *       the table — that is why the batch path is not used for LOG tables.)
 *   <li><b>A KV table (primary key) is read as a snapshot through {@code BatchScanner},</b>
 *       because a KV table's log is a changelog carrying UPDATE_BEFORE/UPDATE_AFTER and DELETE
 *       records; counting those would double-count rows. The snapshot is capped at
 *       {@code BUCKET_ROW_LIMIT}, and reaching that cap FAILS LOUDLY rather than printing a
 *       partial census. The cap is overridable with {@code -Dfluss.probe.bucket.row.limit=N} so
 *       the failure is provable in dev.
 *   <li><b>A bucket is exhausted after THREE consecutive empty polls, not one</b> (P6-377):
 *       {@code pollBatch} is time-bounded, and a single contended/timeout poll would end the
 *       bucket early and undercount.
 *   <li><b>{@code rule_id} must be a STRING column</b> before any row is decoded, so a custom
 *       table with another type fails with a clear message instead of a decode error in the
 *       middle of the census (P6-378).
 * </ul>
 *
 * <p>Known limitation: comparing a read against the server's total detects an under-count, but a
 * table that changes while the census runs (rows appended between the two calls) is reported as a
 * mismatch rather than guessed at. Run it against a quiescent table.
 *
 * <p>Exit codes: 0 census printed, 1 runtime failure, 2 unusable input.
 *
 * <p>Usage: java -cp &lt;cp&gt; FlussRuleCounter [table] [bootstrap]
 */
public class FlussRuleCounter {
    /** P6-377: consecutive empty polls that declare a bucket drained. */
    static final int EMPTY_POLLS_TO_DRAIN = 3;
    /**
     * Row cap for ONE bucket of a KV snapshot. The cap exists because
     * {@code createBatchScanner} rejects a null limit (TableScan.java:125-131) —
     * dropping it does not uncap the census, it makes every scan throw. Reaching
     * the cap is therefore a hard failure, not a quiet partial count.
     */
    static final int BUCKET_ROW_LIMIT = bucketRowLimit();
    static final Duration FIRST_POLL_TIMEOUT = Duration.ofMillis(5000);

    /**
     * The cap is overridable so the saturation failure can be provoked in dev
     * instead of only being reachable at a scale nobody runs.
     */
    private static int bucketRowLimit() {
        int v = Integer.getInteger("fluss.probe.bucket.row.limit", 10_000_000);
        if (v < 1) {
            throw new IllegalArgumentException(
                    "fluss.probe.bucket.row.limit must be >= 1, got " + v);
        }
        return v;
    }
    static final Duration DRAIN_POLL_TIMEOUT = Duration.ofMillis(500);

    static class InputException extends RuntimeException {
        InputException(String msg) {
            super(msg);
        }
    }

    public static void main(String[] args) {
        try {
            run(args);
        } catch (InputException e) {
            System.err.println("FlussRuleCounter: " + e);
            System.exit(2);
        } catch (Throwable t) {
            System.err.println("FlussRuleCounter failed: " + t);
            System.exit(1);
        }
        System.out.flush();
        System.exit(0);
    }

    static void run(String[] args) throws Exception {
        String tableName = args.length > 0 ? args[0] : "Signal_Candidates";
        String bootstrap = args.length > 1 ? args[1] : "localhost:9123";
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        Map<String, Long> byRule = new TreeMap<>();
        long total = 0;
        try (Connection conn = ConnectionFactory.createConnection(conf);
             Admin admin = conn.getAdmin()) {
            TablePath path = TablePath.of("default", tableName);
            TableInfo info = admin.getTableInfo(path).get();
            Schema schema = info.getSchema();
            int ruleIdx = -1;
            for (int i = 0; i < schema.getColumnNames().size(); i++) {
                if (schema.getColumnNames().get(i).equals("rule_id")) {
                    ruleIdx = i;
                }
            }
            if (ruleIdx < 0) {
                throw new IllegalStateException("no rule_id column in " + tableName);
            }
            // P6-378: rule_id is STRING in the DDL, but this probe takes an arbitrary
            // table name, and getString() on another root throws mid-census.
            DataType ruleType = schema.getColumns().get(ruleIdx).getDataType();
            if (ruleType.getTypeRoot() != DataTypeRoot.STRING) {
                throw new InputException("rule_id column in " + tableName + " is " + ruleType
                        + ", expected STRING");
            }
            // The server's own row count is the expected size of the read, for BOTH
            // table kinds. It already accounts for retention (rows deleted) and for
            // rows moved to tiered storage, neither of which a client-side read can
            // know: a reader that cannot open a tiered segment logs the failure and
            // then skips it, so only the comparison can turn a partial read into a
            // visible failure.
            long expected = tableRowCount(admin, path);
            try (Table table = conn.getTable(path)) {
                if (info.hasPrimaryKey()) {
                    total = countKvSnapshot(table, info, ruleIdx, byRule);
                } else {
                    total = countLogByOffset(table, info, ruleIdx, byRule);
                }
            }
            if (total != expected) {
                throw new IllegalStateException(describeMismatch(total, expected, info));
            }
        }
        System.out.println("table=" + tableName + " total=" + total);
        byRule.forEach((rule, count) -> System.out.println("rule=" + rule + " count=" + count));
    }

    /**
     * LOG tables: read by offset with the paged {@code LogScanner}.
     *
     * <p>A LOG table is append-only, so offsets advance monotonically and the
     * scanner pages on its own — the census is uncapped without any row limit,
     * which is what P6-376 asked for and what the batch scanner cannot provide.
     */
    private static long countLogByOffset(Table table, TableInfo info, int ruleIdx,
                                         Map<String, Long> byRule) throws Exception {
        long total = 0;
        if (info.isPartitioned()) {
            // A partitioned table's buckets live per partition, and the plain
            // subscribe(bucket, offset) overload reads only the default partition:
            // it would silently census a fraction of the rows.
            throw new InputException("cannot census partitioned table "
                    + info.getTablePath() + " — subscribe per partition is not implemented");
        }
        try (LogScanner scanner = table.newScan().createLogScanner()) {
            for (int b = 0; b < info.getNumBuckets(); b++) {
                scanner.subscribeFromBeginning(b);
            }
            int emptyPolls = 0;
            while (true) {
                ScanRecords records = scanner.poll(FIRST_POLL_TIMEOUT);
                if (records == null || records.isEmpty()) {
                    // Nothing more is available right now. The scanner keeps its
                    // offset, so a later poll resumes where this one stopped; only
                    // consecutive empties prove the current end has been reached.
                    if (++emptyPolls >= EMPTY_POLLS_TO_DRAIN) {
                        break;
                    }
                    continue;
                }
                emptyPolls = 0;
                for (ScanRecord rec : records) {
                    InternalRow row = rec.getRow();
                    if (row == null) {
                        continue;
                    }
                    total++;
                    byRule.merge(ruleOf(row, ruleIdx), 1L, Long::sum);
                }
            }
        }
        return total;
    }

    /**
     * KV tables: census the current state of every bucket.
     *
     * <p>Not read by offset: a KV table's log is a changelog, so offset-reading
     * would count UPDATE_BEFORE/UPDATE_AFTER pairs and deletions as extra rows.
     */
    private static long countKvSnapshot(Table table, TableInfo info,
                                        int ruleIdx, Map<String, Long> byRule) throws Exception {
        long total = 0;
        for (int b = 0; b < info.getNumBuckets(); b++) {
            TableBucket tb = new TableBucket(info.getTableId(), b);
            long read = 0;
            try (BatchScanner scanner = table.newScan()
                         .limit(BUCKET_ROW_LIMIT)
                         .createBatchScanner(tb)) {
                int emptyPolls = 0;
                while (true) {
                    Duration pollTimeout = emptyPolls == 0 ? FIRST_POLL_TIMEOUT : DRAIN_POLL_TIMEOUT;
                    try (CloseableIterator<InternalRow> batch = scanner.pollBatch(pollTimeout)) {
                        if (batch == null) {
                            break; // end of input, per the BatchScanner contract
                        }
                        boolean any = false;
                        while (batch.hasNext()) {
                            InternalRow row = batch.next();
                            any = true;
                            read++;
                            byRule.merge(ruleOf(row, ruleIdx), 1L, Long::sum);
                        }
                        if (!any && ++emptyPolls >= EMPTY_POLLS_TO_DRAIN) {
                            break;
                        }
                        if (any) {
                            emptyPolls = 0;
                        }
                    }
                    if (read >= BUCKET_ROW_LIMIT) {
                        throw new IllegalStateException("bucket " + b + " of "
                                + info.getTablePath() + " reached the " + BUCKET_ROW_LIMIT
                                + "-row snapshot cap — the census would under-report; raise it with"
                                + " -Dfluss.probe.bucket.row.limit=<n> or aggregate in Flink SQL");
                    }
                }
            }
            total += read;
        }
        return total;
    }

    /**
     * The number of rows Fluss itself reports for the table.
     *
     * <p>This is the server's count ({@code Replica.getRowCount}: a KV tablet's materialized row
     * count, or for a LOG table {@code highWatermark - logStartOffset}), not a client-side tally.
     * It is the only figure that stays correct as retention deletes rows and as segments move to
     * tiered storage.
     */
    private static long tableRowCount(Admin admin, TablePath path) throws Exception {
        try {
            TableStats stats = admin.getTableStats(path).get(60, java.util.concurrent.TimeUnit.SECONDS);
            return stats.getRowCount();
        } catch (java.util.concurrent.ExecutionException e) {
            // An unreadable statistic would silently disable the check, so it is
            // reported as a failure of the census rather than skipped.
            throw new IllegalStateException("could not read the server's row count for " + path
                    + " — this census compares its read against that count, so it does not run"
                    + " without one: " + e.getCause(), e);
        }
    }

    /**
     * Explain a read that disagrees with the server's row count, in the terms an operator needs.
     *
     * <p>An under-count is the dangerous direction: the read is missing rows, and the usual cause
     * is rows that live in tiered storage which this client cannot open (the client logs the
     * failure and skips them).
     */
    private static String describeMismatch(long total, long expected, TableInfo info) {
        String kind = info.hasPrimaryKey() ? "KV snapshot" : "paged log read";
        String common = "census read " + total + " rows but Fluss reports " + expected + " — the "
                + kind + " of " + info.getTablePath() + " and the server's own row count disagree,"
                + " so this total is not trustworthy and is not printed.";
        if (total < expected) {
            return common + " The read is SHORT: rows are missing. Check that this client can read"
                    + " tiered segments — when it cannot, the client logs the failure and then"
                    + " SKIPS those rows, and only this comparison makes that visible (for a"
                    + " container-local remote.data.dir, the path must be readable from wherever"
                    + " this probe runs).";
        }
        return common + " The read is LONG: rows were counted twice or were appended while the"
                + " census ran. Re-run against a quiescent table.";
    }

    private static String ruleOf(InternalRow row, int ruleIdx) {
        return row.isNullAt(ruleIdx) ? "<null>" : row.getString(ruleIdx).toString();
    }
}
