import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypeFamily;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

/**
 * ING-TCP-001 count-based losslessness: per-token row counts from the Fluss
 * sink side. raw_table_1 is lake-enabled LOG -> MUST use LogScanner
 * (subscribeFromBeginning; plain BatchScanner caps at the first segment end,
 * p3-2 finding). ingestion_quarantine is plain LOG -> BatchScanner is fine.
 *
 * Token column is resolved BY NAME from the live table schema (P6-116): the
 * index used to be hardcoded (raw_table_1 = 4), but schema v3 (db513ee0) added
 * event_day as column 0 and silently shifted instrument_token to 5. Index 4 is
 * connection_epoch — also BIGINT, so getLong() did not throw and the probe
 * counted the wrong column without any error.
 * Env: FLUSS_BOOTSTRAP, FLUSS_RAW_TABLE (default raw_table_1),
 *      FLUSS_QUARANTINE_TABLE (default ingestion_quarantine).
 * Output: per-token RAW= and QUAR= counts, then TOKEN TOTAL=, and a final
 * RECONCILE_TOTAL= (raw+quar) for comparison against the bridge's
 * arrow-tick-counts total.
 */
public final class TokenCountReconcile {
    /** Sentinels in the per-token map; real tokens are non-negative. */
    private static final long NULL_TOKEN_BUCKET = -1L;
    private static final long NULL_TOKEN_RAW_BUCKET = -2L;

    /** Column holding the instrument token, in both tables. */
    private static final String TOKEN_COLUMN = "instrument_token";

    /** Parse the scan loop's polling cadence and first-record budget. */
    private static final int POLL_INTERVAL_MS = 500;
    private static final int MAX_POLLS_BEFORE_FIRST_RECORD = 60; // 30s per bucket

    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("FLUSS_BOOTSTRAP", "localhost:9123");
        String rawTable = System.getenv().getOrDefault("FLUSS_RAW_TABLE", "raw_table_1");
        String quarTable = System.getenv().getOrDefault("FLUSS_QUARANTINE_TABLE", "ingestion_quarantine");
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            Map<Long, long[]> counts = new TreeMap<>();
            // P6-426/427: totals are indexed by the caller's slot, not by
            // hardcoded 0/1 inside the scan methods, so a slot change cannot
            // silently write the wrong total.
            long[] totals = new long[2];
            scanLog(connection, rawTable, counts, totals, 0);
            scanBatch(connection, quarTable, counts, totals, 1);
            for (Map.Entry<Long, long[]> e : counts.entrySet()) {
                System.out.printf("TOKEN %d RAW=%d QUAR=%d TOTAL=%d%n",
                        e.getKey(), e.getValue()[0], e.getValue()[1],
                        e.getValue()[0] + e.getValue()[1]);
            }
            System.out.println("RECONCILE_RAW_TOTAL=" + totals[0]);
            System.out.println("RECONCILE_QUAR_TOTAL=" + totals[1]);
            System.out.println("RECONCILE_TOTAL=" + (totals[0] + totals[1]));
            // P6-425: null tokens are counted in their own buckets instead of a
            // real token value, so they can never be attributed to token -1.
            reportNullTokens(counts);
        }
    }

    private static void reportNullTokens(Map<Long, long[]> counts) {
        long[] raw = counts.get(NULL_TOKEN_RAW_BUCKET);
        long[] quar = counts.get(NULL_TOKEN_BUCKET);
        long rawNulls = raw == null ? 0 : raw[0];
        long quarNulls = quar == null ? 0 : quar[1];
        if (rawNulls > 0 || quarNulls > 0) {
            System.out.println("NULL_TOKEN_RAW=" + rawNulls);
            System.out.println("NULL_TOKEN_QUAR=" + quarNulls);
        }
    }

    /**
     * Resolves the token column by name and verifies it is an integer type.
     * P6-116: a positional constant silently follows any schema change; a name
     * lookup fails loudly instead. The old index 4 still resolved (to
     * connection_epoch) because both columns are BIGINT, so this check must
     * validate the NAME, not just the type.
     */
    private static int resolveTokenIndex(RowType rowType, String tableName) {
        int idx = rowType.getFieldIndex(TOKEN_COLUMN);
        if (idx < 0) {
            throw new IllegalStateException(
                    "table " + tableName + " has no '" + TOKEN_COLUMN + "' column; found "
                            + rowType.getFieldNames());
        }
        var type = rowType.getTypeAt(idx);
        if (!type.is(DataTypeFamily.INTEGER_NUMERIC)) {
            throw new IllegalStateException(
                    "table " + tableName + " column '" + TOKEN_COLUMN + "' must be an integer type, got "
                            + type);
        }
        return idx;
    }

    /** Shared per-row accounting for both scan paths. */
    private static void countRow(InternalRow row, int tokenIdx, Map<Long, long[]> counts, int slot) {
        long token;
        if (row.isNullAt(tokenIdx)) {
            token = slot == 0 ? NULL_TOKEN_RAW_BUCKET : NULL_TOKEN_BUCKET;
        } else {
            token = row.getLong(tokenIdx);
        }
        long[] v = counts.computeIfAbsent(token, k -> new long[2]);
        v[slot]++;
    }

    /** LogScanner per-bucket full scan (lake-enabled LOG correctness). */
    private static void scanLog(Connection connection, String tableName,
                                Map<Long, long[]> counts, long[] totals, int slot)
            throws Exception {
        TablePath path = TablePath.of("default", tableName);
        Table table = connection.getTable(path);
        TableInfo info = connection.getAdmin().getTableInfo(path).join();
        int tokenIdx = resolveTokenIndex(info.getRowType(), tableName);
        long rows = 0;
        try (LogScanner scanner = table.newScan().createLogScanner()) {
            for (int b = 0; b < info.getNumBuckets(); b++) {
                scanner.subscribeFromBeginning(b);
                long bucketRows = 0;
                // P6-117: "3 empty polls" is not an end-of-stream signal — a
                // slow broker or a temporarily empty bucket ended the scan
                // early and undercounted. Only an empty poll AFTER records have
                // been seen for this bucket is treated as the end of the
                // retained log; a bucket that never yields anything is reported
                // and the run fails rather than silently reconciling short.
                // The wait for a first record stays BOUNDED (MAX_POLLS_BEFORE_
                // FIRST_RECORD), or a genuinely empty bucket would spin here
                // forever instead of failing.
                boolean sawRecords = false;
                int emptyPolls = 0;
                int pollsWithoutRecords = 0;
                while (emptyPolls < 3) {
                    ScanRecords recs = scanner.poll(Duration.ofMillis(500));
                    int c = recs.count();
                    if (c == 0) {
                        if (sawRecords) {
                            emptyPolls++;
                        } else if (++pollsWithoutRecords >= MAX_POLLS_BEFORE_FIRST_RECORD) {
                            throw new IllegalStateException(
                                    "bucket " + b + " of " + tableName + " produced no records within "
                                            + (MAX_POLLS_BEFORE_FIRST_RECORD * POLL_INTERVAL_MS)
                                            + "ms; refusing to report a partial count");
                        }
                        continue;
                    }
                    bucketRows += c;
                    sawRecords = true;
                    emptyPolls = 0;
                    for (ScanRecord rec : recs) {
                        countRow(rec.getRow(), tokenIdx, counts, slot);
                    }
                }
                scanner.unsubscribe(b);
                if (!sawRecords) {
                    throw new IllegalStateException(
                            "bucket " + b + " of " + tableName
                                    + " produced no records; refusing to report a partial count");
                }
                System.out.println("RAW_BUCKET_" + b + "=" + bucketRows);
                rows += bucketRows;
            }
        }
        totals[slot] = rows;
        System.out.println("RAW_TOTAL=" + rows);
    }

    /** BatchScanner per-bucket scan (plain LOG table). */
    private static void scanBatch(Connection connection, String tableName,
                                  Map<Long, long[]> counts, long[] totals, int slot)
            throws Exception {
        TablePath path = TablePath.of("default", tableName);
        Table table = connection.getTable(path);
        TableInfo info = connection.getAdmin().getTableInfo(path).join();
        int tokenIdx = resolveTokenIndex(info.getRowType(), tableName);
        long rows = 0;
        for (int b = 0; b < info.getNumBuckets(); b++) {
            TableBucket tb = new TableBucket(info.getTableId(), b);
            long bucketRows = 0;
            // P6-118: the scanner used to be capped at limit(1_000_000_000),
            // so a larger table reconciled short with no error. No limit is set
            // now: a full-table count is what this probe is for.
            try (BatchScanner scanner =
                         table.newScan().createBatchScanner(tb);
                 CloseableIterator<InternalRow> it = scanner.pollBatch(Duration.ofMillis(30_000))) {
                while (it.hasNext()) {
                    InternalRow row = it.next();
                    bucketRows++;
                    countRow(row, tokenIdx, counts, slot);
                }
            }
            System.out.println("QUAR_BUCKET_" + b + "=" + bucketRows);
            rows += bucketRows;
        }
        totals[slot] = rows;
        System.out.println("QUAR_TOTAL=" + rows);
    }
}
