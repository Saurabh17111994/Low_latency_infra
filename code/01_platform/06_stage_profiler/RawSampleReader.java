import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.admin.OffsetSpec.LatestSpec;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeFamily;
import org.apache.fluss.types.RowType;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * RawSampleReader — bounded tail sample of the raw LOG table (stage profiler, S1).
 *
 * <p>WHY this file exists: FlussPrefixReader's LOG path calls
 * {@code subscribeFromBeginning(bucket)}, which Fluss 1.0 rejects for a partitioned
 * table ({@code The table is a partitioned table, please use "subscribe(long
 * partitionId, int bucket, long offset)"}). raw_table_1 is event_day-partitioned,
 * so the S1 sample could not be read with the existing probe (observed live
 * 2026-09-26: {@code __ERROR__ ... partitioned table ...}). This reader mirrors the
 * partition-aware read that holistic-analyze.py's embedded LogFullRead already uses:
 * list partitions, list per-bucket log-end offsets, subscribe at
 * {@code (end - rowsPerBucket)} and read a bounded tail.
 *
 * <p>Emits one JSON object per sampled row of the requested tokens:
 * {@code {"instrument_token":<long>,"event_time":<long>,"ingest_ts":<long>}} then the
 * sentinel line {@code __END__ <count>}. The stage profiler's S1 is the percentiles of
 * {@code ingest_ts - event_time} over those rows.
 *
 * <p>Contract:
 * <ul>
 *   <li>READ-ONLY: no writes, no admin mutations.</li>
 *   <li>BOUNDED: at most rowsPerBucket rows per (partition, bucket) plus a hard 60s
 *       deadline; the token filter is applied client-side (a log scan cannot filter
 *       on a non-key column).</li>
 *   <li>the {@code __END__ <count>} sentinel is printed on every exit path; failures
 *       print {@code __ERROR__ <cause>} on stderr, {@code __END__ 0} on stdout and
 *       exit non-zero, so a caller blocking on the sentinel cannot hang.</li>
 *   <li>stdout is flushed and the JVM exits hard after {@code __END__} — the Fluss
 *       client's scanner teardown can block on an idle subscription (same workaround
 *       as FlussPrefixReader, observed 2026-09-04).</li>
 * </ul>
 *
 * <p>Usage: {@code java -cp <cp> RawSampleReader <database> <table> <tokens_csv|*>
 * <rows_per_bucket> [bootstrap]}
 */
public class RawSampleReader {

    public static void main(String[] args) {
        try {
            run(args);
        } catch (Throwable t) {
            System.err.println("__ERROR__ " + t);
            System.out.println("__END__ 0");
            System.out.flush();
            System.exit(1);
        }
    }

    static void run(String[] args) throws Exception {
        String database = args.length > 0 ? args[0] : "default";
        String tableName = args.length > 1 ? args[1] : "raw_table_1";
        String tokensRaw = args.length > 2 ? args[2] : "*";
        int rowsPerBucket = args.length > 3 ? Math.max(1, Integer.parseInt(args[3])) : 300;
        String bootstrap = args.length > 4 ? args[4] : "localhost:9123";

        Set<Long> tokens = parseTokens(tokensRaw);
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        TablePath tablePath = TablePath.of(database, tableName);

        long emitted = 0;
        long deadlineMs = System.currentTimeMillis() + 60_000L;

        try (Connection connection = ConnectionFactory.createConnection(conf);
             Admin admin = connection.getAdmin();
             Table table = connection.getTable(tablePath)) {
            TableInfo info = table.getTableInfo();
            RowType rowType = info.getRowType();
            int tokenIdx = requireIntegerColumn(rowType, "instrument_token", tableName);
            int eventIdx = requireIntegerColumn(rowType, "event_time", tableName);
            int ingestIdx = requireIntegerColumn(rowType, "ingest_ts", tableName);
            int numBuckets = info.getNumBuckets();

            List<PartitionInfo> partitions = new ArrayList<>();
            if (info.isPartitioned()) {
                List<PartitionInfo> found =
                        admin.listPartitionInfos(tablePath).get(5, TimeUnit.SECONDS);
                if (found != null) {
                    partitions.addAll(found);
                }
                if (partitions.isEmpty()) {
                    throw new IllegalStateException(
                            "partitioned table has no partitions: " + tablePath);
                }
            } else {
                partitions.add(null); // non-partitioned: subscribe(bucket, offset)
            }

            System.err.println("RawSampleReader: " + tableName
                    + " partitions=" + partitions.size()
                    + " buckets/partition=" + numBuckets
                    + " rowsPerBucket=" + rowsPerBucket
                    + " tokens=" + (tokens.isEmpty() ? "*" : String.valueOf(tokens.size())));

            for (PartitionInfo partition : partitions) {
                if (System.currentTimeMillis() > deadlineMs) {
                    System.err.println("WARN: 60s deadline hit — sample truncated");
                    break;
                }
                Map<Integer, Long> ends =
                        partition == null ? null : latestOffsets(admin, tablePath, partition, numBuckets);
                for (int bucket = 0; bucket < numBuckets; bucket++) {
                    if (System.currentTimeMillis() > deadlineMs) {
                        break;
                    }
                    long end = ends == null ? -1L : ends.getOrDefault(bucket, -1L);
                    long start = end >= 0 ? Math.max(0L, end - rowsPerBucket) : 0L;
                    long budget = end >= 0 ? Math.max(0L, end - start) : rowsPerBucket;
                    if (budget == 0L) {
                        continue;
                    }
                    emitted += readBucketTail(table, partition, bucket, start, budget,
                            tokens, tokenIdx, eventIdx, ingestIdx, deadlineMs);
                }
            }
        }
        System.out.println("__END__ " + emitted);
        System.out.flush();
        System.exit(0);
    }

    /** One bounded tail read per (partition, bucket): at most {@code budget} rows. */
    private static long readBucketTail(Table table, PartitionInfo partition, int bucket,
                                       long start, long budget, Set<Long> tokens,
                                       int tokenIdx, int eventIdx, int ingestIdx, long deadlineMs)
            throws Exception {
        long emitted = 0;
        long read = 0;
        int emptyPolls = 0;
        try (LogScanner scanner = table.newScan().createLogScanner()) {
            if (partition != null) {
                scanner.subscribe(partition.getPartitionId(), bucket, start);
            } else {
                scanner.subscribe(bucket, start);
            }
            while (read < budget && emptyPolls < 2 && System.currentTimeMillis() < deadlineMs) {
                ScanRecords records = scanner.poll(Duration.ofSeconds(2));
                if (records == null || records.isEmpty()) {
                    emptyPolls++;
                    Thread.sleep(200);
                    continue;
                }
                emptyPolls = 0;
                for (ScanRecord rec : records) {
                    InternalRow row = rec.getRow();
                    if (row == null) {
                        continue;
                    }
                    read++;
                    long token = row.getLong(tokenIdx);
                    if (!tokens.isEmpty() && !tokens.contains(token)) {
                        continue;
                    }
                    System.out.println("{\"instrument_token\":" + token
                            + ",\"event_time\":" + row.getLong(eventIdx)
                            + ",\"ingest_ts\":" + row.getLong(ingestIdx) + "}");
                    emitted++;
                }
            }
        }
        return emitted;
    }

    /** Log-end offset of every bucket of one partition (one RPC, all buckets). */
    private static Map<Integer, Long> latestOffsets(Admin admin, TablePath tablePath,
                                                    PartitionInfo partition, int numBuckets)
            throws Exception {
        List<Integer> bucketIds = new ArrayList<>(numBuckets);
        for (int b = 0; b < numBuckets; b++) {
            bucketIds.add(b);
        }
        ListOffsetsResult pending = admin.listOffsets(
                tablePath, partition.getPartitionName(), bucketIds, new LatestSpec());
        return pending.all().get(5, TimeUnit.SECONDS);
    }

    private static int requireIntegerColumn(RowType rowType, String column, String tableName) {
        int idx = rowType.getFieldIndex(column);
        if (idx < 0) {
            throw new IllegalStateException("table " + tableName + " has no '" + column
                    + "' column; found " + rowType.getFieldNames());
        }
        DataType type = rowType.getTypeAt(idx);
        if (!type.is(DataTypeFamily.INTEGER_NUMERIC)) {
            throw new IllegalStateException("table " + tableName + " column '" + column
                    + "' must be an integer type, got " + type);
        }
        return idx;
    }

    /** Comma-separated tokens; {@code *}/empty means "every row". Unparseable entries skipped. */
    private static Set<Long> parseTokens(String raw) {
        Set<Long> out = new LinkedHashSet<>();
        if (raw == null || raw.trim().isEmpty() || "*".equals(raw.trim())) {
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
                System.err.println("RawSampleReader: skipping bad token '" + s + "'");
            }
        }
        return out;
    }
}
