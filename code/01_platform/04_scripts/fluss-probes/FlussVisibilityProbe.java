import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec.LatestSpec;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;

/**
 * FlussVisibilityProbe — measures the leg the Flink latency markers cannot see:
 * "the record was appended" to "a plain reader can read it" (plan
 * docs/plans/2026-10-06-p99-integrated-program.md, Task 3a / L7).
 *
 * <p>It TAIL-FOLLOWS the newest partition of a LOG table: it subscribes each
 * sampled bucket at the CURRENT log-end offset, so it only ever sees records
 * written after the probe started, and reports the age of the freshest record
 * it can see at each poll:
 *
 * <pre>
 *   &lt;epoch_ms&gt;\t&lt;records&gt;\t&lt;newest_age_ms&gt;\t&lt;stalest_age_ms&gt;\t&lt;buckets&gt;
 * </pre>
 *
 * <p>{@code newest_age_ms = pollWallClock - max(ingest_ts in this poll)}. Both
 * clocks are the host's (ingestion stamps {@code ingest_ts} on the same
 * machine), so no clock skew enters the number.
 *
 * <p>How to read it — {@code min(newest_age_ms)} over a dense window is the
 * estimator for the append-to-readable delay D: the freshest visible record was
 * appended ~D + (inter-arrival gap) ago, and the inter-arrival gap is a few ms
 * at live rates. The percentile summary therefore bounds D from above, while
 * the p99 shows how stale the readable tail gets. Compare with the ingestion
 * client's own {@code append.latency.ms} (accept -&gt; ack) for the same window:
 * a large {@code newest_age} with a small {@code append.latency} puts the delay
 * AFTER the ack (read visibility); a comparable pair puts it in the write path.
 *
 * <p>Fetch behaviour is pinned to the measured job's setting
 * ({@code FLUSS_SCANNER_FETCH_WAIT_MAX_TIME_MS}, default 2 ms) so the probe is
 * not measuring its own batching: with a large fetch wait the reader would
 * accumulate before returning and inflate every number.
 *
 * <p>Usage: {@code java --add-opens=java.base/java.nio=ALL-UNNAMED -cp <cp>
 * FlussVisibilityProbe [db] [table] [bootstrap] [buckets] [seconds] [poll_ms]
 * [fetch_wait_ms]}
 *
 * <p>The {@code --add-opens} flag is NOT optional. Reading row data touches the
 * Fluss client's shaded Arrow, whose {@code MemoryUtil} cannot initialise on JDK
 * 17 unless {@code java.nio} is opened; the failure appears only at the first
 * record, as {@code ExceptionInInitializerError}, and cost one empty run on
 * 2026-10-06. Every other reader probe in this repository is launched the same
 * way ({@code stage-capture.sh}, {@code test_fluss_probes.py}); metadata-only
 * probes such as {@code FlussReadLagProbe} do not need it.
 *
 * <p>Exit codes: 0 sample(s) collected, 1 connection/read failure or no records
 * seen, 2 unusable input. The {@code __END__ <records>} sentinel is printed on
 * every exit path (callers block on it).
 */
public class FlussVisibilityProbe {

    static class InputException extends RuntimeException {
        InputException(String msg) {
            super(msg);
        }
    }

    public static void main(String[] args) {
        long records = 0;
        try {
            records = run(args);
            System.out.println("__END__ " + records);
            System.out.flush();
            System.exit(records > 0 ? 0 : 1);
        } catch (InputException e) {
            System.err.println("FlussVisibilityProbe: " + e);
            System.out.println("__END__ " + records);
            System.out.flush();
            System.exit(2);
        } catch (Throwable t) {
            System.err.println("FlussVisibilityProbe failed: " + t);
            System.out.println("__END__ " + records);
            System.out.flush();
            System.exit(1);
        }
    }

    static long run(String[] args) throws Exception {
        String database = args.length > 0 && !args[0].isEmpty() ? args[0] : "default";
        String table = args.length > 1 && !args[1].isEmpty() ? args[1] : "raw_table_1";
        String bootstrap = args.length > 2 && !args[2].isEmpty() ? args[2] : "localhost:9123";
        int buckets = intArg(args, 3, 4, 1, 64, "buckets");
        int seconds = intArg(args, 4, 60, 1, 3600, "seconds");
        int pollMs = intArg(args, 5, 10, 1, 1000, "poll_ms");
        int fetchWaitMs = intArg(args, 6, 2, 1, 60_000, "fetch_wait_ms");

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        // Mirror the measured job instead of the client default, otherwise the
        // probe measures its own fetch batching.
        conf.setString("client.scanner.log.fetch.wait-max-time", fetchWaitMs + "ms");
        conf.setString("client.scanner.log.max-poll-records", "200");

        TablePath tp = TablePath.of(database, table);
        long total = 0;
        List<Long> ages = new ArrayList<>();
        long firstPollMs = 0;
        long lastPollMs = 0;

        try (Connection c = ConnectionFactory.createConnection(conf);
                Admin admin = c.getAdmin();
                Table t = c.getTable(tp);
                LogScanner scanner = t.newScan().createLogScanner()) {
            RowType rowType = t.getTableInfo().getSchema().getRowType();
            int ingestIdx = rowType.getFieldNames().indexOf("ingest_ts");
            if (ingestIdx < 0) {
                throw new InputException("table " + database + "." + table
                        + " has no ingest_ts column (columns=" + rowType.getFieldNames() + ")");
            }
            int numBuckets = t.getTableInfo().getNumBuckets();
            if (!t.getTableInfo().isPartitioned()) {
                throw new InputException("table " + database + "." + table
                        + " is not partitioned; this probe samples the newest partition"
                        + " (pass a daily-partitioned table)");
            }
            List<PartitionInfo> partitions = admin.listPartitionInfos(tp).get(10, TimeUnit.SECONDS);
            if (partitions == null || partitions.isEmpty()) {
                throw new InputException("no partition on " + database + "." + table);
            }
            // Newest partition = the one the live writer appends to (event_day
            // sorts lexicographically for yyyyMMdd).
            PartitionInfo target = partitions.get(0);
            for (PartitionInfo p : partitions) {
                if (String.valueOf(p.getPartitionName())
                        .compareTo(String.valueOf(target.getPartitionName())) > 0) {
                    target = p;
                }
            }
            List<Integer> bucketIds = new ArrayList<>();
            for (int b = 0; b < Math.min(buckets, numBuckets); b++) {
                bucketIds.add(b);
            }
            Map<Integer, Long> ends = admin
                    .listOffsets(tp, target.getPartitionName(), bucketIds, new LatestSpec())
                    .all().get(10, TimeUnit.SECONDS);
            for (int b : bucketIds) {
                long end = ends.getOrDefault(b, 0L);
                // Subscribe AT the log end: only records written from now on, so
                // the measured age is never a backfilled/old record.
                scanner.subscribe(target.getPartitionId(), b, Math.max(0L, end));
            }
            System.err.println("FlussVisibilityProbe: table=" + database + "." + table
                    + " partition=" + target.getPartitionName()
                    + " buckets=" + bucketIds
                    + " log_end=" + ends
                    + " seconds=" + seconds + " poll_ms=" + pollMs
                    + " fetch_wait_ms=" + fetchWaitMs
                    + " db_partitions=" + partitions.size());

            long deadline = System.currentTimeMillis() + seconds * 1000L;
            long window = pollMs;
            while (System.currentTimeMillis() < deadline) {
                ScanRecords recs = scanner.poll(Duration.ofMillis(window));
                total += drain(recs, ingestIdx, ages);
                if (total > 0) {
                    long now = System.currentTimeMillis();
                    if (firstPollMs == 0) {
                        firstPollMs = now;
                    }
                    lastPollMs = now;
                }
            }
        }

        if (!ages.isEmpty()) {
            List<Long> sorted = new ArrayList<>(ages);
            sorted.sort(Long::compareTo);
            System.err.println("FlussVisibilityProbe: samples=" + sorted.size()
                    + " span_ms=" + (firstPollMs > 0 ? lastPollMs - firstPollMs : 0)
                    + " min_age_ms=" + sorted.get(0)
                    + " p50=" + pct(sorted, 50)
                    + " p90=" + pct(sorted, 90)
                    + " p99=" + pct(sorted, 99)
                    + " max=" + sorted.get(sorted.size() - 1));
        } else {
            System.err.println("FlussVisibilityProbe: no records seen in " + seconds
                    + "s — is a writer appending to " + database + "." + table + "?");
        }
        return total;
    }

    /** Emits one row per poll that saw data; appends the freshest age to `ages`. */
    static long drain(ScanRecords recs, int ingestIdx, List<Long> ages) {
        if (recs == null || recs.isEmpty()) {
            return 0;
        }
        long now = System.currentTimeMillis();
        long newest = Long.MIN_VALUE;
        long stalest = Long.MAX_VALUE;
        int n = 0;
        int buckets = 0;
        for (ScanRecord rec : recs) {
            InternalRow row = rec.getRow();
            if (row == null) {
                continue;
            }
            long ingest = row.getLong(ingestIdx);
            if (ingest <= 0) {
                continue;
            }
            n++;
            buckets++;
            if (ingest > newest) {
                newest = ingest;
            }
            if (ingest < stalest) {
                stalest = ingest;
            }
        }
        if (n == 0) {
            return 0;
        }
        long newestAge = Math.max(0L, now - newest);
        long stalestAge = Math.max(0L, now - stalest);
        ages.add(newestAge);
        System.out.println(now + "\t" + n + "\t" + newestAge + "\t" + stalestAge + "\t" + buckets);
        System.out.flush();
        return n;
    }

    static long pct(List<Long> sorted, int p) {
        int idx = (int) Math.min(sorted.size() - 1L, Math.round((sorted.size() - 1) * p / 100.0));
        return sorted.get(Math.max(0, idx));
    }

    static int intArg(String[] args, int i, int def, int min, int max, String name) {
        if (args.length <= i || args[i].isEmpty()) {
            return def;
        }
        int v;
        try {
            v = Integer.parseInt(args[i].trim());
        } catch (NumberFormatException e) {
            throw new InputException(name + " is not an integer: '" + args[i] + "'");
        }
        if (v < min || v > max) {
            throw new InputException(name + " out of range [" + min + "," + max + "]: " + v);
        }
        return v;
    }
}
