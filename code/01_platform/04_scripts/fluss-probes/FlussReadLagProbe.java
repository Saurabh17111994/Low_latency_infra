import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec.LatestSpec;
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TablePath;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * FlussReadLagProbe — B2 CP3-side passive sampler (plan
 * docs/Investigations_and_Reports/2026-09-01-throughput-degradation.md, Stage B2).
 *
 * Prints the Fluss LOG log-end offsets (admin.listOffsets with LatestSpec)
 * for one table, summed across EVERY partition and bucket, one line per
 * invocation:
 *
 *   &lt;epoch_ms&gt;\t&lt;table&gt;\t&lt;partitions&gt;\t&lt;buckets_per_partition&gt;\t&lt;logEndSum&gt;
 *
 * logEndSum = total records appended to the table (CP3: Fluss append). The
 * Flink consumed count (CP4) is read offline from stage-capture's stages.tsv
 * (source operator numRecordsIn), so CP3-&gt;CP4 read lag = logEndSum(t) -
 * sourceIn(t) — a pure subtraction on the shared timeline.
 *
 * PARTITION-AWARE (2026-09-02, live-observed): raw_table_1 is an
 * auto-partitioned daily table (event_day) with bucket.num=16 PER PARTITION.
 * A naive listOffsets over buckets 0..15 WITHOUT a partition name only
 * reaches one partition's buckets and fails ("Leader not found ... for table
 * bucket TableBucket{tableId=..., bucket=0}") because the client cannot
 * resolve a partition-less bucket of a partitioned table. This probe lists
 * partitions first (listPartitionInfos) and calls the partition-aware
 * listOffsets(tp, partitionName, buckets, spec) overload for each.
 *
 * Cost: one listPartitionInfos + one listOffsets-per-partition RPC per
 * invocation. stage-capture invokes this once per tick (default 5s) —
 * passive, no data-path involvement.
 *
 * Contract (wave 13):
 *   - every listOffsets RPC is ISSUED before any of them is waited on, so the sample
 *     costs one RPC round-trip instead of one per partition (P6-086): a partitioned
 *     table's partition count grows daily and the sequential form overran the 5s tick.
 *   - the printed timestamp is taken AFTER the offsets are known, so it dates the
 *     measurement instead of the start of the RPC fan-out (P6-375).
 *   - a partition that fails is reported on stderr and excluded from the sum, and the
 *     row is still printed with exit 3 (partial sample). If nothing could be read, no
 *     row is printed and the exit code is 1 (P6-085): the series must not show a
 *     made-up total, and a caller must not see a silent gap.
 *
 * Exit codes: 0 complete sample, 1 no sample, 2 unusable input, 3 partial sample.
 *
 * Usage: java -cp <cp> FlussReadLagProbe <database> <table> [bootstrap]
 */
public class FlussReadLagProbe {
    static class InputException extends RuntimeException {
        InputException(String msg) {
            super(msg);
        }
    }

    public static void main(String[] args) {
        try {
            run(args);
        } catch (InputException e) {
            // P6-085: an outer failure must be visible, and must not print a row that
            // looks like a real sample.
            System.err.println("FlussReadLagProbe: " + e);
            System.out.flush();
            System.exit(2);
        } catch (Throwable t) {
            System.err.println("FlussReadLagProbe failed: " + t);
            System.out.flush();
            System.exit(1);
        }
    }

    static void run(String[] args) throws Exception {
        String database = args.length > 0 ? args[0] : "default";
        String table = args.length > 1 ? args[1] : "raw_table_1";
        String bootstrap = args.length > 2 ? args[2] : "localhost:9123";
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        TablePath tp = TablePath.of(database, table);
        try (Connection c = ConnectionFactory.createConnection(conf);
             Admin admin = c.getAdmin();
             Table t = c.getTable(tp)) {
            int numBuckets = t.getTableInfo().getNumBuckets();
            List<Integer> bucketIds = new ArrayList<>();
            for (int b = 0; b < numBuckets; b++) bucketIds.add(b);
            long sum = 0;

            if (t.getTableInfo().isPartitioned()) {
                List<PartitionInfo> partitions =
                        admin.listPartitionInfos(tp).get(5, TimeUnit.SECONDS);
                if (partitions == null) {
                    partitions = List.of();
                }
                // P6-086: issue every listOffsets first, then collect — the offsets are
                // independent RPCs and waiting on each in turn made the sample O(P) in
                // round-trips (P grows daily for an auto-partitioned table).
                List<ListOffsetsResult> pending = new ArrayList<>(partitions.size());
                List<String> pendingNames = new ArrayList<>(partitions.size());
                for (PartitionInfo p : partitions) {
                    pending.add(admin.listOffsets(tp, p.getPartitionName(), bucketIds, new LatestSpec()));
                    pendingNames.add(String.valueOf(p.getPartitionName()));
                }
                int failed = 0;
                for (int i = 0; i < pending.size(); i++) {
                    try {
                        Map<Integer, Long> offsets = pending.get(i).all().get(5, TimeUnit.SECONDS);
                        for (long o : offsets.values()) sum += o;
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw ie;
                    } catch (Exception e) {
                        // P6-085: one slow/bad partition must not discard the sample.
                        failed++;
                        System.err.println("FlussReadLagProbe: partition " + pendingNames.get(i)
                                + " listOffsets failed: " + e);
                    }
                }
                if (failed == pending.size() && !pending.isEmpty()) {
                    throw new IllegalStateException("no partition answered listOffsets ("
                            + failed + " of " + pending.size() + ")");
                }
                // P6-375: date the measurement, not the start of the fan-out.
                long epochMs = System.currentTimeMillis();
                System.out.println(epochMs + "\t" + table + "\t"
                        + partitions.size() + "\t" + numBuckets + "\t" + sum);
                if (failed > 0) {
                    System.err.println("WARN: partial sample — " + failed + " of " + pending.size()
                            + " partitions failed; logEndSum excludes them");
                }
                System.out.flush();
                System.exit(failed > 0 ? 3 : 0);
            } else {
                ListOffsetsResult res = admin.listOffsets(tp, bucketIds, new LatestSpec());
                Map<Integer, Long> offsets = res.all().get(5, TimeUnit.SECONDS);
                for (long o : offsets.values()) sum += o;
                long epochMs = System.currentTimeMillis();
                System.out.println(epochMs + "\t" + table + "\t"
                        + 1 + "\t" + numBuckets + "\t" + sum);
                System.out.flush();
                System.exit(0);
            }
        }
    }
}
