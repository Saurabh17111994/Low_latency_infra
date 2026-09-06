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
 *   <epoch_ms>\t<table>\t<partitions>\t<buckets_per_partition>\t<logEndSum>
 *
 * logEndSum = total records appended to the table (CP3: Fluss append). The
 * Flink consumed count (CP4) is read offline from stage-capture's stages.tsv
 * (source operator numRecordsIn), so CP3->CP4 read lag = logEndSum(t) -
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
 * Usage: java -cp <cp> FlussReadLagProbe <database> <table> [bootstrap]
 */
public class FlussReadLagProbe {
    public static void main(String[] args) throws Exception {
        String database = args.length > 0 ? args[0] : "default";
        String table = args.length > 1 ? args[1] : "raw_table_1";
        String bootstrap = args.length > 2 ? args[2] : "localhost:9123";
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        TablePath tp = TablePath.of(database, table);
        long epochMs = System.currentTimeMillis();
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
                for (PartitionInfo p : partitions) {
                    ListOffsetsResult res = admin.listOffsets(
                            tp, p.getPartitionName(), bucketIds, new LatestSpec());
                    Map<Integer, Long> offsets = res.all().get(5, TimeUnit.SECONDS);
                    for (long o : offsets.values()) sum += o;
                }
                System.out.println(epochMs + "\t" + table + "\t"
                        + partitions.size() + "\t" + numBuckets + "\t" + sum);
            } else {
                ListOffsetsResult res =
                        admin.listOffsets(tp, bucketIds, new LatestSpec());
                Map<Integer, Long> offsets = res.all().get(5, TimeUnit.SECONDS);
                for (long o : offsets.values()) sum += o;
                System.out.println(epochMs + "\t" + table + "\t"
                        + 1 + "\t" + numBuckets + "\t" + sum);
            }
        }
    }
}
