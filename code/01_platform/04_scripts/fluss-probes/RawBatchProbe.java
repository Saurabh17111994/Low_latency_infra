import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec.LatestSpec;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

/**
 * RawBatchProbe — one scratch table per run, cloned from raw_table_1, written at a
 * controlled per-writer rate with the ingestion writer's exact client settings.
 * Answers: how many bytes/row does Fluss store at the live arrival rate for a
 * given compression option and linger? Probe only; never touches raw_table_1.
 *
 * usage:
 *   setup &lt;bootstrap&gt; &lt;name&gt; &lt;default|zstd|none&gt; [zstdLevel]
 *   write &lt;bootstrap&gt; &lt;name&gt; &lt;rows&gt; &lt;rowsPerSec&gt; &lt;lingerMs&gt; [batchKiB]
 */
public final class RawBatchProbe {
    static final String DB = "default";
    static final String SOURCE = "raw_table_1";

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("usage: RawBatchProbe <setup|write> <bootstrap> <name> ...");
            System.exit(2);
        }
        String cmd = args[0];
        String bootstrap = args[1];
        String name = args[2];
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        if ("write".equals(cmd)) {
            conf.setString("client.writer.batch-timeout", args[5] + "ms");
            conf.setString("client.writer.batch-size", (args.length > 6 ? args[6] : "64") + "kb");
            conf.setString("client.writer.dynamic-batch-size.enabled", "false");
        }
        try (Connection conn = ConnectionFactory.createConnection(conf);
                Admin admin = conn.getAdmin()) {
            switch (cmd) {
                case "setup" -> setup(conn, admin, name, args[3],
                        args.length > 4 ? Integer.parseInt(args[4]) : 3);
                case "write" -> write(conn, admin, name, Long.parseLong(args[3]),
                        Integer.parseInt(args[4]), args[5]);
                case "drop" -> {
                    admin.dropTable(TablePath.of(DB, name), true).get(20, TimeUnit.SECONDS);
                    System.out.println("DROPPED\t" + name);
                }
                default -> throw new IllegalArgumentException("unknown command " + cmd);
            }
        }
        System.exit(0);
    }

    static void setup(Connection conn, Admin admin, String name, String compression, int level)
            throws Exception {
        TableInfo info = conn.getTable(TablePath.of(DB, SOURCE)).getTableInfo();
        RowType rowType = info.getSchema().getRowType();
        TablePath tp = TablePath.of(DB, name);
        try {
            admin.dropTable(tp, true).get(20, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // absent
        }
        Map<String, String> props = new java.util.LinkedHashMap<>();
        props.put("table.auto-partition.enabled", "true");
        props.put("table.auto-partition.time-unit", "DAY");
        props.put("table.auto-partition.num-precreate", "2");
        props.put("table.auto-partition.num-retention", "3");
        props.put("table.auto-partition.time-zone", "Asia/Kolkata");
        if ("zstd".equals(compression)) {
            props.put("table.log.arrow.compression.type", "zstd");
            props.put("table.log.arrow.compression.zstd.level", String.valueOf(level));
        } else if ("none".equals(compression)) {
            props.put("table.log.arrow.compression.type", "none");
        } // "default" -> no compression option at all (server default)
        TableDescriptor descriptor = TableDescriptor.builder()
                .schema(Schema.newBuilder().fromRowType(rowType).build())
                .distributedBy(info.getNumBuckets(), List.of("instrument_token"))
                .partitionedBy(List.of("event_day"))
                .properties(props)
                .build();
        admin.createTable(tp, descriptor, false).get(30, TimeUnit.SECONDS);
        long id = conn.getTable(tp).getTableInfo().getTableId();
        System.out.println("TABLE\t" + name + "\t" + id + "\tcompression=" + compression
                + "\tlevel=" + level);
        System.out.flush();
    }

    static void write(Connection conn, Admin admin, String name, long rows, int ratePerSec,
            String linger) throws Exception {
        Table src = conn.getTable(TablePath.of(DB, SOURCE));
        TableInfo srcInfo = src.getTableInfo();
        RowType rowType = srcInfo.getSchema().getRowType();
        int width = rowType.getFieldCount();
        int payloadIdx = rowType.getFieldNames().indexOf("raw_payload");
        int buckets = srcInfo.getNumBuckets();

        Table dst = conn.getTable(TablePath.of(DB, name));
        AppendWriter writer = dst.newAppend().createWriter();
        AtomicLong errors = new AtomicLong();

        LogScanner scanner = src.newScan().createLogScanner();
        List<PartitionInfo> parts = admin.listPartitionInfos(TablePath.of(DB, SOURCE))
                .get(20, TimeUnit.SECONDS);
        for (PartitionInfo p : parts) {
            for (int b = 0; b < buckets; b++) {
                scanner.subscribeFromBeginning(p.getPartitionId(), b);
            }
        }

        long copied = 0;
        long scanBytes = 0;
        long payloadBytes = 0;
        long startNanos = System.nanoTime();
        long deadline = System.currentTimeMillis() + 300_000L;
        while (copied < rows && System.currentTimeMillis() < deadline) {
            ScanRecords records = scanner.poll(Duration.ofMillis(1000));
            if (records == null || records.isEmpty()) {
                continue;
            }
            for (ScanRecord rec : records) {
                if (copied >= rows) {
                    break;
                }
                InternalRow r = rec.getRow();
                if (r == null) {
                    continue;
                }
                GenericRow copy = copy(r, rowType);
                scanBytes += Math.max(0, rec.getSizeInBytes());
                if (payloadIdx >= 0 && !copy.isNullAt(payloadIdx)) {
                    byte[] p = copy.getBytes(payloadIdx);
                    payloadBytes += (p == null ? 0 : p.length);
                }
                CompletableFuture<?> f = writer.append(copy);
                f.whenComplete((res, err) -> {
                    if (err != null) {
                        errors.incrementAndGet();
                    }
                });
                copied++;
                // pace to the target per-writer arrival rate (live ingestion: ~1622/s)
                long nextNanos = startNanos + (long) (copied * 1_000_000_000L / ratePerSec);
                long wait = nextNanos - System.nanoTime();
                if (wait > 0) {
                    LockSupport.parkNanos(wait);
                }
            }
        }
        scanner.close();
        writer.flush();
        Thread.sleep(3000);
        long end = logEndSum(admin, TablePath.of(DB, name), dst.getTableInfo().getNumBuckets());
        System.out.println("WRITE\tname=" + name + "\trows=" + copied + "\trate=" + ratePerSec
                + "\tlinger=" + linger + "\terrors=" + errors.get() + "\tlog_end_sum=" + end
                + "\tscan_bytes=" + scanBytes + "\tpayload_bytes=" + payloadBytes
                + "\telapsed_ms=" + (System.nanoTime() - startNanos) / 1_000_000);
        System.out.flush();
    }

    static long logEndSum(Admin admin, TablePath path, int buckets) throws Exception {
        List<Integer> bucketIds = new ArrayList<>();
        for (int b = 0; b < buckets; b++) {
            bucketIds.add(b);
        }
        long sum = 0;
        List<PartitionInfo> parts = admin.listPartitionInfos(path).get(20, TimeUnit.SECONDS);
        if (parts != null) {
            for (PartitionInfo p : parts) {
                Map<Integer, Long> offs = admin
                        .listOffsets(path, p.getPartitionName(), bucketIds, new LatestSpec())
                        .all().get(20, TimeUnit.SECONDS);
                for (long o : offs.values()) {
                    sum += o;
                }
            }
        }
        return sum;
    }

    static GenericRow copy(InternalRow row, RowType rowType) {
        GenericRow out = new GenericRow(rowType.getFieldCount());
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (row.isNullAt(i)) {
                out.setField(i, null);
                continue;
            }
            DataType type = rowType.getTypeAt(i);
            switch (type.getTypeRoot()) {
                case BIGINT -> out.setField(i, row.getLong(i));
                case STRING -> out.setField(i, BinaryString.fromString(row.getString(i).toString()));
                case BYTES -> {
                    byte[] b = row.getBytes(i);
                    out.setField(i, b == null ? null : Arrays.copyOf(b, b.length));
                }
                default -> throw new IllegalStateException("unsupported type " + type);
            }
        }
        return out;
    }

    private RawBatchProbe() {}
}
