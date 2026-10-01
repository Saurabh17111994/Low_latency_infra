import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
 * RawCompressionProbe — scratch-table comparison of Fluss LOG storage options
 * for the raw_table_1 schema (probe only; never touches raw_table_1).
 *
 * <p>Creates four tables in {@code default}, each with the EXACT raw schema and
 * partitioning, differing only in the storage option:
 * <pre>
 *   raw_probe_none     table.log.arrow.compression.type = none      (control)
 *   raw_probe_lz4      table.log.arrow.compression.type = lz4_frame
 *   raw_probe_zstd     table.log.arrow.compression.type = zstd
 *   raw_probe_indexed  table.log.format = indexed
 * </pre>
 *
 * <p>Replays the first N rows of {@code raw_table_1} (deep-copied) into all
 * four; the disk delta is measured by the caller from
 * {@code du -sk /tmp/fluss/data/default/<table-id>} inside the tablet container.
 *
 * <p>Commands:
 * <pre>
 *   setup  &lt;bootstrap&gt; [source_table]   create/drop scratch tables, print TABLE lines
 *   write  &lt;bootstrap&gt; &lt;rows&gt;          replay rows, print WRITE/SINK/SOURCE/NULLS lines
 *   drop   &lt;bootstrap&gt;                   drop the scratch tables
 * </pre>
 */
public final class RawCompressionProbe {

    static final String DB = "default";
    static final String SOURCE = "raw_table_1";

    /** variant -> probe table name + option map (common options added in create). */
    static final Map<String, String> TABLES = new LinkedHashMap<>();
    static final Map<String, Map<String, String>> OPTIONS = new LinkedHashMap<>();

    static {
        TABLES.put("none", "raw_probe_none");
        TABLES.put("lz4", "raw_probe_lz4");
        TABLES.put("zstd", "raw_probe_zstd");
        TABLES.put("indexed", "raw_probe_indexed");
        OPTIONS.put("none", Map.of("table.log.arrow.compression.type", "none"));
        OPTIONS.put("lz4", Map.of("table.log.arrow.compression.type", "lz4_frame"));
        OPTIONS.put("zstd", Map.of("table.log.arrow.compression.type", "zstd"));
        OPTIONS.put("indexed", Map.of("table.log.format", "indexed"));
    }

    public static void main(String[] args) {
        try {
            if (args.length < 2) {
                System.err.println("usage: RawCompressionProbe <setup|write|drop> <bootstrap> [rows|source]");
                System.exit(2);
            }
            String cmd = args[0];
            String bootstrap = args[1];
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", bootstrap);
            if (args.length > 3) {
                // optional writer batch timeout (e.g. "1ms" to mirror ingestion's linger)
                conf.setString("client.writer.batch-timeout", args[3]);
            }
            try (Connection conn = ConnectionFactory.createConnection(conf);
                    Admin admin = conn.getAdmin()) {
                switch (cmd) {
                    case "setup" -> setup(conn, admin, args.length > 2 ? args[2] : SOURCE);
                    case "write" -> write(conn, admin, args.length > 2 ? Long.parseLong(args[2]) : 200_000L);
                    case "drop" -> drop(admin);
                    default -> throw new IllegalArgumentException("unknown command " + cmd);
                }
            }
            System.exit(0);
        } catch (Throwable t) {
            System.err.println("RawCompressionProbe failed: " + t);
            t.printStackTrace();
            System.exit(1);
        }
    }

    static TablePath path(String name) {
        return TablePath.of(DB, name);
    }

    static void setup(Connection conn, Admin admin, String sourceTable) throws Exception {
        Table src = conn.getTable(path(sourceTable));
        TableInfo info = src.getTableInfo();
        RowType rowType = info.getSchema().getRowType();
        System.out.println("SOURCE_SCHEMA\t" + sourceTable + "\tfields=" + rowType.getFieldCount()
                + "\tbuckets=" + info.getNumBuckets() + "\tpartitioned=" + info.isPartitioned());
        for (String variant : TABLES.keySet()) {
            TablePath tp = path(TABLES.get(variant));
            try {
                admin.dropTable(tp, true).get(20, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // absent
            }
            Map<String, String> props = new LinkedHashMap<>();
            props.put("table.auto-partition.enabled", "true");
            props.put("table.auto-partition.time-unit", "DAY");
            props.put("table.auto-partition.num-precreate", "2");
            props.put("table.auto-partition.num-retention", "3");
            props.put("table.auto-partition.time-zone", "Asia/Kolkata");
            props.putAll(OPTIONS.get(variant));
            Schema schema = Schema.newBuilder().fromRowType(rowType).build();
            TableDescriptor descriptor = TableDescriptor.builder()
                    .schema(schema)
                    .distributedBy(info.getNumBuckets(), List.of("instrument_token"))
                    .partitionedBy(List.of("event_day"))
                    .properties(props)
                    .build();
            admin.createTable(tp, descriptor, false).get(30, TimeUnit.SECONDS);
            long id = conn.getTable(tp).getTableInfo().getTableId();
            System.out.println("TABLE\t" + variant + "\t" + TABLES.get(variant) + "\t" + id);
        }
        System.out.flush();
    }

    static void drop(Admin admin) throws Exception {
        for (String variant : TABLES.keySet()) {
            admin.dropTable(path(TABLES.get(variant)), true).get(20, TimeUnit.SECONDS);
            System.out.println("DROPPED\t" + variant + "\t" + TABLES.get(variant));
        }
        System.out.flush();
    }

    static void write(Connection conn, Admin admin, long rows) throws Exception {
        TablePath srcPath = path(SOURCE);
        Table src = conn.getTable(srcPath);
        TableInfo srcInfo = src.getTableInfo();
        RowType rowType = srcInfo.getSchema().getRowType();
        int width = rowType.getFieldCount();
        int payloadIdx = rowType.getFieldNames().indexOf("raw_payload");
        int buckets = srcInfo.getNumBuckets();

        Map<String, AppendWriter> writers = new LinkedHashMap<>();
        Map<String, AtomicLong> errors = new LinkedHashMap<>();
        Map<String, Table> probeTables = new LinkedHashMap<>();
        for (String variant : TABLES.keySet()) {
            Table t = conn.getTable(path(TABLES.get(variant)));
            probeTables.put(variant, t);
            writers.put(variant, t.newAppend().createWriter());
            errors.put(variant, new AtomicLong());
        }

        LogScanner scanner = src.newScan().createLogScanner();
        if (srcInfo.isPartitioned()) {
            List<PartitionInfo> parts = admin.listPartitionInfos(srcPath).get(20, TimeUnit.SECONDS);
            for (PartitionInfo p : parts) {
                for (int b = 0; b < buckets; b++) {
                    scanner.subscribeFromBeginning(p.getPartitionId(), b);
                }
            }
            System.out.println("SCAN\tpartitions=" + (parts == null ? 0 : parts.size()) + "\tbuckets=" + buckets);
        } else {
            for (int b = 0; b < buckets; b++) {
                scanner.subscribeFromBeginning(b);
            }
        }

        long copied = 0;
        long scanBytes = 0;
        long payloadBytes = 0;
        long[] nulls = new long[width];
        long deadline = System.currentTimeMillis() + 180_000L;
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
                for (int i = 0; i < width; i++) {
                    if (r.isNullAt(i)) {
                        nulls[i]++;
                    }
                }
                for (Map.Entry<String, AppendWriter> e : writers.entrySet()) {
                    CompletableFuture<?> f = e.getValue().append(copy);
                    f.whenComplete((res, err) -> {
                        if (err != null) {
                            errors.get(e.getKey()).incrementAndGet();
                        }
                    });
                }
                copied++;
                if (copied % 25_000 == 0) {
                    for (AppendWriter w : writers.values()) {
                        w.flush();
                    }
                    Thread.sleep(100);
                    System.err.println("progress rows=" + copied);
                }
            }
        }
        scanner.close();
        for (String variant : TABLES.keySet()) {
            writers.get(variant).flush();
        }
        Thread.sleep(5000);
        System.out.println("SOURCE\trows=" + copied + "\tscan_bytes=" + scanBytes
                + "\tpayload_bytes=" + payloadBytes);
        for (String variant : TABLES.keySet()) {
            Table t = probeTables.get(variant);
            long end = logEndSum(admin, path(TABLES.get(variant)), t.getTableInfo().getNumBuckets());
            System.out.println("WRITE\t" + variant + "\tappended=" + copied
                    + "\terrors=" + errors.get(variant).get() + "\tlog_end_sum=" + end);
        }
        for (int i = 0; i < width; i++) {
            if (nulls[i] > 0) {
                System.out.println("NULLS\t" + rowType.getFieldNames().get(i) + "\t" + nulls[i]
                        + "\t" + Math.round(100.0 * nulls[i] / Math.max(1, copied)) + "%");
            }
        }
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

    /** Deep copy of one source row (strings/bytes copied, no buffer aliasing). */
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

    private RawCompressionProbe() {}
}
