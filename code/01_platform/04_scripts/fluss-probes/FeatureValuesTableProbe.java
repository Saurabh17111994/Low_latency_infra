import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;

/**
 * FeatureValuesTableProbe — dev scratch-table tool for the stored feature layer smoke
 * (DEC-056/DEC-057, CHG-350).
 *
 * <p>Creates the {@code feature_values} table with the exact DDL 34 (proposal) shape — KV,
 * PK {@code (instrument_token, tf, window_start)}, one {@code features MAP<INT, DOUBLE>}
 * column, 16 buckets routed by {@code instrument_token}, 7d log TTL, lake OFF — and reads
 * it back for evidence. The DDL file stays an unapplied proposal; this probe exists only
 * because the operator chose the scratch route for the dev smoke.
 *
 * <p>Modes:
 * <pre>
 *   --mode create        create the table if absent (never drops), then describe it
 *   --mode describe      print columns, PK, bucket count and properties
 *   --mode tail          scan every bucket, print the first --rows rows + per-tf counts
 *   --mode upsert-check  write the same PK twice, read back: prove KV upsert (last wins)
 * </pre>
 */
public final class FeatureValuesTableProbe {

    private static final long TIMEOUT_MS = 60_000L;
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(15);
    private static final int FEATURES_FIELD = 3;
    /** PK far outside the NSE cash universe: the live writer can never collide with it. */
    private static final long CHECK_TOKEN = 999_999_999L;

    private static void usage(String message) {
        System.err.println("FeatureValuesTableProbe: " + message);
        System.err.println("usage: FeatureValuesTableProbe --mode create|describe|tail|upsert-check"
                + " [--database default] [--table feature_values]"
                + " [--rows 20] [--bootstrap host:port]");
        System.exit(2);
    }

    public static void main(String[] args) throws Exception {
        String mode = null;
        String database = "default";
        String table = "feature_values";
        String bootstrap = "fluss-coordinator:9123";
        int rows = 20;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--mode":
                    mode = args[++i];
                    break;
                case "--database":
                    database = args[++i];
                    break;
                case "--table":
                    table = args[++i];
                    break;
                case "--rows":
                    rows = Integer.parseInt(args[++i]);
                    break;
                case "--bootstrap":
                    bootstrap = args[++i];
                    break;
                default:
                    usage("unknown argument: " + arg);
            }
        }
        if (mode == null) {
            usage("--mode is required");
        }
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        TablePath path = TablePath.of(database, table);
        try (Connection conn = ConnectionFactory.createConnection(conf);
                Admin admin = conn.getAdmin()) {
            switch (mode) {
                case "create":
                    create(conn, admin, path);
                    break;
                case "describe":
                    describe(admin, path);
                    break;
                case "tail":
                    tail(conn, admin, path, rows);
                    break;
                case "upsert-check":
                    upsertCheck(conn, path);
                    break;
                default:
                    usage("unknown mode: " + mode);
            }
        }
    }

    private static void create(Connection conn, Admin admin, TablePath path) throws Exception {
        if (exists(admin, path)) {
            System.out.println("FEATURE-TABLE create=skipped reason=exists path=" + path);
        } else {
            Schema schema = Schema.newBuilder()
                    .column("instrument_token", DataTypes.BIGINT())
                    .column("tf", DataTypes.STRING())
                    .column("window_start", DataTypes.BIGINT())
                    .column("features", DataTypes.MAP(DataTypes.INT(), DataTypes.DOUBLE()))
                    .primaryKey("instrument_token", "tf", "window_start")
                    .build();
            TableDescriptor descriptor = TableDescriptor.builder()
                    .schema(schema)
                    .property("table.log.ttl", "7d")
                    .property("table.datalake.enabled", "false")
                    .property("table.datalake.format", "iceberg")
                    .property("table.kv.format-version", "2")
                    .distributedBy(16, "instrument_token")
                    .build();
            admin.createTable(path, descriptor, false).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            System.out.println("FEATURE-TABLE create=created path=" + path);
        }
        describe(admin, path);
    }

    private static boolean exists(Admin admin, TablePath path) {
        try {
            admin.getTableInfo(path).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static void describe(Admin admin, TablePath path) throws Exception {
        TableInfo info = admin.getTableInfo(path).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        System.out.println("FEATURE-TABLE describe path=" + path + " tableId=" + info.getTableId()
                + " buckets=" + info.getNumBuckets());
        for (Schema.Column column : info.getSchema().getColumns()) {
            System.out.println("FEATURE-TABLE column=" + column.getName() + ":"
                    + column.getDataType().getTypeRoot().name());
        }
        System.out.println("FEATURE-TABLE pk=" + info.getPrimaryKeys());
        System.out.println("FEATURE-TABLE properties=" + info.getProperties());
    }

    private static void tail(Connection conn, Admin admin, TablePath path, int rows) throws Exception {
        TableInfo info = admin.getTableInfo(path).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        Map<String, Integer> perTf = new TreeMap<>();
        long read = 0;
        try (Table table = conn.getTable(path)) {
            for (int b = 0; b < info.getNumBuckets(); b++) {
                TableBucket bucket = new TableBucket(info.getTableId(), b);
                try (BatchScanner scanner = table.newScan().createBatchScanner(bucket)) {
                    while (true) {
                        try (CloseableIterator<InternalRow> batch = scanner.pollBatch(POLL_TIMEOUT)) {
                            if (batch == null) {
                                break; // end of input, per the BatchScanner contract
                            }
                            while (batch.hasNext()) {
                                InternalRow row = batch.next();
                                read++;
                                String tf = row.getString(1).toString();
                                perTf.merge(tf, 1, Integer::sum);
                                if (read <= rows) {
                                    System.out.println(formatRow(row, tf));
                                }
                            }
                        }
                    }
                }
            }
        }
        System.out.println("FEATURE-TABLE rows=" + read);
        System.out.println("FEATURE-TABLE per_tf=" + perTf);
    }

    private static String formatRow(InternalRow row, String tf) {
        InternalMap features = row.getMap(FEATURES_FIELD);
        StringBuilder sb = new StringBuilder(128);
        sb.append("FEATURE-TABLE row token=").append(row.getLong(0))
                .append(" tf=").append(tf)
                .append(" window_start=").append(row.getLong(2))
                .append(" features={");
        InternalArray keys = features.keyArray();
        InternalArray values = features.valueArray();
        for (int i = 0; i < features.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(keys.getInt(i)).append('=').append(values.getDouble(i));
        }
        return sb.append('}').toString();
    }

    private static void upsertCheck(Connection conn, TablePath path) throws Exception {
        try (Table table = conn.getTable(path)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            Lookuper lookuper = table.newLookup().createLookuper();
            writer.upsert(row(1.5d)).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            double first = readValue(lookuper);
            writer.upsert(row(2.5d)).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            double second = readValue(lookuper);
            boolean ok = Double.compare(first, 1.5d) == 0 && Double.compare(second, 2.5d) == 0;
            System.out.println("FEATURE-TABLE upsert first=" + first + " second=" + second);
            System.out.println("FEATURE-TABLE upsert_result=" + (ok ? "PASS" : "FAIL"));
        }
    }

    private static GenericRow row(double value) {
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put(Integer.valueOf(0), value);
        return GenericRow.of(
                CHECK_TOKEN, BinaryString.fromString("FIFTEEN_S"), 1L, new GenericMap(map));
    }

    private static double readValue(Lookuper lookuper) throws Exception {
        InternalRow row = lookuper
                .lookup(GenericRow.of(CHECK_TOKEN, BinaryString.fromString("FIFTEEN_S"), 1L))
                .get(TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .getSingletonRow();
        if (row == null) {
            return Double.NaN;
        }
        return row.getMap(FEATURES_FIELD).valueArray().getDouble(0);
    }

    private FeatureValuesTableProbe() {}
}
