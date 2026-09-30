import java.time.Duration;
import java.util.LinkedHashMap;
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
 * CandleFeaturesTableProbe — dev scratch-table tool for the merged candle+feature table smoke
 * (Wave B/DEC-059, CHG-474; same route CHG-350 used for feature_values).
 *
 * <p>Creates the {@code candle_features} table with the exact DDL 35 (proposal) shape — KV,
 * PK {@code (instrument_token, tf, window_start)}, the candle contract's 15 columns +
 * {@code features MAP<INT, DOUBLE>} + {@code sealed BOOLEAN}, 16 buckets routed by
 * {@code instrument_token}, 3d log TTL, lake OFF — and reads it back for evidence. The DDL
 * file stays an unapplied proposal; this probe exists only because the operator chose the
 * scratch route for the dev smoke.
 *
 * <p>Modes:
 * <pre>
 *   --mode create        create the table if absent (never drops), then describe it
 *   --mode describe      print columns, PK, bucket count and properties
 *   --mode tail          scan every bucket, print the first --rows rows + per-tf and
 *                        sealed/forming counts
 *   --mode upsert-check  write the same PK twice, read back: prove KV upsert (last wins)
 * </pre>
 */
public final class CandleFeaturesTableProbe {

    private static final long TIMEOUT_MS = 60_000L;
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(15);
    /** DDL 35 index order: the candle contract's 15 columns, then features, then sealed. */
    private static final int TF_FIELD = 3;
    private static final int WINDOW_START_FIELD = 4;
    private static final int CLOSE_FIELD = 9;
    private static final int FEATURES_FIELD = 15;
    private static final int SEALED_FIELD = 16;
    /** PK far outside the NSE cash universe: the live writer can never collide with it. */
    private static final long CHECK_TOKEN = 999_999_999L;

    private static void usage(String message) {
        System.err.println("CandleFeaturesTableProbe: " + message);
        System.err.println("usage: CandleFeaturesTableProbe --mode create|describe|tail|upsert-check"
                + " [--database default] [--table candle_features]"
                + " [--rows 20] [--bootstrap host:port]");
        System.exit(2);
    }

    public static void main(String[] args) throws Exception {
        String mode = null;
        String database = "default";
        String table = "candle_features";
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
            System.out.println("CANDLE-FEATURES-TABLE create=skipped reason=exists path=" + path);
        } else {
            Schema schema = Schema.newBuilder()
                    .column("instrument_token", DataTypes.BIGINT())
                    .column("exchange", DataTypes.STRING())
                    .column("symbol", DataTypes.STRING())
                    .column("tf", DataTypes.STRING())
                    .column("window_start", DataTypes.BIGINT())
                    .column("window_end", DataTypes.BIGINT())
                    .column("open_paise", DataTypes.BIGINT())
                    .column("high_paise", DataTypes.BIGINT())
                    .column("low_paise", DataTypes.BIGINT())
                    .column("close_paise", DataTypes.BIGINT())
                    .column("volume", DataTypes.BIGINT())
                    .column("tick_count", DataTypes.INT())
                    .column("last_event_time", DataTypes.BIGINT())
                    .column("last_event_fingerprint", DataTypes.STRING())
                    .column("schema_version", DataTypes.STRING())
                    .column("features", DataTypes.MAP(DataTypes.INT(), DataTypes.DOUBLE()))
                    .column("sealed", DataTypes.BOOLEAN())
                    .primaryKey("instrument_token", "tf", "window_start")
                    .build();
            TableDescriptor descriptor = TableDescriptor.builder()
                    .schema(schema)
                    .property("table.log.ttl", "3d")
                    .property("table.datalake.enabled", "false")
                    .property("table.datalake.format", "iceberg")
                    .property("table.kv.format-version", "2")
                    .distributedBy(16, "instrument_token")
                    .build();
            admin.createTable(path, descriptor, false).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            System.out.println("CANDLE-FEATURES-TABLE create=created path=" + path);
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
        System.out.println("CANDLE-FEATURES-TABLE describe path=" + path + " tableId="
                + info.getTableId() + " buckets=" + info.getNumBuckets());
        for (Schema.Column column : info.getSchema().getColumns()) {
            System.out.println("CANDLE-FEATURES-TABLE column=" + column.getName() + ":"
                    + column.getDataType().getTypeRoot().name());
        }
        System.out.println("CANDLE-FEATURES-TABLE pk=" + info.getPrimaryKeys());
        System.out.println("CANDLE-FEATURES-TABLE properties=" + info.getProperties());
    }

    private static void tail(Connection conn, Admin admin, TablePath path, int rows)
            throws Exception {
        TableInfo info = admin.getTableInfo(path).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        Map<String, Integer> perTf = new TreeMap<>();
        long read = 0;
        long sealed = 0;
        long forming = 0;
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
                                String tf = row.getString(TF_FIELD).toString();
                                perTf.merge(tf, 1, Integer::sum);
                                if (row.getBoolean(SEALED_FIELD)) {
                                    sealed++;
                                } else {
                                    forming++;
                                }
                                if (read <= rows) {
                                    System.out.println(formatRow(row, tf));
                                }
                            }
                        }
                    }
                }
            }
        }
        System.out.println("CANDLE-FEATURES-TABLE rows=" + read + " sealed=" + sealed
                + " forming=" + forming);
        System.out.println("CANDLE-FEATURES-TABLE per_tf=" + perTf);
    }

    private static String formatRow(InternalRow row, String tf) {
        InternalMap features = row.getMap(FEATURES_FIELD);
        StringBuilder sb = new StringBuilder(160);
        sb.append("CANDLE-FEATURES-TABLE row token=").append(row.getLong(0))
                .append(" tf=").append(tf)
                .append(" window_start=").append(row.getLong(WINDOW_START_FIELD))
                .append(" close=").append(row.getLong(CLOSE_FIELD))
                .append(" sealed=").append(row.getBoolean(SEALED_FIELD))
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
            writer.upsert(row(100L)).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            long first = readClose(lookuper);
            writer.upsert(row(200L)).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            long second = readClose(lookuper);
            boolean ok = first == 100L && second == 200L;
            System.out.println("CANDLE-FEATURES-TABLE upsert first=" + first
                    + " second=" + second);
            System.out.println("CANDLE-FEATURES-TABLE upsert_result=" + (ok ? "PASS" : "FAIL"));
        }
    }

    private static GenericRow row(long close) {
        Map<Object, Object> features = new LinkedHashMap<>();
        features.put(Integer.valueOf(0), 12_345.0d);
        return GenericRow.of(
                CHECK_TOKEN,
                BinaryString.fromString("NSE"),
                BinaryString.fromString("PROBE"),
                BinaryString.fromString("FIFTEEN_S"),
                1L,
                15_001L,
                close,
                close,
                close,
                close,
                0L,
                0,
                1L,
                BinaryString.fromString("probe"),
                BinaryString.fromString("1"),
                new GenericMap(features),
                Boolean.FALSE);
    }

    private static long readClose(Lookuper lookuper) throws Exception {
        InternalRow row = lookuper
                .lookup(GenericRow.of(CHECK_TOKEN, BinaryString.fromString("FIFTEEN_S"), 1L))
                .get(TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .getSingletonRow();
        if (row == null) {
            return -1L;
        }
        return row.getLong(CLOSE_FIELD);
    }

    private CandleFeaturesTableProbe() {}
}
