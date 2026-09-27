import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec.LatestSpec;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

/**
 * FeatureSpikeProbe — MAP-vs-JSON storage spike for the stored feature layer (DEC-056).
 *
 * <p>Writes the SAME payload (2433 instruments x 6 timeframes x windows, N features/row)
 * into three KV tables that differ only in the {@code features} column type:
 *
 * <pre>
 *   zz_feature_spike_map_str   features MAP&lt;STRING, DOUBLE&gt;   (name-keyed)
 *   zz_feature_spike_map_int   features MAP&lt;INT, DOUBLE&gt;      (registry-id-keyed)
 *   zz_feature_spike_json      features STRING                 (compact JSON object)
 *   zz_feature_spike_array     features ARRAY&lt;DOUBLE&gt;          (+ feature_version BIGINT)
 * </pre>
 *
 * and reports, per variant: write throughput + ack percentiles, point-lookup percentiles,
 * decode percentiles, and round-trip correctness. Byte counts are NOT measured here: the
 * runner reads the tablet's per-table data dir with {@code du} after the probe (the server
 * owns the on-disk encoding), then can call {@code drop}.
 *
 * <p>Payload shape mirrors the DEC-056 estimate: 64 features per (instrument, tf, window)
 * row by default, 4 closed windows per tf. Values are deterministic (LCG-ish, two
 * decimals) so round-trip comparison needs no stored expectation and the bytes are not
 * trivially compressible.
 *
 * <p>This probe exercises the raw Fluss client (upsert + KV lookup). The Flink connector
 * path (RowData -&gt; Fluss row, i.e. what the feature writer will use) is pinned separately
 * by {@code FeatureMapConversionTest} in the compute module — same Fluss 1.0.0 types, no
 * live cluster needed.
 *
 * <p>Usage: {@code FeatureSpikeProbe [run|drop] [--windows N] [--features N] [--reads N]
 * [--bootstrap HOST:PORT]}
 */
public final class FeatureSpikeProbe {

    static final int INSTRUMENTS = 2433;
    static final String[] TFS =
            {"FIFTEEN_S", "THIRTY_S", "ONE_M", "THREE_M", "FIVE_M", "FIFTEEN_M"};
    static final String[] VARIANTS = {"map_str", "map_int", "json", "array"};

    static final String TABLE_PREFIX = "zz_feature_spike_";
    static final long BASE_WINDOW = 1_790_000_000_000L;
    static final long WINDOW_MS = 60_000L;

    static final long TIMEOUT_MS = 30_000;
    static final int IN_FLIGHT = 256;
    static final int FEATURES_FIELD = 3;

    private FeatureSpikeProbe() {}

    public static void main(String[] args) throws Exception {
        boolean drop = false;
        int windows = 4;
        int features = 64;
        int reads = 1000;
        String bootstrap = "fluss-coordinator:9123";

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "run":
                    break;
                case "drop":
                    drop = true;
                    break;
                case "--windows":
                    windows = intValue(args, ++i, arg);
                    break;
                case "--features":
                    features = intValue(args, ++i, arg);
                    break;
                case "--reads":
                    reads = intValue(args, ++i, arg);
                    break;
                case "--bootstrap":
                    bootstrap = stringValue(args, ++i, arg);
                    break;
                default:
                    usage("unknown argument: " + arg);
            }
        }
        if (windows <= 0 || features <= 0 || reads <= 0) {
            usage("windows, features and reads must be positive");
        }

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);

        try (Connection conn = ConnectionFactory.createConnection(conf);
                Admin admin = conn.getAdmin()) {
            if (drop) {
                for (String variant : VARIANTS) {
                    dropTable(admin, TablePath.of("default", tableName(variant)));
                }
                System.out.println("SPIKE dropped=" + tableNames());
                return;
            }

            String[] names = new String[features];
            for (int i = 0; i < features; i++) {
                names[i] = String.format("feature_%03d", i);
            }
            ObjectMapper json = new ObjectMapper();

            for (String variant : VARIANTS) {
                TablePath path = TablePath.of("default", tableName(variant));
                dropTable(admin, path);
                admin.createTable(path, descriptor(variant), false)
                        .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
                awaitServing(admin, path, tableName(variant));
                System.out.println("SPIKE table variant=" + variant + " path=default."
                        + tableName(variant));

                WriteStats write = write(conn, path, variant, windows, features, names);
                awaitRowCount(admin, path, write.rows);
                ReadStats read = read(conn, path, variant, windows, features, reads, names, json);
                reportWrite(variant, write);
                reportRead(variant, read);
            }
            long totalRows = (long) windows * TFS.length * INSTRUMENTS;
            System.out.println("SPIKE note tables=" + tableNames()
                    + " rows_per_variant=" + totalRows
                    + " features_per_row=" + features
                    + " -- measure bytes with du, then 'drop'");
        }
    }

    // ── write ────────────────────────────────────────────────────────────────

    static final class WriteStats {
        final long rows;
        final int errors;
        final long elapsedNanos;
        final long[] ackNanos;

        WriteStats(long rows, int errors, long elapsedNanos, long[] ackNanos) {
            this.rows = rows;
            this.errors = errors;
            this.elapsedNanos = elapsedNanos;
            this.ackNanos = ackNanos;
        }
    }

    private static WriteStats write(
            Connection conn,
            TablePath path,
            String variant,
            int windows,
            int features,
            String[] names)
            throws Exception {
        long total = (long) windows * TFS.length * INSTRUMENTS;
        long[] ackNanos = new long[(int) total];
        AtomicInteger errors = new AtomicInteger();
        AtomicInteger index = new AtomicInteger();
        long start = System.nanoTime();
        try (Table table = conn.getTable(path)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            Deque<CompletableFuture<?>> inflight = new ArrayDeque<>();
            for (int w = 0; w < windows; w++) {
                long windowStart = BASE_WINDOW + (long) w * WINDOW_MS;
                for (String tf : TFS) {
                    for (long token = 1; token <= INSTRUMENTS; token++) {
                        GenericRow row =
                                buildRow(variant, token, tf, windowStart, w, features, names);
                        final int slot = index.getAndIncrement();
                        final long submitted = System.nanoTime();
                        CompletableFuture<?> future = writer.upsert(row);
                        future.whenComplete((result, failure) -> {
                            ackNanos[slot] = System.nanoTime() - submitted;
                            if (failure != null) {
                                errors.incrementAndGet();
                            }
                        });
                        inflight.add(future);
                        if (inflight.size() >= IN_FLIGHT) {
                            inflight.poll().get(60, TimeUnit.SECONDS);
                        }
                    }
                }
            }
            for (CompletableFuture<?> future : inflight) {
                future.get(60, TimeUnit.SECONDS);
            }
        }
        return new WriteStats(total, errors.get(), System.nanoTime() - start, ackNanos);
    }

    private static GenericRow buildRow(
            String variant,
            long token,
            String tf,
            long windowStart,
            int w,
            int features,
            String[] names) {
        Object payload;
        if (variant.equals("json")) {
            StringBuilder sb = new StringBuilder(features * 20 + 2).append('{');
            for (int i = 0; i < features; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append('"').append(names[i]).append("\":").append(value(token, w, i));
            }
            payload = BinaryString.fromString(sb.append('}').toString());
        } else if (variant.equals("array")) {
            double[] values = new double[features];
            for (int i = 0; i < features; i++) {
                values[i] = value(token, w, i);
            }
            payload = new GenericArray(values);
        } else {
            Map<Object, Object> map = new LinkedHashMap<>(features * 2);
            for (int i = 0; i < features; i++) {
                Object key = variant.equals("map_int") ? Integer.valueOf(i) : BinaryString.fromString(names[i]);
                map.put(key, value(token, w, i));
            }
            payload = new GenericMap(map);
        }
        if (variant.equals("array")) {
            // The positional encoding needs the registry version to decode ids -> names.
            return GenericRow.of(token, BinaryString.fromString(tf), windowStart, payload, 1L);
        }
        return GenericRow.of(token, BinaryString.fromString(tf), windowStart, payload);
    }

    // ── read / verify ────────────────────────────────────────────────────────

    static final class ReadStats {
        final int reads;
        final int roundTrips;
        final long[] lookupNanos;
        final long[] decodeNanos;

        ReadStats(int reads, int roundTrips, long[] lookupNanos, long[] decodeNanos) {
            this.reads = reads;
            this.roundTrips = roundTrips;
            this.lookupNanos = lookupNanos;
            this.decodeNanos = decodeNanos;
        }
    }

    private static ReadStats read(
            Connection conn,
            TablePath path,
            String variant,
            int windows,
            int features,
            int reads,
            String[] names,
            ObjectMapper json)
            throws Exception {
        long[] lookupNanos = new long[reads];
        long[] decodeNanos = new long[reads];
        int roundTrips = 0;
        Random random = new Random(42);
        try (Table table = conn.getTable(path)) {
            Lookuper lookuper = table.newLookup().createLookuper();
            for (int i = 0; i < reads; i++) {
                long token = 1 + random.nextInt(INSTRUMENTS);
                String tf = TFS[random.nextInt(TFS.length)];
                int w = random.nextInt(windows);
                long windowStart = BASE_WINDOW + (long) w * WINDOW_MS;
                long t0 = System.nanoTime();
                LookupResult result = lookuper
                        .lookup(GenericRow.of(token, BinaryString.fromString(tf), windowStart))
                        .get(5, TimeUnit.SECONDS);
                InternalRow row = result == null ? null : result.getSingletonRow();
                long t1 = System.nanoTime();
                boolean ok = row != null && verify(variant, row, token, w, features, names, json);
                long t2 = System.nanoTime();
                lookupNanos[i] = t1 - t0;
                decodeNanos[i] = t2 - t1;
                if (ok) {
                    roundTrips++;
                }
            }
        }
        return new ReadStats(reads, roundTrips, lookupNanos, decodeNanos);
    }

    private static boolean verify(
            String variant,
            InternalRow row,
            long token,
            int w,
            int features,
            String[] names,
            ObjectMapper json)
            throws Exception {
        if (variant.equals("json")) {
            Map<String, Double> parsed = json.readValue(
                    row.getString(FEATURES_FIELD).toString(), new TypeReference<Map<String, Double>>() {});
            if (parsed.size() != features) {
                return false;
            }
            for (int i = 0; i < features; i++) {
                Double got = parsed.get(names[i]);
                if (got == null || got.doubleValue() != value(token, w, i)) {
                    return false;
                }
            }
            return true;
        }
        if (variant.equals("array")) {
            InternalArray values = row.getArray(FEATURES_FIELD);
            if (values.size() != features) {
                return false;
            }
            for (int i = 0; i < features; i++) {
                if (values.getDouble(i) != value(token, w, i)) {
                    return false;
                }
            }
            return true;
        }
        InternalMap map = row.getMap(FEATURES_FIELD);
        if (map.size() != features) {
            return false;
        }
        InternalArray keys = map.keyArray();
        InternalArray values = map.valueArray();
        for (int i = 0; i < map.size(); i++) {
            boolean keyOk = variant.equals("map_int")
                    ? keys.getInt(i) == i
                    : keys.getString(i).toString().equals(names[i]);
            if (!keyOk || values.getDouble(i) != value(token, w, i)) {
                return false;
            }
        }
        return true;
    }

    // ── deterministic payload ────────────────────────────────────────────────

    /** Three-decimal value in [-500, 500), deterministic per (token, window, feature). */
    private static double value(long token, int w, int feature) {
        long h = token * 1_000_003L + w * 7_919L + feature * 104_729L;
        return (h % 1_000_000L) / 1000.0 - 500.0;
    }

    // ── reporting ────────────────────────────────────────────────────────────

    private static void reportWrite(String variant, WriteStats stats) {
        long[] ack = stats.ackNanos.clone();
        Arrays.sort(ack);
        double seconds = stats.elapsedNanos / 1_000_000_000.0;
        System.out.printf(
                "SPIKE write variant=%s rows=%d elapsed_s=%.1f rows_per_s=%.0f"
                        + " ack_p50_us=%d ack_p99_us=%d errors=%d%n",
                variant,
                stats.rows,
                seconds,
                stats.rows / seconds,
                percentile(ack, 0.50),
                percentile(ack, 0.99),
                stats.errors);
    }

    private static void reportRead(String variant, ReadStats stats) {
        long[] lookup = stats.lookupNanos.clone();
        long[] decode = stats.decodeNanos.clone();
        Arrays.sort(lookup);
        Arrays.sort(decode);
        System.out.printf(
                "SPIKE read variant=%s n=%d lookup_p50_us=%d lookup_p99_us=%d"
                        + " decode_p50_us=%d decode_p99_us=%d roundtrip=%d/%d%n",
                variant,
                stats.reads,
                percentile(lookup, 0.50),
                percentile(lookup, 0.99),
                percentile(decode, 0.50),
                percentile(decode, 0.99),
                stats.roundTrips,
                stats.reads);
    }

    /** Nanos at percentile p of a sorted array, in microseconds. */
    private static long percentile(long[] sorted, double p) {
        int idx = (int) Math.round((sorted.length - 1) * p);
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))] / 1_000L;
    }

    // ── table plumbing ───────────────────────────────────────────────────────

    private static String tableName(String variant) {
        return TABLE_PREFIX + variant;
    }

    private static List<String> tableNames() {
        List<String> names = new ArrayList<>();
        for (String variant : VARIANTS) {
            names.add(tableName(variant));
        }
        return names;
    }

    private static TableDescriptor descriptor(String variant) {
        Schema.Builder schema = Schema.newBuilder()
                .column("instrument_token", DataTypes.BIGINT())
                .column("tf", DataTypes.STRING())
                .column("window_start", DataTypes.BIGINT());
        if (variant.equals("map_str")) {
            schema.column("features", DataTypes.MAP(DataTypes.STRING(), DataTypes.DOUBLE()));
        } else if (variant.equals("map_int")) {
            schema.column("features", DataTypes.MAP(DataTypes.INT(), DataTypes.DOUBLE()));
        } else if (variant.equals("array")) {
            schema.column("features", DataTypes.ARRAY(DataTypes.DOUBLE()));
            // Positional values are meaningless without the registry version they were
            // written under; the array candidate carries that version in the row.
            schema.column("feature_version", DataTypes.BIGINT());
        } else {
            schema.column("features", DataTypes.STRING());
        }
        schema.primaryKey("instrument_token", "tf", "window_start");
        return TableDescriptor.builder()
                .schema(schema.build())
                .property("table.datalake.enabled", "false")
                .property("table.datalake.format", "iceberg")
                .property("table.kv.format-version", "2")
                .distributedBy(16, "instrument_token")
                .build();
    }

    private static void dropTable(Admin admin, TablePath path) throws Exception {
        admin.dropTable(path, true).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    /** Wait until a live leader serves every bucket (placement is not instant on this cluster). */
    private static void awaitServing(Admin admin, TablePath path, String name) throws Exception {
        int bucketCount = admin.getTableInfo(path).get(TIMEOUT_MS, TimeUnit.MILLISECONDS).getNumBuckets();
        List<Integer> buckets = new ArrayList<>(bucketCount);
        for (int b = 0; b < bucketCount; b++) {
            buckets.add(b);
        }
        long deadline = System.currentTimeMillis() + 120_000;
        Throwable lastFailure = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                admin.listOffsets(path, buckets, new LatestSpec())
                        .all()
                        .get(5, TimeUnit.SECONDS);
                return;
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                lastFailure = e;
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("no leader serves every bucket of " + name + " after 120s",
                lastFailure);
    }

    /** Wait until the server's row count agrees with what was written. */
    private static void awaitRowCount(Admin admin, TablePath path, long expected) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000;
        long last = -1;
        while (System.currentTimeMillis() < deadline) {
            last = admin.getTableStats(path).get(TIMEOUT_MS, TimeUnit.MILLISECONDS).getRowCount();
            if (last == expected) {
                return;
            }
            Thread.sleep(250);
        }
        throw new IllegalStateException(
                "server reports " + last + " rows for " + path + ", wrote " + expected);
    }

    private static int intValue(String[] args, int i, String flag) {
        if (i >= args.length) {
            usage(flag + " needs a value");
        }
        try {
            return Integer.parseInt(args[i]);
        } catch (NumberFormatException e) {
            usage(flag + " must be an integer, got '" + args[i] + "'");
            return -1;
        }
    }

    private static String stringValue(String[] args, int i, String flag) {
        if (i >= args.length) {
            usage(flag + " needs a value");
        }
        return args[i];
    }

    private static void usage(String problem) {
        System.err.println("FeatureSpikeProbe: " + problem);
        System.err.println("usage: FeatureSpikeProbe [run|drop] [--windows N] [--features N]"
                + " [--reads N] [--bootstrap HOST:PORT]");
        System.exit(2);
    }
}
