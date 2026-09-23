import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
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
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;

/**
 * FlussKvScanStrategy - A6 measurement: KV scan throughput for the two native scan strategies.
 *
 * <p>The adoption plan's A6 asks whether {@code client.scanner.kv.batch-strategy} can retire the
 * hand-rolled parallel bucket copy (CHG-099: per-row {@code .get()} at ~9.6 rows/s). This measures
 * both native strategies on ONE fixture of known size:
 *
 * <pre>
 *   snapshot-merge  default; merges the latest KV snapshot with the bounded changelog range,
 *                   resumable, single point in time
 *   server-scan     scans live KV state on the tablet, skipping snapshot download and changelog
 *                   replay; NOT resumable, each bucket read at its scanner's open time
 * </pre>
 *
 * <p><b>Negative control.</b> Any other strategy value MUST be rejected by the client. If a bogus
 * value is accepted silently then the config key is wrong, every timing printed is void, and the
 * measurement says nothing about the native path. (Silent-no-op class: this project has now been
 * bitten three times by measuring a knob nobody read.)
 *
 * <p><b>Full scan, not a sample.</b> {@code FlussSignalLatency}'s helper caps rows per bucket and
 * breaks after consecutive empty polls - both right for latency sampling, both wrong here, because
 * a paged read can return only a bucket's first stored segment (CHG-221) and a truncated scan is
 * fast for the wrong reason. This drains until {@code pollBatch} returns null (the documented
 * end-of-input) and then ASSERTS the row count.
 *
 * <p>usage: {@code FlussKvScanStrategy <snapshot-merge|server-scan|drop|control> [bootstrap]
 * [rows] [buckets]}
 *
 * <p>exit: 0 = agreement (or control rejected); 3 = count mismatch; 4 = bogus strategy accepted;
 * 5 = a documented strategy was rejected.
 */
public class FlussKvScanStrategy {

    private static final String TABLE = "zz_a6_scan_fixture";
    private static final TablePath PATH = TablePath.of("default", TABLE);
    private static final String STRATEGY_KEY = "client.scanner.kv.batch-strategy";
    private static final String CONTROL = "bogus-strategy";
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(30);
    private static final long OP_TIMEOUT_MS = 30_000;

    private FlussKvScanStrategy() {}

    public static void main(String[] args) throws Exception {
        String strategy = args.length > 0 ? args[0] : "";
        String bootstrap = args.length > 1 ? args[1] : "localhost:9123";
        int rows = args.length > 2 ? Integer.parseInt(args[2]) : 2000;
        int buckets = args.length > 3 ? Integer.parseInt(args[3]) : 8;
        if (strategy.isEmpty()) {
            System.err.println("usage: FlussKvScanStrategy <snapshot-merge|server-scan|drop|control>"
                    + " [bootstrap] [rows] [buckets]");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        conf.setString(STRATEGY_KEY, strategy); // the knob under test

        try (Connection conn = ConnectionFactory.createConnection(conf);
                Admin admin = conn.getAdmin()) {
            if (strategy.equals("drop")) {
                admin.dropTable(PATH, true).get(OP_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                System.out.println("DROP=ok table=" + TABLE);
                return;
            }
            ensureFixture(admin, conn, rows, buckets);
            scan(conn, admin, strategy, rows);
        } catch (Exception e) {
            // Rejection is the EXPECTED outcome for the control, and the honest answer for a typo:
            // either way it proves the client reads the key.
            System.out.println("STRATEGY=" + strategy);
            System.out.println("CONTROL=rejected");
            System.out.println("DETAIL=" + e.getClass().getSimpleName() + ": " + firstLine(e.getMessage()));
            System.out.println("RESULT=" + (strategy.equals(CONTROL)
                    ? "PASS (control rejected: the config key is live)"
                    : "FAIL (a documented strategy was rejected)"));
            System.exit(strategy.equals(CONTROL) ? 0 : 5);
        }
    }

    /** Seeds only when the fixture is missing: re-seeding per pass would fold write time into the scan. */
    private static void ensureFixture(Admin admin, Connection conn, int rows, int buckets)
            throws Exception {
        if (fixtureExists(admin)) {
            System.out.println("fixture=reused");
            return;
        }
        long t0 = System.nanoTime();
        admin.createTable(PATH, descriptor(buckets), false).get(OP_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        try (Table table = conn.getTable(PATH)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (int i = 0; i < rows; i++) {
                // One ack per row: UpsertWriter is not Closeable in this version, so this .get() is
                // the write (ProbeFixtureSeeder's rule) - an unbounded flush would hide that.
                writer.upsert(GenericRow.of(BinaryString.fromString(key(i))))
                        .get(OP_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }
        }
        System.out.println("fixture=seeded rows=" + rows + " buckets=" + buckets
                + " seed_ms=" + ((System.nanoTime() - t0) / 1_000_000));
    }

    private static String key(int i) {
        return String.format("k%06d", i);
    }

    private static boolean fixtureExists(Admin admin) {
        try {
            admin.getTableInfo(PATH).get(OP_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /** The three properties a raw-client KV scratch table needs here (ProbeFixtureSeeder's shape). */
    private static TableDescriptor descriptor(int buckets) {
        return TableDescriptor.builder()
                .schema(Schema.newBuilder()
                        .column("candidate_id", DataTypes.STRING())
                        .primaryKey("candidate_id")
                        .build())
                .property("table.datalake.enabled", "false") // a scratch table must not write to the lake
                .property("table.datalake.format", "iceberg")
                .property("table.kv.format-version", "2")
                .distributedBy(buckets, "candidate_id")
                .build();
    }

    /** Full, uncapped scan of every bucket. No .limit(), no empty-poll early exit - see the class doc. */
    private static void scan(Connection conn, Admin admin, String strategy, int rows) throws Exception {
        if (!strategy.equals("snapshot-merge") && !strategy.equals("server-scan")) {
            // Reaching here means the client accepted a strategy it does not document, so the knob
            // is not being read and the timings would be meaningless.
            System.out.println("STRATEGY=" + strategy);
            System.out.println("CONTROL=accepted");
            System.out.println("RESULT=FAIL (bogus strategy accepted: the config key is not read)");
            System.exit(4);
        }
        TableInfo info = admin.getTableInfo(PATH).get(OP_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        long read = 0;
        long t0 = System.nanoTime();
        try (Table table = conn.getTable(PATH)) {
            for (int b = 0; b < info.getNumBuckets(); b++) {
                TableBucket tb = new TableBucket(info.getTableId(), b);
                try (BatchScanner scanner = table.newScan().createBatchScanner(tb)) {
                    while (true) {
                        try (CloseableIterator<InternalRow> batch = scanner.pollBatch(POLL_TIMEOUT)) {
                            if (batch == null) {
                                break; // end of input, per the BatchScanner contract
                            }
                            while (batch.hasNext()) {
                                batch.next();
                                read++;
                            }
                        }
                    }
                }
            }
        }
        long ms = (System.nanoTime() - t0) / 1_000_000;
        boolean ok = read == rows;
        System.out.println("STRATEGY=" + strategy);
        System.out.println("ROWS_READ=" + read);
        System.out.println("EXPECTED=" + rows);
        System.out.println("BUCKETS=" + info.getNumBuckets());
        System.out.println("SCAN_MS=" + ms);
        System.out.println("ROWS_PER_S=" + (ms > 0 ? (read * 1000L / ms) : -1));
        System.out.println("COUNT_OK=" + (ok ? "yes" : "no"));
        System.out.println("RESULT=" + (ok ? "PASS" : "FAIL count mismatch"));
        if (!ok) {
            System.exit(3);
        }
    }

    private static String firstLine(String s) {
        if (s == null) {
            return "(no message)";
        }
        int nl = s.indexOf('\n');
        return nl < 0 ? s : s.substring(0, nl);
    }
}
