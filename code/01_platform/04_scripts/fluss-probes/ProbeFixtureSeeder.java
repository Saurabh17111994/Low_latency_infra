import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * ProbeFixtureSeeder — the deterministic KV fixture the census probes are checked against
 * (CHG-221).
 *
 * <p>Why this exists: {@link FlussSignalLatency}'s census refuses to print a total when its own
 * read disagrees with the server's row count, and on the live tables it always disagrees — a
 * paged log read of {@code Execution_Intent} returns a bucket's first stored segment, so the
 * contract legs on those tables can only assert the refusal, never the agreement they are named
 * for. A KV table reads as an exact snapshot ({@code checkAgainstServer} labels it "KV
 * snapshot"), so on a fixture of known size the agreement is reachable — and that is what
 * {@code SignalLatencyContractTests} now checks instead of skipping.
 *
 * <p>The row sets are chosen so every answer is non-trivial:
 *
 * <pre>
 *   zz_probe_fixture_intent   fx-a fx-b fx-c fx-d fx-e    (5 rows)
 *   zz_probe_fixture_signal   fx-a fx-b fx-x              (3 rows)
 *   orphans = intent \ signal = fx-c fx-d fx-e            (3, not 0 and not 5)
 * </pre>
 *
 * <p>Table shape: one STRING column, the primary key, so the bucket key is the whole PK — the
 * single-field case the pinned composite-key matrix (COMPAT-FLUSS-005) documents as writable by
 * the raw client, along with {@code table.kv.format-version=2} and the explicit iceberg datalake
 * format this cluster inherits. Tiering is off: a scratch table must not write to the lake.
 *
 * <p>{@code create} drops first, seeds, and then WAITS until the server's own row count matches
 * what it wrote. That wait is the point: the probe compares against that count, so a fixture
 * whose count lags would produce a refusal (read != server) or, worse, a flaky green. If the
 * count never agrees the seeder fails loudly — a real finding about the count path, not test
 * flakiness.
 *
 * <p>These tables live in the live catalog, so both subcommands are idempotent: {@code create}
 * drops before creating, and the test drops them in a {@code finally}-equivalent cleanup even
 * when it fails. A leaked fixture would show up in the catalog guard's count.
 *
 * <p>Usage: {@code java -cp <FLUSS_PROBE_CP>:. ProbeFixtureSeeder create|drop [bootstrap]}
 */
public final class ProbeFixtureSeeder {

    static final String INTENT_TABLE = "zz_probe_fixture_intent";
    static final String SIGNAL_TABLE = "zz_probe_fixture_signal";
    static final List<String> INTENT_IDS = List.of("fx-a", "fx-b", "fx-c", "fx-d", "fx-e");
    static final List<String> SIGNAL_IDS = List.of("fx-a", "fx-b", "fx-x");
    /** Printed so the test's expected orphan count has one source, in the fixture itself. */
    static final int ORPHAN_INTENTS = 3;

    private static final String DEFAULT_BOOTSTRAP = "localhost:9123";
    private static final long TIMEOUT_MS = 30_000;
    /** How long the server's own row count may take to agree before that is a failure. */
    private static final long COUNT_WAIT_MS = 20_000;
    private static final long COUNT_POLL_MS = 250;

    private ProbeFixtureSeeder() {}

    public static void main(String[] args) throws Exception {
        String cmd = args.length > 0 ? args[0] : "";
        if (!cmd.equals("create") && !cmd.equals("drop")) {
            System.err.println("usage: ProbeFixtureSeeder create|drop [bootstrap]");
            System.err.println("  create (re)creates " + INTENT_TABLE + " ("
                    + INTENT_IDS.size() + " rows) and " + SIGNAL_TABLE + " (" + SIGNAL_IDS.size()
                    + " rows) and waits for the server's row count to agree; drop removes both.");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", args.length > 1 ? args[1] : DEFAULT_BOOTSTRAP);
        try (Connection conn = ConnectionFactory.createConnection(conf);
                Admin admin = conn.getAdmin()) {
            if (cmd.equals("drop")) {
                drop(admin, INTENT_TABLE);
                drop(admin, SIGNAL_TABLE);
                System.out.println("fixture: dropped " + INTENT_TABLE + " " + SIGNAL_TABLE);
                return;
            }
            // Drop first: a previous run may have died between create and cleanup, and
            // createTable on an existing path fails rather than replacing it.
            drop(admin, INTENT_TABLE);
            drop(admin, SIGNAL_TABLE);
            seed(admin, conn, INTENT_TABLE, INTENT_IDS);
            seed(admin, conn, SIGNAL_TABLE, SIGNAL_IDS);
            System.out.println("fixture: intent_table=" + INTENT_TABLE
                    + " intent_rows=" + INTENT_IDS.size()
                    + " signal_table=" + SIGNAL_TABLE
                    + " signal_rows=" + SIGNAL_IDS.size()
                    + " orphan_intents=" + ORPHAN_INTENTS);
        }
    }

    private static void seed(Admin admin, Connection conn, String name, List<String> ids)
            throws Exception {
        TablePath path = TablePath.of("default", name);
        admin.createTable(path, descriptor(), false).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        try (Table table = conn.getTable(path)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (String id : ids) {
                // Bounded, like the pinned matrix's cell writes: UpsertWriter is not
                // Closeable in this Fluss version, so this ack IS the whole write, and an
                // unbounded flush would mask the very timeout it looked like it guarded.
                writer.upsert(GenericRow.of(BinaryString.fromString(id)))
                        .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }
        }
        awaitServerRowCount(admin, path, ids.size());
    }

    /** The three properties a raw-client KV scratch table needs on this cluster. */
    private static TableDescriptor descriptor() {
        return TableDescriptor.builder()
                .schema(Schema.newBuilder()
                        .column("candidate_id", DataTypes.STRING())
                        .primaryKey("candidate_id")
                        .build())
                .property("table.datalake.enabled", "false")
                .property("table.datalake.format", "iceberg")
                .property("table.kv.format-version", "2")
                .distributedBy(1, "candidate_id")
                .build();
    }

    private static void drop(Admin admin, String name) throws Exception {
        // ignoreIfNotExists: `create` drops before creating, and the cleanup may run
        // after a create that never got that far.
        admin.dropTable(TablePath.of("default", name), true)
                .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Wait until {@code Admin.getTableStats} agrees with what was written — the count the probe
     * compares its read against. Never time out quietly: a stale count is the difference between
     * a refusal and a verdict.
     */
    private static void awaitServerRowCount(Admin admin, TablePath path, long expected)
            throws Exception {
        long deadline = System.currentTimeMillis() + COUNT_WAIT_MS;
        long last = -1;
        while (System.currentTimeMillis() < deadline) {
            last = admin.getTableStats(path).get(TIMEOUT_MS, TimeUnit.MILLISECONDS).getRowCount();
            if (last == expected) {
                return;
            }
            Thread.sleep(COUNT_POLL_MS);
        }
        throw new IllegalStateException("the server reports " + last + " rows for " + path
                + " but the fixture wrote " + expected + " within " + COUNT_WAIT_MS + "ms — the"
                + " census compares its read against that count, so this fixture cannot be used"
                + " until they agree (and a count that never converges is its own finding)");
    }
}
