import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

/**
 * LegacyTableDropProbe — dev-only drop of the three retired candle/feature tables after the
 * Wave C cutover (CHG-481). The tables left every writer path at W-C6 (live-proven: +0 rows
 * while the merged table wrote +57,263), so this probe removes them from the dev cluster —
 * the sanctioned replacement for the retired DDL lifecycle (the DDL files themselves are
 * decommissioned in W-C5a; this probe never touches the corpus).
 *
 * <p>Modes:
 * <pre>
 *   --mode list            print each target: present (buckets/columns) or absent
 *   --mode drop --confirm  drop every present target, then read back; refuses without
 *                          --confirm (exit 3), and the target set is FIXED in code —
 *                          never parameterized, so a mistyped name cannot drop a live table
 * </pre>
 */
public final class LegacyTableDropProbe {

    private static final long TIMEOUT_MS = 60_000L;

    /** Fixed targets (W-C7): the three tables DEC-059 collapsed into candle_features. */
    static final List<String> TARGETS =
            List.of("candle_live", "candle_closed", "feature_values");

    public static void main(String[] args) throws Exception {
        String mode = null;
        String database = "default";
        String bootstrap = "fluss-coordinator:9123";
        boolean confirm = false;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--mode":
                    mode = args[++i];
                    break;
                case "--database":
                    database = args[++i];
                    break;
                case "--bootstrap":
                    bootstrap = args[++i];
                    break;
                case "--confirm":
                    confirm = true;
                    break;
                default:
                    usage("unknown argument: " + arg);
            }
        }
        if (mode == null) {
            usage("--mode is required");
        }
        if (!mode.equals("list") && !mode.equals("drop")) {
            usage("unknown mode: " + mode);
        }
        if (mode.equals("drop") && !confirm) {
            System.err.println("DROP-REFUSED: --mode drop requires --confirm (removes "
                    + TARGETS + " on the target cluster)");
            System.exit(3);
        }

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        try (Connection conn = ConnectionFactory.createConnection(conf);
                Admin admin = conn.getAdmin()) {
            switch (mode) {
                case "list":
                    list(admin, database);
                    break;
                case "drop":
                    drop(admin, database);
                    break;
                default:
                    usage("unknown mode: " + mode);
            }
        }
    }

    private static void list(Admin admin, String database) throws Exception {
        for (String name : TARGETS) {
            TablePath path = TablePath.of(database, name);
            if (exists(admin, path)) {
                TableInfo info = admin.getTableInfo(path).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
                System.out.println("LEGACY-DROP list=" + name + " present buckets="
                        + info.getNumBuckets() + " columns="
                        + info.getSchema().getColumns().size());
            } else {
                System.out.println("LEGACY-DROP list=" + name + " absent");
            }
        }
    }

    private static void drop(Admin admin, String database) throws Exception {
        for (String name : TARGETS) {
            TablePath path = TablePath.of(database, name);
            if (!exists(admin, path)) {
                System.out.println("LEGACY-DROP drop=" + name + " skipped reason=absent");
                continue;
            }
            admin.dropTable(path, false).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            System.out.println("LEGACY-DROP drop=" + name + " dropped=true");
        }
        System.out.println("LEGACY-DROP readback:");
        list(admin, database);
    }

    private static boolean exists(Admin admin, TablePath path) {
        try {
            admin.getTableInfo(path).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static void usage(String msg) {
        System.err.println("legacy-candle-tables-drop: " + msg);
        System.err.println("usage: LegacyTableDropProbe --mode list|drop [--confirm]"
                + " [--database DB] [--bootstrap host:port]");
        System.exit(2);
    }

    private LegacyTableDropProbe() {}
}
