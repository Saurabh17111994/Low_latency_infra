import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import java.util.ArrayList;
import java.util.List;

/**
 * SetRawRetention.java (2026-09-30, Wave A/A2) — one-off admin tool: set
 * 'table.log.ttl' and, on partitioned tables, 'table.auto-partition.num-retention'
 * in one ALTER, with a before/after readback.
 *
 * WHY: A2 lowers raw_table_1 from 9d/9 to 3d/3 (operator Q36) after the EOD
 * guard reconciliation. The EOD controller's extend path only RAISES retention
 * for unverified days; lowering is an operator action and had no tool (the DDL
 * applier skips existing tables; RawTableAlter is schema-only). Both options
 * are on Fluss 1.0.0's alterable list (A1/B6-E2 probes).
 *
 * Usage (compile against the ingestion module's classpath; run from the host):
 *   javac -cp "$(cat code/02_services/01_ingestion/target/cp.txt)" \
 *     -d /tmp/fluss-tools SetRawRetention.java
 *   java -cp "/tmp/fluss-tools:$(cat code/02_services/01_ingestion/target/cp.txt)" \
 *     SetRawRetention raw_table_1 3d 3 [--bootstrap localhost:9123]
 *
 * Prints the live ttl / num-retention / datalake.enabled before and after, so
 * the evidence lives in the run log.
 *
 * Revert (A2 rollback): run again with the previous pair — raw_table_1 9d 9.
 * The EOD controller may extend beyond whatever is set here on unverified days;
 * that is the guard, not drift.
 */
public class SetRawRetention {

    public static void main(String[] args) throws Exception {
        String table = null;
        String ttl = null;
        Integer retention = null;
        String bootstrap = "localhost:9123";
        for (int i = 0; i < args.length; i++) {
            if ("--bootstrap".equals(args[i])) {
                if (++i >= args.length) {
                    fail("--bootstrap needs HOST:PORT");
                }
                bootstrap = args[i];
            } else if (table == null) {
                table = args[i];
            } else if (ttl == null) {
                ttl = args[i];
            } else if (retention == null) {
                retention = Integer.parseInt(args[i]);
            } else {
                fail("unexpected argument: " + args[i]);
            }
        }
        if (table == null || ttl == null) {
            System.err.println("usage: SetRawRetention <table> <ttl> [num-retention-days]"
                    + " [--bootstrap HOST:PORT]");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        try (Connection conn = ConnectionFactory.createConnection(conf);
                Admin admin = conn.getAdmin()) {
            TablePath path = TablePath.of("default", table);
            TableInfo before = admin.getTableInfo(path).get();
            System.out.println("before: ttl=" + prop(before, "table.log.ttl")
                    + " num-retention=" + prop(before, "table.auto-partition.num-retention")
                    + " datalake.enabled=" + prop(before, "table.datalake.enabled")
                    + " partitions=" + before.getPartitionKeys());

            List<TableChange> changes = new ArrayList<>();
            changes.add(TableChange.set("table.log.ttl", ttl));
            boolean partitioned = before.getPartitionKeys() != null
                    && !before.getPartitionKeys().isEmpty();
            if (partitioned) {
                if (retention == null) {
                    fail("partitioned table " + table + " needs num-retention-days"
                            + " (both expiry clocks move together)");
                }
                changes.add(TableChange.set("table.auto-partition.num-retention",
                        retention.toString()));
            }
            admin.alterTable(path, changes, false).get();

            TableInfo after = admin.getTableInfo(path).get();
            System.out.println("ALTER OK: " + table + " ttl=" + prop(after, "table.log.ttl")
                    + " num-retention=" + prop(after, "table.auto-partition.num-retention")
                    + " changes=" + changes.size());
        }
    }

    private static String prop(TableInfo info, String key) {
        String v = info.getProperties().toMap().get(key);
        return v == null ? "-" : v;
    }

    private static void fail(String message) {
        System.err.println("ERROR: " + message);
        System.exit(2);
    }
}
