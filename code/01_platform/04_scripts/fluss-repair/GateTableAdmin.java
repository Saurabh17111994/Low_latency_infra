import com.trading.common.schema.ddl.DdlText;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.TreeMap;

/**
 * GateTableAdmin (2026-09-11, CHG-122) — mechanical admin for the Execution_Gate v4
 * recreate (P3-010/P3-012/P3-013).
 *
 * <p>Why this exists: {@code DdlApplyTool} only ever CREATEs and refuses a non-empty
 * catalog, and Fluss has no ALTER, so adopting the VERSIONED merge engine on
 * {@code fence_token} requires a drop + create. Modelled on the CHG-117
 * {@code RawTableAdmin} used to recreate {@code raw_table_1} as v3.
 *
 * <p>Unlike {@code RawTableAdmin}, the descriptor is built from the DDL file itself via
 * {@link DdlText} — the same parser the apply contract uses — so the recreated table
 * cannot drift from {@code 11_execution_gate.sql}, including the merge-engine options
 * and the datalake settings. {@code forceDatalakeDisabled=false} honors the blueprint
 * lake config rather than the dev-cluster deviation.
 *
 * <p>Subcommands:
 * <pre>
 *   show       print the options DdlText derived from the DDL (no cluster needed)
 *   drop       drop default.Execution_Gate (ignoreIfNotExists=true)
 *   create     create from the DDL descriptor
 *   recreate   drop then create — the CHG-122 migration step
 * </pre>
 *
 * <p>Usage:
 * <pre>
 *   java -cp "code/common/target/classes:&lt;cp.txt&gt;" GateTableAdmin show
 *   java -cp "code/common/target/classes:&lt;cp.txt&gt;" GateTableAdmin recreate localhost:9123
 * </pre>
 *
 * <p>WARNING: recreating loses the gate KV fence state (audit survives in the Iceberg
 * lake and the store's audit log). Archive the lake prefix first — a recreate against a
 * stale iceberg dir throws {@code LakeTableAlreadyExistException}. See the CHG-122
 * recreate runbook.
 */
public class GateTableAdmin {

    private static final String DEFAULT_DDL = "code/01_platform/02_sql/ddl/11_execution_gate.sql";

    public static void main(String[] args) throws Exception {
        String cmd = args.length > 0 ? args[0] : "show";
        String bootstrap = args.length > 1 ? args[1] : "localhost:9123";
        String ddlPath = args.length > 2 ? args[2] : DEFAULT_DDL;

        Path ddl = Path.of(ddlPath);
        if (!Files.exists(ddl)) {
            throw new IllegalArgumentException("DDL not found: " + ddl.toAbsolutePath()
                    + " (run from the repo root, or pass the path as arg 3)");
        }
        DdlText.ParsedDdl parsed = DdlText.parse(
                Files.readString(ddl, StandardCharsets.UTF_8), ddlPath);
        TableDescriptor descriptor = DdlText.toDescriptor(parsed, false);
        TablePath path = TablePath.of("default", parsed.tableName());

        if (cmd.equals("show")) {
            System.out.println("TABLE  " + path);
            System.out.println("KEYS   " + parsed.primaryKey()
                    + "  buckets=" + parsed.bucketCount() + " key=" + parsed.bucketKey());
            System.out.println("OPTIONS (from " + ddlPath + "):");
            new TreeMap<>(parsed.options()).forEach((k, v) -> System.out.println("  " + k + " = " + v));
            return;
        }

        assertMergeEnginePresent(parsed);

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        try (Connection conn = ConnectionFactory.createConnection(conf);
             Admin admin = conn.getAdmin()) {
            switch (cmd) {
                case "drop":
                    admin.dropTable(path, true).get();
                    System.out.println("DROP OK: " + path);
                    break;
                case "create":
                    admin.createTable(path, descriptor, false).get();
                    System.out.println("CREATE OK: " + path + " (VERSIONED on fence_token)");
                    break;
                case "recreate":
                    System.out.println("WARNING: dropping " + path
                            + " destroys the gate KV fence state (audit survives in the lake).");
                    admin.dropTable(path, true).get();
                    System.out.println("DROP OK: " + path);
                    admin.createTable(path, descriptor, false).get();
                    System.out.println("CREATE OK: " + path + " (VERSIONED on fence_token)");
                    break;
                default:
                    throw new IllegalArgumentException("unknown subcommand: " + cmd);
            }
        }
    }

    /**
     * Fail loud if the DDL no longer declares the merge engine. The whole point of the
     * recreate is the durable write-ordering, and silently rebuilding an LWW table would
     * leave the fence clobber in place while looking like a successful migration.
     */
    private static void assertMergeEnginePresent(DdlText.ParsedDdl parsed) {
        String engine = parsed.options().get("table.merge-engine");
        String verColumn = parsed.options().get("table.merge-engine.versioned.ver-column");
        if (!"versioned".equals(engine) || !"fence_token".equals(verColumn)) {
            throw new IllegalStateException("refusing to touch the cluster: "
                    + parsed.sourcePath() + " must declare table.merge-engine=versioned with"
                    + " ver-column=fence_token, got engine=" + engine + " ver-column=" + verColumn);
        }
    }
}
