import com.trading.common.schema.RawTableSchema;
import java.util.List;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

/**
 * RawTableAlter (2026-09-24) -- widen a live raw_table_1 to the current contract.
 *
 * <p>Why this exists: the DDL applier creates tables that are absent
 * ({@code DdlApplyTool} "never drops"), so it silently skips an existing
 * raw_table_1, and the only other repair tool drops/recreates (which on this
 * table means archiving 200 MB+/day of live rows first). A schema upgrade that
 * only APPENDS columns (the v4 full-mode field capture: 51 columns after
 * schema_version) is an ALTER, not a recreate.
 *
 * <p>Idempotent and resumable: {@code apply} adds the columns the live table is
 * missing, one ALTER at a time, printing each column as it lands, so an
 * interrupted run is safe to repeat. {@code plan} prints the same diff without
 * changing anything.
 *
 * <p>Safety rail: the live table's existing columns must still equal
 * {@code RawTableSchema.COLUMNS.subList(0, live_count)} and live_count must be at
 * least {@code FROZEN_PREFIX_COLUMNS}. A table that is not this contract's prefix
 * is refused rather than widened, because appending to the wrong table is a
 * data-shape bug, not a repair.
 *
 * <p>Usage: {@code java -cp "<out>:$(cat code/02_services/01_ingestion/target/cp.txt)" \
 *   RawTableAlter <plan|apply|verify> [--bootstrap HOST:PORT]}
 */
public class RawTableAlter {

    private static final TablePath PATH = TablePath.of("default", RawTableSchema.TABLE);
    private static final String DEFAULT_BOOTSTRAP = "localhost:9123";
    private static final String USAGE =
            "usage: RawTableAlter <plan|apply|verify> [--bootstrap HOST:PORT]";

    public static void main(String[] args) throws Exception {
        // P6-384 convention: validate argv before opening a connection.
        String cmd = null;
        String bootstrap = DEFAULT_BOOTSTRAP;
        for (int i = 0; i < args.length; i++) {
            if ("--bootstrap".equals(args[i])) {
                if (i + 1 >= args.length) {
                    fail("--bootstrap needs HOST:PORT");
                }
                bootstrap = args[++i];
            } else if (cmd == null) {
                cmd = args[i];
            } else {
                fail("unexpected argument: " + args[i]);
            }
        }
        if (cmd == null || !List.of("plan", "apply", "verify").contains(cmd)) {
            fail("expected one of plan|apply|verify");
        }

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        try (Connection conn = ConnectionFactory.createConnection(conf);
             Admin admin = conn.getAdmin()) {
            List<String> live = liveColumns(conn);
            System.out.println("live: " + live.size() + " columns, schemaId=" + schemaId(conn));

            // A live table is only ever widened if it is still this contract's prefix.
            for (int i = 0; i < Math.min(live.size(), RawTableSchema.COLUMNS.size()); i++) {
                if (!live.get(i).equals(RawTableSchema.COLUMNS.get(i))) {
                    fail("live column #" + i + " is '" + live.get(i) + "' but the contract has '"
                            + RawTableSchema.COLUMNS.get(i) + "' — refusing to alter a table that "
                            + "does not match this schema's prefix");
                }
            }
            if (live.size() < RawTableSchema.FROZEN_PREFIX_COLUMNS) {
                fail("live table has " + live.size() + " columns, below the frozen prefix of "
                        + RawTableSchema.FROZEN_PREFIX_COLUMNS + " — this does not look like "
                        + RawTableSchema.TABLE);
            }
            if (live.size() > RawTableSchema.COLUMNS.size()) {
                fail("live table has " + live.size() + " columns, more than the contract's "
                        + RawTableSchema.COLUMNS.size() + " — the contract is behind the table");
            }

            List<String> missing = RawTableSchema.COLUMNS.subList(live.size(), RawTableSchema.COLUMNS.size());
            System.out.println("missing: " + missing.size() + " column(s)"
                    + (missing.isEmpty() ? "" : " — " + missing.get(0) + " .. " + missing.get(missing.size() - 1)));

            switch (cmd) {
                case "plan":
                    for (String col : missing) {
                        System.out.println("  would add " + col + " BIGINT NULL");
                    }
                    if (missing.isEmpty()) {
                        System.out.println("IN SYNC: nothing to do");
                    }
                    break;
                case "apply":
                    if (missing.isEmpty()) {
                        System.out.println("IN SYNC: nothing to do");
                        break;
                    }
                    for (int i = live.size(); i < RawTableSchema.COLUMNS.size(); i++) {
                        String col = RawTableSchema.COLUMNS.get(i);
                        admin.alterTable(PATH, List.of(TableChange.addColumn(
                                        col, typeFor(i), "v4 full-mode field capture",
                                        TableChange.ColumnPosition.last())), true)
                                .get();
                        System.out.println("ALTER OK " + i + " " + col);
                    }
                    System.out.println("APPLIED " + missing.size() + " column(s)");
                    report(conn);
                    break;
                default: // verify
                    report(conn);
                    break;
            }
        }
    }

    private static void report(Connection conn) throws Exception {
        List<String> now = liveColumns(conn);
        System.out.println("RESULT columns=" + now.size() + " schemaId=" + schemaId(conn));
        System.out.println("RESULT tail=" + now.subList(Math.max(0, now.size() - 3), now.size()));
        System.out.println("RESULT expected=" + RawTableSchema.COLUMNS.size()
                + " match=" + now.equals(RawTableSchema.COLUMNS));
    }

    private static List<String> liveColumns(Connection conn) throws Exception {
        return conn.getTable(PATH).getTableInfo().getRowType().getFieldNames();
    }

    private static int schemaId(Connection conn) throws Exception {
        return conn.getTable(PATH).getTableInfo().getSchemaId();
    }

    private static DataType typeFor(int index) {
        String root = RawTableSchema.COLUMN_TYPE_ROOTS.get(index);
        switch (root) {
            case "STRING":
                return DataTypes.STRING();
            case "BIGINT":
                return DataTypes.BIGINT();
            case "BYTES":
                return DataTypes.BYTES();
            default:
                throw new IllegalStateException("unsupported type root: " + root);
        }
    }

    private static void fail(String message) {
        System.err.println("ERROR: " + message);
        System.err.println(USAGE);
        System.exit(2);
    }
}
