import com.trading.common.schema.RawTableSchema;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

/**
 * RawTableAdmin (2026-08-31) — mechanical admin for the daily-partition
 * migration (docs/06_operations/07-lake-archive-ops.md (M-15/M-16 rows)).
 * Subcommands:
 *   drop                     drop default.raw_table_1 (ignoreIfNotExists=true)
 *   create                   create v3 partitioned table (options mirror
 *                            code/01_platform/02_sql/ddl/02_raw_table_1.sql)
 *   partitions               print partition names, one per line
 *   add-partition <yyyyMMdd>  createPartition(ignoreIfExists=true) fallback
 *                            if client dynamic partitioning is ever disabled
 * Schema derives from RawTableSchema (single source of truth).
 * Usage: java -cp "<out>:$(cat code/02_services/01_ingestion/target/cp.txt)" RawTableAdmin
 *            <subcommand> [args] [--bootstrap HOST:PORT]
 * The bootstrap address is an OPTION, never a positional argument (P6-004): the old
 * `args[1]`-is-bootstrap rule meant `add-partition 20260101` dialled `20260101` and
 * left no way to pass both a partition and a bootstrap address.
 */
public class RawTableAdmin {

    private static final TablePath PATH = TablePath.of("default", RawTableSchema.TABLE);
    private static final String DEFAULT_BOOTSTRAP = "localhost:9123";
    private static final String USAGE =
            "usage: RawTableAdmin <drop|create|partitions|add-partition <yyyyMMdd>> [--bootstrap HOST:PORT]";

    public static void main(String[] args) throws Exception {
        // P6-384: parse and validate BEFORE opening a connection, so a typo fails
        // instantly instead of after a cluster round-trip.
        Command cmd;
        try {
            cmd = parse(args);
        } catch (IllegalArgumentException ex) {
            System.err.println("ERROR: " + ex.getMessage());
            System.err.println(USAGE);
            System.exit(2);
            return;
        }
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", cmd.bootstrap());
        try (Connection conn = ConnectionFactory.createConnection(conf);
             Admin admin = conn.getAdmin()) {
            switch (cmd.name()) {
                case "drop":
                    admin.dropTable(PATH, true).get();
                    System.out.println("DROP OK: " + PATH);
                    break;
                case "create":
                    admin.createTable(PATH, descriptor(), false).get();
                    System.out.println("CREATE OK: " + PATH + " (partitioned by event_day)");
                    break;
                case "partitions":
                    for (PartitionInfo p : admin.listPartitionInfos(PATH).get()) {
                        System.out.println(p.getPartitionName());
                    }
                    break;
                case "add-partition":
                    admin.createPartition(PATH,
                            ResolvedPartitionSpec.fromPartitionName(
                                    java.util.List.of("event_day"), cmd.partition()).toPartitionSpec(),
                            true).get();
                    // P6-385: report the partition that was created, not argv[1].
                    System.out.println("PARTITION OK: " + cmd.partition());
                    break;
                default:
                    throw new IllegalStateException("parser accepted unknown subcommand: " + cmd.name());
            }
        }
    }

    /** A fully validated invocation: nothing here needs a cluster to check. */
    record Command(String name, String partition, String bootstrap) {}

    static Command parse(String[] args) {
        String name = args.length > 0 ? args[0] : "partitions";
        String bootstrap = DEFAULT_BOOTSTRAP;
        java.util.List<String> rest = new java.util.ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            if ("--bootstrap".equals(args[i])) {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("--bootstrap needs HOST:PORT");
                }
                bootstrap = args[++i];
            } else {
                rest.add(args[i]);
            }
        }
        String partition = null;
        switch (name) {
            case "drop", "create", "partitions":
                if (!rest.isEmpty()) {
                    throw new IllegalArgumentException(name + " takes no arguments, got: " + rest);
                }
                break;
            case "add-partition":
                if (rest.size() != 1) {
                    throw new IllegalArgumentException("add-partition needs exactly one <yyyyMMdd>");
                }
                partition = rest.get(0);
                requireDay(partition);
                break;
            default:
                throw new IllegalArgumentException("unknown subcommand: " + name);
        }
        return new Command(name, partition, bootstrap);
    }

    /**
     * P6-091: the partition name is a Fluss partition value, not free text. A wrong
     * format silently creates a second, non-auto partition that nothing writes to,
     * so the shape AND the calendar date are checked here.
     */
    static void requireDay(String value) {
        try {
            java.time.LocalDate.parse(value, java.time.format.DateTimeFormatter.BASIC_ISO_DATE);
        } catch (java.time.format.DateTimeParseException ex) {
            throw new IllegalArgumentException("partition must be a real yyyyMMdd date, got: " + value);
        }
    }

    private static TableDescriptor descriptor() {
        Schema.Builder sb = Schema.newBuilder();
        for (int i = 0; i < RawTableSchema.FIELD_COUNT; i++) {
            sb.column(RawTableSchema.COLUMNS.get(i), toType(RawTableSchema.COLUMN_TYPE_ROOTS.get(i)));
        }
        return TableDescriptor.builder()
                .schema(sb.build())
                .partitionedBy(RawTableSchema.PARTITION_KEY)
                .distributedBy(RawTableSchema.BUCKET_COUNT, RawTableSchema.BUCKET_KEY)
                .property("table.log.ttl", RawTableSchema.LOG_TTL)
                .property("table.auto-partition.enabled", "true")
                .property("table.auto-partition.time-unit", "DAY")
                .property("table.auto-partition.num-precreate", "2")
                .property("table.auto-partition.num-retention", RawTableSchema.PARTITION_RETENTION)
                .property("table.auto-partition.time-zone", "Asia/Kolkata")
                .property("table.datalake.enabled", "true")
                .property("table.datalake.format", "iceberg")
                .property("table.datalake.freshness", "5min")
                .property("table.datalake.auto-compaction", "true")
                .build();
    }

    private static DataType toType(String root) {
        switch (root) {
            case "STRING": return DataTypes.STRING();
            case "BIGINT": return DataTypes.BIGINT();
            case "BYTES":  return DataTypes.BYTES();
            default: throw new IllegalStateException("unsupported type root: " + root);
        }
    }
}
