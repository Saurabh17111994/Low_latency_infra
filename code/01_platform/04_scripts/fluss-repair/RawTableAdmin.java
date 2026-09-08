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
 *   add-partition <name>     createPartition(ignoreIfExists=true) fallback
 *                            if client dynamic partitioning is ever disabled
 * Schema derives from RawTableSchema (single source of truth).
 * Usage: java -cp "<out>:$(cat code/02_services/01_ingestion/target/cp.txt)" RawTableAdmin <subcommand>
 */
public class RawTableAdmin {

    private static final TablePath PATH = TablePath.of("default", RawTableSchema.TABLE);

    public static void main(String[] args) throws Exception {
        String cmd = args.length > 0 ? args[0] : "partitions";
        String bootstrap = args.length > 1 ? args[1] : "localhost:9123";
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        try (Connection conn = ConnectionFactory.createConnection(conf);
             Admin admin = conn.getAdmin()) {
            switch (cmd) {
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
                    if (args.length < 2) { throw new IllegalArgumentException("usage: add-partition <name>"); }
                    admin.createPartition(PATH,
                            ResolvedPartitionSpec.fromPartitionName(
                                    java.util.List.of("event_day"), args[1]).toPartitionSpec(),
                            true).get();
                    System.out.println("PARTITION OK: " + args[1]);
                    break;
                default:
                    throw new IllegalArgumentException("unknown subcommand: " + cmd);
            }
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
