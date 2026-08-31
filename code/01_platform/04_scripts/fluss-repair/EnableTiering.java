import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TablePath;

import java.util.List;

/**
 * EnableTiering.java (2026-08-31) — one-off admin tool: set
 * 'table.datalake.enabled'='true' on a table via ALTER TABLE.
 *
 * WHY: the 28-table corpus was applied during a post-reboot restack while
 * the coordinator had NO datalake.format (R2 config came after). Fluss
 * rejects CREATE with datalake.enabled=true on a cluster without a datalake
 * format, so the tables landed with enabled=false (verified in ZK:
 * raw_table_1 had 'table.datalake.enabled': 'false'). The tiering service
 * then polled forever with "No available Tiering table found" because the
 * coordinator never tracked the table.
 *
 * table.datalake.enabled is an ALTERABLE option (FlussConfigUtils; only
 * enabled/freshness/tiered-log-local-segments are), so the fix is ALTER,
 * not recreate. On alter, MetadataManager registers the table with
 * LakeTableTieringManager and the tiering service picks it up after the
 * table's freshness window (5min default) on its next 30s poll.
 *
 * Usage (compile once against the ingestion module's classpath — it already
 * carries the fluss client jars; then run from the host, bootstrap is the
 * coordinator's published port):
 *   javac -cp "$(cat code/02_services/01_ingestion/target/cp.txt)" \
 *     -d /tmp/tiering-tools EnableTiering.java
 *   java -cp "/tmp/tiering-tools:$(cat code/02_services/01_ingestion/target/cp.txt)" \
 *     EnableTiering raw_table_1 localhost:9123
 *
 * NOTE: 'table.datalake.format' is NOT alterable (InvalidAlterTableException)
 * — the format comes from the cluster's datalake.format config.
 * NOTE: if the coordinator throws LakeTableAlreadyExistException, a stale
 * iceberg table from a PRE-WIPE cluster occupies the R2 path (new table id,
 * same name). Move the stale objects aside first (archive, don't delete):
 * GET+PUT each object to lake/_stale-<date>/<table>/ then DELETE the
 * originals (plain GET/PUT avoids R2's CopyObject SigV4 quirks).
 * NOTE: R2 rejects AWS region names in the SigV4 scope (InvalidRegionName
 * -> HTTP 400 on every s3a call) — AWS_REGION must be auto/apac/eeur/enam.
 */
public class EnableTiering {
    public static void main(String[] args) throws Exception {
        String table = args[0];
        String bootstrap = args.length > 1 ? args[1] : "localhost:9123";
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        try (Connection conn = ConnectionFactory.createConnection(conf);
                Admin admin = conn.getAdmin()) {
            admin.alterTable(
                            TablePath.of("default", table),
                            List.of(TableChange.set("table.datalake.enabled", "true")),
                            false)
                    .get();
            System.out.println("ALTER OK: " + table + " datalake.enabled=true");
        }
    }
}
