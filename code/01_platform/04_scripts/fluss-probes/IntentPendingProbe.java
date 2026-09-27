import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * IntentPendingProbe — Q4 hop-preflight reader: pending Execution_Intent rows.
 *
 * The gateway's IntentReader replays Execution_Intent from offset zero on every
 * boot and forwards a row whose (instruction_id, request_hash) is not in the
 * durable Execution_Intent_Processed commit index. The gateway cannot tell an
 * old intent from a new one, so a stale uncommitted row is forwarded at the
 * next ENABLED window — a fake order in paper mode, a real order in the live
 * sandbox. This probe answers "what is still pending, and how old?" for the
 * hop guard, which decides warn (paper) / refuse (live).
 *
 * Reads (read-only, bounded):
 *  1. Execution_Intent LOG — scanned from offset zero across every bucket until
 *     drained (two consecutive empty polls) or the scan budget expires;
 *  2. Execution_Intent_Processed KV — point lookup per row (the same durable
 *     commit index the gateway's dispatcher reads; the changelog is not used
 *     because it is not a complete history by contract).
 *
 * Prints one TSV line per PENDING row:
 *
 *   &lt;instruction_id&gt;\t&lt;created_ts&gt;\t&lt;expiry_ts|-1 for null&gt;
 *
 * followed by one summary line:
 *
 *   SUMMARY\t&lt;pending_count&gt;\t&lt;oldest_created_ts|-1&gt;\t&lt;newest_created_ts|-1&gt;
 *
 * Columns are resolved BY NAME from the live TableInfo (P6-371 convention):
 * a reordered/renamed schema fails loudly instead of reading index 0/17/18.
 * Exit codes: 0 = scan completed; 3 = connection/table/scan failure (the
 * caller classifies BLOCKED, never a pass). Expired rows are NOT filtered here
 * — expiry is the guard's policy (the gateway's validator drops expired rows);
 * the probe reports raw pending state.
 */
public final class IntentPendingProbe {

    private static final long SCAN_BUDGET_MS = 20_000L;
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(1500);
    private static final int EMPTY_POLLS_TO_STOP = 2;

    public static void main(String[] args) {
        String database = args.length > 0 ? args[0] : "default";
        String intentTable = args.length > 1 ? args[1] : "Execution_Intent";
        String processedTable = args.length > 2 ? args[2] : "Execution_Intent_Processed";
        String bootstrap = args.length > 3 ? args[3] : "localhost:9123";
        try {
            System.exit(run(database, intentTable, processedTable, bootstrap));
        } catch (Exception e) {
            System.err.println("IntentPendingProbe failed: " + e);
            System.exit(3);
        }
    }

    static int run(String database, String intentTable, String processedTable,
            String bootstrap) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        long deadline = System.currentTimeMillis() + SCAN_BUDGET_MS;
        int pending = 0;
        long oldest = -1L;
        long newest = -1L;
        try (Connection c = ConnectionFactory.createConnection(conf);
                Table intents = c.getTable(TablePath.of(database, intentTable));
                Table processed = c.getTable(TablePath.of(database, processedTable))) {
            List<String> names = intents.getTableInfo().getSchema().getRowType().getFieldNames();
            int idIdx = names.indexOf("instruction_id");
            int createdIdx = names.indexOf("created_ts");
            int expiryIdx = names.indexOf("expiry_ts");
            if (idIdx < 0 || createdIdx < 0 || expiryIdx < 0) {
                throw new IllegalStateException(intentTable
                        + " missing instruction_id/created_ts/expiry_ts (columns: " + names + ")");
            }
            Lookuper lookuper = processed.newLookup().createLookuper();
            LogScanner scanner = intents.newScan().createLogScanner();
            try {
                int buckets = intents.getTableInfo().getNumBuckets();
                for (int b = 0; b < buckets; b++) {
                    scanner.subscribe(b, 0L);
                }
                int emptyPolls = 0;
                while (emptyPolls < EMPTY_POLLS_TO_STOP
                        && System.currentTimeMillis() < deadline) {
                    ScanRecords records = scanner.poll(POLL_TIMEOUT);
                    int n = 0;
                    for (ScanRecord record : records) {
                        n++;
                        InternalRow row = record.getRow();
                        String id = row.isNullAt(idIdx) ? "" : row.getString(idIdx).toString();
                        long created = row.isNullAt(createdIdx) ? -1L : row.getLong(createdIdx);
                        long expiry = row.isNullAt(expiryIdx) ? -1L : row.getLong(expiryIdx);
                        InternalRow key = GenericRow.of(BinaryString.fromString(id));
                        InternalRow hit = lookuper.lookup(key)
                                .get(2, TimeUnit.SECONDS).getSingletonRow();
                        if (hit != null) {
                            continue; // committed — the gateway's durable dedup skips it
                        }
                        System.out.println(id + "\t" + created + "\t" + expiry);
                        pending++;
                        if (oldest < 0 || created < oldest) {
                            oldest = created;
                        }
                        if (newest < 0 || created > newest) {
                            newest = created;
                        }
                    }
                    emptyPolls = (n == 0) ? emptyPolls + 1 : 0;
                }
            } finally {
                scanner.close();
            }
        }
        System.out.println("SUMMARY\t" + pending + "\t" + oldest + "\t" + newest);
        return 0;
    }
}
