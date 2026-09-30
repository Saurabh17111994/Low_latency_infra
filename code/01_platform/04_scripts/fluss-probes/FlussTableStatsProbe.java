import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metadata.TableStats;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

/**
 * FlussTableStatsProbe — state-growth census legs (CHG-461).
 *
 * <p>Mode {@code stats} (default): one Admin.getTableStats RPC per table,
 * printing the row count Fluss reports for it.
 *
 * <pre>
 *   epoch_ms  table  row_count
 * </pre>
 *
 * <p>Mode {@code tf-census}: a KV snapshot scan of every bucket, counting
 * CURRENT stored rows per timeframe — the per-timeframe state of a table whose
 * row count is TTL-bounded (candle_live), where the log-record count from
 * {@code getTableStats} cannot show the live-state size.
 *
 * <pre>
 *   epoch_ms  table  tf  rows  bytes    (bytes = -1: the snapshot row API has no size)
 * </pre>
 *
 * <p>Contract: one table failing (RPC error, scan error, missing tf column) is
 * reported on stderr and the remaining tables are still printed — a partial
 * census must stay visible, never silent. Exit codes: 0 complete census,
 * 3 partial, 2 unusable input, 1 no row at all.
 *
 * <p>Usage: java -cp &lt;cp&gt; FlussTableStatsProbe &lt;bootstrap&gt;
 * &lt;tables_csv&gt; [database] [tf-census]
 */
public final class FlussTableStatsProbe {

    static class InputException extends RuntimeException {
        InputException(String msg) {
            super(msg);
        }
    }

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(10);

    public static void main(String[] args) {
        int code;
        try {
            code = run(args);
        } catch (InputException e) {
            System.err.println("FlussTableStatsProbe: " + e);
            System.exit(2);
            return;
        } catch (Throwable t) {
            System.err.println("FlussTableStatsProbe failed: " + t);
            System.exit(1);
            return;
        }
        System.out.flush();
        System.exit(code);
    }

    static int run(String[] args) throws Exception {
        if (args.length < 2) {
            throw new InputException("usage: <bootstrap> <tables_csv> [database] [tf-census]");
        }
        String bootstrap = args[0];
        String database = args.length > 2 ? args[2] : "default";
        boolean tfCensus = args.length > 3 && "tf-census".equals(args[3]);
        List<String> tables = new ArrayList<>();
        for (String part : args[1].split(",")) {
            String s = part.trim();
            if (!s.isEmpty()) {
                tables.add(s);
            }
        }
        if (tables.isEmpty()) {
            throw new InputException("no usable table in tables_csv '" + args[1] + "'");
        }

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        int ok = 0;
        int failed = 0;
        try (Connection conn = ConnectionFactory.createConnection(conf);
                Admin admin = conn.getAdmin()) {
            if (tfCensus) {
                long epoch = System.currentTimeMillis();
                for (String table : tables) {
                    try {
                        for (Map.Entry<String, Long> e : tfCensus(conn, database, table).entrySet()) {
                            System.out.println(epoch + "\t" + table + "\t" + e.getKey() + "\t"
                                    + e.getValue() + "\t-1");
                        }
                        ok++;
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw ie;
                    } catch (Exception e) {
                        failed++;
                        System.err.println("FlussTableStatsProbe: " + table + " tf-census failed: " + e);
                    }
                }
            } else {
                for (String table : tables) {
                    try {
                        TableStats stats = admin.getTableStats(TablePath.of(database, table))
                                .get(5, TimeUnit.SECONDS);
                        System.out.println(System.currentTimeMillis() + "\t" + table + "\t"
                                + stats.getRowCount());
                        ok++;
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw ie;
                    } catch (Exception e) {
                        failed++;
                        System.err.println("FlussTableStatsProbe: " + table + " stats failed: " + e);
                    }
                }
            }
        }
        if (ok == 0) {
            System.err.println("FlussTableStatsProbe: no table read (" + failed + " failed)");
            return 1;
        }
        return failed > 0 ? 3 : 0;
    }

    /** Current stored rows per tf, draining every bucket's KV snapshot. */
    static Map<String, Long> tfCensus(Connection conn, String database, String tableName)
            throws Exception {
        Map<String, Long> perTf = new TreeMap<>();
        try (Table table = conn.getTable(TablePath.of(database, tableName))) {
            TableInfo info = table.getTableInfo();
            List<String> names = info.getSchema().getRowType().getFieldNames();
            int tfIdx = names.indexOf("tf");
            if (tfIdx < 0) {
                throw new InputException(tableName + " has no tf column (columns: " + names + ")");
            }
            for (int b = 0; b < info.getNumBuckets(); b++) {
                TableBucket bucket = new TableBucket(info.getTableId(), b);
                try (BatchScanner scanner = table.newScan().createBatchScanner(bucket)) {
                    while (true) {
                        try (CloseableIterator<InternalRow> batch = scanner.pollBatch(POLL_TIMEOUT)) {
                            if (batch == null) {
                                break; // end of input, per the BatchScanner contract
                            }
                            while (batch.hasNext()) {
                                InternalRow row = batch.next();
                                perTf.merge(row.getString(tfIdx).toString(), 1L, Long::sum);
                            }
                        }
                    }
                }
            }
        }
        return perTf;
    }

    private FlussTableStatsProbe() {}
}
