import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

/**
 * Full-bucket rule-id census for a signal table (strategy-host smoke,
 * 2026-09-05). Prints total rows + per-rule_id counts. Read-only.
 */
public class FlussRuleCounter {
    public static void main(String[] args) throws Exception {
        String tableName = args.length > 0 ? args[0] : "Signal_Candidates";
        String bootstrap = args.length > 1 ? args[1] : "localhost:9123";
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        Map<String, Long> byRule = new TreeMap<>();
        long total = 0;
        try (Connection conn = ConnectionFactory.createConnection(conf);
             Admin admin = conn.getAdmin()) {
            TablePath path = TablePath.of("default", tableName);
            TableInfo info = admin.getTableInfo(path).get();
            Schema schema = info.getSchema();
            int ruleIdx = -1;
            for (int i = 0; i < schema.getColumnNames().size(); i++) {
                if (schema.getColumnNames().get(i).equals("rule_id")) {
                    ruleIdx = i;
                }
            }
            if (ruleIdx < 0) {
                throw new IllegalStateException("no rule_id column in " + tableName);
            }
            try (Table table = conn.getTable(path)) {
                for (int b = 0; b < info.getNumBuckets(); b++) {
                    TableBucket tb = new TableBucket(info.getTableId(), b);
                    try (BatchScanner scanner = table.newScan()
                                 .limit(1_000_000_000)
                                 .createBatchScanner(tb);
                         CloseableIterator<InternalRow> it =
                                 scanner.pollBatch(Duration.ofMillis(5000))) {
                        while (it.hasNext()) {
                            InternalRow row = it.next();
                            total++;
                            String rule = row.isNullAt(ruleIdx)
                                    ? "<null>" : row.getString(ruleIdx).toString();
                            byRule.merge(rule, 1L, Long::sum);
                        }
                    }
                }
            }
        }
        System.out.println("table=" + tableName + " total=" + total);
        byRule.forEach((rule, count) -> System.out.println("rule=" + rule + " count=" + count));
    }
}
