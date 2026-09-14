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
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.utils.CloseableIterator;

/**
 * Full-bucket rule-id census for a signal table (strategy-host smoke,
 * 2026-09-05). Prints total rows + per-rule_id counts. Read-only.
 *
 * Contract (wave 13):
 *   - the census is uncapped: a per-bucket {@code .limit(1_000_000_000)} silently
 *     truncated a run that describes itself as a full-bucket census, which would
 *     under-report {@code total}/byRule with no warning (P6-376).
 *   - a bucket is exhausted after THREE consecutive null/empty polls, not one:
 *     {@code pollBatch} is time-bounded, and a single contended/timeout poll would end
 *     the bucket early and undercount (P6-377). The confirmation polls use a short 500ms timeout so the extra proof
 * does not inflate the census runtime.
 *   - {@code rule_id} must be a STRING column before any row is decoded, so a custom
 *     table with another type fails with a clear message instead of a decode error in
 *     the middle of the census (P6-378).
 *
 * Exit codes: 0 census printed, 1 runtime failure, 2 unusable input.
 *
 * Usage: java -cp <cp> FlussRuleCounter [table] [bootstrap]
 */
public class FlussRuleCounter {
    /** P6-377: consecutive empty polls that declare a bucket drained. */
    static final int EMPTY_POLLS_TO_DRAIN = 3;

    static class InputException extends RuntimeException {
        InputException(String msg) {
            super(msg);
        }
    }

    public static void main(String[] args) {
        try {
            run(args);
        } catch (InputException e) {
            System.err.println("FlussRuleCounter: " + e);
            System.exit(2);
        } catch (Throwable t) {
            System.err.println("FlussRuleCounter failed: " + t);
            System.exit(1);
        }
        System.out.flush();
        System.exit(0);
    }

    static void run(String[] args) throws Exception {
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
            // P6-378: rule_id is STRING in the DDL, but this probe takes an arbitrary
            // table name, and getString() on another root throws mid-census.
            DataType ruleType = schema.getColumns().get(ruleIdx).getDataType();
            if (ruleType.getTypeRoot() != DataTypeRoot.STRING) {
                throw new InputException("rule_id column in " + tableName + " is " + ruleType
                        + ", expected STRING");
            }
            try (Table table = conn.getTable(path)) {
                for (int b = 0; b < info.getNumBuckets(); b++) {
                    TableBucket tb = new TableBucket(info.getTableId(), b);
                    // P6-376: no per-bucket row cap — a census must not truncate.
                    try (BatchScanner scanner = table.newScan()
                                 .createBatchScanner(tb)) {
                        // Drain every batch: one pollBatch call returns a
                        // single page, so a lone poll undercounts wide buckets.
                        int emptyPolls = 0;
                        while (true) {
                            // The first poll waits for data; the drain-confirmation polls
                            // do not need a 5s window each: three confirmations per bucket
                            // at the full timeout would add minutes to the soak gate's N7
                            // census. The 500ms window is a heuristic either way (the old
                            // rule was a single empty poll), just a cheaper one.
                            Duration pollTimeout = emptyPolls == 0
                                    ? Duration.ofMillis(5000) : Duration.ofMillis(500);
                            try (CloseableIterator<InternalRow> batch =
                                    scanner.pollBatch(pollTimeout)) {
                                if (batch == null || !batch.hasNext()) {
                                    // P6-377: pollBatch is time-bounded; one empty poll
                                    // is not proof the bucket is drained.
                                    if (++emptyPolls >= EMPTY_POLLS_TO_DRAIN) {
                                        break;
                                    }
                                    continue;
                                }
                                emptyPolls = 0;
                                while (batch.hasNext()) {
                                    InternalRow row = batch.next();
                                    total++;
                                    String rule = row.isNullAt(ruleIdx)
                                            ? "<null>" : row.getString(ruleIdx).toString();
                                    byRule.merge(rule, 1L, Long::sum);
                                }
                            }
                        }
                    }
                }
            }
        }
        System.out.println("table=" + tableName + " total=" + total);
        byRule.forEach((rule, count) -> System.out.println("rule=" + rule + " count=" + count));
    }
}
