import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
 * Offline signal/intent latency + correctness census (perf report, 2026-09-05).
 *
 * <p>Mode "signals": scans the Signal_Candidates LOG, keeps rows for one
 * rule_id, and reports the evaluation_ts - detection_ts population (tick age
 * at signal time: detection holds the breaking tick's event time). Prints
 * count, min, p50, p95, p99, max, and the number of negative rows (clock
 * skew markers, excluded from percentiles).
 *
 * <p>Mode "intents": scans the Execution_Intent LOG and reports total rows,
 * distinct candidate_ids, and duplicate intent rows (same candidate twice).
 * Read-only in both modes.
 *
 * <p>Mode "orphans": prints the candidate_ids present in Execution_Intent
 * but absent from the given signal LOG table (phantom-intent check).
 *
 * <p>Usage: FlussSignalLatency signals|intents|orphans [table] [rule] [bootstrap]
 */
public class FlussSignalLatency {
    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "signals";
        String tableName = args.length > 1 ? args[1]
                : ("intents".equals(mode) ? "Execution_Intent" : "Signal_Candidates");
        String rule = args.length > 2 ? args[2] : "n7-range-breakout-v1";
        String bootstrap = args.length > 3 ? args[3] : "localhost:9123";
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        try (Connection conn = ConnectionFactory.createConnection(conf);
             Admin admin = conn.getAdmin()) {
            TablePath path = TablePath.of("default", tableName);
            TableInfo info = admin.getTableInfo(path).get();
            Schema schema = info.getSchema();
            if ("intents".equals(mode)) {
                reportIntents(conn, info, path, schema);
            } else if ("orphans".equals(mode)) {
                reportOrphans(conn, admin, info, path, schema, tableName);
            } else {
                reportSignals(conn, info, path, schema, rule);
            }
        }
    }

    private static int col(Schema schema, String name) {
        int i = schema.getColumnNames().indexOf(name);
        if (i < 0) {
            throw new IllegalStateException("no " + name + " column");
        }
        return i;
    }

    private static void reportSignals(Connection conn, TableInfo info, TablePath path,
            Schema schema, String rule) throws Exception {
        int ruleIdx = col(schema, "rule_id");
        int detIdx = col(schema, "detection_ts");
        int evalIdx = col(schema, "evaluation_ts");
        List<Long> lat = new ArrayList<>();
        long rows = 0;
        long kept = 0;
        long negatives = 0;
        try (Table table = conn.getTable(path)) {
            for (int b = 0; b < info.getNumBuckets(); b++) {
                TableBucket tb = new TableBucket(info.getTableId(), b);
                try (BatchScanner scanner = table.newScan()
                             .limit(1_000_000_000)
                             .createBatchScanner(tb)) {
                    // Drain every batch: one pollBatch call returns a
                    // single page, so a lone poll undercounts wide buckets.
                    while (true) {
                        try (CloseableIterator<InternalRow> batch =
                                scanner.pollBatch(Duration.ofMillis(5000))) {
                            if (batch == null || !batch.hasNext()) {
                                break;
                            }
                            while (batch.hasNext()) {
                        InternalRow row = batch.next();
                        rows++;
                        if (row.isNullAt(ruleIdx)
                                || !rule.equals(row.getString(ruleIdx).toString())) {
                            continue;
                        }
                        kept++;
                        if (row.isNullAt(detIdx) || row.isNullAt(evalIdx)) {
                            continue;
                        }
                        long d = row.getLong(evalIdx) - row.getLong(detIdx);
                        if (d < 0) {
                            negatives++;
                        } else {
                            lat.add(d);
                        }
                            }
                        }
                    }
                }
            }
        }
        Collections.sort(lat);
        System.out.println("table=Signal_Candidates rule=" + rule
                + " rows=" + rows + " kept=" + kept);
        System.out.println("latency_ms n=" + lat.size() + " negatives=" + negatives
                + " min=" + pct(lat, 0) + " p50=" + pct(lat, 50)
                + " p95=" + pct(lat, 95) + " p99=" + pct(lat, 99)
                + " max=" + pct(lat, 100));
    }

    private static void reportIntents(Connection conn, TableInfo info, TablePath path,
            Schema schema) throws Exception {
        int candIdx = col(schema, "candidate_id");
        long rows = 0;
        Set<String> seen = new HashSet<>();
        long dups = 0;
        try (Table table = conn.getTable(path)) {
            for (int b = 0; b < info.getNumBuckets(); b++) {
                TableBucket tb = new TableBucket(info.getTableId(), b);
                try (BatchScanner scanner = table.newScan()
                             .limit(1_000_000_000)
                             .createBatchScanner(tb)) {
                    while (true) {
                        try (CloseableIterator<InternalRow> batch =
                                scanner.pollBatch(Duration.ofMillis(5000))) {
                            if (batch == null || !batch.hasNext()) {
                                break;
                            }
                            while (batch.hasNext()) {
                        InternalRow row = batch.next();
                        rows++;
                        String c = row.isNullAt(candIdx)
                                ? "<null>" : row.getString(candIdx).toString();
                        if (!seen.add(c)) {
                            dups++;
                        }
                            }
                        }
                    }
                }
            }
        }
        System.out.println("table=Execution_Intent rows=" + rows
                + " distinct_candidates=" + seen.size() + " duplicate_rows=" + dups);
    }

    private static void reportOrphans(Connection conn, Admin admin, TableInfo intentInfo,
            TablePath intentPath, Schema intentSchema, String intentTable) throws Exception {
        int candIdx = col(intentSchema, "candidate_id");
        Set<String> intentIds = new HashSet<>();
        try (Table table = conn.getTable(intentPath)) {
            for (int b = 0; b < intentInfo.getNumBuckets(); b++) {
                TableBucket tb = new TableBucket(intentInfo.getTableId(), b);
                try (BatchScanner scanner = table.newScan()
                             .limit(1_000_000_000)
                             .createBatchScanner(tb)) {
                    while (true) {
                        try (CloseableIterator<InternalRow> batch =
                                scanner.pollBatch(Duration.ofMillis(5000))) {
                            if (batch == null || !batch.hasNext()) {
                                break;
                            }
                            while (batch.hasNext()) {
                                InternalRow row = batch.next();
                                if (!row.isNullAt(candIdx)) {
                                    intentIds.add(row.getString(candIdx).toString());
                                }
                            }
                        }
                    }
                }
            }
        }
        TablePath sigPath = TablePath.of("default", "Signal_Candidates");
        TableInfo sigInfo = admin.getTableInfo(sigPath).get();
        int sigCandIdx = col(sigInfo.getSchema(), "candidate_id");
        Set<String> signalIds = new HashSet<>();
        try (Table table = conn.getTable(sigPath)) {
            for (int b = 0; b < sigInfo.getNumBuckets(); b++) {
                TableBucket tb = new TableBucket(sigInfo.getTableId(), b);
                try (BatchScanner scanner = table.newScan()
                             .limit(1_000_000_000)
                             .createBatchScanner(tb)) {
                    while (true) {
                        try (CloseableIterator<InternalRow> batch =
                                scanner.pollBatch(Duration.ofMillis(5000))) {
                            if (batch == null || !batch.hasNext()) {
                                break;
                            }
                            while (batch.hasNext()) {
                                InternalRow row = batch.next();
                                if (!row.isNullAt(sigCandIdx)) {
                                    signalIds.add(row.getString(sigCandIdx).toString());
                                }
                            }
                        }
                    }
                }
            }
        }
        intentIds.removeAll(signalIds);
        System.out.println("orphan_intents=" + intentIds.size());
        intentIds.stream().sorted().limit(20).forEach(id -> System.out.println("orphan=" + id));
    }

    private static long pct(List<Long> sorted, int p) {
        if (sorted.isEmpty()) {
            return -1;
        }
        if (p <= 0) {
            return sorted.get(0);
        }
        if (p >= 100) {
            return sorted.get(sorted.size() - 1);
        }
        return sorted.get((int) Math.ceil(p / 100.0 * sorted.size()) - 1);
    }
}
