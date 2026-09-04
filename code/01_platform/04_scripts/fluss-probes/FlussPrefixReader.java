import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.DataGetters;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * FlussPrefixReader — Phase 5 side-by-side soak reader.
 *
 * Reads every row of a KV table whose bucket key is {@code instrument_token}
 * by prefix lookup (the client's {@code lookupBy("instrument_token")} —
 * see org.apache.fluss.client.table.Table#newLookup docs; the PK columns of
 * the read table must START with instrument_token, which is true for
 * feature_candles_15s (PK token,window_start), candle_closed and candle_live
 * (PK token,tf,window_start)). For a LOG table (no primary key, e.g.
 * Signal_Candidates) a prefix lookup is illegal — the reader instead scans
 * the log from the beginning across every bucket.
 *
 * Emits one JSON object per row (column name -> scalar value), then the
 * sentinel line {@code __END__ <count>}. Column names/types come from the
 * live TableInfo so this reader is layout-agnostic (works for the old and
 * new tables without hard-coded indices).
 *
 * Usage: java -cp <cp> FlussPrefixReader <table> <tokens_csv>
 *                                    <window_start_ms|0> <window_end_ms|0>
 *                                    [bootstrap]
 */
public class FlussPrefixReader {

    public static void main(String[] args) throws Exception {
        String table = args.length > 0 ? args[0] : "feature_candles_15s";
        String tokensRaw = args.length > 1 ? args[1] : "";
        long ws = args.length > 2 ? Long.parseLong(args[2]) : 0L;
        long we = args.length > 3 ? Long.parseLong(args[3]) : 0L;
        String bootstrap = args.length > 4 ? args[4] : "localhost:9123";
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        String[] toks = tokensRaw.split(",");
        TablePath tp = TablePath.of("default", table);

        try (Connection c = ConnectionFactory.createConnection(conf);
             Table t = c.getTable(tp)) {
            RowType rowType = t.getTableInfo().getSchema().getRowType();
            List<String> colNames = rowType.getFieldNames();
            List<DataType> colTypes = rowType.getFieldTypes();
            int total = 0;
            if (t.getTableInfo().hasPrimaryKey()) {
                // Lookup by the token prefix only (bucket key =
                // instrument_token is the first PK column on all three
                // candle tables).
                Lookuper prefix =
                        t.newLookup().lookupBy("instrument_token").createLookuper();
                for (String tokRaw : toks) {
                    long token = Long.parseLong(tokRaw.trim());
                    LookupResult res = prefix.lookup(
                            org.apache.fluss.row.GenericRow.of(token))
                            .get(10, TimeUnit.SECONDS);
                    List<InternalRow> rows =
                            res == null ? List.of() : res.getRowList();
                    if (rows == null) {
                        rows = List.of();
                    }
                    for (InternalRow row : rows) {
                        if (row == null) {
                            continue;
                        }
                        emitJson(colNames, colTypes, rowType, row);
                        total++;
                    }
                }
            } else {
                // LOG table: scan every bucket from the beginning.
                Scan scan = t.newScan();
                LogScanner scanner = scan.createLogScanner();
                int bucketCount = t.getTableInfo().getNumBuckets();
                for (int b = 0; b < bucketCount; b++) {
                    scanner.subscribeFromBeginning(b);
                }
                long deadline = System.currentTimeMillis() + 60_000L;
                int emptyPolls = 0;
                while (System.currentTimeMillis() < deadline) {
                    ScanRecords records = scanner.poll(Duration.ofSeconds(2));
                    if (records == null || records.isEmpty()) {
                        // No more data available now. A LOG table's current
                        // contents are finite: once a poll drains empty, we
                        // have read everything appended so far. Allow a short
                        // settling grace (in-flight writes may still land),
                        // then STOP — otherwise this loop spins until the
                        // 60s deadline on every LOG read and the soak gate's
                        // subprocess timeout (180s) trips when several tables
                        // are read in sequence.
                        if (++emptyPolls >= 3) {
                            break;
                        }
                        Thread.sleep(200);
                        continue;
                    }
                    emptyPolls = 0;
                    boolean advanced = false;
                    for (ScanRecord rec : records) {
                        InternalRow row = rec.getRow();
                        if (row == null) {
                            continue;
                        }
                        emitJson(colNames, colTypes, rowType, row);
                        total++;
                        advanced = true;
                    }
                    if (!advanced) {
                        // poll returned records but all rows were empty —
                        // unlikely; avoid a busy loop.
                        Thread.sleep(200);
                    }
                }
            }
            System.out.println("__END__ " + total);
            // The LOG scanner's teardown (close of an idle from-beginning
            // log subscription) can block indefinitely on the Fluss client
            // (observed 2026-09-04: __END__ printed, process then hung until
            // external timeout). This is a read-only one-shot CLI probe:
            // stdout is already flushed, so exit hard instead of waiting on
            // a client-side fetch/ack that never resolves. The connection
            // dies with the JVM.
            System.out.flush();
            System.exit(0);
        }
    }

    private static void emitJson(List<String> colNames, List<DataType> colTypes,
                                 RowType rowType, InternalRow row) {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('"').append(jsonEsc(colNames.get(i))).append("\":");
            appendJsonValue(sb, row, i, colTypes.get(i));
        }
        sb.append('}');
        System.out.println(sb);
    }

    private static void appendJsonValue(StringBuilder sb, InternalRow row,
                                        int i, DataType type) {
        if (row.isNullAt(i)) {
            sb.append("null");
            return;
        }
        switch (type.getTypeRoot().name()) {
            case "BIGINT":
                sb.append(row.getLong(i));
                break;
            case "INTEGER":
            case "SMALLINT":
            case "TINYINT":
                sb.append(row.getInt(i));
                break;
            case "STRING":
            case "CHAR":
                sb.append('"').append(jsonEsc(row.getString(i).toString())).append('"');
                break;
            case "BOOLEAN":
                sb.append(row.getBoolean(i));
                break;
            case "DOUBLE":
                sb.append(row.getDouble(i));
                break;
            case "FLOAT":
                sb.append(row.getFloat(i));
                break;
            default:
                // Unknown types: emit as string (never crash the reader).
                try {
                    DataGetters g = row;
                    sb.append('"').append(jsonEsc(String.valueOf(
                            g.getString(i)))).append('"');
                } catch (Exception e) {
                    sb.append("null");
                }
        }
    }

    private static String jsonEsc(String s) {
        if (s == null) {
            return "";
        }
        StringBuilder out = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            switch (ch) {
                case '"': out.append("\\\""); break;
                case '\\': out.append("\\\\"); break;
                case '\n': out.append("\\n"); break;
                case '\r': out.append("\\r"); break;
                case '\t': out.append("\\t"); break;
                default:
                    if (ch < 0x20) {
                        out.append(String.format("\\u%04x", (int) ch));
                    } else {
                        out.append(ch);
                    }
            }
        }
        return out.toString();
    }
}
