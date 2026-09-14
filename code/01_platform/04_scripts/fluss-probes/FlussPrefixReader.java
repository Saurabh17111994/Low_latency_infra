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
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.TimestampType;

import java.time.Duration;
import java.time.LocalDate;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * FlussPrefixReader — Phase 5 side-by-side soak reader.
 *
 * Reads every row of a KV table whose bucket key is {@code instrument_token}
 * by lookup (see org.apache.fluss.client.table.Table#newLookup docs; the PK
 * columns of the read table must START with instrument_token, which is true
 * for candle_live/candle_closed (PK token,tf,window_start)). When
 * instrument_token is the WHOLE
 * primary key (e.g. the single-column Signal_Candidates_current), a prefix
 * lookup is illegal — the reader falls back to a full PK lookup instead
 * (fix 2026-09-05: prefix lookup there threw and callers saw a misleading
 * empty read). For a LOG table (no primary key, e.g. Signal_Candidates) a
 * lookup is illegal — the reader instead scans the log from the beginning
 * across every bucket.
 *
 * Emits one JSON object per row (column name -> scalar value), then the
 * sentinel line {@code __END__ <count>}. Column names/types come from the
 * live TableInfo so this reader is layout-agnostic (works for the old and
 * new tables without hard-coded indices).
 *
 * Contract (wave 13):
 *   - the {@code __END__ <count>} sentinel is printed on EVERY exit path, including
 *     failures: a failed run prints {@code __ERROR__ <t>} to stderr, the sentinel with
 *     count 0 to stdout and exits non-zero, so a caller blocking on the sentinel cannot
 *     hang and cannot mistake "probe failed" for "read nothing" (P6-080).
 *   - each token's lookup is independent: one slow or failing token is reported on
 *     stderr and the remaining tokens are still read (P6-083).
 *   - {@code tokens_csv}: comma-separated tokens; empty or {@code *} means "no token
 *     filter" (LOG tables read every row). A non-empty list where no token parses is an
 *     input error (exit 2) rather than a silent unfiltered read (P6-083, P6-084).
 *   - {@code window_start_ms}/{@code window_end_ms}: 0 means unbounded on that side; a
 *     non-zero bound on a table without a window_start column is an input error, never a
 *     silently unfiltered read (P6-081). Both the lookup and the LOG path filter.
 *   - the LOG scan prints {@code __END__ <count>} plus a stderr WARN when it stops on the
 *     read deadline instead of draining, so a partial read is not mistaken for a complete
 *     one (P6-373). The deadline is {@code PROBE_READ_DEADLINE_MS} (default 60000).
 *
 * Exit codes: 0 success, 1 runtime failure (sentinel still printed), 2 unusable input.
 *
 * Usage: java -cp <cp> FlussPrefixReader <table> <tokens_csv|*>
 *                                    <window_start_ms|0> <window_end_ms|0>
 *                                    [bootstrap]
 */
public class FlussPrefixReader {

    /** Input errors: unusable arguments, never a runtime/cluster failure. */
    static class InputException extends RuntimeException {
        InputException(String msg) {
            super(msg);
        }
    }

    public static void main(String[] args) {
        try {
            run(args);
        } catch (Throwable t) {
            // P6-080: callers block on the __END__ sentinel. Any failure after startup
            // must still print it, and exit non-zero so "probe failed" is distinguishable
            // from "probe read 0 rows".
            System.err.println("__ERROR__ " + t);
            System.out.println("__END__ 0");
            System.out.flush();
            System.exit(t instanceof InputException ? 2 : 1);
        }
    }

    static void run(String[] args) throws Exception {
        String table = args.length > 0 ? args[0] : "candle_live";
        String tokensRaw = args.length > 1 ? args[1] : "";
        long ws = args.length > 2 ? parseWindowArg(args[2], "window_start_ms") : 0L;
        long we = args.length > 3 ? parseWindowArg(args[3], "window_end_ms") : 0L;
        String bootstrap = args.length > 4 ? args[4] : "localhost:9123";
        if (ws != 0 && we != 0 && we <= ws) {
            throw new InputException("window_end_ms (" + we + ") must be greater than window_start_ms (" + ws + ")");
        }

        boolean wildcard = tokensRaw.trim().isEmpty() || "*".equals(tokensRaw.trim());
        Set<Long> wanted = wildcard ? new LinkedHashSet<>() : parseTokens(tokensRaw);
        if (!wildcard && wanted.isEmpty()) {
            throw new InputException("no usable token in tokens_csv '" + tokensRaw + "'");
        }
        if (!wildcard) {
            // The set is only a filter here, so dedupe silently keeps the order of the CSV.
            System.err.println("FlussPrefixReader: reading tokens " + wanted);
        }

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        TablePath tp = TablePath.of("default", table);

        try (Connection c = ConnectionFactory.createConnection(conf);
             Table t = c.getTable(tp)) {
            RowType rowType = t.getTableInfo().getSchema().getRowType();
            List<String> colNames = rowType.getFieldNames();
            List<DataType> colTypes = rowType.getFieldTypes();
            int wsIdx = colNames.indexOf("window_start");
            if ((ws != 0 || we != 0) && wsIdx < 0) {
                // P6-081: silently returning unfiltered rows would be read as a
                // filtered comparison by every soak caller.
                throw new InputException("cannot apply the window filter: " + table
                        + " has no window_start column (columns: " + colNames + ")");
            }
            int total = 0;
            if (t.getTableInfo().hasPrimaryKey()) {
                List<String> primaryKeys = t.getTableInfo().getPrimaryKeys();
                // Prefix lookup (lookupBy) is only legal when the lookup
                // columns are a STRICT SUBSET of the primary key. When
                // instrument_token is the whole PK (e.g. the single-column
                // Signal_Candidates_current), lookupBy("instrument_token")
                // throws "lookup columns equal the physical primary keys" —
                // and the throw was silently swallowed by callers' 2>/dev/null,
                // producing a misleading __END__ 0 (observed 2026-09-05:
                // N7 KV-current probe read 0 while a full bucket scan found
                // 2433 rows). Use plain PK lookup in that case.
                boolean fullPkLookup = primaryKeys.size() == 1;
                String bucketKey = primaryKeys.get(0);
                // P6-082: the lookup row below carries ONE value (the token), so the
                // bucket key must be the token column. Anything else would query a
                // different column with the token's value and return wrong rows.
                if (!"instrument_token".equals(bucketKey)) {
                    throw new InputException("FlussPrefixReader looks up by the bucket key, but "
                            + table + "'s primary key starts with '" + bucketKey + "' (pk=" + primaryKeys + ")");
                }
                // P6-731 note: the finding asks for try-with-resources here, but
                // org.apache.fluss.client.lookup.Lookuper is not AutoCloseable in
                // fluss-client 0.9.1-incubating — the interface has no close() method,
                // so that remedy is not implementable. The lookuper holds no socket of
                // its own; it is bounded by the Table/Connection try-with-resources
                // that wraps this block and by the hard exit below.
                Lookuper prefix = fullPkLookup
                        ? t.newLookup().createLookuper()
                        : t.newLookup().lookupBy(bucketKey).createLookuper();
                {
                    for (String tokRaw : tokensRaw.split(",")) {
                        if (wildcard && tokRaw.trim().isEmpty()) {
                            continue;
                        }
                        final long token;
                        try {
                            token = Long.parseLong(tokRaw.trim());
                        } catch (NumberFormatException e) {
                            System.err.println("FlussPrefixReader: skipping bad token '" + tokRaw + "'");
                            continue;
                        }
                        if (!wanted.isEmpty() && !wanted.contains(token)) {
                            continue;
                        }
                        try {
                            LookupResult res = prefix.lookup(GenericRow.of(token))
                                    .get(10, TimeUnit.SECONDS);
                            List<InternalRow> rows = res == null ? List.of() : res.getRowList();
                            if (rows == null) {
                                rows = List.of();
                            }
                            for (InternalRow row : rows) {
                                if (row == null || !inWindow(row, wsIdx, ws, we)) {
                                    continue;
                                }
                                emitJson(colNames, colTypes, rowType, row);
                                total++;
                            }
                        } catch (InterruptedException ie) {
                            // P6-083: one bad token must not discard the others.
                            Thread.currentThread().interrupt();
                            System.err.println("FlussPrefixReader: token=" + token + " interrupted");
                        } catch (Exception e) {
                            System.err.println("FlussPrefixReader: token=" + token + " lookup failed: " + e);
                        }
                    }
                }
            } else {
                // LOG table: scan every bucket from the beginning.
                int tokIdx = colNames.indexOf("instrument_token");
                if (!wanted.isEmpty() && tokIdx < 0) {
                    // P6-084: the KV path filters by token; the LOG path must not quietly
                    // emit every token's rows instead.
                    throw new InputException("cannot filter by token: " + table
                            + " has no instrument_token column (columns: " + colNames + ")");
                }
                Scan scan = t.newScan();
                LogScanner scanner = scan.createLogScanner();
                int bucketCount = t.getTableInfo().getNumBuckets();
                for (int b = 0; b < bucketCount; b++) {
                    scanner.subscribeFromBeginning(b);
                }
                long deadline = System.currentTimeMillis() + readDeadlineMs();
                int emptyPolls = 0;
                boolean drained = false;
                while (System.currentTimeMillis() < deadline) {
                    ScanRecords records = scanner.poll(Duration.ofSeconds(2));
                    if (records == null || records.isEmpty()) {
                        // No more data available now. A LOG table's current
                        // contents are finite: once a poll drains empty, we
                        // have read everything appended so far. Allow a short
                        // settling grace (in-flight writes may still land),
                        // then STOP — otherwise this loop spins until the
                        // deadline on every LOG read and the soak gate's
                        // subprocess timeout (180s) trips when several tables
                        // are read in sequence.
                        if (++emptyPolls >= 3) {
                            drained = true;
                            break;
                        }
                        Thread.sleep(200);
                        continue;
                    }
                    emptyPolls = 0;
                    boolean advanced = false;
                    for (ScanRecord rec : records) {
                        InternalRow row = rec.getRow();
                        if (row == null || !inWindow(row, wsIdx, ws, we)) {
                            continue;
                        }
                        if (!wanted.isEmpty() && row.getLong(tokIdx) != 0
                                && !wanted.contains(row.getLong(tokIdx))) {
                            continue;
                        }
                        emitJson(colNames, colTypes, rowType, row);
                        total++;
                        advanced = true;
                    }
                    if (!advanced) {
                        // poll returned records but all rows were filtered/empty —
                        // unlikely; avoid a busy loop.
                        Thread.sleep(200);
                    }
                }
                if (!drained) {
                    // P6-373: a deadline stop is a partial read. Say so, and keep the
                    // sentinel so existing parsers keep working.
                    System.err.println("WARN: LOG scan hit the " + readDeadlineMs()
                            + "ms deadline — output truncated, count=" + total);
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

    /** Read deadline for the LOG scan; env-overridable so tests need not wait 60s. */
    static long readDeadlineMs() {
        String raw = System.getenv("PROBE_READ_DEADLINE_MS");
        if (raw == null || raw.trim().isEmpty()) {
            return 60_000L;
        }
        try {
            long v = Long.parseLong(raw.trim());
            return v > 0 ? v : 60_000L;
        } catch (NumberFormatException e) {
            System.err.println("FlussPrefixReader: ignoring bad PROBE_READ_DEADLINE_MS='" + raw + "'");
            return 60_000L;
        }
    }

    /**
     * Comma-separated tokens; unparseable entries are skipped with a stderr note
     * (P6-083). Empty input yields an empty set.
     */
    static Set<Long> parseTokens(String raw) {
        Set<Long> out = new LinkedHashSet<>();
        if (raw == null) {
            return out;
        }
        for (String part : raw.split(",")) {
            String s = part.trim();
            if (s.isEmpty()) {
                continue;
            }
            try {
                out.add(Long.parseLong(s));
            } catch (NumberFormatException e) {
                System.err.println("FlussPrefixReader: skipping bad token '" + s + "'");
            }
        }
        return out;
    }

    /** Window bound: 0 = unbounded, otherwise a millisecond epoch. Bad input is an input error. */
    static long parseWindowArg(String raw, String name) {
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            throw new InputException(name + " must be a millisecond epoch or 0, got '" + raw + "'");
        }
    }

    /**
     * Window-membership test for one row: true when the row is inside
     * [{@code ws}, {@code we}) with 0 meaning "unbounded" (P6-081). A missing
     * window_start column (index &lt; 0) cannot filter, and the caller has already
     * rejected that combination for non-zero bounds.
     */
    static boolean inWindow(InternalRow row, int wsIdx, long ws, long we) {
        if (wsIdx < 0 || (ws == 0 && we == 0)) {
            return true;
        }
        long v = row.getLong(wsIdx);
        return (ws == 0 || v >= ws) && (we == 0 || v < we);
    }

    private static void emitJson(List<String> colNames, List<DataType> colTypes,
                                 RowType rowType, InternalRow row) {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('"').append(jsonEsc(colNames.get(i))).append("\":");
            sb.append(formatValue(row, i, colTypes.get(i)));
        }
        sb.append('}');
        System.out.println(sb);
    }

    /**
     * One JSON value for one cell. P6-374: every common type root is handled
     * explicitly, a complex root is emitted as an explicit marker (never a silent
     * null) and the format is stable enough to diff two runs.
     */
    static String formatValue(InternalRow row, int i, DataType type) {
        if (row.isNullAt(i)) {
            return "null";
        }
        switch (type.getTypeRoot().name()) {
            case "BIGINT":
                return String.valueOf(row.getLong(i));
            case "INTEGER":
            case "SMALLINT":
            case "TINYINT":
                return String.valueOf(row.getInt(i));
            case "STRING":
            case "CHAR":
                return '"' + jsonEsc(String.valueOf(row.getString(i))) + '"';
            case "BOOLEAN":
                return String.valueOf(row.getBoolean(i));
            case "DOUBLE":
                return String.valueOf(row.getDouble(i));
            case "FLOAT":
                return String.valueOf(row.getFloat(i));
            case "TIMESTAMP_WITHOUT_TIME_ZONE": {
                TimestampType ts = (TimestampType) type;
                return '"' + String.valueOf(row.getTimestampNtz(i, ts.getPrecision())) + '"';
            }
            case "TIMESTAMP_WITH_LOCAL_TIME_ZONE": {
                TimestampType ts = (TimestampType) type;
                return '"' + String.valueOf(row.getTimestampLtz(i, ts.getPrecision())) + '"';
            }
            case "DATE":
                return '"' + LocalDate.ofEpochDay(row.getInt(i)).toString() + '"';
            case "TIME":
                return '"' + String.valueOf(row.getInt(i)) + '"';
            case "DECIMAL": {
                DecimalType d = (DecimalType) type;
                return '"' + String.valueOf(row.getDecimal(i, d.getPrecision(), d.getScale())) + '"';
            }
            case "BINARY":
            case "BYTES":
                return '"' + hex(row.getBytes(i)) + '"';
            default:
                // Complex roots (ARRAY/MAP/ROW) are not rendered cell-by-cell: emit an
                // explicit marker and say so once on stderr, so a diff shows the gap
                // instead of a fake null.
                System.err.println("FlussPrefixReader: unmapped type root " + type.getTypeRoot()
                        + " at column " + i + " — emitting a marker");
                return '"' + "<unmapped:" + type.getTypeRoot() + ">" + '"';
        }
    }

    private static String hex(byte[] bytes) {
        if (bytes == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }

    static String jsonEsc(String s) {
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
