import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.utils.CloseableIterator;

/**
 * SignalCandidatesViewer — operator table view of the signal rows the strategy host writes
 * when a rule fires (the {@code Signal_Candidates} table).
 *
 * <p>This is the "why did it fire?" eyeball tool: it prints the newest {@code --rows} signal
 * rows, newest detection time first, with symbol/rule/side/quantity and the tf, trigger and
 * range parsed out of the v2 audit JSON in {@code score_inputs} (CHG-504). {@code --full}
 * prints the complete audit document per row — that document carries the market snapshot the
 * strategy host handed the strategy at fire time (bid/ask ladder, day stats, clocks).
 *
 * <p><b>Reads are by name, never by index.</b> Every field index is resolved from the live
 * schema ({@code signal_candidates} DDL order is checked against the required names), so a
 * reordered table fails loudly instead of printing the wrong column.
 *
 * <p><b>LOG vs KV.</b> The DDL declares no primary key, so the live table is an append-only
 * LOG and is read with the offset-paged {@code LogScanner}; if a deployment ever makes it a
 * KV table, the snapshot path reads the current rows instead (same viewer, same output).
 *
 * <p>Read-only. Exit codes: 0 table printed (even when empty), 1 runtime failure, 2 unusable
 * input (missing column, wrong type, bad window/arg).
 *
 * <p>Usage: java -cp &lt;cp&gt; SignalCandidatesViewer [--rows 20] [--rule R] [--full]
 * [--database default] [--table Signal_Candidates] [--bootstrap host:port]
 */
public final class SignalCandidatesViewer {

    private static final Duration FIRST_POLL_TIMEOUT = Duration.ofMillis(5_000);
    private static final Duration DRAIN_POLL_TIMEOUT = Duration.ofMillis(500);
    /** Consecutive empty polls that declare the current log end reached. */
    private static final int EMPTY_POLLS_TO_DRAIN = 3;
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final DateTimeFormatter TIME_FMT =
            DateTimeFormatter.ofPattern("MM-dd HH:mm:ss").withZone(IST);
    private static final Pattern TF_RE = Pattern.compile("\"tf\"\\s*:\\s*\"?([^\",}\\]]*)");
    private static final Pattern TRIGGER_RE =
            Pattern.compile("\"triggerPrice\"\\s*:\\s*\"?([^\",}\\]]*)");
    private static final Pattern RANGE_RE = Pattern.compile("\"range\"\\s*:\\s*\"?([^\",}\\]]*)");
    /** Upper bound on rows read in one run; signals are rare — hitting it is reported. */
    private static final int SCAN_MAX = Integer.getInteger("fluss.probe.signal.scan.max", 500_000);

    static final class InputException extends RuntimeException {
        InputException(String msg) {
            super(msg);
        }
    }

    /** One decoded signal row (only the fields the table view prints). */
    static final class SignalRow {
        final String candidateId;
        final long token;
        final String symbol;
        final String rule;
        final String side;
        final String action;
        final long quantity;
        final long limitPaise;
        final long detectionTs;
        final long evaluationTs;
        final String tf;
        final String trigger;
        final String range;
        final String audit;

        SignalRow(String candidateId, long token, String symbol, String rule, String side,
                  String action, long quantity, long limitPaise, long detectionTs,
                  long evaluationTs, String tf, String trigger, String range, String audit) {
            this.candidateId = candidateId;
            this.token = token;
            this.symbol = symbol;
            this.rule = rule;
            this.side = side;
            this.action = action;
            this.quantity = quantity;
            this.limitPaise = limitPaise;
            this.detectionTs = detectionTs;
            this.evaluationTs = evaluationTs;
            this.tf = tf;
            this.trigger = trigger;
            this.range = range;
            this.audit = audit;
        }
    }

    public static void main(String[] args) {
        try {
            run(args);
        } catch (InputException e) {
            System.err.println("SignalCandidatesViewer: " + e);
            System.exit(2);
        } catch (Throwable t) {
            System.err.println("SignalCandidatesViewer failed: " + t);
            System.exit(1);
        }
        System.out.flush();
        System.exit(0);
    }

    static void run(String[] args) throws Exception {
        int rows = 20;
        String ruleFilter = null;
        boolean full = false;
        String database = "default";
        String table = "Signal_Candidates";
        String bootstrap = "fluss-coordinator:9123";
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--rows":
                    if (++i >= args.length) {
                        throw new InputException("--rows needs a value");
                    }
                    try {
                        rows = Integer.parseInt(args[i]);
                    } catch (NumberFormatException e) {
                        throw new InputException("--rows must be an integer, got " + args[i]);
                    }
                    if (rows < 1) {
                        throw new InputException("--rows must be >= 1, got " + rows);
                    }
                    break;
                case "--rule":
                    if (++i >= args.length) {
                        throw new InputException("--rule needs a value");
                    }
                    ruleFilter = args[i];
                    break;
                case "--full":
                    full = true;
                    break;
                case "--database":
                    if (++i >= args.length) {
                        throw new InputException("--database needs a value");
                    }
                    database = args[i];
                    break;
                case "--table":
                    if (++i >= args.length) {
                        throw new InputException("--table needs a value");
                    }
                    table = args[i];
                    break;
                case "--bootstrap":
                    if (++i >= args.length) {
                        throw new InputException("--bootstrap needs a value");
                    }
                    bootstrap = args[i];
                    break;
                default:
                    throw new InputException("unknown argument: " + args[i]);
            }
        }

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        TablePath path = TablePath.of(database, table);
        try (Connection conn = ConnectionFactory.createConnection(conf);
                Admin admin = conn.getAdmin()) {
            TableInfo info = admin.getTableInfo(path).get();
            Idx idx = Idx.resolve(info);
            List<SignalRow> newest = new ArrayList<>();
            long inspected = 0;
            try (Table t = conn.getTable(path)) {
                inspected = info.hasPrimaryKey()
                        ? scanKv(t, info, idx, ruleFilter, rows, newest)
                        : scanLog(t, info, idx, ruleFilter, rows, newest);
            }
            System.out.println("SIGNALS table=" + path + " inspected=" + inspected
                    + " shown=" + newest.size()
                    + (ruleFilter == null ? "" : " rule=" + ruleFilter));
            printTable(newest, !newest.isEmpty());
            if (full) {
                printFullAudits(newest);
            }
        }
    }

    /** LOG tables: offset-paged from the beginning, newest N retained in a min-heap. */
    private static long scanLog(Table table, TableInfo info, Idx idx, String ruleFilter,
                                int keep, List<SignalRow> out) throws Exception {
        long inspected = 0;
        PriorityQueue<SignalRow> heap =
                new PriorityQueue<>(Comparator.comparingLong(r -> r.detectionTs));
        boolean capped = false;
        try (LogScanner scanner = table.newScan().createLogScanner()) {
            for (int b = 0; b < info.getNumBuckets(); b++) {
                scanner.subscribeFromBeginning(b);
            }
            int emptyPolls = 0;
            while (!capped) {
                ScanRecords records = scanner.poll(FIRST_POLL_TIMEOUT);
                if (records == null || records.isEmpty()) {
                    if (++emptyPolls >= EMPTY_POLLS_TO_DRAIN) {
                        break;
                    }
                    continue;
                }
                emptyPolls = 0;
                for (ScanRecord rec : records) {
                    InternalRow row = rec.getRow();
                    if (row == null) {
                        continue;
                    }
                    inspected++;
                    if (ruleFilter != null && !ruleFilter.equals(stringOr(row, idx.rule, "-"))) {
                        continue;
                    }
                    retain(heap, decode(row, idx), keep);
                    if (inspected >= SCAN_MAX) {
                        capped = true;
                        break;
                    }
                }
            }
        }
        if (capped) {
            System.err.println("SignalCandidatesViewer: reached the " + SCAN_MAX
                    + "-row scan cap — the newest rows shown are those read so far;"
                    + " raise -Dfluss.probe.signal.scan.max=<n> to scan further");
        }
        drain(heap, out);
        return inspected;
    }

    /** KV tables (not the current DDL, but tolerated): snapshot every bucket. */
    private static long scanKv(Table table, TableInfo info, Idx idx, String ruleFilter,
                               int keep, List<SignalRow> out) throws Exception {
        long inspected = 0;
        PriorityQueue<SignalRow> heap =
                new PriorityQueue<>(Comparator.comparingLong(r -> r.detectionTs));
        for (int b = 0; b < info.getNumBuckets(); b++) {
            TableBucket bucket = new TableBucket(info.getTableId(), b);
            try (BatchScanner scanner = table.newScan().createBatchScanner(bucket)) {
                while (true) {
                    try (CloseableIterator<InternalRow> batch = scanner.pollBatch(DRAIN_POLL_TIMEOUT)) {
                        if (batch == null) {
                            break; // end of input, per the BatchScanner contract
                        }
                        while (batch.hasNext()) {
                            InternalRow row = batch.next();
                            inspected++;
                            if (ruleFilter != null
                                    && !ruleFilter.equals(stringOr(row, idx.rule, "-"))) {
                                continue;
                            }
                            retain(heap, decode(row, idx), keep);
                        }
                    }
                }
            }
        }
        drain(heap, out);
        return inspected;
    }

    private static void retain(PriorityQueue<SignalRow> heap, SignalRow row, int keep) {
        heap.add(row);
        if (heap.size() > keep) {
            heap.poll();
        }
    }

    private static void drain(PriorityQueue<SignalRow> heap, List<SignalRow> out) {
        out.addAll(heap);
        out.sort(Comparator.comparingLong((SignalRow r) -> r.detectionTs)
                .thenComparingLong(r -> r.evaluationTs).reversed());
    }

    private static SignalRow decode(InternalRow row, Idx idx) {
        String audit = stringOr(row, idx.scoreInputs, null);
        return new SignalRow(
                stringOr(row, idx.candidateId, "-"),
                row.getLong(idx.token),
                stringOr(row, idx.symbol, "-"),
                stringOr(row, idx.rule, "-"),
                stringOr(row, idx.side, "-"),
                stringOr(row, idx.action, "-"),
                row.getLong(idx.quantity),
                row.isNullAt(idx.limitPrice) ? 0L : row.getLong(idx.limitPrice),
                row.getLong(idx.detectionTs),
                row.getLong(idx.evaluationTs),
                auditField(audit, TF_RE),
                auditField(audit, TRIGGER_RE),
                auditField(audit, RANGE_RE),
                audit);
    }

    private static String stringOr(InternalRow row, int index, String fallback) {
        if (row.isNullAt(index)) {
            return fallback;
        }
        return row.getString(index).toString();
    }

    private static String auditField(String audit, Pattern pattern) {
        if (audit == null || audit.isEmpty()) {
            return "-";
        }
        Matcher m = pattern.matcher(audit);
        return m.find() ? m.group(1) : "-";
    }

    private static void printTable(List<SignalRow> rows, boolean any) {
        String[] headers = {"detected_ist", "symbol", "token", "rule", "side", "action",
                "qty", "limit_px", "tf", "trigger", "range", "candidate"};
        List<String[]> cells = new ArrayList<>();
        for (SignalRow r : rows) {
            cells.add(new String[] {
                    TIME_FMT.format(Instant.ofEpochMilli(r.detectionTs)),
                    r.symbol,
                    Long.toString(r.token),
                    r.rule,
                    r.side,
                    r.action,
                    Long.toString(r.quantity),
                    r.limitPaise == 0L ? "-" : Long.toString(r.limitPaise),
                    r.tf,
                    r.trigger,
                    r.range,
                    r.candidateId.length() > 8 ? r.candidateId.substring(0, 8) : r.candidateId});
        }
        int[] width = new int[headers.length];
        for (int c = 0; c < headers.length; c++) {
            width[c] = headers[c].length();
        }
        for (String[] row : cells) {
            for (int c = 0; c < row.length; c++) {
                width[c] = Math.max(width[c], row[c].length());
            }
        }
        System.out.println("SIGNALS " + (any ? "newest_first" : "empty"));
        System.out.println("SIGNALS " + format(headers, width));
        StringBuilder rule = new StringBuilder();
        for (int c = 0; c < headers.length; c++) {
            rule.append("-".repeat(width[c]));
            if (c < headers.length - 1) {
                rule.append("  ");
            }
        }
        System.out.println("SIGNALS " + rule);
        for (String[] row : cells) {
            System.out.println("SIGNALS " + format(row, width));
        }
    }

    private static String format(String[] cells, int[] width) {
        StringBuilder sb = new StringBuilder();
        for (int c = 0; c < cells.length; c++) {
            sb.append(cells[c]);
            if (c < cells.length - 1) {
                sb.append(" ".repeat(Math.max(0, width[c] - cells[c].length()))).append("  ");
            }
        }
        return sb.toString();
    }

    private static void printFullAudits(List<SignalRow> rows) {
        for (SignalRow r : rows) {
            System.out.println("SIGNALS audit candidate=" + r.candidateId
                    + " detection_ts=" + r.detectionTs);
            System.out.println(r.audit == null ? "SIGNALS audit <null>" : "SIGNALS audit "
                    + r.audit);
        }
    }

    /** Live-schema field indexes, resolved by name so a reordered table fails loudly. */
    static final class Idx {
        final int candidateId;
        final int token;
        final int symbol;
        final int rule;
        final int side;
        final int action;
        final int quantity;
        final int limitPrice;
        final int detectionTs;
        final int evaluationTs;
        final int scoreInputs;

        private Idx(int candidateId, int token, int symbol, int rule, int side, int action,
                    int quantity, int limitPrice, int detectionTs, int evaluationTs,
                    int scoreInputs) {
            this.candidateId = candidateId;
            this.token = token;
            this.symbol = symbol;
            this.rule = rule;
            this.side = side;
            this.action = action;
            this.quantity = quantity;
            this.limitPrice = limitPrice;
            this.detectionTs = detectionTs;
            this.evaluationTs = evaluationTs;
            this.scoreInputs = scoreInputs;
        }

        static Idx resolve(TableInfo info) {
            String[] stringCols = {"candidate_id", "symbol", "rule_id", "side", "action",
                    "score_inputs"};
            String[] bigintCols = {"instrument_token", "quantity", "detection_ts",
                    "evaluation_ts"};
            int[] s = new int[stringCols.length];
            int[] b = new int[bigintCols.length];
            for (int i = 0; i < stringCols.length; i++) {
                s[i] = require(info, stringCols[i], DataTypeRoot.STRING);
            }
            for (int i = 0; i < bigintCols.length; i++) {
                b[i] = require(info, bigintCols[i], DataTypeRoot.BIGINT);
            }
            int limit = optional(info, "limit_price_paise", DataTypeRoot.BIGINT);
            return new Idx(s[0], b[0], s[1], s[2], s[3], s[4], b[1], limit, b[2], b[3], s[5]);
        }

        private static int require(TableInfo info, String name, DataTypeRoot root) {
            int i = optional(info, name, root);
            if (i < 0) {
                throw new InputException("no " + name + " column (" + root + ") in "
                        + info.getTablePath() + " — schema drift?");
            }
            return i;
        }

        private static int optional(TableInfo info, String name, DataTypeRoot root) {
            List<String> names = info.getSchema().getColumnNames();
            int i = names.indexOf(name);
            if (i < 0) {
                return -1;
            }
            DataTypeRoot actual = info.getSchema().getColumns().get(i).getDataType().getTypeRoot();
            if (actual != root) {
                throw new InputException(name + " in " + info.getTablePath() + " is " + actual
                        + ", expected " + root);
            }
            return i;
        }
    }
}
