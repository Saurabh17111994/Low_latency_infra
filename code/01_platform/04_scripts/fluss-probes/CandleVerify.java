import com.trading.common.schema.RawTableSchema;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec.LatestSpec;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

/**
 * Live proof of the v4 aggregation fix: accumulate raw_table_1 rows myself, then read the
 * candle the SignalJob computed for the same token+window and compare volumes.
 *
 *   candle.volume  should equal  SUM(volume_delta WHERE tick_type='TRADE')
 *   and NOT equal              SUM(last_qty) over every row  (the pre-fix metric, which is
 *                              what sum(cumulative volume) / last-qty summing over-counted)
 *
 * Usage: CandleVerify <minutes> [tf] [--bootstrap HOST:PORT]      (tf default 1m)
 */
public class CandleVerify {
    static int highBias = 0, lowBias = 0, rangeWider = 0;
    static int absentNoTrades = 0;
    // How stale the missing window is, in tf periods since the newest closed
    // window (0 = the most recently closed one). A job that emits on the
    // watermark would leave the newest windows missing; a real gap would not.
    static long[] absentAge = new long[17];

    private static final Duration POLL = Duration.ofSeconds(2);
    private static final Duration ADMIN = Duration.ofSeconds(5);

    /** Per (token, window) accumulation from the raw stream. */
    static final class Acc {
    long minPrice = Long.MAX_VALUE;
        long sumDeltaTrade;
    long maxPrice = Long.MIN_VALUE;
        long sumDeltaAll;
        long sumLastQtyAll;
        long tradeRows;
        long rows;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            usage("missing <minutes>");
        }
        int minutes;
        try {
            minutes = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            usage("minutes must be an integer, got '" + args[0] + "'");
            return;
        }
        if (minutes <= 0) {
            usage("minutes must be positive");
        }
        String tf = args.length > 1 && !args[1].startsWith("--") ? args[1] : "ONE_M";
        // Deliberately a format check, not a list of accepted timeframes: the
        // probe only needs the suffix the candle table uses, and inventing an
        // enum here would silently rot when the job adds a timeframe.
        if (!tf.matches("[A-Z0-9_]+")) {
            usage("timeframe must look like ONE_M, got '" + tf + "'");
        }
        String bootstrap = "fluss-coordinator:9123";
        for (int i = 1; i + 1 < args.length; i++) {
            if ("--bootstrap".equals(args[i])) {
                bootstrap = args[i + 1];
            }
        }
        Map<String, Long> tfMillisByName = Map.of(
                "FIFTEEN_S", 15_000L, "THIRTY_S", 30_000L, "ONE_M", 60_000L,
                "THREE_M", 180_000L, "FIVE_M", 300_000L, "FIFTEEN_M", 900_000L);
        if (!tfMillisByName.containsKey(tf)) {
            throw new IllegalArgumentException("unknown tf: " + tf + " (use a Timeframe discriminator)");
        }
        long tfMillis = tfMillisByName.get(tf);

        int tokenIdx = col("instrument_token");
        int eventTimeIdx = col("event_time");
        int tickTypeIdx = col("tick_type");
        int deltaIdx = col("volume_delta");
        int priceIdx = col("last_price_paise");
        int lastQtyIdx = col("last_qty");

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        try (Connection conn = ConnectionFactory.createConnection(conf);
             Admin admin = conn.getAdmin()) {
            Table raw = conn.getTable(TablePath.of("default", RawTableSchema.TABLE));
            List<Integer> buckets = new ArrayList<>();
            for (int b = 0; b < raw.getTableInfo().getNumBuckets(); b++) {
                buckets.add(b);
            }
            Map<String, Acc> acc = new LinkedHashMap<>();
            // I subscribe AFTER the job does, so the window in progress at subscription time
            // was only partly observed by me: judging it would under-count and look like a
            // mismatch. The scan may also replay backlog rows older than my subscription, so
            // "the first window I see" is not a safe boundary; skip by wall-clock time.
            // Any window that started before I subscribed may be partial.
            long subscribedAt = System.currentTimeMillis();
            try (LogScanner scanner = raw.newScan().createLogScanner()) {
                subscribeFromLatest(admin, scanner, TablePath.of("default", RawTableSchema.TABLE), buckets);
                long deadline = System.nanoTime() + minutes * 60_000_000_000L;
                while (System.nanoTime() < deadline) {
                    ScanRecords records = scanner.poll(POLL);
                    if (records == null) {
                        continue;
                    }
                    for (ScanRecord record : records) {
                        InternalRow row = record.getRow();
                        long token = row.getLong(tokenIdx);
                        long window = Math.floorDiv(row.getLong(eventTimeIdx), tfMillis) * tfMillis;
                        Acc a = acc.computeIfAbsent(token + ":" + window, k -> new Acc());
                        long delta = row.isNullAt(deltaIdx) ? 0L : row.getLong(deltaIdx);
                        String type = row.isNullAt(tickTypeIdx) ? "" : row.getString(tickTypeIdx).toString();
                        a.rows++;
                        a.sumDeltaAll += delta;
                        a.sumLastQtyAll += row.isNullAt(lastQtyIdx) ? 0L : row.getLong(lastQtyIdx);
                        { long px = row.isNullAt(priceIdx) ? -1L : row.getLong(priceIdx);
                        if ("TRADE".equals(type)) {
                          if (px > 0) { if (px < a.minPrice) a.minPrice = px; if (px > a.maxPrice) a.maxPrice = px; }
                            // XC-4: these two MUST stay inside the TRADE branch — outside it
                            // they count QUOTE rows too, skewing the parity sum and making
                            // tradeRows>=1 always (killing the zero-trade diagnostic).
                            a.sumDeltaTrade += delta;
                            a.tradeRows++;
                        }
                        }
                    }
                }
            }
            long latestWindow = (System.currentTimeMillis() / tfMillis) * tfMillis;
            System.out.println("raw: accumulated " + acc.size() + " (token,window) pairs over "
                    + minutes + " min(s), tf=" + tf + "; judging only fully closed windows");
            // The repo's own KV lookups use the plain lookuper with the PK values in PK order;
            // lookupBy() is the *prefix* API and Fluss rejects it for the full key.
            try (java.io.PrintWriter dump = new java.io.PrintWriter("/tmp/v4-candle-raw-sums.tsv")) {
                dump.println("token\twindow\tsumDeltaTrade\ttradeRows\tsumDeltaAll\tsumLastQtyAll\trows");
                for (Map.Entry<String, Acc> e : acc.entrySet()) {
                    Acc a = e.getValue();
                    dump.println(e.getKey().replace(':', '\t') + "\t" + a.sumDeltaTrade + "\t"
                            + a.tradeRows + "\t" + a.sumDeltaAll + "\t" + a.sumLastQtyAll + "\t" + a.rows);
                }
            }
            Lookuper lookuper = conn.getTable(TablePath.of("default", "candle_features"))
                    .newLookup().createLookuper();
            int judged = 0;
            int matched = 0;
            int mismatched = 0;
            int absent = 0;
            for (Map.Entry<String, Acc> e : acc.entrySet()) {
                String[] parts = e.getKey().split(":");
                long token = Long.parseLong(parts[0]);
                long window = Long.parseLong(parts[1]);
                if (window >= latestWindow) {
                    continue;   // still open -- the job may not have closed it yet
                }
                if (window < subscribedAt) {
                    continue;   // started before I subscribed -> I may have missed its first rows
                }
                judged++;
                org.apache.fluss.client.lookup.LookupResult res;
                try {
                    res = lookuper.lookup(GenericRow.of(token, BinaryString.fromString(tf), window))
                            .get(5, TimeUnit.SECONDS);
                } catch (Exception lookupFailure) {
                    System.out.println("  LOOKUP_FAILED token=" + token + " window=" + window
                            + " -> " + lookupFailure.getClass().getSimpleName());
                    absent++;
                    continue;
                }
                InternalRow candle = res == null ? null : res.getSingletonRow();
                // Wave C W-C5a: candle_features carries forming rows too — a
                // verdict needs the terminal SEALED row, so an unsealed row
                // counts as absent (the job has not closed the window yet).
                boolean sealedRow = candle != null
                        && candle.getBoolean(col("sealed", "candle_features"));
                if (candle == null || !sealedRow) {
                    absent++;
                    // Distinguishes "the candle table has no row" from "this window
                    // had no trade at all, so there was nothing to open a candle
                    // from". Only the second is expected and benign.
                    if (e.getValue().tradeRows == 0) {
                        absentNoTrades++;
                    }
                    long periods = latestWindow > window ? (latestWindow - window) / tfMillis : 0;
                    absentAge[(int) Math.min(periods, 16)]++;
                    continue;
                }
                int volumeIdx = col("volume", "candle_features");
                int tickCountIdx = col("tick_count", "candle_features");
                int highIdx = col("high_paise", "candle_features");
                long candleVolume = candle.getLong(volumeIdx);
                int lowIdx = col("low_paise", "candle_features");
                int candleTicks = candle.getInt(tickCountIdx);
                long candleHigh = candle.getLong(highIdx);
                boolean ok = candleVolume == e.getValue().sumDeltaTrade;
                long candleLow = candle.getLong(lowIdx);
                // XC-4: the three OHLC diagnostics describe every judged window; they
                // used to run split across the match/mismatch branches (high only when
                // matched, low/range only when mismatched), making the printed counts
                // misleading. They never decide pass/fail.
                if (candleHigh < e.getValue().maxPrice) highBias++;
                if (candleLow > e.getValue().minPrice) lowBias++;
                if (candleHigh - candleLow > e.getValue().maxPrice - e.getValue().minPrice) rangeWider++;
                if (ok) {
                    matched++;
                } else {
                    mismatched++;
                }
                if (judged <= 12 || !ok) {
                    System.out.println("  token=" + token + " window=" + window
                            + " candle(volume=" + candleVolume + ", ticks=" + candleTicks + ")"
                            + " raw(deltaTrade=" + e.getValue().sumDeltaTrade
                            + ", tradeRows=" + e.getValue().tradeRows
                            + ", deltaAll=" + e.getValue().sumDeltaAll
                            + ", naiveLastQty=" + e.getValue().sumLastQtyAll
                            + ", rows=" + e.getValue().rows + ") -> " + (ok ? "MATCH" : "MISMATCH"));
                }
            }
                        StringBuilder ages = new StringBuilder();
            for (int i = 0; i < absentAge.length; i++) {
                if (absentAge[i] > 0) {
                    ages.append(ages.length() == 0 ? "" : " ").append(i == 16 ? "16+" : String.valueOf(i)).append(":").append(absentAge[i]);
                }
            }
            System.out.println("ABSENT by age in tf periods (0 = newest closed): " + ages);
            System.out.println("ABSENT windows with zero TRADE rows: " + absentNoTrades + " of " + absent + " (the rest have trades but no candle row)");
            System.out.println("OHLC judged=" + judged + " high_short_of_raw=" + highBias + " low_above_raw=" + lowBias + " range_wider_than_raw=" + rangeWider);
            System.out.println("SUMMARY judged=" + judged + " matched=" + matched
                    + " mismatched=" + mismatched + " absentCandle=" + absent);
            System.out.println(mismatched == 0 && matched > 0
                    ? "VERDICT candle volume == sum(volume_delta over TRADE rows) on live data"
                    : "VERDICT NOT PROVEN (see lines above)");
        }
    }

    private static void usage(String problem) {
        System.err.println("CandleVerify: " + problem);
        System.err.println("usage: CandleVerify <minutes> [<timeframe>] [--bootstrap <host:port>]");
        System.err.println("  Compares live candle rows against an independent accumulation of");
        System.err.println("  raw_table_1 for the same (token, window): volume against sum(volume_delta");
        System.err.println("  over TRADE rows), and candle high/low against the raw extremes of");
        System.err.println("  last_price_paise. Windows still open are skipped.");
        System.exit(2);
    }

    private static int col(String name) {
        return col(name, RawTableSchema.TABLE);
    }

    private static int col(String name, String table) {
        if (!RawTableSchema.TABLE.equals(table)) {
            // candle_features lives in its own DDL; resolving by reading the
            // table's own schema is not available here, so the indices are
            // pinned by the DDL order instead (DDL 35: the 15 candle columns +
            // features + sealed).
            String[] candleCols = {"instrument_token", "exchange", "symbol", "tf", "window_start",
                    "window_end", "open_paise", "high_paise", "low_paise", "close_paise", "volume",
                    "tick_count", "last_event_time", "last_event_fingerprint", "schema_version",
                    "features", "sealed"};
            for (int i = 0; i < candleCols.length; i++) {
                if (candleCols[i].equals(name)) {
                    return i;
                }
            }
            throw new IllegalStateException("no such candle column: " + name);
        }
        int index = RawTableSchema.COLUMNS.indexOf(name);
        if (index < 0) {
            throw new IllegalStateException("no such column in the contract: " + name);
        }
        return index;
    }

    private static void subscribeFromLatest(
            Admin admin, LogScanner scanner, TablePath tablePath, Collection<Integer> buckets)
            throws Exception {
        long timeoutMs = ADMIN.toMillis();
        for (PartitionInfo p : admin.listPartitionInfos(tablePath).get(timeoutMs, TimeUnit.MILLISECONDS)) {
            Map<Integer, Long> m = admin.listOffsets(tablePath, p.getPartitionName(), buckets, new LatestSpec())
                    .all().get(timeoutMs, TimeUnit.MILLISECONDS);
            m.forEach((bucket, offset) -> scanner.subscribe(p.getPartitionId(), bucket, offset));
        }
    }
}
