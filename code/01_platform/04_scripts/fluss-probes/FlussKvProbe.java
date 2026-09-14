import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * FlussKvProbe — B2 CP9-&gt;CP10 passive sampler (plan
 * docs/Investigations_and_Reports/2026-09-01-throughput-degradation.md, Stage B2).
 *
 * KV point-lookup probe: looks up candle rows by PK
 * (instrument_token, tf, window_start) for the CURRENT 15s window of a small
 * fixed token sample, printing one TSV row per found row:
 *
 *   &lt;epoch_ms&gt;\t&lt;token&gt;\t&lt;window_start&gt;\t&lt;window_end&gt;\t&lt;last_event_time&gt;\t&lt;staleness_ms&gt;
 *
 * staleness_ms = now - last_event_time = CP9(commit) -&gt; CP10(readable)
 * visibility staleness: how long before a consumer reading NOW can see an
 * event committed at last_event_time. window_end is the STATIC event-time
 * boundary of the sampled window (kept in the TSV shape so downstream
 * parsers keep working); last_event_time is the freshness signal (the
 * freshest raw event folded into the row). For candle_live rows emit ~1/s
 * per key, so staleness tracks emit cadence + read path. For candle_closed
 * the probe reports the freshest CLOSED row's age the read path can see.
 * Same synthetic-clock caveat as before: the feed clock may run ahead of
 * wall, so small negative values can appear; a large positive value is
 * genuine visibility lag.
 *
 * Columns are resolved BY NAME from the live TableInfo (P6-371): the old
 * "token=0, tf=3, window_start=4, window_end=5, last_event_time=12" v1 layout is
 * only a default, and a reordered/renamed schema now fails loudly instead of
 * reading whatever happens to sit at index 5 or 12.
 *
 * The probe reads the CURRENT window first (freshest row), falling back to
 * the PREVIOUS window (at a window boundary the fresh window has no row
 * for ~1s). Both windows are within the 60s TTL.
 *
 * Contract (wave 13):
 *   - one token failing (timeout, execution error, client RuntimeException) is reported
 *     on stderr and the remaining tokens are still read; one slow token no longer
 *     discards every other token's rows (P6-079).
 *   - a token whose windows both miss is reported on stderr: "no row within the TTL" is
 *     itself a signal, and it must not look like a probe that never ran (P6-730).
 *   - window_ms must be a positive number and the token list must parse; both are
 *     validated before any RPC (P6-370).
 *   - the timestamp and staleness of a row are sampled when that row is emitted, not
 *     once at startup, so the sequential lookups do not under-report the lag of the
 *     later tokens (P6-372).
 *
 * Exit codes: 0 sample emitted, 1 every token failed, 2 unusable input, 3 some tokens
 * failed or missed (partial sample — rows are still printed).
 *
 * Usage: java -cp &lt;cp&gt; FlussKvProbe &lt;table&gt; &lt;window_ms&gt; &lt;tokens_csv&gt;
 *                                          [bootstrap] [tf]
 */
public class FlussKvProbe {
    static class InputException extends RuntimeException {
        InputException(String msg) {
            super(msg);
        }
    }

    public static void main(String[] args) {
        int code = 1;
        try {
            code = run(args);
        } catch (InputException e) {
            System.err.println("FlussKvProbe: " + e);
            System.exit(2);
        } catch (Throwable t) {
            System.err.println("FlussKvProbe failed: " + t);
            System.exit(1);
        }
        System.out.flush();
        System.exit(code);
    }

    static int run(String[] args) throws Exception {
        String table = args.length > 0 ? args[0] : "candle_live";
        long windowMs = args.length > 1 ? parsePositive(args[1], "window_ms") : 15000L;
        String tokensRaw = args.length > 2 ? args[2] : "4,7,13,17,19";
        String bootstrap = args.length > 3 ? args[3] : "localhost:9123";
        String tf = args.length > 4 ? args[4] : "FIFTEEN_S";
        // P6-370: windowMs <= 0 divides by zero below, and a non-numeric
        // window_ms/token used to escape as a NumberFormatException with no output at
        // all. Both are input errors.
        List<Long> tokens = parseTokens(tokensRaw);
        if (tokens.isEmpty()) {
            throw new InputException("no usable token in tokens_csv '" + tokensRaw + "'");
        }
        long now0 = System.currentTimeMillis();
        long windowStart = (now0 / windowMs) * windowMs;
        // Current window FIRST, previous as fallback (see class doc). The
        // current window's row is the freshest (<= 1 live interval old);
        // preferring it keeps staleness_ms a true commit->read measure. The
        // previous window's row can be up to 15s old yet still within the 60s
        // TTL — reading it first would inflate the lag (observed 2026-09-02
        // smoke: 3.1s instead of <= 1s).
        long[] windows = {windowStart, windowStart - windowMs};
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        TablePath tp = TablePath.of("default", table);
        BinaryString tfBin = BinaryString.fromString(tf);
        int rows = 0;
        int missed = 0;
        try (Connection c = ConnectionFactory.createConnection(conf);
             Table t = c.getTable(tp)) {
            // P6-371: resolve by name and refuse to guess.
            List<String> names = t.getTableInfo().getSchema().getRowType().getFieldNames();
            int windowEndIdx = names.indexOf("window_end");
            int lastEventIdx = names.indexOf("last_event_time");
            if (windowEndIdx < 0 || lastEventIdx < 0) {
                throw new InputException("table " + table + " has no window_end/last_event_time column"
                        + " (columns: " + names + ")");
            }
            Lookuper lookuper = t.newLookup().createLookuper();
            for (long token : tokens) {
                boolean found = false;
                try {
                    for (long w : windows) {
                        InternalRow key = GenericRow.of(token, tfBin, w);
                        InternalRow row = lookuper.lookup(key).get(2, TimeUnit.SECONDS).getSingletonRow();
                        if (row != null) {
                            long windowEnd = row.getLong(windowEndIdx);
                            // P6-371: a null last_event_time is "no event yet", not a
                            // timestamp at 1970 (which would print a 56-year staleness).
                            long lastEventTime = row.isNullAt(lastEventIdx)
                                    ? -1L : row.getLong(lastEventIdx);
                            // P6-372: sample per emitted row. The up-to-10 blocking
                            // lookups above took real time, and reusing a startup
                            // timestamp under-reported every later token's staleness.
                            long now = System.currentTimeMillis();
                            long stalenessMs = lastEventTime < 0 ? -1L : now - lastEventTime;
                            System.out.println(now + "\t" + token + "\t" + w + "\t"
                                    + windowEnd + "\t" + lastEventTime + "\t" + stalenessMs);
                            rows++;
                            found = true;
                            break;
                        }
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw ie;
                } catch (Exception e) {
                    // P6-079: one slow/failed token must not discard the others.
                    System.err.println("FlussKvProbe token=" + token + " lookup failed: " + e);
                    missed++;
                    continue;
                }
                if (!found) {
                    // P6-730: both windows empty for this token — say so, so a gap is
                    // observable instead of silently missing from the TSV.
                    System.err.println("FlussKvProbe token=" + token + " no row for windows "
                            + windows[0] + "," + windows[1]);
                    missed++;
                }
            }
        }
        if (rows == 0 && missed > 0) {
            System.err.println("FlussKvProbe: no row emitted for any of " + missed + " token(s)");
            return 1;
        }
        return missed > 0 ? 3 : 0;
    }

    /** P6-370: a positive numeric argument, or an input error (never a silent default). */
    static long parsePositive(String raw, String name) {
        final long v;
        try {
            v = Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            throw new InputException(name + " must be a number, got '" + raw + "'");
        }
        if (v <= 0) {
            throw new InputException(name + " must be > 0, got " + raw);
        }
        return v;
    }

    /** Comma-separated tokens; unparseable entries are skipped with a stderr note (P6-370). */
    static List<Long> parseTokens(String raw) {
        List<Long> out = new java.util.ArrayList<>();
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
                System.err.println("FlussKvProbe: skipping bad token '" + s + "'");
            }
        }
        return out;
    }
}
