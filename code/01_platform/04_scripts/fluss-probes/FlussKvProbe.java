import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

import java.util.concurrent.TimeUnit;

/**
 * FlussKvProbe — B2 CP9-&gt;CP10 passive sampler (plan
 * docs/plans/2026-09-01-stage-throughput-latency-detection-plan.md, Stage B2).
 *
 * KV point-lookup probe: looks up candle rows by PK
 * (instrument_token, tf, window_start) for the CURRENT 15s window of a small
 * fixed token sample, printing one TSV row per found row:
 *
 *   &lt;epoch_ms&gt;\t&lt;token&gt;\t&lt;window_start&gt;\t&lt;window_end&gt;\t&lt;last_event_time&gt;\t&lt;staleness_ms&gt;
 *
 * staleness_ms = epoch_ms - last_event_time = CP9(commit) -&gt; CP10(readable)
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
 * New-layout column indices (candle_live / candle_closed, 15-col v1):
 * token=0, tf=3, window_start=4, window_end=5, last_event_time=12.
 *
 * The probe reads the CURRENT window first (freshest row), falling back to
 * the PREVIOUS window (at a window boundary the fresh window has no row
 * for ~1s). Both windows are within the 60s TTL.
 *
 * Cost: one KV lookup per token per invocation, on a fixed 5-token sample,
 * invoked once per capture tick (5s) =&gt; &lt;= 1 lookup/s total. Negligible and
 * A/B-validated (probes-on vs probes-off at one tier before trusting).
 *
 * Usage: java -cp &lt;cp&gt; FlussKvProbe &lt;table&gt; &lt;window_ms&gt; &lt;tokens_csv&gt;
 *                                          [bootstrap] [tf]
 */
public class FlussKvProbe {
    public static void main(String[] args) throws Exception {
        String table = args.length > 0 ? args[0] : "candle_live";
        long windowMs = args.length > 1 ? Long.parseLong(args[1]) : 15000L;
        String tokensRaw = args.length > 2 ? args[2] : "4,7,13,17,19";
        String bootstrap = args.length > 3 ? args[3] : "localhost:9123";
        String tf = args.length > 4 ? args[4] : "FIFTEEN_S";
        long epochMs = System.currentTimeMillis();
        long windowStart = (epochMs / windowMs) * windowMs;
        // Current window FIRST, previous as fallback (see class doc). The
        // current window's row is the freshest (&lt;= 1 live interval old);
        // preferring it keeps staleness_ms a true commit-&gt;read measure. The
        // previous window's row can be up to 15s old yet still within the 60s
        // TTL — reading it first would inflate the lag (observed 2026-09-02
        // smoke: 3.1s instead of &lt;= 1s).
        long[] windows = {windowStart, windowStart - windowMs};
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        TablePath tp = TablePath.of("default", table);
        String[] toks = tokensRaw.split(",");
        BinaryString tfBin = BinaryString.fromString(tf);
        try (Connection c = ConnectionFactory.createConnection(conf);
             Table t = c.getTable(tp)) {
            Lookuper lookuper = t.newLookup().createLookuper();
            for (String tok : toks) {
                long token = Long.parseLong(tok.trim());
                for (long w : windows) {
                    InternalRow key = GenericRow.of(token, tfBin, w);
                    InternalRow row = lookuper.lookup(key).get(2, TimeUnit.SECONDS).getSingletonRow();
                    if (row != null) {
                        long windowEnd = row.getLong(5);
                        long lastEventTime = row.getLong(12);
                        long stalenessMs = epochMs - lastEventTime;
                        System.out.println(epochMs + "\t" + token + "\t" + w + "\t"
                                + windowEnd + "\t" + lastEventTime + "\t" + stalenessMs);
                        break;
                    }
                }
            }
        }
    }
}
