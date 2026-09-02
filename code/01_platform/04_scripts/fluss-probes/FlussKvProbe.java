import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

import java.util.concurrent.TimeUnit;

/**
 * FlussKvProbe — B2 CP9->CP10 passive sampler (plan
 * docs/plans/2026-09-01-stage-throughput-latency-detection-plan.md, Stage B2).
 *
 * KV point-lookup probe: looks up preview rows by PK
 * (instrument_token, window_start) for the CURRENT 15s window of a small
 * fixed token sample, printing one TSV row per found row:
 *
 *   <epoch_ms>\t<token>\t<window_start>\t<output_ts>\t<last_event_ts>\t<staleness_ms>
 *
 * staleness_ms = epoch_ms - last_event_ts = CP9(commit) -> CP10(readable)
 * visibility staleness: how long before a consumer reading NOW can see an
 * event committed at last_event_ts. output_ts is the pipeline's EVENT-time
 * stamp (synthetic feed clock, which may run slightly AHEAD of wall — the
 * feed allows 2000ms future skew), so epoch_ms - output_ts goes NEGATIVE
 * under skew and is NOT a wall-clock latency (observed 2026-09-02 A/B:
 * p50 -113ms). last_event_ts is carried from the raw event and tracks the
 * same synthetic clock, but staleness vs it stays meaningful: it measures
 * the emit cadence + read path (preview rows emit ~1/s/key).
 *
 * The probe reads the CURRENT window first (freshest row), falling back to
 * the PREVIOUS window (at a window boundary the fresh window has no preview
 * for ~1s). Both windows are within the 60s TTL.
 *
 * Cost: one KV lookup per token per invocation, on a fixed 5-token sample,
 * invoked once per capture tick (5s) => <= 1 lookup/s total. Negligible and
 * A/B-validated (probes-on vs probes-off at one tier before trusting).
 *
 * Usage: java -cp <cp> FlussKvProbe <table> <window_ms> <tokens_csv>
 *                                          [bootstrap]
 */
public class FlussKvProbe {
    public static void main(String[] args) throws Exception {
        String table = args.length > 0 ? args[0] : "feature_candles_15s_preview";
        long windowMs = args.length > 1 ? Long.parseLong(args[1]) : 15000L;
        String tokensRaw = args.length > 2 ? args[2] : "4,7,13,17,19";
        String bootstrap = args.length > 3 ? args[3] : "localhost:9123";
        long epochMs = System.currentTimeMillis();
        long windowStart = (epochMs / windowMs) * windowMs;
        // Current window FIRST, previous as fallback (see class doc). The
        // current window's row is the freshest (<= 1 preview interval old);
        // preferring it keeps read_lag_ms a true commit->read measure. The
        // previous window's row can be up to 15s old yet still within the 60s
        // TTL — reading it first would inflate the lag (observed 2026-09-02
        // smoke: 3.1s instead of <= 1s).
        long[] windows = {windowStart, windowStart - windowMs};
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        TablePath tp = TablePath.of("default", table);
        String[] toks = tokensRaw.split(",");
        try (Connection c = ConnectionFactory.createConnection(conf);
             Table t = c.getTable(tp)) {
            Lookuper lookuper = t.newLookup().createLookuper();
            for (String tok : toks) {
                long token = Long.parseLong(tok.trim());
                for (long w : windows) {
                    InternalRow key = GenericRow.of(token, w);
                    InternalRow row = lookuper.lookup(key).get(2, TimeUnit.SECONDS).getSingletonRow();
                    if (row != null) {
                        // Column layout of feature_candles_15s_preview (v2,
                        // 15 cols): see CandlePreviewColumns.java — output_ts
                        // = col 12, last_event_ts = col 13.
                        long outputTs = row.getLong(12);
                        long lastEventTs = row.getLong(13);
                        long stalenessMs = epochMs - lastEventTs;
                        System.out.println(epochMs + "\t" + token + "\t" + w + "\t"
                                + outputTs + "\t" + lastEventTs + "\t" + stalenessMs);
                        break;
                    }
                }
            }
        }
    }
}
