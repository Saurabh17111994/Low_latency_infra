import com.trading.common.schema.RawTableSchema;
import java.time.Duration;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.client.admin.OffsetSpec.LatestSpec;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;

/**
 * EventDayProbe - B1/T5.1: how often does a newly written row carry an event_day
 * older than the newest partition in use?
 *
 * WHY: Fluss 1.0.0 sets newDiscoveryOffsetsInitializer = OffsetsInitializer.earliest()
 * with no configuration option, so a partition discovered after job start is read from
 * the beginning. On raw_table_1 that means a late tick routed into an old event_day
 * partition, discovered later, can be read again - a silent replay whose only absorber
 * is the job's dedup horizon. Whether that is a scheduling note or a blocking defect
 * depends on how often late ticks land in old partitions, which the plan (T5.1) says
 * is decidable from raw_table_1. This measures exactly that.
 *
 * HOW: subscribes from the latest offset and watches new rows for N minutes. It does
 * NOT assume a timezone or a date format: it compares each row's event_day against the
 * NEWEST event_day seen in the same observation window, so a partition is "old" purely
 * relative to the newest one in use. Ages are bucketed, with 8+ called out because the
 * DDL retention is 9 days and a write past it fails visibly rather than silently.
 */

public class EventDayProbe {

    private static final Duration POLL = Duration.ofSeconds(2);
    private static final Duration ADMIN = Duration.ofSeconds(5);

    static long rowsSeen = 0;
    static long unparseable = 0;
    static final Map<String, Long> byDay = new LinkedHashMap<>();
    static final long[] ageBuckets = new long[10];   // 0..8 days older, 9 = "9 or more"

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
        String bootstrap = "fluss-coordinator:9123";
        for (int i = 1; i < args.length - 1; i++) {
            if ("--bootstrap".equals(args[i])) {
                bootstrap = args[i + 1];
            }
        }

        int dayIdx = col("event_day");

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        try (Connection conn = ConnectionFactory.createConnection(conf);
             Admin admin = conn.getAdmin()) {
            Table raw = conn.getTable(TablePath.of("default", RawTableSchema.TABLE));
            List<Integer> buckets = new ArrayList<>();
            for (int b = 0; b < raw.getTableInfo().getNumBuckets(); b++) {
                buckets.add(b);
            }
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
                        String day = row.isNullAt(dayIdx) ? "" : row.getString(dayIdx).toString();
                        rowsSeen++;
                        if (day.isEmpty()) {
                            unparseable++;
                            continue;
                        }
                        byDay.merge(day, 1L, Long::sum);
                    }
                }
            }
        }

        String newest = null;
        for (String d : byDay.keySet()) {
            if (newest == null || d.compareTo(newest) > 0) {
                newest = d;
            }
        }
        System.out.println("observed " + rowsSeen + " rows over " + minutes + " min(s), "
                + byDay.size() + " distinct event_day value(s), newest in use: " + newest);
        if (unparseable > 0) {
            System.out.println("  rows with no event_day value: " + unparseable);
        }
        long older = 0;
        for (Map.Entry<String, Long> e : byDay.entrySet()) {
            if (newest != null && e.getKey().compareTo(newest) < 0) {
                older += e.getValue();
                long age = -1;
                try {
                    age = ChronoUnit.DAYS.between(LocalDate.parse(e.getKey()), LocalDate.parse(newest));
                } catch (RuntimeException bad) {
                    age = -1;
                }
                if (age >= 0) {
                    ageBuckets[(int) Math.min(age, 9)] += e.getValue();
                }
                System.out.println("  OLD partition " + e.getKey() + ": " + e.getValue()
                        + " row(s), " + (age >= 0 ? age + " day(s) older" : "age unparsed"));
            }
        }
        StringBuilder hist = new StringBuilder();
        for (int i = 0; i < ageBuckets.length; i++) {
            if (ageBuckets[i] > 0) {
                hist.append(hist.length() == 0 ? "" : " ").append(i == 9 ? "9+" : String.valueOf(i)).append(":").append(ageBuckets[i]);
            }
        }
        System.out.println("AGE histogram in days older than newest (0 = newest): " + hist);
        System.out.println("SUMMARY rows=" + rowsSeen + " rowsOlderThanNewest=" + older
                + " (" + (rowsSeen == 0 ? "0" : String.format("%.2f", 100.0 * older / rowsSeen)) + "%)"
                + " daysWithRows=" + byDay.size());
        if (ageBuckets[9] > 0) {
            System.out.println("VERDICT: " + ageBuckets[9] + " row(s) landed 9 or more days behind the "
                    + "newest partition - at or beyond the 9-day retention, so a replay there cannot be "
                    + "absorbed by any horizon the job keeps.");
        } else if (older == 0) {
            System.out.println("VERDICT: no row carried an older event_day than the newest in use - "
                    + "B1 is a scheduling note for this window, not a blocking defect.");
        } else {
            System.out.println("VERDICT: " + older + " row(s) landed in an older partition, all within "
                    + "retention and within " + (ageBuckets.length - 1) + " days of the newest.");
        }
    }


    private static void usage(String problem) {
        System.err.println("EventDayProbe: " + problem);
        System.err.println("usage: EventDayProbe <minutes> [--bootstrap <host:port>]");
        System.err.println("  Watches new raw_table_1 rows for <minutes> and reports how many carry an");
        System.err.println("  event_day older than the newest one in use (B1/T5.1). Reads from the latest");
        System.err.println("  offset, so it observes writes, not history.");
        System.exit(2);
    }


    private static int col(String name) {
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
