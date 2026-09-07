package com.trading.ingestion;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec.LatestSpec;
import org.apache.fluss.client.admin.ListOffsetsResult;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.fluss.metadata.PartitionInfo;

/**
 * Read-only terminal viewer for newly persisted raw ticks.
 *
 * <p>R-155: columns are addressed by indexes derived from the shared
 * {@code raw_table_1} schema ({@code RawTableSchema.COLUMNS}, v3 21-col
 * event_day-first) — never positional literals. Literals rotted twice
 * (28-col, then v2 20-col: P1-007 displayed every field's left neighbour).
 * A schema change updates the shared list in one place; the startup check
 * in {@code main} refuses a drifted table instead of misreading it.
 */
public final class TickTableViewer {
    private static final int DEFAULT_LIMIT = 20;
    // P1-072: hard cap on the display window (Batch-3 #23 ruling: 10k lines).
    static final int MAX_LIMIT = 10_000;
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    // P1-007 fail-fast: positions derived from the shared schema, so they
    // cannot drift from the writer (FlussClientAdapter) or the DDL.
    private static final int COL_TOKEN =
            com.trading.common.schema.RawTableSchema.COLUMNS.indexOf("instrument_token");
    private static final int COL_EXCHANGE =
            com.trading.common.schema.RawTableSchema.COLUMNS.indexOf("exchange");
    private static final int COL_SYMBOL =
            com.trading.common.schema.RawTableSchema.COLUMNS.indexOf("symbol");
    private static final int COL_EVENT_TIME =
            com.trading.common.schema.RawTableSchema.COLUMNS.indexOf("event_time");
    private static final int COL_TICK_TYPE =
            com.trading.common.schema.RawTableSchema.COLUMNS.indexOf("tick_type");
    private static final int COL_LAST_PRICE =
            com.trading.common.schema.RawTableSchema.COLUMNS.indexOf("last_price_paise");
    private static final int COL_LAST_QTY =
            com.trading.common.schema.RawTableSchema.COLUMNS.indexOf("last_qty");
    private static final int COL_VALIDITY =
            com.trading.common.schema.RawTableSchema.COLUMNS.indexOf("validity_state");

    // Visible for testing: refuse a live table whose layout differs from the
    // shared schema instead of displaying shifted columns.
    static void requireSchema(java.util.List<String> liveCols) {
        java.util.List<String> want = com.trading.common.schema.RawTableSchema.COLUMNS;
        if (!liveCols.equals(want)) {
            throw new IllegalStateException(
                    "raw_table_1 schema drift: live=" + liveCols + " want=" + want);
        }
    }

    private TickTableViewer() {}

    public static void main(String[] args) throws Exception {
        String bootstrap = env("FLUSS_BOOTSTRAP", "localhost:9123");
        String tableName = env("RAW_TABLE_NAME", "raw_table_1");
        int limit = parseLimit(args);

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        TablePath tablePath = TablePath.of("default", tableName);

        try (Connection connection = ConnectionFactory.createConnection(conf);
                Admin admin = connection.getAdmin();
                Table table = connection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            // P1-007 fail-fast: the positional reads below are only valid
            // against the shared schema — fail here, not on shifted data.
            requireSchema(table.getTableInfo().getRowType().getFieldNames());
            int buckets = table.getTableInfo().getNumBuckets();
            List<Integer> bucketIds = new ArrayList<>();
            for (int bucket = 0; bucket < buckets; bucket++) {
                bucketIds.add(bucket);
            }
            subscribeFromLatest(
                    admin, scanner, tablePath, bucketIds, table.getTableInfo().isPartitioned());

            // P1-072: lines, not ScanRecords — the LogScanner may reuse row
            // buffers across polls, so retaining InternalRow references lets
            // the rendered history corrupt. Format (copy values) at poll time
            // and release the record. Also never pre-size from input: the cap
            // above bounds the deque, not the constructor argument.
            Deque<String> latest = new ArrayDeque<>();
            System.out.println("Fluss table: " + tablePath);
            System.out.println("Following new rows; showing the latest " + limit + ". Press Ctrl+C to stop.");
            while (true) {
                ScanRecords records = scanner.poll(POLL_TIMEOUT);
                if (accumulate(records, latest, limit)) {
                    printLatest(latest);
                }
            }
        }
    }

    // P1-008 fail-fast: partition-aware subscribe with bounded admin waits.
    // The naive partition-less listOffsets fails on this table ("Leader not
    // found", see FlussReadLagProbe) and an untimed get() hangs forever on
    // a stalled admin RPC. Visible for testing.
    static final Duration ADMIN_TIMEOUT = Duration.ofSeconds(5);

    static void subscribeFromLatest(
            Admin admin, LogScanner scanner, TablePath tablePath,
            Collection<Integer> buckets, boolean partitioned)
            throws Exception {
        long timeoutMs = ADMIN_TIMEOUT.toMillis();
        if (partitioned) {
            for (PartitionInfo p : admin.listPartitionInfos(tablePath).get(timeoutMs, TimeUnit.MILLISECONDS)) {
                Map<Integer, Long> m = admin.listOffsets(
                                tablePath, p.getPartitionName(), buckets, new LatestSpec())
                        .all().get(timeoutMs, TimeUnit.MILLISECONDS);
                // subscribe(long, int, long) takes the PARTITION id (verified
                // against LogScannerImpl bytecode: it builds
                // TableBucket(tableId, partitionId, bucket)); no
                // partition-name overload exists.
                m.forEach((bucket, offset) -> scanner.subscribe(p.getPartitionId(), bucket, offset));
            }
        } else {
            Map<Integer, Long> m = admin.listOffsets(tablePath, buckets, new LatestSpec())
                    .all().get(timeoutMs, TimeUnit.MILLISECONDS);
            m.forEach(scanner::subscribe);
        }
    }

    // P1-072: visible for testing — arg parsing with the display cap.
    // R-284: a non-numeric limit shows usage, not an uncaught
    // NumberFormatException.
    static int parseLimit(String[] args) {
        int limit;
        if (args.length == 0) {
            limit = DEFAULT_LIMIT;
        } else {
            try {
                limit = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Usage: TickTableViewer [limit]  — limit must be a positive integer, got '"
                                + args[0] + "'");
            }
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be greater than zero");
        }
        if (limit > MAX_LIMIT) {
            throw new IllegalArgumentException(
                    "limit must be at most " + MAX_LIMIT + " (display window cap), got " + limit);
        }
        return limit;
    }

    // P1-073: visible for testing — a null poll result means "no data yet",
    // not a crash. Returns true when at least one line was added.
    static boolean accumulate(ScanRecords records, Deque<String> latest, int limit) {
        if (records == null || records.isEmpty()) {
            return false;
        }
        boolean added = false;
        for (ScanRecord record : records) {
            if (latest.size() == limit) {
                latest.removeFirst();
            }
            latest.addLast(formatRecord(record));
            added = true;
        }
        return added;
    }

    private static void printLatest(Deque<String> records) {
        System.out.print("\033[H\033[2J");
        System.out.flush();
        System.out.println("Latest persisted ticks (" + records.size() + ")");
        System.out.println("event_time | token | exchange | symbol | price | qty | tick_type | validity | log_offset | storage_time");
        System.out.println("-----------+-------+----------+--------+-------+-----+-----------+----------+------------+-------------");
        records.forEach(System.out::println);
        System.out.println("\nUpdated: " + Instant.now() + " | Press Ctrl+C to stop.");
    }

    // P1-072: snapshot one record to a display line at poll time — the
    // caller must not retain the ScanRecord/InternalRow. Visible for testing.
    static String formatRecord(ScanRecord record) {
        InternalRow row = record.getRow();
        // R-205: numeric fields are null-guarded like the string fields — a
        // null BIGINT previously threw via row.getLong().
        return String.format(
                "%s | %s | %s | %s | %s | %s | %s | %s | %d | %s",
                longValue(row, COL_EVENT_TIME).map(TickTableViewer::toMillis).orElse(""),
                longValue(row, COL_TOKEN).map(String::valueOf).orElse(""),
                text(row, COL_EXCHANGE),
                text(row, COL_SYMBOL),
                longValue(row, COL_LAST_PRICE).map(p -> String.format("%.2f", p / 100.0)).orElse(""),
                longValue(row, COL_LAST_QTY).map(String::valueOf).orElse(""),
                text(row, COL_TICK_TYPE),
                text(row, COL_VALIDITY),
                record.logOffset(),
                record.timestamp() < 0 ? "" : Instant.ofEpochMilli(record.timestamp()));
    }

    private static java.util.Optional<Long> longValue(InternalRow row, int index) {
        if (row.isNullAt(index)) {
            return java.util.Optional.empty();
        }
        return java.util.Optional.of(row.getLong(index));
    }

    private static String toMillis(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis).toString();
    }

    private static String text(InternalRow row, int index) {
        if (row.isNullAt(index)) {
            return "";
        }
        BinaryString value = row.getString(index);
        return value == null ? "" : value.toString();
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
