package com.trading.common.schema.eod;

import com.trading.common.schema.fluss.BoundedRetry;

import com.trading.common.schema.EodControllerState;
import com.trading.common.schema.fluss.FlussWriteProfiles;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

// Version note (2026-09-23): the 0.9.1 claims in this file were re-checked against Fluss 1.0.0 and still hold — flush()/close is still unbounded in 1.0.0 (`fluss-client/.../write/RecordAccumulator.java:149`, unchanged since 0.9.1) and `TableWriter`/`UpsertWriter` still expose no `close()`. Re-check on the next upgrade (DEC-052).
/**
 * {@link EodStateStore} backed by the {@code eod_offload_state} KV table via
 * the Fluss raw client (SCH-23). The controller is a plain-JVM runner, so the
 * state table is single-field-PK by design (COMPAT-FLUSS-005: the raw client
 * cannot upsert composite-PK KV tables in Fluss 0.9.1) and all I/O here is
 * raw-client: current-state reads via per-bucket {@link BatchScanner} (folded
 * by {@code record_id} — last-write-wins, so re-runs converge), writes via
 * {@link UpsertWriter}.
 *
 * <p>Owns its {@link Connection} — {@link #close()} releases it.
 */
public final class FlussEodStateStore implements EodStateStore, AutoCloseable {

    private final Connection connection;
    private final Table table;
    private final TableInfo info;
    private final long timeoutMs;

    private FlussEodStateStore(Connection connection, Table table, TableInfo info,
            long timeoutMs) {
        this.connection = connection;
        this.table = table;
        this.info = info;
        this.timeoutMs = timeoutMs;
    }

    /** Open the store against {@code database.stateTable} on the cluster. */
    public static FlussEodStateStore open(String bootstrap, String database, String stateTable,
            Duration timeout) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        // D1 (2026-09-11): without a linger this connection waited on Fluss's 100ms
        // default for every write — which is what the per-record flush() here used to
        // mask. See FlussWriteProfiles for the value, the measurement, and why the
        // flush was removed in favour of this.
        FlussWriteProfiles.bulkPath(conf);
        Connection connection = ConnectionFactory.createConnection(conf);
        try {
            TablePath path = TablePath.of(database, stateTable);
            // C5: retry the table-metadata read. Idempotent, and this runs inside open() —
            // a transient here fails the whole store rather than one operation.
            TableInfo info = BoundedRetry.await(() -> connection.getAdmin().getTableInfo(path)
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS));
            Table table = connection.getTable(path);
            return new FlussEodStateStore(connection, table, info, timeout.toMillis());
        } catch (Exception e) {
            connection.close();
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    @Override
    public List<EodOffloadRecord> readAll() throws Exception {
        Map<String, InternalRow> rows = new LinkedHashMap<>();
        for (int b = 0; b < info.getNumBuckets(); b++) {
            TableBucket tb = new TableBucket(info.getTableId(), b);
            // P4-133: drain until null — one pollBatch is one batch, not the
            // bucket; a single poll silently truncates large/slow scans (the
            // controller then re-drives completed work or misses failures).
            // Bounded by timeoutMs, null-guarded (empty/exhausted bucket).
            // limit() is REQUIRED by Fluss 0.9.1 for a BatchScanner
            // (TableScan.createBatchScanner throws UnsupportedOperationException
            // without it), so readAll — and therefore `status` — failed outright
            // until this was added (2026-09-11, found by running the controller
            // against the live cluster). MAX_VALUE is the "no effective limit"
            // spelling used by the other two scan sites in EodControllerTool;
            // the drain loop below is what terminates the scan.
            try (BatchScanner scanner = table.newScan().limit(Integer.MAX_VALUE)
                    .createBatchScanner(tb)) {
                while (true) {
                    CloseableIterator<InternalRow> batch =
                            scanner.pollBatch(Duration.ofMillis(timeoutMs));
                    if (batch == null) {
                        break;
                    }
                    try (CloseableIterator<InternalRow> it = batch) {
                        boolean any = false;
                        while (it.hasNext()) {
                            any = true;
                            InternalRow row = it.next();
                            rows.put(row.getString(EodOffloadStateColumns.RECORD_ID).toString(),
                                    row);
                        }
                        if (!any) {
                            break;
                        }
                    }
                }
            }
        }
        List<EodOffloadRecord> out = new ArrayList<>();
        for (InternalRow row : rows.values()) {
            String recordId = row.getString(EodOffloadStateColumns.RECORD_ID).toString();
            if (EodOffloadStateColumns.isLeaseRecordId(recordId)) {
                continue; // the lease row is not an offload record
            }
            // P4-137: one corrupt/evolved row must not fail the whole scan
            // (a VERIFIED day disappearing from readAll looks like permission
            // to expire its source). Skip-and-log, fail-safe: skipped rows are
            // treated as missing, never as verified.
            try {
                out.add(toRecord(row));
            } catch (RuntimeException corrupt) {
                System.err.println("eod-state: skipping corrupt row " + recordId + ": "
                        + corrupt.getMessage());
            }
        }
        return out;
    }

    @Override
    public void upsert(EodOffloadRecord record) throws Exception {
        Object[] values = new Object[EodOffloadStateColumns.FIELD_COUNT];
        values[EodOffloadStateColumns.RECORD_ID] = BinaryString.fromString(
                EodOffloadStateColumns.recordId(record.tradingDate(), record.tableName()));
        values[EodOffloadStateColumns.TRADING_DATE] = BinaryString.fromString(record.tradingDate());
        values[EodOffloadStateColumns.TABLE_NAME] = BinaryString.fromString(record.tableName());
        values[EodOffloadStateColumns.SCHEMA_VERSION] = BinaryString.fromString(record.schemaVersion());
        values[EodOffloadStateColumns.SOURCE_OFFSET_START] = record.sourceOffsetStart();
        values[EodOffloadStateColumns.SOURCE_OFFSET_END] = record.sourceOffsetEnd();
        values[EodOffloadStateColumns.ROW_COUNT] = record.rowCount();
        values[EodOffloadStateColumns.BYTE_COUNT] = record.byteCount();
        values[EodOffloadStateColumns.SOURCE_HASH] = BinaryString.fromString(record.sourceHash());
        values[EodOffloadStateColumns.TARGET_HASH] = BinaryString.fromString(record.targetHash());
        values[EodOffloadStateColumns.ICEBERG_SNAPSHOT_ID] =
                BinaryString.fromString(record.icebergSnapshotId());
        values[EodOffloadStateColumns.STATE] = BinaryString.fromString(record.state().name());
        values[EodOffloadStateColumns.RETRY_COUNT] = record.retryCount();
        values[EodOffloadStateColumns.NEXT_RETRY_AT_MS] = record.nextRetryAtMs();
        values[EodOffloadStateColumns.EARLIEST_ALLOWED_SOURCE_EXPIRY_MS] =
                record.earliestAllowedSourceExpiryMs();
        values[EodOffloadStateColumns.UPDATED_AT_MS] = record.updatedAtMs();
        values[EodOffloadStateColumns.STATE_SCHEMA_VERSION] =
                BinaryString.fromString(EodOffloadStateColumns.STATE_SCHEMA_VERSION_V1);
        // P4-134 (final, 2026-09-11): no per-record flush, and the earlier note that
        // get()-before-flush() "risks hanging to timeout" is backwards — it is the
        // flush that is unbounded (see FlussWriteProfiles). This write used to flush
        // before and in a finally; open() set no linger, so the flush was forcing the
        // batch out past Fluss's 100ms default. bulkPath() now bounds the send delay,
        // and the finally flush is gone because in that slot it awaited an unsendable
        // batch and swallowed the TimeoutException instead of propagating it.
        // No try-with-resources: UpsertWriter/TableWriter is flush-only in Fluss 0.9.1
        // (see FlussWriteProfiles for the D2 rationale — same shape as the positions store).
        UpsertWriter writer = table.newUpsert().createWriter();
        // C5/P3-268: retry the EOD state write. KV upsert keyed by the state table's
        // primary key, so idempotent by construction and safe to repeat. await()
        // declares the same three types Future.get() does, so the enclosing handling
        // is unchanged (no new catch arm).
        BoundedRetry.await(() -> {
            writer.upsert(GenericRow.of(values)).get(timeoutMs, TimeUnit.MILLISECONDS);
            return null;
        });
    }

    @Override
    public Lease acquireLease(String token, long nowMs, long leaseTtlMs) throws Exception {
        // P4-294 note: Lookuper is a bare interface in Fluss 0.9.1 (no
        // AutoCloseable — verified by decompile in Group F), so there is
        // nothing to close; the per-call creation holds no releasable handle.
        // P4-135: check-then-act has no atomic CAS in the raw client — two
        // controllers racing an expired lease can both acquire. Best-effort
        // fencing only (see EodStateStore.acquireLease contract); callers
        // must re-check isHeldBy before committing VERIFIED.
        Lookuper lookuper = table.newLookup().createLookuper();
        // C5/P3-268: retry the lease read. Point read by lease_record_id, so idempotent.
        InternalRow found = BoundedRetry.await(() -> lookuper
                .lookup(GenericRow.of(BinaryString.fromString(
                        EodOffloadStateColumns.LEASE_RECORD_ID)))
                .get(timeoutMs, TimeUnit.MILLISECONDS).getSingletonRow());
        if (found != null) {
            long expiry = found.getLong(EodOffloadStateColumns.SOURCE_OFFSET_START);
            String holder = found.getString(EodOffloadStateColumns.SOURCE_HASH).toString();
            if (expiry >= nowMs && !token.equals(holder)) {
                // held by another, unexpired — refuse (best-effort fencing)
                return new Lease(holder, expiry,
                        found.getLong(EodOffloadStateColumns.UPDATED_AT_MS));
            }
        }
        // absent, expired, or our own token — acquire/refresh
        upsertLease(token, nowMs + leaseTtlMs, nowMs);
        return new Lease(token, nowMs + leaseTtlMs, nowMs);
    }

    private void upsertLease(String token, long expiryMs, long acquiredAtMs) throws Exception {
        Object[] values = new Object[EodOffloadStateColumns.FIELD_COUNT];
        values[EodOffloadStateColumns.RECORD_ID] =
                BinaryString.fromString(EodOffloadStateColumns.LEASE_RECORD_ID);
        values[EodOffloadStateColumns.TRADING_DATE] =
                BinaryString.fromString(EodOffloadStateColumns.LEASE_TRADING_DATE);
        values[EodOffloadStateColumns.TABLE_NAME] =
                BinaryString.fromString(EodOffloadStateColumns.LEASE_TABLE_NAME);
        values[EodOffloadStateColumns.SCHEMA_VERSION] =
                BinaryString.fromString(EodOffloadStateColumns.STATE_SCHEMA_VERSION_V1);
        values[EodOffloadStateColumns.SOURCE_OFFSET_START] = expiryMs; // lease expiry
        values[EodOffloadStateColumns.SOURCE_OFFSET_END] = 0L;
        values[EodOffloadStateColumns.ROW_COUNT] = 0L;
        values[EodOffloadStateColumns.BYTE_COUNT] = 0L;
        values[EodOffloadStateColumns.SOURCE_HASH] = BinaryString.fromString(token); // lease token
        values[EodOffloadStateColumns.TARGET_HASH] = BinaryString.fromString("");
        values[EodOffloadStateColumns.ICEBERG_SNAPSHOT_ID] = BinaryString.fromString("LEASE");
        values[EodOffloadStateColumns.STATE] = BinaryString.fromString("ACTIVE");
        values[EodOffloadStateColumns.RETRY_COUNT] = 0;
        values[EodOffloadStateColumns.NEXT_RETRY_AT_MS] = 0L;
        values[EodOffloadStateColumns.EARLIEST_ALLOWED_SOURCE_EXPIRY_MS] = Long.MAX_VALUE;
        values[EodOffloadStateColumns.UPDATED_AT_MS] = acquiredAtMs;
        values[EodOffloadStateColumns.STATE_SCHEMA_VERSION] =
                BinaryString.fromString(EodOffloadStateColumns.STATE_SCHEMA_VERSION_V1);
        // P4-136: same shape as upsert() — one bounded get(), no flush (see there
        // and FlussWriteProfiles); no try-with-resources (UpsertWriter is
        // flush-only in 0.9.1). A lease write that cannot be acked must fail loudly
        // here rather than hang, since acquireLease is the mutual-exclusion path.
        UpsertWriter writer = table.newUpsert().createWriter();
        // C5/P3-268: retry the EOD state write. KV upsert keyed by the state table's
        // primary key, so idempotent by construction and safe to repeat. await()
        // declares the same three types Future.get() does, so the enclosing handling
        // is unchanged (no new catch arm).
        BoundedRetry.await(() -> {
            writer.upsert(GenericRow.of(values)).get(timeoutMs, TimeUnit.MILLISECONDS);
            return null;
        });
    }

    private static EodOffloadRecord toRecord(InternalRow r) {
        // P4-137 (decode half): the compact ctor now fails fast on null/
        // malformed fields, and readAll() skips-and-logs corrupt rows — a
        // corrupt row never fails the scan or masquerades as verified.
        return new EodOffloadRecord(
                r.getString(EodOffloadStateColumns.TRADING_DATE).toString(),
                r.getString(EodOffloadStateColumns.TABLE_NAME).toString(),
                r.getString(EodOffloadStateColumns.SCHEMA_VERSION).toString(),
                r.getLong(EodOffloadStateColumns.SOURCE_OFFSET_START),
                r.getLong(EodOffloadStateColumns.SOURCE_OFFSET_END),
                r.getLong(EodOffloadStateColumns.ROW_COUNT),
                r.getLong(EodOffloadStateColumns.BYTE_COUNT),
                r.getString(EodOffloadStateColumns.SOURCE_HASH).toString(),
                r.getString(EodOffloadStateColumns.TARGET_HASH).toString(),
                r.getString(EodOffloadStateColumns.ICEBERG_SNAPSHOT_ID).toString(),
                EodControllerState.valueOf(r.getString(EodOffloadStateColumns.STATE).toString()),
                r.getInt(EodOffloadStateColumns.RETRY_COUNT),
                r.getLong(EodOffloadStateColumns.NEXT_RETRY_AT_MS),
                r.getLong(EodOffloadStateColumns.EARLIEST_ALLOWED_SOURCE_EXPIRY_MS),
                r.getLong(EodOffloadStateColumns.UPDATED_AT_MS));
    }
}
