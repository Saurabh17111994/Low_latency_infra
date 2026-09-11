package com.trading.execution.gateway;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Bounded, replayable single-writer reader for the Execution_Intent LOG. */
public final class IntentReader implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(IntentReader.class);
    private final Connection connection;
    private final Table table;
    private final LogScanner scanner;
    private final GatewayConfig config;
    private final IntentSink forwarder;
    private final Consumer<String> violationHandler;
    private final IntentDedupStore dedupStore;
    private final DurableIntentDispatcher dispatcher;
    // P3-481/P3-484: process-wide last-seen log offset (Fluss 0.9.1 ScanRecord
    // exposes offset but not bucket). Updated only after disposition, never
    // before forward/commit — a diagnostic, not a progress watermark.
    private final AtomicLong lastSeenOffset = new AtomicLong(-1L);

    public static IntentReader open(GatewayConfig config, IntentSink forwarder,
            Consumer<String> violationHandler) {
        try {
            Configuration c = new Configuration();
            c.setString("bootstrap.servers", config.flussBootstrap());
            Connection connection = ConnectionFactory.createConnection(c);
            // P3-088/P3-090: connection/table leak on partial open — clean up
            // anything acquired if schema check, dedup open, or ctor throws.
            boolean ok = false;
            Table table = null;
            IntentDedupStore dedup = null;
            try {
                table = connection.getTable(TablePath.of(config.flussDatabase(), config.intentTable()));
                if (table.getTableInfo().getRowType().getFieldCount() != 22) {
                    throw new IllegalStateException("Execution_Intent schema must contain 22 columns");
                }
                dedup = FlussIntentDedupStore.open(config);
                IntentReader reader = new IntentReader(connection, table, config, forwarder, violationHandler, dedup);
                ok = true;
                return reader;
            } finally {
                if (!ok) {
                    try { if (dedup != null) dedup.close(); } catch (Exception ignored) {}
                    try { if (table != null) table.close(); } catch (Exception ignored) {}
                    try { connection.close(); } catch (Exception ignored) {}
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("cannot open Execution_Intent", e);
        }
    }

    IntentReader(Connection connection, Table table, GatewayConfig config,
            IntentSink forwarder, Consumer<String> violationHandler,
            IntentDedupStore dedupStore) throws Exception {
        this.connection = connection; this.table = table; this.config = config;
        this.forwarder = forwarder; this.violationHandler = violationHandler;
        this.dedupStore = dedupStore;
        this.dispatcher = new DurableIntentDispatcher(dedupStore);
        this.scanner = table.newScan().createLogScanner();
    }

    /** Subscribe every bucket from offset zero. Replay is intentional after a restart. */
    public void subscribeFromBeginning() {
        // P3-482/P3-483: single metadata lookup, not one per loop iteration.
        int numBuckets = table.getTableInfo().getNumBuckets();
        for (int bucket = 0; bucket < numBuckets; bucket++) scanner.subscribe(bucket, 0L);
    }

    /**
     * Process one bounded poll. The caller owns the single-writer loop.
     *
     * <p>Redelivery contract (P3-086/P3-091): an intent whose handoff throws
     * or returns DEFERRED/REJECTED is NOT committed and NOT retried in-run —
     * the LOG cursor already advanced. Uncommitted intents are recovered by
     * replay from offset zero after restart (durable dedup keeps committed
     * ones FIRST-free). REJECTED goes to the violation handler (audit), DEFERRED
     * is logged by id for ops visibility. No in-run queue: under a sustained
     * HALTED gate it would grow unbounded.
     */
    public int poll(Duration timeout) {
        ScanRecords records = scanner.poll(timeout);
        int accepted = 0;
        for (ScanRecord record : records) {
            try {
                IntentRecord intent = decode(record.getRow(), record.logOffset());
                IntentValidator.validate(intent, config.accountScopeId(), config.executionPartitionId());
                DurableIntentDispatcher.Verdict outcome = dispatcher.classify(
                        intent.instructionId(), intent.requestHash());
                if (outcome == DurableIntentDispatcher.Verdict.HASH_VIOLATION) {
                    safeViolation("instruction hash changed: " + intent.instructionId());
                    lastSeenOffset.set(record.logOffset());
                    continue;
                }
                if (outcome == DurableIntentDispatcher.Verdict.FIRST) {
                    IntentSink.Result result;
                    try {
                        result = forwarder.forward(intent);
                    } catch (Exception deferred) {
                        LOG.warn("intent handoff deferred (instruction_id={}): {}",
                                intent.instructionId(), deferred.getMessage());
                        lastSeenOffset.set(record.logOffset());
                        continue;
                    }
                    // DEV P3-086/P3-091: explicit disposition, no silent fall-through.
                    if (result == IntentSink.Result.REJECTED) {
                        safeViolation("intent rejected: " + intent.instructionId());
                    } else if (result == IntentSink.Result.FORWARDED) {
                        try {
                            // DEV P3-310/P3-311: per-record commit is intentional —
                            // exact per-intent fail-closed audit and local/durable
                            // alignment. Batching would invent durability semantics
                            // for partial batch failures; replay is a one-time
                            // single-writer boot cost.
                            dispatcher.committed(intent.instructionId(), intent.requestHash(),
                                    record.logOffset());
                            accepted++;
                        } catch (Exception commitFailed) {
                            // A durable-commit failure means we cannot prove the
                            // handoff is idempotent; fail closed rather than
                            // silently risk a duplicate side effect on replay.
                            safeViolation("durable intent dedup commit failed: "
                                    + intent.instructionId() + " — " + commitFailed.getMessage());
                        }
                    } else {
                        LOG.warn("intent handoff deferred (instruction_id={})", intent.instructionId());
                    }
                    lastSeenOffset.set(record.logOffset());
                    continue;
                }
                // DUPLICATE: already handed off, skip silently.
                lastSeenOffset.set(record.logOffset());
            } catch (RuntimeException e) {
                // P3-309/P3-312: one poison row must not starve the batch.
                // Interrupt stays responsive: the caller loop (ExecutionGatewayMain
                // reader thread / BoundedRetry-style callers) re-interrupts or
                // unwraps RuntimeException causes; a swallowed interrupt here
                // would only delay shutdown by one bounded poll.
                safeViolation("invalid Execution_Intent at offset " + record.logOffset() + ": " + e.getMessage());
            } catch (Exception e) {
                safeViolation("invalid Execution_Intent at offset " + record.logOffset() + ": " + e.getMessage());
            }
        }
        return accepted;
    }

    // P3-309/P3-312: a throwing handler must not abort the batch or re-enter
    // the catch that invoked it — guard every handler call.
    private void safeViolation(String message) {
        try { violationHandler.accept(message); }
        catch (Exception handlerFailure) { LOG.warn("violation handler failed: {}", handlerFailure.getMessage()); }
    }

    public long lastSeenOffset() { return lastSeenOffset.get(); }
    /** @deprecated prefer {@link #lastSeenOffset()}; kept for API compat. */
    @Deprecated public java.util.Map<Integer, Long> lastOffsets() { return java.util.Map.of(-1, lastSeenOffset.get()); }
    public static IntentRecord decode(InternalRow r, long offset) {
        if (r == null) throw new IllegalArgumentException("null intent row");
        return new IntentRecord(text(r, 0), text(r, 1), text(r, 2), text(r, 3), text(r, 4),
                r.getLong(5), text(r, 6), text(r, 7), text(r, 8), r.getLong(9), text(r, 10),
                nullableLong(r, 11), text(r, 12), text(r, 13), text(r, 14), text(r, 15), text(r, 16),
                r.getLong(17), nullableLong(r, 18), text(r, 19), nullableText(r, 20), text(r, 21), offset);
    }
    private static String text(InternalRow r, int i) { return r.isNullAt(i) ? "" : r.getString(i).toString(); }
    private static String nullableText(InternalRow r, int i) { return r.isNullAt(i) ? null : r.getString(i).toString(); }
    private static Long nullableLong(InternalRow r, int i) { return r.isNullAt(i) ? null : r.getLong(i); }

    @Override public void close() throws Exception {
        // P3-087/P3-093: sequential close leaks the rest on first throw and
        // silently swallows dedup failures — collect with suppressed instead.
        Exception first = null;
        try { scanner.close(); } catch (Exception e) { first = e; }
        try { table.close(); } catch (Exception e) { if (first == null) first = e; else first.addSuppressed(e); }
        try { connection.close(); } catch (Exception e) { if (first == null) first = e; else first.addSuppressed(e); }
        try { dedupStore.close(); } catch (Exception e) {
            LOG.warn("dedup store close failed", e);
            if (first == null) first = e; else first.addSuppressed(e);
        }
        if (first != null) throw first;
    }
}
