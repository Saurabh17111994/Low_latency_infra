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
    // D4: bounded in-run defer tolerance — see forwardWithBoundedDefer.
    private static final int DEFER_MAX_ATTEMPTS = 3;
    private static final long DEFER_RETRY_DELAY_MS = 200L;
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
     * <p>Redelivery contract (P3-086/P3-091, D4): an intent whose handoff returns
     * {@code DEFERRED} is retried in-run within a bounded budget
     * ({@link #forwardWithBoundedDefer}); if it is still deferred it is NOT committed and is
     * logged — a not-ENABLED gate deferring a valid intent is the designed fail-closed outcome,
     * not a violation — while {@code REJECTED} and a sink that throws past the budget ARE
     * reported to the violation handler. Uncommitted intents are recovered by replay
     * from offset zero after restart (durable dedup keeps committed ones
     * FIRST-free). There is no in-run queue — the retry holds at most one in-flight
     * intent, so a sustained HALTED gate cannot grow memory.
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
                        result = forwardWithBoundedDefer(intent);
                    } catch (Exception deferred) {
                        // D4: budget exhausted with a throwing sink — explicit
                        // rejection, never a silent defer.
                        safeViolation("intent handoff deferred past budget: "
                                + intent.instructionId() + " — " + deferred.getMessage());
                        lastSeenOffset.set(record.logOffset());
                        continue;
                    }
                    // DEV P3-086/P3-091: explicit disposition, no silent fall-through.
                    if (result == IntentSink.Result.REJECTED) {
                        safeViolation("intent rejected: " + intent.instructionId());
                    } else if (result == IntentSink.Result.FORWARDED) {
                        try {
                            // DEV P3-310/P3-311: per-record commit is intentional and
                            // RETAINED. The N+1 is real — one blocking durable RPC per
                            // FORWARDED intent (FlussIntentDedupStore.record awaits its
                            // upsert ack), and D1's linger does NOT coalesce it: writes
                            // serialize on that await, so linger only bounds the send
                            // delay rather than batching. Batching was declined because a
                            // partially-failed batch has no honest accounting: it either
                            // drops uncommitted intents silently or re-forwards them on
                            // replay (a duplicate money side effect). Cost is bounded and
                            // one-time (boot replay, single-writer), and durable-first
                            // ordering keeps a throw "unproven → fail closed".
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
                        // D4/P3-091: DEFERRED past the bounded budget is LOGGED,
                        // not routed to the violation handler. A not-ENABLED gate
                        // deferring a valid intent is the designed fail-closed
                        // outcome; the violation channel carries invalid intents,
                        // explicit REJECTEDs and durable-commit failures, so
                        // alarming here would make a planned HALT look like bad
                        // data (B4.2 asserts a valid deferred intent must not
                        // violate). The intent stays uncommitted and is recovered
                        // by replay from offset zero.
                        LOG.warn("intent handoff deferred past budget (instruction_id={}, "
                                + "max_attempts={})", intent.instructionId(), DEFER_MAX_ATTEMPTS);
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

    /**
     * D4: bounded in-run defer tolerance. A {@code DEFERRED} handoff used to be
     * dropped on the first attempt (recovered only by replay-from-zero after a
     * restart). Instead the single-writer loop blocks and retries within a bounded
     * budget — that stall <i>is</i> the backpressure signal (the LOG cursor stops
     * advancing, so the broker/intent producer is slowed rather than ignored), and
     * it costs O(1) memory: one in-flight intent, never an unbounded queue. When
     * the budget is exhausted the caller stops retrying and logs at WARN (never the
     * violation channel — see {@link #poll}), so a sustained HALTED gate stays
     * observable without being mistaken for invalid data.
     *
     * <p>Blocking is bounded by {@code DEFER_MAX_ATTEMPTS × DEFER_RETRY_DELAY_MS},
     * so a permanently-deferred intent cannot stall the loop indefinitely.
     */
    private IntentSink.Result forwardWithBoundedDefer(IntentRecord intent) throws Exception {
        return retryWhileDeferred(forwarder, intent, DEFER_MAX_ATTEMPTS, DEFER_RETRY_DELAY_MS);
    }

    /**
     * D4 (P3-086/P3-091): the bounded in-run defer retry policy itself.
     *
     * <p>Extracted from {@link #forwardWithBoundedDefer} so the policy can be tested directly
     * without a Fluss connection — the retry is what keeps a {@code DEFERRED} money-moving intent
     * from being dropped until a restart, so it needs coverage of its own (attempts, budget
     * exhaustion, and the throwing-sink path).
     *
     * <p>Returns the first non-{@code DEFERRED} result. A sink that keeps throwing is retried to
     * the same budget and then rethrows; a sink that stays {@code DEFERRED} returns {@code DEFERRED}
     * once the budget is spent, which the caller logs (never the violation channel).
     */
    static IntentSink.Result retryWhileDeferred(IntentSink forwarder, IntentRecord intent,
            int maxAttempts, long retryDelayMs) throws Exception {
        for (int attempt = 1; ; attempt++) {
            try {
                IntentSink.Result result = forwarder.forward(intent);
                if (result != IntentSink.Result.DEFERRED || attempt >= maxAttempts) {
                    return result;
                }
            } catch (Exception e) {
                if (attempt >= maxAttempts) {
                    throw e;
                }
            }
            sleepBeforeRetry(intent, retryDelayMs);
        }
    }

    private static void sleepBeforeRetry(IntentRecord intent, long retryDelayMs) {
        try {
            Thread.sleep(retryDelayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "interrupted while retrying deferred intent " + intent.instructionId(), e);
        }
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
