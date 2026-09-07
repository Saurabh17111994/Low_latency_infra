package com.trading.ingestion.write;

import com.trading.ingestion.model.TickPacket;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounded Fluss append writer for {@code raw_table_1}.
 *
 * <p>Contract:
 * <ul>
 *   <li>Each tick is submitted as its own append call — no application-level
 *       batching ({@code INGESTION_MAX_BATCH_RECORDS} bounds the transport
 *       batch; the Fluss client may coalesce rows into transport batches, so
 *       completion order may differ from submission order; every outcome is
 *       keyed by its row's fingerprint).</li>
 *   <li>Backpressure: before every append, calls {@link AppendTracker#tryAccept(int)}
 *       with the row size estimate; rejects if halted</li>
 *   <li>Every append records receive-time, append-start, append-acknowledgement
 *       time, append outcome, record size, and error class</li>
 *   <li>Pending counters decrease only after append completes (success or fail)</li>
 *   <li>Arrow payloads are never compressed in the ingestion→Fluss path</li>
 *   <li>Raw ingestion does not deduplicate fingerprints; Compute owns logical dedup</li>
 *   <li>Retry with exponential backoff (100, 200, 400 ms; up to {@code MAX_RETRY_ATTEMPTS}) for RETRYABLE
 *       failures; FATAL failures halt immediately</li>
 *   <li>On timeout the outcome is {@code UNCERTAIN} — ingestion cannot prove
 *       whether Fluss persisted the row; Compute owns logical dedup</li>
 *   <li>{@link #write(TickPacket)} is asynchronous: it submits the append and
 *       returns {@code ACCEPTED} without waiting for the ack; the terminal
 *       outcome is delivered on the {@link OutcomeListener} when the Fluss
 *       future completes (success, retried failure, timeout, or fatal).</li>
 * </ul>
 *
 * <p>This class wraps the Fluss client table writer. The concrete Fluss
 * append API is version-gated on Fluss {@code 0.9.1-incubating}.
 */
public final class RawTickWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RawTickWriter.class);

    /** Maximum retry attempts for RETRYABLE failures before giving up. */
    static final int MAX_RETRY_ATTEMPTS = 3;
    /** Initial backoff delay between retries (ms). Doubles each attempt. */
    static final long BASE_RETRY_BACKOFF_MS = 100;

    private final FlussRowConverter rowConverter;
    private final AppendTracker tracker;
    private final Duration appendTimeout;
    private final Duration drainDeadline;
    private final String tableName;
    private final AtomicLong appendCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final AtomicLong uncertainCount = new AtomicLong(0);
    /** Wall-clock epoch (ms) of the last SUCCESS ack. 0 = none yet. Feed for
     *  the zero-ack watchdog: a sustained absence of SUCCESS while the broker
     *  keeps sending indicates a wedged Fluss sender (append futures hang
     *  in-flight, never completing) that no per-append path can observe. */
    private volatile long lastAppendSuccessEpochMs = 0L;
    private volatile boolean closed;

    /** Schedules per-attempt timeouts and retry resubmissions (daemon threads). */
    private final ScheduledExecutorService scheduler;
    private volatile OutcomeListener outcomeListener = OutcomeListener.NOOP;

    /**
     * @param rowConverter  converts {@link TickPacket} → Fluss row
     * @param tracker       shared backpressure tracker
     * @param tableName     for logging/observability
     * @param appendTimeout per-append deadline
     * @param drainDeadline max time to wait for pending writes on shutdown
     */
    public RawTickWriter(FlussRowConverter rowConverter,
                         AppendTracker tracker,
                         String tableName,
                         Duration appendTimeout,
                         Duration drainDeadline) {
        this.rowConverter = rowConverter;
        this.tracker = tracker;
        this.tableName = tableName;
        this.appendTimeout = appendTimeout;
        this.drainDeadline = drainDeadline;
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "raw-writer-async");
            t.setDaemon(true);
            return t;
        });
    }

    /** Receives every terminal append outcome (SUCCESS, UNCERTAIN, FAILED, FATAL). */
    @FunctionalInterface
    public interface OutcomeListener {
        void onOutcome(AppendOutcome outcome);

        OutcomeListener NOOP = o -> {};
    }

    public void setOutcomeListener(OutcomeListener l) {
        this.outcomeListener = l != null ? l : OutcomeListener.NOOP;
    }

    /**
     * Convert a tick packet to a Fluss row, reserve backpressure capacity,
     * and submit the append. Returns immediately with {@code ACCEPTED} once
     * the row is submitted; the terminal outcome is delivered asynchronously
     * via the {@link OutcomeListener}.
     *
     * <p>Delivery is at-least-once: every accepted tick is appended exactly
     * once; ingestion never deduplicates by fingerprint (logical dedup belongs
     * to the Signal Flink job, plan §Executive Summary).
     *
     * <p>Retry: on RETRYABLE failures (per {@link RetryClassifier}) the writer
     * retries up to {@link #MAX_RETRY_ATTEMPTS} times with exponential backoff
     * (100, 200, 400 ms — the delay doubles per attempt).
     * Timeout outcomes are classified {@code UNCERTAIN} — the append may have
     * succeeded at Fluss but the ack was lost.
     *
     * @param packet the decoded+normalized+fingerprinted tick
     * @return ACCEPTED (submitted), or REJECTED/SKIPPED synchronously
     */
    public AppendOutcome write(TickPacket packet) {
        // R-069 (P1-113/118): the entry closed-check can still race close() —
        // by design NO monitor is held across submission (holding it across
        // blocking converter I/O would serialize all writers). Instead every
        // post-close failure mode converges: sync throws route via the R-297
        // path, scheduler rejections via the RejectedExecutionException
        // catches, and a write racing close() surfaces as FAILED/UNCERTAIN —
        // never an escape, never a leaked reservation.
        if (isClosed()) {
            errorCount.incrementAndGet();
            return AppendOutcome.skipped("writer closed");
        }

        // 1. Estimate row size for backpressure
        int rowBytes = rowConverter.estimatedRowSize(packet);

        // 2. Reserve backpressure capacity
        if (!tracker.tryAccept(rowBytes)) {
            return AppendOutcome.rejected(
                    tracker.pendingRecords(), tracker.pendingBytes(),
                    "pending-limit exceeded; halted=" + tracker.isHalted());
        }

        // 3. Record ingestion timestamp
        Instant acceptTime = Instant.now();

        // 4. Submit asynchronously (no per-row blocking on the ack)
        submitAppend(packet, rowBytes, acceptTime, 1);
        return AppendOutcome.accepted(rowBytes, acceptTime);
    }

    /**
     * Submit one append attempt. The Fluss future completes on server ack;
     * completion handling runs on the future's completing thread.
     */
    private void submitAppend(TickPacket packet, int rowBytes, Instant acceptTime, int attempt) {
        final CompletableFuture<AppendResult> future;
        try {
            future = rowConverter.append(packet);
        } catch (Throwable t) {
            // R-297 wedge fix: with a bounded client.writer.buffer.wait-timeout
            // the Fluss client throws SYNCHRONOUSLY (EOFException) when the
            // memory pool stays exhausted — e.g. the sender thread is wedged
            // retrying leaderless tables. The tracker slot reserved in
            // write() must release and the failure must classify/retry
            // exactly like an async failure — never leak the reservation and
            // never let the exception escape to the reader loop.
            handleCompletion(packet, rowBytes, acceptTime, attempt, null, t);
            return;
        }

        // Per-attempt timeout: cancel the in-flight append when the deadline
        // passes — R-037: the tracker release is deferred to the future's
        // actual completion (handleCompletion), never to the timeout itself.
        try {
            scheduler.schedule(() -> {
                if (!future.isDone()) {
                    future.cancel(true);
                }
            }, appendTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.RejectedExecutionException shutdown) {
            // P1-113/118: scheduler is shut down (close() raced submission).
            // Cancel inline — the future's own completion then releases the
            // slot and emits exactly one outcome via the normal path. Never
            // let the rejection escape: the reservation would leak.
            future.cancel(true);
        }

        future.whenComplete((result, ex) ->
                handleCompletion(packet, rowBytes, acceptTime, attempt, result, ex));
    }

    private void handleCompletion(TickPacket packet, int rowBytes, Instant acceptTime,
                                  int attempt, AppendResult result, Throwable ex) {
        if (ex == null) {
            // ---- Success ----
            tracker.onAppendSuccess(rowBytes);
            appendCount.incrementAndGet();
            lastAppendSuccessEpochMs = System.currentTimeMillis();
            completeOutcome(AppendOutcome.success(
                    packet, acceptTime, Instant.now(), rowBytes, result));
            return;
        }

        Throwable cause = ex.getCause() != null ? ex.getCause() : ex;

        // B125: never swallow an interrupt — restore the flag so the
        // scheduler/callback thread observes the stop request. Routing
        // stays via the classifier (interrupts classify FATAL there).
        if (RetryClassifier.isInterruptCaused(cause)) {
            Thread.currentThread().interrupt();
        }

        if (cause instanceof CancellationException) {
            // Timeout → UNCERTAIN: we don't know if Fluss persisted the row.
            // Do NOT retry — the same row could already be durably stored.
            // Compute owns logical dedup at the Flink level.
            // R-037: the release is deferred until this completion — the
            // AppendTracker contract "pending counters decrease only after
            // append completes" is honored exactly here.
            tracker.onAppendFailure(rowBytes);
            errorCount.incrementAndGet();
            uncertainCount.incrementAndGet();
            LOG.warn("raw-writer: append UNCERTAIN (table={}, fp={}, timeout={}ms, attempt={})",
                    tableName, fp12(packet), appendTimeout.toMillis(), attempt);
            completeOutcome(AppendOutcome.uncertain(packet, acceptTime, rowBytes, appendTimeout));
            return;
        }

        RetryClassifier.Classification retry = RetryClassifier.classify(cause);

        if (retry == RetryClassifier.Classification.FATAL) {
            // Fatal → no retry, halt the append path
            tracker.onAppendFailure(rowBytes);
            errorCount.incrementAndGet();
            LOG.error("raw-writer: FATAL append error (table={}, class={})",
                    tableName, cause.getClass().getSimpleName());
            completeOutcome(AppendOutcome.fatal(packet, acceptTime, rowBytes, cause));
            return;
        }

        // RETRYABLE — retry with backoff if attempts remain
        if (attempt < MAX_RETRY_ATTEMPTS) {
            long backoffMs = BASE_RETRY_BACKOFF_MS * (1L << (attempt - 1));
            LOG.warn("raw-writer: append retryable (table={}, attempt={}/{}, backoff={}ms, class={})",
                    tableName, attempt, MAX_RETRY_ATTEMPTS, backoffMs,
                    cause.getClass().getSimpleName());
            try {
                scheduler.schedule(() -> submitAppend(packet, rowBytes, acceptTime, attempt + 1),
                        backoffMs, TimeUnit.MILLISECONDS);
            } catch (java.util.concurrent.RejectedExecutionException shutdown) {
                // P1-113/118 + P1-114(b): scheduler is shut down — no retry
                // can ever run. Release the slot and count the error NOW
                // instead of leaking the reservation with no outcome. (The
                // FAILED outcome itself is gated by completeOutcome: post-
                // shutdown delivery would double-count against forceDrain.)
                // Retrying into a dead scheduler is not an option, so the
                // attempt count is moot.
                tracker.onAppendFailure(rowBytes);
                errorCount.incrementAndGet();
                LOG.warn("raw-writer: retry impossible, scheduler shut down (table={})",
                        tableName);
                completeOutcome(AppendOutcome.failed(packet, acceptTime, rowBytes, shutdown));
            }
            return;
        }

        // Exhausted retries
        tracker.onAppendFailure(rowBytes);
        errorCount.incrementAndGet();
        LOG.warn("raw-writer: append failed after {} attempts (table={})",
                MAX_RETRY_ATTEMPTS, tableName, cause);
        completeOutcome(AppendOutcome.failed(packet, acceptTime, rowBytes, cause));
    }

    private void completeOutcome(AppendOutcome outcome) {
        // P1-114: late completions landing after scheduler shutdown already
        // had their slots forgiven by forceDrain — deliver only while the
        // scheduler is alive, so a post-shutdown outcome can neither
        // double-count nor resurrect a drained slot.
        if (!scheduler.isShutdown()) {
            // P1-273: contain listener exceptions — external listener code
            // must never escape into Fluss completion or scheduler threads.
            try {
                outcomeListener.onOutcome(outcome);
            } catch (Throwable t) {
                LOG.warn("raw-writer: outcome listener threw (table={}): {}",
                        tableName, t.toString());
            }
        }
    }

    private static String fp12(TickPacket packet) {
        String fp = packet.eventFingerprint();
        return fp != null ? fp.substring(0, Math.min(12, fp.length())) : "null";
    }

    public long appendCount() { return appendCount.get(); }
    public long errorCount() { return errorCount.get(); }
    public long uncertainCount() { return uncertainCount.get(); }
    public long lastAppendSuccessEpochMs() { return lastAppendSuccessEpochMs; }

    /**
     * Wait for pending appends to complete, up to the drain deadline.
     * Blocks the calling thread until {@code tracker.pendingRecords()} hits
     * zero or the deadline elapses. On expiry the remainder is forgiven via
     * {@link AppendTracker#forceDrain} (P1-115: never synthesize a bulk
     * release through the per-record API — it frees 1 record against N
     * bytes and double-releases racing completions). Forgiven slots are
     * zeroed once; late completions release against the floor and their
     * outcomes are gated (see {@link #completeOutcome}).
     */
    public void drain() {
        drain(drainDeadline);
    }

    /**
     * Drain with an explicit budget (B130: the worker close passes its
     * remaining single-budget share instead of a second full deadline).
     */
    public void drain(java.time.Duration budget) {
        long deadlineNanos = System.nanoTime() + budget.toNanos();
        long pendingAtStart = tracker.pendingRecords();

        while (tracker.pendingRecords() > 0
                && System.nanoTime() < deadlineNanos) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        long remaining = tracker.pendingRecords();
        if (remaining > 0) {
            long[] forgiven = tracker.forceDrain();
            LOG.error("raw-writer: drain incomplete — forgave {} records / {} bytes "
                    + "(deadline={}, started={}); late completions release "
                    + "against the floor, no terminal outcome is emitted for "
                    + "forgiven slots",
                    forgiven[0], forgiven[1], budget, pendingAtStart);
        }
    }

    /**
     * Drain pending writes and close. Waits up to {@code drainDeadline}
     * for pending records to reach zero before force-closing the connection.
     */
    @Override
    public void close() {
        close(drainDeadline);
    }

    /**
     * Close with an explicit total budget for the drain phase (B130).
     * A zero budget force-drains immediately — every forgiven slot is
     * counted FAILED by the tracker, never silent.
     */
    public void close(java.time.Duration maxWait) {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        long pendingAtStart = tracker.pendingRecords();

        // Drain: wait for pending appends to complete (retry resubmissions
        // run on the scheduler while we wait — it is only shut down after).
        drain(maxWait);

        // No new retries/timeouts can matter now — every in-flight append has
        // completed (tracker zero) and no more writes can be accepted.
        // P1-114(a): shutdownNow DISCARDS queued backoff/timeout tasks whose
        // packets are captured in the Runnables and cannot be named — log the
        // count (fail-loud) since no per-row outcome can be emitted for them.
        java.util.List<Runnable> discarded = scheduler.shutdownNow();
        if (!discarded.isEmpty()) {
            LOG.error("raw-writer: shutdown discarded {} queued tasks "
                    + "(table={}); their slots were forgiven by forceDrain",
                    discarded.size(), tableName);
        }

        // R-068: the FlussRowConverter owns the underlying Fluss Connection;
        // RawTickWriter.close() must close it or the connection leaks (and
        // the JVM may hang on shutdown).
        try {
            rowConverter.close();
        } catch (Exception e) {
            LOG.warn("raw-writer: rowConverter.close() failed: {}", e.getMessage());
        }

        LOG.info("raw-writer: closed (table={}, appended={}, errors={}, "
                + "uncertain={}, "
                + "pending_at_start={}, pending_remaining={})",
                tableName, appendCount.get(), errorCount.get(),
                uncertainCount.get(),
                pendingAtStart, tracker.pendingRecords());
    }

    /**
     * R-069: single serialized closed check — the append path and close()
     * contend on the same monitor so no append can slip past a concurrent close.
     */
    private boolean isClosed() {
        synchronized (this) {
            return closed;
        }
    }

    // ---- outcome type ----

    public enum Status { SUCCESS, UNCERTAIN, FAILED, FATAL, REJECTED, SKIPPED, ACCEPTED }

    public record AppendOutcome(
            Status status,
            Instant eventTime,
            Instant acceptTime,
            Instant ackTime,
            int rowBytes,
            String detail,
            long pendingRecords,
            long pendingBytes,
            String fingerprint,
            long instrumentToken,
            String exchange,
            String tradingSymbol
    ) {
        static AppendOutcome success(TickPacket packet, Instant acceptTime,
                                     Instant ackTime, int rowBytes, AppendResult result) {
            return new AppendOutcome(Status.SUCCESS, packet.eventTime(), acceptTime, ackTime,
                    rowBytes, result.toString(), -1, -1,
                    packet.eventFingerprint(), packet.instrumentToken(),
                    packet.exchange(), packet.tradingSymbol());
        }

        /** Append timed out — Fluss may have persisted the row. */
        // P1-110/116: thread packet identity like success() — downstream
        // correlates UNCERTAIN/FAILED/FATAL back to the row by fingerprint.
        static AppendOutcome uncertain(TickPacket packet, Instant acceptTime,
                                       int rowBytes, Duration timeout) {
            return new AppendOutcome(Status.UNCERTAIN, packet.eventTime(), acceptTime, null, rowBytes,
                    "uncertain after " + timeout.toMillis() + "ms timeout; may be a duplicate",
                    -1, -1, packet.eventFingerprint(), packet.instrumentToken(),
                    packet.exchange(), packet.tradingSymbol());
        }

        // P1-111/117: same identity threading for FATAL ...
        static AppendOutcome fatal(TickPacket packet, Instant acceptTime,
                                   int rowBytes, Throwable e) {
            String msg = e.getMessage();
            if (msg == null) msg = e.getClass().getSimpleName();
            return new AppendOutcome(Status.FATAL, packet.eventTime(), acceptTime, null, rowBytes,
                    "FATAL: " + msg, -1, -1, packet.eventFingerprint(),
                    packet.instrumentToken(), packet.exchange(), packet.tradingSymbol());
        }

        // P1-112/117: ... and FAILED (retries-exhausted stays correlatable).
        static AppendOutcome failed(TickPacket packet, Instant acceptTime,
                                    int rowBytes, Throwable e) {
            String msg = e.getMessage();
            if (msg == null) msg = e.getClass().getSimpleName();
            return new AppendOutcome(Status.FAILED, packet.eventTime(), acceptTime, null, rowBytes,
                    msg, -1, -1, packet.eventFingerprint(),
                    packet.instrumentToken(), packet.exchange(), packet.tradingSymbol());
        }

        static AppendOutcome rejected(long pendingRecs, long pendingBytes, String detail) {
            return new AppendOutcome(Status.REJECTED, null, null, null, 0,
                    detail, pendingRecs, pendingBytes, null, 0L, null, null);
        }

        static AppendOutcome skipped(String detail) {
            return new AppendOutcome(Status.SKIPPED, null, null, null, 0, detail, -1, -1,
                    null, 0L, null, null);
        }

        /** Submitted to Fluss; terminal outcome arrives via the OutcomeListener. */
        static AppendOutcome accepted(int rowBytes, Instant acceptTime) {
            return new AppendOutcome(Status.ACCEPTED, null, acceptTime, null, rowBytes,
                    "submitted; awaiting ack", -1, -1, null, 0L, null, null);
        }
    }

    /** Minimal result from Fluss append acknowledgement. */
    public record AppendResult(long offset, String partition) {
        @Override
        public String toString() {
            return "offset=" + offset + ", partition=" + partition;
        }
    }
}
