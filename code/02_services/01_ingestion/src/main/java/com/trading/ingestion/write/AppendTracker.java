package com.trading.ingestion.write;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded pending-append counters — records and bytes.
 *
 * <p>Thread-safe. Enforces the backpressure contract from
 * {@code docs/04_contracts/01-ingestion.md} and streaming-3000 plan T2:
 *
 * <ul>
 *   <li>Max pending records: 150,000 default (tunable 50k/64M for 1k → 150k/192M for 3k)
 *       via {@code MAX_PENDING_APPEND_RECORDS} / alias {@code PENDING_MAX_RECORDS}</li>
 *   <li>Max pending bytes: 192 MiB default (tunable) via
 *       {@code MAX_PENDING_APPEND_BYTES} / alias {@code PENDING_MAX_BYTES}</li>
 *   <li>80% warning (tunable via {@code PENDING_APPEND_WARNING_PERCENT} /
 *       alias {@code PENDING_WARNING_PERCENT}) → readiness false, warning event emitted</li>
 *   <li>100% → stop accepting broker data, readiness false, critical event,
 *       preserved acknowledged-loss record; never silently discard</li>
 *   <li>Pending counters decrease only after append completes</li>
 *   <li>Halt is fail-closed until process restart (ING-FAIL-005)</li>
 * </ul>
 *
 * <p>Thresholds are env-driven: 80% warn / 100% halt. Defaults raised for 3k
 * scale (streaming-3000 G2 Ingest T2). Keep halt logic — never drop silently.
 *
 * <p>Events are delivered through a pluggable {@link BackpressureListener}
 * so the tracker remains independent of logging/telemetry.
 */
public final class AppendTracker {

    // T2 tunable backpressure (G2 Ingest) — streaming-3000: 50k/64M → 150k/192M
    public static final long MAX_PENDING_RECORDS = 150_000L;
    static final long MAX_PENDING_BYTES = 201_326_592L; // 192 MiB
    static final double WARNING_PERCENT = 0.80;

    private final AtomicLong pendingRecords = new AtomicLong(0);
    private final AtomicLong pendingBytes = new AtomicLong(0);

    private final AtomicLong totalAccepted = new AtomicLong(0);
    private final AtomicLong totalAppended = new AtomicLong(0);
    private final AtomicLong totalFailed = new AtomicLong(0);
    private final AtomicLong totalRejected = new AtomicLong(0);
    private final AtomicLong totalBytesAccepted = new AtomicLong(0);

    private volatile boolean halted;
    private volatile Instant lastWarningAt;
    private final AtomicLong warningCount = new AtomicLong(0);
    private final long maxPendingRecords;
    private final long maxPendingBytes;
    private final double warningPercent;

    private volatile BackpressureListener listener = BackpressureListener.NOOP;

    public AppendTracker() {
        this(MAX_PENDING_RECORDS, MAX_PENDING_BYTES, WARNING_PERCENT);
    }

    public AppendTracker(long maxPendingRecords, long maxPendingBytes, double warningPercent) {
        if (maxPendingRecords <= 0 || maxPendingBytes <= 0 || warningPercent <= 0 || warningPercent >= 1) {
            throw new IllegalArgumentException("invalid pending limits");
        }
        this.maxPendingRecords = maxPendingRecords;
        this.maxPendingBytes = maxPendingBytes;
        this.warningPercent = warningPercent;
    }

    // ---- listener ----

    @FunctionalInterface
    public interface BackpressureListener {
        void onEvent(Level level, long pendingRecords, long pendingBytes,
                     long maxRecords, long maxBytes, Instant now);
        enum Level { WARNING, CRITICAL }
        BackpressureListener NOOP = (l, pr, pb, mr, mb, n) -> {};
    }

    public void setListener(BackpressureListener l) {
        this.listener = l != null ? l : BackpressureListener.NOOP;
    }

    // ---- accept gate ----

    /**
     * Reserve capacity for one record. Returns {@code true} if the record
     * can be accepted; {@code false} if it would exceed a limit (halted).
     * Caller MUST NOT submit the record on false — it was not counted.
     */
    public boolean tryAccept(int recordBytes) {
        // P1-262 snapshot slots — assigned under the lock, fired after it.
        BackpressureListener.Level emitLevel = null;
        long emitRecs = 0;
        long emitBytes = 0;
        Instant emitAt = null;
        BackpressureListener emitListener = null;
        boolean emit = false;
        // P1-101: fail BEFORE counting — a non-positive size is a programming
        // bug (all estimators floor >= 256), never a capacity signal. Throwing
        // (not reject-false) so a broken converter cannot disguise itself as
        // backpressure; negative input would otherwise corrupt the halt check.
        if (recordBytes <= 0) {
            throw new IllegalArgumentException(
                    "recordBytes must be > 0, got: " + recordBytes);
        }
        // R-195: check-then-act on `halted` was racy — a thread could pass the
        // check and then increment counters after a concurrent halt. Serialize
        // the accept gate with the halt transition.
        synchronized (this) {
            if (halted) {
                totalRejected.incrementAndGet();
                return false;
            }

            long recs = pendingRecords.incrementAndGet();
            long byt = pendingBytes.addAndGet(recordBytes);

            // 100% halt — immediate, no negotiation
            if (recs > maxPendingRecords || byt > maxPendingBytes) {
                halted = true;
                // P1-263: report the ROLLED-BACK counts, not the pre-rollback
                // overflow (recs/byt total max+1 at trip time) — the alert
                // payload must equal what the tracker actually holds.
                long afterRecs = pendingRecords.decrementAndGet();
                long afterBytes = pendingBytes.addAndGet(-recordBytes);
                totalRejected.incrementAndGet();
                // P1-262: snapshot, never call out under the accept-gate lock —
                // a slow/throwing listener must not stall tryAccept at 50k/s.
                // Micros-only (Instant.now().truncatedTo(MICROS), never nanos);
                // the gate stays counter-compare, time is payload-only.
                emitLevel = BackpressureListener.Level.CRITICAL;
                emitRecs = afterRecs;
                emitBytes = afterBytes;
                emitAt = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MICROS);
                emit = true;
            } else {

            // R-196: counters and totals must be consistent BEFORE the
            // warning listener fires — it reads pending + totals.
            totalAccepted.incrementAndGet();
            totalBytesAccepted.addAndGet(recordBytes);

            // 80% warning — still accept, but flag readiness
            double recPct = (double) recs / maxPendingRecords;
            double bytPct = (double) byt / maxPendingBytes;
            if (recPct >= warningPercent || bytPct >= warningPercent) {
                warningCount.incrementAndGet();
                // P1-262: micros-only, once, inside the lock; the listener
                // ref + lastWarningAt update stay under the lock, the CALL
                // goes outside (see below).
                Instant now = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MICROS);
                // throttle: emit warning at most once per 30 s
                if (lastWarningAt == null || lastWarningAt.plusSeconds(30).isBefore(now)) {
                    lastWarningAt = now;
                    emitLevel = BackpressureListener.Level.WARNING;
                    emitRecs = recs;
                    emitBytes = byt;
                    emitAt = now;
                    emit = true;
                }
            }
            } // close the P1-262 else opened at the halt site
            // ---- P1-262: fire outside the accept-gate lock ----
            // Snapshot the listener ref under the lock (setListener can swap
            // it concurrently); a throwing listener must not break the
            // tryAccept true/false contract — caught and logged, never thrown.
            if (emit) {
                emitListener = listener;
            }
        } // end synchronized
        if (emit) {
            try {
                emitListener.onEvent(emitLevel, emitRecs, emitBytes,
                        maxPendingRecords, maxPendingBytes, emitAt);
            } catch (Throwable th) {
                org.slf4j.LoggerFactory.getLogger(AppendTracker.class)
                        .warn("append-tracker: backpressure listener threw ({}), ignored",
                                th.toString());
            }
            if (emitLevel == BackpressureListener.Level.CRITICAL) {
                return false;
            }
        }
        return true;
    }

    /** MUST be called once per accepted record after Fluss acknowledges the append. */
    public void onAppendSuccess(int recordBytes) {
        // P1-115: floored — a late completion racing forceDrain must not
        // drive the gauges negative (negative pending = over-admission).
        pendingRecords.updateAndGet(v -> Math.max(0, v - 1));
        pendingBytes.updateAndGet(v -> Math.max(0, v - recordBytes));
        totalAppended.incrementAndGet();
    }

    /** MUST be called once per accepted record when Fluss append fails. */
    public void onAppendFailure(int recordBytes) {
        // P1-115: same floor as success — see above.
        pendingRecords.updateAndGet(v -> Math.max(0, v - 1));
        pendingBytes.updateAndGet(v -> Math.max(0, v - recordBytes));
        totalFailed.incrementAndGet();
    }

    /**
     * P1-115: forgive ALL pending slots at once (shutdown drain-expiry).
     * Zeroes both gauges — floored, never negative — and returns what was
     * forgiven {@code [records, bytes]} for the log. Forgiven records count
     * as FAILED (R-260: they never acked — the journal already pinned them).
     * This is the ONLY bulk path: never synthesize it through the per-record
     * API (that frees 1 record against N bytes and double-releases racing
     * completions).
     */
    public long[] forceDrain() {
        long r = Math.max(0, pendingRecords.getAndSet(0));
        long b = Math.max(0, pendingBytes.getAndSet(0));
        totalFailed.addAndGet(r);
        return new long[]{r, b};
    }

    // ---- health ----

    public boolean isHalted() { return halted; }

    public long pendingRecords() { return pendingRecords.get(); }
    public long pendingBytes() { return pendingBytes.get(); }
    /** The configured max pending records (R-109: consumers must use this, not a static). */
    public long maxPendingRecords() { return maxPendingRecords; }
    public long maxPendingBytes() { return maxPendingBytes; }
    public long totalAccepted() { return totalAccepted.get(); }
    public long totalAppended() { return totalAppended.get(); }
    public long totalFailed() { return totalFailed.get(); }
    public long totalRejected() { return totalRejected.get(); }
    public long totalBytesAccepted() { return totalBytesAccepted.get(); }
    public long warningCount() { return warningCount.get(); }

    /**
     * Readiness: not-halted AND below warning threshold on both axes.
     * Per dossier: readiness false at ≥80% of either pending limit.
     */
    public boolean isReady() {
        if (halted) return false;
        double recPct = (double) pendingRecords.get() / maxPendingRecords;
        double bytPct = (double) pendingBytes.get() / maxPendingBytes;
        return recPct < warningPercent && bytPct < warningPercent;
    }
}
