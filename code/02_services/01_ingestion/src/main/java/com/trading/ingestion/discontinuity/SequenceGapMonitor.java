package com.trading.ingestion.discontinuity;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * T7-F11: per-connection sequence-gap detection (contract §7.8 F11).
 *
 * <p>Each bridge connection carries a per-slot monotonic
 * {@code feed_sequence_local} (Go batcher, reset per epoch, R-185). A gap
 * (jump &gt; 1) means ticks were lost between this connection and the Java
 * side — the bridge crashed, the pipe dropped frames, or the broker skipped
 * packets. The contract requires: detect per-connection, emit a metric
 * increment + discontinuity evidence row, and NEVER halt the data path.
 *
 * <p>Semantics:
 * <ul>
 *   <li>First tick for a connection: no baseline — no gap (can't know).</li>
 *   <li>Subsequent ticks: gap if {@code seq > last + 1}. Out-of-order
 *       ({@code seq <= last}) is a duplicate/late delivery, not a gap.</li>
 *   <li>Epoch bump (new connection) resets the baseline (sequences restart
 *       at 1 — a restart after reconnect is NOT a gap).</li>
 *   <li>Non-positive or zero seq is ignored (no baseline possible).</li>
 * </ul>
 *
 * <p>Thread-safety: {@link ConcurrentHashMap} — called from the bridge reader
 * thread (NDJSON + proto paths may alternate after a restart).
 */
public final class SequenceGapMonitor {

    /** Last-seen sequence per connection key {@code slotId + "/" + epoch}. */
    private final ConcurrentMap<String, Long> lastSeqByConn = new ConcurrentHashMap<>();
    // P1-078: LongAdder, not long/AtomicLong — onTick runs on the hot bridge
    // reader path; LongAdder spreads uncontended increments across cells
    // (Batch-3 #26 ruling; finding proposed AtomicLong).
    private final LongAdder gapCount = new LongAdder();

    /**
     * Record one tick's sequence for a connection.
     *
     * @param connectionKey {@code slotId + "/" + connectionEpoch} (new epoch =
     *                      new key = fresh baseline)
     * @param seq           the tick's feed_sequence_local
     * @return {@code true} if this tick reveals a sequence gap (evidence row
     *         + metric increment are the caller's job)
     */
    public boolean onTick(String connectionKey, long seq) {
        if (seq <= 0) {
            return false; // no baseline possible
        }
        // P1-079/P1-080: decide gap + advance high-water inside one
        // compute() — never regress the baseline on duplicate/late ticks
        // (1,2,1 then 3 is NOT a gap), and the check-then-act is atomic per
        // key (concurrent onTick for one connection can't double-count).
        boolean[] gap = new boolean[1];
        lastSeqByConn.compute(connectionKey, (k, prev) -> {
            if (prev == null) {
                return seq; // first tick for this connection/epoch
            }
            if (seq <= prev) {
                return prev; // duplicate/late delivery, not a gap
            }
            if (seq > prev + 1) {
                gap[0] = true;
            }
            return seq;
        });
        if (gap[0]) {
            gapCount.increment();
            return true;
        }
        return false;
    }

    /** Total gaps detected (monotonic; for metrics + evidence). */
    public long gapCount() {
        return gapCount.sum();
    }
}
