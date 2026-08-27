package com.trading.ingestion.discontinuity;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
    private long gapCount;

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
        Long prev = lastSeqByConn.put(connectionKey, seq);
        if (prev == null) {
            return false; // first tick for this connection/epoch
        }
        if (seq > prev + 1) {
            gapCount++;
            return true;
        }
        return false;
    }

    /** Total gaps detected (monotonic; for metrics + evidence). */
    public long gapCount() {
        return gapCount;
    }
}
