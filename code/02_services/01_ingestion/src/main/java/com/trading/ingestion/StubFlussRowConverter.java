package com.trading.ingestion;

import com.trading.ingestion.model.TickPacket;
import com.trading.ingestion.write.FlussRowConverter;
import com.trading.ingestion.write.RawTickWriter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evidence-gated stub implementation of {@link FlussRowConverter}.
 *
 * <p>Records the intent of each append without performing real Fluss I/O.
 * Replaced with a production implementation once Fluss capability evidence
 * is available.
 *
 * <p>Contract: no compression on Arrow payloads in the ingestion-to-Fluss path.
 */
final class StubFlussRowConverter implements FlussRowConverter {

    private static final Logger LOG = LoggerFactory.getLogger(StubFlussRowConverter.class);

    private final String tableName;
    private final AtomicLong counter = new AtomicLong(0);
    private volatile boolean closed;

    StubFlussRowConverter(String tableName) {
        this.tableName = tableName;
        LOG.warn("fluss-stub: using stub converter (table={}) — NO REAL DATA IS PERSISTED", tableName);
        LOG.warn("fluss-stub: replace with production adapter after Fluss capability evidence passes");
    }

    @Override
    public int estimatedRowSize(TickPacket packet) {
        int est = 512 + (packet.raw() != null ? packet.raw().rawPayloadLength() : 0); // P1-087
        return est;
    }

    @Override
    public synchronized CompletableFuture<RawTickWriter.AppendResult> append(TickPacket packet) {
        // R-113: the closed flag must have an effect — appends after close()
        // fail instead of silently acking.
        // P1-234: synchronized with close() on the same monitor so no
        // append can slip between the closed-check and the ack.
        if (closed) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("StubFlussRowConverter is closed"));
        }
        long offset = counter.incrementAndGet();
        if (LOG.isDebugEnabled()) {
            LOG.debug("fluss-stub: append #{} ({} bytes, table={})", offset,
                    estimatedRowSize(packet), tableName);
        }
        return CompletableFuture.completedFuture(
                new RawTickWriter.AppendResult(offset, tableName));
    }

    @Override
    public synchronized void close() {
        closed = true;
    }
}
