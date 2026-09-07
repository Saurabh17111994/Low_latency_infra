package com.trading.ingestion.write;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.TickPacketFixtures;
import com.trading.ingestion.model.TickPacket;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * P1-273: a throwing {@link RawTickWriter.OutcomeListener} must be contained —
 * it must never escape into the Fluss completion or scheduler thread.
 *
 * <p>The converter below throws synchronously, so {@code handleCompletion}
 * runs inline on the calling thread and the listener exception would escape
 * {@code write()} without the try-catch in {@code completeOutcome}.
 */
@DisplayName("P1-273: outcome listener exceptions are contained")
class RawTickWriterListenerIsolationTest {

    /** Converter whose append is cancelled synchronously (UNCERTAIN path). */
    static final class SyncCancellingConverter implements FlussRowConverter {
        @Override
        public CompletableFuture<RawTickWriter.AppendResult> append(TickPacket packet) {
            throw new CancellationException("wedged sender");
        }

        @Override
        public int estimatedRowSize(TickPacket packet) {
            return 100;
        }

        @Override
        public void close() {}
    }

    @Test
    @DisplayName("throwing listener does not escape write(), writer stays usable")
    void throwingListenerDoesNotEscape() throws Exception {
        AppendTracker tracker = new AppendTracker();
        RawTickWriter writer = new RawTickWriter(
                new SyncCancellingConverter(), tracker, "default.raw_table_1",
                Duration.ofSeconds(5), Duration.ofSeconds(5));
        CountDownLatch first = new CountDownLatch(1);
        CountDownLatch second = new CountDownLatch(1);
        AtomicInteger deliveries = new AtomicInteger();
        writer.setOutcomeListener(o -> {
            deliveries.incrementAndGet();
            (deliveries.get() == 1 ? first : second).countDown();
            throw new RuntimeException("boom from listener");
        });

        RawTickWriter.AppendOutcome out = assertDoesNotThrow(
                () -> writer.write(TickPacketFixtures.validTrade(0)),
                "listener exception must not escape write()");
        assertEquals(RawTickWriter.Status.ACCEPTED, out.status());
        assertTrue(first.await(5, TimeUnit.SECONDS), "first outcome still delivered");
        assertEquals(0, tracker.pendingRecords(), "reservation released despite listener throw");

        // The writer must stay usable after a listener throw.
        assertDoesNotThrow(() -> writer.write(TickPacketFixtures.validTrade(1)));
        assertTrue(second.await(5, TimeUnit.SECONDS), "second outcome still delivered");
        assertEquals(2, deliveries.get());
        assertEquals(0, tracker.pendingRecords());

        writer.close();
    }
}
