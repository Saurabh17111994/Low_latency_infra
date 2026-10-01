package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.model.TickPacket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.fluss.client.table.writer.AppendResult;
import org.apache.fluss.client.table.writer.TypedAppendWriter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Guards for P1-061 (ack_ts alignment), P1-074 (cancellation identity), P1-075 (close flush). */
@DisplayName("TypedFlussRowConverter: identity, alignment, lifecycle")
class TypedFlussRowConverterTest {

    /** Fake typed writer: captures rows, plays a scripted future, records flush. */
    static final class FakeTypedWriter implements TypedAppendWriter<TypedFlussRowConverter.TickRow> {
        final List<TypedFlussRowConverter.TickRow> rows = new ArrayList<>();
        volatile CompletableFuture<AppendResult> next =
                CompletableFuture.completedFuture(null);
        volatile boolean flushed;
        @Override
        public CompletableFuture<AppendResult> append(TypedFlussRowConverter.TickRow row) {
            rows.add(row);
            return next;
        }
        @Override public void flush() { flushed = true; }
    }

    /** Fake connection: records close, everything else unsupported. */
    static class FakeConnection implements org.apache.fluss.client.Connection {
        volatile boolean closed;
        @Override public org.apache.fluss.config.Configuration getConfiguration() {
            throw new UnsupportedOperationException();
        }
        @Override public org.apache.fluss.client.admin.Admin getAdmin() {
            throw new UnsupportedOperationException();
        }
        @Override public org.apache.fluss.client.table.Table getTable(
                org.apache.fluss.metadata.TablePath tablePath) {
            throw new UnsupportedOperationException();
        }
        // Fluss 1.0 added MultiTable to the Connection interface; this fake never
        // exercises the multi-table path, so it stays unimplemented like its siblings.
        @Override public org.apache.fluss.client.table.MultiTable getMultiTable() {
            throw new UnsupportedOperationException();
        }
        @Override public void close() { closed = true; }
    }

    private static TickPacket trade() {
        return TickPacketFixtures.validTrade(100_000);
    }

    @Test
    @DisplayName("P1-061: ack_ts is 0 (unknown), aligned with the generic path")
    void ackTsAlignedToZero() throws Exception {
        FakeTypedWriter writer = new FakeTypedWriter();
        TypedFlussRowConverter converter =
                new TypedFlussRowConverter(writer, new FakeConnection(), "default.raw_table_1");
        converter.append(trade()).get();
        assertEquals(1, writer.rows.size());
        assertEquals(0L, writer.rows.get(0).ack_ts, "ack_ts must be 0 = unknown (R-010), not null");
    }

    @Test
    @DisplayName("CHG-488: payload_hash is verified in memory, never persisted")
    void payloadHashIsNotPersisted() throws Exception {
        FakeTypedWriter writer = new FakeTypedWriter();
        TypedFlussRowConverter converter =
                new TypedFlussRowConverter(writer, new FakeConnection(), "default.raw_table_1");
        converter.append(trade()).get();
        assertEquals("", writer.rows.get(0).payload_hash,
                "the stored payload_hash must stay empty: the SHA-256 is checked at admission and "
                        + "is recomputable from raw_payload, and no consumer ever read the stored "
                        + "text (CHG-488 audit, 2026-10-01)");
    }

    @Test
    @DisplayName("v4: tick_type needs a positive volume_delta, not just TRADE validity")
    void tickTypeFollowsVolumeDeltaNotValidity() throws Exception {
        FakeTypedWriter writer = new FakeTypedWriter();
        TypedFlussRowConverter converter =
                new TypedFlussRowConverter(writer, new FakeConnection(), "default.raw_table_1");
        converter.append(TickPacketFixtures.validTrade(1)).get();
        converter.append(TickPacketFixtures.validTradeWithQty(2, 25L, 100L, 0L)).get();
        assertEquals("TRADE", writer.rows.get(0).tick_type,
                "a trade tick that moved quantity is a TRADE");
        assertEquals("QUOTE", writer.rows.get(1).tick_type,
                "VALID_TRADE validity with a zero delta must NOT be TRADE: the v4 live read-back "
                        + "found 4 of 4 TRADE rows carrying volume_delta=0, which is exactly what made "
                        + "`WHERE tick_type = 'TRADE'` over-count by ~6x");
        assertEquals(0L, (long) writer.rows.get(1).volume_delta,
                "the delta itself is still stored as given — only the label is honest now");
    }

    @Test
    @DisplayName("P1-074: CancellationException survives without a RuntimeException wrap")
    void cancellationIdentityPreserved() {
        FakeTypedWriter writer = new FakeTypedWriter();
        writer.next = CompletableFuture.failedFuture(new CancellationException("wedged sender"));
        TypedFlussRowConverter converter =
                new TypedFlussRowConverter(writer, new FakeConnection(), "default.raw_table_1");
        CompletableFuture<?> future = converter.append(trade());
        ExecutionException thrown = null;
        try {
            future.get();
        } catch (ExecutionException e) {
            thrown = e;
        } catch (Exception e) {
            throw new AssertionError("unexpected checked failure: " + e);
        }
        assertTrue(thrown != null, "append future must fail");
        assertInstanceOf(CancellationException.class, thrown.getCause(),
                "cancellation identity must survive (handleCompletion unwraps one level to UNCERTAIN)");
    }

    @Test
    @DisplayName("P1-075: close flushes the writer, then the connection, exactly once")
    void closeFlushesWriterThenConnectionOnce() {
        List<String> order = new ArrayList<>();
        TypedAppendWriter<TypedFlussRowConverter.TickRow> orderWriter =
                new TypedAppendWriter<>() {
                    @Override public CompletableFuture<AppendResult> append(
                            TypedFlussRowConverter.TickRow row) {
                        return CompletableFuture.completedFuture(null);
                    }
                    @Override public void flush() { order.add("flush"); }
                };
        TypedFlussRowConverter converter =
                new TypedFlussRowConverter(orderWriter, new FakeConnection() {
                    @Override public org.apache.fluss.config.Configuration getConfiguration() {
                        throw new UnsupportedOperationException();
                    }
                    @Override public org.apache.fluss.client.admin.Admin getAdmin() {
                        throw new UnsupportedOperationException();
                    }
                    @Override public org.apache.fluss.client.table.Table getTable(
                            org.apache.fluss.metadata.TablePath tablePath) {
                        throw new UnsupportedOperationException();
                    }
                    @Override public void close() { order.add("connection"); }
                }, "default.raw_table_1");
        converter.close();
        converter.close(); // idempotent
        assertEquals(List.of("flush", "connection"), order,
                "flush before connection, exactly once");
    }

    @Test
    @DisplayName("P0: last_qty carries LTQ, never cumulative volume")
    void lastQtyIsLtqNotVolume() throws Exception {
        FakeTypedWriter writer = new FakeTypedWriter();
        TypedFlussRowConverter converter =
                new TypedFlussRowConverter(writer, new FakeConnection(), "default.raw_table_1");
        // LTQ 62 vs cumulative volume 9,000,000 - the shape a real tick has.
        converter.append(TickPacketFixtures.validTradeWithQty(1, 62L, 9_000_000L)).get();
        assertEquals(62L, writer.rows.get(0).last_qty,
                "last_qty must be LTQ: filling it from volume made "
                        + "CandleAggregateFunction:102 sum cumulative volume into candle volume");
    }
}
