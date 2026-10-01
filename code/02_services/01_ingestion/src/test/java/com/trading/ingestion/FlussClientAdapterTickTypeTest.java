package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.common.schema.RawTableSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.fluss.client.table.writer.AppendResult;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.row.InternalRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The stored {@code tick_type} must agree with the stored {@code volume_delta}: both come from
 * one read of {@code packet.volumeDelta()} in {@link RealFlussRowConverter#append}. This is the
 * path a live read-back caught on 2026-09-24 — 9 of 12 rows were labelled TRADE while carrying
 * volume_delta=0, because the label then came from validity alone.
 */
@DisplayName("RealFlussRowConverter: tick_type follows volume_delta, not validity")
class FlussClientAdapterTickTypeTest {

    /** Fake append writer: captures the rows the converter builds. */
    static final class FakeAppendWriter implements AppendWriter {
        final List<InternalRow> rows = new ArrayList<>();

        @Override
        public CompletableFuture<AppendResult> append(InternalRow row) {
            rows.add(row);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void flush() {
            // nothing to flush in a fake
        }
    }

    private static int col(String name) {
        int index = RawTableSchema.COLUMNS.indexOf(name);
        if (index < 0) {
            throw new IllegalStateException("no such column in the contract: " + name);
        }
        return index;
    }

    @Test
    void tickTypeFollowsVolumeDeltaNotValidity() throws Exception {
        FakeAppendWriter writer = new FakeAppendWriter();
        RealFlussRowConverter converter =
                new RealFlussRowConverter(writer, null, "default.raw_table_1");

        converter.append(TickPacketFixtures.validTrade(1)).get();                       // delta = qty = 25
        converter.append(TickPacketFixtures.validTradeWithQty(2, 25L, 100L, 0L)).get(); // nothing traded

        assertEquals(2, writer.rows.size(), "both ticks must be built into rows");
        int tickType = col("tick_type");
        int volumeDelta = col("volume_delta");
        assertEquals("TRADE", writer.rows.get(0).getString(tickType).toString(),
                "a trade tick that moved quantity is a TRADE");
        assertEquals("QUOTE", writer.rows.get(1).getString(tickType).toString(),
                "VALID_TRADE validity with volume_delta=0 must not be labelled TRADE -- that is the "
                        + "live 2026-09-24 defect, and it is what made WHERE tick_type='TRADE' over-count");
        assertEquals(0L, writer.rows.get(1).getLong(volumeDelta),
                "the delta itself is stored as given; only the label became honest");
        assertEquals(25L, writer.rows.get(0).getLong(volumeDelta),
                "the trade tick stores its own quantity as the delta");
    }

    @Test
    @DisplayName("CHG-488: payload_hash is verified in memory, never persisted")
    void payloadHashIsNotPersisted() throws Exception {
        FakeAppendWriter writer = new FakeAppendWriter();
        RealFlussRowConverter converter =
                new RealFlussRowConverter(writer, null, "default.raw_table_1");
        converter.append(TickPacketFixtures.validTrade(1)).get();
        assertEquals("", writer.rows.get(0).getString(col("payload_hash")).toString(),
                "the stored payload_hash must stay empty: the SHA-256 is checked at admission and "
                        + "is recomputable from raw_payload, and no consumer ever read the stored "
                        + "text (CHG-488 audit, 2026-10-01)");
    }
}
