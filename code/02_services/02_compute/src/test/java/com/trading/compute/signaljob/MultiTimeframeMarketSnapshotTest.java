package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Strategy market snapshot on forming rows (2026-10-01): the aggregator
 * captures the 16 raw extras from every accepted tick (trade or quote) and
 * carries the latest-known values on every forming row — NULL when never seen,
 * never a fabricated zero; a later tick that does not carry a field never
 * erases the last known value.
 */
@DisplayName("market snapshot: raw extras ride every forming row (trade + quote capture)")
class MultiTimeframeMarketSnapshotTest {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final long TOKEN = 4242L;
    private static final long LIVE_INTERVAL = 1_000L;

    private KeyedOneInputStreamOperatorTestHarness<Long, RowData, RowData> harness;
    private MultiTimeframeAggregateFunction fn;

    private static long ist(int y, int m, int d, int h, int min, int s, int ms) {
        return ZonedDateTime.of(LocalDate.of(y, m, d), LocalTime.of(h, min, s, ms * 1_000_000), IST)
                .toInstant().toEpochMilli();
    }

    private void openFast() throws Exception {
        openWith(new MultiTimeframeAggregateFunction(LIVE_INTERVAL, false, true, true));
    }

    private void openWith(MultiTimeframeAggregateFunction f) throws Exception {
        fn = f;
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                fn,
                row -> row.getLong(RawTableColumns.INSTRUMENT_TOKEN),
                Types.LONG);
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
            harness = null;
        }
    }

    private static GenericRowData tick(long eventTime, String fp, String tickType, long price, long qty) {
        return TestRawRows.row(TOKEN, eventTime, fp, tickType, price, qty);
    }

    /** Sets all 16 market raw columns to {@code v+1 .. v+16} (order = CandleLiveColumns.MKT_*). */
    private static GenericRowData marketAll(RowData base, long v) {
        GenericRowData r = (GenericRowData) base;
        r.setField(RawTableColumns.OPEN_PAISE, v + 1);
        r.setField(RawTableColumns.HIGH_PAISE, v + 2);
        r.setField(RawTableColumns.LOW_PAISE, v + 3);
        r.setField(RawTableColumns.CLOSE_PAISE, v + 4);
        r.setField(RawTableColumns.VWAP_PAISE, v + 5);
        r.setField(RawTableColumns.TOTAL_BUY_QTY, v + 6);
        r.setField(RawTableColumns.TOTAL_SELL_QTY, v + 7);
        r.setField(RawTableColumns.OPEN_INTEREST, v + 8);
        r.setField(RawTableColumns.OI_DAY_HIGH, v + 9);
        r.setField(RawTableColumns.OI_DAY_LOW, v + 10);
        r.setField(RawTableColumns.BID_PX_1, v + 11);
        r.setField(RawTableColumns.BID_QTY_1, v + 12);
        r.setField(RawTableColumns.ASK_PX_1, v + 13);
        r.setField(RawTableColumns.ASK_QTY_1, v + 14);
        r.setField(RawTableColumns.LOWER_LIMIT_PAISE, v + 15);
        r.setField(RawTableColumns.UPPER_LIMIT_PAISE, v + 16);
        return r;
    }

    private List<RowData> fastRows() {
        List<RowData> out = new ArrayList<>();
        var side = harness.getSideOutput(MultiTimeframeAggregateFunction.LIVE_TICK_TAG);
        if (side != null) {
            for (StreamRecord<RowData> sr : side) {
                out.add(sr.getValue());
            }
        }
        return out;
    }

    private List<RowData> mirrorRows() {
        List<RowData> out = new ArrayList<>();
        var side = harness.getSideOutput(MultiTimeframeAggregateFunction.LIVE_TAG);
        if (side != null) {
            for (StreamRecord<RowData> sr : side) {
                out.add(sr.getValue());
            }
        }
        return out;
    }

    @Test
    @DisplayName("a trade tick carries the captured snapshot on all six forming rows")
    void tradeTickCarriesSnapshotOnAllSixRows() throws Exception {
        openFast();
        long T0 = ist(2026, 9, 4, 10, 0, 0, 0);
        harness.processElement(
                marketAll(tick(T0 + 1_000L, "fp-m1", "TRADE", 100_00L, 10L), 1_000L), T0 + 1_000L);

        List<RowData> rows = fastRows();
        assertEquals(6, rows.size(), "one forming row per TF");
        for (RowData r : rows) {
            assertEquals(1_001L, r.getLong(CandleLiveColumns.MKT_DAY_OPEN_PAISE));
            assertEquals(1_002L, r.getLong(CandleLiveColumns.MKT_DAY_HIGH_PAISE));
            assertEquals(1_003L, r.getLong(CandleLiveColumns.MKT_DAY_LOW_PAISE));
            assertEquals(1_004L, r.getLong(CandleLiveColumns.MKT_PREV_CLOSE_PAISE));
            assertEquals(1_005L, r.getLong(CandleLiveColumns.MKT_VWAP_PAISE));
            assertEquals(1_006L, r.getLong(CandleLiveColumns.MKT_TOTAL_BUY_QTY));
            assertEquals(1_007L, r.getLong(CandleLiveColumns.MKT_TOTAL_SELL_QTY));
            assertEquals(1_008L, r.getLong(CandleLiveColumns.MKT_OPEN_INTEREST));
            assertEquals(1_009L, r.getLong(CandleLiveColumns.MKT_OI_DAY_HIGH));
            assertEquals(1_010L, r.getLong(CandleLiveColumns.MKT_OI_DAY_LOW));
            assertEquals(1_011L, r.getLong(CandleLiveColumns.MKT_BID_PX_1));
            assertEquals(1_012L, r.getLong(CandleLiveColumns.MKT_BID_QTY_1));
            assertEquals(1_013L, r.getLong(CandleLiveColumns.MKT_ASK_PX_1));
            assertEquals(1_014L, r.getLong(CandleLiveColumns.MKT_ASK_QTY_1));
            assertEquals(1_015L, r.getLong(CandleLiveColumns.MKT_LOWER_LIMIT_PAISE));
            assertEquals(1_016L, r.getLong(CandleLiveColumns.MKT_UPPER_LIMIT_PAISE));
        }
    }

    @Test
    @DisplayName("quote ticks update the snapshot without emitting; the next trade carries the fresh values")
    void quoteTickUpdatesSnapshotForTheNextTrade() throws Exception {
        openFast();
        long T0 = ist(2026, 9, 4, 10, 0, 0, 0);
        // Trade first: no market fields yet -> all NULL on the forming row.
        harness.processElement(tick(T0 + 1_000L, "fp-m1", "TRADE", 100_00L, 10L), T0 + 1_000L);
        List<RowData> afterTrade = fastRows();
        assertEquals(6, afterTrade.size());
        assertTrue(afterTrade.get(0).isNullAt(CandleLiveColumns.MKT_BID_PX_1));

        // Quote with depth + day stats: no new forming row...
        harness.processElement(
                marketAll(tick(T0 + 2_000L, "fp-q1", "QUOTE", 100_50L, 0L), 2_000L), T0 + 2_000L);
        assertEquals(6, fastRows().size(), "a quote tick must not emit a forming row");

        // ...but the next trade carries the quote-updated snapshot.
        harness.processElement(tick(T0 + 3_000L, "fp-m2", "TRADE", 101_00L, 5L), T0 + 3_000L);
        List<RowData> rows = fastRows();
        assertEquals(12, rows.size(), "six forming rows for each accepted trade tick");
        RowData latest = rows.get(11); // second trade's FIFTEEN_M row
        assertEquals(2_011L, latest.getLong(CandleLiveColumns.MKT_BID_PX_1));
        assertEquals(2_014L, latest.getLong(CandleLiveColumns.MKT_ASK_QTY_1));
        assertEquals(2_005L, latest.getLong(CandleLiveColumns.MKT_VWAP_PAISE));
    }

    @Test
    @DisplayName("an absent field on a later tick keeps the last known value — never erased, never zero")
    void absentFieldKeepsLastKnownValue() throws Exception {
        openFast();
        long T0 = ist(2026, 9, 4, 10, 0, 0, 0);
        harness.processElement(
                marketAll(tick(T0 + 1_000L, "fp-a", "TRADE", 100_00L, 10L), 1_000L), T0 + 1_000L);

        // Second trade carries only bid_px_1 and vwap; every other field is NULL.
        GenericRowData partial = tick(T0 + 2_000L, "fp-b", "TRADE", 101_00L, 5L);
        partial.setField(RawTableColumns.BID_PX_1, 9_999L);
        partial.setField(RawTableColumns.VWAP_PAISE, 8_888L);
        harness.processElement(partial, T0 + 2_000L);

        RowData r = fastRows().get(11);
        assertEquals(9_999L, r.getLong(CandleLiveColumns.MKT_BID_PX_1), "present field updates");
        assertEquals(8_888L, r.getLong(CandleLiveColumns.MKT_VWAP_PAISE), "present field updates");
        assertEquals(1_002L, r.getLong(CandleLiveColumns.MKT_DAY_HIGH_PAISE), "absent field keeps previous");
        assertEquals(1_014L, r.getLong(CandleLiveColumns.MKT_ASK_QTY_1), "absent field keeps previous");
    }

    @Test
    @DisplayName("fields never seen render NULL — never a fabricated zero")
    void neverSeenFieldsRenderAsNull() throws Exception {
        openFast();
        long T0 = ist(2026, 9, 4, 10, 0, 0, 0);
        harness.processElement(tick(T0 + 1_000L, "fp-a", "TRADE", 100_00L, 10L), T0 + 1_000L);

        RowData r = fastRows().get(0);
        for (int i = CandleLiveColumns.MARKET_SECTION_START; i < CandleLiveColumns.FIELD_COUNT; i++) {
            assertTrue(r.isNullAt(i),
                    "market column " + CandleLiveColumns.COLUMN_NAMES.get(i) + " must be NULL, not zero");
        }
    }

    @Test
    @DisplayName("the 1 s mirror fallback (MULTITF_FAST_LIVE_FEED=false) carries the same snapshot")
    void mirrorRowsCarrySnapshot() throws Exception {
        openWith(new MultiTimeframeAggregateFunction(LIVE_INTERVAL, false, true));
        long T0 = ist(2026, 9, 4, 10, 0, 0, 0);
        harness.processElement(
                marketAll(tick(T0 + 2_000L, "fp-a", "TRADE", 100_00L, 10L), 5_000L), T0 + 2_000L);
        // W3-d: the live-mirror scan is record-driven — advance the processing
        // clock, then any record (a quote here) runs the due scan.
        harness.setProcessingTime(20_000L);
        harness.processElement(tick(T0 + 3_000L, "fp-q", "QUOTE", 100_00L, 0L), T0 + 3_000L);

        List<RowData> mirror = mirrorRows();
        assertFalse(mirror.isEmpty(), "the due mirror must emit forming rows");
        for (RowData r : mirror) {
            assertEquals(5_011L, r.getLong(CandleLiveColumns.MKT_BID_PX_1));
            assertEquals(5_016L, r.getLong(CandleLiveColumns.MKT_UPPER_LIMIT_PAISE));
        }
    }
}
