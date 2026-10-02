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
 * Strategy market snapshot on forming rows (2026-10-01 native design): the
 * aggregator captures all 49 raw market values — 12 stats + the 30-column
 * depth ladder + 7 extra feed-provided values (CHG-516) — from every accepted
 * tick (trade or quote), tracks two change clocks, and carries the section on
 * the canonical FIFTEEN_S row only (one row per tick, not six). NULL means
 * "never seen", never a fabricated zero; a later tick that does not carry a
 * field never erases the last known value.
 */
@DisplayName("market snapshot: 49 values + two clocks ride the canonical row")
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

    /** Fast feed + the CHG-505 market-tick flag on. */
    private void openFastWithMarketTick() throws Exception {
        openWith(new MultiTimeframeAggregateFunction(LIVE_INTERVAL, false, true, true,
                SignalJobConfig.DEFAULT_ALLOWED_LATENESS_MS, true));
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

    /**
     * Sets all 49 market raw columns to {@code v+1 .. v+49}. Order = the raw
     * groups: 12 stats (open/high/low/prev-close/vwap/tbq/tsq/oi/oi-high/oi-low/
     * lower/upper), then depth bid px 1..5, bid qty 1..5, bid ord 1..5,
     * ask px 1..5, ask qty 1..5, ask ord 1..5, then the 7 extras (CHG-516):
     * day volume, change flag, imbalance, indicative close, reference price,
     * atv, btv.
     */
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
        r.setField(RawTableColumns.LOWER_LIMIT_PAISE, v + 11);
        r.setField(RawTableColumns.UPPER_LIMIT_PAISE, v + 12);
        r.setField(RawTableColumns.BID_PX_1, v + 13);
        r.setField(RawTableColumns.BID_PX_2, v + 14);
        r.setField(RawTableColumns.BID_PX_3, v + 15);
        r.setField(RawTableColumns.BID_PX_4, v + 16);
        r.setField(RawTableColumns.BID_PX_5, v + 17);
        r.setField(RawTableColumns.BID_QTY_1, v + 18);
        r.setField(RawTableColumns.BID_QTY_2, v + 19);
        r.setField(RawTableColumns.BID_QTY_3, v + 20);
        r.setField(RawTableColumns.BID_QTY_4, v + 21);
        r.setField(RawTableColumns.BID_QTY_5, v + 22);
        r.setField(RawTableColumns.BID_ORD_1, v + 23);
        r.setField(RawTableColumns.BID_ORD_2, v + 24);
        r.setField(RawTableColumns.BID_ORD_3, v + 25);
        r.setField(RawTableColumns.BID_ORD_4, v + 26);
        r.setField(RawTableColumns.BID_ORD_5, v + 27);
        r.setField(RawTableColumns.ASK_PX_1, v + 28);
        r.setField(RawTableColumns.ASK_PX_2, v + 29);
        r.setField(RawTableColumns.ASK_PX_3, v + 30);
        r.setField(RawTableColumns.ASK_PX_4, v + 31);
        r.setField(RawTableColumns.ASK_PX_5, v + 32);
        r.setField(RawTableColumns.ASK_QTY_1, v + 33);
        r.setField(RawTableColumns.ASK_QTY_2, v + 34);
        r.setField(RawTableColumns.ASK_QTY_3, v + 35);
        r.setField(RawTableColumns.ASK_QTY_4, v + 36);
        r.setField(RawTableColumns.ASK_QTY_5, v + 37);
        r.setField(RawTableColumns.ASK_ORD_1, v + 38);
        r.setField(RawTableColumns.ASK_ORD_2, v + 39);
        r.setField(RawTableColumns.ASK_ORD_3, v + 40);
        r.setField(RawTableColumns.ASK_ORD_4, v + 41);
        r.setField(RawTableColumns.ASK_ORD_5, v + 42);
        r.setField(RawTableColumns.VOLUME, v + 43);
        r.setField(RawTableColumns.CHANGE_FLAG, v + 44);
        r.setField(RawTableColumns.IMBALANCE_QTY, v + 45);
        r.setField(RawTableColumns.INDICATIVE_CLOSE_PAISE, v + 46);
        r.setField(RawTableColumns.REF_PRICE_PAISE, v + 47);
        r.setField(RawTableColumns.ATV, v + 48);
        r.setField(RawTableColumns.BTV, v + 49);
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

    /** Fast-feed rows whose TF discriminator matches. */
    private static List<RowData> rowsWithTf(List<RowData> rows, String tf) {
        List<RowData> out = new ArrayList<>();
        for (RowData r : rows) {
            if (!r.isNullAt(CandleLiveColumns.TF)
                    && tf.equals(r.getString(CandleLiveColumns.TF).toString())) {
                out.add(r);
            }
        }
        return out;
    }

    /** The FIFTEEN_S row of the n-th accepted trade tick (six rows per tick). */
    private static RowData canonical(List<RowData> rows, int tickIndex) {
        return rows.get(tickIndex * 6);
    }

    private static void assertAllMarketColumnsNull(RowData r) {
        for (int i = CandleLiveColumns.MARKET_SECTION_START; i < CandleLiveColumns.FIELD_COUNT; i++) {
            assertTrue(r.isNullAt(i),
                    "non-canonical row: market column "
                            + CandleLiveColumns.COLUMN_NAMES.get(i) + " must be NULL");
        }
    }

    @Test
    @DisplayName("all 42 values + both clocks ride the FIFTEEN_S row; the other five stay NULL")
    void canonicalRowCarriesEverythingOthersStayNull() throws Exception {
        openFast();
        long T0 = ist(2026, 9, 4, 10, 0, 0, 0);
        long T = T0 + 1_000L;
        harness.processElement(marketAll(tick(T, "fp-m1", "TRADE", 100_00L, 10L), 1_000L), T);

        List<RowData> rows = fastRows();
        assertEquals(6, rows.size(), "one forming row per TF");
        assertEquals("FIFTEEN_S", canonical(rows, 0).getString(CandleLiveColumns.TF).toString());

        RowData r = canonical(rows, 0);
        // 12 stats
        assertEquals(1_006L, r.getLong(CandleLiveColumns.MKT_TOTAL_BUY_QTY));
        assertEquals(1_007L, r.getLong(CandleLiveColumns.MKT_TOTAL_SELL_QTY));
        assertEquals(1_001L, r.getLong(CandleLiveColumns.MKT_DAY_OPEN_PAISE));
        assertEquals(1_002L, r.getLong(CandleLiveColumns.MKT_DAY_HIGH_PAISE));
        assertEquals(1_003L, r.getLong(CandleLiveColumns.MKT_DAY_LOW_PAISE));
        assertEquals(1_004L, r.getLong(CandleLiveColumns.MKT_PREV_CLOSE_PAISE));
        assertEquals(1_005L, r.getLong(CandleLiveColumns.MKT_VWAP_PAISE));
        assertEquals(1_008L, r.getLong(CandleLiveColumns.MKT_OPEN_INTEREST));
        assertEquals(1_009L, r.getLong(CandleLiveColumns.MKT_OI_DAY_HIGH));
        assertEquals(1_010L, r.getLong(CandleLiveColumns.MKT_OI_DAY_LOW));
        assertEquals(1_011L, r.getLong(CandleLiveColumns.MKT_LOWER_LIMIT_PAISE));
        assertEquals(1_012L, r.getLong(CandleLiveColumns.MKT_UPPER_LIMIT_PAISE));
        // 30 depth values
        assertEquals(1_013L, r.getLong(CandleLiveColumns.MKT_BID_PX_1));
        assertEquals(1_014L, r.getLong(CandleLiveColumns.MKT_BID_PX_2));
        assertEquals(1_015L, r.getLong(CandleLiveColumns.MKT_BID_PX_3));
        assertEquals(1_016L, r.getLong(CandleLiveColumns.MKT_BID_PX_4));
        assertEquals(1_017L, r.getLong(CandleLiveColumns.MKT_BID_PX_5));
        assertEquals(1_018L, r.getLong(CandleLiveColumns.MKT_BID_QTY_1));
        assertEquals(1_019L, r.getLong(CandleLiveColumns.MKT_BID_QTY_2));
        assertEquals(1_020L, r.getLong(CandleLiveColumns.MKT_BID_QTY_3));
        assertEquals(1_021L, r.getLong(CandleLiveColumns.MKT_BID_QTY_4));
        assertEquals(1_022L, r.getLong(CandleLiveColumns.MKT_BID_QTY_5));
        assertEquals(1_023L, r.getLong(CandleLiveColumns.MKT_BID_ORD_1));
        assertEquals(1_024L, r.getLong(CandleLiveColumns.MKT_BID_ORD_2));
        assertEquals(1_025L, r.getLong(CandleLiveColumns.MKT_BID_ORD_3));
        assertEquals(1_026L, r.getLong(CandleLiveColumns.MKT_BID_ORD_4));
        assertEquals(1_027L, r.getLong(CandleLiveColumns.MKT_BID_ORD_5));
        assertEquals(1_028L, r.getLong(CandleLiveColumns.MKT_ASK_PX_1));
        assertEquals(1_029L, r.getLong(CandleLiveColumns.MKT_ASK_PX_2));
        assertEquals(1_030L, r.getLong(CandleLiveColumns.MKT_ASK_PX_3));
        assertEquals(1_031L, r.getLong(CandleLiveColumns.MKT_ASK_PX_4));
        assertEquals(1_032L, r.getLong(CandleLiveColumns.MKT_ASK_PX_5));
        assertEquals(1_033L, r.getLong(CandleLiveColumns.MKT_ASK_QTY_1));
        assertEquals(1_034L, r.getLong(CandleLiveColumns.MKT_ASK_QTY_2));
        assertEquals(1_035L, r.getLong(CandleLiveColumns.MKT_ASK_QTY_3));
        assertEquals(1_036L, r.getLong(CandleLiveColumns.MKT_ASK_QTY_4));
        assertEquals(1_037L, r.getLong(CandleLiveColumns.MKT_ASK_QTY_5));
        assertEquals(1_038L, r.getLong(CandleLiveColumns.MKT_ASK_ORD_1));
        assertEquals(1_039L, r.getLong(CandleLiveColumns.MKT_ASK_ORD_2));
        assertEquals(1_040L, r.getLong(CandleLiveColumns.MKT_ASK_ORD_3));
        assertEquals(1_041L, r.getLong(CandleLiveColumns.MKT_ASK_ORD_4));
        assertEquals(1_042L, r.getLong(CandleLiveColumns.MKT_ASK_ORD_5));
        // both change clocks (first observation changes every value)
        assertEquals(T, r.getLong(CandleLiveColumns.MKT_STATS_CHANGED_AT));
        assertEquals(T, r.getLong(CandleLiveColumns.MKT_DEPTH_CHANGED_AT));
        // 7 extra raw values (CHG-516)
        assertEquals(1_043L, r.getLong(CandleLiveColumns.MKT_DAY_VOLUME));
        assertEquals(1_044L, r.getLong(CandleLiveColumns.MKT_CHANGE_FLAG));
        assertEquals(1_045L, r.getLong(CandleLiveColumns.MKT_IMBALANCE_QTY));
        assertEquals(1_046L, r.getLong(CandleLiveColumns.MKT_INDICATIVE_CLOSE_PAISE));
        assertEquals(1_047L, r.getLong(CandleLiveColumns.MKT_REF_PRICE_PAISE));
        assertEquals(1_048L, r.getLong(CandleLiveColumns.MKT_ATV));
        assertEquals(1_049L, r.getLong(CandleLiveColumns.MKT_BTV));

        for (int i = 1; i < rows.size(); i++) {
            assertAllMarketColumnsNull(rows.get(i));
        }
    }

    @Test
    @DisplayName("quote ticks update the snapshot without emitting; the next trade carries the fresh values")
    void quoteTickUpdatesSnapshotForTheNextTrade() throws Exception {
        openFast();
        long T0 = ist(2026, 9, 4, 10, 0, 0, 0);
        // Trade first: no market fields yet -> all NULL on the canonical row.
        harness.processElement(tick(T0 + 1_000L, "fp-m1", "TRADE", 100_00L, 10L), T0 + 1_000L);
        List<RowData> afterTrade = fastRows();
        assertEquals(6, afterTrade.size());
        assertTrue(canonical(afterTrade, 0).isNullAt(CandleLiveColumns.MKT_BID_PX_1));

        // Quote with depth + day stats: no new forming row...
        harness.processElement(
                marketAll(tick(T0 + 2_000L, "fp-q1", "QUOTE", 100_50L, 0L), 2_000L), T0 + 2_000L);
        assertEquals(6, fastRows().size(), "a quote tick must not emit a forming row");

        // ...but the next trade carries the quote-updated snapshot.
        harness.processElement(tick(T0 + 3_000L, "fp-m2", "TRADE", 101_00L, 5L), T0 + 3_000L);
        List<RowData> rows = fastRows();
        assertEquals(12, rows.size(), "six forming rows for each accepted trade tick");
        RowData latest = canonical(rows, 1);
        assertEquals(2_013L, latest.getLong(CandleLiveColumns.MKT_BID_PX_1));
        assertEquals(2_042L, latest.getLong(CandleLiveColumns.MKT_ASK_ORD_5));
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

        RowData r = canonical(fastRows(), 1);
        assertEquals(9_999L, r.getLong(CandleLiveColumns.MKT_BID_PX_1), "present field updates");
        assertEquals(8_888L, r.getLong(CandleLiveColumns.MKT_VWAP_PAISE), "present field updates");
        assertEquals(1_002L, r.getLong(CandleLiveColumns.MKT_DAY_HIGH_PAISE), "absent field keeps previous");
        assertEquals(1_042L, r.getLong(CandleLiveColumns.MKT_ASK_ORD_5), "absent field keeps previous");
    }

    @Test
    @DisplayName("fields never seen render NULL — never a fabricated zero")
    void neverSeenFieldsRenderAsNull() throws Exception {
        openFast();
        long T0 = ist(2026, 9, 4, 10, 0, 0, 0);
        harness.processElement(tick(T0 + 1_000L, "fp-a", "TRADE", 100_00L, 10L), T0 + 1_000L);

        assertAllMarketColumnsNull(canonical(fastRows(), 0));
    }

    @Test
    @DisplayName("a provided 0 extra rides as 0; a never-provided extra stays NULL (CHG-516)")
    void extrasKeepProvidedZerosAndAbsentNulls() throws Exception {
        openFast();
        long T0 = ist(2026, 9, 4, 10, 0, 0, 0);
        GenericRowData partial = tick(T0 + 1_000L, "fp-x", "TRADE", 100_00L, 10L);
        partial.setField(RawTableColumns.CHANGE_FLAG, 0L);
        partial.setField(RawTableColumns.VOLUME, 777L);
        harness.processElement(partial, T0 + 1_000L);

        RowData r = canonical(fastRows(), 0);
        assertEquals(0L, r.getLong(CandleLiveColumns.MKT_CHANGE_FLAG),
                "a provided 0 is a real value and must not become NULL");
        assertEquals(777L, r.getLong(CandleLiveColumns.MKT_DAY_VOLUME));
        assertTrue(r.isNullAt(CandleLiveColumns.MKT_ATV), "a never-provided extra stays NULL");
    }

    @Test
    @DisplayName("the 1 s mirror fallback carries the section on its FIFTEEN_S row only")
    void mirrorRowsCarrySnapshotOnCanonicalRowOnly() throws Exception {
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
        RowData canonicalRow = mirror.get(0);
        assertEquals("FIFTEEN_S", canonicalRow.getString(CandleLiveColumns.TF).toString());
        assertEquals(5_013L, canonicalRow.getLong(CandleLiveColumns.MKT_BID_PX_1));
        assertEquals(5_012L, canonicalRow.getLong(CandleLiveColumns.MKT_UPPER_LIMIT_PAISE));
        for (int i = 1; i < mirror.size(); i++) {
            assertAllMarketColumnsNull(mirror.get(i));
        }
    }

    @Test
    @DisplayName("the stats clock moves only on stats changes, the depth clock only on depth changes")
    void clocksAdvanceOnlyWhenTheirGroupChanges() throws Exception {
        openFast();
        long T0 = ist(2026, 9, 4, 10, 0, 0, 0);
        long t1 = T0 + 1_000L;
        long t2 = T0 + 2_000L;
        long t3 = T0 + 3_000L;
        long t4 = T0 + 4_000L;

        // T1: full snapshot -> both clocks at t1.
        harness.processElement(marketAll(tick(t1, "fp-1", "TRADE", 100_00L, 10L), 1_000L), t1);
        // T2: stats-only change (day open) -> stats clock moves, depth stays.
        GenericRowData statsOnly = tick(t2, "fp-2", "TRADE", 100_10L, 5L);
        statsOnly.setField(RawTableColumns.OPEN_PAISE, 9_999L);
        harness.processElement(statsOnly, t2);
        // T3: same values again -> neither clock moves.
        GenericRowData repeat = tick(t3, "fp-3", "TRADE", 100_20L, 5L);
        repeat.setField(RawTableColumns.OPEN_PAISE, 9_999L);
        repeat.setField(RawTableColumns.BID_PX_1, 1_013L);
        harness.processElement(repeat, t3);
        // T4: depth-only change (level-2 bid qty) -> depth clock moves, stats stays.
        GenericRowData depthOnly = tick(t4, "fp-4", "TRADE", 100_30L, 5L);
        depthOnly.setField(RawTableColumns.BID_QTY_2, 77_777L);
        harness.processElement(depthOnly, t4);

        List<RowData> rows = fastRows();
        RowData r2 = canonical(rows, 1);
        assertEquals(t2, r2.getLong(CandleLiveColumns.MKT_STATS_CHANGED_AT), "stats changed at t2");
        assertEquals(t1, r2.getLong(CandleLiveColumns.MKT_DEPTH_CHANGED_AT), "depth unchanged since t1");

        RowData r3 = canonical(rows, 2);
        assertEquals(t2, r3.getLong(CandleLiveColumns.MKT_STATS_CHANGED_AT), "repeat values do not move the stats clock");
        assertEquals(t1, r3.getLong(CandleLiveColumns.MKT_DEPTH_CHANGED_AT), "repeat values do not move the depth clock");

        RowData r4 = canonical(rows, 3);
        assertEquals(t2, r4.getLong(CandleLiveColumns.MKT_STATS_CHANGED_AT), "depth change must not touch the stats clock");
        assertEquals(t4, r4.getLong(CandleLiveColumns.MKT_DEPTH_CHANGED_AT), "depth changed at t4");
        assertEquals(77_777L, r4.getLong(CandleLiveColumns.MKT_BID_QTY_2));
    }

    @Test
    @DisplayName("a quote tick can move a clock without emitting; the next trade shows it")
    void quoteTickMovesClockForTheNextTrade() throws Exception {
        openFast();
        long T0 = ist(2026, 9, 4, 10, 0, 0, 0);
        harness.processElement(marketAll(tick(T0 + 1_000L, "fp-1", "TRADE", 100_00L, 10L), 1_000L), T0 + 1_000L);

        GenericRowData quote = tick(T0 + 2_000L, "fp-q", "QUOTE", 100_50L, 0L);
        quote.setField(RawTableColumns.BID_PX_1, 9_999L);
        harness.processElement(quote, T0 + 2_000L);

        harness.processElement(tick(T0 + 3_000L, "fp-2", "TRADE", 101_00L, 5L), T0 + 3_000L);
        RowData r = canonical(fastRows(), 1);
        assertEquals(T0 + 2_000L, r.getLong(CandleLiveColumns.MKT_DEPTH_CHANGED_AT),
                "the quote's depth change is stamped at the quote's event time");
        assertEquals(T0 + 1_000L, r.getLong(CandleLiveColumns.MKT_STATS_CHANGED_AT),
                "the quote carried no stats change");
        assertEquals(9_999L, r.getLong(CandleLiveColumns.MKT_BID_PX_1));
    }

    // — market-only rows (CHG-505) ————————————————————————————————————————

    @Test
    @DisplayName("a changed quote emits one TF=MKT row: identity + market section + clocks")
    void changedQuoteEmitsOneMarketRow() throws Exception {
        openFastWithMarketTick();
        long T0 = ist(2026, 9, 4, 10, 0, 0, 0);
        harness.processElement(
                marketAll(tick(T0 + 1_000L, "fp-t", "TRADE", 100_00L, 10L), 1_000L),
                T0 + 1_000L);
        assertEquals(0, rowsWithTf(fastRows(), CandleLiveColumns.TF_MARKET_TICK).size(),
                "an accepted trade carries the section on its forming rows, never a market row");

        harness.processElement(
                marketAll(tick(T0 + 2_000L, "fp-q", "QUOTE", 100_50L, 0L), 3_000L),
                T0 + 2_000L);

        List<RowData> marketRows = rowsWithTf(fastRows(), CandleLiveColumns.TF_MARKET_TICK);
        assertEquals(1, marketRows.size(), "one market row per changed quote");
        RowData r = marketRows.get(0);
        assertEquals(TOKEN, r.getLong(CandleLiveColumns.INSTRUMENT_TOKEN));
        assertEquals("NSE", r.getString(CandleLiveColumns.EXCHANGE).toString());
        assertEquals("TEST", r.getString(CandleLiveColumns.SYMBOL).toString());
        assertEquals(3_013L, r.getLong(CandleLiveColumns.MKT_BID_PX_1));
        assertEquals(3_042L, r.getLong(CandleLiveColumns.MKT_ASK_ORD_5));
        assertEquals(T0 + 2_000L, r.getLong(CandleLiveColumns.MKT_STATS_CHANGED_AT));
        assertEquals(T0 + 2_000L, r.getLong(CandleLiveColumns.MKT_DEPTH_CHANGED_AT));
        // Not a forming candle: the candle prefix is zero-defaulted, and no
        // forming row was emitted for the quote (six forming rows, one per TF,
        // plus this one market row).
        assertEquals(0L, r.getLong(CandleLiveColumns.CLOSE_PAISE));
        assertEquals(T0 + 2_000L, r.getLong(CandleLiveColumns.LAST_EVENT_TIME));
        assertEquals(7, fastRows().size(), "six forming rows + one market row");
        assertEquals(1, rowsWithTf(fastRows(), "FIFTEEN_S").size(),
                "a quote still emits no forming row");
    }

    @Test
    @DisplayName("a repeated identical quote emits nothing — no change, no row")
    void repeatedQuoteEmitsNothing() throws Exception {
        openFastWithMarketTick();
        long T0 = ist(2026, 9, 4, 10, 0, 0, 0);
        harness.processElement(
                marketAll(tick(T0 + 1_000L, "fp-t", "TRADE", 100_00L, 10L), 1_000L),
                T0 + 1_000L);
        harness.processElement(
                marketAll(tick(T0 + 2_000L, "fp-q1", "QUOTE", 100_50L, 0L), 3_000L),
                T0 + 2_000L);
        harness.processElement(
                marketAll(tick(T0 + 3_000L, "fp-q2", "QUOTE", 100_50L, 0L), 3_000L),
                T0 + 3_000L);

        assertEquals(1, rowsWithTf(fastRows(), CandleLiveColumns.TF_MARKET_TICK).size(),
                "the second quote repeated every value — neither clock moved, no row");
    }

    @Test
    @DisplayName("flag off: a changed quote emits no market row")
    void flagOffEmitsNoMarketRow() throws Exception {
        openFast();
        long T0 = ist(2026, 9, 4, 10, 0, 0, 0);
        harness.processElement(
                marketAll(tick(T0 + 1_000L, "fp-t", "TRADE", 100_00L, 10L), 1_000L),
                T0 + 1_000L);
        harness.processElement(
                marketAll(tick(T0 + 2_000L, "fp-q", "QUOTE", 100_50L, 0L), 3_000L),
                T0 + 2_000L);

        assertEquals(0, rowsWithTf(fastRows(), CandleLiveColumns.TF_MARKET_TICK).size());
    }

    @Test
    @DisplayName("fast feed off: the market row is inert (the 1s mirror carries the section)")
    void marketRowInertOnFallbackFeed() throws Exception {
        openWith(new MultiTimeframeAggregateFunction(LIVE_INTERVAL, false, true, false,
                SignalJobConfig.DEFAULT_ALLOWED_LATENESS_MS, true));
        long T0 = ist(2026, 9, 4, 10, 0, 0, 0);
        harness.processElement(
                marketAll(tick(T0 + 1_000L, "fp-t", "TRADE", 100_00L, 10L), 1_000L),
                T0 + 1_000L);
        harness.processElement(
                marketAll(tick(T0 + 2_000L, "fp-q", "QUOTE", 100_50L, 0L), 3_000L),
                T0 + 2_000L);

        assertEquals(0, rowsWithTf(fastRows(), CandleLiveColumns.TF_MARKET_TICK).size());
    }
}
