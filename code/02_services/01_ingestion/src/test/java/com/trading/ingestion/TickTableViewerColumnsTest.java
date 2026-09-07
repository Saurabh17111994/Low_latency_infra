package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.junit.jupiter.api.Test;

import com.trading.common.schema.RawTableSchema;

// P1-007 guard: viewer positions must equal the verified v3 layout
// (writer + DDL + shared schema agree: token=5 … validity=18, 21 cols).
// Positions are derived from RawTableSchema.COLUMNS, so this test pins the
// contract — a legitimate schema change updates list, writer, DDL and this
// test together; anything less fails loudly instead of shifting the display.
final class TickTableViewerColumnsTest {

    @Test
    void schemaIsV3EventDayFirst() {
        assertEquals(21, RawTableSchema.COLUMNS.size());
        assertEquals("event_day", RawTableSchema.COLUMNS.get(0));
    }

    @Test
    void viewerPositionsMatchVerifiedLayout() {
        assertEquals(5, RawTableSchema.COLUMNS.indexOf("instrument_token"));
        assertEquals(6, RawTableSchema.COLUMNS.indexOf("exchange"));
        assertEquals(7, RawTableSchema.COLUMNS.indexOf("symbol"));
        assertEquals(8, RawTableSchema.COLUMNS.indexOf("event_time"));
        assertEquals(11, RawTableSchema.COLUMNS.indexOf("tick_type"));
        assertEquals(12, RawTableSchema.COLUMNS.indexOf("last_price_paise"));
        assertEquals(13, RawTableSchema.COLUMNS.indexOf("last_qty"));
        assertEquals(18, RawTableSchema.COLUMNS.indexOf("validity_state"));
    }

    @Test
    void matchingSchemaPasses() {
        TickTableViewer.requireSchema(RawTableSchema.COLUMNS);
    }

    // P1-072: the display window is capped (Batch-3 #23: 10k lines) so a
    // huge/MAX_VALUE arg cannot OOM the viewer via pre-sizing.
    @Test
    void parseLimitDefaultsAndCaps() {
        assertEquals(20, TickTableViewer.parseLimit(new String[0]));
        assertEquals(50, TickTableViewer.parseLimit(new String[]{"50"}));
        assertEquals(10_000, TickTableViewer.parseLimit(new String[]{"10000"}));
        assertThrows(IllegalArgumentException.class,
                () -> TickTableViewer.parseLimit(new String[]{"10001"}));
        assertThrows(IllegalArgumentException.class,
                () -> TickTableViewer.parseLimit(new String[]{String.valueOf(Integer.MAX_VALUE)}));
        assertThrows(IllegalArgumentException.class,
                () -> TickTableViewer.parseLimit(new String[]{"0"}));
        assertThrows(IllegalArgumentException.class,
                () -> TickTableViewer.parseLimit(new String[]{"abc"}));
    }

    // P1-073: a null poll result is "no data yet", not an NPE.
    @Test
    void accumulateIgnoresNullAndEmptyPoll() {
        Deque<String> latest = new ArrayDeque<>();
        assertFalse(TickTableViewer.accumulate(null, latest, 20));
        assertTrue(latest.isEmpty());
        assertFalse(TickTableViewer.accumulate(ScanRecords.EMPTY, latest, 20));
        assertTrue(latest.isEmpty());
    }

    // P1-072: the window trims to `limit` and stores lines, not row refs.
    @Test
    void accumulateTrimsToLimit() {
        ScanRecords records = new ScanRecords(Map.of(new TableBucket(1L, 0),
                List.of(row(1L), row(2L), row(3L))));
        Deque<String> latest = new ArrayDeque<>();
        assertTrue(TickTableViewer.accumulate(records, latest, 2));
        assertEquals(2, latest.size());
        assertTrue(latest.getFirst().contains("| 2 |"),
                "oldest line must be evicted first, got: " + latest.getFirst());
        assertTrue(latest.getLast().contains("| 3 |"));
    }

    // P1-072: the formatted line is a snapshot — mutating the row afterwards
    // (what a buffer-reusing LogScanner does across polls) must not rewrite
    // history.
    @Test
    void formatRecordSnapshotsValuesAtPollTime() {
        GenericRow buf = rowBuf(3045L);
        String line = TickTableViewer.formatRecord(new ScanRecord(7L, 1L, ChangeType.APPEND_ONLY, buf));
        assertTrue(line.contains("3045"), "line must carry the token, got: " + line);
        buf.setField(5, 9999L); // scanner reuses the buffer for the next poll
        assertTrue(line.contains("3045"),
                "formatted line must not change when the row buffer is reused, got: " + line);
        assertTrue(TickTableViewer.formatRecord(new ScanRecord(8L, 1L, ChangeType.APPEND_ONLY, buf))
                .contains("9999"));
    }

    private static ScanRecord row(long token) {
        return new ScanRecord(0L, 0L, ChangeType.APPEND_ONLY, rowBuf(token));
    }

    private static GenericRow rowBuf(long token) {
        GenericRow row = new GenericRow(21);
        row.setField(5, token);
        row.setField(6, BinaryString.fromString("NSE"));
        row.setField(7, BinaryString.fromString("RELIANCE"));
        return row;
    }

    @Test
    void driftedSchemaRefused() {
        // v2 layout the viewer used before the fix: must be rejected now.
        assertThrows(
                IllegalStateException.class,
                () -> TickTableViewer.requireSchema(List.of("a", "b", "c")));
    }
}
