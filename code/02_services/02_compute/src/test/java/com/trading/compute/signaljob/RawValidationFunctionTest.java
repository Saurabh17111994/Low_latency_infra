package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Schema/validity gate decisions — pure classification, no runtime context needed. */
class RawValidationFunctionTest {

    private RawValidationFunction fn;

    @BeforeEach
    void setUp() {
        Map<String, String> env = new java.util.HashMap<>();
        env.put("DEDUP_TTL_MS", "60000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        fn = new RawValidationFunction(SignalJobConfig.from(env));
    }

    @Test
    void acceptsValidTradeRow() {
        RowData row = TestRawRows.row(2885L, 1_750_000_000_000L, "fp-1", "TRADE", 100, 5);
        assertNull(fn.invalidReason(row));
    }

    @Test
    void acceptsValidNonTradeQuoteRow() {
        RowData row = TestRawRows.row(2885L, 1_750_000_000_000L, "fp-1", "QUOTE", 100, 0);
        assertNull(fn.invalidReason(row));
    }

    @Test
    void rejectsInvalidValidityState() {
        RowData row = TestRawRows.row(2885L, 1_750_000_000_000L, "fp-1", "TRADE", 100, 5);
        assertEquals("validity-state", fn.invalidReason(TestRawRows.withValidity(row, "INVALID_VALUES")));
    }

    @Test
    void rejectsNonPositivePrice() {
        RowData row = TestRawRows.row(2885L, 1_750_000_000_000L, "fp-1", "TRADE", 100, 5);
        assertEquals("non-positive-price", fn.invalidReason(TestRawRows.withPrice(row, 0)));
        assertEquals("non-positive-price", fn.invalidReason(TestRawRows.withPrice(row, -12)));
    }

    @Test
    void rejectsNegativeQuantity() {
        RowData row = TestRawRows.row(2885L, 1_750_000_000_000L, "fp-1", "TRADE", 100, 5);
        assertEquals("negative-qty", fn.invalidReason(TestRawRows.withQty(row, -1)));
    }

    @Test
    void rejectsUnknownSchemaVersion() {
        RowData row = TestRawRows.row(2885L, 1_750_000_000_000L, "fp-1", "TRADE", 100, 5);
        assertEquals("schema-version", fn.invalidReason(TestRawRows.withSchemaVersion(row, "3")));
    }

    @Test
    void rejectsLegacyV1LabelUnderDefaultConfig() {
        // Pre-fix ingestion labeled the v2-shaped row "1" (TickPacket.schemaVersion=1).
        // Under the v2-only contract that label stays rejected — no silent acceptance
        // of a label that would mask a future producer regression.
        RowData row = TestRawRows.row(2885L, 1_750_000_000_000L, "fp-1", "TRADE", 100, 5);
        assertEquals("schema-version", fn.invalidReason(TestRawRows.withSchemaVersion(row, "1")));
    }

    @Test
    void rejectsBlankFingerprint() {
        // Tracker 14 P6.3: a blank fingerprint collapses every blank row of a
        // token into one dedup key for the whole TTL — distinct ticks would be
        // silently dropped. Both null and whitespace-only fingerprints reject.
        RowData row = TestRawRows.row(2885L, 1_750_000_000_000L, "fp-1", "TRADE", 100, 5);
        assertEquals("blank-fingerprint",
                fn.invalidReason(TestRawRows.withFingerprint(row, null)));
        assertEquals("blank-fingerprint",
                fn.invalidReason(TestRawRows.withFingerprint(row, "   ")));
    }

    @Test
    void rejectsNonPositiveEventTime() {
        // Tracker 14 P6.3: a non-positive epoch-millis event time would keep the
        // bounded-out-of-orderness watermark near Long.MIN_VALUE and fire windows
        // early. Zero (null-field read) and negatives must reject.
        RowData row = TestRawRows.row(2885L, 1_750_000_000_000L, "fp-1", "TRADE", 100, 5);
        assertEquals("non-positive-event-time",
                fn.invalidReason(TestRawRows.withEventTime(row, 0L)));
        assertEquals("non-positive-event-time",
                fn.invalidReason(TestRawRows.withEventTime(row, -1L)));
        assertEquals("non-positive-event-time",
                fn.invalidReason(TestRawRows.withEventTime(row, Long.MIN_VALUE)));
    }

    @Test
    void rejectsEventTimeInWindowArithmeticOverflowRange() {
        // Tracker 14 P6.3: EventTimeTrigger computes window.maxTimestamp() +
        // allowedLateness; a near-Long.MAX_VALUE event time overflows that sum to
        // a negative timer and fires the window early on one tick. Cap =
        // Long.MAX_VALUE - candleWindowMs - allowedLatenessMs - 1 (15000/5000
        // defaults -> Long.MAX_VALUE - 20001). At-or-below the cap is accepted.
        RowData row = TestRawRows.row(2885L, 1_750_000_000_000L, "fp-1", "TRADE", 100, 5);
        assertEquals("event-time-overflow-window",
                fn.invalidReason(TestRawRows.withEventTime(row, Long.MAX_VALUE)));
        assertEquals("event-time-overflow-window",
                fn.invalidReason(TestRawRows.withEventTime(row, Long.MAX_VALUE - 20_000L)));
        assertNull(fn.invalidReason(TestRawRows.withEventTime(row, Long.MAX_VALUE - 20_001L)));
    }

    @Test
    void rejectsNonInsertRowKind() {
        RowData row = TestRawRows.row(2885L, 1_750_000_000_000L, "fp-1", "TRADE", 100, 5);
        ((org.apache.flink.table.data.GenericRowData) row).setRowKind(RowKind.UPDATE_AFTER);
        assertEquals("rowkind-not-insert", fn.invalidReason(row));
    }
}
