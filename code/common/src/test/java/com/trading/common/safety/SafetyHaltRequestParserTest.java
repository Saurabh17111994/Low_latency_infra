package com.trading.common.safety;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Row-parse contract: column names and value domain mirror the DDL
 * (18_safety_halt_requests.sql) and SafetyHaltWriter (contract_version 2);
 * malformed rows raise {@link SafetyHaltRequestParser.ParseException}.
 */
@DisplayName("SafetyHaltRequestParser: DDL v3 row contract")
class SafetyHaltRequestParserTest {

    @Test
    @DisplayName("valid UNSAFE row parses with reason")
    void validUnsafeRow() {
        SlotSafetyRequest r = SafetyHaltRequestParser.parse(unsafeRow("hft-0", 5L, "FEED_STALLED"));
        assertEquals("hft-0", r.slotId());
        assertEquals(5L, r.connectionEpoch());
        assertEquals(SlotSafetyStatus.UNSAFE, r.status());
        assertEquals("FEED_STALLED", r.reasonCode());
        assertEquals(2, r.contractVersion());
    }

    @Test
    @DisplayName("valid RECOVERED row parses with empty reason")
    void validRecoveredRow() {
        SlotSafetyRequest r = SafetyHaltRequestParser.parse(recoveredRow("hft-1", 6L));
        assertEquals(SlotSafetyStatus.RECOVERED, r.status());
        assertEquals("", r.reasonCode());
        assertEquals("hft-1", r.slotId());
        assertEquals(6L, r.connectionEpoch());
    }

    @Test
    @DisplayName("numeric columns accept Integer and Long forms")
    void numericForms() {
        Map<String, Object> row = unsafeRow("hft-0", 5L, "FEED_STALLED");
        row.put(SafetyHaltRequestParser.COL_CONNECTION_EPOCH, Integer.valueOf(5));
        SlotSafetyRequest r = SafetyHaltRequestParser.parse(row);
        assertEquals(5L, r.connectionEpoch());
    }

    @Test
    @DisplayName("malformed rows raise ParseException with a reason")
    void malformedRows() {
        assertThrows(SafetyHaltRequestParser.ParseException.class,
                () -> SafetyHaltRequestParser.parse(null));

        Map<String, Object> noContract = unsafeRow("hft-0", 5L, "FEED_STALLED");
        noContract.put(SafetyHaltRequestParser.COL_CONTRACT_VERSION, 1);
        SafetyHaltRequestParser.ParseException e1 = assertThrows(SafetyHaltRequestParser.ParseException.class,
                () -> SafetyHaltRequestParser.parse(noContract));
        assertTrue(e1.getMessage().contains("contract_version"));

        Map<String, Object> badState = unsafeRow("hft-0", 5L, "FEED_STALLED");
        badState.put(SafetyHaltRequestParser.COL_STATE, "HALTED");
        SafetyHaltRequestParser.ParseException e2 = assertThrows(SafetyHaltRequestParser.ParseException.class,
                () -> SafetyHaltRequestParser.parse(badState));
        assertTrue(e2.getMessage().contains("UNSAFE or RECOVERED"));

        Map<String, Object> noReason = unsafeRow("hft-0", 5L, "FEED_STALLED");
        noReason.put(SafetyHaltRequestParser.COL_REASON_CODE, "");
        SafetyHaltRequestParser.ParseException e3 = assertThrows(SafetyHaltRequestParser.ParseException.class,
                () -> SafetyHaltRequestParser.parse(noReason));
        assertTrue(e3.getMessage().contains("reason"));

        Map<String, Object> missingSlot = unsafeRow("hft-0", 5L, "FEED_STALLED");
        missingSlot.remove(SafetyHaltRequestParser.COL_SLOT_ID);
        assertThrows(SafetyHaltRequestParser.ParseException.class,
                () -> SafetyHaltRequestParser.parse(missingSlot));

        Map<String, Object> nonNumericEpoch = unsafeRow("hft-0", 5L, "FEED_STALLED");
        nonNumericEpoch.put(SafetyHaltRequestParser.COL_CONNECTION_EPOCH, "five");
        SafetyHaltRequestParser.ParseException e5 = assertThrows(SafetyHaltRequestParser.ParseException.class,
                () -> SafetyHaltRequestParser.parse(nonNumericEpoch));
        assertTrue(e5.getMessage().contains("connection_epoch"));
    }

    @Test
    @DisplayName("L5-2: the long bound is exclusive 2^63 — no saturation")
    void longBoundIsExclusiveTwoToTheSixtyThree() {
        // (double) Long.MAX_VALUE rounds UP to 2^63; the old `d > Long.MAX_VALUE`
        // accepted it and longValue() saturated it to Long.MAX_VALUE.
        Object[] beyond = {
            (double) Long.MAX_VALUE, // 2^63 exactly
            0x1p63,                  // 2^63
            Double.MAX_VALUE,
            (float) Long.MAX_VALUE,  // 2^63 as float
            0x1p63f,                 // 2^63 as float
            Float.MAX_VALUE,
            Double.NaN,
            Double.POSITIVE_INFINITY,
            Double.NEGATIVE_INFINITY,
            Math.scalb(-1.0, 63) - 2048.0, // below -2^63
        };
        for (Object value : beyond) {
            Map<String, Object> row = unsafeRow("hft-0", 5L, "FEED_STALLED");
            row.put(SafetyHaltRequestParser.COL_CONNECTION_EPOCH, value);
            assertThrows(SafetyHaltRequestParser.ParseException.class,
                    () -> SafetyHaltRequestParser.parse(row),
                    "must reject " + value);
        }
    }

    @Test
    @DisplayName("L5-2: the largest value below the bound is accepted exactly")
    void largestValueBelowTheBoundIsAcceptedExactly() {
        double below = Math.nextDown(0x1p63); // 2^63 - 1024
        Map<String, Object> row = unsafeRow("hft-0", 5L, "FEED_STALLED");
        row.put(SafetyHaltRequestParser.COL_CONNECTION_EPOCH, below);
        assertEquals(9223372036854774784L, SafetyHaltRequestParser.parse(row).connectionEpoch());
        assertEquals((long) below, SafetyHaltRequestParser.parse(row).connectionEpoch());

        // A float in range widens exactly.
        Map<String, Object> viaFloat = unsafeRow("hft-0", 5L, "FEED_STALLED");
        viaFloat.put(SafetyHaltRequestParser.COL_CONNECTION_EPOCH, 5.0f);
        assertEquals(5L, SafetyHaltRequestParser.parse(viaFloat).connectionEpoch());
    }

    private static Map<String, Object> unsafeRow(String slotId, long epoch, String reason) {
        Map<String, Object> row = baseRow(slotId, epoch);
        row.put(SafetyHaltRequestParser.COL_STATE, "UNSAFE");
        row.put(SafetyHaltRequestParser.COL_REASON_CODE, reason);
        return row;
    }

    private static Map<String, Object> recoveredRow(String slotId, long epoch) {
        Map<String, Object> row = baseRow(slotId, epoch);
        row.put(SafetyHaltRequestParser.COL_STATE, "RECOVERED");
        row.put(SafetyHaltRequestParser.COL_REASON_CODE, "");
        return row;
    }

    private static Map<String, Object> baseRow(String slotId, long epoch) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put(SafetyHaltRequestParser.COL_HALT_REQUEST_ID, "req-" + slotId + "-" + epoch);
        row.put(SafetyHaltRequestParser.COL_SOURCE_COMPONENT, "INGESTION");
        row.put(SafetyHaltRequestParser.COL_SLOT_ID, slotId);
        row.put(SafetyHaltRequestParser.COL_CONNECTION_EPOCH, epoch);
        row.put(SafetyHaltRequestParser.COL_MANIFEST_FINGERPRINT, "m".repeat(64));
        row.put(SafetyHaltRequestParser.COL_ASSIGNED_TOKEN_SET_HASH, "h".repeat(64));
        row.put(SafetyHaltRequestParser.COL_DETECTION_TIME, epoch * 1000L);
        row.put(SafetyHaltRequestParser.COL_CONTRACT_VERSION, 2);
        return row;
    }
}
