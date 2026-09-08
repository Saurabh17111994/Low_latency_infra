package com.trading.common.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class EventDayTest {

    @Test
    void istBoundaryFlipsAt1830Utc() {
        // 2026-08-30T18:29:59Z == 2026-08-30T23:59:59 IST
        assertEquals("20260830", EventDay.of(Instant.parse("2026-08-30T18:29:59Z")));
        // 2026-08-30T18:30:00Z == 2026-08-31T00:00:00 IST
        assertEquals("20260831", EventDay.of(Instant.parse("2026-08-30T18:30:00Z")));
        // midday IST
        assertEquals("20260831", EventDay.of(Instant.parse("2026-08-31T07:00:00Z")));
    }

    @Test
    void epochMilliOverloadMatches() {
        long ms = Instant.parse("2026-08-31T10:00:00Z").toEpochMilli();
        assertEquals(EventDay.of(Instant.ofEpochMilli(ms)), EventDay.of(ms));
    }

    @Test
    void validateRejectsMalformedPartitionKey() {
        assertEquals("20260831", EventDay.validate("20260831"));
        assertEquals("20260831", EventDay.ofValidated(Instant.parse("2026-08-31T07:00:00Z")));
        assertThrows(IllegalArgumentException.class, () -> EventDay.validate(null));
        assertThrows(IllegalArgumentException.class, () -> EventDay.validate("2026-08-31"));
        assertThrows(IllegalArgumentException.class, () -> EventDay.validate("2026083"));
        assertThrows(IllegalArgumentException.class, () -> EventDay.validate("2026083X"));
    }
}
