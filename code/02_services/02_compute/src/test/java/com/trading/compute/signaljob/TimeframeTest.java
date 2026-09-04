package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

/**
 * Phase 0 — Track B: Timeframe enum invariants.
 *
 * <p>Covers only the enum's own contracts — no bucket math (that is Phase 1).
 * Validates: exactly 6 values in ascending windowMs, codes unique and
 * uppercase-with-underscores, sessionAligned true only for 3m/5m/15m, and exact
 * windowMs values.
 */
class TimeframeTest {

    private static final Pattern CODE_PATTERN = Pattern.compile("^[A-Z0-9]+_[A-Z]+$");

    @Test
    void enumHasExactlySixValues() {
        assertEquals(6, Timeframe.values().length,
                "Timeframe must have exactly 6 values");
    }

    @Test
    void valuesInAscendingWindowMs() {
        Timeframe[] values = Timeframe.values();
        for (int i = 1; i < values.length; i++) {
            assertTrue(values[i].windowMs() > values[i - 1].windowMs(),
                    "values must be in ascending windowMs order: "
                            + values[i - 1] + "(" + values[i - 1].windowMs() + ") should be < "
                            + values[i] + "(" + values[i].windowMs() + ")");
        }
    }

    @Test
    void windowMsValuesExact() {
        assertEquals(15_000L, Timeframe.FIFTEEN_S.windowMs());
        assertEquals(30_000L, Timeframe.THIRTY_S.windowMs());
        assertEquals(60_000L, Timeframe.ONE_M.windowMs());
        assertEquals(180_000L, Timeframe.THREE_M.windowMs());
        assertEquals(300_000L, Timeframe.FIVE_M.windowMs());
        assertEquals(900_000L, Timeframe.FIFTEEN_M.windowMs());
    }

    @Test
    void codesUniqueAndUppercaseWithUnderscores() {
        Set<String> seen = new HashSet<>();
        for (Timeframe tf : Timeframe.values()) {
            String code = tf.code();
            assertTrue(CODE_PATTERN.matcher(code).matches(),
                    "code must be uppercase-with-underscores, got: " + code + " for " + tf.name());
            assertTrue(seen.add(code),
                    "codes must be unique, duplicate: " + code);
        }
        assertEquals(Timeframe.values().length, seen.size());
    }

    @Test
    void codesMatchExpectedExactStrings() {
        assertEquals("FIFTEEN_S", Timeframe.FIFTEEN_S.code());
        assertEquals("THIRTY_S", Timeframe.THIRTY_S.code());
        assertEquals("ONE_M", Timeframe.ONE_M.code());
        assertEquals("THREE_M", Timeframe.THREE_M.code());
        assertEquals("FIVE_M", Timeframe.FIVE_M.code());
        assertEquals("FIFTEEN_M", Timeframe.FIFTEEN_M.code());
    }

    @Test
    void sessionAlignedTrueOnlyForThreeMPlus() {
        assertFalse(Timeframe.FIFTEEN_S.isSessionAligned(),
                "FIFTEEN_S must be epoch-aligned (sessionAligned=false)");
        assertFalse(Timeframe.THIRTY_S.isSessionAligned(),
                "THIRTY_S must be epoch-aligned");
        assertFalse(Timeframe.ONE_M.isSessionAligned(),
                "ONE_M must be epoch-aligned");

        assertTrue(Timeframe.THREE_M.isSessionAligned(),
                "THREE_M must be session-aligned");
        assertTrue(Timeframe.FIVE_M.isSessionAligned(),
                "FIVE_M must be session-aligned");
        assertTrue(Timeframe.FIFTEEN_M.isSessionAligned(),
                "FIFTEEN_M must be session-aligned");
    }

    @Test
    void codeMatchesEnumName() {
        for (Timeframe tf : Timeframe.values()) {
            assertEquals(tf.name(), tf.code(),
                    "code() must equal enum name() for DDL contract: " + tf.name());
        }
    }

    @Test
    void implementsSerializableAndNoFlinkImports() {
        assertTrue(Serializable.class.isAssignableFrom(Timeframe.class),
                "Timeframe must be Serializable");
    }

    @Test
    void ascendingWindowMsMatchesDeclarationOrder() {
        Timeframe[] expectedOrder = {
            Timeframe.FIFTEEN_S,
            Timeframe.THIRTY_S,
            Timeframe.ONE_M,
            Timeframe.THREE_M,
            Timeframe.FIVE_M,
            Timeframe.FIFTEEN_M
        };
        Timeframe[] actual = Timeframe.values();
        assertEquals(expectedOrder.length, actual.length);
        for (int i = 0; i < expectedOrder.length; i++) {
            assertEquals(expectedOrder[i], actual[i],
                    "declaration order must be ascending windowMs at index " + i);
        }
    }
}
