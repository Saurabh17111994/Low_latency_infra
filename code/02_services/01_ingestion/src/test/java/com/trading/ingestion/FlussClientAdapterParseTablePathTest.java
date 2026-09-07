package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.fluss.metadata.TablePath;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * P1-221: {@code FlussClientAdapter.parseTablePath} must reject bad input
 * with a clear {@link IllegalArgumentException} — never an NPE on null, and
 * never a silent mis-split on multi-dot or empty parts.
 */
@DisplayName("P1-221: parseTablePath validates input")
class FlussClientAdapterParseTablePathTest {

    private static TablePath parse(String tablePath) {
        try {
            Method m = Class.forName("com.trading.ingestion.FlussClientAdapter")
                    .getDeclaredMethod("parseTablePath", String.class);
            m.setAccessible(true);
            return (TablePath) m.invoke(null, tablePath);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new AssertionError(cause);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    @DisplayName("null, blank inputs are rejected with IllegalArgumentException")
    void nullAndBlankRejected() {
        assertThrows(IllegalArgumentException.class, () -> parse(null));
        assertThrows(IllegalArgumentException.class, () -> parse(""));
        assertThrows(IllegalArgumentException.class, () -> parse("   "));
    }

    @Test
    @DisplayName("multi-dot and empty parts are rejected with IllegalArgumentException")
    void malformedRejected() {
        assertThrows(IllegalArgumentException.class, () -> parse("db.table.extra"));
        assertThrows(IllegalArgumentException.class, () -> parse(".t"));
        assertThrows(IllegalArgumentException.class, () -> parse("db."));
        assertThrows(IllegalArgumentException.class, () -> parse("."));
    }

    @Test
    @DisplayName("valid inputs still parse (explicit db, or default db)")
    void validInputsParse() {
        TablePath explicit = parse("mydb.raw_table_1");
        assertEquals("mydb", explicit.getDatabaseName());
        assertEquals("raw_table_1", explicit.getTableName());
        TablePath bare = parse("raw_table_1");
        assertEquals("default", bare.getDatabaseName());
        assertEquals("raw_table_1", bare.getTableName());
    }
}
