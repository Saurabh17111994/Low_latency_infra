package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

// P1-006 guard: only the two documented modes pass validation. Before the
// fix, every input (including none) fell through to the destructive drop.
final class DropRawTableTest {

    // P1-218 guard: FAILED ensure must exit non-zero so calling scripts see it.
    @Test
    void ensureExitCodeSignalsFailure() {
        assertEquals(0, DropRawTable.ensureExitCode(true));
        assertEquals(1, DropRawTable.ensureExitCode(false));
    }

    // P1-219 guard: the admin-RPC bound is a pinned contract — a hanging
    // coordinator must fail the clean-baseline procedure in 30s, not forever.
    @Test
    void adminTimeoutIsThirtySeconds() {
        assertEquals(Duration.ofSeconds(30), DropRawTable.ADMIN_TIMEOUT);
    }

    @Test
    void dropAccepted() {
        assertEquals("drop", DropRawTable.requireMode(new String[] {"drop"}));
    }

    @Test
    void ensureAccepted() {
        assertEquals("ensure", DropRawTable.requireMode(new String[] {"ensure", "localhost:9123"}));
    }

    @Test
    void bareInvocationRejected() {
        assertThrows(IllegalArgumentException.class, () -> DropRawTable.requireMode(new String[] {}));
    }

    @Test
    void typoRejected() {
        assertThrows(IllegalArgumentException.class, () -> DropRawTable.requireMode(new String[] {"ensrue"}));
    }

    @Test
    void helpRejected() {
        assertThrows(IllegalArgumentException.class, () -> DropRawTable.requireMode(new String[] {"--help"}));
    }

    // P1-058 guards: non-local clusters need explicit --force, checked
    // before any connection is opened.
    @Test
    void localDropNeedsNoForce() {
        DropRawTable.requireLocalOrForce("localhost:9123", new String[] {"drop"});
        DropRawTable.requireLocalOrForce("127.0.0.1:9123", new String[] {"drop"});
    }

    @Test
    void remoteDropRefusedWithoutForce() {
        assertThrows(IllegalArgumentException.class, () -> DropRawTable.requireLocalOrForce(
                "prod-fluss.internal:9123", new String[] {"drop", "prod-fluss.internal:9123"}));
    }

    @Test
    void remoteDropAllowedWithForce() {
        DropRawTable.requireLocalOrForce(
                "prod-fluss.internal:9123",
                new String[] {"drop", "prod-fluss.internal:9123", "--force"});
    }
}
