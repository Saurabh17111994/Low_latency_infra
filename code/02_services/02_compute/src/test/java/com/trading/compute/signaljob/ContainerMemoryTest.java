package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tracker 14 box 906 (2026-08-12): {@link ContainerMemory} must read the
 * enclosing container's memory usage + limit from cgroup v2, falling back to
 * cgroup v1, and return {@code null} on ANY failure — the callers register
 * gauges only when a snapshot is available, so a missing cgroup hierarchy
 * yields a metric gap, never a task crash.
 */
@DisplayName("ContainerMemory cgroup v2/v1 reader (tracker 14 box 906)")
class ContainerMemoryTest {

    @TempDir
    Path tmp;

    private Path file(String name, String content) throws IOException {
        Path p = tmp.resolve(name);
        Files.write(p, content.getBytes(StandardCharsets.US_ASCII));
        return p;
    }

    @Test
    @DisplayName("cgroup v2: usage + numeric limit parsed")
    void v2NumericLimit() throws IOException {
        ContainerMemory.Snapshot s = ContainerMemory.read(
                file("current", "123456\n"),
                file("max", "1048576\n"),
                tmp.resolve("no-v1"), tmp.resolve("no-v1"));
        assertEquals(123_456L, s.usageBytes());
        assertEquals(1_048_576L, s.limitBytes());
        assertTrue(!s.unlimited());
    }

    @Test
    @DisplayName("cgroup v2: literal 'max' limit means unlimited (-1)")
    void v2Unlimited() throws IOException {
        ContainerMemory.Snapshot s = ContainerMemory.read(
                file("current", "77\n"),
                file("max", "max\n"),
                tmp.resolve("no-v1"), tmp.resolve("no-v1"));
        assertEquals(77L, s.usageBytes());
        assertEquals(-1L, s.limitBytes());
        assertTrue(s.unlimited());
    }

    @Test
    @DisplayName("v2 absent → cgroup v1 usage_in_bytes / limit_in_bytes fallback")
    void v1Fallback() throws IOException {
        ContainerMemory.Snapshot s = ContainerMemory.read(
                tmp.resolve("no-v2"),
                tmp.resolve("no-v2"),
                file("usage", "424242\n"),
                file("limit", "2147483648\n"));
        assertEquals(424_242L, s.usageBytes());
        assertEquals(2_147_483_648L, s.limitBytes());
    }

    @Test
    @DisplayName("v2 preferred when both hierarchies exist")
    void v2PreferredOverV1() throws IOException {
        ContainerMemory.Snapshot s = ContainerMemory.read(
                file("current", "10\n"),
                file("max", "20\n"),
                file("usage", "999\n"),
                file("limit", "888\n"));
        assertEquals(10L, s.usageBytes());
        assertEquals(20L, s.limitBytes());
    }

    @Test
    @DisplayName("nothing readable → null (gauges absent, never a crash)")
    void missingEverythingIsNull() {
        assertNull(ContainerMemory.read(
                tmp.resolve("nope"), tmp.resolve("nope"),
                tmp.resolve("nope"), tmp.resolve("nope")));
    }

    @Test
    @DisplayName("malformed content → null, not NumberFormatException")
    void malformedIsNull() throws IOException {
        assertNull(ContainerMemory.read(
                file("current", "not-a-number\n"),
                file("max", "1048576\n"),
                tmp.resolve("no-v1"), tmp.resolve("no-v1")));
        assertNull(ContainerMemory.read(
                file("current", "123\n"),
                file("max", "also-not-a-number\n"),
                tmp.resolve("no-v1"), tmp.resolve("no-v1")));
    }

    @Test
    @DisplayName("cgroup v1 unlimited sentinel normalizes to -1 like v2 'max' (P2-028)")
    void v1UnlimitedSentinelNormalized() throws IOException {
        // PAGE_COUNTER_MAX-style sentinel (~9.2e18): unlimited, not a real limit.
        ContainerMemory.Snapshot s = ContainerMemory.read(
                tmp.resolve("no-v2"),
                tmp.resolve("no-v2"),
                file("usage", "424242\n"),
                file("limit", "9223372036854771712\n"));
        assertEquals(424_242L, s.usageBytes());
        assertEquals(-1L, s.limitBytes());
        assertTrue(s.unlimited());
        // Boundary: exactly 1L<<60 normalizes too; just below stays numeric.
        ContainerMemory.Snapshot atEdge = ContainerMemory.read(
                tmp.resolve("no-v2"),
                tmp.resolve("no-v2"),
                file("usage2", "1\n"),
                file("limit2", Long.toString(1L << 60)));
        assertEquals(-1L, atEdge.limitBytes());
        ContainerMemory.Snapshot belowEdge = ContainerMemory.read(
                tmp.resolve("no-v2"),
                tmp.resolve("no-v2"),
                file("usage3", "1\n"),
                file("limit3", Long.toString((1L << 60) - 1)));
        assertEquals((1L << 60) - 1, belowEdge.limitBytes());
        assertTrue(!belowEdge.unlimited());
    }

    @Test
    @DisplayName("zero/negative usage/limit → null via loud Snapshot validation (P2-131)")
    void invalidValuesAreNull() throws IOException {
        // Numeric zero limit and negative usage parse but are malformed.
        assertNull(ContainerMemory.read(
                file("current1", "123\n"),
                file("max1", "0\n"),
                tmp.resolve("no-v1"), tmp.resolve("no-v1")));
        assertNull(ContainerMemory.read(
                file("current2", "-5\n"),
                file("max2", "1048576\n"),
                tmp.resolve("no-v1"), tmp.resolve("no-v1")));
        // The validation itself is loud (read() converts it to null).
        assertThrows(IllegalArgumentException.class,
                () -> new ContainerMemory.Snapshot(-1L, 100L));
        assertThrows(IllegalArgumentException.class,
                () -> new ContainerMemory.Snapshot(100L, 0L));
    }

    @Test
    @DisplayName("corrupt v2 files fall through to healthy v1 instead of metric-blind null (P2-132)")
    void corruptV2FallsBackToV1() throws IOException {
        ContainerMemory.Snapshot s = ContainerMemory.read(
                file("current", "not-a-number\n"),
                file("max", "1048576\n"),
                file("usage", "424242\n"),
                file("limit", "2147483648\n"));
        assertEquals(424_242L, s.usageBytes());
        assertEquals(2_147_483_648L, s.limitBytes());
        ContainerMemory.Snapshot s2 = ContainerMemory.read(
                file("currentB", "123\n"),
                file("maxB", "also-not-a-number\n"),
                file("usageB", "7\n"),
                file("limitB", "8\n"));
        assertEquals(7L, s2.usageBytes());
        assertEquals(8L, s2.limitBytes());
    }
}
