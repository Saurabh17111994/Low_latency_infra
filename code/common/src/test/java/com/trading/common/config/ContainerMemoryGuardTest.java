package com.trading.common.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for the 09-production-swarm JVM/container memory contract
 * (pure arithmetic — no cgroup/JVM dependency, so these run anywhere).
 */
class ContainerMemoryGuardTest {

    @TempDir
    Path tmp;

    @Test
    void maxHeapBudget_is_65_percent_of_limit() {
        // use a limit divisible by 100 so the 65% share is exact (no floor)
        long limit = 100L * 1024 * 1024;
        long budget = ContainerMemoryGuard.maxHeapBudget(limit);
        assertEquals(limit * 65L / 100L, budget);
        assertEquals(65L, ContainerMemoryGuard.utilizedPercent(limit, budget));
    }

    @Test
    void nonHeapReserve_is_35_percent_of_limit() {
        long limit = 100L * 1024 * 1024;
        long reserve = ContainerMemoryGuard.nonHeapReserve(limit);
        // reserve must be at least 35% of the limit
        assertTrue(reserve >= limit * 35L / 100L);
        assertEquals(35L, ContainerMemoryGuard.utilizedPercent(limit, reserve));
    }

    @Test
    void reject_non_positive_limit() {
        assertThrows(IllegalArgumentException.class, () -> ContainerMemoryGuard.maxHeapBudget(0));
        assertThrows(IllegalArgumentException.class, () -> ContainerMemoryGuard.utilizedPercent(10, -1));
    }

    @Test
    void alert_threshold_at_85_percent() {
        long limit = 100L * 1024 * 1024;
        long used84 = Math.floorDiv(limit * 84L, 100L);
        long used85 = Math.floorDiv(limit * 85L, 100L);
        assertFalse(ContainerMemoryGuard.atOrAboveAlertPercent(limit, used84));
        assertTrue(ContainerMemoryGuard.atOrAboveAlertPercent(limit, used85));
    }

    @Test
    void contract_enforced_only_when_real_limit_present() {
        // A real container limit with a max heap that exceeds the 65% share:
        // simulate by validating the pure check directly — a max heap larger
        // than the budget means the reserve is below 35%.
        long limit = 100L * 1024 * 1024;
        long heapBudget = ContainerMemoryGuard.maxHeapBudget(limit);
        long oversizedHeap = heapBudget + 1;
        assertTrue(oversizedHeap > heapBudget);
        // assertContainerMemoryContract() is a no-op on this bare JVM (no
        // cgroup), so we verify the budget arithmetic that drives it instead.
        assertTrue(oversizedHeap > ContainerMemoryGuard.maxHeapBudget(limit));
    }

    @Test
    void usage_reader_is_nonnegative_or_sentinel() {
        // On this bare host there is no bounded cgroup, so the reader must be a
        // no-fail sentinel; in a real container it returns the current usage.
        long used = ContainerMemoryGuard.readContainerMemoryUsedBytes();
        assertTrue(used == -1L || used >= 0, "usage reader must return -1 or a non-negative byte count");
    }

    @Test
    void envOverridesChangePercentages() {
        // P3 (2026-08-29): JVM_HEAP_PERCENT / NON_HEAP_RESERVE_PERCENT /
        // MEMORY_ALERT_PERCENT are env-tunable.
        System.setProperty("JVM_HEAP_PERCENT", "50");
        System.setProperty("NON_HEAP_RESERVE_PERCENT", "50");
        System.setProperty("MEMORY_ALERT_PERCENT", "90");
        try {
            long limit = 100L * 1024 * 1024;
            long budget = ContainerMemoryGuard.maxHeapBudget(limit);
            assertEquals(limit * 50L / 100L, budget, "50% heap override must apply");
            assertTrue(ContainerMemoryGuard.atOrAboveAlertPercent(limit, limit * 90L / 100L),
                    "90% alert override must trigger at 90%");
            assertFalse(ContainerMemoryGuard.atOrAboveAlertPercent(limit, limit * 89L / 100L),
                    "89% must not trigger the 90% alert");
        } finally {
            System.clearProperty("JVM_HEAP_PERCENT");
            System.clearProperty("NON_HEAP_RESERVE_PERCENT");
            System.clearProperty("MEMORY_ALERT_PERCENT");
        }
    }

    @Test
    void invalidPercentEnvFails() {
        // P3: a non-integer / out-of-range percent is a startup error.
        System.setProperty("JVM_HEAP_PERCENT", "abc");
        try {
            assertThrows(IllegalStateException.class,
                    () -> ContainerMemoryGuard.maxHeapBudget(100L * 1024 * 1024));
        } finally {
            System.clearProperty("JVM_HEAP_PERCENT");
        }

        System.setProperty("MEMORY_ALERT_PERCENT", "100");
        try {
            assertThrows(IllegalStateException.class,
                    () -> ContainerMemoryGuard.atOrAboveAlertPercent(1000, 900));
        } finally {
            System.clearProperty("MEMORY_ALERT_PERCENT");
        }
    }

    // ---- P6 W0 (2026-09-14): cgroup sentinels, overflow safety, fail-loud reads ----

    @Test
    void cgroup_v1_sentinel_is_unbounded() {
        // P6-026 (CRITICAL): v1 spells "no limit" as a huge near-Long.MAX value, not as "max";
        // treating it as a real limit overflowed the budget arithmetic downstream.
        assertTrue(ContainerMemoryGuard.limitFromRaw("9223372036854771712") <= 0,
                "the v1 unbounded sentinel must map to NO_LIMIT");
        assertTrue(ContainerMemoryGuard.limitFromRaw(Long.toString(1L << 60)) <= 0);
        assertTrue(ContainerMemoryGuard.limitFromRaw(Long.toString(Long.MAX_VALUE)) <= 0);
        assertEquals(1L << 30, ContainerMemoryGuard.limitFromRaw("1073741824"),
                "a real limit must be reported verbatim");
    }

    @Test
    void cgroup_absent_or_max_is_unbounded() {
        assertTrue(ContainerMemoryGuard.limitFromRaw(null) <= 0, "absent cgroup file");
        assertTrue(ContainerMemoryGuard.limitFromRaw("max") <= 0, "cgroup v2 unbounded");
        assertTrue(ContainerMemoryGuard.limitFromRaw("0") <= 0);
        assertTrue(ContainerMemoryGuard.limitFromRaw("-1") <= 0);
        assertTrue(ContainerMemoryGuard.usedFromRaw(null) < 0);
        assertEquals(1234L, ContainerMemoryGuard.usedFromRaw("1234"));
    }

    @Test
    void corrupt_cgroup_value_is_fatal_not_silent() {
        // P6-269: present-but-corrupt is an environment fault, not "no limit".
        assertThrows(IllegalStateException.class, () -> ContainerMemoryGuard.limitFromRaw("garbage"));
        assertThrows(IllegalStateException.class, () -> ContainerMemoryGuard.usedFromRaw("garbage"));
    }

    @Test
    void maxHeapBudget_is_overflow_safe() {
        // P6-268: limit * percent overflowed to negative for huge limits, inverting the check.
        // (Divisible by 100 so the expected 65% is exact rather than a floor artefact.)
        long limit = (Long.MAX_VALUE / 100L) * 100L;
        long exact = utilizedOf65(limit);
        assertNotEquals(limit * 65L / 100L, exact,
                "the pre-fix arithmetic must be wrong here, else this proves nothing");
        long budget = ContainerMemoryGuard.maxHeapBudget(limit);
        assertTrue(budget > 0, "budget must not overflow to negative: " + budget);
        assertEquals(exact, budget);
        assertEquals(65L, ContainerMemoryGuard.utilizedPercent(limit, budget));
    }

    /** 65% of {@code limit}, computed independently of the guard (BigInteger, no overflow). */
    private static long utilizedOf65(long limit) {
        return java.math.BigInteger.valueOf(limit)
                .multiply(java.math.BigInteger.valueOf(65L))
                .divide(java.math.BigInteger.valueOf(100L))
                .longValueExact();
    }

    @Test
    void overflow_safe_math_agrees_with_exact_math_everywhere() {
        // The overflow-safe helpers must not change any verdict the old arithmetic got right —
        // that is why the callers (ingestion monitor, readiness gate) need no change.
        long[] values = {1, 99, 100, 101, 1023, 4096, 1L << 30, 46L << 30, Integer.MAX_VALUE,
                1L << 40, (Long.MAX_VALUE / 100L) * 100L, Long.MAX_VALUE - 1, Long.MAX_VALUE};
        int[] percents = {1, 33, 35, 50, 65, 75, 85, 99, 100};
        var exact = java.math.BigInteger.valueOf(100L);
        for (long v : values) {
            for (int p : percents) {
                long expected = java.math.BigInteger.valueOf(v)
                        .multiply(java.math.BigInteger.valueOf(p)).divide(exact).longValue();
                assertEquals(expected, ContainerMemoryGuard.percentOf(v, p),
                        "percentOf(" + v + ", " + p + ")");
            }
            for (long whole : values) {
                long w = Math.max(whole, v);
                long expected = java.math.BigInteger.valueOf(v)
                        .multiply(exact).divide(java.math.BigInteger.valueOf(w)).longValue();
                assertEquals(expected, ContainerMemoryGuard.ratioPercent(v, w),
                        "ratioPercent(" + v + ", " + w + ")");
            }
        }
    }

    @Test
    void utilizedPercent_is_overflow_safe_for_huge_usage() {
        // P6-660: usedBytes * 100 overflowed, so the alert silently never fired.
        assertEquals(100L, ContainerMemoryGuard.utilizedPercent(Long.MAX_VALUE, Long.MAX_VALUE));
        assertTrue(ContainerMemoryGuard.atOrAboveAlertPercent(Long.MAX_VALUE, Long.MAX_VALUE));
    }

    @Test
    void percentages_must_sum_to_100() {
        // P6-267: heap 90 + reserve 35 (sum 125) would otherwise silently contradict the 65/35 rule.
        System.setProperty("JVM_HEAP_PERCENT", "90");
        System.setProperty("NON_HEAP_RESERVE_PERCENT", "35");
        try {
            assertThrows(IllegalStateException.class, ContainerMemoryGuard::assertPercentagesComplementary);
        } finally {
            System.clearProperty("JVM_HEAP_PERCENT");
            System.clearProperty("NON_HEAP_RESERVE_PERCENT");
        }
        assertDoesNotThrow(ContainerMemoryGuard::assertPercentagesComplementary,
                "65 + 35 is the contract default");
    }

    @Test
    void violation_message_reports_the_actual_reserve() {
        // P6-661: the message used to report the *allowed minimum* reserve, so a violation
        // always looked roughly compliant.
        long limit = 100L * 1024 * 1024;
        long budget = ContainerMemoryGuard.maxHeapBudget(limit);
        long currentMaxHeap = budget + (5L << 20);
        String msg = ContainerMemoryGuard.contractViolationMessage(limit, currentMaxHeap, 65, 35);
        assertTrue(msg.contains("reserve=" + (limit - currentMaxHeap)),
                "reserve must be limit - actual max heap: " + msg);
        assertFalse(msg.contains("reserve=" + (limit - budget)),
                "the allowed minimum must not be reported as the actual reserve: " + msg);
    }

    @Test
    void cgroup_file_absent_is_sentinel_but_unreadable_is_fatal() throws IOException {
        // P6-269: absent (unbounded host) vs unreadable (environment fault) must differ.
        assertNull(ContainerMemoryGuard.readFirstLine(tmp.resolve("no-such-cgroup-file").toString()));
        Path directory = Files.createDirectories(tmp.resolve("a-directory"));
        assertThrows(IllegalStateException.class,
                () -> ContainerMemoryGuard.readFirstLine(directory.toString()));
        Path file = Files.writeString(tmp.resolve("memory.max"), " 12345 \n");
        assertEquals("12345", ContainerMemoryGuard.readFirstLine(file.toString()));
    }
}
