package com.trading.common.config;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;

/**
 * JVM / container memory contract (09-production-swarm § JVM and memory
 * configuration). Implements the load-bearing 65/35/85 policy as a pure,
 * testable guard:
 *
 * <ul>
 *   <li>max heap {@code = 65%} of the container memory limit
 *       ({@link PlatformConfig#JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT});</li>
 *   <li>the non-heap reserve — {@code container limit − max heap} — must be at
 *       least {@code 35%} of the limit
 *       ({@link PlatformConfig#NON_HEAP_MEMORY_RESERVE_PERCENT}); a smaller
 *       reserve means the process would OOM on off-heap/direct/metaspace, so
 *       startup is refused rather than allowed to degrade silently;</li>
 *   <li>total container memory at/above {@code 85%}
 *       ({@link PlatformConfig#CONTAINER_MEMORY_ALERT_PERCENT}) is the alert
 *       threshold a caller can use to refuse production readiness.</li>
 * </ul>
 *
 * <p>The guard is deliberately <em>non-fatal on a bare JVM</em>: when no real
 * container memory limit can be read (plain host JVM, test runner), there is no
 * bounded budget to validate, so {@link #assertContainerMemoryContract()} is a
 * no-op. It only enforces inside a real container where {@code cgroup} exposes a
 * finite limit. This keeps dev/test runs unaffected while making production
 * deployment fail fast on a mis-sized container.
 */
public final class ContainerMemoryGuard {
    private static final long NO_LIMIT = -1L;

    /**
     * At/above this, a cgroup byte value means "no limit": cgroup v1 reports unbounded as a
     * huge near-{@code Long.MAX_VALUE} value (9223372036854771712 on a 4 GiB page-boundary
     * kernel) rather than the v2 string {@code max} (P6-026).
     */
    private static final long UNBOUNDED_AT = 1L << 60;

    // ---- env-tunable percentages (P3, 2026-08-29) ----
    private static final String HEAP_PCT_KEY = "JVM_HEAP_PERCENT";
    private static final String RESERVE_PCT_KEY = "NON_HEAP_RESERVE_PERCENT";
    private static final String ALERT_PCT_KEY = "MEMORY_ALERT_PERCENT";

    /** Heap share of the container limit, env-tunable (default 65). */
    static int heapPercent() {
        return percentFromEnv(HEAP_PCT_KEY, PlatformConfig.JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT);
    }

    /** Non-heap reserve share, env-tunable (default 35). */
    static int reservePercent() {
        return percentFromEnv(RESERVE_PCT_KEY, PlatformConfig.NON_HEAP_MEMORY_RESERVE_PERCENT);
    }

    /** Alert threshold share, env-tunable (default 85). */
    static int alertPercent() {
        return percentFromEnv(ALERT_PCT_KEY, PlatformConfig.CONTAINER_MEMORY_ALERT_PERCENT);
    }

    private static int percentFromEnv(String key, int fallback) {
        // System property wins (testability); falls back to the env var.
        String v = System.getProperty(key);
        if (v == null) {
            v = System.getenv(key);
        }
        if (v == null || v.isBlank()) {
            return fallback;
        }
        int n;
        try {
            n = Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Config " + key + " must be an integer percent, got '" + v + "'");
        }
        if (n < 1 || n > 99) {
            throw new IllegalStateException("Config " + key + " must be in 1..99, got " + n);
        }
        return n;
    }

    private ContainerMemoryGuard() {
    }

    /**
     * {@code floor(value * pct / 100)}, computed without overflowing. The naive form
     * ({@code value * pct}) overflowed to a negative number for large limits and inverted the
     * startup decision (P6-268); dividing first keeps both partial products in range.
     * Exact for {@code value >= 0} and {@code 0 <= pct <= 100}.
     */
    static long percentOf(long value, int pct) {
        return Math.floorDiv(value, 100L) * pct + Math.floorDiv((value % 100L) * pct, 100L);
    }

    /**
     * {@code floor(part * 100 / whole)}, computed without overflowing. {@code part * 100}
     * overflowed for huge usage values, which made the 85% alert unreachable (P6-660). The
     * common case allocates nothing; only values that would overflow use exact BigInteger math.
     */
    static long ratioPercent(long part, long whole) {
        if (part <= Long.MAX_VALUE / 100L) {
            return Math.floorDiv(part * 100L, whole);
        }
        BigInteger exact = BigInteger.valueOf(part)
                .multiply(BigInteger.valueOf(100L))
                .divide(BigInteger.valueOf(whole));
        return exact.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0 ? Long.MAX_VALUE : exact.longValue();
    }

    /** Maximum heap the 65/35 contract allows for a given container limit. */
    public static long maxHeapBudget(long containerLimitBytes) {
        if (containerLimitBytes <= 0) {
            throw new IllegalArgumentException("containerLimitBytes must be positive, got " + containerLimitBytes);
        }
        return percentOf(containerLimitBytes, heapPercent());
    }

    /** Non-heap reserve = container limit − allowed max heap. */
    public static long nonHeapReserve(long containerLimitBytes) {
        return containerLimitBytes - maxHeapBudget(containerLimitBytes);
    }

    /** Percentage of the container limit the given used bytes represent. */
    public static long utilizedPercent(long containerLimitBytes, long usedBytes) {
        if (containerLimitBytes <= 0) {
            throw new IllegalArgumentException("containerLimitBytes must be positive, got " + containerLimitBytes);
        }
        if (usedBytes < 0) {
            throw new IllegalArgumentException("usedBytes must be non-negative, got " + usedBytes);
        }
        return ratioPercent(usedBytes, containerLimitBytes);
    }

    /**
     * The heap and reserve shares are complements by definition (65/35 by default). A pair that
     * does not sum to 100 silently contradicts the contract it claims to enforce — e.g. 90/35
     * promises a bigger heap <em>and</em> a reserve that is no longer there (P6-267).
     *
     * @throws IllegalStateException when {@code JVM_HEAP_PERCENT + NON_HEAP_RESERVE_PERCENT != 100}
     */
    static void assertPercentagesComplementary() {
        int heap = heapPercent();
        int reserve = reservePercent();
        if (heap + reserve != 100) {
            throw new IllegalStateException("Container memory percentages must sum to 100: "
                    + HEAP_PCT_KEY + "=" + heap + " + " + RESERVE_PCT_KEY + "=" + reserve
                    + " = " + (heap + reserve) + " — the non-heap reserve is the complement of the"
                    + " heap share (default 65/35; docs/08_implementation/09-production-swarm.md"
                    + " § JVM and memory configuration).");
        }
    }

    /**
     * The contract-violation message. The reserve reported is the one actually left
     * ({@code limit − currentMaxHeap}); the old text printed {@code nonHeapReserve(limit)}, the
     * <em>allowed minimum</em>, so a violation always looked roughly compliant (P6-661).
     */
    static String contractViolationMessage(long limit, long currentMaxHeap, int heapPercent, int reservePercent) {
        long heapBudget = percentOf(limit, heapPercent);
        long reserve = limit - currentMaxHeap;
        long reserveMin = percentOf(limit, reservePercent);
        return "Container memory contract violated: container limit=" + limit
                + " bytes, JVM max heap=" + currentMaxHeap + " bytes exceeds the "
                + heapPercent + "% share (" + heapBudget + "), leaving non-heap reserve=" + reserve
                + " below the required " + reservePercent + "% (" + reserveMin + "). Set an explicit"
                + " container memory limit consistent with the 65/35 rule"
                + " (docs/08_implementation/09-production-swarm.md § JVM and memory configuration)."
                + " Refusing to start.";
    }

    /**
     * True when total container memory usage is at or above the 85% alert
     * threshold. A caller (e.g. the readiness probe) refuses production
     * readiness while this holds.
     */
    public static boolean atOrAboveAlertPercent(long containerLimitBytes, long usedBytes) {
        return utilizedPercent(containerLimitBytes, usedBytes) >= alertPercent();
    }

    /**
     * Enforce the 65/35 contract for the current JVM inside a container.
     *
     * <p>Reads the cgroup memory limit (v2 then v1). If no finite limit is
     * present (bare host JVM / test run) this is a no-op — there is no budget
     * to validate. When a real limit exists, verifies that the JVM's configured
     * max heap stays within the 65% share; otherwise the non-heap reserve would
     * be below 35% and the container would OOM on off-heap/direct/metaspace, so
     * it refuses to proceed.
     *
     * @throws IllegalStateException when the 65/35 contract is violated inside
     *                               a real container
     */
    public static void assertContainerMemoryContract() {
        long limit = readContainerMemoryLimitBytes();
        if (limit <= 0) {
            return; // no bounded budget — nothing to enforce (dev/test JVM)
        }
        assertPercentagesComplementary();
        long heapBudget = maxHeapBudget(limit);
        long currentMaxHeap = Runtime.getRuntime().maxMemory();
        if (currentMaxHeap > heapBudget) {
            throw new IllegalStateException(
                    contractViolationMessage(limit, currentMaxHeap, heapPercent(), reservePercent()));
        }
    }

    /**
     * Best-effort read of the container memory limit in bytes, or a sentinel
     * {@code <= 0} when the current JVM is not inside a bounded cgroup.
     */
    public static long readContainerMemoryLimitBytes() {
        // cgroup v2: /sys/fs/cgroup/memory.max
        String v2 = readFirstLine("/sys/fs/cgroup/memory.max");
        if (v2 != null) {
            return limitFromRaw(v2);
        }
        // cgroup v1: /sys/fs/cgroup/memory/memory.limit_in_bytes
        String v1 = readFirstLine("/sys/fs/cgroup/memory/memory.limit_in_bytes");
        return v1 == null ? NO_LIMIT : limitFromRaw(v1);
    }

    /**
     * A cgroup memory-limit reading as bytes, or {@code NO_LIMIT} when the file is absent, the
     * cgroup says {@code max}, or the value is the cgroup v1 unbounded sentinel. A present but
     * non-numeric value is an environment fault and is raised rather than reported as
     * "unbounded" (P6-026, P6-269).
     */
    static long limitFromRaw(String raw) {
        if (raw == null || raw.equals("max")) {
            return NO_LIMIT;
        }
        long v;
        try {
            v = Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException("cgroup memory limit is not a number: '" + raw + "'", e);
        }
        return (v <= 0 || v >= UNBOUNDED_AT) ? NO_LIMIT : v;
    }

    /**
     * Best-effort read of the container's current memory usage in bytes, or a
     * sentinel {@code < 0} when the current JVM is not inside a bounded cgroup.
     * Mirrors {@link #readContainerMemoryLimitBytes()} (v2 then v1) so callers
     * can compute {@code utilizedPercent(limit, used)} for the 85% alert
     * gate. Note: cgroup v2 {@code memory.current} includes page cache; for a
     * readiness WARN gate this is acceptable (it errs toward refusing, which
     * is the safe direction) — a tuned setpoint may exclude reclaimable cache.
     */
    public static long readContainerMemoryUsedBytes() {
        String v2 = readFirstLine("/sys/fs/cgroup/memory.current");
        if (v2 != null) {
            return usedFromRaw(v2);
        }
        String v1 = readFirstLine("/sys/fs/cgroup/memory/memory.usage_in_bytes");
        return v1 == null ? -1L : usedFromRaw(v1);
    }

    /**
     * A cgroup memory-usage reading as bytes, or {@code -1} when the file is absent. A present
     * but non-numeric value is an environment fault and is raised (P6-269).
     */
    static long usedFromRaw(String raw) {
        if (raw == null) {
            return -1L;
        }
        try {
            long v = Long.parseLong(raw.trim());
            return v >= 0 ? v : -1L;
        } catch (NumberFormatException e) {
            throw new IllegalStateException("cgroup memory usage is not a number: '" + raw + "'", e);
        }
    }

    /**
     * First line of a cgroup file, or {@code null} when the file does not exist (an unbounded
     * host). A file that exists but cannot be read is an environment fault and is raised, never
     * silently reported as "unbounded" — that turned the guard into a no-op exactly when the
     * environment was unexpected (P6-269).
     */
    static String readFirstLine(String path) {
        try {
            List<String> lines = Files.readAllLines(Path.of(path));
            return lines.isEmpty() ? null : lines.get(0).trim();
        } catch (NoSuchFileException e) {
            return null; // no such cgroup file
        } catch (IOException e) {
            throw new IllegalStateException("Unable to read cgroup file " + path + ": " + e, e);
        }
    }
}
