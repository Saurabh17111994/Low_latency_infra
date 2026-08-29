package com.trading.common.config;

import java.nio.file.Files;
import java.nio.file.Path;

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

    /** Maximum heap the 65/35 contract allows for a given container limit. */
    public static long maxHeapBudget(long containerLimitBytes) {
        if (containerLimitBytes <= 0) {
            throw new IllegalArgumentException("containerLimitBytes must be positive, got " + containerLimitBytes);
        }
        return Math.floorDiv(containerLimitBytes * heapPercent(), 100L);
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
        return Math.floorDiv(usedBytes * 100L, containerLimitBytes);
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
        long heapBudget = maxHeapBudget(limit);
        long currentMaxHeap = Runtime.getRuntime().maxMemory();
        if (currentMaxHeap > heapBudget) {
            long reserve = nonHeapReserve(limit);
            long reserveMin = Math.floorDiv(limit * reservePercent(), 100L);
            throw new IllegalStateException(
                "Container memory contract violated: container limit=" + limit
                    + " bytes, JVM max heap=" + currentMaxHeap + " bytes exceeds the "
                    + heapPercent()
                    + "% share (" + heapBudget + "), leaving non-heap reserve=" + reserve
                    + " below the required " + reservePercent()
                    + "% (" + reserveMin + "). Set an explicit container memory limit consistent with the 65/35 rule"
                    + " (docs/08_implementation/09-production-swarm.md § JVM and memory configuration). Refusing to start.");
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
            if (v2.equals("max")) {
                return NO_LIMIT; // unbounded
            }
            try {
                long v = Long.parseLong(v2.trim());
                return v > 0 ? v : NO_LIMIT;
            } catch (NumberFormatException e) {
                return NO_LIMIT;
            }
        }
        // cgroup v1: /sys/fs/cgroup/memory/memory.limit_in_bytes
        String v1 = readFirstLine("/sys/fs/cgroup/memory/memory.limit_in_bytes");
        if (v1 != null) {
            try {
                long v = Long.parseLong(v1.trim());
                return v > 0 ? v : NO_LIMIT;
            } catch (NumberFormatException e) {
                return NO_LIMIT;
            }
        }
        return NO_LIMIT;
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
            try {
                long v = Long.parseLong(v2.trim());
                return v >= 0 ? v : -1L;
            } catch (NumberFormatException e) {
                return -1L;
            }
        }
        String v1 = readFirstLine("/sys/fs/cgroup/memory/memory.usage_in_bytes");
        if (v1 != null) {
            try {
                long v = Long.parseLong(v1.trim());
                return v >= 0 ? v : -1L;
            } catch (NumberFormatException e) {
                return -1L;
            }
        }
        return -1L;
    }

    private static String readFirstLine(String path) {
        try {
            Path p = Path.of(path);
            if (!Files.exists(p)) {
                return null;
            }
            java.util.List<String> lines = Files.readAllLines(p);
            return lines.isEmpty() ? null : lines.get(0).trim();
        } catch (Exception e) {
            return null;
        }
    }
}
