package com.trading.compute.signaljob;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Strategy registry (strategy-host design, 2026-09-05): the only place that
 * maps a config {@code STRATEGIES} id to a constructor. The host resolves
 * ids here; unknown ids fail fast at startup, never silently.
 *
 * <p>To add a strategy: implement {@link SignalStrategy} (copy
 * {@link StubSmokeStrategy}), add one line below, add the id to the
 * {@code STRATEGIES} env list. Nothing else changes — no
 * {@code SignalJob} edit, no UID change, no filter edit.
 *
 * <p>N7 ({@code n7-range-breakout-v1}) is the first registered strategy
 * (2026-09-05 cutover, batch 2): it runs ONLY on the host. Its old operator
 * ({@code n7-signal-v1}, retired) must never come back — two emitters with
 * separate dedup maps would double-emit every setup.
 */
public final class Strategies {

    private Strategies() {}

    private static final Map<String, BiFunction<SignalJobConfig, SignalStrategy.Metrics, SignalStrategy>> ALL =
            buildAll();

    /**
     * Unit-test factories (see {@link #registerForTest}). Consulted by
     * {@link #create} and {@link #isKnown} alongside the frozen map so test
     * harnesses can run emitting strategies without touching production.
     */
    private static final Map<String, BiFunction<SignalJobConfig, SignalStrategy.Metrics, SignalStrategy>> TEST_OVERRIDES =
            new java.util.concurrent.ConcurrentHashMap<>();

    private static Map<String, BiFunction<SignalJobConfig, SignalStrategy.Metrics, SignalStrategy>> buildAll() {
        Map<String, BiFunction<SignalJobConfig, SignalStrategy.Metrics, SignalStrategy>> m =
                new LinkedHashMap<>();
        m.put(StubSmokeStrategy.RULE_ID, (config, metrics) -> new StubSmokeStrategy());
        m.put(N7RangeBreakoutStrategy.RULE_ID, N7RangeBreakoutStrategy::new);
        return Collections.unmodifiableMap(m);
    }

    /** Ids the host accepts in {@code STRATEGIES} (fail-fast reference). */
    public static Set<String> knownIds() {
        return ALL.keySet();
    }

    /**
     * Test-only escape hatch: registers a strategy factory for the life of
     * the JVM. Lets unit tests exercise the host with emitting strategies
     * while the production registry stays frozen. Never call from production
     * code or job config.
     */
    public static void registerForTest(
            String ruleId,
            BiFunction<SignalJobConfig, SignalStrategy.Metrics, SignalStrategy> factory) {
        TEST_OVERRIDES.put(
                java.util.Objects.requireNonNull(ruleId),
                java.util.Objects.requireNonNull(factory));
    }

    /**
     * Test-registered ids (via {@link #registerForTest}) are known too —
     * unit harnesses open the host with test strategies without touching the
     * production registry.
     */
    public static boolean isKnown(String ruleId) {
        return ruleId != null && (TEST_OVERRIDES.containsKey(ruleId) || ALL.containsKey(ruleId));
    }

    /**
     * Fresh instance for one instrument. Throws
     * {@link IllegalStateException} on unknown ids — startup refusal, never
     * a silent no-op.
     */
    public static SignalStrategy create(
            String ruleId, SignalJobConfig config, SignalStrategy.Metrics metrics) {
        BiFunction<SignalJobConfig, SignalStrategy.Metrics, SignalStrategy> f =
                TEST_OVERRIDES.get(ruleId);
        if (f == null) {
            f = ALL.get(ruleId);
        }
        if (f == null) {
            throw new IllegalStateException("unknown strategy id '" + ruleId
                    + "' (known: " + ALL.keySet() + ") — refusing to run a "
                    + "topology that silently drops a requested strategy");
        }
        return f.apply(config, metrics);
    }

    /** Read-only view for tests. */
    static Map<String, BiFunction<SignalJobConfig, SignalStrategy.Metrics, SignalStrategy>> allForTest() {
        return ALL;
    }
}
