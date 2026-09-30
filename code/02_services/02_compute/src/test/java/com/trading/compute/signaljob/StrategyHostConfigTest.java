package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Config tests for the strategy-host rollout (2026-09-05): the
 * {@code STRATEGIES} list parses trims/dedups strictly, and the host flag
 * refuses incoherent combinations at startup.
 */
class StrategyHostConfigTest {

    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_WINDOW_ENTRIES", "2000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        return env;
    }

    @Test
    @DisplayName("host off by default, strategy list empty by default")
    void defaultsOff() {
        SignalJobConfig cfg = SignalJobConfig.from(env());
        assertTrue(!cfg.strategyHostEnabled());
        assertEquals(List.of(), cfg.strategyIds());
    }

    @Test
    @DisplayName("host on requires a non-empty STRATEGIES list")
    void hostOnRequiresStrategies() {
        Map<String, String> env = env();
        env.put("STRATEGY_HOST_ENABLED", "true");
        assertThrows(IllegalStateException.class, () -> SignalJobConfig.from(env));
    }

    @Test
    @DisplayName("host on with the stub id parses")
    void hostOnWithStubParses() {
        Map<String, String> env = env();
        env.put("STRATEGY_HOST_ENABLED", "true");
        env.put("MULTITF_ENABLED", "true");
        env.put("STRATEGIES", StubSmokeStrategy.RULE_ID);
        SignalJobConfig cfg = SignalJobConfig.from(env);
        assertTrue(cfg.strategyHostEnabled());
        assertEquals(List.of(StubSmokeStrategy.RULE_ID), cfg.strategyIds());
    }

    @Test
    @DisplayName("unknown STRATEGIES id fails fast with the known set named")
    void unknownStrategyIdFailsFast() {
        Map<String, String> env = env();
        env.put("STRATEGY_HOST_ENABLED", "true");
        env.put("STRATEGIES", "ghost-strategy-v1");
        IllegalStateException e =
                assertThrows(IllegalStateException.class, () -> SignalJobConfig.from(env));
        assertTrue(e.getMessage().contains("ghost-strategy-v1"));
    }

    @Test
    @DisplayName("duplicate STRATEGIES id fails fast")
    void duplicateStrategyIdFailsFast() {
        Map<String, String> env = env();
        env.put("STRATEGY_HOST_ENABLED", "true");
        env.put("STRATEGIES",
                StubSmokeStrategy.RULE_ID + ", " + StubSmokeStrategy.RULE_ID);
        assertThrows(IllegalStateException.class, () -> SignalJobConfig.from(env));
    }

    @Test
    @DisplayName("parse trims, drops blanks, keeps order")
    void parseTrimsAndDropsBlanks() {
        assertEquals(List.of(StubSmokeStrategy.RULE_ID),
                SignalJobConfig.parseStrategyIds("  , " + StubSmokeStrategy.RULE_ID + " ,, "));
        assertEquals(List.of(), SignalJobConfig.parseStrategyIds("  , ,"));
        assertEquals(List.of(), SignalJobConfig.parseStrategyIds(null));
    }

    // — C1 context provider (docs/plans/2026-09-30-strategy-context-live-fetch.md) —

    @Test
    @DisplayName("context provider off by default with the approved budgets")
    void contextDefaultsOff() {
        SignalJobConfig cfg = SignalJobConfig.from(env());
        assertFalse(cfg.strategyContextEnabled());
        assertEquals(8L * 1024 * 1024, cfg.contextCacheBytes());
        assertEquals(32, cfg.contextMaxInflight());
        assertEquals(100L, cfg.contextFetchTimeoutMs());
        assertEquals(250L, cfg.contextRetryCooldownMs());
    }

    @Test
    @DisplayName("context provider on requires the strategy host")
    void contextRequiresHost() {
        Map<String, String> env = env();
        env.put("STRATEGY_CONTEXT_ENABLED", "true");
        IllegalStateException e =
                assertThrows(IllegalStateException.class, () -> SignalJobConfig.from(env));
        assertTrue(e.getMessage().contains("STRATEGY_HOST_ENABLED"));
    }

    @Test
    @DisplayName("context knobs parse when the host is on")
    void contextKnobsParse() {
        Map<String, String> env = env();
        env.put("STRATEGY_HOST_ENABLED", "true");
        env.put("MULTITF_ENABLED", "true");
        env.put("STRATEGIES", StubSmokeStrategy.RULE_ID);
        env.put("STRATEGY_CONTEXT_ENABLED", "true");
        env.put("STRATEGY_CONTEXT_CACHE_BYTES", "1048576");
        env.put("STRATEGY_CONTEXT_MAX_INFLIGHT", "8");
        env.put("STRATEGY_CONTEXT_FETCH_TIMEOUT_MS", "50");
        env.put("STRATEGY_CONTEXT_RETRY_COOLDOWN_MS", "125");
        SignalJobConfig cfg = SignalJobConfig.from(env);
        assertTrue(cfg.strategyContextEnabled());
        assertEquals(1_048_576L, cfg.contextCacheBytes());
        assertEquals(8, cfg.contextMaxInflight());
        assertEquals(50L, cfg.contextFetchTimeoutMs());
        assertEquals(125L, cfg.contextRetryCooldownMs());
    }

    @Test
    @DisplayName("context max-inflight above the sanity cap fails fast")
    void contextInflightAboveCapFailsFast() {
        Map<String, String> env = env();
        env.put("STRATEGY_HOST_ENABLED", "true");
        env.put("MULTITF_ENABLED", "true");
        env.put("STRATEGIES", StubSmokeStrategy.RULE_ID);
        env.put("STRATEGY_CONTEXT_ENABLED", "true");
        env.put("STRATEGY_CONTEXT_MAX_INFLIGHT", "5000");
        assertThrows(IllegalStateException.class, () -> SignalJobConfig.from(env));
    }
}
