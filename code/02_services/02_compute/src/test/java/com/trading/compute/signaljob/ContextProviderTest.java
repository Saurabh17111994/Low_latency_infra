package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ContextProvider} — C1 of the live-candle strategy
 * context plan (docs/plans/2026-09-30-strategy-context-live-fetch.md).
 *
 * <p>The provider is the fetch machinery behind the strategy-facing
 * {@link ContextView}: cache hits are local, a miss schedules exactly one
 * fetch (single flight), results arrive on Fluss client threads and are only
 * promoted by {@link ContextProvider#drainArrivals()} on the mailbox thread.
 * Bounds under test: the byte-budgeted LRU cache, the in-flight cap, the
 * fetch timeout, and the retry cooldown. The clock is injected so every
 * timing assertion is deterministic.
 */
class ContextProviderTest {

    private static final long CACHE_BYTES = 1L << 20;
    private static final int MAX_INFLIGHT = 32;
    private static final long TIMEOUT_MS = 100L;
    private static final long COOLDOWN_MS = 250L;

    private final FakeFetcher fetcher = new FakeFetcher();
    private final long[] now = {1_000_000L};
    private ContextProvider provider;

    @AfterEach
    void tearDown() {
        if (provider != null) {
            provider.close();
            provider = null;
        }
    }

    private ContextProvider provider() {
        if (provider == null) {
            provider = newProvider(CACHE_BYTES, MAX_INFLIGHT);
        }
        return provider;
    }

    private ContextProvider newProvider(long cacheBytes, int maxInflight) {
        return new ContextProvider(
                fetcher,
                cacheBytes,
                maxInflight,
                TIMEOUT_MS,
                COOLDOWN_MS,
                () -> now[0],
                new UnregisteredMetricsGroup());
    }

    private static ContextKey key(long token, Timeframe tf, long windowStart) {
        return new ContextKey(token, tf, windowStart);
    }

    private static ContextCandle candle(ContextKey key, long closePaise) {
        return new ContextCandle(
                key.token(),
                key.tf(),
                key.windowStart(),
                key.windowStart() + key.tf().windowMs(),
                closePaise,
                closePaise,
                closePaise,
                closePaise,
                100L,
                5,
                key.windowStart() + 500L);
    }

    /** Completes the last scheduled fetch for {@code key} with a candle. */
    private void completeLast(ContextKey key) {
        fetcher.last(key).complete(candle(key, 1_234L));
    }

    // ── hits, misses, single flight ─────────────────────────────────────────

    @Test
    @DisplayName("miss schedules one fetch; the drained value is served from cache")
    void missThenHit() {
        ContextKey key = key(7L, Timeframe.ONE_M, 60_000L);

        assertNull(provider().lookup(key));
        assertEquals(1, fetcher.fetchCount);
        assertEquals(1L, provider().metrics().requests.getCount());
        assertEquals(1L, provider().metrics().misses.getCount());

        completeLast(key);
        assertFalse(provider().drainArrivals().isEmpty());

        ContextCandle hit = provider().lookup(key);
        assertNotNull(hit);
        assertEquals(1_234L, hit.closePaise());
        assertEquals(1, fetcher.fetchCount, "a cache hit must not fetch again");
        assertEquals(1L, provider().metrics().hits.getCount());
    }

    @Test
    @DisplayName("second lookup while in flight stays single-flight")
    void singleFlight() {
        ContextKey key = key(7L, Timeframe.ONE_M, 60_000L);

        assertNull(provider().lookup(key));
        assertNull(provider().lookup(key));

        assertEquals(1, fetcher.fetchCount, "one key = one in-flight fetch");
        assertEquals(1, provider().pendingCount());

        completeLast(key);
        assertFalse(provider().drainArrivals().isEmpty());
        assertNotNull(provider().lookup(key));
    }

    @Test
    @DisplayName("different keys fetch independently")
    void distinctKeysFetchIndependently() {
        assertNull(provider().lookup(key(1L, Timeframe.ONE_M, 60_000L)));
        assertNull(provider().lookup(key(2L, Timeframe.ONE_M, 60_000L)));

        assertEquals(2, fetcher.fetchCount);
        assertEquals(2, provider().pendingCount());
    }

    // ── failure modes: timeout, late, failed, absent ────────────────────────

    @Test
    @DisplayName("a fetch past the timeout is counted absent and cooldown retries it")
    void timeoutMarksAbsentAndCooldownPreventsImmediateRetry() {
        ContextKey key = key(7L, Timeframe.ONE_M, 60_000L);

        assertNull(provider().lookup(key));
        now[0] += TIMEOUT_MS + 1;

        assertTrue(provider().drainArrivals().isEmpty(), "a timeout promotes nothing");
        assertEquals(1L, provider().metrics().timedout.getCount());

        assertNull(provider().lookup(key));
        assertEquals(1, fetcher.fetchCount, "cooldown must suppress the retry");
        assertEquals(1L, provider().metrics().cooldownSkips.getCount());

        now[0] += COOLDOWN_MS;
        assertNull(provider().lookup(key));
        assertEquals(2, fetcher.fetchCount, "after the cooldown the key retries");
    }

    @Test
    @DisplayName("an arrival for a superseded fetch is dropped as late")
    void lateArrivalAfterTimeoutIsDropped() {
        ContextKey key = key(7L, Timeframe.ONE_M, 60_000L);

        assertNull(provider().lookup(key));
        now[0] += TIMEOUT_MS + 1;
        provider().drainArrivals(); // times the first fetch out
        now[0] += COOLDOWN_MS;
        assertNull(provider().lookup(key)); // second fetch, new sequence

        fetcher.first(key).complete(candle(key, 999L)); // the stale future lands
        provider().drainArrivals();

        assertEquals(1L, provider().metrics().late.getCount());
        assertEquals(0, provider().cacheCount(), "a late arrival must not enter the cache");
        assertEquals(1, provider().pendingCount(), "the live fetch still owns the key");

        completeLast(key);
        assertFalse(provider().drainArrivals().isEmpty());
        assertNotNull(provider().lookup(key));
    }

    @Test
    @DisplayName("a failed fetch is counted, never cached, and cooled down")
    void failedFetchCountsAndCooldowns() {
        ContextKey key = key(7L, Timeframe.ONE_M, 60_000L);

        assertNull(provider().lookup(key));
        fetcher.last(key).completeExceptionally(new RuntimeException("fluss down"));
        assertTrue(provider().drainArrivals().isEmpty());

        assertEquals(1L, provider().metrics().failed.getCount());
        assertEquals(0, provider().cacheCount());

        assertNull(provider().lookup(key));
        assertEquals(1, fetcher.fetchCount, "cooldown must suppress the retry");
        now[0] += COOLDOWN_MS;
        assertNull(provider().lookup(key));
        assertEquals(2, fetcher.fetchCount);
    }

    @Test
    @DisplayName("an empty lookup result (window not written) counts absent and cools down")
    void absentRowCountsAndCooldowns() {
        ContextKey key = key(7L, Timeframe.ONE_M, 60_000L);

        assertNull(provider().lookup(key));
        fetcher.last(key).complete(null); // Fluss: no row for this PK
        assertTrue(provider().drainArrivals().isEmpty());

        assertEquals(1L, provider().metrics().absent.getCount());
        assertEquals(0, provider().cacheCount());

        assertNull(provider().lookup(key));
        assertEquals(1, fetcher.fetchCount, "an absent window must not be refetched on every tick");
        now[0] += COOLDOWN_MS;
        assertNull(provider().lookup(key));
        assertEquals(2, fetcher.fetchCount);
    }

    // ── bounds: in-flight cap, byte-budgeted LRU ────────────────────────────

    @Test
    @DisplayName("the in-flight cap rejects new keys without scheduling")
    void inflightCapRejects() {
        provider = newProvider(CACHE_BYTES, 2);

        assertNull(provider.lookup(key(1L, Timeframe.ONE_M, 60_000L)));
        assertNull(provider.lookup(key(2L, Timeframe.ONE_M, 60_000L)));
        assertNull(provider.lookup(key(3L, Timeframe.ONE_M, 60_000L)));

        assertEquals(2, fetcher.fetchCount, "the cap holds at 2 scheduled fetches");
        assertEquals(1L, provider.metrics().rejected.getCount());
        assertEquals(2, provider.pendingCount());
    }

    @Test
    @DisplayName("the cache evicts least-recently-used entries past its byte budget")
    void lruEvictionRespectsByteBudget() {
        long budget = 2 * ContextProvider.ENTRY_BYTES_ESTIMATE;
        provider = newProvider(budget, MAX_INFLIGHT);

        ContextKey k1 = key(1L, Timeframe.ONE_M, 60_000L);
        ContextKey k2 = key(2L, Timeframe.ONE_M, 60_000L);
        ContextKey k3 = key(3L, Timeframe.ONE_M, 60_000L);
        for (ContextKey k : List.of(k1, k2, k3)) {
            assertNull(provider.lookup(k));
            completeLast(k);
            assertFalse(provider.drainArrivals().isEmpty());
        }

        assertEquals(2, provider.cacheCount(), "budget holds two entries");
        assertEquals(1L, provider.metrics().evictions.getCount(), "the eldest was evicted");
        assertTrue(provider.cacheBytesUsed() <= budget);

        assertNull(provider.lookup(k1), "the evicted key misses again");
        assertEquals(4, fetcher.fetchCount);
        assertNotNull(provider.lookup(k3), "the newest entry is still cached");
    }

    // ── the mailbox hand-off with concurrent completions ────────────────────

    @Test
    @DisplayName("concurrent completions from client threads drain exactly once each")
    void concurrentCompletionsAreMailboxSafe() throws Exception {
        int n = 64;
        provider = newProvider(CACHE_BYTES, n);
        List<ContextKey> keys = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            ContextKey k = key(i, Timeframe.FIVE_M, 180_000L);
            keys.add(k);
            assertNull(provider.lookup(k));
        }
        assertEquals(n, fetcher.fetchCount);

        ExecutorService pool = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);
        List<CompletableFuture<?>> completions = new ArrayList<>();
        for (ContextKey k : keys) {
            completions.add(CompletableFuture.runAsync(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                fetcher.last(k).complete(candle(k, 42L));
            }, pool));
        }
        start.countDown();
        CompletableFuture.allOf(completions.toArray(new CompletableFuture<?>[0]))
                .get(10, TimeUnit.SECONDS);
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertFalse(provider.drainArrivals().isEmpty());
        assertEquals(n, provider.metrics().completed.getCount());
        assertEquals(n, provider.cacheCount());
        assertEquals(0L, provider.metrics().late.getCount());
        assertEquals(0, provider.pendingCount());
    }

    // ── view seam + close ───────────────────────────────────────────────────

    @Test
    @DisplayName("the view is enabled and delegates reads to the provider")
    void viewDelegates() {
        ContextView view = provider().view();
        assertTrue(view.isEnabled());

        ContextKey key = key(7L, Timeframe.ONE_M, 60_000L);
        assertNull(view.candle(key.token(), key.tf(), key.windowStart()));
        completeLast(key);
        assertFalse(provider().drainArrivals().isEmpty());

        ContextCandle c = view.candle(key.token(), key.tf(), key.windowStart());
        assertNotNull(c);
        assertEquals(key.windowStart(), c.windowStart());
    }

    @Test
    @DisplayName("close closes the fetcher")
    void closeClosesFetcher() {
        provider().close();
        assertTrue(fetcher.closed.get());
        provider = null; // torn down already
    }

    // ── C2 additions: promoted-key reporting + per-token pending ────────────

    @Test
    @DisplayName("drainArrivals returns exactly the keys promoted in that pass")
    void drainReturnsPromotedKeys() {
        ContextKey k1 = key(1L, Timeframe.ONE_M, 60_000L);
        ContextKey k2 = key(2L, Timeframe.ONE_M, 60_000L);
        assertNull(provider().lookup(k1));
        assertNull(provider().lookup(k2));

        completeLast(k1);
        assertEquals(List.of(k1), provider().drainArrivals());
        assertTrue(provider().drainArrivals().isEmpty(), "the pass already consumed k1");

        completeLast(k2);
        assertEquals(List.of(k2), provider().drainArrivals());
    }

    @Test
    @DisplayName("hasPendingForToken reports only tokens with an in-flight fetch")
    void hasPendingForToken() {
        ContextKey k1 = key(1L, Timeframe.ONE_M, 60_000L);
        assertFalse(provider().hasPendingForToken(1L), "nothing scheduled yet");

        assertNull(provider().lookup(k1));
        assertTrue(provider().hasPendingForToken(1L));
        assertFalse(provider().hasPendingForToken(2L), "another token never counts");

        completeLast(k1);
        provider().drainArrivals();
        assertFalse(provider().hasPendingForToken(1L), "resolved transfers stop counting");
    }

    // ── fakes ───────────────────────────────────────────────────────────────

    /** Controllable fetcher: records every call, completion driven by the test. */
    private static final class FakeFetcher implements CandleFetcher {
        private final Map<ContextKey, List<CompletableFuture<ContextCandle>>> calls =
                new HashMap<>();
        private final AtomicBoolean closed = new AtomicBoolean();
        private int fetchCount;

        @Override
        public CompletableFuture<ContextCandle> fetch(ContextKey key) {
            fetchCount++;
            CompletableFuture<ContextCandle> future = new CompletableFuture<>();
            calls.computeIfAbsent(key, k -> new ArrayList<>()).add(future);
            return future;
        }

        @Override
        public void close() {
            closed.set(true);
        }

        private List<CompletableFuture<ContextCandle>> futures(ContextKey key) {
            List<CompletableFuture<ContextCandle>> list = calls.get(key);
            assertNotNull(list, "no fetch was scheduled for " + key);
            return list;
        }

        CompletableFuture<ContextCandle> first(ContextKey key) {
            return futures(key).get(0);
        }

        CompletableFuture<ContextCandle> last(ContextKey key) {
            List<CompletableFuture<ContextCandle>> list = futures(key);
            return list.get(list.size() - 1);
        }
    }
}
