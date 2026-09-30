package com.trading.compute.signaljob;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

/**
 * On-demand closed-candle provider of the strategy host — C1 of the live-candle
 * strategy context plan (docs/plans/2026-09-30-strategy-context-live-fetch.md).
 *
 * <p><b>Why inside the operator.</b> A fetch that leaves the host and returns
 * as a stream would need a job-graph cycle; the pinned Flink 2.2.1 has no
 * iteration API, so the only acyclic native shape is a fetch inside the host:
 * a miss schedules one async Fluss lookup (single flight), the Fluss client
 * thread publishes the completion into {@link #arrivals}, and the mailbox
 * thread promotes it with {@link #drainArrivals()} — the same mailbox
 * discipline the rest of the host uses.
 *
 * <p><b>Bounds (operator-approved defaults, see {@code SignalJobConfig}).</b>
 * The cache is a byte-budgeted LRU (default 8 MB/subtask); at most
 * {@code maxInflight} fetches are in flight per subtask; a fetch older than
 * the timeout is counted absent and retried only after a cooldown, so a
 * missing window is not refetched on every tick. Nothing here is Flink state:
 * the cache is heap-only and rebuilt on demand after a restore.
 *
 * <p><b>Threading contract.</b> {@link #lookup(ContextKey)},
 * {@link #drainArrivals()}, {@link #close()}, and the test seams are
 * mailbox-thread only. The whenComplete callbacks execute on Fluss client
 * threads and touch exactly one structure: the concurrent arrivals map.
 *
 * <p>Counters are registered under {@code compute.context.*} for the run
 * evidence (requests/hits/misses/fetches/completed/absent/failed/timedout/
 * late/rejected/cooldown.skips/evictions + inflight/cache gauges).
 */
final class ContextProvider implements AutoCloseable {

    /**
     * Conservative per-entry heap estimate for the cache budget (LRU map
     * entry + key + decoded candle). The budget is a bound, not a byte-exact
     * accountant — the same trade-off the host's other heap structures make.
     */
    static final long ENTRY_BYTES_ESTIMATE = 192L;

    /** Insertion-order cap for the cooldown map: evicting early only allows an earlier retry. */
    private static final int MAX_COOLDOWN_ENTRIES = 4096;

    /** Completion published by a Fluss client thread; consumed only by drainArrivals(). */
    private record Arrival(long fetchSeq, ContextCandle candle, Throwable error) {}

    /** One scheduled fetch (mailbox-owned). */
    private record Pending(long scheduledAtMs, long fetchSeq) {}

    private final CandleFetcher fetcher;
    private final long cacheBytes;
    private final int maxInflight;
    private final long fetchTimeoutMs;
    private final long retryCooldownMs;
    private final LongSupplier clock;
    private final ContextMetrics metrics;

    /** Access-ordered LRU (mailbox-owned). */
    private final Map<ContextKey, ContextCandle> cache =
            new LinkedHashMap<>(64, 0.75f, true);

    /** In-flight fetches by key (mailbox-owned). */
    private final Map<ContextKey, Pending> pending = new HashMap<>();

    /** Keys whose last attempt failed/absent/timed out, until retryAfter (mailbox-owned). */
    private final Map<ContextKey, Long> cooldownUntilMs =
            new LinkedHashMap<>(64, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<ContextKey, Long> eldest) {
                    return size() > MAX_COOLDOWN_ENTRIES;
                }
            };

    /** Cross-thread hand-off: client threads write, the mailbox thread drains. */
    private final ConcurrentHashMap<ContextKey, Arrival> arrivals = new ConcurrentHashMap<>();

    private long cacheBytesUsed;
    private long fetchSeq;

    /** Production factory: opens the real Fluss fetcher (fail-closed). */
    static ContextProvider open(SignalJobConfig config, MetricGroup metricGroup) {
        CandleFetcher fetcher = FlussCandleFetcher.open(config);
        try {
            return new ContextProvider(
                    fetcher,
                    config.contextCacheBytes(),
                    config.contextMaxInflight(),
                    config.contextFetchTimeoutMs(),
                    config.contextRetryCooldownMs(),
                    System::currentTimeMillis,
                    metricGroup);
        } catch (RuntimeException e) {
            fetcher.close();
            throw e;
        }
    }

    ContextProvider(
            CandleFetcher fetcher,
            long cacheBytes,
            int maxInflight,
            long fetchTimeoutMs,
            long retryCooldownMs,
            LongSupplier clock,
            MetricGroup metricGroup) {
        this.fetcher = fetcher;
        this.cacheBytes = cacheBytes;
        this.maxInflight = maxInflight;
        this.fetchTimeoutMs = fetchTimeoutMs;
        this.retryCooldownMs = retryCooldownMs;
        this.clock = clock;
        this.metrics = new ContextMetrics(metricGroup, this);
    }

    ContextView view() {
        return new ContextView(this);
    }

    /**
     * Mailbox thread only: returns the cached candle, or schedules one fetch
     * and returns {@code null}. A miss schedules only when the key has no
     * in-flight fetch (single flight), is not cooling down, and the in-flight
     * cap allows it.
     */
    ContextCandle lookup(ContextKey key) {
        metrics.requests.inc();
        ContextCandle hit = cache.get(key); // access order also refreshes the LRU position
        if (hit != null) {
            metrics.hits.inc();
            return hit;
        }
        metrics.misses.inc();
        long now = clock.getAsLong();
        Long retryAfter = cooldownUntilMs.get(key);
        if (retryAfter != null) {
            if (now < retryAfter) {
                metrics.cooldownSkips.inc();
                return null;
            }
            cooldownUntilMs.remove(key);
        }
        if (pending.containsKey(key)) {
            return null; // single flight — the in-flight fetch will publish the value
        }
        if (pending.size() >= maxInflight) {
            metrics.rejected.inc();
            return null;
        }
        long seq = ++fetchSeq;
        pending.put(key, new Pending(now, seq));
        metrics.fetches.inc();
        CompletableFuture<ContextCandle> future = fetcher.fetch(key);
        future.whenComplete(
                (candle, error) -> arrivals.put(key, new Arrival(seq, candle, error)));
        return null;
    }

    /**
     * Mailbox thread only: true when this token has at least one in-flight
     * fetch. The host uses it to decide whether to keep a live snapshot and
     * arm the wake-up timer (C2).
     */
    boolean hasPendingForToken(long token) {
        for (ContextKey key : pending.keySet()) {
            if (key.token() == token) {
                return true;
            }
        }
        return false;
    }

    /**
     * Mailbox thread only: moves completed fetches into the cache, expires
     * transfers past the fetch timeout, and returns exactly the keys promoted
     * in this pass (C2 re-dispatches those to the strategies waiting on the
     * live path; an empty list means nothing became ready).
     *
     * <p>An arrival whose fetch sequence no longer matches the pending entry
     * (timed out, or superseded by a retry) is dropped as late — it must not
     * satisfy a newer request.
     */
    List<ContextKey> drainArrivals() {
        List<ContextKey> promoted = new ArrayList<>();
        long now = clock.getAsLong();
        if (!arrivals.isEmpty()) {
            for (Iterator<Map.Entry<ContextKey, Arrival>> it = arrivals.entrySet().iterator();
                    it.hasNext(); ) {
                Map.Entry<ContextKey, Arrival> entry = it.next();
                ContextKey key = entry.getKey();
                Arrival arrival = entry.getValue();
                it.remove();
                Pending inFlight = pending.get(key);
                if (inFlight == null || inFlight.fetchSeq() != arrival.fetchSeq()) {
                    metrics.late.inc();
                    continue;
                }
                pending.remove(key);
                if (arrival.error() != null) {
                    metrics.failed.inc();
                    cooldownUntilMs.put(key, now + retryCooldownMs);
                    continue;
                }
                if (arrival.candle() == null) {
                    metrics.absent.inc();
                    cooldownUntilMs.put(key, now + retryCooldownMs);
                    continue;
                }
                putCache(key, arrival.candle());
                metrics.completed.inc();
                promoted.add(key);
            }
        }
        if (!pending.isEmpty()) {
            for (Iterator<Map.Entry<ContextKey, Pending>> it = pending.entrySet().iterator();
                    it.hasNext(); ) {
                Map.Entry<ContextKey, Pending> entry = it.next();
                if (now - entry.getValue().scheduledAtMs() > fetchTimeoutMs) {
                    it.remove();
                    metrics.timedout.inc();
                    cooldownUntilMs.put(entry.getKey(), now + retryCooldownMs);
                    // An eventual completion is dropped by the sequence check above.
                }
            }
        }
        return promoted;
    }

    private void putCache(ContextKey key, ContextCandle candle) {
        ContextCandle previous = cache.put(key, candle);
        if (previous == null) {
            cacheBytesUsed += ENTRY_BYTES_ESTIMATE;
        }
        while (cacheBytesUsed > cacheBytes && !cache.isEmpty()) {
            Iterator<Map.Entry<ContextKey, ContextCandle>> eldest = cache.entrySet().iterator();
            eldest.next();
            eldest.remove();
            cacheBytesUsed -= ENTRY_BYTES_ESTIMATE;
            metrics.evictions.inc();
        }
    }

    @Override
    public void close() {
        fetcher.close();
    }

    // ── test seams (mailbox-thread reads) ────────────────────────────────

    ContextMetrics metrics() {
        return metrics;
    }

    int pendingCount() {
        return pending.size();
    }

    int cacheCount() {
        return cache.size();
    }

    long cacheBytesUsed() {
        return cacheBytesUsed;
    }

    /** Counters + gauges of the provider; package-private so unit tests read them. */
    static final class ContextMetrics {
        final Counter requests;
        final Counter hits;
        final Counter misses;
        final Counter fetches;
        final Counter completed;
        final Counter absent;
        final Counter failed;
        final Counter timedout;
        final Counter late;
        final Counter rejected;
        final Counter cooldownSkips;
        final Counter evictions;

        ContextMetrics(MetricGroup group, ContextProvider provider) {
            this.requests = group.counter("compute.context.requests");
            this.hits = group.counter("compute.context.hits");
            this.misses = group.counter("compute.context.misses");
            this.fetches = group.counter("compute.context.fetches");
            this.completed = group.counter("compute.context.completed");
            this.absent = group.counter("compute.context.absent");
            this.failed = group.counter("compute.context.failed");
            this.timedout = group.counter("compute.context.timedout");
            this.late = group.counter("compute.context.late");
            this.rejected = group.counter("compute.context.rejected");
            this.cooldownSkips = group.counter("compute.context.cooldown.skips");
            this.evictions = group.counter("compute.context.evictions");
            group.gauge("compute.context.inflight", (Gauge<Long>) () -> (long) provider.pendingCount());
            group.gauge("compute.context.cache.bytes", (Gauge<Long>) provider::cacheBytesUsed);
            group.gauge("compute.context.cache.entries", (Gauge<Long>) () -> (long) provider.cacheCount());
        }
    }
}
