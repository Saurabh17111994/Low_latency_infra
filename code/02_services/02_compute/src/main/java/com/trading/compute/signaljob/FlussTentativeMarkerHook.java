package com.trading.compute.signaljob;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cluster wiring of {@link EarlySignalFunction.TentativeMarkerHook} onto the
 * {@link FlussTentativeMarkerStore} (CHG-121, 2026-09-01; async rework
 * 2026-09-02).
 *
 * <p>Lazily opens the underlying store on the TASK side
 * ({@link #open()}) — the hook object is created during topology build on
 * the client, but Fluss connections belong to the task process. Opening
 * throws if the marker table is configured but absent/unreachable: fail
 * closed (the F4 contract is either reconciled or the job does not start
 * pretending it is).
 *
 * <p>ASYNC CONTRACT (2026-09-02 rework): every operation runs on a single
 * dedicated background thread and returns a {@link CompletableFuture}; the
 * calling task thread NEVER blocks. Rationale: the first cluster drill with
 * synchronous markers (tm-kill-full-load-20260902-005900) stalled the task
 * mailbox during post-crash replay catch-up — each blocking lookup (5s
 * ceiling, one per final candle with pending==null) serialized on the task
 * thread until checkpoint RPCs timed out and Flink escalated to a global
 * failure. The single-thread executor also makes store access race-free;
 * only {@link #open()}/{@link #close()} run on the task thread.
 */
public final class FlussTentativeMarkerHook
        implements EarlySignalFunction.TentativeMarkerHook, java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FlussTentativeMarkerHook.class);

    private final String bootstrap;
    private final String database;
    private final String tableName;
    private final Duration timeout;

    // Task-side resources — created in open() on the TaskManager, never on
    // the client, and never serialized (transient: the hook itself MUST
    // survive job-graph shipping; the store/executor must not).
    private transient FlussTentativeMarkerStore store;
    private transient ExecutorService executor;

    public FlussTentativeMarkerHook(String bootstrap, String database, String tableName,
            Duration timeout) {
        this.bootstrap = bootstrap;
        this.database = database;
        this.tableName = tableName;
        this.timeout = timeout;
    }

    @Override
    public void open() throws Exception {
        if (store == null) {
            store = FlussTentativeMarkerStore.open(bootstrap, database, tableName, timeout);
        }
        if (executor == null) {
            executor = Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable, "tentative-marker-store");
                thread.setDaemon(true);
                return thread;
            });
        }
    }

    @Override
    public void close() throws Exception {
        if (executor != null) {
            // Give in-flight ops their bounded timeout to finish, then drop
            // the rest — anything not yet surfaced to the operator was never
            // emitted, so losing it is replay-safe.
            executor.shutdown();
            try {
                if (!executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            executor = null;
        }
        if (store != null) {
            store.close();
            store = null;
        }
    }

    @Override
    public CompletableFuture<Void> mark(String candidateId, long instrumentToken,
            long windowEnd) {
        long markedTs = System.currentTimeMillis();
        return CompletableFuture.runAsync(
                () -> {
                    try {
                        requireStore().mark(candidateId, instrumentToken, windowEnd, markedTs);
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                },
                requireExecutor());
    }

    @Override
    public CompletableFuture<Boolean> exists(String candidateId) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return requireStore().exists(candidateId);
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                },
                requireExecutor());
    }

    @Override
    public CompletableFuture<Void> clear(String candidateId) {
        // Fire-and-forget: a failed clear only leaves a stale marker that
        // the 2d table TTL reclaims (settlement already emitted).
        return CompletableFuture.runAsync(
                () -> {
                    try {
                        requireStore().clear(candidateId);
                    } catch (Exception e) {
                        LOG.warn("tentative-marker clear failed for {} (stale marker will "
                                + "age out via table TTL): {}", candidateId, e.toString());
                    }
                },
                requireExecutor());
    }

    private FlussTentativeMarkerStore requireStore() {
        if (store == null) {
            throw new IllegalStateException(
                    "marker hook used before open() — store is null");
        }
        return store;
    }

    private ExecutorService requireExecutor() {
        if (executor == null) {
            throw new IllegalStateException(
                    "marker hook used before open() — executor is null");
        }
        return executor;
    }
}
