package com.trading.ingestion;

import com.trading.ingestion.write.RetryClassifier;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounded, leader-aware Fluss readiness wait for the ingestion startup path
 * (CHG-322).
 *
 * <p>Compose can start ingestion while the Fluss layer is still electing
 * leaders for {@code raw_table_1}'s buckets. Without this wait the schema
 * verification step fails, the process exits 1, and the {@code on-failure:3}
 * policy retries into the same wall — the feed stays down until the ingestion
 * service is recreated by hand (the documented {@code make up} hazard). Compose
 * dependency conditions cannot fix this: Swarm ignores them.
 *
 * <p>Before the schema step, this class retries a read-only metadata probe —
 * the same {@code admin.getTableInfo} predicate {@code pipeline-lib.sh} uses
 * for "Fluss ready" (client metadata initialization requires a live tablet).
 * Only failures classified {@link RetryClassifier.Classification#RETRYABLE}
 * are retried; recognized fatal failures (auth, schema, stale handle) fail
 * fast, and unrecognized failures fail closed per R-285. A missing table is
 * not a readiness problem — the schema step owns create-or-report — so the
 * wait ends and lets it run.
 *
 * <p>{@code FLUSS_STARTUP_WAIT_MS} bounds the wait (default 180 000, range
 * 0..600 000). {@code 0} disables the wait entirely and restores the
 * pre-CHG-322 fail-fast path.
 */
public final class FlussStartupReadiness {

    private static final Logger LOG = LoggerFactory.getLogger(FlussStartupReadiness.class);

    /** Per-attempt probe timeout — matches the pipeline-lib readiness probe. */
    static final long PROBE_TIMEOUT_MS = 5_000;
    static final long INITIAL_BACKOFF_MS = 250;
    static final long MAX_BACKOFF_MS = 5_000;

    private FlussStartupReadiness() {}

    /** One readiness attempt; throws when Fluss is not serving table metadata. */
    @FunctionalInterface
    interface Probe {
        void run() throws Exception;
    }

    /** Injectable sleep so tests never wait in real time. */
    @FunctionalInterface
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    /** What to do with a failed attempt. */
    enum Action {
        /** Transient — retry until the budget elapses. */
        RETRY,
        /** Not a readiness problem (table absent) — end the wait, let the schema step decide. */
        PROCEED,
        /** Cannot succeed on retry — fail fast with the original error. */
        FAIL
    }

    /** The wait budget elapsed with Fluss still not ready — fail closed. */
    public static final class StartupWaitTimeoutException extends Exception {
        StartupWaitTimeoutException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Wait until a read-only metadata probe for {@code rawTableName} succeeds,
     * or fail closed when the budget elapses. Retryable-only; fatal errors
     * propagate immediately.
     */
    static void awaitReady(String bootstrapServers, String rawTableName, long budgetMs)
            throws Exception {
        LOG.info("fluss-startup: waiting up to {} ms for table {} metadata readiness (bootstrap={})",
                budgetMs, rawTableName, bootstrapServers);
        awaitWith(() -> probe(bootstrapServers, rawTableName), budgetMs,
                System::currentTimeMillis, Thread::sleep, LOG::info);
    }

    /**
     * Read-only probe: client metadata for the table requires a live tablet.
     * Uses the same predicate as {@code pipeline-lib.sh}'s FlussReadyProbe.
     */
    static void probe(String bootstrapServers, String rawTableName) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrapServers);
        TablePath path = FlussClientAdapter.parseTablePath(rawTableName);
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Admin admin = connection.getAdmin()) {
            TableInfo info = admin.getTableInfo(path).get(PROBE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (info.getNumBuckets() <= 0) {
                throw new IllegalStateException("table has no buckets: " + path);
            }
        }
    }

    /** Failure taxonomy decision — pure, unit-tested without a cluster. */
    static Action actionFor(Throwable failure) {
        if (RetryClassifier.isTableMissing(failure)) {
            return Action.PROCEED;
        }
        return RetryClassifier.classify(failure)
                == RetryClassifier.Classification.RETRYABLE ? Action.RETRY : Action.FAIL;
    }

    /** Capped exponential backoff: 250, 500, 1000, 2000, 4000, 5000 ms, ... */
    static long backoffMs(int attempt) {
        return Math.min(INITIAL_BACKOFF_MS * (1L << Math.min(attempt - 1, 5)), MAX_BACKOFF_MS);
    }

    /**
     * Bounded wait loop with injected clock, sleeper, and log sink (tests).
     * Exceptions of the {@link Probe} propagate unchanged for FAIL outcomes.
     */
    static void awaitWith(Probe probe, long budgetMs, LongSupplier nowMs,
                          Sleeper sleeper, Consumer<String> log) throws Exception {
        long startMs = nowMs.getAsLong();
        long deadlineMs = startMs + Math.max(0L, budgetMs);
        int attempts = 0;
        while (true) {
            attempts++;
            try {
                probe.run();
                if (attempts > 1) {
                    log.accept("fluss-startup: ready after " + attempts + " attempts ("
                            + (nowMs.getAsLong() - startMs) + " ms)");
                }
                return;
            } catch (Exception failure) {
                Action action = actionFor(failure);
                if (action == Action.PROCEED) {
                    log.accept("fluss-startup: " + describe(failure)
                            + " — not a readiness problem; the schema step decides");
                    return;
                }
                if (action == Action.FAIL) {
                    throw failure;
                }
                long nowMsValue = nowMs.getAsLong();
                long remainingMs = deadlineMs - nowMsValue;
                if (remainingMs <= 0) {
                    throw new StartupWaitTimeoutException(
                            "Fluss not ready after " + budgetMs + " ms (" + attempts
                                    + " attempt(s); last error: " + describe(failure) + ")",
                            failure);
                }
                long sleepMs = Math.min(backoffMs(attempts), remainingMs);
                log.accept("fluss-startup: not ready (attempt " + attempts + ", "
                        + describe(failure) + ") — retrying in " + sleepMs + " ms");
                try {
                    sleeper.sleep(sleepMs);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw interrupted;
                }
            }
        }
    }

    private static String describe(Throwable t) {
        String message = t.getMessage();
        return message == null ? t.getClass().getSimpleName()
                : t.getClass().getSimpleName() + ": " + message;
    }
}
