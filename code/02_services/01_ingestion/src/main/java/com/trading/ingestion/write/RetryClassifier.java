package com.trading.ingestion.write;

/**
 * Classifies Fluss append failures as retryable or fatal.
 *
 * <h3>Retryable</h3>
 * Transient failures where the Fluss client's built-in retry should handle
 * recovery: network timeouts, temporary tablet unavailability, coordinator
 * re-election, connection reset.
 *
 * <h3>Fatal</h3>
 * Failures that cannot succeed on retry and should trigger the halt gate:
 * authentication failure, table not found, schema mismatch, authorization
 * denied, table deleted.
 *
 * <p>Dossier reference: {@code docs/08_implementation/03-ingestion.md} §G8.
 */
public final class RetryClassifier {

    private RetryClassifier() {}

    /** Outcome of classifying an exception. */
    public enum Classification {
        /** Safe to retry — transient error. */
        RETRYABLE,
        /** Fatal — halt the append path, open the safety gate. */
        FATAL
    }

    /**
     * Classify a Throwable from a failed append.
     * Inspects the cause chain for known error categories.
     *
     * <p>Fatal patterns take precedence across the <em>entire</em> cause
     * chain (R-038): a retryable wrapper (e.g. an {@code ExecutionException}
     * whose message mentions "connection" or "timeout") must not mask an
     * underlying fatal cause such as {@code AuthenticationException}. Only
     * after the full chain has been checked without finding a fatal cause
     * is the outcome RETRYABLE.
     */
    public static Classification classify(Throwable t) {
        // P1-124: null is unknown, and unknown fails closed per R-285 below —
        // a null here is a programming error, never a proven-transient fault.
        if (t == null) return Classification.FATAL;

        // Walk the entire cause chain; a fatal cause anywhere wins.
        boolean sawRetryable = false;
        Throwable current = t;
        while (current != null) {
            String msg = current.getMessage();
            String name = current.getClass().getName();

            if (isFatal(name, msg)) return Classification.FATAL;
            // Recognized transient patterns are noted — the walk continues so
            // a deeper fatal cause is not masked by a retryable wrapper.
            if (isRetryable(name, msg)) sawRetryable = true;

            current = current.getCause();
        }

        // R-285: retry ONLY on a recognized transient pattern; a completely
        // unknown exception fails closed (FATAL) — by the time this classifier
        // runs the Fluss client's built-in retries are exhausted, and an
        // unclassified failure means we cannot prove the append is safe to
        // retry. For money-safety evidence, open the halt gate.
        return sawRetryable ? Classification.RETRYABLE : Classification.FATAL;
    }

    /**
     * True when an interrupt drove the failure anywhere in the chain
     * (B125: interrupts are FATAL at once, never retried — and the caller
     * must restore the thread flag; see RawTickWriter.handleCompletion).
     */
    static boolean isInterruptCaused(Throwable t) {
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            if (cur.getClass().getName().contains("Interrupted")) {
                return true;
            }
        }
        return false;
    }

    /** Recognized transient patterns — the Fluss client usually recovers. */
    private static boolean isRetryable(String name, String msg) {
        // B125: bare contains("Connect") also matched manager/connector
        // bookkeeping failures (non-transient) and contains("Interrupted")
        // retried threads told to stop. Class-name matching is now failure
        // nouns only; the message heuristics below stay the broad net.
        if (name.contains("Timeout") || name.contains("ConnectException")) {
            return true;
        }
        if (msg != null) {
            String lower = msg.toLowerCase();
            return lower.contains("timeout")
                    || lower.contains("connection")
                    || lower.contains("refused")
                    || lower.contains("leader")
                    || lower.contains("unavailable")
                    || lower.contains("re-elect")
                    || lower.contains("retry")
                    // R-297 wedge fix: pool exhaustion after the bounded wait
                    // ("Failed to allocate new segment within the configured
                    // max blocking time ...") is transient — the sender drains
                    // the pool once the leaderless-table wedge clears. Retry
                    // on the scheduler thread instead of dropping the tick.
                    || lower.contains("allocate new segment");
        }
        return false;
    }

    /** Fatal patterns — return true if this link of the chain is fatal. */
    private static boolean isFatal(String name, String msg) {
        // B125: interrupts fail at once — a thread told to stop must never
        // spin the 3 bounded retries. Checked first; precedes everything.
        if (name.contains("Interrupted")) {
            return true;
        }
        if (name.contains("Authentication")
                || name.contains("AccessControl")
                || name.contains("Security")) {
            return true;
        }
        if (name.contains("TableNotExist")
                || name.contains("NoSuchTable")
                || name.contains("UnknownTable")) {
            return true;
        }
        // R-3xx stale-handle guard: a partition/table-bucket reference that no
        // longer exists on the server, or a tablet that is neither leader nor
        // follower for the referenced bucket. Inside the Fluss client these are
        // retried (RetriableException) while the client refreshes metadata — but
        // when one SURFACES here, the client has already exhausted its own retry
        // budget against a handle the server considers dead (e.g. the table was
        // dropped/recreated under a long-lived writer and the cached tableId is
        // stale). Retrying at the app layer is a busy-loop against a dead handle.
        if (name.contains("NotLeaderOrFollower")
                || name.contains("PartitionNotExist")
                || name.contains("UnknownTableOrBucket")) {
            return true;
        }
        if (name.contains("Schema") && (name.contains("Mismatch")
                || name.contains("Exception")
                || name.contains("Validation"))) {
            return true;
        }
        if (msg != null) {
            String lower = msg.toLowerCase();
            if (lower.contains("table") && lower.contains("not found")) {
                return true;
            }
            if (lower.contains("unauthorized")
                    || lower.contains("forbidden")
                    || lower.contains("access denied")) {
                return true;
            }
        }
        return false;
    }
}
