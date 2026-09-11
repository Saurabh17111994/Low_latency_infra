package com.trading.common.schema.projection;

import java.util.List;

/** Durable immutable Postback_Quarantine LOG writer (16_postback_quarantine.sql). */
public interface PostbackQuarantineStore {

    /**
     * Durably append {@code row} to the quarantine LOG.
     *
     * <p>P3-410: full-LOG contract — {@link #all()} materializes every row
     * with no pagination or bound, and Fluss-backed implementations mirror
     * each append into an in-memory delegate (heap grows with the log).
     * Prefer a bounded read (e.g. list(offset, limit) / after(cursor)) plus a
     * retention/TTL contract before production volume.
     *
     * <p>DEV P3-515: broad throws Exception is intentional — no caller
     * branches on exception type today, so a narrower checked/unchecked type
     * would churn every impl and caller for zero consumers.
     */
    void append(QuarantinedPostback row) throws Exception;

    /**
     * Return every quarantined row. Unbounded by contract (see append);
     * callers must not assume a size bound.
     */
    java.util.List<QuarantinedPostback> all() throws Exception;
}
