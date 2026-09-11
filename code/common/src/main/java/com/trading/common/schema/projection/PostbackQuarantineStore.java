package com.trading.common.schema.projection;

import java.util.List;

/** Durable immutable Postback_Quarantine LOG writer (16_postback_quarantine.sql). */
public interface PostbackQuarantineStore {

    /**
     * Durably append {@code row} to the quarantine LOG.
     *
     * <p>P3-167: implementations persist FIRST and expose nothing on failure. There is no
     * process-local view that could diverge from durable state.
     */
    void append(QuarantinedPostback row) throws Exception;

    /**
     * Read at most {@code limit} rows from the durable LOG, in LOG (append) order.
     *
     * <p>P3-410/P3-169: this replaces an unbounded {@code all()}. That read materialized the entire
     * LOG with no bound, and the Fluss implementation served it from an in-memory mirror that grew
     * with every append — so a long-lived process's heap tracked the quarantine log. Both are gone:
     * the read is a bounded scan of the table itself, which also sees rows appended by earlier
     * processes (the mirror was process-local, so those stayed invisible until a restart).
     *
     * <p>Bounded by contract — there is deliberately no unbounded variant, because an unbounded read
     * of an append-only LOG is a heap-growth bug waiting for volume. Note this is a <i>bounded
     * snapshot</i>, not pagination: it returns the first {@code limit} rows in LOG order and holds
     * no cursor. A resumable/recent-N read needs a cursor or an offset and should be added with the
     * first caller that needs one, rather than guessed at here.
     *
     * @throws IllegalArgumentException if {@code limit} is not positive
     */
    List<QuarantinedPostback> scan(int limit) throws Exception;
}
