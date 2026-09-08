package com.trading.common.schema.eod;

import com.trading.common.schema.EodControllerState;
import java.time.LocalDate;

/**
 * Durable per-day per-table offload record for the EOD controller (SCH-23;
 * docs/08_implementation/02-schema-storage.md "EOD controller and offload
 * gate").
 *
 * <p>Fields follow the dossier's offload-record spec: trading date, table and
 * schema version, source offset/range, row and byte counts, source/target
 * hashes, Iceberg snapshot/commit ID, verification state, retry count + next
 * retry, and the earliest allowed source expiry.
 *
 * <p>{@code tradingDate} is an ISO-8601 date string (yyyy-MM-dd) — the record
 * is Jackson-serializable without the jsr310 module (the repo pins
 * jackson-databind only). {@link #formatTradingDate}/{@link #parseTradingDate}
 * convert to/from {@link LocalDate} for arithmetic.
 *
 * <p><b>Transitions are validated, never assumed:</b> {@link #transition}
 * enforces the PENDING → WRITING → COMMITTED → VERIFYING → VERIFIED state
 * machine with the two failure exits and the retry/reconcile edges, so a
 * VERIFIED day can never silently regress. Source data cannot expire unless
 * the state is {@link EodControllerState#VERIFIED}; every non-VERIFIED day
 * requires retention extension ({@link #requiresRetentionExtension()}, the
 * load-bearing rule of {@link EodControllerState}).
 *
 * @param tradingDate ISO-8601 trading date (yyyy-MM-dd)
 * @param tableName physical table the record covers
 * @param schemaVersion table contract version at offload time
 * @param sourceOffsetStart inclusive source offset range start (-1 unknown)
 * @param sourceOffsetEnd exclusive source offset range end (-1 unknown)
 * @param rowCount rows copied to the lake target
 * @param byteCount bytes copied to the lake target
 * @param sourceHash source content hash/checksum ("" until computed)
 * @param targetHash target content hash/checksum ("" until verified)
 * @param icebergSnapshotId lake snapshot/commit ID ("" until committed)
 * @param state verification state (never null)
 * @param retryCount retries consumed for the current failure/offload
 * @param nextRetryAtMs epoch-millis of the next automatic retry (0 when none)
 * @param earliestAllowedSourceExpiryMs earliest instant the source may expire;
 *        Long.MAX_VALUE while unverified (never), set at VERIFIED time
 * @param updatedAtMs epoch-millis of the last transition
 */
public record EodOffloadRecord(
        String tradingDate,
        String tableName,
        String schemaVersion,
        long sourceOffsetStart,
        long sourceOffsetEnd,
        long rowCount,
        long byteCount,
        String sourceHash,
        String targetHash,
        String icebergSnapshotId,
        EodControllerState state,
        int retryCount,
        long nextRetryAtMs,
        long earliestAllowedSourceExpiryMs,
        long updatedAtMs) {

    // P4-286/288: fail fast in the canonical record — a durable expiry-gate
    // record must not accept null identity/state, malformed dates, or
    // negative counts only to NPE far from the write site. Evidence presence
    // (hashes/snapshot) is deliberately NOT gated here: the R2 tiering path
    // legitimately carries "" (read-only check, P4-302), and P4-129/130's
    // transition-time evidence gate is refused (it would break that path —
    // evidence stays executor-side via verify-against-committed).
    public EodOffloadRecord {
        java.util.Objects.requireNonNull(tradingDate, "tradingDate");
        java.util.Objects.requireNonNull(tableName, "tableName");
        java.util.Objects.requireNonNull(schemaVersion, "schemaVersion");
        java.util.Objects.requireNonNull(sourceHash, "sourceHash");
        java.util.Objects.requireNonNull(targetHash, "targetHash");
        java.util.Objects.requireNonNull(icebergSnapshotId, "icebergSnapshotId");
        java.util.Objects.requireNonNull(state, "state");
        parseTradingDate(tradingDate); // fail fast on malformed date
        if (rowCount < 0 || byteCount < 0 || retryCount < 0) {
            throw new IllegalArgumentException(
                    "counts must be >= 0: row=" + rowCount + " byte=" + byteCount
                            + " retry=" + retryCount);
        }
    }

    private static final long NEVER = Long.MAX_VALUE;

    /** True when {@code from -> to} is a legal transition of the state machine. */
    public static boolean isLegalTransition(EodControllerState from, EodControllerState to) {
        return from.canTransitionTo(to);
    }

    /**
     * New PENDING record for a trading day/table. Earliest allowed source
     * expiry starts at {@code NEVER} (Long.MAX_VALUE) — while unverified the
     * source must not expire; the planner/controller only releases it when the
     * day reaches VERIFIED.
     */
    public static EodOffloadRecord initial(LocalDate tradingDate, String tableName,
            String schemaVersion, long nowMs) {
        return new EodOffloadRecord(formatTradingDate(tradingDate), tableName, schemaVersion,
                -1, -1, 0, 0, "", "", "", EodControllerState.PENDING, 0, 0, NEVER, nowMs);
    }

    /**
     * Validate and apply a transition. Returns a new record with the updated
     * state/timestamps; throws {@link IllegalStateException} on any illegal or
     * regressive transition. Side effects:
     *
     * <ul>
     *   <li>entering FAILED_RETRYABLE: retryCount++ and nextRetryAtMs computed
     *       via {@link EodBackoff};</li>
     *   <li>leaving FAILED_RETRYABLE (retry): nextRetryAtMs cleared (the
     *       controller reschedules when the retry fails again);</li>
     *   <li>reaching VERIFIED: earliestAllowedSourceExpiryMs = now — from this
     *       instant the day's data may expire under the retention policy
     *       (permitsSourceExpiry), and the retry schedule clears
     *       (nextRetryAtMs = 0). retryCount is intentionally RETAINED as
     *       monotonic per-day history, not a per-attempt budget (P4-341/345):
     *       the record is terminal here, so no future backoff can consume it;</li>
     *   <li>FAILED_MANUAL → PENDING (manual reset): retryCount AND nextRetryAtMs
     *       reset to 0 (P4-341/345) — the next failure after a manual reset
     *       backs off from a clean slate, not from the pre-reset count.</li>
     */
    public EodOffloadRecord transition(EodControllerState next, long nowMs) {
        return transition(next, nowMs, EodBackoff.DEFAULT_BASE_MS, EodBackoff.DEFAULT_MAX_MS,
                EodBackoff.rng());
    }

    /**
     * Validate and apply a transition with explicit backoff tuning (P4-342/344:
     * deterministic tests pass a seeded rng; operators tune base/max without
     * editing the record). The 2-arg form delegates with the governed
     * defaults. Side effects are identical to {@link #transition(EodControllerState, long)}.
     */
    public EodOffloadRecord transition(EodControllerState next, long nowMs, long baseMs,
            long maxMs, java.util.Random rng) {
        if (!isLegalTransition(state, next)) {
            throw new IllegalStateException("illegal EOD transition " + state + " -> " + next
                    + " for " + tableName + " " + tradingDate);
        }
        int retryCount = this.retryCount;
        long nextRetryAtMs = this.nextRetryAtMs;
        long earliestAllowed = this.earliestAllowedSourceExpiryMs;
        if (next == EodControllerState.FAILED_RETRYABLE) {
            retryCount += 1;
            nextRetryAtMs = EodBackoff.nextRetryAtMs(nowMs, retryCount, baseMs, maxMs, rng);
        } else if (next == EodControllerState.VERIFIED) {
            earliestAllowed = nowMs;
            nextRetryAtMs = 0;
        } else if (next == EodControllerState.PENDING
                && state == EodControllerState.FAILED_MANUAL) {
            retryCount = 0;
            nextRetryAtMs = 0;
        } else if (state == EodControllerState.FAILED_RETRYABLE) {
            // retrying: the schedule is recomputed on the next failure
            nextRetryAtMs = 0;
        }
        return new EodOffloadRecord(tradingDate, tableName, schemaVersion, sourceOffsetStart,
                sourceOffsetEnd, rowCount, byteCount, sourceHash, targetHash, icebergSnapshotId,
                next, retryCount, nextRetryAtMs, earliestAllowed, nowMs);
    }

    /** Source may expire only when the manifest is VERIFIED. The
     *  earliestAllowedSourceExpiryMs timestamp is informational-only
     *  (P4-343/346): the planner derives expiry bounds from
     *  EodRetentionPolicy, never from this field — a deserialized row's
     *  timestamp is never read for decisions. */
    public boolean permitsSourceExpiry() {
        return state.permitsSourceExpiry();
    }

    /** Every non-VERIFIED state requires live retention extension. */
    public boolean requiresRetentionExtension() {
        return state.requiresRetentionExtension();
    }

    public LocalDate tradingDateAsLocalDate() {
        return parseTradingDate(tradingDate);
    }

    public static String formatTradingDate(LocalDate date) {
        return date.toString(); // ISO-8601 yyyy-MM-dd
    }

    public static LocalDate parseTradingDate(String isoDate) {
        return LocalDate.parse(isoDate);
    }
}
