package com.trading.common.schema.position;

import com.trading.common.model.PositionState;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.fluss.row.GenericRow;

/**
 * The SCH-20 operator-wiring core (Action Capture, Phase 4): a stateful driver
 * that holds the current {@link PositionSnapshot} per {@code position_id} in
 * memory, mints {@code position_id}s, and projects fills through the pure
 * {@link PositionProjector} with version gating.
 *
 * <p>Position identity (05-execution-core.md &rarr; "Position states"): account/instrument/side
 * uniqueness; {@code position_id} is minted
 * on the first uniquely correlated fill that creates exposure; a re-entry after
 * a full close (CLOSED) mints a NEW position_id. Minted ids are deterministic
 * ({@code pos-<account>-<instrumentToken>-<side>-<cycle>}) so a clean replay
 * converges.
 *
 * <p>Two feed paths:
 * <ul>
 *   <li>{@link #feed(GenericRow, FillContext, long)} — the operator path:
 *       resolves/mints the position id, maps the Fills row via
 *       {@link FillEventMapper}, and projects;</li>
 *   <li>{@link #feed(FillEvent, long)} — callers that already hold a resolved
 *       {@link FillEvent} (e.g. a Fluss changelog consumer reading pre-mapped
 *       rows).</li>
 * </ul>
 *
 * <p>Rejections (STALE / VIOLATION) are reported, never swallowed — the caller
 * (Action Capture) owns quarantine + halt. Deterministic: {@code nowMs} is a
 * parameter, never {@code System.currentTimeMillis()}.
 */
public final class PositionProjectorDriver {

    /** Account/instrument/side uniqueness key (dossier position protocol). */
    public record PositionKey(String accountScopeId, long instrumentToken, String side) {}

    public enum FeedOutcome {
        /** Projected — the new snapshot replaced the previous one. */
        APPLIED,
        /** Same source version + event already reflected — no-op. */
        DUPLICATE,
        /** Older source version — rejected. */
        STALE,
        /** Quantity/lifecycle violation — rejected, requires quarantine + halt. */
        VIOLATION,
        /** The row is not a fill (fill_qty missing/non-positive) — ignored. */
        NOT_A_FILL
    }

    public record FeedResult(FeedOutcome outcome, PositionSnapshot snapshot,
                             String positionId, String reason) {

        public static FeedResult applied(PositionSnapshot s, String positionId) {
            return new FeedResult(FeedOutcome.APPLIED, s, positionId, null);
        }

        public static FeedResult duplicate(PositionSnapshot s, String positionId) {
            return new FeedResult(FeedOutcome.DUPLICATE, s, positionId, null);
        }

        /**
         * P3-500 sibling: the reason is the projector's, passed through rather than rebuilt — the
         * same shape the VIOLATION arm uses below, so the rejected fill is named in exactly one
         * place and cannot drift from it.
         */
        public static FeedResult stale(PositionSnapshot current, String positionId, String reason) {
            return new FeedResult(FeedOutcome.STALE, current, positionId, reason);
        }

        public static FeedResult violation(String positionId, String reason) {
            return new FeedResult(FeedOutcome.VIOLATION, null, positionId, reason);
        }

        public static FeedResult notAFill(String positionId) {
            return new FeedResult(FeedOutcome.NOT_A_FILL, null, positionId,
                    "row is not a fill (fill_qty missing/non-positive)");
        }
    }

    /**
     * Placeholder bound while mapping a row before its position id is known
     * (see {@link #feed(GenericRow, FillContext, long)}). Never persisted and
     * never returned to callers.
     */
    private static final String UNRESOLVED_POSITION_ID = "pos-unresolved";

    /**
     * P3-501: the driver retains closure state for the life of the process — it never evicts a
     * retired (fully CLOSED) position. The bound is the run's position universe, not the fill
     * volume: {@code active} holds one entry per account/instrument/side key, {@code cycles} one
     * counter per such key (incremented once per minted cycle), and {@code snapshots} one entry per
     * position id ever minted. Retention is deliberate (the closed snapshot is history —
     * {@code reEntryAfterCloseMintsNewPositionId} pins that the driver tracks 2 positions after a
     * close and re-entry), so this is a documented contract rather than a leak. If a process is ever
     * expected to outlive its position universe — a multi-day single process, or ids minted without
     * bound — add an eviction/compaction policy here for retired positions once the caller has
     * confirmed or archived them, and update that test with it.
     */
    private final Map<PositionKey, String> active = new HashMap<>();
    private final Map<PositionKey, Integer> cycles = new HashMap<>();
    private final Map<String, PositionSnapshot> snapshots = new HashMap<>();

    /**
     * Operator path: decide fill-ness, map, resolve/mint the position id, and project.
     *
     * <p>The order here is pinned by two findings:
     * <ul>
     *   <li><b>P3-395</b> — fill-ness is decided BEFORE the id is resolved.
     *       Resolving first minted an id for a status-only row, so
     *       {@code positionIdFor(key)} returned an id holding no snapshot and a
     *       status-only row on a CLOSED key displaced {@code active} off the
     *       closed position.</li>
     *   <li><b>P3-166</b> — the row is mapped before the id is resolved, because
     *       a re-entry may only mint a new cycle when the incoming fill is
     *       genuinely newer than the closed snapshot (see
     *       {@link #resolvePositionId(PositionKey, FillEvent)}).</li>
     * </ul>
     */
    public FeedResult feed(GenericRow fillsRow, FillContext ctx, long nowMs) {
        Objects.requireNonNull(fillsRow, "fillsRow");
        Objects.requireNonNull(ctx, "ctx");
        if (!FillEventMapper.isFill(fillsRow)) {
            // No id is minted and `active` is untouched: a status-only postback
            // carries no exposure, so it can never create a position.
            return FeedResult.notAFill(null);
        }
        FillEvent mapped = FillEventMapper.mapIfFill(fillsRow, UNRESOLVED_POSITION_ID, ctx)
                .orElseThrow(() -> new IllegalStateException(
                        "isFill() accepted a row that mapIfFill() rejected"));
        // The key is built from the MAPPED fill: reading ACCOUNT_SCOPE_ID off the row here as well
        // would NPE ahead of the mapper's guard (P3-161), so identity validation lives in exactly
        // one place and its diagnostic rejection is the only path.
        PositionKey key = new PositionKey(mapped.accountScopeId(), ctx.instrumentToken(),
                ctx.side());
        return feed(mapped.withPositionId(resolvePositionId(key, mapped)), nowMs);
    }

    /** Direct path for callers that already hold a resolved {@link FillEvent}. */
    public FeedResult feed(FillEvent fill, long nowMs) {
        Objects.requireNonNull(fill, "fill");
        PositionSnapshot current = snapshots.get(fill.positionId());
        PositionProjector.ProjectionResult r = PositionProjector.apply(current, fill, nowMs);
        switch (r.outcome()) {
            case APPLIED -> snapshots.put(fill.positionId(), r.snapshot());
            case DUPLICATE, STALE, VIOLATION -> {
                // reported to the caller — quarantine/halt is the operator's job
            }
        }
        return toFeedResult(r, fill.positionId());
    }

    private static FeedResult toFeedResult(PositionProjector.ProjectionResult r, String positionId) {
        return switch (r.outcome()) {
            case APPLIED -> FeedResult.applied(r.snapshot(), positionId);
            case DUPLICATE -> FeedResult.duplicate(r.snapshot(), positionId);
            case STALE -> FeedResult.stale(r.snapshot(), positionId, r.reason());
            case VIOLATION -> FeedResult.violation(positionId, r.reason());
        };
    }

    /**
     * Mints or reuses the position id for a key. Re-entry after a full close
     * mints a NEW id (dossier: "re-entry (new position_id after closure)") —
     * only for a BUY re-open; a SELL against a CLOSED position stays on the
     * closed id and is rejected as an oversell.
     *
     * <p><b>P3-166:</b> re-entry requires the incoming fill to be genuinely
     * newer than the closed snapshot, and not the event already reflected
     * there. Deciding from the prior state alone minted a fresh id whose
     * snapshot was null, so {@link PositionProjector} evaluated the version gate
     * against version 0 and returned APPLIED for any version &gt; 0 — applying a
     * redelivered older opening fill as a phantom OPEN position and bypassing
     * the STALE/DUPLICATE guarantee entirely. Anything not strictly newer stays
     * on the closed id, where the version gate rejects it.
     */
    private String resolvePositionId(PositionKey key, FillEvent fill) {
        String currentId = active.get(key);
        if (currentId == null) {
            return mint(key);
        }
        PositionSnapshot current = snapshots.get(currentId);
        if (current == null) {
            return currentId;
        }
        if (current.state() == PositionState.CLOSED
                && FillEvent.SIDE_BUY.equals(key.side())
                && fill.sourceVersion() > current.sourceVersion()
                && !fill.sourceEventId().equals(current.sourceEventId())) {
            return mint(key);
        }
        return currentId;
    }

    private String mint(PositionKey key) {
        int cycle = cycles.merge(key, 1, Integer::sum);
        String id = "pos-" + key.accountScopeId() + "-" + key.instrumentToken()
                + "-" + key.side() + "-" + cycle;
        active.put(key, id);
        return id;
    }

    public PositionSnapshot snapshot(String positionId) {
        return snapshots.get(positionId);
    }

    /** The current active position id for a key (null if never fed). */
    public String positionIdFor(PositionKey key) {
        return active.get(key);
    }

    public int size() {
        return snapshots.size();
    }
}
