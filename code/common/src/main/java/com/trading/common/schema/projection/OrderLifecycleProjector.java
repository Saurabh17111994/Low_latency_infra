package com.trading.common.schema.projection;

import com.trading.common.model.OrderLifecycleState;
import com.trading.common.schema.KvStateUpdateProtocol;
import java.util.Objects;

/**
 * Lifecycle monotonicity before writing Order_Lifecycle (T6, CHG-045):
 * <ul>
 *   <li>exact duplicate (same version + same content) is a no-op;</li>
 *   <li>older source version is stale evidence (rejected, audited, no write);</li>
 *   <li>equal version with different content is {@link QuarantineReason#LIFECYCLE_CONFLICT};</li>
 *   <li>terminal regression (state moving backward through a reached terminal),
 *       impossible quantity, or unknown status are quarantine + halt.</li>
 * </ul>
 * Deterministic and side-effect free; the driver writes only clean APPLIED
 * snapshots and handles all non-clean outcomes.
 */
public final class OrderLifecycleProjector {

    private OrderLifecycleProjector() {}

    /** End-state order for monotone FILLED progression (UNKNOWN is non-rankable). */
    private static int rank(OrderLifecycleState s) {
        return switch (s) {
            case SUBMITTING -> 1;
            case PENDING -> 2;
            case PARTIAL -> 3;
            case FILLED -> 4;
            case CANCELLED, REJECTED -> 5; // terminal, no forward ordering
            case UNKNOWN -> 0;
        };
    }

    public enum Outcome { APPLIED, DUPLICATE, STALE, CONFLICT, REGRESSION, UNKNOWN }

    public record LifecycleResult(Outcome outcome,
                                  OrderLifecycleSnapshot snapshot,
                                  QuarantineReason reason,
                                  String detail) {

        public static LifecycleResult applied(OrderLifecycleSnapshot s) {
            return new LifecycleResult(Outcome.APPLIED, s, null, null);
        }
        public static LifecycleResult duplicate(OrderLifecycleSnapshot s) {
            return new LifecycleResult(Outcome.DUPLICATE, s, null, null);
        }
        public static LifecycleResult stale(OrderLifecycleSnapshot current) {
            return new LifecycleResult(Outcome.STALE, current, QuarantineReason.STALE_EVENT,
                    "older source version " + current.sourceVersion());
        }
        public static LifecycleResult conflict(OrderLifecycleSnapshot current) {
            return new LifecycleResult(Outcome.CONFLICT, current,
                    QuarantineReason.LIFECYCLE_CONFLICT,
                    "equal source version with different content");
        }
        public static LifecycleResult regression(OrderLifecycleSnapshot current) {
            return new LifecycleResult(Outcome.REGRESSION, current,
                    QuarantineReason.TERMINAL_REGRESSION, "terminal/lifecycle regression");
        }
        public static LifecycleResult unknown(QuarantineReason r, String detail) {
            return new LifecycleResult(Outcome.UNKNOWN, null, r, detail);
        }
    }

    /**
     * Project {@code p} onto {@code current} (null = no row yet). Never mutates.
     * Content equality for duplicate/conflict is keyed on source event id +
     * normalized state + quantities.
     */
    public static LifecycleResult apply(OrderLifecycleSnapshot current, NormalizedPostback p,
            AttemptRef ref, long nowMs) {
        Objects.requireNonNull(p, "postback");
        Objects.requireNonNull(ref, "ref");

        // --- Quantity sanity (impossible quantity) ---------------------------
        if (p.cumulativeQty() < p.pendingQty() || p.cumulativeQty() < 0 || p.pendingQty() < 0) {
            return LifecycleResult.unknown(QuarantineReason.IMPOSSIBLE_QUANTITY,
                    "cumulative " + p.cumulativeQty() + " < pending " + p.pendingQty());
        }
        if (p.fillQty() > p.cumulativeQty()) {
            return LifecycleResult.unknown(QuarantineReason.FILL_OVERRUN,
                    "fill " + p.fillQty() + " exceeds cumulative " + p.cumulativeQty());
        }

        OrderLifecycleState state = parseState(p.orderStatus());
        if (state == null) {
            return LifecycleResult.unknown(QuarantineReason.UNKNOWN_STATUS,
                    "unrecognized order status " + p.orderStatus());
        }

        boolean contentMatches = current != null
                && Objects.equals(current.sourceEventId(), p.sourceEventId())
                && current.normalizedState() == state
                && current.cumulativeQty() == p.cumulativeQty()
                && current.pendingQty() == p.pendingQty();
        // P3-402: same sentinel collision as P3-170 — current==null uses 0L
        // while sourceSequence 0 is legal, so a first row at 0 evaluates
        // (0,0,false)=CONFLICT and quarantines legitimate startup data.
        // A null row can never be a collision.
        if (current == null) {
            return LifecycleResult.applied(new OrderLifecycleSnapshot(
                    ref.accountScopeId(),
                    p.brokerOrderId(),
                    ref.instructionId(),
                    ref.executionAttemptId(),
                    ref.tradeContextId(),
                    state,
                    p.cumulativeQty(),
                    p.pendingQty(),
                    p.isFill() && p.fillPricePaise() > 0 ? p.fillPricePaise() : 0L,
                    p.sourceEventId(),
                    p.sourceSequence(),
                    p.eventTimeMs(),
                    p.receiveTimeMs(),
                    "CORRELATED",
                    com.trading.common.schema.ownership.OrderLifecycleColumns.SCHEMA_VERSION_V2));
        }
        long currentVersion = current.sourceVersion();

        // --- Source-version gate (SCH-09 KvStateUpdateProtocol semantics) ----
        switch (KvStateUpdateProtocol.evaluate(currentVersion, p.sourceSequence(),
                contentMatches)) {
            case DUPLICATE -> { return LifecycleResult.duplicate(current); }
            case STALE, REGRESSION -> { return LifecycleResult.stale(current); }
            case CONFLICT -> { return LifecycleResult.conflict(current); }
            case UNKNOWN -> {
                return LifecycleResult.unknown(QuarantineReason.STALE_EVENT,
                        "negative/ambiguous version " + p.sourceSequence());
            }
            case APPLIED -> { /* fall through to content checks */ }
        }

        // --- Terminal regression check ---------------------------------------
        // P3-403: monotonicity was enforced only when the CURRENT state was
        // terminal — a forward move (PARTIAL->FILLED) carrying a SMALLER
        // cumulative (PARTIAL(5,5)->FILLED(2,0)) passed every branch and wrote
        // a backward quantity. Guard decreasing cumulative generally.
        if (current != null && p.cumulativeQty() < current.cumulativeQty()) {
            return LifecycleResult.regression(current);
        }
        if (current != null && isTerminal(current.normalizedState())) {
            if (state != current.normalizedState()) {
                return LifecycleResult.regression(current);
            }
            if (p.cumulativeQty() < current.cumulativeQty()) {
                return LifecycleResult.regression(current);
            }
        } else if (current != null && rank(state) < rank(current.normalizedState())
                && !isTerminal(state)) {
            return LifecycleResult.regression(current);
        }
        // P3-505: dead branch removed — terminal states sit at max rank 5, so
        // rank(state) < rank(current) can never hold for a terminal state
        // (and a terminal current is handled above). Quantity regressions are
        // covered by the general decreasing-cumulative guard.

        long avgFillPaise = current == null ? 0L : current.averageFillPricePaise();
        if (p.isFill() && p.fillPricePaise() > 0) {
            // P3-404: quantity-weighted average, not last-fill-wins — across
            // legs at different prices the persisted "average" must reflect
            // every leg. Zero-guard for the first fill (no division by zero).
            avgFillPaise = (current == null || current.cumulativeQty() == 0)
                    ? p.fillPricePaise()
                    : (current.cumulativeQty() * current.averageFillPricePaise()
                            + p.fillQty() * p.fillPricePaise())
                            / (current.cumulativeQty() + p.fillQty());
        }
        OrderLifecycleSnapshot next = new OrderLifecycleSnapshot(
                current == null ? ref.accountScopeId() : current.accountScopeId(),
                current == null ? p.brokerOrderId() : current.brokerOrderId(),
                ref.instructionId(),
                ref.executionAttemptId(),
                ref.tradeContextId(),
                state,
                p.cumulativeQty(),
                p.pendingQty(),
                avgFillPaise,
                p.sourceEventId(),
                p.sourceSequence(),
                p.eventTimeMs(),
                p.receiveTimeMs(),
                "CORRELATED",
                com.trading.common.schema.ownership.OrderLifecycleColumns.SCHEMA_VERSION_V2);
        return LifecycleResult.applied(next);
    }

    private static OrderLifecycleState parseState(String s) {
        if (s == null) {
            return null;
        }
        try {
            return OrderLifecycleState.valueOf(s);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    static boolean isTerminal(OrderLifecycleState s) {
        return s == OrderLifecycleState.FILLED
                || s == OrderLifecycleState.CANCELLED
                || s == OrderLifecycleState.REJECTED;
    }
}
