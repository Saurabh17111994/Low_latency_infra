package com.trading.execution.gateway;

import com.trading.common.model.OrderLifecycleState;
import com.trading.common.schema.ownership.OrderLifecycleColumns;
import com.trading.common.schema.projection.AttemptRef;
import com.trading.common.schema.projection.NormalizedPostback;
import com.trading.common.schema.projection.OrderLifecycleProjector;
import com.trading.common.schema.projection.OrderLifecycleSnapshot;

import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * M1-6: the monotonic read-evaluate-write gate in front of {@code Order_Lifecycle}.
 *
 * <p>The old gateway path blind-upserted every lifecycle event: a replayed pre-cancel ack could
 * regress a CANCELED row, and an equal-version/different-content pair silently overwrote state.
 * This gate reads the stored row for the full composite key {@code (account_scope_id,
 * broker_order_id)} (DDL 09 key discipline — never broker_order_id alone), applies
 * {@link OrderLifecycleProjector}, and acts on the outcome:
 *
 * <ul>
 *   <li>{@code APPLIED} → the projected snapshot is upserted (the projector's weighted average,
 *       not the raw event);</li>
 *   <li>{@code DUPLICATE} → no write, no noise (a replayed or resumed event is idempotent);</li>
 *   <li>{@code STALE} → no write; the stale evidence is surfaced to the caller's audit sink;</li>
 *   <li>{@code CONFLICT} / {@code REGRESSION} / {@code UNKNOWN} → Postback_Quarantine append and a
 *       partition halt (epoch+1) — the fail-closed direction; a conflict never overwrites and
 *       never disappears.</li>
 * </ul>
 *
 * <p>Read-evaluate-write is serialized per composite key (striped locks) so two concurrent events
 * for the same order cannot lost-update. A lookup failure propagates unchanged: the writer must
 * refuse the write (the HTTP layer answers 503) rather than fall back to a blind upsert.
 *
 * <p>A halt sink that is absent (tests, warm-up writers) is loud but not fatal: the conflict is
 * still quarantined and nothing is written; production always wires the durable gate store.
 */
final class OrderLifecycleWriteGate {

    /** Bounded stripe count: one stable lock per composite key, no per-key map for the process life. */
    static final int STRIPE_COUNT = 64;

    /** Reads the stored {@code Order_Lifecycle} row; {@code null} = no row yet. Failures propagate. */
    @FunctionalInterface
    interface StoredRowLookup {
        OrderLifecycleSnapshot lookup(String accountScopeId, String brokerOrderId) throws Exception;
    }

    /** Durable partition halt (epoch+1) for a fail-closed lifecycle outcome. */
    @FunctionalInterface
    interface PartitionHalt {
        void halt(String partitionId, String reason, String evidenceHash);
    }

    /** APPLIED → the caller upserts this snapshot (the projector's projected row). */
    @FunctionalInterface
    interface AppliedSink {
        void upsert(OrderLifecycleSnapshot next) throws Exception;
    }

    /** STALE → the caller records the rejected evidence; never a write. */
    @FunctionalInterface
    interface StaleSink {
        void stale(NormalizedExecutionEvent event, String detail);
    }

    /** CONFLICT/REGRESSION/UNKNOWN → the caller appends immutable quarantine evidence. */
    @FunctionalInterface
    interface QuarantineSink {
        void quarantine(NormalizedExecutionEvent event, String reason) throws Exception;
    }

    private final StoredRowLookup lookup;
    private final PartitionHalt halt;
    private final AppliedSink applied;
    private final StaleSink stale;
    private final QuarantineSink quarantine;
    private final LongSupplier nowMs;
    private final Lock[] stripes = newStripes();

    OrderLifecycleWriteGate(StoredRowLookup lookup, PartitionHalt halt, AppliedSink applied,
                            StaleSink stale, QuarantineSink quarantine, LongSupplier nowMs) {
        this.lookup = Objects.requireNonNull(lookup, "lookup");
        this.halt = halt;
        this.applied = Objects.requireNonNull(applied, "applied");
        this.stale = Objects.requireNonNull(stale, "stale");
        this.quarantine = Objects.requireNonNull(quarantine, "quarantine");
        this.nowMs = Objects.requireNonNull(nowMs, "nowMs");
    }

    private static Lock[] newStripes() {
        Lock[] locks = new Lock[STRIPE_COUNT];
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new ReentrantLock();
        }
        return locks;
    }

    /** Package-visible seam for the ordering test: one stable stripe per composite key. */
    Lock stripeFor(String accountScopeId, String brokerOrderId) {
        return stripes[Math.floorMod((accountScopeId + "|" + brokerOrderId).hashCode(), stripes.length)];
    }

    void apply(NormalizedExecutionEvent event) throws Exception {
        NormalizedExecutionEvent.Lifecycle lifecycle = event.lifecycle();
        if (lifecycle == null) {
            return; // the caller gates on this too; never a blind write from a lifecycle-less event
        }
        if (lifecycle.brokerOrderId() == null || lifecycle.brokerOrderId().isBlank()) {
            throw new IllegalStateException("Order_Lifecycle write without broker_order_id");
        }
        if (lifecycle.instructionId() == null || lifecycle.instructionId().isBlank()) {
            failClosed(event, "Order_Lifecycle write without instruction_id");
            return;
        }
        Lock stripe = stripeFor(event.accountScopeId(), lifecycle.brokerOrderId());
        stripe.lock();
        try {
            // Read first: a lookup failure propagates (503), it never falls through to a write.
            OrderLifecycleSnapshot current = lookup.lookup(event.accountScopeId(), lifecycle.brokerOrderId());
            OrderLifecycleState state = canonicalState(lifecycle.normalizedState());
            if (state == null) {
                failClosed(event, "unrecognized lifecycle state " + lifecycle.normalizedState());
                return;
            }
            NormalizedPostback postback = toPostback(event, lifecycle, state);
            AttemptRef ref = new AttemptRef(event.accountScopeId(), lifecycle.instructionId(),
                    lifecycle.executionAttemptId(), lifecycle.tradeContextId());
            OrderLifecycleProjector.LifecycleResult result =
                    OrderLifecycleProjector.apply(current, postback, ref, nowMs.getAsLong());
            switch (result.outcome()) {
                case APPLIED -> applied.upsert(result.snapshot());
                case DUPLICATE -> { /* exact replay: no write, no noise */ }
                case STALE -> stale.stale(event, result.detail());
                default -> failClosed(event, describe(result));
            }
        } finally {
            stripe.unlock();
        }
    }

    private void failClosed(NormalizedExecutionEvent event, String reason) throws Exception {
        quarantine.quarantine(event, reason);
        if (halt != null) {
            String evidence = event.audit() != null && event.audit().evidenceHash() != null
                    ? event.audit().evidenceHash() : event.postbackEventId();
            halt.halt(event.executionPartitionId(), reason, evidence);
        } else {
            java.util.logging.Logger.getLogger(OrderLifecycleWriteGate.class.getName()).severe(
                    "Order_Lifecycle conflict without a halt sink (no GateStateStore wired): " + reason);
        }
    }

    private static String describe(OrderLifecycleProjector.LifecycleResult result) {
        StringBuilder detail = new StringBuilder("Order_Lifecycle ").append(result.outcome());
        if (result.reason() != null) {
            detail.append(" (").append(result.reason()).append(')');
        }
        if (result.detail() != null) {
            detail.append(": ").append(result.detail());
        }
        return detail.toString();
    }

    private static NormalizedPostback toPostback(NormalizedExecutionEvent event,
            NormalizedExecutionEvent.Lifecycle lifecycle, OrderLifecycleState state) {
        String fingerprint = event.fill() != null && event.fill().postbackFingerprint() != null
                ? event.fill().postbackFingerprint() : event.postbackEventId();
        String fingerprintVersion = event.fill() != null && event.fill().fingerprintVersion() != null
                ? event.fill().fingerprintVersion() : OrderLifecycleColumns.SCHEMA_VERSION_V2;
        long eventTime = lifecycle.sourceEventTime() != null
                ? lifecycle.sourceEventTime() : lifecycle.lastReceiveTime();
        return new NormalizedPostback(
                event.postbackEventId(),
                event.postbackEventId(),
                lifecycle.sourceVersion(),
                fingerprint,
                fingerprintVersion,
                lifecycle.brokerOrderId(),
                event.correlation() != null ? event.correlation().clientOrderRef() : null,
                event.accountScopeId(),
                0L,
                null,
                null,
                null,
                state.name(),
                lifecycle.cumulativeQty(),
                lifecycle.pendingQty(),
                0L,
                0L,
                eventTime,
                lifecycle.lastReceiveTime(),
                OrderLifecycleColumns.SCHEMA_VERSION_V2,
                null,
                lifecycle.tradeContextId());
    }

    /**
     * The executor's wire vocabulary is not the closed {@link OrderLifecycleState} set (the sync
     * ack defaults to {@code ACCEPTED}, Arrow says {@code CANCELED}/{@code COMPLETE}). Normalize
     * here, once, before evaluation; an unmapped word returns {@code null} and the caller
     * quarantines + halts — fail closed on vocabulary drift, never a silent default.
     */
    static OrderLifecycleState canonicalState(String raw) {
        if (raw == null) {
            return null;
        }
        return switch (raw.trim().toUpperCase(Locale.ROOT)) {
            case "SUBMITTING" -> OrderLifecycleState.SUBMITTING;
            case "PENDING", "ACCEPTED", "NEW", "NEWACK", "NEW_ACK", "PENDING_NEW", "PENDINGNEW", "OPEN" ->
                    OrderLifecycleState.PENDING;
            case "PARTIAL", "PARTIALLY_FILLED", "PARTIALLYFILLED" -> OrderLifecycleState.PARTIAL;
            case "FILLED", "COMPLETE", "COMPLETED" -> OrderLifecycleState.FILLED;
            case "CANCELLED", "CANCELED" -> OrderLifecycleState.CANCELLED;
            case "REJECTED" -> OrderLifecycleState.REJECTED;
            case "UNKNOWN" -> OrderLifecycleState.UNKNOWN;
            default -> null;
        };
    }
}
