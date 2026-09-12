package com.trading.execution.gateway;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Separate health/readiness dimensions; health never implies execution readiness.
 *
 * <p><b>Latch policy: {@link #fail(String)} is a one-way, process-lifetime latch.</b> A violation
 * that reaches it — a poison {@code Execution_Intent} row, an intent-reader death — is treated as an
 * operator event. Every dimension is forced false and stays false; no dimension updater can
 * resurrect a flag afterwards, and there is deliberately <b>no</b> {@code recover()} path. The only
 * recovery is a process restart, which is why {@link ExecutionGatewayMain} exits non-zero when the
 * reader dies. This is fail-closed on purpose: the gateway does not get to decide by itself that a
 * violated contract is safe to resume.
 *
 * <p>Note the latch is engaged by {@code fail()} only. A dimension going not-ready and then ready
 * again (fluss down, backlog drained, bridge reconnected) is normal operation and still toggles
 * freely.
 */
public final class GatewayReadiness {
    public record Snapshot(boolean healthy, boolean flussReady, boolean protocolReady,
                           boolean durableWriteReady, String reason) {
        /**
         * P3-300: the reason is rendered in {@code /readyz} and matched by operators, but callers
         * passed {@code e.getMessage()} unguarded — which is null for plenty of exceptions (NPE, any
         * no-arg throwable), producing a Snapshot whose reason serializes ambiguously and NPEs on
         * {@code reason().contains(...)} checks. Normalised once here rather than at every call site.
         */
        public Snapshot {
            if (reason == null || reason.isBlank()) {
                reason = "unknown";
            }
        }

        public boolean executionReady() {
            return healthy && flussReady && protocolReady && durableWriteReady;
        }
    }

    private final AtomicReference<Snapshot> state = new AtomicReference<>(
            new Snapshot(true, false, false, false, "starting"));

    public Snapshot snapshot() { return state.get(); }
    public void fluss(boolean ready, String reason) { update(s -> new Snapshot(s.healthy(), ready,
            s.protocolReady(), s.durableWriteReady(), reason)); }
    public void protocol(boolean ready, String reason) { update(s -> new Snapshot(s.healthy(),
            s.flussReady(), ready, s.durableWriteReady(), reason)); }
    public void durableWrites(boolean ready, String reason) { update(s -> new Snapshot(s.healthy(),
            s.flussReady(), s.protocolReady(), ready, reason)); }
    /**
     * P3-294: atomic full-drain restore. Sets durableWriteReady=true only when
     * {@code inFlight.getAsInt()==0} still holds INSIDE the updateAndGet —
     * the old decrement-then-separate-snapshot-then-set let a concurrent shed
     * lose (or a drainer falsely clear a live backlog).
     */
    public void restoreIfDrained(java.util.function.IntSupplier inFlight, String reason) {
        update(s -> inFlight.getAsInt() == 0 && !s.durableWriteReady()
                ? new Snapshot(s.healthy(), s.flussReady(), s.protocolReady(), true, reason)
                : s);
    }

    /**
     * P3-081/P3-082: latches the gateway HALTED until restart — see the class javadoc.
     *
     * <p>Routed through the same CAS loop as every dimension setter. It used to be a bare
     * {@code state.set(...)}, which raced the dimension updaters' {@code updateAndGet}: a concurrent
     * {@code fluss(true)} could partially resurrect a flag after the fail (healthy=false,
     * flussReady=true) and clobber the fail reason, or the fail could silently drop a concurrent
     * dimension update.
     *
     * <p>The FIRST failure is the one that sticks. Because the latch makes later updates no-ops, a
     * reader that keeps hitting the same poison row cannot overwrite the original cause with a
     * repeat of itself.
     */
    public void fail(String reason) {
        update(s -> new Snapshot(false, false, false, false, reason));
    }

    private void update(java.util.function.UnaryOperator<Snapshot> fn) {
        state.updateAndGet(s -> s.healthy() ? fn.apply(s) : s);
    }
}
