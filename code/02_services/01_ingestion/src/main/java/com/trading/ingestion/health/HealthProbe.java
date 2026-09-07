package com.trading.ingestion.health;

import com.trading.ingestion.write.AppendTracker;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Ingestion health probe: liveness and readiness per the
 * {@code docs/08_implementation/03-ingestion.md} dossier.
 *
 * <h3>Liveness</h3>
 * {@code true} while the process is running and the main loop has not
 * terminated abnormally. Always true during start-up (even if not ready).
 *
 * <h3>Readiness (all must be true)</h3>
 * <ol>
 *   <li>Fluss connection is established and table schema validated</li>
 *   <li>{@link AppendTracker} is ready (not halted, below warning)</li>
 *   <li>Broker connection is subscribed and receiving (recent frame)</li>
 *   <li>Subscription is complete (all manifest instruments subscribed)</li>
 *   <li>Clock offset is within policy (≤2000 ms / 2s, T10) — verified via {@link NtpClockChecker}</li>
 * </ol>
 */
public final class HealthProbe {

    private static final Duration FRAME_STALE_TIMEOUT = Duration.ofSeconds(15);

    private final AtomicBoolean alive = new AtomicBoolean(true);
    private final AppendTracker tracker;
    private final NtpClockChecker clockChecker;

    // readiness dimensions
    private final AtomicBoolean flussReady = new AtomicBoolean(false);
    private final AtomicBoolean brokerConnected = new AtomicBoolean(false);
    private final AtomicBoolean subscriptionComplete = new AtomicBoolean(false);
    private final AtomicBoolean otlpHealthy = new AtomicBoolean(false);
    private final AtomicBoolean memoryBlocked = new AtomicBoolean(false);
    private volatile long lastFrameReceivedNanos;
    private final ConcurrentHashMap<String, SlotHealth> slots = new ConcurrentHashMap<>();

    public static final class SlotHealth {
        public volatile String state = "TERMINAL";
        public volatile int assigned;
        public volatile int acknowledged;
        public volatile int rejected;
        public volatile long lastFrameNanos;
        public volatile long epoch;
        // Safety evidence (plan Amendment): unsafe is true from the first
        // unsafe transition until RECOVERED; unsafeSinceNanos is the
        // monotonic stamp of that first transition (0 while safe);
        // capacityRemaining = connection limit − assigned.
        public volatile boolean unsafe;
        public volatile long unsafeSinceNanos;
        public volatile long capacityRemaining;
    }

    /**
     * @param tracker       backpressure tracker for append health
     * @param clockChecker  NTP clock offset checker; may be null (clock check skipped)
     */
    public HealthProbe(AppendTracker tracker, NtpClockChecker clockChecker) {
        this.tracker = tracker;
        this.clockChecker = clockChecker;
    }

    /** Back-compat constructor — no clock checking. */
    public HealthProbe(AppendTracker tracker) {
        this(tracker, null);
    }

    // ---- liveness ----

    public boolean isAlive() { return alive.get(); }

    /** Called from a shutdown hook — marks the process as not-alive. */
    public void markNotAlive() { alive.set(false); }

    // ---- readiness setters (called by IngestionService) ----

    public void setFlussReady(boolean ready) { this.flussReady.set(ready); }
    public void setBrokerConnected(boolean connected) { this.brokerConnected.set(connected); }
    public boolean isBrokerConnected() { return this.brokerConnected.get(); }
    public void setSubscriptionComplete(boolean complete) { this.subscriptionComplete.set(complete); }
    public boolean isSubscriptionComplete() { return this.subscriptionComplete.get(); }
    public void setLastFrameReceived(long nanoTime) {
        this.lastFrameReceivedNanos = nanoTime;
        // P1-081: no per-slot fan-out here — one slot's frames must never
        // refresh another slot's recency (a frame from A kept silent B fresh
        // forever, so isDataReady() stayed true with B missing). Per-slot
        // recency advances only via setSlotFrameReceived(), called per tick
        // with the tick's own slotId by IngestionService.processTickEvent
        // (this preserves the R-031 steady-state property truthfully).
    }

    /**
     * P1-081: record arrival evidence for ONE slot. Called per tick with the
     * tick's own slotId — never fanned out across slots. Only touches an
     * already-tracked slot: a tick for an unknown slot must not plant a
     * TERMINAL entry that would veto readiness (the lifecycle event creates
     * the slot via updateSlot).
     */
    public void setSlotFrameReceived(String slotId, long nanoTime) {
        SlotHealth slot = slots.get(slotId);
        if (slot != null) {
            slot.lastFrameNanos = nanoTime;
        }
    }

    /** OTLP collector reachability + last export success (plan: telemetry readiness). */
    public void setOtlpHealthy(boolean healthy) { this.otlpHealthy.set(healthy); }

    /**
     * JVM/container memory readiness gate (09-production-swarm § JVM and
     * memory configuration + AlertThresholds.CONTAINER_MEMORY): when
     * {@code true}, the probe refuses readiness so a container sustained at or
     * above the 85% alert threshold stops being a live-data source instead of
     * OOM-ing mid-flight. Set by the JVM heap monitor once the breach is
     * sustained (never on a transient spike); cleared only on a sustained
     * recovery below the hysteresis setpoint.
     */
    public void setMemoryBlocked(boolean blocked) { this.memoryBlocked.set(blocked); }

    /** Memory gate open (not blocked) — true unless the heap monitor declared a sustained breach. */
    public boolean isMemoryReady() { return !memoryBlocked.get(); }

    /**
     * Telemetry readiness: the OTLP collector is reachable and the most recent
     * export succeeded. Required for live-money release readiness, not for data
     * ingestion container health.
     */
    public boolean isTelemetryReady() { return otlpHealthy.get(); }

    public SlotHealth slot(String slotId) { return slots.computeIfAbsent(slotId, ignored -> new SlotHealth()); }

    /** Slots currently tracked (used to fan safety evidence across slots). */
    public java.util.Set<String> slotIds() { return java.util.Set.copyOf(slots.keySet()); }

    /**
     * Slot safety evidence (plan Amendment). The first safe→unsafe transition
     * stamps {@code unsafeSinceNanos} (monotonic); re-emissions of the same
     * unsafe state keep the original stamp so the unsafe-duration gauge does
     * not reset. {@code false} clears both — a slot is safe again only via a
     * RECOVERED transition.
     */
    public void setSlotUnsafe(String slotId, boolean unsafe) {
        SlotHealth slot = slot(slotId);
        // P1-082: stamp + flag written atomically (diagnostics reads both).
        synchronized (slot) {
            if (unsafe && !slot.unsafe) {
                slot.unsafeSinceNanos = System.nanoTime();
            }
            if (!unsafe) {
                slot.unsafeSinceNanos = 0;
            }
            slot.unsafe = unsafe;
        }
    }

    /** Remaining subscription capacity (connection limit − assigned). */
    public void setSlotCapacityRemaining(String slotId, long remaining) {
        slot(slotId).capacityRemaining = remaining;
    }

    /**
     * Reset every tracked slot to AUTHENTICATING with zero coverage — used when
     * a fresh bridge process starts (plan: reset all slot states on restart).
     * Safety evidence is deliberately NOT reset here: an unsafe slot stays
     * unsafe until a RECOVERED transition (full ACTIVE ack on a strictly
     * greater epoch), so a bridge restart cannot silently clear the flag.
     * P1-245: epoch + capacityRemaining ARE reset (generation-unknown, no
     * headroom claimed) — otherwise diagnostics mix old epoch (e.g. 5) with
     * new AUTHENTICATING state and report stale headroom until the next
     * bridge event overwrites them.
     */
    public void resetSlotsToAuthenticating() {
        slots.forEach((id, slot) -> {
            // P1-082: same tear class as updateSlot — compound write under lock.
            synchronized (slot) {
                slot.state = "AUTHENTICATING";
                slot.assigned = 0;
                slot.acknowledged = 0;
                slot.rejected = 0;
                slot.lastFrameNanos = 0;
                slot.epoch = 0;
                slot.capacityRemaining = 0;
                // unsafe/unsafeSinceNanos intentionally retained: only RECOVERED clears.
            }
        });
    }

    public void updateSlot(String slotId, String state, long epoch, int assigned, int acknowledged, int rejected, long frameNanos) {
        SlotHealth slot = slot(slotId);
        // P1-082: compound write under the per-slot lock — a concurrent
        // isDataReady()/diagnostics() reader must never see new assigned
        // with old acknowledged (false assigned!=acknowledged flap).
        synchronized (slot) {
            slot.state = state; slot.epoch = epoch; slot.assigned = assigned;
            slot.acknowledged = acknowledged; slot.rejected = rejected;
            if (frameNanos > 0) slot.lastFrameNanos = frameNanos;
        }
    }

    public boolean isDataReady() {
        if (slots.isEmpty()) return false;
        // P1-082: snapshot each slot under its lock before judging —
        // worst-slot-wins (Batch-3 #29), but on values from one write.
        return slots.values().stream().allMatch(slot -> {
            String state;
            int assigned, acknowledged, rejected;
            long frameNanos;
            synchronized (slot) {
                state = slot.state;
                assigned = slot.assigned;
                acknowledged = slot.acknowledged;
                rejected = slot.rejected;
                frameNanos = slot.lastFrameNanos;
            }
            return "ACTIVE".equals(state)
                    && assigned == acknowledged && rejected == 0
                    && frameNanos > 0
                    && System.nanoTime() - frameNanos < FRAME_STALE_TIMEOUT.toNanos();
        });
    }

    // ---- readiness ----

    public boolean isReady() {
        return alive.get()
                && flussReady.get()
                && tracker.isReady()
                && brokerConnected.get()
                && subscriptionComplete.get()
                && !memoryBlocked.get()
                && isDataReady()
                && isFrameRecent()
                && isClockOk();
    }

    private boolean isFrameRecent() {
        // R-178: System.nanoTime() has an arbitrary boot-time origin; 0 means
        // "no frame ever received". A probe that has never seen a frame must
        // not be reported as 'recent' merely because nanoTime() is still small.
        if (lastFrameReceivedNanos == 0) return false;
        long ago = System.nanoTime() - lastFrameReceivedNanos;
        return ago < FRAME_STALE_TIMEOUT.toNanos();
    }

    private boolean isClockOk() {
        if (clockChecker == null) return true; // no checker configured
        return clockChecker.isWithinLimit();
    }

    // ---- diagnostics ----

    /** Returns a human-readable readiness breakdown for logging/debugging. */
    public Map<String, Object> diagnostics() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("alive", alive.get());
        m.put("fluss_ready", flussReady.get());
        m.put("tracker_ready", tracker.isReady());
        m.put("tracker_pending_records", tracker.pendingRecords());
        m.put("tracker_pending_bytes", tracker.pendingBytes());
        m.put("tracker_halted", tracker.isHalted());
        m.put("broker_connected", brokerConnected.get());
        m.put("subscription_complete", subscriptionComplete.get());
        m.put("telemetry_ready", otlpHealthy.get());
        m.put("memory_blocked", memoryBlocked.get());
        m.put("memory_ready", !memoryBlocked.get());
        m.put("frame_recent", isFrameRecent());
        // P1-248: one atomic snapshot read — offset and ok always come from
        // the SAME check (the writer publishes complete records; two
        // separate getter calls could straddle an update and disagree).
        NtpClockChecker.ClockSnapshot clockSnap =
                clockChecker != null ? clockChecker.snapshot() : null;
        long offsetMs = clockSnap != null ? clockSnap.offsetMs() : 0;
        boolean ok = clockSnap != null ? clockSnap.passed() : true;
        m.put("clock_offset_ms", offsetMs);
        m.put("clock_ok", ok);
        m.put("ready", isReady());
        Map<String, Object> slotDiagnostics = new LinkedHashMap<>();
        slots.forEach((id, slot) -> {
            // P1-082: read the slot's fields as one snapshot under its lock.
            String state; long epoch; int assigned, acknowledged, rejected;
            long frameNanos; boolean unsafe; long unsafeSinceNanos; long capacityRemaining;
            synchronized (slot) {
                state = slot.state; epoch = slot.epoch;
                assigned = slot.assigned; acknowledged = slot.acknowledged;
                rejected = slot.rejected; frameNanos = slot.lastFrameNanos;
                unsafe = slot.unsafe; unsafeSinceNanos = slot.unsafeSinceNanos;
                capacityRemaining = slot.capacityRemaining;
            }
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("state", state); values.put("epoch", epoch);
            values.put("assigned", assigned); values.put("acknowledged", acknowledged);
            values.put("rejected", rejected);
            values.put("frame_age_ms", frameNanos == 0 ? -1 :
                    Duration.ofNanos(Math.max(0, System.nanoTime() - frameNanos)).toMillis());
            values.put("unsafe", unsafe);
            values.put("unsafe_duration_ms", unsafeSinceNanos == 0 ? 0 :
                    Duration.ofNanos(Math.max(0, System.nanoTime() - unsafeSinceNanos)).toMillis());
            values.put("capacity_remaining", capacityRemaining);
            slotDiagnostics.put(id, values);
        });
        m.put("slots", slotDiagnostics);
        m.put("data_ready", isDataReady());
        return m;
    }
}
