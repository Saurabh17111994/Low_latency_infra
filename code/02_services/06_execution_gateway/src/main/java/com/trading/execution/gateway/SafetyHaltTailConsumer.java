package com.trading.execution.gateway;

import com.trading.common.schema.execution.GateStateStore;
import com.trading.common.schema.execution.SafetyHaltRequest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.fluss.row.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * H1-1: the durable {@code Safety_Halt_Requests} consumer.
 *
 * <p>The gateway's in-process halt flag is lost on restart, and the executor's durable HALT
 * report (H2-5/D2) can land while the gateway is down or before it is wired. This consumer is the
 * missing reader: every pass scans the halt table, applies the rows that are still {@code OPEN},
 * and records the outcome on the row itself ({@code application_result}/{@code applied_ts}), so
 * the replay is idempotent across restarts with no offset state to lose.
 *
 * <p>Contract:
 * <ul>
 *   <li><b>Boot replay is synchronous and throwing.</b> {@code main} calls {@link #replayOnce()}
 *       before readiness claims the gateway usable; a store failure refuses startup rather than
 *       leaving a gateway that would forward orders past a durable halt.</li>
 *   <li><b>Gate first, audit second.</b> A row's halt is applied through
 *       {@link SafetyHaltTailProcessor} (durable epoch +1), then the application result is
 *       written. A crash between the two re-applies on restart — the halt path is idempotent, so
 *       the safe direction is chosen deliberately.</li>
 *   <li><b>Only {@code UNSAFE} halts.</b> A {@code RECOVERED} row is audited with no gate
 *       effect and never auto-enables anything; only the operator's DEC-044 approval can.</li>
 *   <li><b>Greatest per-source epoch wins.</b> The max is rebuilt on every pass from rows already
 *       {@code APPLIED} (restart-safe without offsets) and advanced by rows applied in the same
 *       pass; a lower epoch is rejected as stale. Candidates are processed greatest-first so a
 *       same-batch stale halt does not bump the gate epoch after its successor halted.</li>
 *   <li><b>Scope is account AND partition.</b> The processor rejects account-scope mismatches
 *       against the gate row; this class additionally refuses every halt not addressed to this
 *       gateway's own {@code execution_partition_id} — a foreign partition's halt must not touch
 *       this gate.</li>
 *   <li><b>Death is loud.</b> Any throw out of a poll pass stops the loop, fails readiness, and
 *       the caller exits non-zero.</li>
 * </ul>
 */
public final class SafetyHaltTailConsumer implements AutoCloseable {

    /** Default poll period when the caller does not supply {@code SAFETY_HALT_POLL_MS}. */
    public static final long DEFAULT_POLL_MS = 1000L;

    private static final Logger LOG = LoggerFactory.getLogger(SafetyHaltTailConsumer.class);

    private final ControlStateStore controls;
    private final String executionPartitionId;
    private final long pollMs;
    private final Consumer<Throwable> onDeath;
    private final SafetyHaltTailProcessor processor;
    private final AtomicLong applied = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();
    private final AtomicLong deferred = new AtomicLong();
    private final AtomicLong recovered = new AtomicLong();
    private final AtomicLong invalid = new AtomicLong();
    private volatile boolean running;
    private volatile Thread thread;
    private volatile Throwable failure;

    /** One still-OPEN row and its decoded request. */
    private record Candidate(InternalRow row, SafetyHaltRequest request) {}

    /**
     * @param onDeath called once on the poll thread when a pass throws (before the loop stops);
     *     null is allowed for tests and offline use, but the service always supplies one — an
     *     unobserved dead consumer is the failure this class exists to prevent.
     */
    public SafetyHaltTailConsumer(
            ControlStateStore controls,
            GateStateStore gates,
            String executionPartitionId,
            long pollMs,
            Consumer<Throwable> onDeath) {
        this.controls = Objects.requireNonNull(controls, "controls");
        Objects.requireNonNull(gates, "gates");
        this.executionPartitionId = Objects.requireNonNull(executionPartitionId, "executionPartitionId");
        if (pollMs <= 0) {
            throw new IllegalArgumentException("pollMs must be positive");
        }
        this.pollMs = pollMs;
        this.onDeath = onDeath;
        this.processor = new SafetyHaltTailProcessor(gates);
    }

    /**
     * One full replay+apply pass. Throws on any store failure; the boot caller lets that refuse
     * startup and the poll loop treats it as consumer death.
     */
    public void replayOnce() {
        List<Candidate> candidates = new ArrayList<>();
        Map<String, Long> maxEpochBySource = new HashMap<>();
        controls.replaySafetyHalts(row -> {
            final SafetyHaltTailProcessor.Decoded decoded;
            try {
                decoded = SafetyHaltTailProcessor.decodeWithApplication(row);
            } catch (RuntimeException e) {
                invalid.incrementAndGet();
                LOG.warn("safety halt row undecodable; left untouched (INVALID)", e);
                return;
            }
            String result = decoded.applicationResult();
            if (SafetyHaltTailProcessor.RESULT_APPLIED.equals(result)) {
                // Rebuild the per-source-instance epoch fence from durable truth on every pass.
                // Only this gateway's partition counts: a sibling gateway's APPLIED rows share
                // the table but must not fence our epoch stream (the partition dimension of the
                // dossier's (source_component, source_instance, execution_partition_id) key).
                if (executionPartitionId.equals(decoded.request().executionPartitionId())) {
                    bumpMax(maxEpochBySource, decoded.request());
                }
                return;
            }
            if (!SafetyHaltTailProcessor.RESULT_OPEN.equals(result)) {
                return; // REJECTED / EXPIRED: terminal, never re-applied
            }
            candidates.add(new Candidate(row, decoded.request()));
        });
        candidates.sort(Comparator.comparingLong((Candidate c) -> c.request().sourceEpoch()).reversed());
        for (Candidate candidate : candidates) {
            apply(candidate, maxEpochBySource);
        }
    }

    private void apply(Candidate candidate, Map<String, Long> maxEpochBySource) {
        SafetyHaltRequest req = candidate.request();
        // H1-1 scope: only halts addressed to THIS gateway's partition may touch its gate. The
        // account-scope comparison happens in the processor against the gate row; the partition
        // comparison must happen here because the request names the partition it addresses.
        if (req.executionPartitionId() == null
                || !req.executionPartitionId().equals(executionPartitionId)) {
            rejected.incrementAndGet();
            record(candidate.row(), SafetyHaltTailProcessor.RESULT_REJECTED);
            LOG.warn("safety halt {} rejected: partition {} is not this gateway's {}",
                    req.haltRequestId(), req.executionPartitionId(), executionPartitionId);
            return;
        }
        if (!SafetyHaltTailProcessor.STATE_UNSAFE.equals(req.state())) {
            // RECOVERED (or any other non-UNSAFE state): audited, no gate effect, never
            // auto-enables. The state column keeps the distinction visible in the row.
            recovered.incrementAndGet();
            record(candidate.row(), SafetyHaltTailProcessor.RESULT_APPLIED);
            bumpMax(maxEpochBySource, req);
            return;
        }
        Long max = maxEpochBySource.get(epochKey(req));
        if (max != null && req.sourceEpoch() < max) {
            rejected.incrementAndGet();
            record(candidate.row(), SafetyHaltTailProcessor.RESULT_REJECTED);
            LOG.warn("safety halt {} rejected: source {} epoch {} is stale (max {})",
                    req.haltRequestId(), epochKey(req), req.sourceEpoch(), max);
            return;
        }
        SafetyHaltTailProcessor.ApplyResult result = processor.apply(req, System.currentTimeMillis());
        switch (result) {
            case APPLIED -> {
                applied.incrementAndGet();
                record(candidate.row(), SafetyHaltTailProcessor.RESULT_APPLIED);
                bumpMax(maxEpochBySource, req);
            }
            case DUPLICATE -> {
                // The gate already halted in this process; only the audit is missing (a crash
                // between the halt and the write, or a same-process re-scan). Never a second halt.
                record(candidate.row(), SafetyHaltTailProcessor.RESULT_APPLIED);
                bumpMax(maxEpochBySource, req);
            }
            case CROSS_SCOPE_REJECT -> {
                rejected.incrementAndGet();
                record(candidate.row(), SafetyHaltTailProcessor.RESULT_REJECTED);
                LOG.warn("safety halt {} rejected: account scope does not match the gate row",
                        req.haltRequestId());
            }
            case NOT_FOUND -> {
                // The gate row (or partition) is not readable right now. Leave the row OPEN so
                // the next pass retries — never choke a real halt on a missing row.
                deferred.incrementAndGet();
                LOG.warn("safety halt {} deferred: gate row for partition {} not readable",
                        req.haltRequestId(), req.executionPartitionId());
            }
            case INVALID_ID -> {
                invalid.incrementAndGet();
                LOG.warn("safety halt {} not applied: id invalid", req.haltRequestId());
            }
        }
    }

    /** Gate first, audit second (H1-1); a throw here is consumer death by design. */
    private void record(InternalRow row, String result) {
        controls.recordApplication(row, result, System.currentTimeMillis());
    }

    private static void bumpMax(Map<String, Long> maxEpochBySource, SafetyHaltRequest req) {
        maxEpochBySource.merge(epochKey(req), req.sourceEpoch(), Math::max);
    }

    /**
     * The dossier's epoch-fence key: per source component + instance (the partition dimension is
     * enforced by the scope check, since only this gateway's rows reach the fence).
     */
    private static String epochKey(SafetyHaltRequest req) {
        return req.sourceComponent() + "|" + Objects.toString(req.sourceInstance(), "");
    }

    /** Starts the daemon poll thread. The boot replay must have run first. */
    public Thread start() {
        if (thread != null) {
            throw new IllegalStateException("safety halt consumer already started");
        }
        running = true;
        Thread t = new Thread(this::pollLoop, "execution-halt-consumer");
        t.setDaemon(true);
        thread = t;
        t.start();
        return t;
    }

    /** True once the loop has died from a failure (readiness + exit are the caller's job). */
    public boolean failed() {
        return failure != null;
    }

    /** The failure that stopped the loop, or null while healthy. */
    public Throwable failure() {
        return failure;
    }

    private void pollLoop() {
        while (running) {
            try {
                replayOnce();
            } catch (Throwable t) {
                // H1-1: a dead consumer must be loud. The forward leg would otherwise happily
                // forward intents while a durable halt sits unapplied in the table.
                failure = t;
                running = false;
                LOG.error("safety halt consumer stopped", t);
                if (onDeath != null) {
                    onDeath.accept(t);
                }
                return;
            }
            try {
                Thread.sleep(pollMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @Override
    public void close() {
        running = false;
        Thread t = thread;
        if (t == null) {
            return;
        }
        t.interrupt();
        try {
            t.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (t.isAlive()) {
            LOG.warn("safety halt consumer did not stop within 5000ms");
        }
    }

    public long appliedCount() {
        return applied.get();
    }

    public long rejectedCount() {
        return rejected.get();
    }

    public long deferredCount() {
        return deferred.get();
    }

    public long recoveredCount() {
        return recovered.get();
    }

    public long invalidCount() {
        return invalid.get();
    }
}
