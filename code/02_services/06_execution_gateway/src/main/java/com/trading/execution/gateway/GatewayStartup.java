package com.trading.execution.gateway;

import com.trading.common.model.GateState;
import com.trading.common.schema.execution.FlussAttemptStore;
import com.trading.common.schema.execution.FlussGateStateStore;
import com.trading.common.schema.execution.GateRow;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testable startup seam for {@link ExecutionGatewayMain}.
 *
 * <p>Extracted so the startup sequence can be pinned by a test without a Fluss cluster: opening the
 * durable stores, probing the durable authority tables, and applying the readiness wiring for the
 * enabled/disabled branch.
 */
public final class GatewayStartup {
    private static final Logger LOG = LoggerFactory.getLogger(GatewayStartup.class);

    private GatewayStartup() {}

    /**
     * The stores the running process actually uses, held open for its lifetime and closed in
     * reverse open order.
     *
     * <p>H2-5/D2 (CHG-334): the gate store is now held and authoritative — the {@code /v1/gate}
     * report endpoint writes the durable row through it, and the row is created HALTED here at
     * startup (before the intent replay) so the forward leg's {@code NOT_FOUND} can no longer
     * flip {@code flussReady=false} for the process lifetime (CHG-331 attempt 3). The attempt
     * store is still probe-only: no code path reads it.
     */
    public record Stores(FlussGateStateStore gates,
                         FlussControlStateStore controls,
                         FlussProjectionWriter projections,
                         FlussProjectionLedgerStore ledger) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            ledger.close();
            projections.close();
            controls.close();
            gates.close();
        }
    }

    /**
     * Opens the durable Fluss stores. The only place the running gateway talks to Fluss, and the
     * single point the executionEnabled gate has to precede to keep a disabled gateway offline
     * (P3-060).
     *
     * <p>The gate row is created here if absent and left untouched if present
     * ({@code init} never clobbers a fenced gate, P3-151) — a previously ENABLED row is
     * transitioned to HALTED by the executor's boot report, not by the gateway.
     */
    public static Stores openStores(GatewayConfig config) throws Exception {
        FlussGateStateStore gates = FlussGateStateStore.open(
                config.flussBootstrap(), config.flussDatabase(), config.gateTable(),
                config.requestTimeout(), Set.of(GatewayHttpServer.SINGLE_OPERATOR));
        gates.init(bootGateRow(config, System.currentTimeMillis()));
        return new Stores(
                gates,
                FlussControlStateStore.open(config),
                FlussProjectionWriter.open(config),
                FlussProjectionLedgerStore.open(config));
    }

    /**
     * The HALTED, unfenced row created at startup. Epoch 1, no owner, fence token 0 (never
     * fenced) — it authorizes nothing; only a reported approval can enable it. Evidence is null
     * on purpose: the first approval defines the binding (P3-151). Epoch 1 rather than 0: the
     * executor's gate model rejects 0 as a sentinel ("no term declared"), so the durable
     * generation starts at 1 and the executor adopts it at boot.
     */
    static GateRow bootGateRow(GatewayConfig config, long nowTs) {
        return new GateRow(config.executionPartitionId(), config.accountScopeId(), GateState.HALTED,
                1L, "gateway boot", null, null, null, null, null, 0L, null, null, null, nowTs, null);
    }

    /**
     * Fail-fast existence probe for the durable gate/attempt backplane (WP-3).
     *
     * <p>Open fails fast if Execution_Gate / Execution_Attempts (v3 DDL) are absent or unreachable,
     * so the gateway is never "ready" without its durable authority tables. The handles are then
     * closed: crash-window zero-duplicate depends on {@code IntentReader}'s dedup store and
     * {@code NautilusIntentClient}'s controls lookup, not on these connections, so retaining them
     * for the process lifetime bought nothing (P3-275).
     */
    public static void probeAuthorityTables(GatewayConfig config) throws Exception {
        try (FlussGateStateStore gates = FlussGateStateStore.open(
                config.flussBootstrap(), config.flussDatabase(), config.gateTable(),
                config.requestTimeout(), Set.of("saurabh"));
                FlussAttemptStore attempts = FlussAttemptStore.open(
                        config.flussBootstrap(), config.flussDatabase(), config.attemptsTable(),
                        config.requestTimeout(),
                        () -> LOG.warn(
                                "execution attempt contract violation -> request a safety halt"))) {
            // Opened as an existence probe only; nothing is read from them here.
            LOG.info("durable authority tables present: gate={} attempts={}",
                    config.gateTable(), config.attemptsTable());
        }
    }

    /**
     * C1: warms the projection tables before readiness is reported, so the first request-path
     * write does not pay the post-CREATE window on a user request.
     *
     * <p>Failure here fails startup: a gateway that cannot complete a row-free handle warm-up
     * cannot serve, and advertising ready for it is the same class of lie {@code /control/approve}
     * used to tell. Opens its own short-lived writer on the {@code probeAuthorityTables}
     * pattern - retaining it for the process lifetime would buy nothing, since the request path
     * opens its own (P3-275).
     */
    public static void prewarmTables(GatewayConfig config) throws Exception {
        try (FlussProjectionWriter writer = FlussProjectionWriter.open(config)) {
            writer.prewarm();
        }
        LOG.info("projection tables pre-warmed; the first request-path write is no longer the "
                + "one that pays the post-CREATE window (C1)");
    }

    /**
     * Applies the startup readiness wiring for the enabled/disabled branch.
     *
     * <p>The enabled branch reports ONE fluss dimension for both the table open and the authority
     * probe — previously it called {@code fluss(true, ...)} twice, and since a dimension update
     * replaces the shared reason the operator only ever saw the second (P3-479). The disabled
     * branch keeps every dimension false so {@code executionReady()} stays false, intents defer,
     * and the bridge remains disabled.
     */
    public static void applyStartupReadiness(GatewayReadiness readiness, boolean executionEnabled) {
        if (executionEnabled) {
            readiness.fluss(true,
                    "Fluss tables + Execution_Gate / Execution_Attempts stores opened (WP-3)");
            readiness.protocol(true, "private protocol configured");
            readiness.durableWrites(true, "projection ledger opened");
        } else {
            // Fail-closed HALTED default when execution is disabled: keep all readiness dimensions
            // false so executionReady stays false, intents defer, and bridge remains disabled.
            String disabledReason = "execution disabled via EXECUTION_ENABLED=false";
            readiness.fluss(false, disabledReason);
            readiness.protocol(false, disabledReason);
            readiness.durableWrites(false, disabledReason);
            LOG.warn("execution disabled via EXECUTION_ENABLED=false; gateway remains HALTED "
                    + "(fail-closed)");
        }
    }
}
