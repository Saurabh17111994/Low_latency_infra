package com.trading.execution.gateway;

import com.trading.common.schema.execution.FlussAttemptStore;
import com.trading.common.schema.execution.FlussGateStateStore;
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
     * <p>Deliberately does NOT include the gate/attempt stores: no code path reads them, so holding
     * those handles idle for the process lifetime was misleading beside the hydration comment
     * (P3-275). They are proven to exist by {@link #probeAuthorityTables} and closed immediately.
     */
    public record Stores(FlussControlStateStore controls,
                         FlussProjectionWriter projections,
                         FlussProjectionLedgerStore ledger) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            ledger.close();
            projections.close();
            controls.close();
        }
    }

    /**
     * Opens the durable Fluss stores. The only place the running gateway talks to Fluss, and the
     * single point the executionEnabled gate has to precede to keep a disabled gateway offline
     * (P3-060).
     */
    public static Stores openStores(GatewayConfig config) throws Exception {
        return new Stores(
                FlussControlStateStore.open(config),
                FlussProjectionWriter.open(config),
                FlussProjectionLedgerStore.open(config));
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
