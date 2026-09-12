package com.trading.execution.gateway;

import com.trading.common.schema.execution.FlussAttemptStore;
import com.trading.common.schema.execution.FlussGateStateStore;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testable startup seam for {@link ExecutionGatewayMain}.
 *
 * <p>Extracted with <b>no behaviour change</b> (G1): {@link #openStores} is exactly the store-open
 * block that was inline in {@code main()}, and {@link #applyStartupReadiness} is exactly the
 * enabled/disabled readiness branch. Both were previously unreachable from a test because they sat
 * inside a single {@code main(String[])} that talks to Fluss the moment it starts, which is why the
 * gateway's startup ordering had no characterization coverage at all.
 */
public final class GatewayStartup {
    private static final Logger LOG = LoggerFactory.getLogger(GatewayStartup.class);

    private GatewayStartup() {}

    /**
     * The durable stores the process holds open for its lifetime, closed in reverse open order.
     */
    public record Stores(FlussControlStateStore controls,
                         FlussProjectionWriter projections,
                         FlussProjectionLedgerStore ledger,
                         FlussGateStateStore gates,
                         FlussAttemptStore attempts) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            attempts.close();
            gates.close();
            ledger.close();
            projections.close();
            controls.close();
        }
    }

    /**
     * Opens the durable Fluss stores. The only place startup talks to Fluss, and the single point
     * a later change has to branch on to keep a disabled gateway offline (P3-060).
     */
    public static Stores openStores(GatewayConfig config) throws Exception {
        return new Stores(
                FlussControlStateStore.open(config),
                FlussProjectionWriter.open(config),
                FlussProjectionLedgerStore.open(config),
                // WP-3: the durable gate/attempt backplane. Open fails fast if Execution_Gate /
                // Execution_Attempts (v3 DDL) are absent or unreachable, so the gateway is never
                // "ready" without its durable authority tables. Hydration on read/prepare
                // re-derives prior fences/attempts after a restart (crash-window zero-duplicate).
                FlussGateStateStore.open(
                        config.flussBootstrap(), config.flussDatabase(), config.gateTable(),
                        config.requestTimeout(), Set.of("saurabh")),
                FlussAttemptStore.open(
                        config.flussBootstrap(), config.flussDatabase(), config.attemptsTable(),
                        config.requestTimeout(),
                        () -> LOG.warn(
                                "execution attempt contract violation -> request a safety halt")));
    }

    /**
     * Applies the startup readiness wiring for the enabled/disabled branch.
     *
     * <p>The enabled branch calls {@code fluss(true, ...)} twice — the second call targets the same
     * dimension and only replaces the reason, so the operator loses the distinction between "tables
     * opened" and "gate/attempt stores opened" (P3-479). That is preserved verbatim here, and pinned
     * by the characterization test, so the later fix is a deliberate and visible change rather than
     * a silent drift.
     */
    public static void applyStartupReadiness(GatewayReadiness readiness, boolean executionEnabled) {
        if (executionEnabled) {
            readiness.fluss(true, "Fluss tables opened");
            readiness.protocol(true, "private protocol configured");
            readiness.durableWrites(true, "projection ledger opened");
            readiness.fluss(true, "Execution_Gate / Execution_Attempts stores opened (WP-3)");
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
