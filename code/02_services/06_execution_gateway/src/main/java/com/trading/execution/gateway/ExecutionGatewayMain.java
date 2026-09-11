package com.trading.execution.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.common.schema.execution.FlussAttemptStore;
import com.trading.common.schema.execution.FlussGateStateStore;

/** Process entry point. Startup is intentionally HALTED until Fluss and protocol are proven ready. */
public final class ExecutionGatewayMain {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionGatewayMain.class);
    private ExecutionGatewayMain() {}

    public static void main(String[] args) throws Exception {
        GatewayConfig config = GatewayConfig.fromEnvironment();
        GatewayReadiness readiness = new GatewayReadiness();
        try (FlussControlStateStore controls = FlussControlStateStore.open(config);
                FlussProjectionWriter projections = FlussProjectionWriter.open(config);
                FlussProjectionLedgerStore ledger = FlussProjectionLedgerStore.open(config);
                // WP-3: the durable gate/attempt backplane. Open fails fast if Execution_Gate /
                // Execution_Attempts (v3 DDL) are absent or unreachable, so the gateway is never
                // "ready" without its durable authority tables. Hydration on read/prepare re-derives
                // prior fences/attempts after a restart (crash-window zero-duplicate).
                FlussGateStateStore gates = FlussGateStateStore.open(
                        config.flussBootstrap(), config.flussDatabase(), config.gateTable(),
                        config.requestTimeout(), java.util.Set.of("saurabh"));
                FlussAttemptStore attempts = FlussAttemptStore.open(
                        config.flussBootstrap(), config.flussDatabase(), config.attemptsTable(),
                        config.requestTimeout(),
                        () -> LOG.warn("execution attempt contract violation -> request a safety halt"))) {
            ProjectionApplier applier = new ProjectionApplier(projections, ledger);
            NautilusIntentClient outbound = new NautilusIntentClient(config, controls, readiness);
            IntentReader reader = IntentReader.open(config, outbound,
                    reason -> { readiness.fail(reason); LOG.error("execution intent halted: {}", reason); });
            reader.subscribeFromBeginning();
            if (config.executionEnabled()) {
                readiness.fluss(true, "Fluss tables opened");
                readiness.protocol(true, "private protocol configured");
                readiness.durableWrites(true, "projection ledger opened");
                readiness.fluss(true, "Execution_Gate / Execution_Attempts stores opened (WP-3)");
            } else {
                // Fail-closed HALTED default when execution is disabled: keep all readiness dimensions false
                // so executionReady stays false, intents defer, and bridge remains disabled. Offline,
                // no FLUSS_BOOTSTRAP / Arrow deps are required to evaluate this gate.
                String disabledReason = "execution disabled via EXECUTION_ENABLED=false";
                readiness.fluss(false, disabledReason);
                readiness.protocol(false, disabledReason);
                readiness.durableWrites(false, disabledReason);
                LOG.warn("execution disabled via EXECUTION_ENABLED=false; gateway remains HALTED (fail-closed)");
            }
            ObjectMapper mapper = new ObjectMapper();
            try (GatewayHttpServer server = new GatewayHttpServer(config, readiness,
                    payload -> {
                        try { applier.apply(mapper.treeToValue(payload, NormalizedExecutionEvent.class)); }
                        catch (Exception e) { readiness.durableWrites(false, e.getMessage()); throw new IllegalStateException(e); }
                    })) {
                LOG.warn("execution-gateway started; execution readiness still depends on Execution_Gate=ENABLED");
                Thread readerThread = new Thread(() -> {
                    while (!Thread.currentThread().isInterrupted()) {
                        try { reader.poll(config.pollTimeout()); }
                        catch (RuntimeException e) { readiness.fail(e.getMessage()); LOG.error("intent reader stopped", e); break; }
                    }
                }, "execution-intent-reader");
                readerThread.setDaemon(true);
                readerThread.start();
                server.start();
                Thread.currentThread().join();
            } finally { reader.close(); }
        }
    }
}
