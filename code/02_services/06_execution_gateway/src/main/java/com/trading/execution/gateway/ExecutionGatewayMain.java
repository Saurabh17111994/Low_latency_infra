package com.trading.execution.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Process entry point. Startup is intentionally HALTED until Fluss and protocol are proven ready. */
public final class ExecutionGatewayMain {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionGatewayMain.class);
    private ExecutionGatewayMain() {}

    public static void main(String[] args) throws Exception {
        GatewayConfig config = GatewayConfig.fromEnvironment();
        GatewayReadiness readiness = new GatewayReadiness();

        if (!config.executionEnabled()) {
            // P3-060: the gate now precedes EVERY Fluss call, so a disabled gateway genuinely boots
            // offline — no FLUSS_BOOTSTRAP, no reachable tables, no durable stores — which is what
            // the fail-closed comment always claimed but the old ordering contradicted by opening
            // all five stores first.
            //
            // Health/readyz are still served. GatewayHttpServer independently refuses execution
            // endpoints when executionEnabled is false (and whenever readiness is incomplete), so no
            // projection consumer is wired and none is needed: the consumer is unreachable.
            GatewayStartup.applyStartupReadiness(readiness, false);
            try (GatewayHttpServer server = new GatewayHttpServer(config, readiness, null)) {
                server.start();
                LOG.warn("execution-gateway started DISABLED (fail-closed, offline); serving "
                        + "/healthz + /readyz only");
                Thread.currentThread().join();
            }
            return;
        }

        // WP-3 fail-fast existence probe; the handles are not retained (P3-275).
        GatewayStartup.probeAuthorityTables(config);
        try (GatewayStartup.Stores stores = GatewayStartup.openStores(config)) {
            ProjectionApplier applier = new ProjectionApplier(stores.projections(), stores.ledger());
            NautilusIntentClient outbound =
                    new NautilusIntentClient(config, stores.controls(), readiness);
            // P3-277: the reader is owned by try-with-resources so a throw from
            // subscribeFromBeginning() or the readiness wiring can no longer leak its connection,
            // table and dedup store — it used to be closed only by the inner server's finally.
            try (IntentReader reader = IntentReader.open(config, outbound,
                    reason -> { readiness.fail(reason); LOG.error("execution intent halted: {}", reason); })) {
                reader.subscribeFromBeginning();
                GatewayStartup.applyStartupReadiness(readiness, true);
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
                }
            }
        }
    }
}
