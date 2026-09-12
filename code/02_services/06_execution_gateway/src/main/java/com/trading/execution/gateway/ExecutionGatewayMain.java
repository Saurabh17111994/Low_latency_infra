package com.trading.execution.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Process entry point. Startup is intentionally HALTED until Fluss and protocol are proven ready. */
public final class ExecutionGatewayMain {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionGatewayMain.class);
    private ExecutionGatewayMain() {}

    public static void main(String[] args) throws Exception {
        GatewayConfig config = GatewayConfig.fromEnvironment();
        GatewayReadiness readiness = new GatewayReadiness();

        // P3-276: park on a latch, not `Thread.currentThread().join()` (a self-join that only
        // returned if the join call itself was interrupted, left the interrupt status unrestored,
        // and gave a SIGTERM nothing to release). `stop` is the drain signal; `cleaned` lets the
        // hook wait for the try-with-resources below to finish closing the reader and the Fluss
        // stores, because the JVM halts once every hook returns regardless of what main is doing.
        CountDownLatch stop = new CountDownLatch(1);
        CountDownLatch cleaned = new CountDownLatch(1);
        AtomicBoolean readerFailed = new AtomicBoolean(false);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stop.countDown();
            try {
                if (!cleaned.await(15, TimeUnit.SECONDS)) {
                    LOG.warn("gateway cleanup did not finish within 15s of shutdown");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "gateway-shutdown"));

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
                stop.await();
            }
            cleaned.countDown();
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
                            catch (Throwable e) {
                                // P3-064: this caught only RuntimeException, so an Error killed the
                                // thread silently — no fail(), no restart, /readyz still green. I
                                // reproduced that live: an ExceptionInInitializerError from Arrow
                                // killed the reader while /readyz kept answering 200. Any Throwable
                                // now marks the gateway failed and releases the drain latch; main
                                // then exits non-zero so an orchestrator restarts it.
                                readerFailed.set(true);
                                readiness.fail(String.valueOf(e.getMessage()));
                                LOG.error("intent reader stopped", e);
                                break;
                            }
                        }
                        stop.countDown();
                    }, "execution-intent-reader");
                    readerThread.setDaemon(true);
                    readerThread.start();
                    server.start();
                    stop.await();
                    // The poll thread must be STOPPED AND JOINED before the try-with-resources
                    // closes the reader. Fluss's LogScanner is not safe for multithreaded access,
                    // so closing it while the poll loop is still inside it throws
                    // ConcurrentModificationException out of main — observed live on SIGTERM the
                    // first time this drain path existed at all. The old self-join never hit it
                    // because the JVM halted without ever closing the reader.
                    // Drain markers go to stderr, not the logger: log4j2 registers its OWN shutdown
                    // hook, and once it tears the appenders down during JVM shutdown any drain line
                    // written from main is silently dropped. That made "the drain ran cleanly"
                    // indistinguishable from "main never woke and the JVM just halted", which is
                    // exactly the ambiguity these two lines exist to remove.
                    System.err.println("execution-gateway draining: stopping the intent reader");
                    readerThread.interrupt();
                    try {
                        readerThread.join(TimeUnit.SECONDS.toMillis(10));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    System.err.println("execution-gateway draining: closing intent reader and Fluss stores");
                }
            }
        }
        // Every try-with-resources above has now closed (reader, then stores) before the hook stops
        // waiting, so a SIGTERM drains rather than being cut off mid-close.
        cleaned.countDown();
        if (readerFailed.get()) {
            // P3-064: a dead reader must not present as a clean shutdown, or an orchestrator will
            // leave a gateway whose execution path is silently gone. Exiting non-zero after cleanup
            // is what makes the restart happen.
            LOG.error("execution-gateway exiting non-zero: intent reader stopped ({})",
                    readiness.snapshot().reason());
            System.exit(1);
        }
    }
}
