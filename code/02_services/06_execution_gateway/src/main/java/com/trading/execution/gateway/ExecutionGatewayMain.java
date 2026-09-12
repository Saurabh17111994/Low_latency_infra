package com.trading.execution.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/** Process entry point. Startup is intentionally HALTED until Fluss and protocol are proven ready. */
public final class ExecutionGatewayMain {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionGatewayMain.class);
    private ExecutionGatewayMain() {}

    /**
     * The reader step the poll loop performs. {@code Throwable} is deliberate: the P3-064 contract
     * is about an {@code Error}, not only a RuntimeException.
     */
    @FunctionalInterface
    interface ReaderPoll {
        void poll() throws Throwable;
    }

    /**
     * The reader loop's failure contract, extracted from the thread body so a test can drive it
     * with a poll that throws (P3-064 regression pin).
     *
     * <p>Any {@code Throwable} must mark the gateway failed and stop the loop; the caller then
     * releases the drain latch and {@code main} exits non-zero, so an orchestrator restarts a
     * gateway whose execution path would otherwise be silently gone.
     *
     * @return true when the loop stopped because the reader failed, false when interrupted
     */
    static boolean runReaderLoop(ReaderPoll poll, GatewayReadiness readiness,
                                 AtomicBoolean readerFailed) {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                poll.poll();
            } catch (Throwable e) {
                // P3-064: this caught only RuntimeException, so an Error killed the thread
                // silently — no fail(), no restart, /readyz still green. Reproduced live: an
                // ExceptionInInitializerError from Arrow killed the reader while /readyz kept
                // answering 200. Any Throwable now marks the gateway failed and releases the
                // drain latch; main then exits non-zero so an orchestrator restarts it.
                readerFailed.set(true);
                readiness.fail(String.valueOf(e));
                LOG.error("intent reader stopped", e);
                return true;
            }
        }
        return false;
    }

    /**
     * P3-063: the projection handler's failure contract, extracted from the server lambda so a test
     * can drive it with a message-less exception, the same way {@link #runReaderLoop} pins the loop
     * contract.
     *
     * <p>Both the readiness reason and the wrapper's message go through {@code String.valueOf(e)}:
     * {@code e.getMessage()} is null for any no-message throwable (an NPE, a no-arg
     * {@code IllegalStateException}), which P3-300 normalises to the literal reason "unknown" and
     * leaves an operator unable to tell a projection bug from a Fluss outage.
     */
    static void applyProjection(Callable<Void> action, GatewayReadiness readiness) {
        try {
            action.call();
        } catch (Exception e) {
            readiness.durableWrites(false, String.valueOf(e));
            throw new IllegalStateException(String.valueOf(e), e);
        }
    }

    public static void main(String[] args) throws Exception {
        GatewayConfig config = GatewayConfig.fromEnvironment();
        GatewayReadiness readiness = new GatewayReadiness();

        // P3-276: the drain contract -- a park SIGTERM can release, a bounded cleanup wait, and a
        // reader join before the stores close -- lives in GatewayShutdown, so it is test-pinned
        // instead of being reachable only through a live SIGTERM.
        GatewayShutdown shutdown = new GatewayShutdown();
        AtomicBoolean readerFailed = new AtomicBoolean(false);
        shutdown.installHook();

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
                shutdown.parkUntilStop();
            }
            shutdown.markCleaned();
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
            // P3-061: a malformed row reaches this handler on EVERY poll, so the previous
            // unconditional LOG.error produced hundreds of identical lines for one incident (566 in
            // an observed live run) and buried the original cause in its own repeats. The latch
            // keeps the first reason, so report the transition once and drop repeats to debug.
            AtomicBoolean haltReported = new AtomicBoolean(false);
            try (IntentReader reader = IntentReader.open(config, outbound, reason -> {
                readiness.fail(reason);
                if (haltReported.compareAndSet(false, true)) {
                    LOG.error("execution intent halted; gateway HALTED until restart: {}", reason);
                } else {
                    LOG.debug("execution intent already halted: {}", reason);
                }
            })) {
                reader.subscribeFromBeginning();
                // C1: pay the post-CREATE window before readiness claims Fluss is usable and
                // before the port opens, so a user request is never the first writer to a table.
                GatewayStartup.prewarmTables(config);
                GatewayStartup.applyStartupReadiness(readiness, true);
                ObjectMapper mapper = new ObjectMapper();
                try (GatewayHttpServer server = new GatewayHttpServer(config, readiness,
                        payload -> applyProjection(() -> {
                            applier.apply(mapper.treeToValue(payload, NormalizedExecutionEvent.class));
                            return null;
                        }, readiness))) {
                    LOG.warn("execution-gateway started; execution readiness still depends on Execution_Gate=ENABLED");
                    Thread readerThread = new Thread(() -> {
                        runReaderLoop(() -> reader.poll(config.pollTimeout()), readiness, readerFailed);
                        shutdown.signalStop();
                    }, "execution-intent-reader");
                    readerThread.setDaemon(true);
                    readerThread.start();
                    server.start();
                    shutdown.parkUntilStop();
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
                    shutdown.drainReader(readerThread, GatewayShutdown.READER_JOIN_MILLIS);
                    System.err.println("execution-gateway draining: closing intent reader and Fluss stores");
                }
            }
        }
        // Every try-with-resources above has now closed (reader, then stores) before the hook stops
        // waiting, so a SIGTERM drains rather than being cut off mid-close.
        shutdown.markCleaned();
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
