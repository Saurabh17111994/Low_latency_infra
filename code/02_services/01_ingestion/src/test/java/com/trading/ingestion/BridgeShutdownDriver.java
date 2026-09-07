package com.trading.ingestion;

import com.trading.ingestion.config.IngestionConfig;
import com.trading.ingestion.discontinuity.DiscontinuitySink;
import com.trading.ingestion.discontinuity.DiscontinuityWriter;
import com.trading.ingestion.quarantine.QuarantineSink;
import com.trading.ingestion.quarantine.QuarantineWriter;
import com.trading.ingestion.safety.SafetyHaltWriter;
import com.trading.ingestion.safety.SafetySink;
import com.trading.ingestion.write.FlussRowConverter;
import com.trading.ingestion.write.RawTickWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Cluster-free service driver for {@link BridgeShutdownHookTest} (ING-UNIT-024).
 *
 * <p>Mirrors {@code IngestionService.main()}'s shutdown wiring — capture the
 * main thread ({@code mainThread}) and register the shutdown hook that runs the
 * real {@code shutdown()} path — but with the ING-DQ-010 seam (a stub
 * {@link FlussRowConverter} and no-op evidence sinks) and a scripted fake
 * bridge via {@code ARROW_BRIDGE_BIN}, so no Fluss and no Go binaries are
 * involved. The test SIGTERMs this JVM; the hook runs {@code shutdown()}
 * (signal the bridge via {@code kill -TERM}, drain its stderr, bounded join of
 * the main thread), the main loop unwinds to its final report, and the JVM
 * exits 143.
 *
 * <p>{@code mainThread} and {@code shutdown()} are private in
 * {@code IngestionService}, so both are wired through reflection (the
 * superseded {@code a980a33} draft accessed them directly and never compiled
 * against this revision).
 *
 * <p>Required env (set by the test): {@code ARROW_BRIDGE_BIN},
 * {@code ARROW_APP_ID}, {@code ARROW_APP_SECRET}, {@code ARROW_TOKEN},
 * {@code FLUSS_BOOTSTRAP}, {@code RAW_TABLE_NAME},
 * {@code ARROW_MAX_EVENT_AGE_MS}, {@code ARROW_MAX_FUTURE_EVENT_SKEW_MS},
 * {@code UNCERTAINTY_JOURNAL_PATH}, {@code LOG_DIR}.
 */
public final class BridgeShutdownDriver {

    private BridgeShutdownDriver() {
    }

    public static void main(String[] args) throws Exception {
        IngestionConfig config = buildConfig();
        IngestionService service = new IngestionService(
                "ing-unit-024", instruments(), new NoopConverter(), config, null,
                noopQuarantine(), noopDiscontinuity(), noopSafety());

        // Mirror main(): capture the main thread so the hook can join it
        // (bounded) — without the join, the JVM halts the moment the hook
        // returns and the main thread's final "bridge loop ended" report races
        // the halt.
        Field mainThread = IngestionService.class.getDeclaredField("mainThread");
        mainThread.setAccessible(true);
        mainThread.set(service, Thread.currentThread());

        Method shutdown = IngestionService.class.getDeclaredMethod("shutdown");
        shutdown.setAccessible(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                shutdown.invoke(service);
            } catch (Exception e) {
                throw new RuntimeException("shutdown hook failed", e);
            } finally {
                // Mirror prod main() (IngestionService:451): stop the logging
                // context AFTER the drain so buffered output + .gz rollover
                // trailers flush (log4j2 shutdownHook="disable" + no explicit
                // stop would otherwise truncate them).
                org.apache.logging.log4j.LogManager.shutdown();
            }
        }));

        String bridgeBin = System.getenv().getOrDefault(
                "ARROW_BRIDGE_BIN", "go-bridge/arrow-bridge");
        service.runWithBridge(bridgeBin);
    }

    private static IngestionConfig buildConfig() throws Exception {
        Map<String, String> env = new HashMap<>();
        // P1-077: the driver must forward the env declaration or the child
        // refuses startup fail-closed (blank env is not silently dev).
        for (String key : List.of("DEPLOYMENT_ENV", "DEPLOY_ENV", "ARROW_APP_ID", "ARROW_APP_SECRET", "ARROW_USER_ID", "ARROW_PASSWORD", "ARROW_TOTP_KEY",
                "FLUSS_BOOTSTRAP", "RAW_TABLE_NAME", "ARROW_MAX_EVENT_AGE_MS",
                "ARROW_MAX_FUTURE_EVENT_SKEW_MS", "UNCERTAINTY_JOURNAL_PATH")) {
            env.put(key, System.getenv().getOrDefault(key, ""));
        }
        Method validateFrom = IngestionConfig.class.getDeclaredMethod("validateFrom", Map.class);
        validateFrom.setAccessible(true);
        return (IngestionConfig) validateFrom.invoke(null, env);
    }

    private static List<com.trading.ingestion.model.Instrument> instruments() {
        return List.of(new com.trading.ingestion.model.Instrument.Builder()
                .instrumentToken(100_000L).tradingSymbol("SYM1-EQ")
                .exchange("NSE").segment("CM").lotSize(1).manifestVersion(1).build());
    }

    // ---- no-op evidence sinks (ING-DQ-010 seam) ----

    private static QuarantineSink noopQuarantine() {
        return new QuarantineSink() {
            @Override
            public void write(byte[] rawPayload, QuarantineWriter.Reason reason, String detail) {
            }

            @Override
            public void write(byte[] rawPayload, QuarantineWriter.Reason reason, String detail,
                              Long instrumentToken, String exchange, String symbol) {
            }

            @Override
            public void close() {
            }
        };
    }

    private static DiscontinuitySink noopDiscontinuity() {
        return new DiscontinuitySink() {
            @Override
            public void write(DiscontinuityWriter.Reason reason, String note,
                              DiscontinuityWriter.LastTickSnapshot before) {
            }

            @Override
            public void write(DiscontinuityWriter.Reason reason, String note,
                              DiscontinuityWriter.LastTickSnapshot before,
                              Long instrumentToken, String exchange, String symbol) {
            }

            @Override
            public void writeBridgeEvent(com.trading.ingestion.bridge.BridgeEvent event,
                                         DiscontinuityWriter.LastTickSnapshot before) {
            }

            @Override
            public void close() {
            }
        };
    }

    private static SafetySink noopSafety() {
        return new SafetySink() {
            @Override
            public String write(String slotId, long connectionEpoch,
                                SafetyHaltWriter.SafetyState state,
                                SafetyHaltWriter.ReasonCode reasonCode, String assignedTokenHash,
                                String evidenceReference, long detectedTsMs) {
                return "noop";
            }

            @Override
            public void close() {
            }
        };
    }

    /** No-op converter — the SIGTERM-drain scenario never reaches the writer. */
    static final class NoopConverter implements FlussRowConverter {
        @Override
        public CompletableFuture<RawTickWriter.AppendResult> append(
                com.trading.ingestion.model.TickPacket packet) {
            return CompletableFuture.completedFuture(new RawTickWriter.AppendResult(1, "p0"));
        }

        @Override
        public int estimatedRowSize(com.trading.ingestion.model.TickPacket packet) {
            return 256;
        }

        @Override
        public void close() {
        }
    }
}
