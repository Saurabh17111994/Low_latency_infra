package com.trading.ingestion.telemetry;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Phase 2 (P1-133): the only hand-placed spans in the service — 3 batch-level
 * (or coarser) markers, never per-tick. The agent auto-injects the ambient
 * trace/span IDs into every log line emitted inside these scopes, which is
 * what makes {@code trace_id} real in OpenObserve.
 *
 * <p>Agentless safety: with no javaagent, {@link GlobalOpenTelemetry} yields
 * the spec no-op tracer — every method below is a silent no-op (invalid
 * spans, no-op scopes), so unit tests and bare JVMs behave identically.
 *
 * <p>Sampling is deterministic in code (every Nth market batch), NOT via the
 * SDK sampler env: a ratio sampler would also dilute the 3 lifecycle spans to
 * 1%, usually erasing boot/shutdown traces. Steady-state volume at full flow
 * is ~batches/sec / N (1ms Go linger → ~1000 batches/s → ~10 spans/s at
 * N=100). Override with {@code INGESTION_TRACE_EVERY_N_BATCHES}; garbage
 * falls back to 100 with a warning (observability knob, never fail-closed).
 */
public final class Tracing {

    private static final Logger LOG = LoggerFactory.getLogger(Tracing.class);

    private Tracing() {}

    private static final Tracer TRACER =
            GlobalOpenTelemetry.getTracer("com.trading.ingestion");

    private static final int DEFAULT_EVERY_N = 100;

    private static final int MARKET_BATCH_EVERY_N = everyN();

    private static final AtomicLong MARKET_BATCH_COUNT = new AtomicLong();

    private static int everyN() {
        String raw = System.getenv("INGESTION_TRACE_EVERY_N_BATCHES");
        if (raw == null || raw.isBlank()) return DEFAULT_EVERY_N;
        try {
            int n = Integer.parseInt(raw.trim());
            if (n > 0) return n;
        } catch (NumberFormatException ignored) {
            // fall through to default below
        }
        LOG.warn("tracing: INGESTION_TRACE_EVERY_N_BATCHES={} invalid; using {}",
                raw, DEFAULT_EVERY_N);
        return DEFAULT_EVERY_N;
    }

    /** A started span plus its scope, closed together. Uniform for real, invalid, and skipped spans. */
    public static final class TracedSpan implements AutoCloseable {
        public final Span span;
        private final Scope scope;

        private TracedSpan(Span span) {
            this.span = span;
            this.scope = span.makeCurrent();
        }

        /** Record a failure before it propagates (behavior unchanged — caller still throws). */
        public void error(Throwable t) {
            span.recordException(t);
            span.setStatus(StatusCode.ERROR);
        }

        @Override
        public void close() {
            try {
                scope.close();
            } finally {
                span.end();
            }
        }
    }

    /** Always-on span (lifecycle: manifest load, shutdown). ~3 per process lifetime. */
    public static TracedSpan span(String name) {
        return new TracedSpan(TRACER.spanBuilder(name).startSpan());
    }

    /** Pure sampling gate (package-visible for the ratio pin). */
    static boolean admit(long count) {
        return count % MARKET_BATCH_EVERY_N == 0;
    }

    /** Deterministic 1-in-N span for Go market batches (the batch-level marker). */
    public static TracedSpan marketBatch(int eventCount, String connectionId, long connectionEpoch) {
        // Counter first: skipped batches allocate nothing (no span object at
        // 60k-ticks/s scale); the invalid span below is a singleton no-op.
        if (!admit(MARKET_BATCH_COUNT.incrementAndGet())) {
            return new TracedSpan(Span.getInvalid());
        }
        Span span = TRACER.spanBuilder("ingestion.market-batch").startSpan();
        span.setAttribute("tick.events", eventCount);
        span.setAttribute("connection.id", connectionId == null ? "" : connectionId);
        span.setAttribute("connection.epoch", connectionEpoch);
        return new TracedSpan(span);
    }
}
