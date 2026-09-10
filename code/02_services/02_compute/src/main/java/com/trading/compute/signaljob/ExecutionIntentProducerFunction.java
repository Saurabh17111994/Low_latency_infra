package com.trading.compute.signaljob;

import java.util.Objects;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts valid signal candidates to immutable execution-intent rows.
 *
 * <p>This operator performs no network I/O and has no broker authority. It is
 * installed only when {@code EXECUTION_INTENT_ENABLED=true}; invalid or
 * unsupported candidates are counted and dropped here. A 1-in-1000 sampler
 * records tick-to-intent and signal-to-intent latencies into Flink
 * histograms; sampling keeps the observer cost at two subtractions and one
 * counter increment per record. Durable quarantine and changed-identity
 * enforcement belong to the Java execution gateway (T2).
 */
public final class ExecutionIntentProducerFunction
        extends RichFlatMapFunction<RowData, RowData> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionIntentProducerFunction.class);

    /** One latency sample per thousand intents — observer cost stays flat. */
    static final int SAMPLE_EVERY = 1000;

    /**
     * P2-136: sane-latency ceiling — anything older than one trading day is
     * a unit mixup or corrupt ts, not a real latency. Never clips real
     * latencies (tick-to-intent is ms-scale).
     */
    static final long MAX_SANE_LATENCY_MS = 24L * 60 * 60 * 1_000;

    private final SignalJobConfig config;
    private transient Counter rejectedCounter;
    private transient Histogram tickToIntent;
    private transient Histogram signalToIntent;
    private transient long seen;

    public ExecutionIntentProducerFunction(SignalJobConfig config) {
        // P2-034: kill the per-record NPE class entirely — a null config
        // must fail at construction, never per row inside flatMap.
        this.config = Objects.requireNonNull(config, "config");
    }

    @Override
    public void open(OpenContext openContext) {
        rejectedCounter = getRuntimeContext().getMetricGroup()
                .counter("compute.execution_intent.rejected");
        tickToIntent = getRuntimeContext().getMetricGroup().histogram(
                "compute.latency.tick_to_intent",
                LatencyHistograms.create());
        signalToIntent = getRuntimeContext().getMetricGroup().histogram(
                "compute.latency.signal_to_intent",
                LatencyHistograms.create());
    }

    @Override
    public void flatMap(RowData candidate, Collector<RowData> out) {
        try {
            String tradeContextId = ExecutionIntentContextResolver.resolveEntry(
                    candidate, config.executionAccountScopeId());
            ExecutionIntent intent = ExecutionIntentBuilder.fromCandidate(
                    candidate,
                    config.executionAccountScopeId(),
                    config.executionPartitionId(),
                    config.executionProductType(),
                    config.executionTimeInForce(),
                    config.configurationVersion(),
                    tradeContextId);
            out.collect(ExecutionIntentBuilder.build(intent));
        } catch (RuntimeException e) {
            // P2-034: wide catch — NPE/ClassCast/OOB/SHA-256 all become a
            // counted drop, never a replay loop. P2-222: stack + identity so
            // a burst is triageable instead of message-only noise.
            if (rejectedCounter != null) {
                rejectedCounter.inc();
            }
            LOG.warn("execution-intent: rejected candidate {} token={} before executable output: {}",
                    candidateIdOrNull(candidate), tokenOrNull(candidate), e.toString(), e);
            return;
        }
        // P2-135: sampling runs OUTSIDE the emit try — an observer throw
        // after out.collect must never replay-duplicate the intent.
        try {
            sampleLatency(candidate, System.currentTimeMillis());
        } catch (RuntimeException e) {
            LOG.debug("execution-intent: latency sample dropped", e);
        }
    }

    private void sampleLatency(RowData candidate, long now) {
        if (!isSampled(seen++)) {
            return;
        }
        // Detection holds the breaking tick's event time; evaluation holds
        // the signal emit time. Null-tolerant: pre-host rows may lack either.
        if (tickToIntent != null && !candidate.isNullAt(SignalCandidatesTableColumns.DETECTION_TS)) {
            long latency = latencyOrNegative(
                    now, candidate.getLong(SignalCandidatesTableColumns.DETECTION_TS));
            if (latency >= 0) {
                tickToIntent.update(latency);
            }
        }
        if (signalToIntent != null && !candidate.isNullAt(SignalCandidatesTableColumns.EVALUATION_TS)) {
            long latency = latencyOrNegative(
                    now, candidate.getLong(SignalCandidatesTableColumns.EVALUATION_TS));
            if (latency >= 0) {
                signalToIntent.update(latency);
            }
        }
    }

    static boolean isSampled(long seen) {
        return seen % SAMPLE_EVERY == 0;
    }

    static long latencyOrNegative(long now, long ts) {
        // P2-136: both stamps are millis-epoch by contract (DETECTION_TS is
        // the breaking tick's event time, now is wall-clock). Cap at 24h so
        // a unit mixup (s vs ms ≈ 1.7T ms) or corrupt ancient ts can't
        // pollute the 4096-sample percentiles; over-cap drops DEBUG-count.
        if (ts <= 0 || now < ts) {
            return -1;
        }
        long latency = now - ts;
        return latency <= MAX_SANE_LATENCY_MS ? latency : -1;
    }

    // P2-222: null-safe identity for the reject log — logging must never
    // throw on the very row it describes.
    private static String candidateIdOrNull(RowData candidate) {
        try {
            if (candidate == null
                    || candidate.getArity() <= SignalCandidatesTableColumns.CANDIDATE_ID
                    || candidate.isNullAt(SignalCandidatesTableColumns.CANDIDATE_ID)) {
                return "null";
            }
            return candidate.getString(SignalCandidatesTableColumns.CANDIDATE_ID).toString();
        } catch (RuntimeException e) {
            return "unreadable";
        }
    }

    private static String tokenOrNull(RowData candidate) {
        try {
            if (candidate == null
                    || candidate.getArity() <= SignalCandidatesTableColumns.INSTRUMENT_TOKEN
                    || candidate.isNullAt(SignalCandidatesTableColumns.INSTRUMENT_TOKEN)) {
                return "null";
            }
            return String.valueOf(
                    candidate.getLong(SignalCandidatesTableColumns.INSTRUMENT_TOKEN));
        } catch (RuntimeException e) {
            return "unreadable";
        }
    }
}
