package com.trading.compute.signaljob;

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

    private final SignalJobConfig config;
    private transient Counter rejectedCounter;
    private transient Histogram tickToIntent;
    private transient Histogram signalToIntent;
    private transient long seen;

    public ExecutionIntentProducerFunction(SignalJobConfig config) {
        this.config = config;
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
            sampleLatency(candidate, System.currentTimeMillis());
        } catch (IllegalArgumentException e) {
            if (rejectedCounter != null) {
                rejectedCounter.inc();
            }
            LOG.warn("execution-intent: rejected candidate before executable output: {}",
                    e.getMessage());
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
        return (ts > 0 && now >= ts) ? now - ts : -1;
    }
}
