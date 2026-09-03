package com.trading.compute.signaljob;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.table.data.RowData;

/**
 * Observability-only pass-through latency monitor (step-2 per-tick leg).
 *
 * <p>Every accepted tick already carries {@code ingest_ts} — the wall-clock
 * (same host, Asia/Kolkata epoch millis) at which the ingestion service
 * accepted it. This operator sits on the deduped per-tick stream, sees every
 * tick exactly once (non-keyed, no redistribution), and records
 * {@code now - ingest_ts} into a per-operator Flink {@link Histogram}
 * (exported as {@code compute.latency.ingest_to_monitor}). p50/p95/p99/max
 * over ALL ticks are then reported through the Prometheus scrape — exact,
 * not sampled by Flink's latency tracker.
 *
 * <p>Deliberately NO state, NO output change (identity map), NO schema
 * change, NO side effects beyond the metric. Fails safe: a missing/negative
 * {@code ingest_ts} is skipped, never forwarded.
 */
public class IngestLatencyMonitorFunction extends RichMapFunction<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private transient Histogram latency;

    @Override
    public void open(OpenContext openContext) {
        latency = getRuntimeContext().getMetricGroup().histogram(
                "compute.latency.ingest_to_monitor",
                LatencyHistograms.create());
    }

    @Override
    public RowData map(RowData row) {
        if (!row.isNullAt(RawTableColumns.INGEST_TS)) {
            long ingest = row.getLong(RawTableColumns.INGEST_TS);
            long now = System.currentTimeMillis();
            if (ingest > 0 && now >= ingest) {
                latency.update(now - ingest);
            }
        }
        return row;
    }
}
