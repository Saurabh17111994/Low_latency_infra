package com.trading.compute.signaljob;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;

/**
 * Emits candle-preview rows to {@code feature_candles_15s_preview}
 * (low-latency candles Phase 1, 2026-08-29) — the live "growing" OHLCV of
 * each in-progress 15s window, overwritten every {@code PREVIEW_INTERVAL_MS}
 * (default 1s) by a KV upsert on the same PK
 * {@code (instrument_token, window_start)} as the final candle.
 *
 * <p>Fired by {@link CandlePreviewTrigger}:
 * <ul>
 *   <li>on processing time (every 1s): build the CURRENT accumulator as a
 *       preview row ({@code is_preview=TRUE}) and emit to the preview sink —
 *       the row overwrites the previous preview for the same window.</li>
 *   <li>on window end (event time, FIRE_AND_PURGE): emit the FINAL preview
 *       row with {@code is_preview=FALSE}? <b>No</b> — the final candle is
 *       emitted by the existing {@link CandleEmitFunction} to the MAIN table.
 *       This function emits previews ONLY; at window end it emits nothing
 *       (the window's accumulator is already handled by the main path).</li>
 * </ul>
 *
 * <p><b>Invariant check:</b> previews do NOT run the five
 * {@link CandleInvariantCheck} OHLC invariants — they are partial (a high
 * that later gets beaten, a close that moves). The invariants gate the FINAL
 * candle only (unchanged, in {@link CandleEmitFunction}).
 *
 * <p><b>First-write-wins:</b> previews BYPASS {@link CandleKvFirstWriteWinsFunction}
 * — they are updates to the same KV key, not second emissions of a window.
 * The preview sink writes directly (upsert semantics), so each 1s tick
 * overwrites the previous preview row.
 *
 * <p><b>Restart-safety:</b> the {@code emitted} window-state flag in
 * {@link CandleEmitFunction} guards the FINAL candle; previews are
 * idempotent upserts (same PK, overwrite) — a restore re-fires the last
 * preview, which is harmless (it overwrites the same row).
 */
public class CandlePreviewEmitFunction
        extends ProcessWindowFunction<CandleAccumulator, RowData, Long, TimeWindow> {

    private static final long serialVersionUID = 1L;

    private final SignalJobConfig config;

    private transient Counter previewCounter;

    public CandlePreviewEmitFunction(SignalJobConfig config) {
        this.config = config;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        previewCounter =
                getRuntimeContext().getMetricGroup().counter("compute.candles.previews.emitted");
    }

    @Override
    public void process(Long instrumentToken, Context context, Iterable<CandleAccumulator> elements,
            Collector<RowData> out) throws Exception {
        CandleAccumulator acc = elements.iterator().next();
        TimeWindow window = context.window();

        previewCounter.inc();
        out.collect(buildRow(instrumentToken, acc, window, context.currentProcessingTime(), config));
    }

    /**
     * Builds the 14-column preview row (CandlePreviewColumns order) for the
     * given window's current accumulator. Extracted so the row content is
     * testable without a window-operator harness.
     */
    static GenericRowData buildRow(long instrumentToken, CandleAccumulator acc, TimeWindow window,
            long outputTs, SignalJobConfig config) {
        GenericRowData row = new GenericRowData(CandlePreviewColumns.FIELD_COUNT);
        row.setField(CandlePreviewColumns.INSTRUMENT_TOKEN, instrumentToken);
        row.setField(CandlePreviewColumns.EXCHANGE, StringData.fromString(acc.exchange));
        row.setField(CandlePreviewColumns.SYMBOL, StringData.fromString(acc.symbol));
        row.setField(CandlePreviewColumns.WINDOW_START, window.getStart());
        row.setField(CandlePreviewColumns.WINDOW_END, window.getEnd());
        row.setField(CandlePreviewColumns.OPEN_PAISE, acc.openPaise);
        row.setField(CandlePreviewColumns.HIGH_PAISE, acc.highPaise);
        row.setField(CandlePreviewColumns.LOW_PAISE, acc.lowPaise);
        row.setField(CandlePreviewColumns.CLOSE_PAISE, acc.closePaise);
        row.setField(CandlePreviewColumns.VOLUME, acc.volume);
        row.setField(CandlePreviewColumns.TICK_COUNT, (int) acc.tickCount);
        row.setField(CandlePreviewColumns.IS_PREVIEW, true);
        row.setField(CandlePreviewColumns.OUTPUT_TS, outputTs);
        row.setField(CandlePreviewColumns.SCHEMA_VERSION,
                StringData.fromString(config.previewSchemaVersion()));
        return row;
    }
}
