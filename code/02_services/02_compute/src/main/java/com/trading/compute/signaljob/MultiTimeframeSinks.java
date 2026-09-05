package com.trading.compute.signaljob;

import java.time.Duration;
import java.util.Objects;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.fluss.flink.sink.FlussSink;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;

/**
 * Fluss sinks for the multi-timeframe candle projections (Phase 3 Track A,
 * design §F + §A decisions 8,10,12).
 *
 * <p>The multi-timeframe aggregator (already committed) emits closed rows on
 * its MAIN output and live rows on {@link MultiTimeframeAggregateFunction#LIVE_TAG}.
 * This class attaches the two durable sinks:
 * <ul>
 *   <li>{@code candle_live} — KV, PK {@code (instrument_token, tf, window_start)},
 *       16 buckets, upsert semantics {@code (false,false)}: the same row key is
 *       overwritten each 1 s snapshot so the live candle "grows" (14.6 k/s at
 *       2 433 × 6, §F.1).</li>
 *   <li>{@code candle_closed} — KV, same PK, immutable history, first-write-wins
 *       via {@link MultiTimeframeClosedFirstWriteWinsFunction} keyed by the
 *       composite {@code (instrument_token, tf, window_start)} so a re-emitted
 *       closed candle after restore never overwrites the already-written row
 *       (mirror of {@link CandleKvFirstWriteWinsFunction}, design Decision 12).</li>
 * </ul>
 *
 * <p>Builder pattern mirrors {@code SignalJob.java} candle-live/closed sinks
 * (FlussSink for {@code candle_live}/{@code candle_closed}) <b>exactly</b>:
 * {@code .setBootstrapServers/.setDatabase/.setTable/.setSerializationSchema(new RowDataSerializationSchema(false,false))/.setOption client.request-timeout + retries}.
 *
 * <p><b>Contract validator seam.</b> The two tables must be validated before
 * the graph is built via {@link TableContractValidator#validateCandleLiveTable}
 * and {@link TableContractValidator#validateCandleClosedTable}. The check
 * is performed in {@code SignalJob#preflightTableContracts} (Phase 4 will wire
 * it behind {@code MULTITF_ENABLED}); sinks themselves assume the deployed
 * tables already pass those validators — they are <i>not</i> re-validated
 * here (which would need live {@code TableInfo}).
 *
 * <p><b>Phase 3 → Phase 4 seam (IMPORTANT).</b> {@link SignalJobConfig} does
 * <b>not</b> yet expose {@code candleLiveTable()/candleClosedTable()}
 * accessors — that is Phase 4's edit surface (strict scope: Track A must not
 * touch {@code SignalJobConfig}). Accordingly the table names are taken as
 * explicit {@code String liveTableName / closedTableName} parameters on the
 * static factories/constructors. Phase 4 will add the config accessors and
 * wire {@code config.candleLiveTable()/candleClosedTable()} through these
 * parameters. Defaults are {@code candle_live} / {@code candle_closed}
 * (DDL 32/33 v1). Javadoc marks the seam so the Phase 4 change is
 * mechanical.
 */
public final class MultiTimeframeSinks {

    private MultiTimeframeSinks() {}

    /** Default live table (DDL 32 v1, 60 s TTL, no lake). */
    public static final String DEFAULT_LIVE_TABLE = "candle_live";

    /** Default closed table (DDL 33 v1, 7 d TTL, iceberg offload). */
    public static final String DEFAULT_CLOSED_TABLE = "candle_closed";

    // ── operator / sink identity (pinned UIDs — Phase 4 will assert them) ──────

    private static final String LIVE_SINK_NAME = "candle-live-sink";
    private static final String LIVE_SINK_UID = "candle-live-sink";

    private static final String CLOSED_FILTER_NAME = "candle-closed-first-write-wins";
    private static final String CLOSED_FILTER_UID = "candle-closed-first-write-wins";

    private static final String CLOSED_SINK_NAME = "candle-closed-sink";
    private static final String CLOSED_SINK_UID = "candle-closed-sink";

    // ── live sink — KV upsert, no dedup (overwrites same PK each 1 s) ──────

    /**
     * Attach the {@code candle_live} KV upsert sink to {@code liveStream}
     * (the {@link MultiTimeframeAggregateFunction#LIVE_TAG} side-output).
     *
     * <p>Upsert semantics: {@code new RowDataSerializationSchema(false,false)}
     * maps INSERT → UPSERT on PK {@code (instrument_token, tf, window_start)}
     * so successive 1 s snapshots of the same forming window overwrite the same
     * row (live growth), and replay converges.
     *
     * <p>Table name is explicit (Phase 3→4 seam) — Phase 4 will pass
     * {@code config.candleLiveTable()}. Default overload uses
     * {@value #DEFAULT_LIVE_TABLE}. Caller must have validated the table via
     * {@link TableContractValidator#validateCandleLiveTable} before the graph
     * is built.
     *
     * @param liveStream live candle stream (RowData in {@link CandleLiveColumns} layout)
     * @param config runtime config (bootstrap, database, sink timeouts/retries)
     * @param liveTableName Fluss table name (explicit seam — Phase 4 wires config)
     * @return {@code liveStream} after the sink is attached (mirrors SignalJob style)
     */
    public static DataStream<RowData> sinkLive(
            DataStream<RowData> liveStream, SignalJobConfig config, String liveTableName) {
        Objects.requireNonNull(liveStream, "liveStream");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(liveTableName, "liveTableName");
        liveStream.sinkTo(FlussSink.<RowData>builder()
                        .setBootstrapServers(config.bootstrapServers())
                        .setDatabase(config.database())
                        .setTable(liveTableName)
                        .setSerializationSchema(new RowDataSerializationSchema(false, false))
                        .setOption("client.request-timeout",
                                config.sinkWriteStallTimeoutMs() + "ms")
                        .setOption("client.writer.retries", String.valueOf(config.writerRetries()))
                        .build())
                .name(LIVE_SINK_NAME)
                .uid(LIVE_SINK_UID);
        return liveStream;
    }

    /**
     * Default-table overload: {@code candle_live} (DDL 32 v1).
     *
     * @see #sinkLive(DataStream, SignalJobConfig, String)
     */
    public static DataStream<RowData> sinkLive(
            DataStream<RowData> liveStream, SignalJobConfig config) {
        return sinkLive(liveStream, config, DEFAULT_LIVE_TABLE);
    }

    // ── closed sink — first-write-wins keyed filter, then KV upsert ──────

    /**
     * Attach the {@code candle_closed} immutable sink to {@code closedStream}
     * (the aggregator's MAIN output). The stream is first keyed by the
     * composite PK {@code (instrument_token, tf, window_start)} and passed
     * through {@link MultiTimeframeClosedFirstWriteWinsFunction} so a
     * re-emitted closed candle after checkpoint-restore never overwrites the
     * already-written row (Decision 12, mirror of {@link CandleKvFirstWriteWinsFunction}).
     *
     * <p>Filtered rows are then sunk with the same KV upsert builder as
     * {@link #sinkLive} but against the closed table (DDL 33 v1, 7 d + lake).
     * Second emissions are dropped and counted via
     * {@code compute.candles.multitf.duplicate_window}.
     *
     * <p>Table name is explicit (Phase 3→4 seam) — Phase 4 will pass
     * {@code config.candleClosedTable()}. Default overload uses
     * {@value #DEFAULT_CLOSED_TABLE}. Caller must have validated the table via
     * {@link TableContractValidator#validateCandleClosedTable}.
     *
     * @param closedStream closed candle stream (RowData in {@link CandleClosedColumns} layout)
     * @param config runtime config
     * @param closedTableName Fluss table name (explicit seam — Phase 4 wires config)
     * @return the first-write-wins-filtered stream after the sink is attached
     */
    public static DataStream<RowData> sinkClosed(
            DataStream<RowData> closedStream, SignalJobConfig config, String closedTableName) {
        Objects.requireNonNull(closedStream, "closedStream");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(closedTableName, "closedTableName");
        DataStream<RowData> filtered = closedStream
                .keyBy(MultiTimeframeClosedFirstWriteWinsFunction.keySelector(),
                        Types.TUPLE(Types.LONG, Types.STRING, Types.LONG))
                .process(new MultiTimeframeClosedFirstWriteWinsFunction())
                .returns(CandleClosedColumns.ROW_TYPE_INFO)
                .name(CLOSED_FILTER_NAME)
                .uid(CLOSED_FILTER_UID);
        filtered.sinkTo(FlussSink.<RowData>builder()
                        .setBootstrapServers(config.bootstrapServers())
                        .setDatabase(config.database())
                        .setTable(closedTableName)
                        .setSerializationSchema(new RowDataSerializationSchema(false, false))
                        .setOption("client.request-timeout",
                                config.sinkWriteStallTimeoutMs() + "ms")
                        .setOption("client.writer.retries", String.valueOf(config.writerRetries()))
                        .build())
                .name(CLOSED_SINK_NAME)
                .uid(CLOSED_SINK_UID);
        return filtered;
    }

    /**
     * Default-table overload: {@code candle_closed} (DDL 33 v1).
     *
     * @see #sinkClosed(DataStream, SignalJobConfig, String)
     */
    public static DataStream<RowData> sinkClosed(
            DataStream<RowData> closedStream, SignalJobConfig config) {
        return sinkClosed(closedStream, config, DEFAULT_CLOSED_TABLE);
    }

    // ── first-write-wins filter — composite key (instrument, tf, window_start) ──────

    /**
     * KV-level first-write-wins guard for the {@code candle_closed} sink
     * (Phase 3 Track A, Decision 12 — composite-key generalisation of
     * {@link CandleKvFirstWriteWinsFunction}).
     *
     * <p>Keyed by the candle_closed primary key
     * {@code (instrument_token, tf, window_start)} (DDLs 32/33 v1, 16 buckets).
     * Forwards the first emission of a key and drops + counts any second emission
     * via {@code compute.candles.multitf.duplicate_window} — a re-emission
     * after checkpoint-restore or same-day rollback must never overwrite the
     * immutable closed row (no correction rows, R-012 + Decision 12). Empty
     * windows produce no elements, so nothing is emitted.
     *
     * <p>TTL/boundedness mirrors {@link CandleKvFirstWriteWinsFunction}: marker
     * is {@code ValueState<Boolean>} with native {@code StateTtlConfig} TTL
     * 24 h (one trading day), {@code OnCreateAndWrite} + {@code NeverReturnExpired}.
     * State contract: exactly one Boolean per emitted composite key, no timers,
     * no payload — SIG-UNIT-008 style.
     */
    public static class MultiTimeframeClosedFirstWriteWinsFunction
            extends KeyedProcessFunction<Tuple3<Long, String, Long>, RowData, RowData> {

        private static final long serialVersionUID = 1L;

        private static final String WRITTEN_STATE_NAME = "mtf-candle-closed-written";

        private static final long WRITTEN_MARK_TTL_HOURS = 24L;

        /**
         * Written-marker retention: one trading day. Covers window lateness,
         * checkpoint cadence, and same-day savepoint rollback with wide margin
         * while bounding marker state. Package-visible so TTL-expiry test can
         * advance the harness clock past it — same visibility as the single-TF
         * counterpart.
         */
        static final Duration WRITTEN_MARK_TTL = Duration.ofHours(WRITTEN_MARK_TTL_HOURS);

        private transient ValueState<Boolean> written;
        private transient Counter duplicateWindows;

        /** The closed-stream key selector: PK of candle_closed. */
        public static KeySelector<RowData, Tuple3<Long, String, Long>> keySelector() {
            return row -> {
                String tf = null;
                if (!row.isNullAt(CandleClosedColumns.TF)) {
                    tf = row.getString(CandleClosedColumns.TF).toString();
                }
                return Tuple3.of(
                        row.getLong(CandleClosedColumns.INSTRUMENT_TOKEN),
                        tf,
                        row.getLong(CandleClosedColumns.WINDOW_START));
            };
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            ValueStateDescriptor<Boolean> descriptor =
                    new ValueStateDescriptor<>(WRITTEN_STATE_NAME, Types.BOOLEAN);
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(WRITTEN_MARK_TTL)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .build());
            written = getRuntimeContext().getState(descriptor);

            // Decision 12 (multi-TF Track A): every second emission of the same
            // (instrument_token, tf, window_start) is counted here — composite-key
            // generalisation of CandleKvFirstWriteWinsFunction's counter.
            duplicateWindows = getRuntimeContext().getMetricGroup()
                    .counter("compute.candles.multitf.duplicate_window");
        }

        @Override
        public void processElement(RowData row, Context ctx, Collector<RowData> out)
                throws Exception {
            if (written.value() != null) {
                duplicateWindows.inc();
                return;
            }
            written.update(true);
            out.collect(row);
        }

        /** Counter-source accessor (tests): the value the MetricGroup counter exports. */
        long duplicateWindowCountForTest() {
            return duplicateWindows == null ? 0L : duplicateWindows.getCount();
        }
    }
}
