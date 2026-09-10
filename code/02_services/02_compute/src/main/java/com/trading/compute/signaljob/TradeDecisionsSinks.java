package com.trading.compute.signaljob;

import java.time.Duration;
import java.util.Objects;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.fluss.flink.sink.FlussSink;
import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;

/**
 * SCH-19 dual-sink machinery (machinery only — the feed is the ranking stage,
 * Slice 3): attaches the two sinks every published decision must reach, both
 * plain {@code FlussSink}s (CHG-023 item 4, 2026-08-17: the StallGuardedSink
 * watchdog was removed — the Fluss client's own {@code client.request-timeout}
 * bounds each write, and the checkpoint timeout + fixed-delay restart fail
 * the job rather than hang it):
 *
 * <ol>
 *   <li><b>{@code Trade_Decisions} LOG</b> — append-only
 *       {@code RowDataSerializationSchema(true, true)}: every published
 *       decision is one immutable instruction row (REQ-FLS-008);</li>
 *   <li><b>{@code trade_instruction_state} KV index</b> — the 4-column index
 *       row (via {@link TradeDecisionIndexMapper}, which recomputes the
 *       canonical content hash) upserted with
 *       {@code RowDataSerializationSchema(false, false)}: the durable
 *       instruction → hash record the instruction-feed protocol checks before
 *       every append (REQ-FLS-015; DEC-038: Fluss is the authoritative index,
 *       rebuildable from the LOG).</li>
 * </ol>
 *
 * <p>Both sinks carry pinned UIDs ({@code trade-decisions-sink},
 * {@code trade-instruction-state-sink}) so their checkpoint-restore anchors
 * survive topology changes (CHECKPOINT-RESTORE-001 habit). Partial visibility
 * between the two sinks is reconciled by {@code instruction_id}
 * (SIG-INT-002 pattern).
 *
 * <p>Instructions are immutable: the same {@code instruction_id} may be
 * re-emitted only by a replay after checkpoint-restore, never by a second
 * publication (single-writer premise, REQ-FLS-008). The KV index is
 * therefore protected by a keyed first-write-wins filter (P4-076 — same
 * pattern as {@code MultiTimeframeClosedFirstWriteWinsFunction}): only the
 * first emission of an id reaches {@link TradeDecisionIndexMapper} and the
 * index sink; a replay after restore is dropped before it can overwrite the
 * canonical hash or {@code first_written_ts} (a divergent-hash replay would
 * silently destroy the violation evidence the instruction-feed protocol
 * checks; the LOG twin preserves forensics either way).
 */
public final class TradeDecisionsSinks {

    private static final String INDEX_FILTER_NAME = "trade-instruction-state-first-write-wins";
    private static final String INDEX_FILTER_UID = "trade-instruction-state-first-write-wins";

    private TradeDecisionsSinks() {}

    /**
     * Attach the LOG + KV-index dual sinks to the decision stream. The stream
     * is consumed twice (fan-out).
     */
    public static void attach(DataStream<RowData> decisions, SignalJobConfig config) {
        // P2-242: wiring mistakes must fail at graph-build, not as an opaque
        // NPE inside the builder chain (mirrors MultiTimeframeSinks).
        Objects.requireNonNull(decisions, "decisions");
        Objects.requireNonNull(config, "config");
        // (a) immutable instruction LOG — append-only
        decisions
                .sinkTo(FlussSink.<RowData>builder()
                                .setBootstrapServers(config.bootstrapServers())
                                .setDatabase(config.database())
                                .setTable(config.tradeDecisionsTable())
                                .setSerializationSchema(new RowDataSerializationSchema(true, true))
                                .setOption("client.request-timeout",
                                        config.sinkWriteStallTimeoutMs() + "ms")
                                // P2-185: governed retry budget — same knob as
                                // MultiTimeframeSinks, LOG and index stay consistent.
                                .setOption("client.writer.retries",
                                        String.valueOf(config.writerRetries()))
                                .build())
                .name("trade-decisions-sink")
                .uid("trade-decisions-sink");

        // (b) instruction-hash KV index — keyed first-write-wins filter, then
        // upsert with canonical hash recomputed (P4-076)
        //
        // NOTE (P2-065): LOG + index are two independent at-least-once sinks
        // with no atomic commit. LOG duplicates on timeout-retry/restore-replay
        // are expected; downstream must dedup by instruction_id and tolerate
        // LOG-without-index windows (SIG-INT-002). Do not assume 1:1 visibility.
        decisions
                .keyBy(InstructionStateFirstWriteWinsFunction.keySelector(), Types.STRING)
                .process(new InstructionStateFirstWriteWinsFunction())
                .returns(TradeDecisionsTableColumns.ROW_TYPE_INFO)
                .name(INDEX_FILTER_NAME)
                .uid(INDEX_FILTER_UID)
                .map(new TradeDecisionIndexMapper())
                .name("trade-instruction-index-map")
                .uid("trade-instruction-index-map")
                .sinkTo(FlussSink.<RowData>builder()
                                .setBootstrapServers(config.bootstrapServers())
                                .setDatabase(config.database())
                                .setTable(config.tradeInstructionStateTable())
                                .setSerializationSchema(new RowDataSerializationSchema(false, false))
                                .setOption("client.request-timeout",
                                        config.sinkWriteStallTimeoutMs() + "ms")
                                // P2-186: same governed retry budget as the LOG
                                // sink (P2-185) — consistent timeout+retry vs
                                // checkpoint-timeout/restart behavior.
                                .setOption("client.writer.retries",
                                        String.valueOf(config.writerRetries()))
                                .build())
                .name("trade-instruction-state-sink")
                .uid("trade-instruction-state-sink");
    }

    // ── first-write-wins filter — keyed by instruction_id (KV PK) ──────

    /**
     * KV-level first-write-wins guard for the {@code trade_instruction_state}
     * index sink (P4-076, 2026-09-09 — closes the open gap recorded in
     * {@code 25_trade_instruction_state.sql}: the sink path previously had no
     * read-before-write, so a divergent-hash replay upsert could silently
     * overwrite {@code canonical_hash} + {@code first_written_ts}).
     *
     * <p>Keyed by the table primary key {@code instruction_id} (DDL 25 v1,
     * 8 buckets). Forwards the first emission of an id and drops + counts any
     * second emission via {@code compute.trade_decisions.duplicate_instruction}
     * — instructions are immutable and published once (single-writer premise
     * REQ-FLS-008), so any re-arrival is a restore replay and must not
     * overwrite the first write. Empty input produces no elements.
     *
     * <p>TTL/boundedness mirrors {@code MultiTimeframeClosedFirstWriteWinsFunction}:
     * marker is {@code ValueState<Boolean>} with native {@code StateTtlConfig}
     * TTL 24 h (one trading day), {@code OnCreateAndWrite} +
     * {@code NeverReturnExpired}. State contract: exactly one Boolean per
     * emitted instruction id, no timers, no payload.
     */
    public static class InstructionStateFirstWriteWinsFunction
            extends KeyedProcessFunction<String, RowData, RowData> {

        private static final long serialVersionUID = 1L;

        private static final String WRITTEN_STATE_NAME = "trade-instruction-state-written";

        private static final long WRITTEN_MARK_TTL_HOURS = 24L;

        /**
         * Written-marker retention: one trading day. Covers restore-replay
         * lateness and same-day savepoint rollback with wide margin while
         * bounding marker state (mirror of
         * {@code MultiTimeframeClosedFirstWriteWinsFunction.WRITTEN_MARK_TTL}).
         * Package-visible so TTL-expiry test can advance the harness clock.
         */
        static final Duration WRITTEN_MARK_TTL = Duration.ofHours(WRITTEN_MARK_TTL_HOURS);

        private transient ValueState<Boolean> written;
        private transient Counter duplicateInstructions;

        /** The index-stream key selector: PK of trade_instruction_state. */
        public static KeySelector<RowData, String> keySelector() {
            return row -> row.getString(TradeDecisionsTableColumns.INSTRUCTION_ID).toString();
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

            // P4-076: every replay arrival of the same instruction id is
            // counted here — mirror of the candle_closed duplicate counter.
            duplicateInstructions = getRuntimeContext().getMetricGroup()
                    .counter("compute.trade_decisions.duplicate_instruction");
        }

        @Override
        public void processElement(RowData row, Context ctx, Collector<RowData> out)
                throws Exception {
            if (written.value() != null) {
                duplicateInstructions.inc();
                return;
            }
            written.update(true);
            out.collect(row);
        }

        /** Counter-source accessor (tests): the MetricGroup counter value. */
        long duplicateInstructionCountForTest() {
            return duplicateInstructions == null ? 0L : duplicateInstructions.getCount();
        }
    }
}
