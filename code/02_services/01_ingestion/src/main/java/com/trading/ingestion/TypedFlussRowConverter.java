package com.trading.ingestion;

import com.trading.common.schema.RawTableSchema;
import com.trading.ingestion.model.RawTick;
import com.trading.common.schema.EventDay;
import com.trading.ingestion.model.TickPacket;
import com.trading.ingestion.shutdown.BoundedClose;
import com.trading.ingestion.write.FlussRowConverter;
import com.trading.ingestion.write.RawTickWriter;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.table.writer.AppendResult;
import org.apache.fluss.client.table.writer.TypedAppendWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A/B bench (2026-08-27, forensic audit): Fluss {@code TypedAppendWriter} path.
 *
 * <p>Uses {@code table.newAppend().createTypedWriter(TickRow.class)} — the
 * Fluss SDK's reflection-based POJO → {@code GenericRow} conversion
 * ({@code PojoToRowConverter.toRow}): {@code new GenericRow(fieldCount)} +
 * per-field {@code readAndConvert} reflection per tick, then the same
 * {@code AppendWriter.append(InternalRow)} as {@link RealFlussRowConverter}.
 *
 * <p>Hypothesis (from the forensic audit): the typed path will NOT beat the
 * hand-optimized {@code GenericRow.of(20)} + {@code BinaryString} build —
 * reflection per field is expected to allocate/CPU at least as much. This
 * class exists ONLY to measure that; the default remains the generic path.
 *
 * <p>Column order must match {@code code/01_platform/02_sql/ddl/02_raw_table_1.sql}
 * (20 columns, schema v2 — R-054/R-231). Field names must match column names
 * for the SDK's {@code PojoToRowConverter} to project them.
 */
final class TypedFlussRowConverter implements FlussRowConverter {

    private static final Logger LOG = LoggerFactory.getLogger(TypedFlussRowConverter.class);

    private final TypedAppendWriter<TickRow> writer;
    private final Connection connection;
    private final String tablePath;
    private volatile boolean closed;

    TypedFlussRowConverter(TypedAppendWriter<TickRow> writer, Connection connection, String tablePath) {
        this.writer = writer;
        this.connection = connection;
        this.tablePath = tablePath;
    }

    /** POJO matching the 20-column raw_table_1 DDL (names must equal columns). */
    public static final class TickRow {
        public String event_day;            // partition key, yyyyMMdd IST (v3)
        public String event_fingerprint;
        public String fingerprint_version;
        public String connection_id;
        public Long connection_epoch;
        public Long instrument_token;
        public String exchange;
        public String symbol;
        public Long event_time;
        public Long ingest_ts;
        public Long ack_ts;         // NULLABLE
        public String tick_type;
        public Long last_price_paise;
        public Long last_qty;
        public byte[] raw_payload;
        public String payload_hash;
        public String decoder_version;
        public String protocol_version;
        public String validity_state;
        public String validity_reason;
        public String schema_version;
        // --- v4 (indexes 21-71): all BIGINT NULL. Boxed Long so "absent" is
        // --- distinguishable from 0, which is a real value for each of them. ---
        public Long open_paise;
        public Long high_paise;
        public Long low_paise;
        public Long close_paise;
        public Long vwap_paise;
        public Long volume;
        public Long volume_delta;
        public Long total_buy_qty;
        public Long total_sell_qty;
        public Long open_interest;
        public Long bid_px_1;
        public Long bid_px_2;
        public Long bid_px_3;
        public Long bid_px_4;
        public Long bid_px_5;
        public Long bid_qty_1;
        public Long bid_qty_2;
        public Long bid_qty_3;
        public Long bid_qty_4;
        public Long bid_qty_5;
        public Long bid_ord_1;
        public Long bid_ord_2;
        public Long bid_ord_3;
        public Long bid_ord_4;
        public Long bid_ord_5;
        public Long ask_px_1;
        public Long ask_px_2;
        public Long ask_px_3;
        public Long ask_px_4;
        public Long ask_px_5;
        public Long ask_qty_1;
        public Long ask_qty_2;
        public Long ask_qty_3;
        public Long ask_qty_4;
        public Long ask_qty_5;
        public Long ask_ord_1;
        public Long ask_ord_2;
        public Long ask_ord_3;
        public Long ask_ord_4;
        public Long ask_ord_5;
        public Long change_flag;
        public Long oi_day_high;
        public Long oi_day_low;
        public Long lower_limit_paise;
        public Long upper_limit_paise;
        public Long imbalance_qty;
        public Long indicative_close_paise;
        public Long ref_price_paise;
        public Long last_traded_time;
        public Long atv;
        public Long btv;
    }

    // SC2 (2026-08-29): the POJO above is the SDK's reflection target, so its
    // fields cannot be *derived* from RawTableSchema at runtime — instead we
    // verify field names + count against the single source at class load, so a
    // schema change is one edit (RawTableSchema) and a loud failure here if the
    // POJO lags. Field *types* are pinned by the DDL (STRING/BIGINT/BYTES).
    static {
        java.lang.reflect.Field[] fields = TickRow.class.getDeclaredFields();
        if (fields.length != RawTableSchema.FIELD_COUNT) {
            throw new IllegalStateException("TickRow declares " + fields.length
                    + " fields but RawTableSchema requires " + RawTableSchema.FIELD_COUNT
                    + " — update TickRow or RawTableSchema (SC2)");
        }
        for (int i = 0; i < fields.length; i++) {
            if (!fields[i].getName().equals(RawTableSchema.COLUMNS.get(i))) {
                throw new IllegalStateException("TickRow field #" + i + " is '"
                        + fields[i].getName() + "' but RawTableSchema requires '"
                        + RawTableSchema.COLUMNS.get(i) + "' (SC2)");
            }
        }
    }

    @Override
    public CompletableFuture<RawTickWriter.AppendResult> append(TickPacket packet) {
        if (closed) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Fluss writer is closed"));
        }
        Instant now = Instant.now();
        RawTick raw = packet.raw();

        TickRow row = new TickRow();
        row.event_day = EventDay.ofValidated(packet.eventTime());
        row.event_fingerprint = packet.eventFingerprint();
        row.fingerprint_version = String.valueOf(packet.fingerprintVersion());
        row.connection_id = packet.connectionId();
        row.connection_epoch = packet.connectionEpoch();
        row.instrument_token = packet.instrumentToken();
        row.exchange = packet.exchange();
        row.symbol = packet.tradingSymbol();
        row.event_time = packet.eventTime().toEpochMilli();
        row.ingest_ts = now.toEpochMilli();
        // P1-061: 0 = unknown (R-010), aligned with the generic path (0L) —
        // NULL-vs-0 divergence broke IS NULL vs =0 queries across A/B modes.
        row.ack_ts = 0L;
        // v4: identical rule to the generic path (IngestionService.processTickEvent) --
        // TRADE means this tick actually moved quantity, which is the same quantity the
        // candles sum. The old validity-only rule labelled every non-QUOTE row TRADE,
        // including zero-delta snapshots, so `WHERE tick_type = 'TRADE'` over-counted
        // roughly 6x (live read-back 2026-09-24: 4 of 4 TRADE rows had volume_delta=0).
        Long volumeDelta = packet.volumeDelta();
        row.tick_type = (packet.validity() == com.trading.ingestion.model.ValidityClassification.VALID_TRADE
                && volumeDelta != null && volumeDelta > 0) ? "TRADE" : "QUOTE";
        row.last_price_paise = packet.lastPricePaise();
        row.last_qty = packet.lastQty();
        row.raw_payload = raw != null ? raw.rawPayload() : new byte[0]; // P1-087: retained copy
        row.payload_hash = raw != null ? raw.payloadHash() : "";
        row.decoder_version = raw != null ? raw.decoderVersion() : "go-arrow-sdk";
        row.protocol_version = raw != null ? raw.protocolVersion() : "";
        row.validity_state = packet.validity().name();
        row.validity_reason = packet.validityReason() != null ? packet.validityReason() : "";
        row.schema_version = String.valueOf(packet.schemaVersion());
        // v4: see FlussClientAdapter.append — the boxed values carry NULL through.
        row.open_paise = packet.ohlcOpenPaise();
        row.high_paise = packet.ohlcHighPaise();
        row.low_paise = packet.ohlcLowPaise();
        row.close_paise = packet.ohlcClosePaise();
        row.vwap_paise = packet.averagePricePaise();
        row.volume = packet.volume();
        row.volume_delta = packet.volumeDelta();
        row.total_buy_qty = packet.totalBuyQty();
        row.total_sell_qty = packet.totalSellQty();
        row.open_interest = packet.openInterest();
        row.bid_px_1 = packet.bidPx()[0];
        row.bid_px_2 = packet.bidPx()[1];
        row.bid_px_3 = packet.bidPx()[2];
        row.bid_px_4 = packet.bidPx()[3];
        row.bid_px_5 = packet.bidPx()[4];
        row.bid_qty_1 = packet.bidQty()[0];
        row.bid_qty_2 = packet.bidQty()[1];
        row.bid_qty_3 = packet.bidQty()[2];
        row.bid_qty_4 = packet.bidQty()[3];
        row.bid_qty_5 = packet.bidQty()[4];
        row.bid_ord_1 = packet.bidOrd()[0];
        row.bid_ord_2 = packet.bidOrd()[1];
        row.bid_ord_3 = packet.bidOrd()[2];
        row.bid_ord_4 = packet.bidOrd()[3];
        row.bid_ord_5 = packet.bidOrd()[4];
        row.ask_px_1 = packet.askPx()[0];
        row.ask_px_2 = packet.askPx()[1];
        row.ask_px_3 = packet.askPx()[2];
        row.ask_px_4 = packet.askPx()[3];
        row.ask_px_5 = packet.askPx()[4];
        row.ask_qty_1 = packet.askQty()[0];
        row.ask_qty_2 = packet.askQty()[1];
        row.ask_qty_3 = packet.askQty()[2];
        row.ask_qty_4 = packet.askQty()[3];
        row.ask_qty_5 = packet.askQty()[4];
        row.ask_ord_1 = packet.askOrd()[0];
        row.ask_ord_2 = packet.askOrd()[1];
        row.ask_ord_3 = packet.askOrd()[2];
        row.ask_ord_4 = packet.askOrd()[3];
        row.ask_ord_5 = packet.askOrd()[4];
        row.change_flag = packet.changeFlag();
        row.oi_day_high = packet.oiDayHigh();
        row.oi_day_low = packet.oiDayLow();
        row.lower_limit_paise = packet.lowerLimitPaise();
        row.upper_limit_paise = packet.upperLimitPaise();
        row.imbalance_qty = packet.imbalanceQty();
        row.indicative_close_paise = packet.indicativeClosePaise();
        row.ref_price_paise = packet.refPricePaise();
        row.last_traded_time = packet.lastTradedTimeMs();
        row.atv = packet.atv();
        row.btv = packet.btv();

        return writer.append(row)
                .thenApply(result -> new RawTickWriter.AppendResult(0, tablePath))
                .exceptionally(ex -> {
                    // Propagate failures to RawTickWriter — do NOT swallow into a
                    // success-shaped AppendResult. A swallowed exception previously
                    // masked every append failure as SUCCESS (no FATAL/retry, no
                    // tracker release) and defeated the fail-fast guards. The writer
                    // layer (RawTickWriter.handleCompletion) owns the policy:
                    // RetryClassifier for the cause type, CancellationException →
                    // UNCERTAIN. R-190: ex.getCause() may be null (plain exception).
                    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                    LOG.warn("fluss: typed append failed (table={}): {}", tablePath, cause.getMessage());
                    // P1-074: preserve cancellation identity — handleCompletion
                    // unwraps ONE level for CancellationException→UNCERTAIN; a
                    // fresh RuntimeException wrap would hide it and the timeout
                    // would misclassify as FATAL (fail-closed halt) instead of
                    // deferring dedup to Compute.
                    if (cause instanceof java.util.concurrent.CancellationException ce) {
                        throw ce;
                    }
                    if (cause instanceof RuntimeException re) {
                        throw re;
                    }
                    throw new RuntimeException("Fluss typed append failed", cause);
                });
    }

    @Override
    public int estimatedRowSize(TickPacket packet) {
        RawTick raw = packet.raw();
        return (raw != null ? raw.rawPayloadLength() : 0) + 256; // P1-087
    }

    @Override
    public void close() {
        // P1-075: the TypedAppendWriter was never flushed/closed here —
        // buffered batches were lost and the writer leaked. Flush first,
        // then the connection. Idempotent: double-close flushes once.
        // Residual append-vs-close races converge via the writer error
        // paths (R-069 philosophy), never via a lock on the hot path.
        // The check-and-set alone IS synchronized: concurrent closes must
        // not double-flush/double-close. The I/O below stays outside it.
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        // Bounded release (2026-09-12): append() is fire-and-forget from this
        // writer's side (it hands back a future without awaiting it), so the
        // flush is load-bearing — it is what gives buffered batches a chance to
        // land — and it is retained rather than deleted. flush() and
        // Version note (2026-09-23): the 0.9.1 claims in this file were re-checked against Fluss 1.0.0 and still hold — flush()/close is still unbounded in 1.0.0 (`fluss-client/.../write/RecordAccumulator.java:149`, unchanged since 0.9.1) and `TableWriter`/`UpsertWriter` still expose no `close()`. Re-check on the next upgrade (DEC-052).
        // connection.close() are both unbounded in Fluss 0.9.1; the whole
        // release is bounded here so a wedged cluster cannot hang shutdown.
        // See BoundedClose.
        BoundedClose.run("typed-fluss-converter", () -> {
            try {
                writer.flush();
            } catch (Exception e) {
                LOG.warn("fluss: typed converter flush failed: {}", e.getMessage());
            }
            try {
                connection.close();
            } catch (Exception e) {
                LOG.warn("fluss: typed converter close failed: {}", e.getMessage());
            }
        });
    }
}
