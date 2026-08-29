package com.trading.ingestion;

import com.trading.common.schema.RawTableSchema;
import com.trading.ingestion.model.RawTick;
import com.trading.ingestion.model.TickPacket;
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
        row.event_fingerprint = packet.eventFingerprint();
        row.fingerprint_version = String.valueOf(packet.fingerprintVersion());
        row.connection_id = packet.connectionId();
        row.connection_epoch = packet.connectionEpoch();
        row.instrument_token = packet.instrumentToken();
        row.exchange = packet.exchange();
        row.symbol = packet.tradingSymbol();
        row.event_time = packet.eventTime().toEpochMilli();
        row.ingest_ts = now.toEpochMilli();
        row.ack_ts = null; // 0 = unknown (R-010)
        row.tick_type = packet.validity() == com.trading.ingestion.model.ValidityClassification.VALID_NON_TRADE ? "QUOTE" : "TRADE";
        row.last_price_paise = packet.lastPricePaise();
        row.last_qty = packet.volume();
        row.raw_payload = raw != null ? raw.rawPayloadUnsafe() : new byte[0];
        row.payload_hash = raw != null ? raw.payloadHash() : "";
        row.decoder_version = raw != null ? raw.decoderVersion() : "go-arrow-sdk";
        row.protocol_version = raw != null ? raw.protocolVersion() : "";
        row.validity_state = packet.validity().name();
        row.validity_reason = packet.validityReason() != null ? packet.validityReason() : "";
        row.schema_version = String.valueOf(packet.schemaVersion());

        return writer.append(row)
                .thenApply(result -> new RawTickWriter.AppendResult(0, tablePath))
                .exceptionally(ex -> {
                    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                    LOG.warn("fluss: typed append failed: {}", cause.getMessage());
                    return new RawTickWriter.AppendResult(0, tablePath);
                });
    }

    @Override
    public int estimatedRowSize(TickPacket packet) {
        RawTick raw = packet.raw();
        return (raw != null ? raw.rawPayloadUnsafe().length : 0) + 256;
    }

    @Override
    public void close() {
        closed = true;
        try {
            connection.close();
        } catch (Exception e) {
            LOG.warn("fluss: typed converter close failed: {}", e.getMessage());
        }
    }
}
