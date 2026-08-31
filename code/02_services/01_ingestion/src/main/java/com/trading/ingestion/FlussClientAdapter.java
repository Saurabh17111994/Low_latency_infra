package com.trading.ingestion;

import com.trading.ingestion.model.RawTick;
import com.trading.ingestion.model.TickPacket;
import com.trading.common.schema.EventDay;
import com.trading.ingestion.write.FlussRowConverter;
import com.trading.ingestion.write.RawTickWriter;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendResult;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.TypedAppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fluss client adapter — connects to a Fluss cluster and provides a row
 * converter backed by {@link AppendWriter} for {@code raw_table_1}.
 *
 * <h3>Real Fluss API (0.9.1-incubating)</h3>
 * <ol>
 *   <li>{@link ConnectionFactory#createConnection(Configuration)} — bootstrap</li>
 *   <li>{@link Connection#getTable(TablePath)} — access the LOG table</li>
 *   <li>{@link Table#newAppend()} → {@link AppendWriter} — create writer</li>
 *   <li>{@link AppendWriter#append(org.apache.fluss.row.InternalRow)} → {@link CompletableFuture}</li>
 * </ol>
 *
 * <p>Each accepted tick is converted to a {@link GenericRow} matching the
 * 28-column {@code raw_table_1} DDL and appended individually (no batching).
 *
 * <p>See {@code /home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/fluss}
 * for the upstream Fluss source (Apache 2.0 licensed).
 */
final class FlussClientAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(FlussClientAdapter.class);

    /** Writer mode for the A/B bench: generic (default) or typed. */
    enum WriterMode {
        GENERIC,
        TYPED
    }

    private FlussClientAdapter() {}

    /**
     * Connect to Fluss, get the table, create an append writer, and return
     * a row converter that uses the real client.
     */
    static FlussRowConverter connect(String bootstrapServers, String tablePath,
                                      int writerBatchTimeoutMs) {
        return connect(bootstrapServers, tablePath, writerBatchTimeoutMs, 0, WriterMode.GENERIC);
    }

    static FlussRowConverter connect(String bootstrapServers, String tablePath,
                                      int writerBatchTimeoutMs, int writerBatchSizeBytes) {
        return connect(bootstrapServers, tablePath, writerBatchTimeoutMs,
                writerBatchSizeBytes, WriterMode.GENERIC);
    }

    /**
     * A/B bench (2026-08-27, forensic audit): connect with an explicit writer
     * mode. {@code GENERIC} = our hand-optimized {@code GenericRow.of(20)} path
     * (default, locked); {@code TYPED} = Fluss {@code TypedAppendWriter} POJO
     * reflection path. The typed path exists ONLY to measure whether the SDK's
     * POJO converter beats our explicit build — it is not the default.
     */
    static FlussRowConverter connect(String bootstrapServers, String tablePath,
                                      int writerBatchTimeoutMs, int writerBatchSizeBytes,
                                      WriterMode writerMode) {
        LOG.info("fluss: connecting (bootstrap={}, table={}, mode={})",
                bootstrapServers, tablePath, writerMode);

        // 1. Configure bootstrap
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrapServers);
        // O-2 RESOLVED 2026-08-27: linger 1ms (measured p99 10.5ms vs 38ms @
        // 20ms — THR-PROBE-002). Config-driven (FLUSS_WRITER_BATCH_TIMEOUT_MS)
        // so T8 can sweep. Default is 100ms — never fall back to it.
        conf.setString("client.writer.batch-timeout", writerBatchTimeoutMs + "ms");
        // Forensic audit 2026-08-27 (Fluss source lever): pin the batch at the
        // measured optimum and DISABLE the client's dynamic batch-size estimator.
        // Fluss default batch-size is 2mb and dynamic-batch-size.enabled=true
        // grows batches toward that cap at sustained throughput — the Exp 4
        // 1MiB cliff (e2e p99 696ms vs 24ms at 64KiB) is that estimator running.
        // Pinning 64KiB + disabling dynamic = quality-preserving (same data,
        // same acks/idempotence) and prevents the 29x latency regression.
        // FLUSS_WRITER_BATCH_SIZE_BYTES (A/B sweep) overrides when >0; 0 = pin 64KiB.
        int batchSizeBytes = writerBatchSizeBytes > 0 ? writerBatchSizeBytes : 64 * 1024;
        conf.setString("client.writer.batch-size", batchSizeBytes + "b");
        conf.setString("client.writer.dynamic-batch-size.enabled", "false");
        // R-297 wedge fix: bound the writer memory-pool wait. The default
        // client.writer.buffer.wait-timeout is infinite — when the sender
        // thread is wedged (leaderless tables) the 64MB pool exhausts and
        // append() parks the calling thread forever, stalling the whole
        // ingestion pipeline. 30s converts that park into a sync EOFException.
        conf.setString("client.writer.buffer.wait-timeout", "30s");

        // 2. Create connection
        Connection connection = ConnectionFactory.createConnection(conf);

        // 3. Parse table path "database.table_name"
        TablePath path = parseTablePath(tablePath);

        // 4. Get table handle
        Table table = connection.getTable(path);

        // 5. Verify schema — TableInfo contains the table schema
        org.apache.fluss.metadata.TableInfo info = table.getTableInfo();
        LOG.info("fluss: schema verified (table={}, schemaId={}, columns={})",
                tablePath, info.getSchemaId(), info.getRowType().getFieldCount());

        // 6. Create append writer (starts background Sender + MetadataUpdater).
        //    A/B: typed mode uses the SDK's reflection POJO converter instead
        //    of our explicit GenericRow build (measured — see forensic audit).
        if (writerMode == WriterMode.TYPED) {
            TypedAppendWriter<TypedFlussRowConverter.TickRow> typedWriter =
                    table.newAppend().createTypedWriter(TypedFlussRowConverter.TickRow.class);
            LOG.info("fluss: connected (table={}, path={}, mode=TYPED)", tablePath, path);
            return new TypedFlussRowConverter(typedWriter, connection, path.toString());
        }

        AppendWriter appendWriter = table.newAppend().createWriter();
        LOG.info("fluss: connected (table={}, path={})", tablePath, path);
        return new RealFlussRowConverter(appendWriter, connection, path.toString());
    }

    private static TablePath parseTablePath(String tablePath) {
        int dot = tablePath.indexOf('.');
        if (dot > 0) {
            return TablePath.of(tablePath.substring(0, dot), tablePath.substring(dot + 1));
        }
        // default database
        return TablePath.of("default", tablePath);
    }
}

/**
 * Real Fluss row converter — converts {@link TickPacket} → {@link GenericRow}
 * and appends through {@link AppendWriter}.
 *
 * <p>Column order matches {@code code/01_platform/02_sql/ddl/02_raw_table_1.sql}:
 */
class RealFlussRowConverter implements FlussRowConverter {

    private static final Logger LOG = LoggerFactory.getLogger(RealFlussRowConverter.class);

    private final AppendWriter writer;
    private final Connection connection;
    private final String tablePath;
    private volatile boolean closed;

    RealFlussRowConverter(AppendWriter writer, Connection connection, String tablePath) {
        this.writer = writer;
        this.connection = connection;
        this.tablePath = tablePath;
    }

    /** Shorthand to convert a String to Fluss's internal BinaryString type. */
    private static BinaryString bs(String s) {
        return s != null ? BinaryString.fromString(s) : BinaryString.EMPTY_UTF8;
    }

    /**
     * Convert a {@link TickPacket} to a {@link GenericRow} matching the
     * {@code raw_table_1} DDL column order, then append to Fluss.
     *
     * <p>DDL column order (20 columns, schema v2 — R-054/R-231 removed the
     * bid/ask and option-metadata columns that the bridge never populates):
     * <pre>{@code
     *   event_day, event_fingerprint, fingerprint_version, connection_id, connection_epoch,
     *   instrument_token, exchange, symbol, event_time, ingest_ts, ack_ts,
     *   tick_type, last_price_paise, last_qty, raw_payload, payload_hash,
     *   decoder_version, protocol_version, validity_state, validity_reason,
     *   schema_version
     * }</pre>
     */
    @Override
    public CompletableFuture<RawTickWriter.AppendResult> append(TickPacket packet) {
        if (closed) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Fluss writer is closed"));
        }

        Instant now = Instant.now();
        RawTick raw = packet.raw();

        GenericRow row = GenericRow.of(
                // partition (v3: daily yyyyMMdd IST — order MUST match DDL)
                bs(EventDay.of(packet.eventTime())),                // event_day STRING (partition key)
                // identity and routing
                bs(packet.eventFingerprint()),                      // event_fingerprint STRING
                bs(String.valueOf(packet.fingerprintVersion())),   // fingerprint_version STRING
                bs(packet.connectionId()),                          // connection_id STRING
                packet.connectionEpoch(),                           // connection_epoch BIGINT
                packet.instrumentToken(),                           // instrument_token BIGINT
                bs(packet.exchange()),                              // exchange STRING
                bs(packet.tradingSymbol()),                         // symbol STRING
                // timestamps
                packet.eventTime().toEpochMilli(),                  // event_time BIGINT
                now.toEpochMilli(),                                 // ingest_ts BIGINT
                0L,                                                 // ack_ts BIGINT NULL — 0 = unknown
                                                                    // (R-010: Fluss LOG rows are immutable;
                                                                    //  the broker ack time is not known at
                                                                    //  row-build time)
                // trade fields
                // R-244: explicit enum comparison — the substring match on
                // validity().name().contains("NON_TRADE") silently reclassified
                // any future enum whose name merely contained the substring.
                bs(packet.validity() == com.trading.ingestion.model.ValidityClassification.VALID_NON_TRADE ? "QUOTE" : "TRADE"),
                packet.lastPricePaise(),                            // last_price_paise BIGINT
                packet.volume(),                                    // last_qty BIGINT
                // payload preservation
                raw != null ? raw.rawPayloadUnsafe() : new byte[0],       // raw_payload BYTES
                bs(raw != null ? raw.payloadHash() : ""),           // payload_hash STRING
                bs(raw != null ? raw.decoderVersion() : "go-arrow-sdk"), // decoder_version STRING
                bs(raw != null ? raw.protocolVersion() : ""),       // protocol_version STRING
                // provenance
                bs(packet.validity().name()),                       // validity_state STRING
                bs(packet.validityReason() != null ? packet.validityReason() : ""), // validity_reason
                bs(String.valueOf(packet.schemaVersion()))          // schema_version STRING
        );

        return writer.append(row)
                .thenApply(result -> {
                    // Record ack timestamp
                    long appendCount = 0; // counter is external (in tracker)
                    return new RawTickWriter.AppendResult(appendCount, tablePath);
                })
                .exceptionally(ex -> {
                    // R-190: ex.getCause() may be null (plain exception).
                    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                    LOG.warn("fluss: append failed (table={}): {}", tablePath, cause.getMessage());
                    throw new RuntimeException("Fluss append failed", cause);
                });
    }

    /**
     * Estimate row size in bytes for backpressure accounting.
     * Conservative estimate: ~512 bytes fixed + raw payload size.
     */
    @Override
    public int estimatedRowSize(TickPacket packet) {
        int payloadSize = (packet.raw() != null && packet.raw().rawPayloadUnsafe() != null)
                ? packet.raw().rawPayloadUnsafe().length : 512;
        return 512 + payloadSize;
    }

    @Override
    public void close() {
        closed = true;
        try {
            connection.close();
        } catch (Exception e) {
            LOG.warn("fluss: close error", e);
        }
        LOG.info("fluss: closed (table={})", tablePath);
    }
}
