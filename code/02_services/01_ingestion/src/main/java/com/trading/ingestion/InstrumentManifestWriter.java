package com.trading.ingestion;

import com.trading.ingestion.shutdown.BoundedClose;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operator instrument-loader persistence: writes an approved instrument
 * manifest into the {@code instruments} KV table through the raw Fluss
 * client.
 *
 * <p><b>First production composite-PK raw-client writer.</b> The table's
 * primary key is {@code (instrument_token, manifest_version)} — composite.
 * Fluss 0.9.1's raw client can upsert composite-PK KV tables only when the
 * table carries {@code table.kv.format-version=2} AND the bucket key is a
 * single-field subset of the PK ({@code CompactedKeyEncoder} path); otherwise
 * {@code IcebergKeyEncoder} throws "Key fields must have exactly one field
 * for iceberg format". {@code instruments} is configured exactly that way
 * (DDL v3, 2026-08-15) and this writer is the proving consumer — the matrix
 * itself is permanently re-verified by COMPAT-FLUSS-005
 * ({@code CompatFlussCompositeKeyIntegrationTest}). The Flink connector does
 * not need this configuration; raw-client writers do.
 *
 * <p>Semantics: per-row upsert keyed on the composite PK, so re-loading the
 * same manifest version overwrites its rows (idempotent) while a new version
 * appends alongside — the R-090 contract "current AND prior manifest
 * versions". The writer fails closed: an empty manifest or duplicate
 * composite keys are rejected before any write, and the constructor verifies
 * the target table is the composite-PK KV shape this writer depends on.
 */
public final class InstrumentManifestWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(InstrumentManifestWriter.class);

    private static final String TABLE_DB = "default";
    private static final String DEFAULT_TABLE_NAME = "instruments";
    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    /** Primary key the writer upserts against (must match the DDL exactly). */
    private static final List<String> COMPOSITE_PK = List.of("instrument_token", "manifest_version");

    /** One full row of the {@code instruments} DDL (14 columns, DDL order). */
    public record ManifestEntry(
            long instrumentToken,
            String tradingSymbol,
            String exchange,
            String segment,
            String instrumentType,
            int lotSize,
            Long tickSizePaise,
            Long strikePaise,
            Long expiry,
            String optionType,
            int manifestVersion,
            boolean isActive,
            long loadedTs,
            String schemaVersion) {

        /** R-115/R-116/R-193 discipline: routing identity + keys are validated, never defaulted. */
        public ManifestEntry {
            if (instrumentToken <= 0) {
                throw new IllegalArgumentException(
                        "instrumentToken must be positive, got " + instrumentToken);
            }
            if (tradingSymbol == null || tradingSymbol.isBlank()) {
                throw new IllegalArgumentException("tradingSymbol must not be blank");
            }
            if (exchange == null || exchange.isBlank()) {
                throw new IllegalArgumentException("exchange must not be blank");
            }
            if (segment == null || segment.isBlank()) {
                throw new IllegalArgumentException("segment must not be blank (DDL NOT NULL)");
            }
            if (lotSize <= 0) {
                throw new IllegalArgumentException("lotSize must be positive, got " + lotSize);
            }
            // P4-066: tick/expiry/option coherence — invalid ticks/lots must
            // fail at the loader, never propagate to sizing and risk.
            if (tickSizePaise != null && tickSizePaise <= 0) {
                throw new IllegalArgumentException(
                        "tickSizePaise must be positive, got " + tickSizePaise);
            }
            boolean isOpt = "OPT".equals(instrumentType);
            boolean isEquity = "EQUITY".equals(instrumentType);
            if (optionType != null && !(optionType.equals("CE") || optionType.equals("PE"))) {
                throw new IllegalArgumentException(
                        "optionType must be CE/PE, got " + optionType);
            }
            if (isOpt && optionType == null) {
                throw new IllegalArgumentException(
                        "optionType (CE/PE) required when instrumentType=OPT");
            }
            if (!isOpt && optionType != null) {
                throw new IllegalArgumentException(
                        "optionType must be null unless instrumentType=OPT, got " + optionType);
            }
            if (isEquity && expiry != null) {
                throw new IllegalArgumentException(
                        "expiry must be null for EQUITY (non-expiring)");
            }
            if (manifestVersion <= 0) {
                throw new IllegalArgumentException(
                        "manifestVersion must be positive, got " + manifestVersion);
            }
            if (schemaVersion == null || schemaVersion.isBlank()) {
                throw new IllegalArgumentException("schemaVersion must not be blank");
            }
        }
    }

    private final Connection connection;
    private final Table table;
    private final UpsertWriter writer;
    private final String tableName;

    /** Default target: the platform {@code instruments} table. */
    public InstrumentManifestWriter(String bootstrapServers) {
        this(bootstrapServers, DEFAULT_TABLE_NAME);
    }

    /** Write to a named table (scratch names in tests; never a platform table other than instruments). */
    public InstrumentManifestWriter(String bootstrapServers, String tableName) {
        this.tableName = tableName;
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrapServers);
        // Bound the writer memory-pool wait (R-297 wedge fix, same rationale
        // as SafetyHaltWriter) so a wedged sender fails cleanly instead of
        // parking the calling thread forever.
        conf.setString("client.writer.buffer.wait-timeout", "30s");
        TablePath path = TablePath.of(TABLE_DB, tableName);
        try {
            this.connection = ConnectionFactory.createConnection(conf);
            // Preflight: this writer depends on the composite-PK KV shape
            // (kv.format-version=2 + single-field subset bucket key). Verify
            // the effective table before writing — a wrong table must fail
            // fast with a clear message, not a raw IcebergKeyEncoder error.
            TableInfo info = connection.getAdmin().getTableInfo(path)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            if (info == null || !info.hasPrimaryKey()) {
                throw new IllegalStateException(
                        "instrument-manifest-writer: " + path + " is not a KV table");
            }
            if (!info.getPrimaryKeys().equals(COMPOSITE_PK)) {
                throw new IllegalStateException("instrument-manifest-writer: " + path
                        + " primary key " + info.getPrimaryKeys()
                        + " must be " + COMPOSITE_PK + " (composite-PK KV contract)");
            }
            if (!info.getBucketKeys().equals(List.of("instrument_token"))) {
                throw new IllegalStateException("instrument-manifest-writer: " + path
                        + " bucket keys " + info.getBucketKeys()
                        + " must be [instrument_token] (single-field subset of the PK — "
                        + "the raw client's composite-PK path, kv.format-version=2)");
            }
            // P1-231: the version gate — PK + bucket key alone can PASS on a
            // v1 table that still dies later with the raw IcebergKeyEncoder
            // error this preflight claims to prevent. A missing key means a
            // pre-key-era (v1) table, which must also fail, not sail through.
            requireKvFormatVersion(path, info.getProperties().toMap().get("table.kv.format-version"));
            this.table = connection.getTable(path);
            this.writer = table.newUpsert().createWriter();
            LOG.info("instrument-manifest-writer: connected (table={}, composite-PK KV preflight PASS)",
                    path);
        } catch (InterruptedException ie) {
            closeQuietly();
            Thread.currentThread().interrupt();
            throw new RuntimeException("Cannot create InstrumentManifestWriter (interrupted)", ie);
        } catch (Exception e) {
            closeQuietly();
            if (e instanceof IllegalStateException ise) {
                throw ise;
            }
            LOG.error("instrument-manifest-writer: failed to connect to Fluss: {}", e.getMessage(), e);
            throw new RuntimeException("Cannot create InstrumentManifestWriter", e);
        }
    }

    // P1-231: pure version gate over the extracted property value, so the
    // refusal is pinnable without a live cluster. Visible for testing.
    static void requireKvFormatVersion(TablePath path, String kvFormat) {
        if (!"2".equals(kvFormat)) {
            throw new IllegalStateException("instrument-manifest-writer: " + path
                    + " table.kv.format-version=" + kvFormat
                    + " must be 2 (raw-client composite-PK path)");
        }
    }

    /**
     * Validate a manifest before any write: non-empty, no duplicate composite
     * keys. Fails closed — an operator never half-loads a broken manifest.
     */
    static void validate(List<ManifestEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            throw new IllegalArgumentException(
                    "instrument-manifest-writer: refusing to write an empty instrument manifest");
        }
        Set<String> seen = new HashSet<>();
        for (ManifestEntry e : entries) {
            // P1-232: null element — fail-closed with the documented
            // IllegalArgumentException (a plain NPE carried no context).
            if (e == null) {
                throw new IllegalArgumentException(
                        "instrument-manifest-writer: null manifest entry — refusing to write");
            }
            String key = e.instrumentToken() + ":" + e.manifestVersion();
            if (!seen.add(key)) {
                throw new IllegalArgumentException(
                        "instrument-manifest-writer: duplicate composite key in one manifest: "
                                + key + " (" + e.tradingSymbol() + ") — a manifest must carry "
                                + "each (instrument_token, manifest_version) once");
            }
        }
    }

    /** Build the DDL-order GenericRow for one entry (14 columns). */
    static InternalRow toRow(ManifestEntry e) {
        return GenericRow.of(
                e.instrumentToken(),                                      // instrument_token BIGINT
                BinaryString.fromString(e.tradingSymbol()),               // trading_symbol STRING
                BinaryString.fromString(e.exchange()),                    // exchange STRING
                BinaryString.fromString(e.segment()),                     // segment STRING
                e.instrumentType() == null ? null                         // instrument_type STRING
                        : BinaryString.fromString(e.instrumentType()),
                e.lotSize(),                                              // lot_size INT
                e.tickSizePaise(),                                        // tick_size_paise BIGINT
                e.strikePaise(),                                          // strike_paise BIGINT
                e.expiry(),                                               // expiry BIGINT
                e.optionType() == null ? null                             // option_type STRING
                        : BinaryString.fromString(e.optionType()),
                e.manifestVersion(),                                      // manifest_version INT
                e.isActive(),                                             // is_active BOOLEAN
                e.loadedTs(),                                             // loaded_ts BIGINT
                BinaryString.fromString(e.schemaVersion()));              // schema_version STRING
    }

    /**
     * Upsert the manifest rows against the composite PK. Idempotent per
     * (instrument_token, manifest_version); a new version is retained
     * alongside prior ones (R-090). Returns the number of rows written.
     */
    public int write(List<ManifestEntry> entries) throws Exception {
        validate(entries);
        // P1-071: NOT atomic - Fluss KV has no multi-row transaction. A mid-loop
        // failure leaves a prefix persisted (idempotent per composite PK, so the
        // caller must retry the full manifest to converge). Do not claim
        // never-half-loads; the thrown exception carries the written prefix size.
        // P1-233: fire ALL upserts first, then await each — the old
        // upsert-then-get per row serialized 1024 round-trips (~5s at 5ms
        // each); pipelined it converges in ~max-latency instead (~0.5s).
        // Same semantics: each future still gets the full TIMEOUT, written++
        // only on success, first failure throws with the prefix count.
        java.util.List<java.util.concurrent.CompletableFuture<?>> futures =
                new java.util.ArrayList<>(entries.size());
        for (ManifestEntry e : entries) {
            futures.add(writer.upsert(toRow(e)));
        }
        int written = 0;
        for (java.util.concurrent.CompletableFuture<?> f : futures) {
            f.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            written++;
        }
        // No flush here (2026-09-12): every future above was awaited, so it is a
        // no-op on the success path, and on the failure path f.get() throws before
        // reaching this line — it was unreachable, never load-bearing. See
        // BoundedClose for why a bare flush() is not a free call anyway.
        LOG.info("instrument-manifest-writer: upserted {} rows into {} "
                + "(composite-PK raw-client path)", written, tableName);
        return written;
    }

    @Override
    public void close() {
        // Bounded release (2026-09-12): RETAINED here (unlike the write-path flush
        // above), because on the mid-loop failure path this is the only thing that
        // gives the un-awaited stragglers a chance to land — load-bearing, so it is
        // bounded rather than deleted (flush() and Connection.close() are both
        // Version note (2026-09-23): the 0.9.1 claims in this file were re-checked against Fluss 1.0.0 and still hold — flush()/close is still unbounded in 1.0.0 (`fluss-client/.../write/RecordAccumulator.java:149`, unchanged since 0.9.1) and `TableWriter`/`UpsertWriter` still expose no `close()`. Re-check on the next upgrade (DEC-052).
        // unbounded in Fluss 0.9.1; see BoundedClose).
        BoundedClose.run("instrument-manifest-writer", () -> {
            try {
                writer.flush();
            } catch (Exception e) {
                LOG.warn("instrument-manifest-writer: final flush failed: {}", e.getMessage());
            }
            closeQuietly();
        });
    }

    /** R-141: release the Fluss Connection + Table held since construction. */
    private void closeQuietly() {
        try {
            if (table != null) {
                table.close();
            }
        } catch (Exception ignored) {
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception ignored) {
        }
    }
}
