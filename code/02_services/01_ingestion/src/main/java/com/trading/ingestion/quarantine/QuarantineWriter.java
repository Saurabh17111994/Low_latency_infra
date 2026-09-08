package com.trading.ingestion.quarantine;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.concurrent.CompletableFuture;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.client.table.writer.AppendResult;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes ingestion-side quarantine evidence to {@code ingestion_quarantine}
 * in Fluss. This is intentionally separate from Action Capture's
 * {@code Postback_Quarantine} schema.
 *
 * <p>Per dossier §B6:</p>
 * <blockquote>
 * Quarantine unsupported/malformed packet evidence.
 * Append to Postback_Quarantine with bytes + reason + timestamp.
 * </blockquote>
 *
 * <p>Column mapping ({@code 21_ingestion_quarantine.sql} — 10 columns, the
 * writer's own DDL; R-277 fixed the reference which pointed at Action
 * Capture's 18-column {@code 16_postback_quarantine.sql}):</p>
 * <pre>
 * quarantine_id       STRING   — UUID
 * reason              STRING   — classifier
 * instrument_token    BIGINT   — may be null if missing
 * exchange            STRING   — may be null
 * symbol              STRING   — may be null
 * raw_payload         BYTES    — original broker frame bytes
 * payload_hash        STRING   — SHA-256 hex
 * detected_ts         BIGINT   — epoch ms
 * detail              STRING   — scrubbed operator detail
 * schema_version      STRING   — "1" (header/manifest v1; P4-330)
 * </pre>
 */
public class QuarantineWriter implements QuarantineSink {

    /** Row schema version — must match the DDL header + manifest (P4-330). */
    static final String SCHEMA_VERSION = "1";

    private static final Logger LOG = LoggerFactory.getLogger(QuarantineWriter.class);

    private static final String TABLE_DB = "default";
    private static final String TABLE_NAME = "ingestion_quarantine";
        private static final Pattern SECRET_PATTERN = Pattern.compile(
            "(?i)(ARROW_APP_SECRET|ARROW_PASSWORD|ARROW_TOTP_KEY|ARROW_TOKEN|access_token|authorization|appID|token)([=:][^&\\s,}]+)");
    /** Runs first so `Bearer <token>` (space-separated) is consumed before the
     *  name=value pattern can eat only the literal `Bearer` and leak the token. */
    private static final Pattern BEARER_PATTERN = Pattern.compile(
            "(?i)\\bBearer[=:\\s]+[^\\s,}]+");

    private final AppendWriter writer;
    private Connection connection; // R-253
    private Table table; // R-253
    private final String instanceId;

    /**
     * Reason — deliberately broad; ingestion can't classify as
     * MISSING_BROKER_ID etc. Those are postback-only categories.
     */
    public enum Reason {
        /** Frame payload is not valid (kept for vocabulary compatibility). */
        MALFORMED_JSON,
        /** JSON parsed but required fields missing or of wrong type. */
        INVALID_SCHEMA,
        /** Instrument token not found in the daily manifest. */
        MISSING_INSTRUMENT,
        /** Price/timestamp/volume failed ValidityClassification. */
        INVALID_VALUES,
        FUTURE_BROKER_TIMESTAMP,
        STALE_BROKER_TIMESTAMP,
        HASH_MISMATCH,
        /** Ingestion-internal processing exception. */
        INTERNAL_ERROR,
        /** Canonical fingerprint could not be computed. */
        FINGERPRINT_FAILURE
    }

    /**
     * Creates a quarantine writer connected to the Fluss coordinator.
     * Inline creates its own {@link AppendWriter} so this table is
     * independent of the raw_table_1 writer.
     */
    public QuarantineWriter(String bootstrapServers, String instanceId) {
        this.instanceId = instanceId;

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrapServers);
        // R-297 wedge fix: bound the writer memory-pool wait (default is
        // infinite) so a wedged sender cannot park the ingestion main thread
        // forever in append() — the quarantine write fails cleanly instead.
        conf.setString("client.writer.buffer.wait-timeout", "30s");

        try {
            // R-253: retain Connection + Table so close() releases them.
            this.connection = ConnectionFactory.createConnection(conf);
            TablePath path = TablePath.of(TABLE_DB, TABLE_NAME);
            this.table = connection.getTable(path);
            this.writer = table.newAppend().createWriter();
            LOG.info("quarantine-writer: connected (table={}, instanceId={})",
                    path, instanceId);
        } catch (Exception e) {
            closeQuietly();
            LOG.error("quarantine-writer: failed to connect to Fluss: {}", e.getMessage(), e);
            throw new RuntimeException("Cannot create QuarantineWriter", e);
        }
    }

    /**
     * Write a quarantined tick. Never throws — failures are logged
     * at ERROR and must not block the ingestion pipeline.
     *
     * @param rawPayload  raw broker frame bytes (may be null if we have no bytes)
     * @param reason      why this is quarantined
     * @param detail      human-readable detail for operator, logged inline
     */
    public void write(byte[] rawPayload, Reason reason, String detail) {
        write(rawPayload, reason, detail, null, null, null);
    }

    /**
     * Write with optional instrument context.
     */
    public void write(byte[] rawPayload, Reason reason, String detail,
                      Long instrumentToken, String exchange, String symbol) {

        String quarantineId = instanceId + "-" + UUID.randomUUID();
        Instant now = Instant.now();
        String payloadHash = computePayloadHash(rawPayload);
        byte[] payload = rawPayload != null ? rawPayload : new byte[0];

        // P1-090: a null reason previously NPE'd at reason.name() OUTSIDE the
        // try, breaking the never-throws contract. Default to INTERNAL_ERROR
        // (an unwitnessed quarantine cause IS an internal error) and build the
        // row inside the try so NOTHING in this method throws.
        String reasonName = safeReasonName(reason);
        String safeDetail = sanitizeDetail(detail);
        try {
            GenericRow row = GenericRow.of(
                    bs(quarantineId), bs(reasonName), instrumentToken,
                    bs(exchange), bs(symbol), payload, bs(payloadHash),
                    now.toEpochMilli(), bs(safeDetail), bs(SCHEMA_VERSION));
            // Fluss appends are asynchronous: failures surface on the future,
            // not by throwing from append(). Observe it so async failures are
            // logged at ERROR and never silently swallowed (R-033).
            observe(writer.append(row), quarantineId, reasonName);
        } catch (Exception e) {
            LOG.error("quarantine-writer: append failed (id={}, reason={}): {}",
                    quarantineId, reasonName, e.getMessage(), e);
        }
    }

    /**
     * P1-090: null-safe reason mapping (package-visible for direct unit
     * testing — write() itself needs a live Fluss writer).
     */
    static String safeReasonName(Reason reason) {
        return reason != null ? reason.name() : Reason.INTERNAL_ERROR.name();
    }

    /**
     * Observe an asynchronous Fluss append (R-033). {@link AppendWriter}
     * completes the returned future exceptionally on broker-side or
     * serialization failures; a discarded future would silently lose the
     * quarantine evidence. Success (DEBUG) is only logged after the future
     * completes; failures are logged at ERROR per the class contract
     * ("failures are logged at ERROR and must not block the ingestion
     * pipeline").
     *
     * @return a future mirroring the append outcome (for tests)
     */
    static CompletableFuture<AppendResult> observe(
            CompletableFuture<AppendResult> future, String id, String detail) {
        return future.whenComplete((result, ex) -> {
            if (ex != null) {
                Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                LOG.error("quarantine-writer: append failed (id={}, detail={}): {}",
                        id, detail, cause.getMessage(), cause);
            } else {
                LOG.debug("quarantine-writer: wrote {} (detail={})", id, detail);
            }
        });
    }

    /** Shorthand to convert a String to Fluss's internal BinaryString type. */
    /** Shorthand to convert a String to Fluss's internal BinaryString type. Returns null for null input (nullable column). */
    private static BinaryString bs(String s) {
        return s != null ? BinaryString.fromString(s) : null;
    }

    @Override
    public void close() {
        try {
            writer.flush();
            // AppendWriter (TableWriter) does not have close() in Fluss 0.9.1-incubating
        } catch (Exception e) {
            LOG.warn("quarantine-writer: close failed: {}", e.getMessage(), e);
        }
        closeQuietly();
    }

    /** R-253: release the Fluss Connection + Table held since construction. */
    private void closeQuietly() {
        try {
            if (table != null) table.close();
        } catch (Exception ignored) {
        }
        try {
            if (connection != null) connection.close();
        } catch (Exception ignored) {
        }
    }

    /**
     * Best-effort SHA-256 hash of payload bytes (P4-222: missing payloads
     * hash their actual bytes — never "" — so every stored row's
     * payload_hash validates its raw_payload and no two distinct causes
     * share one sentinel).
     */
    static String computePayloadHash(byte[] data) {
        if (data == null) {
            data = new byte[0];
        }
        return sha256Hex(data);
    }

    private static String sha256Hex(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(data);
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    static String sanitizeDetail(String value) {
        if (value == null) return "";
        // Two-pass: Bearer tokens first (space-separated), then name=value pairs.
        String safe = BEARER_PATTERN.matcher(value).replaceAll("Bearer=[REDACTED]");
        safe = SECRET_PATTERN.matcher(safe).replaceAll("$1=[REDACTED]");
        return safe.length() > 512 ? safe.substring(0, 512) : safe;
    }
}
