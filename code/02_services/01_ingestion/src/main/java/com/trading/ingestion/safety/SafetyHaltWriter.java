package com.trading.ingestion.safety;

import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.concurrent.CompletableFuture;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes slot-scoped safety requests to {@code Safety_Halt_Requests}
 * (plan Amendment §Slot-scoped safety propagation).
 *
 * <p>One row per unsafe/recovered transition. {@code halt_request_id} is the
 * SHA-256 hex of {@code manifest_fingerprint|slot_id|connection_epoch|state|
 * reason_code}. The table is a KV table (DDL v3, review R-089), so the storage
 * layer enforces one row per {@code halt_request_id}: a duplicate delivery is
 * an upsert no-op. The caller additionally dedups so it never emits a second
 * state transition. The request never contains credentials, raw payload bytes,
 * token lists, symbol lists, or free-form SDK exceptions.
 *
 * <p>Row schema version {@code "3"} (DDL header v3; contract_version stays 2 —
 * the parser gates on contract_version, not schema_version).
 */
public final class SafetyHaltWriter implements SafetySink {

    /** Row schema version — must match the DDL header (P4-213 class: writer/header drift). */
    static final String SCHEMA_VERSION = "3";

    private static final Logger LOG = LoggerFactory.getLogger(SafetyHaltWriter.class);

    private static final String TABLE_DB = "default";
    private static final String TABLE_NAME = "Safety_Halt_Requests";
    private static final String CONTRACT_VERSION = "2";

    /**
     * Ingestion safety reason codes (plan Amendment — exact list, extended
     * additively 2026-08-08 with the market-data quality-class codes; the
     * extension is additive so existing halt_request_id tuples are stable).
     */
    public enum ReasonCode {
        FEED_STALLED,
        HEARTBEAT_FAILED,
        READ_FAILURE,
        SUBSCRIPTION_PARTIAL,
        SUBSCRIPTION_TIMEOUT,
        AUTH_FAILURE,
        DECODE_ERROR_BURST,
        BRIDGE_EXIT,
        RESOURCE_EXHAUSTED,
        // Market-data quality class (plan §Market-data quality classification):
        // slot-unsafe evidence for records the broker itself mis-dated.
        // Mirrors QuarantineWriter.Reason vocabulary. (BROKER_LIMIT_VIOLATION
        // was removed 2026-08-14 with the Standard feed — the circuit-limit
        // check relied on standard-feed lower/upper limit fields.)
        FUTURE_BROKER_TIMESTAMP,
        STALE_BROKER_TIMESTAMP
    }

    /** Safety state of a slot as seen by the Signal job. */
    public enum SafetyState {
        UNSAFE,
        RECOVERED
    }

    private final UpsertWriter writer;
    private Connection connection; // R-141
    private Table table; // R-141
    private final String sourceInstance;
    private final String manifestFingerprint;
    private final String accountScopeId;

    /**
     * @param bootstrapServers    Fluss coordinator bootstrap
     * @param sourceInstance      ingestion instance id (source_instance)
     * @param manifestFingerprint deterministic manifest fingerprint (plan)
     * @param accountScopeId      account scope id (existing DDL column)
     */
    public SafetyHaltWriter(String bootstrapServers, String sourceInstance,
                            String manifestFingerprint, String accountScopeId) {
        // P1-091: these land in DDL NOT NULL columns (manifest_fingerprint,
        // account_scope_id) — fail at construction, not on the first write.
        if (manifestFingerprint == null || manifestFingerprint.isBlank()) {
            throw new IllegalArgumentException("manifestFingerprint must not be blank");
        }
        if (accountScopeId == null || accountScopeId.isBlank()) {
            throw new IllegalArgumentException("accountScopeId must not be blank");
        }
        this.sourceInstance = sourceInstance;
        this.manifestFingerprint = manifestFingerprint;
        this.accountScopeId = accountScopeId;

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrapServers);
        // R-297 wedge fix: bound the writer memory-pool wait (default is
        // infinite) so a wedged sender cannot park the calling thread forever
        // in append() — the safety-halt write fails cleanly instead.
        conf.setString("client.writer.buffer.wait-timeout", "30s");
        try {
            // R-141: keep the Connection + Table so close() can release them
            // (previously local vars leaked until process exit).
            this.connection = ConnectionFactory.createConnection(conf);
            TablePath path = TablePath.of(TABLE_DB, TABLE_NAME);
            this.table = connection.getTable(path);
            this.writer = table.newUpsert().createWriter();
            LOG.info("safety-halt-writer: connected (table={}, instance={})", path, sourceInstance);
        } catch (Exception e) {
            closeQuietly();
            LOG.error("safety-halt-writer: failed to connect to Fluss: {}", e.getMessage(), e);
            throw new RuntimeException("Cannot create SafetyHaltWriter", e);
        }
    }

    /**
     * Write one safety request row.
     *
     * @param slotId             slot id (hft-N)
     * @param connectionEpoch    slot epoch at the transition
     * @param state              UNSAFE or RECOVERED
     * @param reasonCode         exact plan reason code (UNSAFE only)
     * @param assignedTokenHash  SHA-256 of the slot's sorted token set
     * @param evidenceReference  optional link to discontinuity/quarantine evidence
     * @param detectedTsMs       epoch ms of the transition
     * @return the computed halt_request_id (for caller-side dedup)
     */
    public String write(String slotId, long connectionEpoch, SafetyState state,
                        ReasonCode reasonCode, String assignedTokenHash,
                        String evidenceReference, long detectedTsMs) {
        // P1-091: fail fast — a null state NPE'd with no row written, an
        // UNSAFE with null reasonCode produced reason="" which the downstream
        // parser rejects (slot stays un-halted), and blank slotIds violate
        // DDL NOT NULL. RECOVERED carries null reasonCode by contract.
        requireWritableArgs(slotId, state, reasonCode);
        String reason = reasonCode != null ? reasonCode.name() : "";
        String haltRequestId = computeHaltRequestId(
                manifestFingerprint, slotId, connectionEpoch, state.name(), reason);
        // R-255: the unused `Instant now` was dead code — the row carries
        // detectedTsMs; no local clock value is needed here.

        GenericRow row = GenericRow.of(
                bs(haltRequestId),
                bs(accountScopeId),
                null,                                   // portfolio_id
                null,                                   // execution_partition_id
                bs("INGESTION"),                        // source_component
                bs(sourceInstance),                     // source_instance
                bs(reason),                             // reason_code
                bs(reason.isEmpty() ? null : reason),   // reason_detail (scrubbed)
                detectedTsMs,                           // detection_time
                connectionEpoch,                        // source_epoch
                bs(assignedTokenHash),                  // evidence_hash
                bs("OPEN"),                             // application_result
                null,                                   // applied_ts
                bs(SCHEMA_VERSION),                     // schema_version
                bs(slotId),                             // slot_id
                connectionEpoch,                        // connection_epoch
                bs(manifestFingerprint),                // manifest_fingerprint
                bs(assignedTokenHash),                  // assigned_token_set_hash
                bs(state.name()),                       // state
                bs(evidenceReference != null ? evidenceReference : ""), // evidence_reference
                Integer.parseInt(CONTRACT_VERSION)      // contract_version
        );

        try {
            // Fluss upserts are asynchronous: failures complete the future
            // exceptionally, never by throwing from upsert(). Observe so an
            // undelivered safety-halt request is logged at ERROR, and success
            // is only logged after the write actually completes (R-034).
            observe(writer.upsert(row), haltRequestId, slotId, state, reason, connectionEpoch);
        } catch (Exception e) {
            // P1-092: fail VISIBLY — returning the id after a swallowed sync
            // failure made a lost UNSAFE halt indistinguishable from success
            // (and the caller's dedup entry blocked any retry). Callers catch
            // this, evict their dedup entry, and retry on the next tick
            // (idempotent KV upsert makes retry safe).
            LOG.error("safety-halt-writer: append failed (id={}, reason={}): {}",
                    haltRequestId, reason, e.getMessage());
            throw new RuntimeException("safety halt write failed (id=" + haltRequestId + ")", e);
        }
        return haltRequestId;
    }

    /**
     * P1-091: argument contract (package-visible for direct unit testing —
     * write() itself needs a live Fluss writer).
     */
    static void requireWritableArgs(String slotId, SafetyState state, ReasonCode reasonCode) {
        java.util.Objects.requireNonNull(state, "state");
        if (slotId == null || slotId.isBlank()) {
            throw new IllegalArgumentException("slotId must not be blank");
        }
        if (state == SafetyState.UNSAFE && reasonCode == null) {
            throw new IllegalArgumentException("UNSAFE requires a non-null reasonCode");
        }
        if (state == SafetyState.RECOVERED && reasonCode != null) {
            throw new IllegalArgumentException("RECOVERED requires null reasonCode (empty reason in ID tuple)");
        }
    }

    /**
     * Observe an asynchronous Fluss write (R-034). A discarded future would
     * silently lose a safety-halt request — an unsafe state could go un-halted
     * with no alert. Success is logged at INFO only after the future completes;
     * failures are logged at ERROR.
     *
     * @param future the append or upsert future to observe
     * @return a future mirroring the write outcome (for tests)
     */
    static <T> CompletableFuture<T> observe(
            CompletableFuture<T> future, String id, String slotId,
            SafetyState state, String reason, long epoch) {
        return future.whenComplete((result, ex) -> {
            if (ex != null) {
                Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                LOG.error("safety-halt-writer: append failed (id={}, slot={}, state={}, "
                        + "reason={}, epoch={}): {}",
                        id, slotId, state, reason, epoch, cause.getMessage());
            } else {
                LOG.info("safety-halt-writer: wrote {} (slot={}, state={}, reason={}, epoch={})",
                        id, slotId, state, reason, epoch);
            }
        });
    }

    /**
     * halt_request_id = sha256(manifest_fingerprint|slot_id|connection_epoch|state|reason_code)
     */
    public static String computeHaltRequestId(String manifestFingerprint, String slotId,
                                              long connectionEpoch, String state, String reasonCode) {
        String tuple = manifestFingerprint + "|" + slotId + "|" + connectionEpoch
                + "|" + state + "|" + reasonCode;
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    tuple.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    /** SHA-256 of the slot's sorted token set (assigned_token_set_hash). */
    public static String computeAssignedTokenHash(java.util.List<Long> tokens) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            java.util.List<Long> sorted = new java.util.ArrayList<>(tokens);
            sorted.sort(Long::compareTo);
            for (long token : sorted) {
                md.update((byte) (token >>> 56));
                md.update((byte) (token >>> 48));
                md.update((byte) (token >>> 40));
                md.update((byte) (token >>> 32));
                md.update((byte) (token >>> 24));
                md.update((byte) (token >>> 16));
                md.update((byte) (token >>> 8));
                md.update((byte) (token));
            }
            return HexFormat.of().formatHex(md.digest());
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static BinaryString bs(String s) {
        return s != null ? BinaryString.fromString(s) : null;
    }

    @Override
    public void close() {
        try {
            writer.flush();
        } catch (Exception e) {
            LOG.warn("safety-halt-writer: close failed: {}", e.getMessage());
        }
        closeQuietly();
    }

    /** R-141: release the Fluss Connection + Table held since construction. */
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
}
