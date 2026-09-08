package com.trading.ingestion.discontinuity;

import com.trading.ingestion.bridge.BridgeEvent;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
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
 * Writes suspected data-gap evidence to {@code suspected_discontinuities}
 * in Fluss. Records ingestion-side discontinuities: bridge crashes,
 * reconnections (epoch bumps), heartbeat timeouts, NTP clock jumps,
 * and feed-health signals.
 *
 * <p>Per dossier §B5:</p>
 * <blockquote>
 * Write evidence to suspected_discontinuities Fluss table on connection
 * loss, heartbeat timeout, reconnect, or feed-health warning.
 * </blockquote>
 *
 * <p>Column mapping (19_suspected_discontinuities.sql):</p>
 * <pre>
 * discontinuity_id           STRING  — UUID
 * source                     STRING  — who detected the gap (bridge instance / slot)
 * reason                     STRING  — DROP|HEARTBEAT_GAP|TIME_JUMP|FEED_HEALTH|RECONNECT
 * connection_epoch           BIGINT  — monotonically increasing
 * last_tick_ts               BIGINT  — epoch ms of last tick before gap
 * last_tick_fingerprint      STRING  — fingerprint before gap (null on startup crash)
 * last_tick_token            BIGINT  — bucket key; null for connection-wide events
 * last_tick_exchange         STRING  — null for connection-wide
 * last_tick_symbol           STRING  — null for connection-wide
 * detected_ts                BIGINT  — epoch ms when gap was detected
 * schema_version             STRING  — "2" (header/manifest v2; P4-213)
 * </pre>
 */
public class DiscontinuityWriter implements DiscontinuitySink {

    /** Row schema version — must match the DDL header + manifest (P4-213). */
    static final String SCHEMA_VERSION = "2";

    private static final Logger LOG = LoggerFactory.getLogger(DiscontinuityWriter.class);

    private static final String TABLE_DB = "default";
    private static final String TABLE_NAME = "suspected_discontinuities";

    /** Reason per DDL enum. */
    public enum Reason {
        /** Connection dropped unexpectedly (bridge crash, stdin EOF). */
        DROP,
        /** No tick received within heartbeat window. */
        HEARTBEAT_GAP,
        /** NTP clock offset exceeded limit. */
        TIME_JUMP,
        /** Feed-health signal from broker (e.g. broker-side gap warning). */
        FEED_HEALTH,
        /** Reconnection occurred — epoch bumped while pipeline is continuous. */
        RECONNECT,
        /** T7-F11: per-connection feed_sequence_local jumped (lost ticks). */
        SEQUENCE_GAP
    }

    private final AppendWriter writer;
    private Connection connection; // R-062
    private Table table; // R-062
    private final String instanceId;
    private final String connectionId;
    private final AtomicLong connectionEpoch;

    /**
     * Snapshot of the most recent tick for before/after gap evidence.
     */
    public static class LastTickSnapshot {
        public final long timestampMs;
        public final String fingerprint;
        public final long instrumentToken;
        public final String exchange;
        public final String symbol;

        public LastTickSnapshot(long timestampMs, String fingerprint,
                                long instrumentToken, String exchange, String symbol) {
            this.timestampMs = timestampMs;
            this.fingerprint = fingerprint;
            this.instrumentToken = instrumentToken;
            this.exchange = exchange;
            this.symbol = symbol;
        }
    }

    /**
     * Creates a discontinuity writer connected to the Fluss coordinator.
     * Maintains its own independent {@link AppendWriter}.
     */
    public DiscontinuityWriter(String bootstrapServers, String instanceId,
                                String connectionId, AtomicLong connectionEpoch) {
        this.instanceId = instanceId;
        this.connectionId = connectionId;
        this.connectionEpoch = connectionEpoch;

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrapServers);
        // R-297 wedge fix: bound the writer memory-pool wait (default is
        // infinite) so a wedged sender cannot park the calling thread forever
        // in append() — the discontinuity write fails cleanly instead.
        conf.setString("client.writer.buffer.wait-timeout", "30s");

        try {
            // R-062: retain so close() can release them.
            this.connection = ConnectionFactory.createConnection(conf);
            TablePath path = TablePath.of(TABLE_DB, TABLE_NAME);
            this.table = connection.getTable(path);
            this.writer = table.newAppend().createWriter();
            LOG.info("discontinuity-writer: connected (table={}, instanceId={})",
                    path, instanceId);
        } catch (Exception e) {
            LOG.error("discontinuity-writer: failed to connect to Fluss: {}", e.getMessage(), e);
            throw new RuntimeException("Cannot create DiscontinuityWriter", e);
        }
    }

    /**
     * Write a connection-wide discontinuity (no instrument context).
     * Used for bridge crashes and reconnects.
     *
     * @param reason        detection reason
     * @param note          free-text operator note — carried in the append
     *                      log detail; no DDL column exists for it (R-249)
     * @param before        last tick before gap (null if none)
     */
    public void write(Reason reason, String note,
                       LastTickSnapshot before) {
        write(reason, note, before, null, null, null);
    }

    /**
     * Map a bridge lifecycle event to discontinuity evidence (plan §DiscontinuityWriter).
     *
     * <pre>
     * DISCONNECTED / BRIDGE_SHUTDOWN / AUTH_FAILURE -> DROP
     * HEARTBEAT_FAILED / FEED_STALLED             -> HEARTBEAT_GAP
     * RECONNECT                                    -> RECONNECT
     * SUBSCRIPTION_PARTIAL (rejected tokens > 0)   -> FEED_HEALTH
     * </pre>
     *
     * <p>One immutable row per transition, using the event's own connection
     * epoch and source. Never includes secrets or full raw lines.
     */
    public void writeBridgeEvent(BridgeEvent event, LastTickSnapshot before) {
        if (!carriesDiscontinuityEvidence(event)) {
            return; // full subscription ack / non-evidence event
        }
        Reason reason = mapEventToReason(event.event());
        if (reason == null) {
            return; // not a discontinuity-bearing event
        }
        writeWithEpoch(reason, event.connectionEpoch(), event.slotId(),
                event.connectionId(), before, event.reason());
    }

    /**
     * Whether a bridge event carries discontinuity evidence (R-029).
     *
     * <p>The Go bridge validates events against exactly
     * {@code {slot_state, subscription_ack, heartbeat_failed, feed_stalled,
     * disconnect, reconnect, auth_failure, bridge_shutdown}} — {@code bridge_exit}
     * is not in that vocabulary, the real exit event is {@code bridge_shutdown}.
     * A full {@code subscription_ack} (rejectedTokens == 0) is healthy, not a
     * discontinuity; only a partial subscription carries FEED_HEALTH evidence.
     */
    static boolean carriesDiscontinuityEvidence(BridgeEvent event) {
        if (event == null) return false;
        if ("subscription_ack".equals(event.event())) {
            return event.rejectedTokens() > 0;
        }
        return mapEventToReason(event.event()) != null;
    }

    /** Map a bridge event name to a discontinuity {@link Reason}, or null if not applicable. */
    static Reason mapEventToReason(String eventName) {
        if (eventName == null) return null;
        return switch (eventName) {
            case "disconnect", "bridge_shutdown", "auth_failure" -> Reason.DROP;
            case "heartbeat_failed", "feed_stalled" -> Reason.HEARTBEAT_GAP;
            case "reconnect" -> Reason.RECONNECT;
            case "subscription_ack" -> Reason.FEED_HEALTH;
            default -> null;
        };
    }

    /**
     * Write a connection-wide discontinuity with an explicit epoch and source
     * (used for per-slot lifecycle events where the shared connectionEpoch
     * counter is not the slot's epoch).
     */
    private void writeWithEpoch(Reason reason, long epoch, String source,
                                String connectionIdValue, LastTickSnapshot before, String note) {
        String discontinuityId = instanceId + "-" + UUID.randomUUID();
        Instant now = Instant.now();
        // source is NOT NULL in the DDL — never allow null through.
        // Prefer the connection id (the bridge process instance that detected
        // the gap); fall back to the slot id, then a constant.
        String sourceValue = connectionIdValue != null ? connectionIdValue
                : (source != null ? source : "ingestion");
        GenericRow row = GenericRow.of(
                bs(discontinuityId),
                bs(sourceValue),
                bs(reason.name()),
                epoch,
                before != null ? before.timestampMs : null,
                before != null ? bs(before.fingerprint) : null,
                before != null ? before.instrumentToken : null,
                before != null ? bs(before.exchange) : null,
                before != null ? bs(before.symbol) : null,
                now.toEpochMilli(),
                bs(SCHEMA_VERSION)
        );
        try {
            observe(writer.append(row), discontinuityId,
                    reason.name() + "/" + note + "/source=" + sourceValue);
        } catch (Exception e) {
            LOG.error("discontinuity-writer: append failed (id={}, reason={}): {}",
                    discontinuityId, reason, e.getMessage());
        }
    }

    /**
     * Write an instrument-scoped discontinuity.
     */
    public void write(Reason reason, String note,
                       LastTickSnapshot before,
                       Long instrumentToken, String exchange, String symbol) {

        String discontinuityId = instanceId + "-" + UUID.randomUUID();
        Instant now = Instant.now();

        // R-063: connection-wide events write SQL NULL, not 0L.
        Long instToken = instrumentToken != null ? instrumentToken
                : (before != null ? before.instrumentToken : null);
        String exch = exchange != null ? exchange
                : (before != null ? before.exchange : null);
        String sym = symbol != null ? symbol
                : (before != null ? before.symbol : null);

        GenericRow row = GenericRow.of(
                bs(discontinuityId),
                bs(connectionId),
                bs(reason.name()),
                connectionEpoch.get(),
                before != null ? before.timestampMs : null,
                before != null ? bs(before.fingerprint) : null,
                instToken,
                bs(exch),
                bs(sym),
                now.toEpochMilli(),
                bs(SCHEMA_VERSION)
        );

        try {
            observe(writer.append(row), discontinuityId,
                    reason.name() + "/" + note);
        } catch (Exception e) {
            LOG.error("discontinuity-writer: append failed (id={}, reason={}): {}",
                    discontinuityId, reason, e.getMessage());
        }
    }

    /**
     * Observe an asynchronous Fluss append (R-030). The {@link AppendWriter}
     * surfaces broker-side/serialization failures by completing the future
     * exceptionally — never by throwing from {@code append()} — so a discarded
     * future silently loses discontinuity evidence (a money-safety evidence
     * path). Success is only logged after the future actually completes;
     * failures are logged at ERROR.
     *
     * @return a future mirroring the append outcome (for tests)
     */
    static CompletableFuture<AppendResult> observe(
            CompletableFuture<AppendResult> future, String id, String detail) {
        return future.whenComplete((result, ex) -> {
            if (ex != null) {
                Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                LOG.error("discontinuity-writer: append failed (id={}, detail={}): {}",
                        id, detail, cause.getMessage());
            } else {
                LOG.info("discontinuity-writer: wrote {} (detail={})", id, detail);
            }
        });
    }

    /** Shorthand to convert a String to Fluss's internal BinaryString type. */
    private static BinaryString bs(String s) {
        // R-063: null must stay SQL NULL — EMPTY_UTF8 fabricated an empty
        // string for connection-wide evidence rows.
        return s != null ? BinaryString.fromString(s) : null;
    }

    @Override
    public void close() {
        try {
            writer.flush();
            // AppendWriter (TableWriter) does not have close() in Fluss 0.9.1-incubating.
        } catch (Exception e) {
            LOG.warn("discontinuity-writer: close failed: {}", e.getMessage());
        }
        // R-062: close the Connection + Table this writer created.
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
