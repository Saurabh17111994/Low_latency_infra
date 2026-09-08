package com.trading.ingestion;

import java.util.List;
import java.util.Map;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import com.trading.common.schema.RawTableSchema;
import org.apache.fluss.metadata.TablePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DDL bootstrap and read-only schema verification for the platform tables.
 *
 * <p><b>Create-only, never destructive.</b> This class never drops, alters,
 * or recreates an existing table: schema reconciliation is owned by the
 * offline DDL gate ({@code ddl_apply.py} / {@code schema_manifest.json}), not
 * by a runtime bootstrap. A column-count heuristic must never decide to drop
 * DDL-provisioned state.
 *
 * <p>Two entry points:
 * <ul>
 *   <li>{@link #verifyTables(String)} — read-only. Confirms the default
 *       database exists and every expected table exists. For the tables this
 *       service actually writes ({@link #OWNED_TABLES}) it additionally checks
 *       the exact DDL column count, so a schema drift is caught at startup.
 *       Other platform tables are existence-checked only — their owning
 *       services are not built yet, so their in-code placeholder schemas must
 *       not be compared by column count.</li>
 *   <li>{@link #ensureTables(String)} — local-development only. Creates any
 *       missing table with its bucket routing. Idempotent; never touches
 *       existing tables.</li>
 * </ul>
 */
public final class DdlBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(DdlBootstrap.class);

    /**
     * P1-215: bound every admin RPC — an unresponsive coordinator must fail
     * bootstrap in 30s, not hang startup forever. Mirrors
     * {@code DropRawTable.ADMIN_TIMEOUT} (same value, cross-cited; not
     * imported — different class, no dependency). P1-055 create-race catch
     * still matches ExecutionException(already-exists); a TimeoutException
     * is NOT already-exists and propagates as failure.
     */
    static final java.time.Duration ADMIN_TIMEOUT = java.time.Duration.ofSeconds(30);

    private DdlBootstrap() {}

    /**
     * Read-only verification that the default database and all expected tables
     * exist. For the tables this service owns (writes) the exact DDL column
     * count is verified; for the remaining platform tables only existence is
     * checked. Never creates, drops, or alters anything. This is the default
     * production start path.
     *
     * @return true if the database exists, every expected table exists, and
     *         every owned table has the expected column count
     */
    public static boolean verifyTables(String bootstrapServers) {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrapServers);

        LOG.info("ddl-bootstrap: verifying schema at {} (read-only) ...", bootstrapServers);

        try (Connection c = ConnectionFactory.createConnection(conf);
             Admin admin = c.getAdmin()) {

            if (!databaseExists(admin, "default")) {
                LOG.error("ddl-bootstrap: database 'default' does not exist — run DDL first");
                return false;
            }

            int ok = 0, missing = 0, schemaMismatch = 0;
            for (Map.Entry<String, TableDescriptor> entry : ALL_TABLES.entrySet()) {
                String name = entry.getKey();
                TableDescriptor td = entry.getValue();
                TablePath path = TablePath.of("default", name);

                try (org.apache.fluss.client.table.Table table = c.getTable(path)) {
                    // P1-054: Table is AutoCloseable — the previous
                    // c.getTable(path).getTableInfo() leaked the handle per
                    // table per verify run.
                    org.apache.fluss.metadata.TableInfo ti = table.getTableInfo();
                    if (OWNED_TABLES.contains(name)) {
                        // P1-054: count-only checks pass silent renames /
                        // retypes / reorders — compare names+types in order.
                        java.util.Optional<String> mismatch =
                                describeSchemaMismatch(td.getSchema(), ti.getRowType());
                        if (mismatch.isPresent()) {
                            LOG.error("ddl-bootstrap: default.{} schema mismatch — {}",
                                    name, mismatch.get());
                            schemaMismatch++;
                        } else {
                            ok++;
                        }
                    } else {
                        // Not owned by this service yet — existence is enough;
                        // column layout is the owning service's contract.
                        ok++;
                    }
                } catch (Exception e) {
                    LOG.error("ddl-bootstrap: default.{} missing or not readable: {}", name, e.getMessage());
                    missing++;
                }
            }

            LOG.info("ddl-bootstrap: verified {} tables ok, {} missing, {} schema-mismatch",
                    ok, missing, schemaMismatch);
            return missing == 0 && schemaMismatch == 0;

        } catch (Exception e) {
            LOG.error("ddl-bootstrap: connection failed — {}", e.getMessage());
            return false;
        }
    }

    /**
     * Order-sensitive names+types comparison of the expected descriptor
     * schema against the live row type. Empty = match; otherwise a
     * one-line human description of the first divergence (fail-fast at
     * startup instead of a row-converter serialization failure at runtime).
     * Pure function — no RPC — so unit tests pin it without a cluster.
     */
    static java.util.Optional<String> describeSchemaMismatch(
            org.apache.fluss.metadata.Schema expected, org.apache.fluss.types.RowType actual) {
        java.util.List<String> wantNames = expected.getColumnNames();
        java.util.List<String> haveNames = actual.getFieldNames();
        if (!wantNames.equals(haveNames)) {
            return java.util.Optional.of(
                    "columns " + haveNames + ", expected " + wantNames);
        }
        for (int i = 0; i < wantNames.size(); i++) {
            String wantType = expected.getColumn(wantNames.get(i))
                    .getDataType().asSerializableString();
            String haveType = actual.getTypeAt(i).asSerializableString();
            if (!wantType.equals(haveType)) {
                return java.util.Optional.of("column '" + wantNames.get(i)
                        + "' is " + haveType + ", expected " + wantType);
            }
        }
        return java.util.Optional.empty();
    }

    /**
     * True when the failure is a concurrent-creator AlreadyExists anywhere
     * in the cause chain. Typed exceptions first (immune to message
     * rewording, locale, and wrapping); the message walk stays as fallback
     * for server versions that surface it as text. Pure — unit-tested
     * without a cluster.
     */
    static boolean isAlreadyExists(Throwable e) {
        for (Throwable cur = e; cur != null; cur = cur.getCause()) {
            // P1-216: match the Fluss typed exceptions, not just text — a
            // reworded / localized "already exists" message used to sail
            // past and fail bootstrap on a benign create race.
            if (cur instanceof org.apache.fluss.exception.DatabaseAlreadyExistException
                    || cur instanceof org.apache.fluss.exception.TableAlreadyExistException) {
                return true;
            }
            String msg = cur.getMessage();
            if (msg != null && msg.toLowerCase().contains("already exist")) {
                return true;
            }
        }
        return false;
    }

    private static boolean databaseExists(Admin admin, String name) {
        try {
            return admin.listDatabases().get(ADMIN_TIMEOUT.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS).contains(name);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.warn("ddl-bootstrap: interrupted listing databases");
            return false;
        } catch (Exception e) {
            LOG.warn("ddl-bootstrap: could not list databases: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Ensure the tables this service owns exist on the given Fluss cluster.
     *
     * <p><b>Owned tables only (A4.4, CANDLE-KV-REPLAY-001 P4).</b> The
     * registry contains full platform tables whose owning services are not
     * built yet; {@code ensureTables} must never bootstrap-create those —
     * their creation is the offline DDL gate's job (schema reconciliation is
     * owned by {@code ddl_apply.py} / {@code schema_manifest.json}). The
     * compute tables ({@code feature_candles_15s}, {@code
     * Signal_Candidates}, {@code
     * Signal_Candidates_current}, …) are
     * provisioned out-of-band; this method only ever creates
     * {@link #OWNED_TABLES}.
     *
     * <p><b>Create-only:</b> existing tables are never dropped or recreated,
     * even when their column count differs from the in-code schema. Intended
     * for local development; production should apply the DDLs out-of-band and
     * start through {@link #verifyTables(String)}.
     *
     * @return true if every owned table exists (created or already present)
     */
    public static boolean ensureTables(String bootstrapServers) {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrapServers);

        LOG.info("ddl-bootstrap: connecting to {} ...", bootstrapServers);

        try (Connection c = ConnectionFactory.createConnection(conf);
             Admin admin = c.getAdmin()) {

            ensureDatabase(admin, "default");

            int ok = 0, failed = 0;
            for (String name : OWNED_TABLES) {
                TableDescriptor td = ALL_TABLES.get(name);
                TablePath path = TablePath.of("default", name);

                try {
                    if (admin.tableExists(path).get(ADMIN_TIMEOUT.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)) {
                        LOG.debug("ddl-bootstrap: default.{} already exists", name);
                        ok++;
                        continue;
                    }
                    try {
                        admin.createTable(path, td, false).get(ADMIN_TIMEOUT.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
                    } catch (java.util.concurrent.ExecutionException concurrent) {
                        // P1-055: parallel startup (multi-pod) — the loser of
                        // the create race must count the table as ok, not failed.
                        if (isAlreadyExists(concurrent)) {
                            LOG.info("ddl-bootstrap: default.{} created concurrently", name);
                        } else {
                            throw concurrent;
                        }
                    }
                    LOG.info("ddl-bootstrap: ✓ default.{} created", name);
                    ok++;
                } catch (Exception e) {
                    LOG.error("ddl-bootstrap: ✗ default.{} — {}", name, e.getMessage());
                    failed++;
                }
            }

            LOG.info("ddl-bootstrap: {} tables ok, {} failed", ok, failed);
            return failed == 0;

        } catch (Exception e) {
            LOG.error("ddl-bootstrap: connection failed — {}", e.getMessage());
            return false;
        }
    }

    private static void ensureDatabase(Admin admin, String name) throws Exception {
        try {
            admin.createDatabase(name, DatabaseDescriptor.builder().build(), false).get(ADMIN_TIMEOUT.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
            LOG.info("ddl-bootstrap: database '{}' created", name);
        } catch (Exception e) {
            // P1-216: reuse isAlreadyExists (typed + cause-walk + message fallback),
            // not a bare top-level message check — ExecutionException wraps the real cause.
            if (isAlreadyExists(e)) {
                LOG.info("ddl-bootstrap: database '{}' exists", name);
            } else {
                throw e;
            }
        }
    }

    // ── Table registry: name → descriptor (bucket routing only) ──────

    /**
     * Tables this service writes and therefore verifies by exact column count
     * against the DDL. All other tables are existence-checked only until their
     * owning service is built.
     */
    private static final List<String> OWNED_TABLES =
            List.of("raw_table_1", "suspected_discontinuities", "ingestion_quarantine");

    /**
     * Full 20-column schema for raw_table_1 matching DDL v2 (R-054/R-231).
     * SC2 (2026-08-29): derived from {@link RawTableSchema} — the single
     * source of truth — so DDL, bootstrap, and the row converter cannot drift.
     */
    private static final Schema RAW_TABLE_1_SCHEMA = rawTableSchema();

    /** Builds the Fluss {@link Schema} from {@link RawTableSchema} (SC2). */
    private static Schema rawTableSchema() {
        org.apache.fluss.types.DataType[] types = new org.apache.fluss.types.DataType[
                RawTableSchema.FIELD_COUNT];
        for (int i = 0; i < RawTableSchema.FIELD_COUNT; i++) {
            types[i] = toFlussType(RawTableSchema.COLUMN_TYPE_ROOTS.get(i));
        }
        org.apache.fluss.metadata.Schema.Builder b = Schema.newBuilder();
        for (int i = 0; i < RawTableSchema.FIELD_COUNT; i++) {
            b.column(RawTableSchema.COLUMNS.get(i), types[i]);
        }
        return b.build();
    }

    /**
     * Maps a {@code DataTypeRoot} name to the Fluss {@link org.apache.fluss.types.DataType}.
     * Package-private for DdlBootstrapTest. P1-056: INT was already used by
     * sibling schemas in this file (contract_version) while this mapping —
     * the choke point for every RawTableSchema evolution — rejected it.
     */
    static org.apache.fluss.types.DataType toFlussType(String root) {
        switch (root) {
            case "STRING":
                return org.apache.fluss.types.DataTypes.STRING();
            case "BIGINT":
                return org.apache.fluss.types.DataTypes.BIGINT();
            case "INT":
                return org.apache.fluss.types.DataTypes.INT();
            case "BOOLEAN":
                return org.apache.fluss.types.DataTypes.BOOLEAN();
            case "BYTES":
                return org.apache.fluss.types.DataTypes.BYTES();
            default:
                throw new IllegalStateException("Unsupported raw-table type root: " + root);
        }
    }

    /** Full 13-column schema for Postback_Quarantine matching DDL 16. */
    private static final Schema POSTBACK_QUARANTINE_SCHEMA = Schema.newBuilder()
            .column("quarantine_id", org.apache.fluss.types.DataTypes.STRING())
            .column("postback_event_id", org.apache.fluss.types.DataTypes.STRING())
            .column("reason", org.apache.fluss.types.DataTypes.STRING())
            .column("original_payload", org.apache.fluss.types.DataTypes.BYTES())
            .column("payload_hash", org.apache.fluss.types.DataTypes.STRING())
            .column("broker_order_id", org.apache.fluss.types.DataTypes.STRING())
            .column("instruction_id", org.apache.fluss.types.DataTypes.STRING())
            .column("correlation_attempt", org.apache.fluss.types.DataTypes.STRING())
            .column("disposition", org.apache.fluss.types.DataTypes.STRING())
            .column("disposition_reason", org.apache.fluss.types.DataTypes.STRING())
            .column("quarantined_ts", org.apache.fluss.types.DataTypes.BIGINT())
            .column("disposition_ts", org.apache.fluss.types.DataTypes.BIGINT())
            .column("schema_version", org.apache.fluss.types.DataTypes.STRING())
            .build();

    /** Full 11-column schema for suspected_discontinuities matching DDL 19. */
    private static final Schema DISCONTINUITY_SCHEMA = Schema.newBuilder()
            .column("discontinuity_id", org.apache.fluss.types.DataTypes.STRING())
            .column("source", org.apache.fluss.types.DataTypes.STRING())
            .column("reason", org.apache.fluss.types.DataTypes.STRING())
            .column("connection_epoch", org.apache.fluss.types.DataTypes.BIGINT())
            .column("last_tick_ts", org.apache.fluss.types.DataTypes.BIGINT())
            .column("last_tick_fingerprint", org.apache.fluss.types.DataTypes.STRING())
            .column("last_tick_token", org.apache.fluss.types.DataTypes.BIGINT())
            .column("last_tick_exchange", org.apache.fluss.types.DataTypes.STRING())
            .column("last_tick_symbol", org.apache.fluss.types.DataTypes.STRING())
            .column("detected_ts", org.apache.fluss.types.DataTypes.BIGINT())
            .column("schema_version", org.apache.fluss.types.DataTypes.STRING())
            .build();

    /** Full 10-column schema for ingestion_quarantine matching DDL 21 and QuarantineWriter. */
    private static final Schema INGESTION_QUARANTINE_SCHEMA = Schema.newBuilder()
            .column("quarantine_id", org.apache.fluss.types.DataTypes.STRING())
            .column("reason", org.apache.fluss.types.DataTypes.STRING())
            .column("instrument_token", org.apache.fluss.types.DataTypes.BIGINT())
            .column("exchange", org.apache.fluss.types.DataTypes.STRING())
            .column("symbol", org.apache.fluss.types.DataTypes.STRING())
            .column("raw_payload", org.apache.fluss.types.DataTypes.BYTES())
            .column("payload_hash", org.apache.fluss.types.DataTypes.STRING())
            .column("detected_ts", org.apache.fluss.types.DataTypes.BIGINT())
            .column("detail", org.apache.fluss.types.DataTypes.STRING())
            .column("schema_version", org.apache.fluss.types.DataTypes.STRING())
            .build();

    /** Full 21-column KV schema for Safety_Halt_Requests matching the migrated v3 DDL. */
    private static final Schema SAFETY_HALT_SCHEMA = Schema.newBuilder()
            .column("halt_request_id", org.apache.fluss.types.DataTypes.STRING())
            .column("account_scope_id", org.apache.fluss.types.DataTypes.STRING())
            .column("portfolio_id", org.apache.fluss.types.DataTypes.STRING())
            .column("execution_partition_id", org.apache.fluss.types.DataTypes.STRING())
            .column("source_component", org.apache.fluss.types.DataTypes.STRING())
            .column("source_instance", org.apache.fluss.types.DataTypes.STRING())
            .column("reason_code", org.apache.fluss.types.DataTypes.STRING())
            .column("reason_detail", org.apache.fluss.types.DataTypes.STRING())
            .column("detection_time", org.apache.fluss.types.DataTypes.BIGINT())
            .column("source_epoch", org.apache.fluss.types.DataTypes.BIGINT())
            .column("evidence_hash", org.apache.fluss.types.DataTypes.STRING())
            .column("application_result", org.apache.fluss.types.DataTypes.STRING())
            .column("applied_ts", org.apache.fluss.types.DataTypes.BIGINT())
            .column("schema_version", org.apache.fluss.types.DataTypes.STRING())
            .column("slot_id", org.apache.fluss.types.DataTypes.STRING())
            .column("connection_epoch", org.apache.fluss.types.DataTypes.BIGINT())
            .column("manifest_fingerprint", org.apache.fluss.types.DataTypes.STRING())
            .column("assigned_token_set_hash", org.apache.fluss.types.DataTypes.STRING())
            .column("state", org.apache.fluss.types.DataTypes.STRING())
            .column("evidence_reference", org.apache.fluss.types.DataTypes.STRING())
            .column("contract_version", org.apache.fluss.types.DataTypes.INT())
            .primaryKey("halt_request_id") // DDL v3 (R-089): LOG→KV for PK dedup
            .build();

    /**
     * Full 22-column LOG schema for Signal_Candidates matching the v3 DDL
     * (05_signal_candidates.sql, DEC-035). Written by the compute job's
     * signal producers (forming-bar/N7, DEC-034/2026-09-05) as append-only audit — one row per
     * fired signal, never updated. Current-state consumers read the KV
     * projection {@link #SIGNAL_CANDIDATES_CURRENT_SCHEMA}.
     */
    private static final Schema SIGNAL_CANDIDATES_SCHEMA = Schema.newBuilder()
            .column("candidate_id", org.apache.fluss.types.DataTypes.STRING())
            .column("instruction_id", org.apache.fluss.types.DataTypes.STRING())
            .column("trade_context_id", org.apache.fluss.types.DataTypes.STRING())
            .column("instrument_token", org.apache.fluss.types.DataTypes.BIGINT())
            .column("exchange", org.apache.fluss.types.DataTypes.STRING())
            .column("symbol", org.apache.fluss.types.DataTypes.STRING())
            .column("strategy_id", org.apache.fluss.types.DataTypes.STRING())
            .column("strategy_version", org.apache.fluss.types.DataTypes.STRING())
            .column("rule_id", org.apache.fluss.types.DataTypes.STRING())
            .column("detection_ts", org.apache.fluss.types.DataTypes.BIGINT())
            .column("evaluation_ts", org.apache.fluss.types.DataTypes.BIGINT())
            .column("action", org.apache.fluss.types.DataTypes.STRING())
            .column("side", org.apache.fluss.types.DataTypes.STRING())
            .column("quantity", org.apache.fluss.types.DataTypes.BIGINT())
            .column("order_type", org.apache.fluss.types.DataTypes.STRING())
            .column("limit_price_paise", org.apache.fluss.types.DataTypes.BIGINT())
            .column("score_inputs", org.apache.fluss.types.DataTypes.STRING())
            .column("formation_snapshot_ref", org.apache.fluss.types.DataTypes.STRING())
            .column("validity_reason", org.apache.fluss.types.DataTypes.STRING())
            .column("supersedes_candidate_id", org.apache.fluss.types.DataTypes.STRING())
            .column("superseded_by_candidate_id", org.apache.fluss.types.DataTypes.STRING())
            .column("schema_version", org.apache.fluss.types.DataTypes.STRING())
            .build();

    /**
     * Full 22-column KV schema for Signal_Candidates_current matching DDL 23
     * (DEC-035): same columns as the LOG twin plus
     * PRIMARY KEY (instrument_token) — the idempotent current-state
     * projection consumers read instead of scanning the append-only LOG.
     * Bucket key instrument_token equals the PK, keeping per-ticker
     * colocation with the LOG twin.
     */
    private static final Schema SIGNAL_CANDIDATES_CURRENT_SCHEMA = Schema.newBuilder()
            .column("candidate_id", org.apache.fluss.types.DataTypes.STRING())
            .column("instruction_id", org.apache.fluss.types.DataTypes.STRING())
            .column("trade_context_id", org.apache.fluss.types.DataTypes.STRING())
            .column("instrument_token", org.apache.fluss.types.DataTypes.BIGINT())
            .column("exchange", org.apache.fluss.types.DataTypes.STRING())
            .column("symbol", org.apache.fluss.types.DataTypes.STRING())
            .column("strategy_id", org.apache.fluss.types.DataTypes.STRING())
            .column("strategy_version", org.apache.fluss.types.DataTypes.STRING())
            .column("rule_id", org.apache.fluss.types.DataTypes.STRING())
            .column("detection_ts", org.apache.fluss.types.DataTypes.BIGINT())
            .column("evaluation_ts", org.apache.fluss.types.DataTypes.BIGINT())
            .column("action", org.apache.fluss.types.DataTypes.STRING())
            .column("side", org.apache.fluss.types.DataTypes.STRING())
            .column("quantity", org.apache.fluss.types.DataTypes.BIGINT())
            .column("order_type", org.apache.fluss.types.DataTypes.STRING())
            .column("limit_price_paise", org.apache.fluss.types.DataTypes.BIGINT())
            .column("score_inputs", org.apache.fluss.types.DataTypes.STRING())
            .column("formation_snapshot_ref", org.apache.fluss.types.DataTypes.STRING())
            .column("validity_reason", org.apache.fluss.types.DataTypes.STRING())
            .column("supersedes_candidate_id", org.apache.fluss.types.DataTypes.STRING())
            .column("superseded_by_candidate_id", org.apache.fluss.types.DataTypes.STRING())
            .column("schema_version", org.apache.fluss.types.DataTypes.STRING())
            .primaryKey("instrument_token")
            .build();

    /**
     * Full 15-column KV schema for feature_candles_15s matching DDL 03
     * (03_feature_candles_15s.sql, schema v2): PK
     * (instrument_token, window_start) — the storage layer enforces one row
     * per closed window per instrument, so a replay/restart re-emits the same
     * key as an idempotent upsert instead of a duplicate LOG append (user
     * requirement 2026-08-13: candle tables are KV-only, no LOG+KV twin).
     * Bucket key instrument_token is a strict subset of the PK (Fluss
     * requires pk ⊇ bucketKey), keeping per-ticker colocation. Written by the
     * compute job's candle slice; column names/order mirror
     * {@code com.trading.common.schema.CandleTableSchema} — the shared
     * contract the candle sink serializes against.
     */
    private static final Schema FEATURE_CANDLES_SCHEMA = Schema.newBuilder()
            .column("instrument_token", org.apache.fluss.types.DataTypes.BIGINT())
            .column("exchange", org.apache.fluss.types.DataTypes.STRING())
            .column("symbol", org.apache.fluss.types.DataTypes.STRING())
            .column("window_start", org.apache.fluss.types.DataTypes.BIGINT())
            .column("window_end", org.apache.fluss.types.DataTypes.BIGINT())
            .column("open_paise", org.apache.fluss.types.DataTypes.BIGINT())
            .column("high_paise", org.apache.fluss.types.DataTypes.BIGINT())
            .column("low_paise", org.apache.fluss.types.DataTypes.BIGINT())
            .column("close_paise", org.apache.fluss.types.DataTypes.BIGINT())
            .column("volume", org.apache.fluss.types.DataTypes.BIGINT())
            .column("tick_count", org.apache.fluss.types.DataTypes.INT())
            .column("algorithm_version", org.apache.fluss.types.DataTypes.STRING())
            .column("configuration_version", org.apache.fluss.types.DataTypes.STRING())
            .column("output_ts", org.apache.fluss.types.DataTypes.BIGINT())
            .column("schema_version", org.apache.fluss.types.DataTypes.STRING())
            .primaryKey("instrument_token", "window_start")
            .build();

    /**
     * 14-column KV schema for feature_candles_15s_preview (low-latency candles
     * Phase 1, 2026-08-29): live OHLCV of in-progress 15s windows, overwritten
     * every 1s by CandlePreviewEmitFunction (compute job). PK
     * (instrument_token, window_start) — same as the final candle table, so an
     * upsert overwrites the same row each tick (the row "grows" live) and the
     * row auto-expires after the 60s TTL. Columns mirror
     * {@code com.trading.common.schema.CandlePreviewTableSchema} — the shared
     * contract the preview sink serializes against. is_preview is always TRUE
     * here (the marker exists so consumers can distinguish preview rows from
     * final candles without joining tables).
     */
    private static final Schema FEATURE_CANDLES_PREVIEW_SCHEMA = Schema.newBuilder()
            .column("instrument_token", org.apache.fluss.types.DataTypes.BIGINT())
            .column("exchange", org.apache.fluss.types.DataTypes.STRING())
            .column("symbol", org.apache.fluss.types.DataTypes.STRING())
            .column("window_start", org.apache.fluss.types.DataTypes.BIGINT())
            .column("window_end", org.apache.fluss.types.DataTypes.BIGINT())
            .column("open_paise", org.apache.fluss.types.DataTypes.BIGINT())
            .column("high_paise", org.apache.fluss.types.DataTypes.BIGINT())
            .column("low_paise", org.apache.fluss.types.DataTypes.BIGINT())
            .column("close_paise", org.apache.fluss.types.DataTypes.BIGINT())
            .column("volume", org.apache.fluss.types.DataTypes.BIGINT())
            .column("tick_count", org.apache.fluss.types.DataTypes.INT())
            .column("is_preview", org.apache.fluss.types.DataTypes.BOOLEAN())
            .column("output_ts", org.apache.fluss.types.DataTypes.BIGINT())
            .column("schema_version", org.apache.fluss.types.DataTypes.STRING())
            .primaryKey("instrument_token", "window_start")
            .build();

    /**
     * Full 15-column KV schema for candle_live matching DDL 32
     * (32_candle_live.sql, schema v1): PK (instrument_token, tf,
     * window_start) — live per-timeframe snapshots, upserted every 1s by the
     * compute job's MultiTfAggregatorFunction; rows auto-expire via the 60s
     * log TTL. Columns mirror
     * {@code com.trading.compute.signaljob.CandleLiveColumns} — the shared
     * contract the live sink serializes against. Registry-only
     * (compute-owned, A4.4): existence-checked, never bootstrap-created.
     */
    private static final Schema CANDLE_LIVE_SCHEMA = Schema.newBuilder()
            .column("instrument_token", org.apache.fluss.types.DataTypes.BIGINT())
            .column("exchange", org.apache.fluss.types.DataTypes.STRING())
            .column("symbol", org.apache.fluss.types.DataTypes.STRING())
            .column("tf", org.apache.fluss.types.DataTypes.STRING())
            .column("window_start", org.apache.fluss.types.DataTypes.BIGINT())
            .column("window_end", org.apache.fluss.types.DataTypes.BIGINT())
            .column("open_paise", org.apache.fluss.types.DataTypes.BIGINT())
            .column("high_paise", org.apache.fluss.types.DataTypes.BIGINT())
            .column("low_paise", org.apache.fluss.types.DataTypes.BIGINT())
            .column("close_paise", org.apache.fluss.types.DataTypes.BIGINT())
            .column("volume", org.apache.fluss.types.DataTypes.BIGINT())
            .column("tick_count", org.apache.fluss.types.DataTypes.INT())
            .column("last_event_time", org.apache.fluss.types.DataTypes.BIGINT())
            .column("last_event_fingerprint", org.apache.fluss.types.DataTypes.STRING())
            .column("schema_version", org.apache.fluss.types.DataTypes.STRING())
            .primaryKey("instrument_token", "tf", "window_start")
            .build();

    /**
     * Full 15-column KV schema for candle_closed matching DDL 33
     * (33_candle_closed.sql, schema v1): PK (instrument_token, tf,
     * window_start) — immutable closed history, one row per non-empty bucket
     * per timeframe per instrument, first-write-wins. Same 15 columns as
     * candle_live in identical DDL order. Columns mirror
     * {@code com.trading.compute.signaljob.CandleClosedColumns}.
     * Registry-only (compute-owned, A4.4): existence-checked, never
     * bootstrap-created.
     */
    private static final Schema CANDLE_CLOSED_SCHEMA = Schema.newBuilder()
            .column("instrument_token", org.apache.fluss.types.DataTypes.BIGINT())
            .column("exchange", org.apache.fluss.types.DataTypes.STRING())
            .column("symbol", org.apache.fluss.types.DataTypes.STRING())
            .column("tf", org.apache.fluss.types.DataTypes.STRING())
            .column("window_start", org.apache.fluss.types.DataTypes.BIGINT())
            .column("window_end", org.apache.fluss.types.DataTypes.BIGINT())
            .column("open_paise", org.apache.fluss.types.DataTypes.BIGINT())
            .column("high_paise", org.apache.fluss.types.DataTypes.BIGINT())
            .column("low_paise", org.apache.fluss.types.DataTypes.BIGINT())
            .column("close_paise", org.apache.fluss.types.DataTypes.BIGINT())
            .column("volume", org.apache.fluss.types.DataTypes.BIGINT())
            .column("tick_count", org.apache.fluss.types.DataTypes.INT())
            .column("last_event_time", org.apache.fluss.types.DataTypes.BIGINT())
            .column("last_event_fingerprint", org.apache.fluss.types.DataTypes.STRING())
            .column("schema_version", org.apache.fluss.types.DataTypes.STRING())
            .primaryKey("instrument_token", "tf", "window_start")
            .build();

    /**
     * Minimal placeholder schema for platform tables whose owning service is
     * not built yet. These tables are only existence-checked at runtime — the
     * full DDL (applied by the offline DDL gate) is authoritative for their
     * column layout.
     */
    private static final Schema MINIMAL_SCHEMA = Schema.newBuilder()
            .column("instrument_token", org.apache.fluss.types.DataTypes.STRING())
            .column("execution_partition_id", org.apache.fluss.types.DataTypes.STRING())
            .column("portfolio_id", org.apache.fluss.types.DataTypes.STRING())
            .column("account_scope_id", org.apache.fluss.types.DataTypes.STRING())
            .column("broker_order_id", org.apache.fluss.types.DataTypes.STRING())
            .column("instruction_id", org.apache.fluss.types.DataTypes.STRING())
            .column("client_order_ref", org.apache.fluss.types.DataTypes.STRING())
            .column("record_id", org.apache.fluss.types.DataTypes.STRING())
            .build();

    private static TableDescriptor logTable(String... bucketKeys) {
        return TableDescriptor.builder()
                .schema(MINIMAL_SCHEMA)
                .distributedBy(4, bucketKeys)
                .build();
    }

    /** Package-private accessor for tests (schema-agreement guard). */
    static Map<String, TableDescriptor> tableRegistry() {
        return ALL_TABLES;
    }

    /** Package-private accessor for tests. */
    static List<String> ownedTables() {
        return OWNED_TABLES;
    }

    private static final Map<String, TableDescriptor> ALL_TABLES =
            Map.ofEntries(
                    Map.entry("raw_table_1",
                            TableDescriptor.builder()
                                    .schema(RAW_TABLE_1_SCHEMA)
                                    .partitionedBy(RawTableSchema.PARTITION_KEY)
                                    .distributedBy(RawTableSchema.BUCKET_COUNT, RawTableSchema.BUCKET_KEY)
                                    .property("table.log.ttl", RawTableSchema.LOG_TTL)
                                    .property("table.auto-partition.enabled", "true")
                                    .property("table.auto-partition.time-unit", "DAY")
                                    .property("table.auto-partition.num-precreate", "2")
                                    .property("table.auto-partition.num-retention", RawTableSchema.PARTITION_RETENTION)
                                    .property("table.auto-partition.time-zone", "Asia/Kolkata")
                                    .property("table.datalake.enabled", "true")
                                    .property("table.datalake.format", "iceberg")
                                    .property("table.datalake.freshness", "5min")
                                    .property("table.datalake.auto-compaction", "true")
                                    .build()),
                    Map.entry("feature_candles_15s",
                            TableDescriptor.builder()
                                    .schema(FEATURE_CANDLES_SCHEMA)
                                    .distributedBy(16, "instrument_token")
                                    .build()),
                    Map.entry("feature_candles_15s_preview",
                            TableDescriptor.builder()
                                    .schema(FEATURE_CANDLES_PREVIEW_SCHEMA)
                                    .distributedBy(16, "instrument_token")
                                    .build()),
                    Map.entry("candle_live",
                            TableDescriptor.builder()
                                    .schema(CANDLE_LIVE_SCHEMA)
                                    .distributedBy(16, "instrument_token")
                                    .property("table.log.ttl", "60s")
                                    .property("table.datalake.enabled", "false")
                                    .property("table.kv.format-version", "2")
                                    .build()),
                    Map.entry("candle_closed",
                            TableDescriptor.builder()
                                    .schema(CANDLE_CLOSED_SCHEMA)
                                    .distributedBy(16, "instrument_token")
                                    .property("table.log.ttl", "7d")
                                    .property("table.datalake.enabled", "true")
                                    .property("table.datalake.format", "iceberg")
                                    .property("table.datalake.freshness", "5min")
                                    .property("table.datalake.auto-compaction", "true")
                                    .property("table.kv.format-version", "2")
                                    .build()),
                    Map.entry("Signal_Candidates",
                            TableDescriptor.builder().schema(SIGNAL_CANDIDATES_SCHEMA).distributedBy(16, "instrument_token").build()),
                    Map.entry("Signal_Candidates_current",
                            TableDescriptor.builder().schema(SIGNAL_CANDIDATES_CURRENT_SCHEMA).distributedBy(16, "instrument_token").build()),
                    Map.entry("Ranking_Results",
                            logTable("execution_partition_id")),
                    Map.entry("Trade_Decisions",
                            logTable("execution_partition_id")),
                    Map.entry("Execution_Intent",
                            logTable("instruction_id")),
                    Map.entry("Fills",
                            logTable("portfolio_id")),
                    Map.entry("Execution_Audit",
                            logTable("execution_partition_id")),
                    Map.entry("Postback_Quarantine",
                            TableDescriptor.builder().schema(POSTBACK_QUARANTINE_SCHEMA).distributedBy(8, "quarantine_id").build()),
                    Map.entry("Safety_Halt_Requests",
                            TableDescriptor.builder().schema(SAFETY_HALT_SCHEMA).distributedBy(4, "halt_request_id").build()),
                    Map.entry("suspected_discontinuities",
                            TableDescriptor.builder().schema(DISCONTINUITY_SCHEMA).distributedBy(4, "discontinuity_id").build()),
                    Map.entry("ingestion_quarantine",
                            TableDescriptor.builder().schema(INGESTION_QUARANTINE_SCHEMA).distributedBy(8, "quarantine_id").build()),
                    // P1-057: LOG semantics (was kvTable — byte-identical twin
                    // with no PK, i.e. silently LOG). KV/upsert is TBD when the
                    // owning service lands; this registry is existence-check only.
                    Map.entry("forming_bar",
                            logTable("instrument_token")),
                    // P1-057: LOG semantics (was kvTable — byte-identical twin
                    // with no PK, i.e. silently LOG). KV/upsert is TBD when the
                    // owning service lands; this registry is existence-check only.
                    Map.entry("Order_Lifecycle",
                            logTable("account_scope_id")),
                    // P1-057: LOG semantics (was kvTable — byte-identical twin
                    // with no PK, i.e. silently LOG). KV/upsert is TBD when the
                    // owning service lands; this registry is existence-check only.
                    Map.entry("Positions",
                            logTable("portfolio_id")),
                    Map.entry("Execution_Gate",
                            TableDescriptor.builder().schema(MINIMAL_SCHEMA).distributedBy(1, "execution_partition_id").build()),
                    Map.entry("Execution_Attempts",
                            TableDescriptor.builder().schema(MINIMAL_SCHEMA).distributedBy(1, "instruction_id").build()),
                    Map.entry("Order_Correlation",
                            TableDescriptor.builder().schema(MINIMAL_SCHEMA).distributedBy(1, "instruction_id").build()),
                    Map.entry("Portfolio_Reservations",
                            logTable("portfolio_id")),
                    Map.entry("Postback_Projection_Ledger",
                            TableDescriptor.builder().schema(MINIMAL_SCHEMA).distributedBy(1, "broker_order_id").build()),
                    Map.entry("instruments",
                            TableDescriptor.builder().schema(MINIMAL_SCHEMA).distributedBy(1, "instrument_token").build()),
                    Map.entry("fingerprint_dedup",
                            logTable("instrument_token")),
                    Map.entry("trade_instruction_state",
                            logTable("instruction_id")),
                    Map.entry("eod_offload_state",
                            logTable("record_id")),
                    Map.entry("Position_State",
                            TableDescriptor.builder()
                                    .schema(MINIMAL_SCHEMA)
                                    .distributedBy(16, "instrument_token")
                                    .build()),
                    Map.entry("Execution_Intent_Processed",
                            TableDescriptor.builder().schema(MINIMAL_SCHEMA).distributedBy(8, "instruction_id").build())
            );
}
