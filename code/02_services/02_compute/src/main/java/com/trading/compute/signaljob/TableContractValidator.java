package com.trading.compute.signaljob;

import com.trading.common.schema.CandlePreviewTableSchema;
import com.trading.common.schema.CandleTableSchema;
import java.util.Arrays;
import java.util.List;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;

/**
 * Read-only table-contract checks run before the graph is built (and before
 * any Fluss sink/source serialization is wired): the job must prove that the
 * deployed tables match the contracts the write paths rely on.
 *
 * <ul>
 *   <li>{@code feature_candles_15s} — candle KV current-state table
 *       (user requirement 2026-08-13: candle tables are KV-only, no LOG+KV
 *       twin): primary key exactly {@code [instrument_token, window_start]},
 *       bucket.key exactly {@code instrument_token} (a subset of the PK, so
 *       per-ticker colocation holds), 16 buckets, exact 15-column v2 schema
 *       (tracker 14 P1 — CANDLE-SCHEMA-002).</li>
 *   <li>{@code Signal_Candidates} — immutable signal LOG (DEC-035, v3):
 *       <b>no</b> primary key, bucket.key exactly {@code instrument_token},
 *       16 buckets, exact 22-column schema.</li>
 *   <li>{@code Signal_Candidates_current} — KV current-state projection
 *       (DEC-035): primary key exactly {@code [instrument_token]},
 *       bucket.key exactly {@code [instrument_token]} (a subset of the PK, so
 *       per-ticker colocation holds), 16 buckets, the same exact 22-column
 *       schema as the LOG twin (tracker 14 re-scoped P2 — SIGNAL-SCHEMA-001).</li>
 * </ul>
 *
 * <p>Each check compares {@code TableInfo.getSchema().getColumns()} against
 * the shared column contract ({@link CandleTableSchema} or
 * {@link SignalCandidatesTableColumns}) column by column — names in DDL order
 * and Fluss type root per column — so a future DDL that renames, reorders,
 * widens, or re-types a column fails startup instead of silently
 * mis-serializing rows.
 *
 * <p>Nullability enforcement follows what Fluss actually guarantees in live
 * metadata (verified against the dev cluster 2026-08-10): a LOG table reports
 * every column nullable (DDL NOT NULL is not carried into LOG metadata).
 * DDL-level NOT NULL is reported by {@link #schemaReport(TableInfo, List)} as
 * divergence information, not a failure — the storage layer cannot enforce
 * it.
 *
 * <p>Violations fail startup with a {@link ContractViolation} — never a
 * degraded write. The checks are pure {@link TableInfo} inspection so they are
 * unit-testable without a cluster; {@link SignalJob#preflightTableContracts}
 * feeds them from the live cluster before the environment is created.
 */
public final class TableContractValidator {

    /** Thrown when a deployed table violates the contract the write path relies on. */
    public static final class ContractViolation extends IllegalStateException {
        public ContractViolation(String message) {
            super(message);
        }
    }

    private static final String SIGNAL_CONTRACT = "tracker 14 re-scoped P2, SIGNAL-SCHEMA-001";
    private static final String CANDLE_CONTRACT = "tracker 14 P1, CANDLE-SCHEMA-002";
    private static final String TRADE_CONTRACT = "SCH-19, TRADE-SCHEMA-001";
    private static final String EXECUTION_INTENT_CONTRACT =
            "REQ-EXE-004, EXECUTION-INTENT-SCHEMA-001";
    private static final String DEDUP_CONTRACT = "DEC-038, DEDUP-SCHEMA-001";
    private static final String FORMING_BAR_CONTRACT = "DEC-038, FORMING-BAR-SCHEMA-001";
    private static final String POSITION_STATE_CONTRACT = "Option-B, POSITION-STATE-SCHEMA-001";

    private TableContractValidator() {}

    /**
     * Candle KV: PK exactly [instrument_token, window_start],
     * instrument_token routing, exact 15-col schema.
     */
    public static void validateCandleKvTable(TableInfo info) {
        List<String> expectedPk = CandleTableSchema.PRIMARY_KEY_COLUMNS;
        if (!info.hasPrimaryKey()) {
            throw new ContractViolation(
                    "KV table " + info.getTablePath() + " must carry primary key exactly "
                            + expectedPk + " (one row per closed window per instrument), "
                            + "but has NO primary key (" + CANDLE_CONTRACT + ")");
        }
        if (!expectedPk.equals(info.getPrimaryKeys())) {
            throw new ContractViolation(
                    "KV table " + info.getTablePath() + " must carry primary key exactly "
                            + expectedPk + " (one row per closed window per instrument), got "
                            + info.getPrimaryKeys() + " (" + CANDLE_CONTRACT + ")");
        }
        validateSchema(info, CandleTableSchema.COLUMNS, CandleTableSchema.COLUMN_TYPE_ROOTS,
                "15-column v2 candle", CANDLE_CONTRACT);
        validateRouting(info, CandleTableSchema.BUCKET_KEY, CandleTableSchema.BUCKET_COUNT,
                CANDLE_CONTRACT);
    }

    /**
     * Preview table contract (low-latency candles Phase 1, 2026-08-29):
     * KV, PK (instrument_token, window_start) — same as the final candle, so
     * an upsert overwrites the same row each 1s tick; exact 14-column schema
     * per {@link CandlePreviewTableSchema}; bucket key instrument_token.
     */
    public static void validatePreviewTable(TableInfo info) {
        List<String> expectedPk = CandlePreviewTableSchema.PRIMARY_KEY_COLUMNS;
        if (!info.hasPrimaryKey()) {
            throw new ContractViolation(
                    "KV table " + info.getTablePath() + " must carry primary key exactly "
                            + expectedPk + " (one row per in-progress window per instrument), "
                            + "but has NO primary key (" + CANDLE_CONTRACT + ")");
        }
        if (!expectedPk.equals(info.getPrimaryKeys())) {
            throw new ContractViolation(
                    "KV table " + info.getTablePath() + " must carry primary key exactly "
                            + expectedPk + " (one row per in-progress window per instrument), got "
                            + info.getPrimaryKeys() + " (" + CANDLE_CONTRACT + ")");
        }
        validateSchema(info, CandlePreviewTableSchema.COLUMNS,
                CandlePreviewTableSchema.COLUMN_TYPE_ROOTS,
                "14-column v1 candle preview", CANDLE_CONTRACT);
        validateRouting(info, CandlePreviewTableSchema.BUCKET_KEY,
                CandlePreviewTableSchema.BUCKET_COUNT, CANDLE_CONTRACT);
    }

    /** Signal LOG: append-only, no primary key, instrument_token routing, exact 22-col schema. */
    public static void validateSignalLogTable(TableInfo info) {
        requireNoPrimaryKey(info, "append-only signal LOG", SIGNAL_CONTRACT);
        validateSchema(info, signalNames(), SignalCandidatesTableColumns.TYPE_ROOTS,
                "22-column v3 signal", SIGNAL_CONTRACT);
        validateRouting(info, CandleTableSchema.BUCKET_KEY, CandleTableSchema.BUCKET_COUNT,
                SIGNAL_CONTRACT);
    }

    /**
     * Trade_Decisions LOG: append-only, no primary key, instruction_id
     * routing, exact 25-col v2 schema (SCH-19, REQ-FLS-008).
     */
    public static void validateTradeDecisionsLogTable(TableInfo info) {
        requireNoPrimaryKey(info, "immutable instruction LOG", TRADE_CONTRACT);
        validateSchema(info, Arrays.asList(TradeDecisionsTableColumns.NAMES),
                TradeDecisionsTableColumns.TYPE_ROOTS, "25-column v2 trade decision",
                TRADE_CONTRACT);
        validateRouting(info, "instruction_id", 8, TRADE_CONTRACT);
    }

    /**
     * trade_instruction_state KV: PK exactly [instruction_id], bucket.key
     * exactly [instruction_id] (single-field PK — raw-client writable per the
     * COMPAT-FLUSS-005 matrix), exact 4-col v1 schema (SCH-19 index).
     */
    public static void validateTradeInstructionStateKvTable(TableInfo info) {
        List<String> expectedPk = List.of(TradeInstructionStateColumns.NAMES[
                TradeInstructionStateColumns.INSTRUCTION_ID]);
        requireExactPrimaryKey(info, expectedPk, TRADE_CONTRACT);
        validateSchema(info, Arrays.asList(TradeInstructionStateColumns.NAMES),
                TradeInstructionStateColumns.TYPE_ROOTS, "4-column v1 instruction index",
                TRADE_CONTRACT);
        validateRouting(info, "instruction_id", 8, TRADE_CONTRACT);
    }

    /** Execution_Intent LOG: immutable request feed, no primary key, instruction routing. */
    public static void validateExecutionIntentLogTable(TableInfo info) {
        requireNoPrimaryKey(info, "append-only execution-intent LOG", EXECUTION_INTENT_CONTRACT);
        validateSchema(info, Arrays.asList(ExecutionIntentTableColumns.NAMES),
                ExecutionIntentTableColumns.TYPE_ROOTS, "22-column v1 execution intent",
                EXECUTION_INTENT_CONTRACT);
        validateRouting(info, "instruction_id", 8, EXECUTION_INTENT_CONTRACT);
    }

    /**
     * fingerprint_dedup KV: PK exactly [instrument_token, fingerprint_version,
     * event_fingerprint], bucket.key exactly [instrument_token] (PK prefix —
     * per-instrument colocation), exact 6-col v1 schema (DEC-038). Invoked by
     * the extended preflight when the dedup externalization stage wires the
     * table into the job (SIG-STATE-001/002/003); the validator itself is
     * unit-tested regardless.
     */
    public static void validateFingerprintDedupTable(TableInfo info) {
        List<String> expectedPk = List.of(
                FingerprintDedupTableColumns.NAMES[FingerprintDedupTableColumns.INSTRUMENT_TOKEN],
                FingerprintDedupTableColumns.NAMES[FingerprintDedupTableColumns.FINGERPRINT_VERSION],
                FingerprintDedupTableColumns.NAMES[FingerprintDedupTableColumns.EVENT_FINGERPRINT]);
        requireExactPrimaryKey(info, expectedPk, DEDUP_CONTRACT);
        validateSchema(info, Arrays.asList(FingerprintDedupTableColumns.NAMES),
                FingerprintDedupTableColumns.TYPE_ROOTS, "6-column v1 dedup state",
                DEDUP_CONTRACT);
        validateRouting(info, "instrument_token", 16, DEDUP_CONTRACT);
    }

    /**
     * Forming-bar current-state KV (forming-bar persistence phase,
     * 2026-08-16): PK exactly [instrument_token], instrument_token routing,
     * exact 11-column v1 schema. The durable home of the live forming bar
     * (DEC-038 state-ownership matrix); the writer emits one upsert per
     * instrument per cadence — current-state only, never history.
     */
    public static void validateFormingBarKvTable(TableInfo info) {
        requireExactPrimaryKey(info,
                List.of(FormingBarTableColumns.NAMES[FormingBarTableColumns.INSTRUMENT_TOKEN]),
                FORMING_BAR_CONTRACT);
        validateSchema(info, Arrays.asList(FormingBarTableColumns.NAMES),
                FormingBarTableColumns.TYPE_ROOTS, "11-column v1 forming bar",
                FORMING_BAR_CONTRACT);
        validateRouting(info, FormingBarTableColumns.NAMES[FormingBarTableColumns.INSTRUMENT_TOKEN],
                16, FORMING_BAR_CONTRACT);
    }

    /**
     * Position_State KV (Option B, 2026-08-18): PK exactly [instrument_token],
     * instrument_token routing, exact 7-column v1 schema. The handshake table:
     * Execution Gateway / Nautilus UPSERTS OPEN on fill and CLOSED on exit;
     * Signal job reads the changelog and clears its per-instrument ACTIVE
     * block only on CLOSED (or ADMIN_CLEAR). No TTL — survives restarts.
     */
    public static void validatePositionStateKvTable(TableInfo info) {
        requireExactPrimaryKey(info,
                List.of(PositionStateTableColumns.NAMES[PositionStateTableColumns.INSTRUMENT_TOKEN]),
                POSITION_STATE_CONTRACT);
        validateSchema(info, Arrays.asList(PositionStateTableColumns.NAMES),
                PositionStateTableColumns.TYPE_ROOTS, "7-column v1 position_state",
                POSITION_STATE_CONTRACT);
        validateRouting(info,
                PositionStateTableColumns.NAMES[PositionStateTableColumns.INSTRUMENT_TOKEN],
                16, POSITION_STATE_CONTRACT);
    }

    /** Signal current-state KV: PK exactly [instrument_token], instrument_token routing, exact 22-col schema. */
    public static void validateSignalCurrentKvTable(TableInfo info) {
        List<String> expectedPk = List.of(SignalCandidatesTableColumns.NAMES[
                SignalCandidatesTableColumns.INSTRUMENT_TOKEN]);
        if (!info.hasPrimaryKey()) {
            throw new ContractViolation(
                    "KV table " + info.getTablePath() + " must carry primary key exactly "
                            + expectedPk + " (per-ticker current state), but has NO primary key ("
                            + SIGNAL_CONTRACT + ")");
        }
        if (!expectedPk.equals(info.getPrimaryKeys())) {
            throw new ContractViolation(
                    "KV table " + info.getTablePath() + " must carry primary key exactly "
                            + expectedPk + " (per-ticker current state), got "
                            + info.getPrimaryKeys() + " (" + SIGNAL_CONTRACT + ")");
        }
        validateSchema(info, signalNames(), SignalCandidatesTableColumns.TYPE_ROOTS,
                "22-column v3 signal", SIGNAL_CONTRACT);
        validateRouting(info, CandleTableSchema.BUCKET_KEY, CandleTableSchema.BUCKET_COUNT,
                SIGNAL_CONTRACT);
    }

    /**
     * Human-readable schema report for the startup log (and evidence):
     * column name, live type root, live nullability, DDL intent, and a
     * divergence marker where the DDL's NOT NULL is not carried into live
     * metadata. {@code nullableInDdl.get(i)} is {@code true} when the DDL
     * declares column {@code i} nullable (same semantic as
     * {@link CandleTableSchema#COLUMN_NULLABLE_IN_DDL}). Never throws — the
     * strict checks are {@code validate*Table}.
     */
    public static String schemaReport(TableInfo info, List<Boolean> nullableInDdl) {
        StringBuilder sb = new StringBuilder(512);
        sb.append("table ").append(info.getTablePath()).append(" schema=[");
        List<Schema.Column> columns = info.getSchema().getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            Schema.Column c = columns.get(i);
            boolean ddlNotNull = i < nullableInDdl.size() && !nullableInDdl.get(i);
            boolean liveNotNull = !c.getDataType().isNullable();
            sb.append(c.getName()).append(':').append(c.getDataType().getTypeRoot().name());
            if (liveNotNull) {
                sb.append(" NOT NULL");
            } else {
                sb.append(ddlNotNull ? " nullable(DDL NOT NULL not carried)" : " nullable");
            }
        }
        return sb.append(']').toString();
    }
    private static void requireNoPrimaryKey(TableInfo info, String what, String contractId) {
        if (info.hasPrimaryKey()) {
            throw new ContractViolation(
                    "table " + info.getTablePath() + " must have NO primary key (" + what
                            + " contract), got " + info.getPrimaryKeys() + " (" + contractId
                            + ")");
        }
    }


    private static void validateSchema(TableInfo info, List<String> expectedNames,
            List<String> expectedTypes, String contract, String contractId) {
        Schema schema = info.getSchema();
        List<Schema.Column> columns = schema.getColumns();
        if (columns.size() != expectedNames.size()) {
            throw new ContractViolation(
                    "table " + info.getTablePath() + " must carry exactly "
                            + expectedNames.size() + " columns (" + contract + " contract), got "
                            + columns.size() + " (" + contractId + ")");
        }
        for (int i = 0; i < columns.size(); i++) {
            Schema.Column column = columns.get(i);
            String actualType = column.getDataType().getTypeRoot().name();
            if (!expectedNames.get(i).equals(column.getName())) {
                throw new ContractViolation(
                        "table " + info.getTablePath() + " column #" + i + " must be named '"
                                + expectedNames.get(i) + "' (DDL order), got '" + column.getName()
                                + "' (" + contractId + ")");
            }
            if (!expectedTypes.get(i).equals(actualType)) {
                throw new ContractViolation(
                        "table " + info.getTablePath() + " column '" + column.getName()
                                + "' must be " + expectedTypes.get(i) + " (" + contract
                                + " contract), got " + actualType + " (" + contractId + ")");
            }
        }
    }
    private static void requireExactPrimaryKey(TableInfo info, List<String> expectedPk,
            String contractId) {
        if (!info.hasPrimaryKey()) {
            throw new ContractViolation(
                    "KV table " + info.getTablePath() + " must carry primary key exactly "
                            + expectedPk + " but has NO primary key (" + contractId + ")");
        }
        if (!expectedPk.equals(info.getPrimaryKeys())) {
            throw new ContractViolation(
                    "KV table " + info.getTablePath() + " must carry primary key exactly "
                            + expectedPk + ", got " + info.getPrimaryKeys() + " (" + contractId + ")");
        }
    }

    /**
     * Routing check: bucket.key must equal {@code expectedBucketKey} exactly
     * and the bucket count must equal {@code expectedBucketCount}.
     */
    private static void validateRouting(TableInfo info, String expectedBucketKey,
            int expectedBucketCount, String contractId) {
        if (!info.hasBucketKey()) {
            throw new ContractViolation(
                    "table " + info.getTablePath() + " must set bucket.key explicitly, but uses the default ("
                            + contractId + ")");
        }
        List<String> bucketKeys = info.getBucketKeys();
        if (!List.of(expectedBucketKey).equals(bucketKeys)) {
            throw new ContractViolation(
                    "table " + info.getTablePath() + " must be distributed by bucket.key exactly ["
                            + expectedBucketKey + "] (colocation), got "
                            + bucketKeys + " (" + contractId + ")");
        }
        int buckets = info.getNumBuckets();
        if (buckets != expectedBucketCount) {
            throw new ContractViolation(
                    "table " + info.getTablePath() + " must have " + expectedBucketCount
                            + " buckets (colocation), got " + buckets
                            + " (" + contractId + ")");
        }
    }

    private static List<String> signalNames() {
        return Arrays.asList(SignalCandidatesTableColumns.NAMES);
    }
}
