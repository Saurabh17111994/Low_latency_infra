package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.common.schema.CandleTableSchema;
import java.util.Arrays;
import java.util.List;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Table-contract validator (tracker 14 P1 — CANDLE-SCHEMA-002 for the candle
 * KV; tracker 14 re-scoped P2 — SIGNAL-SCHEMA-001 for the signal LOG/KV
 * pair): each deployed table must carry the exact contract the write path
 * relies on — candle KV PK exactly {@code [instrument_token, window_start]},
 * signal LOG (no PK), signal current-state KV PK exactly
 * {@code [instrument_token]}, per-ticker routing (bucket.key
 * {@code instrument_token}, 16 buckets), and the exact frozen column schema
 * (names in DDL order, Fluss type root per column) — or startup fails
 * closed.
 */
@DisplayName("TableContractValidator")
class TableContractValidatorTest {

    private static final String LOG = "feature_candles_15s";
    private static final String SIGNAL_LOG = "Signal_Candidates";
    private static final String SIGNAL_CURRENT = "Signal_Candidates_current";
    private static final String TOKEN = "instrument_token";

    private static final List<String> CANDLE_NAMES = CandleTableSchema.COLUMNS;
    private static final List<String> CANDLE_TYPES = CandleTableSchema.COLUMN_TYPE_ROOTS;
    private static final List<String> SIGNAL_NAMES = Arrays.asList(SignalCandidatesTableColumns.NAMES);
    private static final List<String> SIGNAL_TYPES = SignalCandidatesTableColumns.TYPE_ROOTS;
    private static final List<String> TRADE_NAMES = Arrays.asList(TradeDecisionsTableColumns.NAMES);
    private static final List<String> TRADE_TYPES = TradeDecisionsTableColumns.TYPE_ROOTS;
    private static final List<String> INSTRUCTION_NAMES = Arrays.asList(TradeInstructionStateColumns.NAMES);
    private static final List<String> INSTRUCTION_TYPES = TradeInstructionStateColumns.TYPE_ROOTS;
    private static final List<String> EXECUTION_INTENT_NAMES =
            Arrays.asList(ExecutionIntentTableColumns.NAMES);
    private static final List<String> EXECUTION_INTENT_TYPES = ExecutionIntentTableColumns.TYPE_ROOTS;
    private static final List<String> DEDUP_NAMES = Arrays.asList(FingerprintDedupTableColumns.NAMES);
    private static final List<String> DEDUP_TYPES = FingerprintDedupTableColumns.TYPE_ROOTS;
    private static final List<String> FORMING_BAR_NAMES = Arrays.asList(FormingBarTableColumns.NAMES);
    private static final List<String> FORMING_BAR_TYPES = FormingBarTableColumns.TYPE_ROOTS;

    private static final String TRADE_LOG = "Trade_Decisions";
    private static final String TRADE_INDEX = "trade_instruction_state";
    private static final String DEDUP_TABLE = "fingerprint_dedup";
    private static final String FORMING_BAR_TABLE = "forming_bar";
    private static final String INSTRUCTION_ID = "instruction_id";
    private static final String EXECUTION_INTENT = "Execution_Intent";

    // ── candle KV (CANDLE-SCHEMA-002) ──

    private static final List<String> CANDLE_PK = List.of(TOKEN, "window_start");

    @Test
    @DisplayName("candle KV with PK exactly [instrument_token, window_start] and matching routing passes")
    void candleKvTableExactPkPasses() {
        assertDoesNotThrow(() -> TableContractValidator.validateCandleKvTable(
                candle(LOG, CANDLE_PK, List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("candle KV without a primary key is rejected (one row per closed window)")
    void candleKvTableNoPkRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleKvTable(
                        candle(LOG, null, List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("candle KV with a narrower PK is rejected (exact (instrument_token, window_start))")
    void candleKvTableWrongPkRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleKvTable(
                        candle(LOG, List.of(TOKEN), List.of(TOKEN), 16)));
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleKvTable(
                        candle(LOG, List.of(TOKEN, "window_start", "output_ts"),
                                List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("candle KV with exact 15-column v2 schema and all-nullable live metadata passes")
    void candleKvExactSchemaPasses() {
        // Live metadata reports every column nullable (DDL NOT NULL is not
        // carried into Fluss metadata — verified 2026-08-10); that must pass.
        assertDoesNotThrow(() -> TableContractValidator.validateCandleKvTable(
                candle(LOG, CANDLE_PK, List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("candle wrong column count is rejected (14 or 16 columns)")
    void candleWrongColumnCountRejected() {
        List<String> shortTypes = new java.util.ArrayList<>(CANDLE_TYPES);
        shortTypes.remove(14); // drop schema_version -> 14 columns
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleKvTable(
                        candle(LOG, CANDLE_PK, List.of(TOKEN), 16, shortTypes, false)));
        List<String> longTypes = new java.util.ArrayList<>(CANDLE_TYPES);
        longTypes.add("BIGINT"); // extra column -> 16
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleKvTable(
                        candle(LOG, CANDLE_PK, List.of(TOKEN), 16, longTypes, false)));
    }

    @Test
    @DisplayName("candle renamed column is rejected (name drift breaks writer layout)")
    void candleRenamedColumnRejected() {
        Schema.Builder sb = Schema.newBuilder();
        for (int i = 0; i < CANDLE_NAMES.size(); i++) {
            String col = i == 1 ? "exchng" : CANDLE_NAMES.get(i);
            sb.column(col, dataType(CANDLE_TYPES.get(i), false));
        }
        sb.primaryKey(CANDLE_PK);
        TableInfo info = info(LOG, sb, List.of(TOKEN), 16);
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleKvTable(info));
    }

    @Test
    @DisplayName("candle reordered column is rejected (DDL index order is part of the contract)")
    void candleReorderedColumnRejected() {
        Schema.Builder sb = Schema.newBuilder();
        for (int i = 0; i < CANDLE_NAMES.size(); i++) {
            String col = i == 1 ? CANDLE_NAMES.get(2)
                    : i == 2 ? CANDLE_NAMES.get(1)
                    : CANDLE_NAMES.get(i);
            sb.column(col, dataType(CANDLE_TYPES.get(i), false));
        }
        sb.primaryKey(CANDLE_PK);
        TableInfo info = info(LOG, sb, List.of(TOKEN), 16);
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleKvTable(info));
    }

    @Test
    @DisplayName("candle wrong type root per column is rejected (exchange STRING vs BIGINT)")
    void candleWrongTypeRootRejected() {
        List<String> types = new java.util.ArrayList<>(CANDLE_TYPES);
        types.set(1, "BIGINT"); // exchange must be STRING
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleKvTable(
                        candle(LOG, CANDLE_PK, List.of(TOKEN), 16, types, false)));
    }

    @Test
    @DisplayName("candle tick_count must be INTEGER, not BIGINT")
    void candleTickCountMustBeInteger() {
        List<String> types = new java.util.ArrayList<>(CANDLE_TYPES);
        types.set(10, "BIGINT");
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleKvTable(
                        candle(LOG, CANDLE_PK, List.of(TOKEN), 16, types, false)));
    }

    @Test
    @DisplayName("candle schemaReport never throws and names the DDL-vs-live nullability divergence")
    void candleSchemaReportIsInformational() {
        TableInfo kv = candle(LOG, CANDLE_PK, List.of(TOKEN), 16);
        String report = TableContractValidator.schemaReport(kv, CandleTableSchema.COLUMN_NULLABLE_IN_DDL);
        assertNotNull(report);
        assertTrue(report.contains("instrument_token:BIGINT"),
                "report must name live type roots, got: " + report);
        assertTrue(report.contains("nullable(DDL NOT NULL not carried)"),
                "report must surface the DDL-vs-live nullability divergence, got: " + report);
    }

    // ── signal LOG (SIGNAL-SCHEMA-001) ──

    @Test
    @DisplayName("signal LOG without primary key and with instrument_token routing passes")
    void signalLogTableWithoutPkPasses() {
        assertDoesNotThrow(() -> TableContractValidator.validateSignalLogTable(
                signal(SIGNAL_LOG, null, List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("signal LOG that gained a primary key is rejected (append-only contract)")
    void signalLogTableWithPkIsRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateSignalLogTable(
                        signal(SIGNAL_LOG, List.of(TOKEN), List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("signal LOG with exact 22-column v3 schema passes")
    void signalLogExactSchemaPasses() {
        assertDoesNotThrow(() -> TableContractValidator.validateSignalLogTable(
                signal(SIGNAL_LOG, null, List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("signal wrong column count is rejected (21 or 23 columns)")
    void signalWrongColumnCountRejected() {
        List<String> shortTypes = new java.util.ArrayList<>(SIGNAL_TYPES);
        shortTypes.remove(21); // drop schema_version -> 21 columns
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateSignalLogTable(
                        signal(SIGNAL_LOG, null, List.of(TOKEN), 16, shortTypes, false)));
        List<String> longTypes = new java.util.ArrayList<>(SIGNAL_TYPES);
        longTypes.add("STRING"); // extra column -> 23
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateSignalLogTable(
                        signal(SIGNAL_LOG, null, List.of(TOKEN), 16, longTypes, false)));
    }

    @Test
    @DisplayName("signal wrong type root per column is rejected (instrument_token BIGINT vs STRING)")
    void signalWrongTypeRootRejected() {
        List<String> types = new java.util.ArrayList<>(SIGNAL_TYPES);
        types.set(3, "STRING"); // instrument_token must be BIGINT
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateSignalLogTable(
                        signal(SIGNAL_LOG, null, List.of(TOKEN), 16, types, false)));
    }

    @Test
    @DisplayName("signal LOG with a non-instrument_token bucket key is rejected")
    void signalLogWrongBucketKeyRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateSignalLogTable(
                        signal(SIGNAL_LOG, null, List.of("candidate_id"), 16)));
    }

    @Test
    @DisplayName("signal LOG with a bucket count other than 16 is rejected")
    void signalLogWrongBucketCountRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateSignalLogTable(
                        signal(SIGNAL_LOG, null, List.of(TOKEN), 17)));
    }

    // ── signal current-state KV (SIGNAL-SCHEMA-001) ──

    @Test
    @DisplayName("signal KV with PK exactly [instrument_token] and matching routing passes")
    void signalCurrentKvExactPasses() {
        assertDoesNotThrow(() -> TableContractValidator.validateSignalCurrentKvTable(
                signal(SIGNAL_CURRENT, List.of(TOKEN), List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("signal KV without a primary key is rejected (current-state contract)")
    void signalCurrentKvNoPkRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateSignalCurrentKvTable(
                        signal(SIGNAL_CURRENT, null, List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("signal KV with a wider primary key is rejected (exact per-ticker current state)")
    void signalCurrentKvWrongPkRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateSignalCurrentKvTable(
                        signal(SIGNAL_CURRENT, List.of(TOKEN, "detection_ts"),
                                List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("signal KV with a bucket key outside the primary key is rejected")
    void signalCurrentKvWrongBucketKeyRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateSignalCurrentKvTable(
                        signal(SIGNAL_CURRENT, List.of(TOKEN), List.of("candidate_id"), 16)));
    }

    @Test
    @DisplayName("signal KV with schema drift is rejected (21 columns)")
    void signalCurrentKvSchemaDriftRejected() {
        List<String> shortTypes = new java.util.ArrayList<>(SIGNAL_TYPES);
        shortTypes.remove(21);
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateSignalCurrentKvTable(
                        signal(SIGNAL_CURRENT, List.of(TOKEN), List.of(TOKEN), 16,
                                shortTypes, false)));
    }

    @Test
    @DisplayName("signal KV schemaReport names live types and DDL nullability intent")
    void signalCurrentKvSchemaReportIsInformational() {
        TableInfo kv = signal(SIGNAL_CURRENT, List.of(TOKEN), List.of(TOKEN), 16);
        String report = TableContractValidator.schemaReport(
                kv, SignalCandidatesTableColumns.COLUMN_NULLABLE_IN_DDL);
        assertNotNull(report);
        assertTrue(report.contains("instrument_token:BIGINT"),
                "report must name live type roots, got: " + report);
        assertTrue(report.contains("nullable(DDL NOT NULL not carried)"),
                "report must surface the DDL-vs-live nullability divergence, got: " + report);
    }

    // ── trade decisions LOG (SCH-19, TRADE-SCHEMA-001) ──

    @Test
    @DisplayName("Trade_Decisions LOG without primary key and with instruction_id routing passes")
    void tradeDecisionsLogTableWithoutPkPasses() {
        assertDoesNotThrow(() -> TableContractValidator.validateTradeDecisionsLogTable(
                tradeDecisions(TRADE_LOG, null, List.of(INSTRUCTION_ID), 8)));
    }

    @Test
    @DisplayName("Trade_Decisions LOG that gained a primary key is rejected (append-only contract)")
    void tradeDecisionsLogTableWithPkIsRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateTradeDecisionsLogTable(
                        tradeDecisions(TRADE_LOG, List.of(INSTRUCTION_ID), List.of(INSTRUCTION_ID), 8)));
    }

    @Test
    @DisplayName("Trade_Decisions LOG with a non-instruction_id bucket key is rejected")
    void tradeDecisionsLogWrongBucketKeyRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateTradeDecisionsLogTable(
                        tradeDecisions(TRADE_LOG, null, List.of("candidate_id"), 8)));
    }

    @Test
    @DisplayName("Trade_Decisions LOG with a bucket count other than 8 is rejected")
    void tradeDecisionsLogWrongBucketCountRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateTradeDecisionsLogTable(
                        tradeDecisions(TRADE_LOG, null, List.of(INSTRUCTION_ID), 9)));
    }

    @Test
    @DisplayName("Trade_Decisions wrong column count is rejected (24 or 26 columns)")
    void tradeDecisionsWrongColumnCountRejected() {
        List<String> shortTypes = new java.util.ArrayList<>(TRADE_TYPES);
        shortTypes.remove(24); // drop schema_version -> 24 columns
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateTradeDecisionsLogTable(
                        tradeDecisions(TRADE_LOG, null, List.of(INSTRUCTION_ID), 8,
                                shortTypes, false)));
        List<String> longTypes = new java.util.ArrayList<>(TRADE_TYPES);
        longTypes.add("STRING"); // extra column -> 26
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateTradeDecisionsLogTable(
                        tradeDecisions(TRADE_LOG, null, List.of(INSTRUCTION_ID), 8,
                                longTypes, false)));
    }

    @Test
    @DisplayName("Trade_Decisions wrong type root per column is rejected (composite_score STRING vs DOUBLE)")
    void tradeDecisionsWrongTypeRootRejected() {
        List<String> types = new java.util.ArrayList<>(TRADE_TYPES);
        types.set(17, "STRING"); // composite_score must be DOUBLE
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateTradeDecisionsLogTable(
                        tradeDecisions(TRADE_LOG, null, List.of(INSTRUCTION_ID), 8,
                                types, false)));
    }

    // ── Execution_Intent LOG (REQ-EXE-004) ──

    @Test
    @DisplayName("Execution_Intent LOG without primary key and with instruction routing passes")
    void executionIntentLogExactPasses() {
        assertDoesNotThrow(() -> TableContractValidator.validateExecutionIntentLogTable(
                executionIntent(EXECUTION_INTENT, null, List.of(INSTRUCTION_ID), 8)));
    }

    @Test
    @DisplayName("Execution_Intent LOG with a primary key is rejected")
    void executionIntentLogPrimaryKeyRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateExecutionIntentLogTable(
                        executionIntent(EXECUTION_INTENT, List.of(INSTRUCTION_ID),
                                List.of(INSTRUCTION_ID), 8)));
    }

    @Test
    @DisplayName("Execution_Intent LOG with schema drift is rejected")
    void executionIntentLogSchemaDriftRejected() {
        List<String> shortTypes = new java.util.ArrayList<>(EXECUTION_INTENT_TYPES);
        shortTypes.remove(21);
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateExecutionIntentLogTable(
                        executionIntent(EXECUTION_INTENT, null, List.of(INSTRUCTION_ID), 8,
                                shortTypes, false)));
    }

    // ── trade_instruction_state KV index (SCH-19, TRADE-SCHEMA-001) ──

    @Test
    @DisplayName("instruction-state KV with PK exactly [instruction_id] and matching routing passes")
    void instructionStateKvExactPasses() {
        assertDoesNotThrow(() -> TableContractValidator.validateTradeInstructionStateKvTable(
                instructionState(TRADE_INDEX, List.of(INSTRUCTION_ID),
                        List.of(INSTRUCTION_ID), 8)));
    }

    @Test
    @DisplayName("instruction-state KV without a primary key is rejected (durable index contract)")
    void instructionStateKvNoPkRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateTradeInstructionStateKvTable(
                        instructionState(TRADE_INDEX, null, List.of(INSTRUCTION_ID), 8)));
    }

    @Test
    @DisplayName("instruction-state KV with a wider primary key is rejected (exact [instruction_id])")
    void instructionStateKvWrongPkRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateTradeInstructionStateKvTable(
                        instructionState(TRADE_INDEX, List.of(INSTRUCTION_ID, "canonical_hash"),
                                List.of(INSTRUCTION_ID), 8)));
    }

    @Test
    @DisplayName("instruction-state KV with a bucket key outside the primary key is rejected")
    void instructionStateKvWrongBucketKeyRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateTradeInstructionStateKvTable(
                        instructionState(TRADE_INDEX, List.of(INSTRUCTION_ID),
                                List.of("canonical_hash"), 8)));
    }

    @Test
    @DisplayName("instruction-state KV with schema drift is rejected (3 columns)")
    void instructionStateKvSchemaDriftRejected() {
        List<String> shortTypes = new java.util.ArrayList<>(INSTRUCTION_TYPES);
        shortTypes.remove(3);
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateTradeInstructionStateKvTable(
                        instructionState(TRADE_INDEX, List.of(INSTRUCTION_ID),
                                List.of(INSTRUCTION_ID), 8, shortTypes, false)));
    }

    // ── fingerprint_dedup KV (DEC-038, DEDUP-SCHEMA-001) ──

    private static final List<String> DEDUP_PK = List.of("instrument_token", "fingerprint_version",
            "event_fingerprint");

    @Test
    @DisplayName("dedup KV with PK exactly [instrument_token, fingerprint_version, event_fingerprint] passes")
    void dedupKvExactPasses() {
        assertDoesNotThrow(() -> TableContractValidator.validateFingerprintDedupTable(
                dedup(DEDUP_TABLE, DEDUP_PK, List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("dedup KV without a primary key is rejected (composite identity contract)")
    void dedupKvNoPkRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateFingerprintDedupTable(
                        dedup(DEDUP_TABLE, null, List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("dedup KV with a narrower primary key is rejected (exact composite PK)")
    void dedupKvNarrowerPkRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateFingerprintDedupTable(
                        dedup(DEDUP_TABLE, List.of(TOKEN), List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("dedup KV with a bucket key outside the primary key is rejected")
    void dedupKvWrongBucketKeyRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateFingerprintDedupTable(
                        dedup(DEDUP_TABLE, DEDUP_PK, List.of("event_fingerprint"), 16)));
    }

    @Test
    @DisplayName("dedup KV with schema drift is rejected (5 columns)")
    void dedupKvSchemaDriftRejected() {
        List<String> shortTypes = new java.util.ArrayList<>(DEDUP_TYPES);
        shortTypes.remove(5);
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateFingerprintDedupTable(
                        dedup(DEDUP_TABLE, DEDUP_PK, List.of(TOKEN), 16, shortTypes, false)));
    }

    @Test
    @DisplayName("dedup KV with wrong bucket count is rejected")
    void dedupKvWrongBucketCountRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateFingerprintDedupTable(
                        dedup(DEDUP_TABLE, DEDUP_PK, List.of(TOKEN), 17)));
    }

    // ── forming_bar KV current-state home (DEC-038, FORMING-BAR-SCHEMA-001) ──

    @Test
    @DisplayName("forming-bar KV with PK exactly [instrument_token] and matching routing passes")
    void formingBarKvExactPasses() {
        assertDoesNotThrow(() -> TableContractValidator.validateFormingBarKvTable(
                formingBar(FORMING_BAR_TABLE, List.of(TOKEN), List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("forming-bar KV without a primary key is rejected (current-state contract)")
    void formingBarKvNoPkRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateFormingBarKvTable(
                        formingBar(FORMING_BAR_TABLE, null, List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("forming-bar KV with a wider primary key is rejected (exact per-ticker current state)")
    void formingBarKvWiderPkRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateFormingBarKvTable(
                        formingBar(FORMING_BAR_TABLE, List.of(TOKEN, "window_start"),
                                List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("forming-bar KV with schema drift is rejected (10 columns)")
    void formingBarKvSchemaDriftRejected() {
        List<String> shortTypes = new java.util.ArrayList<>(FORMING_BAR_TYPES);
        shortTypes.remove(10); // drop schema_version -> 10 columns
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateFormingBarKvTable(
                        formingBar(FORMING_BAR_TABLE, List.of(TOKEN), List.of(TOKEN), 16,
                                shortTypes, false)));
    }

    // ── candle_live KV (2026-09-05 multi-TF aggregator Phase 0, CANDLE-MULTITF-001) ──

    private static final List<String> CANDLE_LIVE_NAMES = java.util.Arrays.asList(CandleLiveColumns.NAMES);
    private static final List<String> CANDLE_LIVE_TYPES = CandleLiveColumns.TYPE_ROOTS;
    private static final List<String> CANDLE_CLOSED_NAMES = java.util.Arrays.asList(CandleClosedColumns.NAMES);
    private static final List<String> CANDLE_CLOSED_TYPES = CandleClosedColumns.TYPE_ROOTS;
    private static final String CANDLE_LIVE_TABLE = "candle_live";
    private static final String CANDLE_CLOSED_TABLE = "candle_closed";
    private static final List<String> CANDLE_MULTITF_PK = List.of("instrument_token", "tf", "window_start");

    @Test
    @DisplayName("candle_live KV with PK exactly [instrument_token, tf, window_start] and matching routing passes")
    void candleLiveKvExactPasses() {
        assertDoesNotThrow(() -> TableContractValidator.validateCandleLiveTable(
                candleLive(CANDLE_LIVE_TABLE, CANDLE_MULTITF_PK, List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("candle_live KV without a primary key is rejected")
    void candleLiveKvNoPkRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleLiveTable(
                        candleLive(CANDLE_LIVE_TABLE, null, List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("candle_live KV with a narrower PK is rejected (exact composite)")
    void candleLiveKvNarrowerPkRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleLiveTable(
                        candleLive(CANDLE_LIVE_TABLE, List.of(TOKEN, "tf"), List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("candle_live KV with a wider PK is rejected")
    void candleLiveKvWiderPkRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleLiveTable(
                        candleLive(CANDLE_LIVE_TABLE, List.of(TOKEN, "tf", "window_start", "window_end"),
                                List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("candle_live KV with schema drift is rejected (14 columns)")
    void candleLiveKvSchemaDriftRejected() {
        List<String> shortTypes = new java.util.ArrayList<>(CANDLE_LIVE_TYPES);
        shortTypes.remove(14);
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleLiveTable(
                        candleLive(CANDLE_LIVE_TABLE, CANDLE_MULTITF_PK, List.of(TOKEN), 16, shortTypes, false)));
    }

    @Test
    @DisplayName("candle_live KV with wrong bucket key is rejected")
    void candleLiveKvWrongBucketKeyRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleLiveTable(
                        candleLive(CANDLE_LIVE_TABLE, CANDLE_MULTITF_PK, List.of("tf"), 16)));
    }

    @Test
    @DisplayName("candle_live KV with wrong bucket count is rejected")
    void candleLiveKvWrongBucketCountRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleLiveTable(
                        candleLive(CANDLE_LIVE_TABLE, CANDLE_MULTITF_PK, List.of(TOKEN), 17)));
    }

    @Test
    @DisplayName("candle_live KV with wrong type root is rejected (volume BIGINT vs STRING)")
    void candleLiveKvWrongTypeRootRejected() {
        List<String> types = new java.util.ArrayList<>(CANDLE_LIVE_TYPES);
        types.set(10, "STRING");
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleLiveTable(
                        candleLive(CANDLE_LIVE_TABLE, CANDLE_MULTITF_PK, List.of(TOKEN), 16, types, false)));
    }

    @Test
    @DisplayName("candle_live schemaReport is informational")
    void candleLiveSchemaReportIsInformational() {
        TableInfo info = candleLive(CANDLE_LIVE_TABLE, CANDLE_MULTITF_PK, List.of(TOKEN), 16);
        String report = TableContractValidator.schemaReport(info, CandleLiveColumns.COLUMN_NULLABLE_IN_DDL);
        assertNotNull(report);
        assertTrue(report.contains("instrument_token:BIGINT"));
        assertTrue(report.contains("tf:STRING"));
    }

    // ── candle_closed KV (same contract) ──

    @Test
    @DisplayName("candle_closed KV with PK exactly [instrument_token, tf, window_start] passes")
    void candleClosedKvExactPasses() {
        assertDoesNotThrow(() -> TableContractValidator.validateCandleClosedTable(
                candleClosed(CANDLE_CLOSED_TABLE, CANDLE_MULTITF_PK, List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("candle_closed KV without a primary key is rejected")
    void candleClosedKvNoPkRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleClosedTable(
                        candleClosed(CANDLE_CLOSED_TABLE, null, List.of(TOKEN), 16)));
    }

    @Test
    @DisplayName("candle_closed KV with schema drift is rejected (14 columns)")
    void candleClosedKvSchemaDriftRejected() {
        List<String> shortTypes = new java.util.ArrayList<>(CANDLE_CLOSED_TYPES);
        shortTypes.remove(14);
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleClosedTable(
                        candleClosed(CANDLE_CLOSED_TABLE, CANDLE_MULTITF_PK, List.of(TOKEN), 16, shortTypes, false)));
    }

    @Test
    @DisplayName("candle_closed KV with wrong bucket key is rejected")
    void candleClosedKvWrongBucketKeyRejected() {
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleClosedTable(
                        candleClosed(CANDLE_CLOSED_TABLE, CANDLE_MULTITF_PK, List.of("window_start"), 16)));
    }

    @Test
    @DisplayName("candle_closed KV with wrong type root is rejected")
    void candleClosedKvWrongTypeRootRejected() {
        List<String> types = new java.util.ArrayList<>(CANDLE_CLOSED_TYPES);
        types.set(3, "BIGINT");
        assertThrows(TableContractValidator.ContractViolation.class,
                () -> TableContractValidator.validateCandleClosedTable(
                        candleClosed(CANDLE_CLOSED_TABLE, CANDLE_MULTITF_PK, List.of(TOKEN), 16, types, false)));
    }

    // ── fixtures ──

    /**
     * Builds a TableInfo via the explicit 12-arg constructor (no cluster
     * needed). Verified against the decompiled ctor: slot 6 = bucketKeys,
     * slot 7 = partitionKeys, and the primaryKeys field is derived from the
     * Schema's {@code primaryKey(...)} — so {@code schemaPk} drives both the
     * PK assertions and the routing defaults.
     *
     * <p>The schema mirrors the live dev-cluster metadata (verified
     * 2026-08-10): all columns present in DDL order with the exact type
     * roots, all nullable (LOG metadata never carries NOT NULL).
     * {@code columnTypes} / {@code pkNonNullable} override that default to
     * build the negative fixtures.
     */
    private static TableInfo candle(String name, List<String> schemaPk, List<String> bucketKeys,
            int numBuckets) {
        return table(name, schemaPk, bucketKeys, numBuckets, CANDLE_NAMES, CANDLE_TYPES, null, true);
    }

    private static TableInfo candle(String name, List<String> schemaPk, List<String> bucketKeys,
            int numBuckets, List<String> columnTypes, boolean pkNonNullable) {
        return table(name, schemaPk, bucketKeys, numBuckets, CANDLE_NAMES, CANDLE_TYPES,
                columnTypes, pkNonNullable);
    }

    private static TableInfo signal(String name, List<String> schemaPk, List<String> bucketKeys,
            int numBuckets) {
        return table(name, schemaPk, bucketKeys, numBuckets, SIGNAL_NAMES, SIGNAL_TYPES, null, true);
    }

    private static TableInfo signal(String name, List<String> schemaPk, List<String> bucketKeys,
            int numBuckets, List<String> columnTypes, boolean pkNonNullable) {
        return table(name, schemaPk, bucketKeys, numBuckets, SIGNAL_NAMES, SIGNAL_TYPES,
                columnTypes, pkNonNullable);
    }

    private static TableInfo tradeDecisions(String name, List<String> schemaPk,
            List<String> bucketKeys, int numBuckets) {
        return table(name, schemaPk, bucketKeys, numBuckets, TRADE_NAMES, TRADE_TYPES, null, true);
    }

    private static TableInfo executionIntent(String name, List<String> schemaPk,
            List<String> bucketKeys, int numBuckets) {
        return table(name, schemaPk, bucketKeys, numBuckets, EXECUTION_INTENT_NAMES,
                EXECUTION_INTENT_TYPES, null, true);
    }

    private static TableInfo executionIntent(String name, List<String> schemaPk,
            List<String> bucketKeys, int numBuckets, List<String> columnTypes,
            boolean pkNonNullable) {
        return table(name, schemaPk, bucketKeys, numBuckets, EXECUTION_INTENT_NAMES,
                EXECUTION_INTENT_TYPES, columnTypes, pkNonNullable);
    }

    private static TableInfo tradeDecisions(String name, List<String> schemaPk,
            List<String> bucketKeys, int numBuckets, List<String> columnTypes,
            boolean pkNonNullable) {
        return table(name, schemaPk, bucketKeys, numBuckets, TRADE_NAMES, TRADE_TYPES,
                columnTypes, pkNonNullable);
    }

    private static TableInfo instructionState(String name, List<String> schemaPk,
            List<String> bucketKeys, int numBuckets) {
        return table(name, schemaPk, bucketKeys, numBuckets, INSTRUCTION_NAMES, INSTRUCTION_TYPES,
                null, true);
    }

    private static TableInfo instructionState(String name, List<String> schemaPk,
            List<String> bucketKeys, int numBuckets, List<String> columnTypes,
            boolean pkNonNullable) {
        return table(name, schemaPk, bucketKeys, numBuckets, INSTRUCTION_NAMES, INSTRUCTION_TYPES,
                columnTypes, pkNonNullable);
    }

    private static TableInfo dedup(String name, List<String> schemaPk, List<String> bucketKeys,
            int numBuckets) {
        return table(name, schemaPk, bucketKeys, numBuckets, DEDUP_NAMES, DEDUP_TYPES, null, true);
    }

    private static TableInfo dedup(String name, List<String> schemaPk, List<String> bucketKeys,
            int numBuckets, List<String> columnTypes, boolean pkNonNullable) {
        return table(name, schemaPk, bucketKeys, numBuckets, DEDUP_NAMES, DEDUP_TYPES,
                columnTypes, pkNonNullable);
    }

    private static TableInfo formingBar(String name, List<String> schemaPk, List<String> bucketKeys,
            int numBuckets) {
        return table(name, schemaPk, bucketKeys, numBuckets, FORMING_BAR_NAMES,
                FORMING_BAR_TYPES, null, true);
    }

    private static TableInfo formingBar(String name, List<String> schemaPk, List<String> bucketKeys,
            int numBuckets, List<String> columnTypes, boolean pkNonNullable) {
        return table(name, schemaPk, bucketKeys, numBuckets, FORMING_BAR_NAMES,
                FORMING_BAR_TYPES, columnTypes, pkNonNullable);
    }

    private static TableInfo candleLive(String name, List<String> schemaPk, List<String> bucketKeys,
            int numBuckets) {
        return table(name, schemaPk, bucketKeys, numBuckets, CANDLE_LIVE_NAMES, CANDLE_LIVE_TYPES, null, true);
    }

    private static TableInfo candleLive(String name, List<String> schemaPk, List<String> bucketKeys,
            int numBuckets, List<String> columnTypes, boolean pkNonNullable) {
        return table(name, schemaPk, bucketKeys, numBuckets, CANDLE_LIVE_NAMES, CANDLE_LIVE_TYPES,
                columnTypes, pkNonNullable);
    }

    private static TableInfo candleClosed(String name, List<String> schemaPk, List<String> bucketKeys,
            int numBuckets) {
        return table(name, schemaPk, bucketKeys, numBuckets, CANDLE_CLOSED_NAMES, CANDLE_CLOSED_TYPES, null, true);
    }

    private static TableInfo candleClosed(String name, List<String> schemaPk, List<String> bucketKeys,
            int numBuckets, List<String> columnTypes, boolean pkNonNullable) {
        return table(name, schemaPk, bucketKeys, numBuckets, CANDLE_CLOSED_NAMES, CANDLE_CLOSED_TYPES,
                columnTypes, pkNonNullable);
    }

    private static TableInfo table(String name, List<String> schemaPk, List<String> bucketKeys,
            int numBuckets, List<String> names, List<String> typeRoots, List<String> columnTypes,
            boolean pkNonNullable) {
        Schema.Builder sb = Schema.newBuilder();
        int cols = columnTypes == null ? names.size() : columnTypes.size();
        for (int i = 0; i < cols; i++) {
            String col = i < names.size() ? names.get(i) : "extra_col_" + i;
            String type = columnTypes == null ? typeRoots.get(i) : columnTypes.get(i);
            sb.column(col, dataType(type, pkNonNullable && schemaPk != null && schemaPk.contains(col)));
        }
        if (schemaPk != null) {
            sb.primaryKey(schemaPk);
        }
        return info(name, sb, bucketKeys, numBuckets);
    }

    private static TableInfo info(String name, Schema.Builder sb, List<String> bucketKeys,
            int numBuckets) {
        return new TableInfo(TablePath.of("default", name), 1L, 1, sb.build(), bucketKeys,
                List.of(), numBuckets, new Configuration(), new Configuration(), null, 0L, 0L);
    }

    private static org.apache.fluss.types.DataType dataType(String typeRoot, boolean notNull) {
        org.apache.fluss.types.DataType t;
        switch (typeRoot) {
            case "BIGINT":
                t = DataTypes.BIGINT();
                break;
            case "STRING":
                t = DataTypes.STRING();
                break;
            case "INTEGER":
                t = DataTypes.INT();
                break;
            case "DOUBLE":
                t = DataTypes.DOUBLE();
                break;
            default:
                throw new IllegalArgumentException("unexpected type root " + typeRoot);
        }
        return notNull ? t.copy(false) : t.copy(true);
    }
}
