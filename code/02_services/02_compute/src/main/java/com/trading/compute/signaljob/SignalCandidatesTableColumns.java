package com.trading.compute.signaljob;

import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Physical column layout of the signal tables as written by the Signal job.
 *
 * <p>Must mirror the frozen 22-column layout of
 * {@code code/01_platform/02_sql/ddl/05_signal_candidates.sql} v3
 * (LOG, DEC-035) and its KV current-state companion
 * {@code 23_signal_candidates_current.sql} column-for-column. The Fluss
 * sinks' {@code RowDataSerializationSchema} projects by column name, so
 * emitted {@code GenericRowData} fields must be positioned in DDL column
 * order.
 */
public final class SignalCandidatesTableColumns {

    private SignalCandidatesTableColumns() {}

    public static final int CANDIDATE_ID = 0;
    public static final int INSTRUCTION_ID = 1;
    public static final int TRADE_CONTEXT_ID = 2;
    public static final int INSTRUMENT_TOKEN = 3;
    public static final int EXCHANGE = 4;
    public static final int SYMBOL = 5;
    public static final int STRATEGY_ID = 6;
    public static final int STRATEGY_VERSION = 7;
    public static final int RULE_ID = 8;
    public static final int DETECTION_TS = 9;
    public static final int EVALUATION_TS = 10;
    public static final int ACTION = 11;
    public static final int SIDE = 12;
    public static final int QUANTITY = 13;
    public static final int ORDER_TYPE = 14;
    public static final int LIMIT_PRICE_PAISE = 15;
    public static final int SCORE_INPUTS = 16;
    public static final int FORMATION_SNAPSHOT_REF = 17;
    public static final int VALIDITY_REASON = 18;
    public static final int SUPERSEDES_CANDIDATE_ID = 19;
    public static final int SUPERSEDED_BY_CANDIDATE_ID = 20;
    public static final int SCHEMA_VERSION = 21;

    public static final int FIELD_COUNT = 22;

    /** Fixed candidate values written by the MVP detection operator (DEC-034). */
    public static final String ACTION_ENTRY = "ENTRY";
    /** Early-signal supersession action (Phase 2, 2026-08-29): a tentative
     *  that failed at window end is CANCEL-led (auditable, never silent). */
    public static final String ACTION_CANCEL = "CANCEL";
    public static final String SIDE_BUY = "BUY";
    /** Short-entry side (N7 range-breakout, 2026-09-05): a close below the
     *  setup low emits a SELL entry candidate. The MVP 20-candle rule was
     *  long-only; the DDL column is a free string so no contract changes. */
    public static final String SIDE_SELL = "SELL";
    public static final String ORDER_TYPE_MARKET = "MARKET";
    public static final String VALIDITY_REASON_VALID = "VALID";
    /** Early-signal lifecycle reasons (Phase 2, 2026-08-29). */
    public static final String VALIDITY_REASON_TENTATIVE = "TENTATIVE";
    public static final String VALIDITY_REASON_CONFIRMED = "CONFIRMED";
    public static final String VALIDITY_REASON_SUPERSEDED = "SUPERSEDED";
    public static final String SCHEMA_VERSION_V2 = "2";

    /**
     * Canonical signal identity (DEC-035, tracker 14 re-scoped P2 —
     * SIGNAL-SCHEMA-001): the only {@code (schema_version, strategy_id,
     * strategy_version, rule_id)} combination allowed into the
     * {@code Signal_Candidates_current} KV projection. The config defaults
     * for {@code SIGNAL_STRATEGY_ID}/{@code SIGNAL_STRATEGY_VERSION} are
     * exactly these values (mirroring the candle pair policy); each rule
     * producer pins its own rule id (forming bar, N7 — both admitted as
     * alternates below). The LOG sink keeps every emitted signal regardless.
     */
    public static final String CANONICAL_STRATEGY_ID = "simple-breakout";
    public static final String CANONICAL_STRATEGY_VERSION = "1.0.0";
    public static final String CANONICAL_RULE_ID = "breakout-20-bullish-trend";

    /**
     * Second pinned canonical rule id (Slice 2.2 forming-bar handoff,
     * Phase C): the forming-bar placeholder detector's rule. The KV
     * current-state filter admits BOTH canonical rule ids (the candle rule
     * and the forming-bar rule) with the same canonical strategy identity;
     * everything else stays filtered from the KV projection. PLACEHOLDER
     * identity — the real strategy replaces it without pipeline changes.
     */
    public static final String CANONICAL_FORMING_RULE_ID = "breakout-5-forming-bar";

    /**
     * Third pinned canonical rule id (N7 range-contraction entry, 2026-09-05):
     * the N7 operator's rule. The KV current-state filter admits this id too
     * (design {@code docs/plans/2026-09-05-n7-signal-design.md}) so N7
     * candidates reach the KV current-state projection like the forming-bar
     * rule's. Same canonical strategy identity; the LOG keeps every signal.
     */
    public static final String CANONICAL_N7_RULE_ID = "n7-range-breakout-v1";

    /**
     * Fluss {@code DataTypeRoot} name per column, DDL index order (frozen
     * 22-column v3 layout). The re-targeted table contract validator compares
     * live metadata roots against this list column by column.
     */
    public static final List<String> TYPE_ROOTS = List.of(
            "STRING", "STRING", "STRING", "BIGINT", "STRING", "STRING", "STRING",
            "STRING", "STRING", "BIGINT", "BIGINT", "STRING", "STRING", "BIGINT",
            "STRING", "BIGINT", "STRING", "STRING", "STRING", "STRING", "STRING",
            "STRING");

    /**
     * DDL nullability per column (05/23 DDLs): {@code true} = nullable,
     * {@code false} = NOT NULL. The KV PK column {@code instrument_token}
     * plus the identity/decision columns are NOT NULL; audit linkage columns
     * are nullable. Same semantic as
     * {@link com.trading.common.schema.CandleTableSchema#COLUMN_NULLABLE_IN_DDL}
     * — used only for the schema-report divergence marker (live LOG metadata
     * does not carry NOT NULL).
     */
    public static final List<Boolean> COLUMN_NULLABLE_IN_DDL = List.of(
            false, true, true, false, false, false, false, false, false, false,
            false, false, false, false, true, true, true, true, true, true,
            true, false);


    /** DDL column names in index order (diagnostics). */
    public static final String[] NAMES = {
        "candidate_id", "instruction_id", "trade_context_id", "instrument_token",
        "exchange", "symbol", "strategy_id", "strategy_version", "rule_id",
        "detection_ts", "evaluation_ts", "action", "side", "quantity",
        "order_type", "limit_price_paise", "score_inputs", "formation_snapshot_ref",
        "validity_reason", "supersedes_candidate_id", "superseded_by_candidate_id",
        "schema_version"
    };

    /**
     * Stream type info for emitted candidate rows, derived from the frozen
     * v3 DDL column order. Declared explicitly because TypeExtractor cannot
     * resolve a bare {@code RowData} to a schema — it would fall back to
     * GenericTypeInfo and route RowData through Kryo at operator boundaries.
     */
    public static final TypeInformation<RowData> ROW_TYPE_INFO = InternalTypeInfo.ofFields(
            new LogicalType[] {
                new VarCharType(VarCharType.MAX_LENGTH), // candidate_id
                new VarCharType(VarCharType.MAX_LENGTH), // instruction_id
                new VarCharType(VarCharType.MAX_LENGTH), // trade_context_id
                new BigIntType(),                        // instrument_token
                new VarCharType(VarCharType.MAX_LENGTH), // exchange
                new VarCharType(VarCharType.MAX_LENGTH), // symbol
                new VarCharType(VarCharType.MAX_LENGTH), // strategy_id
                new VarCharType(VarCharType.MAX_LENGTH), // strategy_version
                new VarCharType(VarCharType.MAX_LENGTH), // rule_id
                new BigIntType(),                        // detection_ts
                new BigIntType(),                        // evaluation_ts
                new VarCharType(VarCharType.MAX_LENGTH), // action
                new VarCharType(VarCharType.MAX_LENGTH), // side
                new BigIntType(),                        // quantity
                new VarCharType(VarCharType.MAX_LENGTH), // order_type
                new BigIntType(),                        // limit_price_paise
                new VarCharType(VarCharType.MAX_LENGTH), // score_inputs
                new VarCharType(VarCharType.MAX_LENGTH), // formation_snapshot_ref
                new VarCharType(VarCharType.MAX_LENGTH), // validity_reason
                new VarCharType(VarCharType.MAX_LENGTH), // supersedes_candidate_id
                new VarCharType(VarCharType.MAX_LENGTH), // superseded_by_candidate_id
                new VarCharType(VarCharType.MAX_LENGTH)  // schema_version
            },
            NAMES);
}
