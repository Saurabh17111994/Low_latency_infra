-- Signal_Candidates: immutable LOG — one row per fired signal, never updated
-- Owner: Signal job
-- Type: LOG (no primary key)
-- Bucket key: instrument_token
-- Retention: ≤7 calendar days via table.log.ttl
-- Lake: 5-min tiering to Iceberg (datalake.freshness=5min); EOD VERIFIED guard
--   via the EodControllerTool extend path when this table is included explicitly
--   (not in DEFAULT_TABLES — pass --tables Signal_Candidates; P4-018: header
--   previously claimed EOD offload while the property is 5-min tiering)
-- Scope: none (global pre-portfolio signal; portfolio scoping starts at
--   Trade_Decisions — P4-016: header previously claimed portfolio_id with no column)
-- Schema version: 3
--
-- Domain contract (P4-174 — Fluss has no CHECK/DEFAULT; enforced in job code):
--   detection_ts/evaluation_ts epoch-millis UTC, evaluation_ts >= detection_ts;
--   quantity > 0; action ENTRY|CANCEL, side BUY|SELL, order_type MARKET|LIMIT
--   (LIMIT requires limit_price_paise — enforced in ExecutionIntentBuilder.validate
--   + TradeDecisionBuilder.requireValid); schema_version writer-set '3'.
-- Dedup authority (P4-017): candidate_id is the logical identity; producer
--   retries reuse the same id and StrategyHostFunction.emittedIds (checkpointed
--   MapState keyed by candidate_id) suppresses re-emission before either sink;
--   downstream joins/offload must still DEDUP BY candidate_id.
--
-- v2 (2026-08-03, review R-084): was LOG; converted to KV keyed on
-- candidate_id so the supersede chain could update appended rows.
-- v3 (2026-08-13, DEC-035 requirement change): reverts to LOG. The supersede
-- chain never materialized in this phase, and current-state consumers read
-- the KV projection Signal_Candidates_current (23_signal_candidates_current.sql)
-- keyed by instrument_token — so this table can stay append-only audit.
-- Supersede columns are retained for audit linkage (22-column layout frozen).
--
-- Forward link (P4-175): superseded_by_candidate_id is ALWAYS NULL on this
-- LOG — rows are never backfilled. Forward linkage lives ONLY in the KV
-- projection (upsertable) or via self-join on supersedes_candidate_id.
-- Readers must never filter WHERE superseded_by_candidate_id IS NOT NULL here.

CREATE TABLE Signal_Candidates (
    candidate_id            STRING      NOT NULL,
    instruction_id          STRING,
    trade_context_id        STRING,
    instrument_token        BIGINT      NOT NULL,
    exchange                STRING      NOT NULL,
    symbol                  STRING      NOT NULL,
    strategy_id             STRING      NOT NULL,
    strategy_version        STRING      NOT NULL,
    rule_id                 STRING      NOT NULL,
    detection_ts            BIGINT      NOT NULL,
    evaluation_ts           BIGINT      NOT NULL,
    action                  STRING      NOT NULL,
    side                    STRING      NOT NULL,
    quantity                BIGINT      NOT NULL,
    order_type              STRING,
    limit_price_paise       BIGINT,
    score_inputs            STRING,
    formation_snapshot_ref  STRING,
    validity_reason         STRING,
    supersedes_candidate_id STRING,
    superseded_by_candidate_id STRING,
    schema_version          STRING      NOT NULL
) WITH (
    'bucket.num' = '16',
    'bucket.key' = 'instrument_token',
    'table.log.ttl' = '7d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
