-- Signal_Candidates_current: KV current-state companion to the immutable LOG
-- Owner: Signal job
-- Type: KV (primary key on instrument_token)
-- Bucket key: instrument_token (colocates with Signal_Candidates; the Fluss
--   connector requires bucket.key to be a SUBSET of the primary key, and this
--   single-column PK keeps every signal of a ticker in the same bucket as its
--   LOG twin — DEC-035)
-- Retention: changelog 7d via table.log.ttl; KV snapshot UNBOUNDED until
--   superseded/overwritten/tombstoned (P4-071 — log TTL never bounds KV
--   state). No expiry_ms/cleanup pass on this table by design (single-active
--   row per instrument is always current-or-stale, never garbage); consumers
--   restarting after days must treat a row older than the changelog as
--   possibly-stale and re-derive from the LOG twin.
-- Staleness (P4-070/224): KV upserts are last-writer-wins on
-- instrument_token with NO ordering guard in the sink — a delayed/retried
-- out-of-order signal CAN clobber a newer row. Ordering is the producer's
-- contract (evaluation_ts/detection_ts epoch-millis UTC, evaluation_ts >=
-- detection_ts; see 05 twin domain contract): producers emit in order and
-- the job processes in event-time order; no Flink deduplicate/row-number
-- stage exists today. ts unit: epoch millis UTC (P4-224 — same convention
-- as the LOG twin; no _ms rename, frozen layout; no valid_until column —
-- staleness is derived, not stored).
-- Bucketing (P4-332, DEC-035 colocation): bucket.num=16/bucket.key=
-- instrument_token MUST stay identical to the 05 LOG twin — single-ticker
-- reads stay single-bucket on both sides. Pinned by
-- TableContractValidatorTest; a change to either side needs the other.
-- Lake: 5-min tiering to Iceberg; EOD VERIFIED guard via EodControllerTool
--   extend path when included explicitly (same P4-018 note as the LOG twin)
-- Scope: none (global pre-portfolio signal — P4-016; portfolio scoping starts
--   at Trade_Decisions; single active row per instrument_token BY DESIGN, P4-003)
-- Schema version: 1
--
-- Single-active semantics (P4-003 — deliberate, not a missing column): one row
-- per instrument_token; a supersession upserts the same key. Cross-portfolio /
-- cross-strategy fan-out is filtered BEFORE this sink by CanonicalSignalPolicy
-- (only pinned strategy/rule ids admitted; count noncanonical via
-- compute.signal.kv.filtered.noncanonical). A composite
-- (portfolio_id, instrument_token, strategy_id, rule_id) key is a deliberate
-- v-next: it needs portfolio_id plumbed through SignalJobConfig +
-- SignalCandidatesTableColumns + DdlBootstrap schema + this PK + the contract
-- test's exact-PK pin — not a header edit.
--
-- Why KV (2026-08-13, DEC-035): Signal_Candidates is an immutable LOG (v3) —
-- one row per fired signal, never updated — so consumers of "the current
-- signal per instrument" cannot tell which row is current. As a KV table the
-- storage layer enforces one row per instrument_token and a supersession
-- upserts the same key — the LOG stays as immutable audit, this table is the
-- idempotent current-state projection.

CREATE TABLE Signal_Candidates_current (
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
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (instrument_token) NOT ENFORCED
) WITH (
    'bucket.num' = '16',
    'bucket.key' = 'instrument_token',
    'table.log.ttl' = '7d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
