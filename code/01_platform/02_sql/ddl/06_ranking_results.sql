-- Ranking_Results: Immutable LOG — per-evaluation ranking audit
-- Owner: Signal job
-- Type: LOG (no primary key)
-- Bucket key: evaluation_id (R-136 — was candidate_id, which scattered one
-- evaluation's rows across all buckets; a consumer reading per evaluation now
-- reads a single bucket)
-- Retention: 7 calendar days via table.log.ttl (T8 G1/G4 7d hardening
-- 2026-08-22 — was 2d; now 7d + block-delete-unverified guard: Fluss delete
-- blocked until iceberg manifest VERIFIED, else EOD controller extends;
-- critical alert)
-- Lake: EOD Iceberg offload
-- Scope: portfolio_id
-- Schema version: 2
--
-- Writer contract (P4-019/020/021 — the ranking feed does not exist yet as a
-- live writer; these bind the future writer, enforced in writer code + a
-- validation job, never in DDL — Fluss has no CHECK/UNIQUE):
--   uniqueness: UNIQUE(evaluation_id, candidate_id) expected; writer
--     dedups retries, readers defend via dedup-by-(evaluation_id,candidate_id).
--   outcome: rank >= 1, UNIQUE(evaluation_id, rank); exactly one
--     selected=true per evaluation_id; selected=true => rejection_reason
--     IS NULL; selected=false => rejection_reason NOT NULL.
--   reproducibility: selected=true => reservation_snapshot/version/hash +
--     tie_break_data NOT NULL; unselected rows carry rejection_reason.
-- Blobs (P4-176): normalized_scores/reservation_snapshot/tie_break_data are
--   JSON shown as STRING (frozen layout) — shape pinned by the payload schema
--   registry; malformed JSON fails in writer validation, not in storage.
-- Version (P4-177): schema_version writer-stamps "2" (bare numeric);
--   evaluation_ts epoch-millis UTC.
-- Retention guard (P4-022): 7d log TTL + 5min tiering is the transport, not
--   the VERIFIED gate — EOD offload extends/blocks expiry on lag/failure via
--   the controller extend path (critical alert), same guard as siblings.
-- Bucketing (P4-178): bucket.key=evaluation_id colocates one evaluation's
--   rows for single-bucket reads; bursty per-evaluation writes hotspot one
--   bucket by design (evaluations are small; 8 buckets). Revisit only on
--   measured skew — no guessed rebucket.

CREATE TABLE Ranking_Results (
    evaluation_id           STRING      NOT NULL,
    candidate_id            STRING      NOT NULL,
    instruction_id          STRING,
    portfolio_id            STRING      NOT NULL,
    model_id                STRING      NOT NULL,
    configuration_version   STRING      NOT NULL,
    normalized_scores       STRING      NOT NULL,
    weight_id               STRING      NOT NULL,
    composite_score         DOUBLE      NOT NULL,
    rank                    INT         NOT NULL,
    selected                BOOLEAN     NOT NULL,
    rejection_reason        STRING,
    reservation_snapshot    STRING,
    reservation_version     STRING,
    capacity_config_hash    STRING,
    evaluation_trigger      STRING      NOT NULL,
    tie_break_data          STRING,
    evaluation_ts           BIGINT      NOT NULL,
    schema_version          STRING      NOT NULL
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'evaluation_id',
    'table.log.ttl' = '7d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
