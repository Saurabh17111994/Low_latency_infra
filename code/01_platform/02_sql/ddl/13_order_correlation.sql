-- Order_Correlation: KV lookup — maps platform IDs to broker IDs
-- Owner: Executor
-- Type: KV (primary key on instruction_id, execution_attempt_id)
-- Retention: active + reconciliation window (30 calendar days via table.log.ttl)
-- Lake: encrypted immutable audit under approved policy (one-year minimum target)
-- Scope: account_scope_id (R-145: column materialized — the header declared
--   account scoping but the schema had no such column)
-- Schema version: 2 (routing/encoding compatibility amendment; columns unchanged)
--
-- v2 (2026-08-03, review R-086): composite PK. A single instruction can be
-- retried as multiple execution attempts, each producing a distinct
-- broker_order_id; keying the correlation row only on instruction_id meant a
-- retry silently overwrote the previous attempt's correlation. The composite
-- key (instruction_id, execution_attempt_id) keeps one correlation row per
-- attempt.
--
-- Domain contract (P4-045/P4-046/P4-047/P4-048/P4-196/P4-197 — NOT ENFORCED
-- is engine reality; Fluss has no CHECK/RLS/indexes; enforced in code):
--   tenant predication (P4-045, composite-PK rewrite REJECTED): every access
--     path predicates on account_scope_id (lookups carry it —
--     approvedReconciliation(accountScopeId, brokerOrderId), AttemptRef
--     requires it). instruction_ids are ins-v1- + hash (unguessable, not
--     sequential) — the cross-tenant-guessing threat does not exist. The
--     triple-column PK + re-bucketing would break the R-086 composite key,
--     the columns-class pin, and raw-client writability (same ruling as
--     P4-075/P4-085).
--   verification (P4-046): verification_state closed set
--     (PENDING/VERIFIED/FAILED/SUPERSEDED) lives in the correlator/
--     quarantine path; VERIFIED requires broker_order_id + evidence (writer
--     rule, no CHECK in Fluss). NULL broker_order_id = uncorrelated BY
--     DESIGN (pre-broker rows join on client_order_ref instead).
--   uniqueness (P4-047): PK NOT ENFORCED is a planner hint; the guarantee is
--     KV upsert on the full composite key + single-writer premise (same as
--     P4-043/P4-086) — no ENFORCED/index surgery exists in Fluss.
--   retention split (P4-048): 30d hot changelog; 1-year history in the lake
--     (admin-owned lifecycle; no lake-retention property in Fluss DDL).
--     Deletes tombstone to lake before log expiry; lake lag/failure is
--     monitored, not DDL-declared.
--   units/version (P4-196): correlated_ts epoch-millis UTC; schema_version
--     writer-pinned '2'. Reverse lookup (broker_order_id -> instruction_id)
--     fans out — no secondary indexes in Fluss; accepted cost, or a lookup
--     table if reconciliation volume demands it (follow-up, not this batch).
--   bucket key (P4-197): instruction_id colocation is the RIGHT key here —
--     per-instruction attempt locality serves the retry-correlation read
--     path. execution_attempt_id adds no distribution benefit; stated.

CREATE TABLE Order_Correlation (
    instruction_id          STRING      NOT NULL,
    execution_attempt_id    STRING      NOT NULL,
    account_scope_id        STRING      NOT NULL,
    client_order_ref        STRING      NOT NULL,
    broker_order_id         STRING,
    trade_context_id        STRING,
    position_id             STRING,
    verification_state      STRING      NOT NULL,
    verification_evidence   STRING,
    correlated_ts           BIGINT      NOT NULL,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (instruction_id, execution_attempt_id) NOT ENFORCED
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'instruction_id',
    'table.kv.format-version' = '2',
    'table.log.ttl' = '30d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
