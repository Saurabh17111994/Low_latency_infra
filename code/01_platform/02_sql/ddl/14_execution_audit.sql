-- Execution_Audit: Immutable LOG — all execution lifecycle events
-- Owner: Executor
-- Type: LOG (no primary key)
-- Bucket key: audit_event_id
-- Retention: ≥3 trading days — 5 calendar days via table.log.ttl (R-087:
--   3 calendar days could contain only 1-2 trading days over a weekend/
--   holiday; 5 calendar days always covers 3 trading days)
-- Lake: encrypted immutable audit under approved policy (one-year minimum target)
-- Scope: account_scope_id, execution_partition_id
-- Schema version: 2
--
-- Domain contract (P4-049/P4-050/P4-198/P4-199/P4-200 — LOG takes no PK;
-- Fluss has no CHECK/DEFAULT/write-mode/PARTITION; enforced in code):
--   tamper barrier (P4-049): no PK BY DESIGN (LOG audit trail; a PK pushes
--     toward KV semantics). Barriers are writer-side: audit() append-only
--     API + AuditDeletionControl hash chain (non-repudiation); evidence_hash
--     non-null guidance per event_type lives in the writer, not the column.
--     write-mode='append-only' is a Paimon property, not Fluss — rejected.
--   retention split (P4-050): 5d table.log.ttl is the HOT window per R-087
--     (5 calendar days always cover 3 trading days); 1-year history lives in
--     the Iceberg lake (datalake enabled, audit carrier). No lake-retention
--     property exists in Fluss DDL — lake lifecycle is admin-owned.
--   envelope (P4-198): event_type closed set lives at the AuditRecord call
--     sites (FENCE_ACQUIRE/FENCE_RENEW/FENCE_REVOKE/APPROVE/BRIDGE_OUTCOME…);
--     schema_version writer-pinned '2'; event_ts epoch-millis UTC >= 0;
--     gate_epoch monotonic per partition by the fencing sequence.
--   correlation (P4-199): instruction_id/execution_attempt_id nullable BY
--     DESIGN (gate-level events have no attempt); actor_id is the
--     authenticated principal at the call site (executor instance / saurabh
--     approval); evidence_summary unbounded — blob-off-table is future writer
--     work, hash pointer stays here.
--   bucket key (P4-200): bucket.key = audit_event_id (UUID) is uniform-write
--     optimal; scoped/time-range replays fan out across 8 buckets — accepted
--     cost for a 5d forensic window. No time partitioning in Fluss DDL.

CREATE TABLE Execution_Audit (
    audit_event_id          STRING      NOT NULL,
    event_type              STRING      NOT NULL,
    instruction_id          STRING,
    execution_attempt_id    STRING,
    execution_partition_id  STRING      NOT NULL,
    account_scope_id        STRING      NOT NULL,
    gate_epoch              BIGINT      NOT NULL,
    actor_id                STRING      NOT NULL,
    evidence_hash           STRING,
    evidence_summary        STRING,
    event_ts                BIGINT      NOT NULL,
    schema_version          STRING      NOT NULL
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'audit_event_id',
    'table.log.ttl' = '5d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
