-- Safety_Halt_Requests: KV control table — durable halt requests from authorized components
-- Owner: Authorized components (Signal job, Action Capture, platform health, operators)
-- Type: KV (primary key on halt_request_id)
-- Bucket key: halt_request_id
-- Retention: safety/reconciliation window (30 calendar days via table.log.ttl)
-- Lake: encrypted immutable audit under approved policy (one-year minimum target)
-- Scope: account_scope_id, portfolio_id, execution_partition_id
-- Schema version: 3
--
-- Row contract (P4-213 class — writer/header drift): SafetyHaltWriter stamps
-- schema_version "3" (bare numeric, SCHEMA_VERSION pin + unit test); the
-- parser gates on contract_version=2, not schema_version.
-- halt_request_id binding (P4-060): the ID is caller-computed — DDL/Fluss
-- cannot derive or CHECK it — but the sole writer
-- (SafetyHaltWriter.computeHaltRequestId) SHA-256 hexes the documented
-- tuple, the parser rejects malformed rows, and the KV PK makes a duplicate
-- delivery an upsert no-op. Any second writer MUST reuse computeHaltRequestId.
-- Scope of v2 slot fields (P4-061): slot-scoped ONLY. The live writer
-- requires slot_id/connection_epoch/manifest_fingerprint
-- (requireWritableArgs); there is no account-halt path — account/portfolio
-- halts are future work, not dummy-valued rows on this table.
-- Audit target (P4-062): 30d online + 5min lake sync is the transport, not
-- the 1-year immutable audit — that needs Iceberg snapshot retention ≥365d +
-- WORM/object-lock + KMS + lag alerting, owned lake-side, not in this DDL.
-- Lifecycle (P4-210): application_result vocab OPEN|APPLIED|REJECTED|EXPIRED;
-- writer inserts OPEN + applied_ts NULL (initial state, not a contradiction).
-- Empty-string status values are rejected by the parser (readString), not by DDL.
-- Dedup semantics (P4-211): PRIMARY KEY NOT ENFORCED is required Fluss
-- syntax; single-row-per-ID comes from KV upsert (last-writer-wins).
-- A re-delivery with different application_result/applied_ts is a CLOBBER,
-- not a no-op — mutable apply-state shares the row by design; readers must
-- treat the latest row as current, not as immutable history.
-- Bucketing (P4-212): bucket.key=halt_request_id optimizes point lookup by
-- ID (the SafetyHaltJob path); scope/time reconciliation scans are full-table
-- by design at this volume — no PARTITIONED BY, no secondary index.
--
-- v3 (2026-08-03, review R-089): was LOG. halt_request_id is the deterministic
-- SHA-256 of the transition tuple, and the caller dedups on it — but a LOG
-- table does not enforce uniqueness, so two deliveries of the same halt would
-- append twice. As a KV table the storage layer enforces one row per
-- halt_request_id: a duplicate delivery is an upsert no-op.
--
-- v2 (offline migration, plan B2): adds ingestion slot-scoped safety fields so
-- the Ingestion service can emit per-slot unsafe/recovered evidence consumed by
-- the Signal job. halt_request_id is the SHA-256 hex of the pipe-separated
-- tuple: manifest_fingerprint|slot_id|connection_epoch|state|reason_code.
-- DDL is applied only through the offline make ddl/reconciliation gate — never
-- at runtime.

CREATE TABLE Safety_Halt_Requests (
    halt_request_id         STRING      NOT NULL,
    account_scope_id        STRING      NOT NULL,
    portfolio_id            STRING,
    execution_partition_id  STRING,
    source_component        STRING      NOT NULL,
    source_instance         STRING      NOT NULL,
    reason_code             STRING      NOT NULL,
    reason_detail           STRING,
    detection_time          BIGINT      NOT NULL,
    source_epoch            BIGINT      NOT NULL,
    evidence_hash           STRING      NOT NULL,
    application_result      STRING      NOT NULL,
    applied_ts              BIGINT,
    schema_version          STRING      NOT NULL,
    -- v2 additions (plan B2 slot-scoped safety):
    slot_id                 STRING      NOT NULL,
    connection_epoch        BIGINT      NOT NULL,
    manifest_fingerprint    STRING      NOT NULL,
    assigned_token_set_hash STRING      NOT NULL,
    state                   STRING      NOT NULL,
    evidence_reference      STRING,
    contract_version        INT         NOT NULL,
    PRIMARY KEY (halt_request_id) NOT ENFORCED
) WITH (
    'bucket.num' = '4',
    'bucket.key' = 'halt_request_id',
    'table.log.ttl' = '30d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
