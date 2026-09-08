-- eod_offload_state: durable per-day per-table offload records for the EOD
--   controller (SCH-23; docs/08_implementation/01-foundation.md "EOD controller
--   and offload gate", docs/08_implementation/02-schema-storage.md "EOD
--   controller and offload gate")
-- Owner: EOD controller (sole writer)
-- Type: KV state table (PK record_id) — durable with restart/resume; every
--   transition goes through the validated PENDING → WRITING → COMMITTED →
--   VERIFYING → VERIFIED machine (FAILED_RETRYABLE / FAILED_MANUAL exits)
-- Bucket key: record_id. SINGLE-FIELD PK BY DESIGN: the EOD controller is a
--   plain-JVM runner driven by the raw client, which cannot upsert
--   composite-PK KV tables in Fluss 0.9.1 (iceberg key encoder — COMPAT-FLUSS-005
--   matrix). trading_date|table_name is the deterministic record_id;
--   trading_date/table_name stay queryable columns. The single-writer lease
--   row uses the reserved identity 'lease|controller' (token in source_hash,
--   lease expiry in source_offset_start, acquired time in updated_at_ms).
--   DELIBERATE multiplex (P4-077): a dedicated eod_controller_lease table was
--   considered and rejected — a second table + store surgery for a working,
--   tested lease. Every data-plane read (SUM row_count/byte_count, AVG,
--   hash/offset verification, WHERE state=... scans) MUST exclude the lease
--   row ('lease|controller'); FlussEodStateStore.readAll filters it by string
--   equality, guarded by EodOffloadStateColumns.isLeaseRecordId. A bug in any
--   future reader silently skews offload accounting or breaks writer election.
--   Sentinel pattern (P4-079/P4-080): all lifecycle-dependent columns are
--   NOT NULL by design (frozen layout + agreement pin; flipping 6 columns to
--   nullable breaks BinaryString.fromString in the store). PENDING/WRITING rows
--   therefore carry ''/0 sentinels for hashes/offsets/snapshot — empty-string
--   hashes must NEVER satisfy a source==target verification gate
--   ('' == '' is vacuously true); earliest_allowed_source_expiry_ms sentinel 0
--   means expiry FORBIDDEN (NULL-equivalent) until VERIFIED sets it — consumers
--   reading permitsSourceExpiry as now >= gate must treat 0/MAX as blocked.
--   No DDL CHECK in Fluss: the state enum + transition rules live in the
--   sole-writer validator (EodOffloadRecord.transition).
--   Version columns (P4-232): schema_version = payload/row contract version;
--   state_schema_version = PENDING→…→VERIFIED machine version (pin "1").
--   iceberg_snapshot_id STRING (P4-231): Iceberg ids are int64 but the column
--   stays STRING until a full recreate (same ruling as positions P4-187) —
--   string '' sentinel pre-COMMITTED; never let it satisfy snapshot-exists.
-- Retention: table.log.ttl = 2d bounds the changelog; the durable contract is
--   the KV current state (P4-234: kept 2d deliberately, NOT bumped — the log
--   is a lag buffer, not the gate. permitsSourceExpiry MUST read the KV
--   snapshot, never log replay; a consumer replaying the log after >2d sees
--   VERIFIED records disappear. VERIFIED days release source expiry via
--   permitsSourceExpiry; the record is recreated per trading day)
-- Lake: none — transient operational state; the lake target holds the
--   offloaded data, not the controller's own ledger
-- Scope: global (P4-078: singleton EOD controller; record_id =
--   trading_date|table_name is global — no account_scope_id column. If
--   per-scope isolation is ever required, add account_scope_id NOT NULL and
--   redefine record_id = account_scope|date|table, see P4-291 guard)
-- Schema version: 1
-- Buckets (P4-333): 16 buckets for tens of rows is oversized but harmless —
--   kept (changing bucket.num needs a recreate; point lookups stay O(1))

CREATE TABLE eod_offload_state (
    record_id                      STRING      NOT NULL,
    trading_date                   STRING      NOT NULL,
    table_name                     STRING      NOT NULL,
    schema_version                 STRING      NOT NULL,
    source_offset_start            BIGINT      NOT NULL,
    source_offset_end              BIGINT      NOT NULL,
    row_count                      BIGINT      NOT NULL,
    byte_count                     BIGINT      NOT NULL,
    source_hash                    STRING      NOT NULL,
    target_hash                    STRING      NOT NULL,
    iceberg_snapshot_id            STRING      NOT NULL,
    state                          STRING      NOT NULL,
    retry_count                    INT         NOT NULL,
    next_retry_at_ms               BIGINT      NOT NULL,
    earliest_allowed_source_expiry_ms BIGINT   NOT NULL,
    updated_at_ms                  BIGINT      NOT NULL,
    state_schema_version           STRING      NOT NULL,
    PRIMARY KEY (record_id) NOT ENFORCED
) WITH (
    'bucket.num' = '16',
    'bucket.key' = 'record_id',
    'table.log.ttl' = '2d'
);
