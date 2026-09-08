-- suspected_discontinuities: Immutable LOG — connection/subscription/heartbeat/time evidence
-- Owner: Ingestion
-- Type: LOG (no primary key)
-- Bucket key: discontinuity_id (synthetic)
-- Retention: operational investigation window
-- Lake: optional operational lake retention
-- Scope: deployment-scoped (P4-063: single ACCOUNT_SCOPE_ID per ingestion
--   instance — connection-level evidence, no per-row account_scope_id column.
--   Multi-tenant isolation is by deployment, not by row filter.)
-- Schema version: 2 (P4-213: writer emits "2" bare numeric — DiscontinuityWriter
--   SCHEMA_VERSION pin + unit test; readers gating on schema_version must
--   expect "2", not "v1". v1->v2 delta: header completed + writer contract.)
--
-- source vocabulary (P4-215): write() stamps the shared connectionId;
-- writeWithEpoch() prefers the bridge connectionId, then slotId, then the
-- "ingestion" constant. No slot_id/connection_id/instance_id columns by
-- design (frozen layout) — per-slot triage parses source/discontinuity_id
-- prefix (instanceId-UUID); instance correlation via the discontinuity_id prefix.
-- detail loss (P4-064, known): the writer's note rides the log line only
-- (R-249) — no detail column in this frozen layout. Operator/bridge reasons
-- survive in process logs (OpenObserve), not queryably here; a detail column
-- needs a recreate. detected_ts is epoch-ms detection wall-clock
-- (Instant.now().toEpochMilli), NOT log-append time — TTL expiry follows
-- append time, so skew/backfill can reorder detected_ts vs expiry.
-- Idempotency (P4-214): LOG + instanceId-UUID ids accept duplicates by
-- design — retries/at-least-once bridge events append. Consumers dedup by
-- (source, reason, detected_ts) window, not by ID; deterministic IDs would
-- change writer identity + break caller dedup.
-- Bucketing (P4-216): bucket.key=discontinuity_id = point-lookup only;
-- time/source investigation scans scatter by design (2d window, low volume).
-- Lake (P4-217): Fluss 2d window + lake mirror; lake retention is
-- operational/triage-window (same 2d horizon) — delayed triage beyond 2d
-- queries the lake, not Fluss.

CREATE TABLE suspected_discontinuities (
    discontinuity_id        STRING      NOT NULL,
    source                  STRING      NOT NULL,
    reason                  STRING      NOT NULL,
    connection_epoch        BIGINT      NOT NULL,
    last_tick_ts            BIGINT,
    last_tick_fingerprint   STRING,
    last_tick_token         BIGINT,
    last_tick_exchange      STRING,
    last_tick_symbol        STRING,
    detected_ts             BIGINT      NOT NULL,
    schema_version          STRING      NOT NULL
) WITH (
    'bucket.num' = '4',
    'bucket.key' = 'discontinuity_id',
    'table.log.ttl' = '2d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
