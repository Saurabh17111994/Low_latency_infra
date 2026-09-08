-- ingestion_quarantine: immutable evidence for malformed or unsafe broker packets
-- Owner: Ingestion; separate from Action Capture Postback_Quarantine.
-- Type: LOG (no primary key)
-- Bucket key: quarantine_id
-- Retention: operational investigation window (2 calendar days via table.log.ttl)
-- Scope: deployment-scoped (P4-067: single ACCOUNT_SCOPE_ID per ingestion
--   instance — quarantine_id carries instanceId-UUID; no per-row
--   account_scope_id column. Multi-tenant isolation is by deployment.)
-- Schema version: 1 (P4-330: writer emits "1" bare numeric — QuarantineWriter
--   SCHEMA_VERSION pin + unit test; readers must expect "1", not "v1".)
--
-- reason vocabulary (P4-221): QuarantineWriter.Reason —
--   MALFORMED_JSON|INVALID_SCHEMA|MISSING_INSTRUMENT|INVALID_VALUES|
--   FUTURE_BROKER_TIMESTAMP|STALE_BROKER_TIMESTAMP|HASH_MISMATCH|
--   INTERNAL_ERROR|FINGERPRINT_FAILURE. Writer-only enum (DDL has no CHECK);
--   detail is scrubbed (SECRET/BEARER) + truncated to 512 chars writer-side.
--   No disposition lifecycle on this table — disposition lives in the external
--   triage workflow before the 2d TTL deletes the row.
-- raw_payload governance (P4-068): verbatim broker bytes, writer-coerced
--   null->empty; detail is the only scrubbed field. Payloads are retained
--   for triage under the platform access policy (no per-row encryption flag
--   in DDL). A max-size/truncate policy is future work — no silent truncation
--   today, by design (evidence fidelity over storage bound).
-- payload_hash (P4-222): SHA-256 hex (64 chars) of the STORED bytes —
--   null/empty payloads hash their actual (empty) bytes, never "".
--   hash-validates-raw_payload holds for every row.
-- detected_ts (P4-223): epoch-ms detection wall-clock
--   (Instant.now().toEpochMilli); TTL expiry follows log-append time, not
--   this column. Instance correlation via the quarantine_id instanceId- prefix
--   (no separate instance_id/connection_epoch columns, frozen layout).
-- Retention (P4-069): 2d Fluss window is AUTHORITATIVE — no lake twin
--   (manifest lake_policy off; EodControllerTool cannot tier it). Triage past
--   2d is unsupported by design; extend TTL (recreate) if the window must grow.
-- Identity (P4-329): LOG + instanceId-UUIDv4 ids — duplicates tolerable,
--   consumers key on (quarantine_id, payload_hash); no KV promotion.
-- Bucketing (P4-331): bucket.key=quarantine_id = even spread, point-lookup
--   only; per-instrument investigations full-scan by design (matches
--   DdlBootstrap.distributedBy(8, quarantine_id) — no drift).
--
-- v1 (2026-08-15): header completed — the DDL previously carried no
-- Type/Retention/Schema-version header, leaving the manifest's
-- schema_version/retention_policy fields unemittable for this table.
CREATE TABLE ingestion_quarantine (
    quarantine_id       STRING NOT NULL,
    reason              STRING NOT NULL,
    instrument_token    BIGINT,
    exchange            STRING,
    symbol              STRING,
    raw_payload         BYTES NOT NULL,
    payload_hash        STRING NOT NULL,
    detected_ts         BIGINT NOT NULL,
    detail              STRING,
    schema_version      STRING NOT NULL
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'quarantine_id',
    'table.log.ttl' = '2d'
);
