-- fingerprint_dedup: HISTORICAL DDL (DEC-038 era, superseded 2026-08-17 by Design B:
--   the dedup set is authoritative Flink keyed state; this table is no longer a SignalJob
--   startup dependency — kept on file as the DDL record). One row per accepted fingerprint
--   within its logical TTL (DEC-038; docs/08_implementation/04-signal-job.md
--   "Design — fingerprint_dedup dedup state table")
-- Owner: Signal job
-- Type: KV state table (PK instrument_token, fingerprint_version, event_fingerprint)
-- Bucket key: instrument_token (PK prefix — per-instrument colocation; the Fluss
--   connector requires bucket.key ⊆ primary key)
-- Retention: table.log.ttl = 7d bounds the underlying log (T8 G1/G4 7d hardening
--   2026-08-22 — was 2d; now 7d + block-delete-unverified guard: Fluss delete
--   blocked until iceberg manifest VERIFIED, else EOD controller extends;
--   critical alert). The logical dedup lifetime is the column-based expiry
--   (DEDUP_TTL_MS = 60000), enforced by the writer + cleanup pass — never
--   the log TTL alone
-- Lake: none — transient state (logical life ≤ 1 min); no EOD/audit value; avoids
--   lake churn at the write rate (datalake disabled, like forming_bar)
-- Scope: account_scope_id
-- Schema version: 1

CREATE TABLE fingerprint_dedup (
    instrument_token     BIGINT      NOT NULL,
    fingerprint_version  STRING      NOT NULL,
    event_fingerprint    STRING      NOT NULL,
    first_seen_ms        BIGINT      NOT NULL,
    expiry_ms            BIGINT      NOT NULL,
    schema_version       STRING      NOT NULL,
    PRIMARY KEY (instrument_token, fingerprint_version, event_fingerprint) NOT ENFORCED
) WITH (
    'bucket.num' = '16',
    'bucket.key' = 'instrument_token',
    -- Composite-PK KV tables need kv.format-version=2 + a single-field subset
    -- bucket key for the raw client (COMPAT-FLUSS-005 matrix; same config as
    -- feature_candles_15s/instruments). The Flink connector writes the table
    -- regardless, but the apply smoke must stay green.
    'table.kv.format-version' = '2',
    'table.log.ttl' = '7d'
);
