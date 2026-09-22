-- fingerprint_dedup: HISTORICAL DDL — DO NOT APPLY (P4-073). Retired
-- 2026-08-17 by Design B: the dedup set is authoritative Flink keyed state
-- (FingerprintDedupFunction, heap-window operator — SignalJob "State-
-- authoritative dedup"); this table is no longer a SignalJob startup
-- dependency. Retained as the DDL record only. Auto-apply tooling MUST skip
-- this file; if manifest-listed, that entry is stale and must be removed.
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
--   lake churn at the write rate (datalake disabled by omission, like
--   forming_bar — P4-226: no explicit 'table.datalake.enabled=false' knob
--   recorded; effective behavior falls back to cluster default, acceptable
--   for a retired table, revisit only on reactivation)
-- Scope: instrument_token-global (P4-072: no account_scope_id column —
--   isolation rests on instrument_token global uniqueness; acceptable for a
--   retired table, PK widening is recreate-only and unneeded while retired)
-- Schema version: 1
--
-- Retired-table rulings (no live writer — notes only, revisit on reactivation):
--   writer/cleanup contract (P4-074): expiry_ms = first_seen_ms + 60000,
--     writer-set, min-across-races; cleanup scans expiry_ms.
--   reactivation TTL (2026-09-22, scratch probe): prefer native table.kv.ttl
--     (Fluss 1.0.0, create-time only — ALTER is rejected) over reviving the
--     expiry column plus cleanup-scan contract. Probe: CREATE with ttl stores
--     PT1H; time-column without ttl is rejected; all scratch tables dropped.
--   first_seen race (P4-225): NOT ENFORCED PK = last-writer-wins; the writer
--     MUST use insert-if-absent / keep-earliest first_seen_ms, never blind upsert.
--   TTL mismatch (P4-227): 7d log vs 60s logical life is intentional while
--     retired (EOD-replay buffer); reactivation wants minimal log TTL (hours)
--     + a bucket-scan cleanup plan, not 7d.

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
