-- raw_table_1: Immutable LOG — every accepted market tick
-- Owner: Ingestion
-- Type: LOG (no primary key)
-- Bucket key: instrument_token
-- Retention: 9 calendar days via table.log.ttl (P4-001: EOD VERIFIED buffer —
-- was 7d; live 7d + partition-retention 7d both expired ticks before the
-- iceberg manifest was VERIFIED. The block-delete-unverified guard is the
-- EodControllerTool extend path (isDeleteBlocked/planTables/extend), not a
-- Fluss-side lock — see EodControllerTool "extend". Do not lower this or
-- num-retention without the controller guard in place).
-- Lake: EOD Iceberg offload (R-011: datalake options restored — they were
-- dropped in a rewrite while the header still claimed offload)
-- Scope: none (global market data, not per-account — P4-013: raw ticks carry
-- no account_scope_id column; no per-account projection is possible)
-- Schema version: 3
--
-- v3 (2026-08-31, daily-partition migration — docs/plans/2026-08-31-r2-daily-
-- partitioning-and-archive-plan.md): event_day added as FIRST column and
-- partition key. Fluss auto-partition creates day partitions (yyyyMMdd,
-- Asia/Kolkata) and expires Fluss-side after num-retention; the tiering job
-- mirrors them as iceberg identity partitions -> R2 layout
-- lake/default/raw_table_1/data/event_day=YYYYMMDD/... Partition rules are
-- CREATE-time-frozen (verified: MetadataManager has no partition ALTER path)
-- — this table was RECREATED for v3; prior data archived in R2 under
-- lake/_stale-20260831-v1/. Ingestion fills event_day from event_time (IST).
-- Partition key must be STRING (IcebergLakeCatalog rejects other types);
-- value MUST be exactly yyyyMMdd to match auto-partition naming — a wrong
-- format silently lands rows in a second, non-auto partition (smoke GUARD E
-- asserts folder == today's IST date).
--
-- v2 (2026-08-03, review R-054/R-231): removed the 8 columns the ingestion
-- path never populates — quote fields (bid_price_paise/bid_qty/ask_price_paise/
-- ask_qty, hardcoded 0 by RealFlussRowConverter) and option metadata
-- (instrument_type/strike_paise/expiry/option_type, always empty/null).
-- The DDL must tell the truth: these columns return when the bridge
-- carries quote/derivative data. Column count drops 28 -> 20 (+1 in v3).
--
-- Late/out-of-order ticks (P4-170): event_day derives from event_time
-- (EventDay.ofValidated — format-checked, NOT recency-checked). A tick with
-- an old event_time writes to its old partition, NOT today's — by design
-- (audit fidelity: the tick belongs to its event day). There is NO late-tick
-- quarantine path and NO ingest_ts derivation — a partition older than the
-- 9d retention is already GC'd and the write fails visibly, never silently.
-- Backfill older than retention is unsupported (replay the day's lake data).
-- ack_ts (P4-171): NULL-allowed in DDL (frozen nullability), but BOTH live
-- converters write 0L for unknown (TypedFlussRowConverter + generic 0L path,
-- P1-061) — readers MUST treat NULL and 0 as the same unknown, never
-- WHERE ack_ts = 0 alone. Iceberg stats may show both; that is known.
-- raw_payload lake cost (P4-172): inline BYTES on every tick IS the design —
-- the lake holds full replayable blobs (r2-restore proves a day: 2.29M rows
-- → 313MB parquet). No hash-pointer refactor, no compaction thresholds
-- beyond auto-compaction=true — revisit only on measured R2 cost, not on theory.
-- validity invariant (P4-173): the converters write "" (empty, not NULL)
-- when no reason; NULL arises only from legacy/generic paths. Readers MUST
-- treat NULL and "" identically:
--   invalid <=> validity_state <> 'VALID' (reason may be "" when unset —
--   writer SHOULD set it, but its absence never flips the verdict).
-- schema_version column (P4-327): per-row BRIDGE payload version (converters
-- stamp packet.schemaVersion), NOT the DDL header version (table layout v3).
-- Same name, different domains by design — frozen name; rename needs a
-- recreate + converter + bootstrap + reader change for zero behavioral gain.

CREATE TABLE raw_table_1 (
    event_day               STRING      NOT NULL, -- yyyyMMdd IST from event_time; validated in bridge (EventDay.of+validate) + smoke GUARD E (Fluss has no CHECK)
    event_fingerprint       STRING      NOT NULL, -- not unique in LOG; dedup downstream via fingerprint_dedup + event_fingerprint (P4-169)
    fingerprint_version     STRING      NOT NULL,
    connection_id           STRING      NOT NULL,
    connection_epoch        BIGINT      NOT NULL,
    instrument_token        BIGINT      NOT NULL,
    exchange                STRING      NOT NULL,
    symbol                  STRING      NOT NULL,
    event_time              BIGINT      NOT NULL,
    ingest_ts               BIGINT      NOT NULL,
    ack_ts                  BIGINT      NULL, -- 0 = unknown (R-010)
    tick_type               STRING      NOT NULL,
    last_price_paise        BIGINT      NOT NULL,
    last_qty                BIGINT      NOT NULL,
    raw_payload             BYTES       NOT NULL,
    payload_hash            STRING      NOT NULL,
    decoder_version         STRING      NOT NULL,
    protocol_version        STRING      NOT NULL,
    validity_state          STRING      NOT NULL,
    validity_reason         STRING,
    schema_version          STRING      NOT NULL
) PARTITIONED BY (event_day) WITH (
    'bucket.num' = '16',
    'bucket.key' = 'instrument_token',
    'table.log.ttl' = '9d', -- P4-001: VERIFIED buffer (was 7d); keep > num-retention + EOD SLA
    -- daily partitions: IST day boundary; retains last 9 closed IST days + 2
    -- precreated extra (P4-015: live + precreated = up to 11 physical; the
    -- EOD job plans against the protected bound so the ±1-day IST-midnight
    -- skew between partition GC (day-granular) and log TTL (commit-time) is covered)
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-unit' = 'DAY',
    'table.auto-partition.num-precreate' = '2',
    'table.auto-partition.num-retention' = '9', -- P4-001/P4-015: matches log.ttl 9d (both were 7d; partition GC alone could drop before VERIFIED)
    'table.auto-partition.time-zone' = 'Asia/Kolkata',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
