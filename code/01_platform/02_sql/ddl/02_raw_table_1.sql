-- raw_table_1: Immutable LOG — every accepted market tick
-- Owner: Ingestion
-- Type: LOG (no primary key)
-- Bucket key: instrument_token
-- Retention: 7 calendar days via table.log.ttl (R-011; T8 G1/G4 7d hardening
-- 2026-08-22 — was 2d; block-delete-unverified guard: Fluss TTL delete blocked
-- until iceberg manifest VERIFIED, otherwise EOD controller extends via shadow
-- rewrite; critical alert on unverified).
-- Lake: EOD Iceberg offload (R-011: datalake options restored — they were
-- dropped in a rewrite while the header still claimed offload)
-- Scope: account_scope_id
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

CREATE TABLE raw_table_1 (
    event_day               STRING      NOT NULL, -- yyyyMMdd IST, from event_time (partition key)
    event_fingerprint       STRING      NOT NULL,
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
    'table.log.ttl' = '7d',
    -- daily partitions: IST day boundary; retention aligned with log.ttl (7d)
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-unit' = 'DAY',
    'table.auto-partition.num-precreate' = '2',
    'table.auto-partition.num-retention' = '7',
    'table.auto-partition.time-zone' = 'Asia/Kolkata',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
