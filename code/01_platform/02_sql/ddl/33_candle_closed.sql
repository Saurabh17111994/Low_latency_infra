-- candle_closed: KV immutable closed history per timeframe (multi-TF aggregator Phase 0)
-- Owner: Signal job
-- Type: KV (primary key on instrument_token, tf, window_start)
-- Bucket key: instrument_token (strict subset of the PK — per-ticker
--   colocation, and the Fluss connector requires bucket.key ⊆ primary key).
-- Retention: 7 calendar days via table.log.ttl (same as feature_candles_15s,
--   R-055 / T8 G1/G4 7d hardening).
-- Lake: EOD Iceberg offload with 5min freshness + auto-compaction (same as
--   03_feature_candles_15s — history kept, offloaded).
-- Scope: account_scope_id
-- Schema version: 1 (2026-09-05 — multi-TF aggregator Phase 0; tf discriminator)
--
-- Columns: same 15 as candle_live (DDL order) — tf discriminator values:
--   FIFTEEN_S, THIRTY_S, ONE_M, THREE_M, FIVE_M, FIFTEEN_M.
-- Writes: MultiTfAggregatorFunction boundary timers — one row per non-empty
--   bucket per timeframe per instrument, first-write-wins (immutable).

CREATE TABLE candle_closed (
    instrument_token        BIGINT      NOT NULL,
    exchange                STRING,
    symbol                  STRING,
    tf                      STRING      NOT NULL,
    window_start            BIGINT      NOT NULL,
    window_end              BIGINT      NOT NULL,
    open_paise              BIGINT      NOT NULL,
    high_paise              BIGINT      NOT NULL,
    low_paise               BIGINT      NOT NULL,
    close_paise             BIGINT      NOT NULL,
    volume                  BIGINT      NOT NULL,
    tick_count              INT         NOT NULL,
    last_event_time         BIGINT      NOT NULL,
    last_event_fingerprint  STRING,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (instrument_token, tf, window_start) NOT ENFORCED
) WITH (
    'bucket.num' = '16',
    'bucket.key' = 'instrument_token',
    'table.log.ttl' = '7d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true',
    'table.kv.format-version' = '2'
);
