-- candle_live: KV current-state — live candle snapshots per timeframe (multi-TF aggregator Phase 0)
-- Owner: Signal job
-- Type: KV (primary key on instrument_token, tf, window_start)
-- Bucket key: instrument_token (strict subset of the PK — per-ticker
--   colocation, and the Fluss connector requires bucket.key ⊆ primary key).
-- Retention: 60 seconds via table.log.ttl — live snapshots are ephemeral by design
--   (the closed history lives in candle_closed, 7d). Auto-expiry means no
--   cleanup job; a consumer only ever sees live rows for windows still forming.
-- Lake: none — live snapshots are transient; no Iceberg offload (mirrors
--   30_feature_candles_15s_preview rationale: transient KV with short TTL,
--   point-lookup only).
-- Scope: account_scope_id
-- Schema version: 1 (2026-09-05 — multi-TF aggregator Phase 0; tf discriminator)
--
-- Columns: 15 in DDL order — tf discriminator values: FIFTEEN_S, THIRTY_S,
--   ONE_M, THREE_M, FIVE_M, FIFTEEN_M (STRING NOT NULL, part of PK).
-- Writes: MultiTfAggregatorFunction (compute job) — 1s processing-time timer
--   upserts the same PK each second; the row "grows" live (high climbs,
--   close tracks, volume accumulates) until the boundary timer seals it to
--   candle_closed.

CREATE TABLE candle_live (
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
    'table.log.ttl' = '60s',
    'table.datalake.enabled' = 'false',
    'table.kv.format-version' = '2'
);
