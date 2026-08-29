-- feature_candles_15s_preview: KV current-state — live candle previews
--   (one row per in-progress 15s window per instrument; overwritten every 1s)
-- Owner: Signal job
-- Type: KV (primary key on instrument_token, window_start)
-- Bucket key: instrument_token (strict subset of the PK — per-ticker
--   colocation, and the Fluss connector requires bucket.key ⊆ primary key).
-- Retention: 60 seconds via table.log.ttl — previews are ephemeral by design
--   (the final candle lives in feature_candles_15s, 7d). Auto-expiry means no
--   cleanup job; a consumer only ever sees previews for windows still forming.
-- Lake: none — previews are transient; no Iceberg offload (the final
--   candle table owns the historical record).
-- Scope: account_scope_id
-- Schema version: 1 (2026-08-29 — low-latency candles Phase 1)
--
-- Writes: CandlePreviewEmitFunction (compute job) — same PK as the final
--   candle, so an upsert overwrites the same row each 1s tick; the row "grows"
--   live (high climbs, close tracks, volume accumulates). The final candle
--   emission at window end goes to feature_candles_15s (unchanged); this
--   preview row simply stops being updated and expires after TTL.

CREATE TABLE feature_candles_15s_preview (
    instrument_token        BIGINT      NOT NULL,
    exchange                STRING      NOT NULL,
    symbol                  STRING      NOT NULL,
    window_start            BIGINT      NOT NULL,
    window_end              BIGINT      NOT NULL,
    open_paise              BIGINT      NOT NULL,
    high_paise              BIGINT      NOT NULL,
    low_paise               BIGINT      NOT NULL,
    close_paise             BIGINT      NOT NULL,
    volume                  BIGINT      NOT NULL,
    tick_count              INT         NOT NULL,
    is_preview              BOOLEAN     NOT NULL,
    output_ts               BIGINT      NOT NULL,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (instrument_token, window_start) NOT ENFORCED
) WITH (
    'bucket.num' = '16',
    'bucket.key' = 'instrument_token',
    'table.log.ttl' = '60s',
    -- Previews are ephemeral (60s TTL) and MUST stay in local KV: with the
    -- tablet server default datalake.format=iceberg, an unset datalake flag
    -- makes this table lake-tiered, and tiered KV point lookups return empty
    -- (observed 2026-08-29: sink wrote 850K previews, KV probe FOUND=0).
    'table.datalake.enabled' = 'false',
    'table.kv.format-version' = '2'
);
