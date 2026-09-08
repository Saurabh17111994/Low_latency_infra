-- candle_closed: KV immutable closed history per timeframe (multi-TF aggregator Phase 0)
-- Owner: Signal job
-- Type: KV (primary key on instrument_token, tf, window_start)
-- Bucket key: instrument_token (strict subset of the PK — per-ticker
--   colocation, and the Fluss connector requires bucket.key ⊆ primary key).
-- Retention: 7 calendar days via table.log.ttl (same as feature_candles_15s,
--   R-055 / T8 G1/G4 7d hardening).
-- Lake: EOD Iceberg offload with 5min freshness + auto-compaction (same as
--   03_feature_candles_15s — history kept, offloaded).
-- Scope: none (global single-tenant market data — P4-092: no
--   account_scope_id column; same ruling as the 32_candle_live twin. Isolation
--   is by deployment, not by row filter.)
-- Schema version: 1 (2026-09-05 — multi-TF aggregator Phase 0; tf discriminator)
--
-- Columns: same 15 as candle_live (DDL order) — tf discriminator values:
--   FIFTEEN_S, THIRTY_S, ONE_M, THREE_M, FIVE_M, FIFTEEN_M (P4-241: pinned by
--   the Timeframe enum — typoed tf values fail in producer code, not in
--   storage; Fluss has no CHECK).
-- Writes: MultiTfAggregatorFunction boundary timers — one row per non-empty
--   bucket per timeframe per instrument, first-write-wins (immutable —
--   P4-093: enforced by MultiTimeframeClosedFirstWriteWinsFunction keyed on
--   the composite PK, NOT by DDL — KV upsert alone is last-writer-wins; a
--   re-emitted closed candle after restore never overwrites the sealed row).
-- Integrity (P4-242): OHLCV math lives in CandleAggregateFunction (open/high/
--   low/close from ordered prices, TRADE-only volume/tickCount) — corrupt
--   upstream emissions are bounded there, not by DDL CHECKs. Units: all BIGINT
--   ts fields epoch-millis UTC (window_start/window_end/last_event_time);
--   window_end = window_start + tf-window; high >= max(open,close),
--   low <= min(open,close), volume/tick_count >= 0 by construction.
-- Retention (P4-094): 7d log TTL bounds the changelog; KV snapshot holds
--   sealed history until overwritten (immutable => grows: instruments x 6 TFs).
--   5min freshness is continuous tiering, EOD offload is the VERIFIED gate —
--   no contradiction, two different consumers. No row expires before offload:
--   freshness << TTL is monitored (EOD extend path on lag).
-- Bucketing (P4-243): bucket.key=instrument_token colocates all 6 TFs x all
--   windows per ticker (single-bucket ticker reads); boundary-alignment seal
--   storms hotspot one bucket by design at this universe size. Revisit
--   bucket.num only on measured skew — no guessed rebucket, no Iceberg
--   partitioning (time-range scans ride the tiered lake layout).

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
