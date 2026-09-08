-- candle_live: KV current-state — live candle snapshots per timeframe (multi-TF aggregator Phase 0)
-- Owner: Signal job (compute service; writer MultiTfAggregatorFunction; contract
--   pinned by CandleLiveColumnsAgreementTest — P4-334)
-- Type: KV (primary key on instrument_token, tf, window_start)
-- Bucket key: instrument_token (strict subset of the PK — per-ticker
--   colocation, and the Fluss connector requires bucket.key ⊆ primary key).
-- Retention: 60 seconds via table.log.ttl — covers the CHANGELOG only, not the
--   KV snapshots (P4-006: Fluss 0.9.1 has no per-key TTL, so sealed rows do NOT
--   auto-expire — "no cleanup job" was false; a seal-time DELETE path is future
--   work. The writer holds windows in keyed state + processing-time timers, so
--   the 60s log bound does not lose mid-window history; the closed history
--   lives in candle_closed, 7d). TTL value intentionally unchanged.
-- Lake: none — live snapshots are transient; no Iceberg offload (mirrors
--   30_feature_candles_15s_preview rationale: transient KV with short TTL,
--   point-lookup only).
-- Scope: none (global single-tenant market data; deployment ACCOUNT_SCOPE_ID —
--   P4-091: header previously claimed account_scope_id with no column; no
--   per-row tenant column by design, same as raw_table_1)
-- Schema version: 1 (2026-09-05 — multi-TF aggregator Phase 0; tf discriminator)
--
-- Columns: 15 in DDL order — tf discriminator values: FIFTEEN_S, THIRTY_S,
--   ONE_M, THREE_M, FIVE_M, FIFTEEN_M (STRING NOT NULL, part of PK).
-- tf authority (P4-239 — Fluss has no CHECK; enforced in job code): Timeframe
--   enum is the single writer-side source (code()==name() contract), rows are
--   built with tf.code(), and readers use Timeframe.valueOf(code) which throws
--   on any typo/case variant — fail-closed on read.
-- Time base + OHLC (P4-240): window_start/window_end epoch-millis UTC,
--   half-open [start, start+tf.windowMs()); HIGH/LOW accumulate trade-gated
--   (TRADE && qty>0) in MultiTfAggregatorFunction — quotes never mutate OHLC.
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
