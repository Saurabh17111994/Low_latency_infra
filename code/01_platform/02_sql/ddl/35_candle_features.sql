-- candle_features: the merged candle+feature KV table (Wave B, DEC-059)
-- Owner: Signal job strategy host (ONE writer — DEC-059)
-- Type: KV (primary key on instrument_token, tf, window_start)
-- Bucket key: instrument_token (strict subset of the PK — per-ticker
--   colocation; the Fluss connector requires bucket.key ⊆ primary key).
-- Retention: 3 calendar days via table.log.ttl (DEC-059: the merged era's local
--   window; 1 d extension runway via the EOD guard, CHG-463).
-- Lake: opt-in per DEC-060 — options present but table.datalake.enabled=false;
--   the configured archive list enables it via r2-archive-sync. The merged
--   table is ONE selectable archive entry; it never forces a merged lake table.
-- Scope: none (global single-tenant market data — no account_scope_id column).
-- Schema version: 1 (2026-09-30 — Wave B merged table; supersedes candle_live +
--   candle_closed + feature_values at cutover).
--
-- Columns: candle_closed's 15 columns (same order/types) + features MAP<INT,
--   DOUBLE> (DEC-057: append-only registry ids, never renumbered or reused) +
--   sealed BOOLEAN.
-- Writes (DEC-059; closed-only storage 2026-10-01): the strategy host writes
--   exactly one sealed row per closed window with the close-cadence features.
--   Nothing is written on the live cadence — forming candles and their features
--   live in Flink memory (the strategy host's in-memory view). Sealed rows are
--   NEVER rewritten (late ticks are dropped — enforced in the writer, not DDL).
-- Readers: every stored row is a finished window (sealed=true). The live
--   now-view is the strategy host's in-memory view, never this table.
-- Feature add/remove: one registry line + one pin line (RETIRED to remove) —
--   no DDL change (DEC-057/DEC-059).
-- Bucketing (P4-243 twin): bucket.key=instrument_token colocates all TFs x all
--   windows per ticker; revisit only on measured skew.
-- Cutover: staged dual-write; candle_live/candle_closed/feature_values stay the
--   authoritative path until the operator opens the cutover (every runtime path
--   ships behind MERGED_CANDLE_FEATURES_ENABLED=false).

CREATE TABLE candle_features (
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
    features                MAP<INT, DOUBLE>,
    sealed                  BOOLEAN     NOT NULL,
    PRIMARY KEY (instrument_token, tf, window_start) NOT ENFORCED
) WITH (
    'bucket.num' = '16',
    'bucket.key' = 'instrument_token',
    'table.log.ttl' = '3d',
    'table.datalake.enabled' = 'false', -- DEC-060: opt-in via r2-archive-sync
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true',
    'table.kv.format-version' = '2'
);
