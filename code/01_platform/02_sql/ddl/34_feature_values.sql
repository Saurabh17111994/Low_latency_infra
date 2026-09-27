-- feature_values: stored feature layer (DEC-056/DEC-057) — one row per
-- (instrument_token, tf, window_start) carrying the feature values the strategy
-- host held at that window's close.
-- Owner: Signal job
-- Writer: the feature writer side output -> FlussSink; no other writer.
-- Type: KV (primary key on instrument_token, tf, window_start)
-- Bucket key: instrument_token (strict subset of the PK — per-ticker colocation,
--   same ruling as candle_closed; the connector requires bucket.key ⊆ PK).
-- Encoding (DEC-057): features = MAP<INT, DOUBLE> keyed by the append-only
--   registry feature id (FeatureRegistry, DEC-056/057; ids are never renumbered
--   and never reused). A missing key means "not declared for this tf" or "not
--   ready at close". An empty snapshot emits NO row (the table never carries a
--   placeholder).
-- Writes: the strategy host computes features once per instrument (shared by
--   every strategy); on each closed candle the writer emits one row per
--   non-empty snapshot. Re-emitted windows after a restore upsert the same PK
--   with the same deterministic values (convergent — no first-write-wins).
-- Retention: 7d log TTL (proposal). The layer is recomputable from raw_table_1
--   + the registry, but a replay under a NEW registry version can differ, so the
--   TTL should cover the analysis window; lake offload is deliberately OFF in
--   this proposal and is an open item (operator decides with consumers).
-- Schema version: 1 (2026-09-27 — DEC-056/DEC-057).
--
-- STATUS: PROPOSAL — not applied anywhere. Production apply stays blocked on
-- the DDL hazard (pinned Fluss/Flink compatibility + schema lifecycle tests);
-- a dev smoke table must be created through the operator-approved route.

CREATE TABLE feature_values (
    instrument_token  BIGINT  NOT NULL,
    tf                STRING  NOT NULL,
    window_start      BIGINT  NOT NULL,
    features          MAP<INT, DOUBLE>,
    PRIMARY KEY (instrument_token, tf, window_start) NOT ENFORCED
) WITH (
    'bucket.num' = '16',
    'bucket.key' = 'instrument_token',
    'table.log.ttl' = '7d',
    'table.kv.format-version' = '2',
    'table.datalake.enabled' = 'false'
);
