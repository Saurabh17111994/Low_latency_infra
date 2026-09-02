-- Signal_Tentative_Markers: durable crash-reconciliation for early-signal
-- tentatives (CHG-121, 2026-09-01).
-- Owner: Signal job (sole writer)
-- Type: KV (primary key on candidate_id)
--
-- WHY THIS TABLE EXISTS: a TENTATIVE is a decision made on PARTIAL preview
-- state and written to the immutable Signal_Candidates LOG before the
-- tracking Flink state is checkpointed. A TM crash between the LOG write and
-- the next checkpoint rolls the tracking state back while the LOG row
-- survives; on replay the watermark advances in bursts, replayed previews
-- carry FULL-window accumulators, and the partial-data decision is not
-- re-derivable — the tentative can never be re-emitted and settles nothing
-- (F4 orphans, drill tm-kill-full-load-20260901-234448: 84/84 orphans had
-- exactly one pre-kill tentative row and full-window replays).
--
-- CONTRACT: EarlySignalFunction upserts a marker row AT TENTATIVE-EMIT TIME
-- (before the LOG row leaves the operator), and on a final candle with no
-- in-state pending (the post-crash path) looks the marker up by the
-- deterministic candidate_id. Marker present → the settle decision is
-- CONFIRM/CANCEL per the FINAL candle rule and the marker is deleted.
-- Marker absent → no tentative ever existed for that window — silent as
-- today. Markers live only for the settlement horizon (window + lateness),
-- so the table stays small; table.log.ttl bounds any residue.
--
-- Bucket key: candidate_id (PK subset; single-field bucket key + composite
-- PK + kv.format-version=2 is the COMPAT-FLUSS-005 raw-client combo, same
-- as fingerprint_dedup / instruments).
-- Retention: 2d (settlement horizon is minutes; TTL is the backstop)
-- Lake: none — transient reconciliation state, no audit value
-- Scope: account_scope_id
-- Schema version: 1

CREATE TABLE Signal_Tentative_Markers (
    candidate_id            STRING      NOT NULL,
    instrument_token        BIGINT      NOT NULL,
    window_end              BIGINT      NOT NULL,
    marked_ts               BIGINT      NOT NULL,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (candidate_id) NOT ENFORCED
) WITH (
    'bucket.num' = '4',
    'bucket.key' = 'candidate_id',
    'table.kv.format-version' = '2',
    'table.log.ttl' = '2d'
);
