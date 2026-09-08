-- Positions: KV projection — current position aggregate keyed by position_id
-- Owner: Position projector (Action Capture, in-process)
-- Type: KV (primary key on position_id)
-- Retention: current state + rebuild window (90 calendar days via table.log.ttl
--   — R-280: 7 days expired an OPEN position that simply had no fill/postback
--   for a week, deleting it from the current-state view; P4-002: 90d only
--   extends the horizon — the long-term fix is changelog-TTL-only + explicit
--   CLOSED-state GC in the projector/periodic job, so idle OPEN rows are never
--   TTL-deleted with no tombstone)
-- Scope: account_scope_id
-- Schema version: 2
--
-- Domain contract (P4-035/P4-036/P4-187/P4-188 — Fluss has no CHECK/CONSTRAINT/
-- DEFAULT/enum DDL, so these live in code, not here):
--   side/state: free-form STRING in DDL; allowed side LONG/SHORT/BUY/SELL and
--     state OPEN/CLOSED/PENDING_CLOSE are enforced in the projector
--     (PositionProjector/PositionLifecycle); quantities open/closed >= 0 and
--     closed <= open enforced there too — derived current = open - closed.
--   PK position_id NOT ENFORCED: idempotent upsert keyed by position_id with
--     last-writer-wins on source_version + dedup on source_event_id, enforced
--     in PositionProjector.apply via KvStateUpdateProtocol.evaluate (STALE/
--     REGRESSION rejected) — see P4-036.
--   average_entry/exit_paise BIGINT paise, nullable: NULL iff the matching
--     quantity is 0 (entry NULL iff open=0, exit NULL iff closed=0); fractional
--     paise TRUNCATES on every fill — accepted for now, revisit DECIMAL(18,4)
--     only if compounding PnL error proves material (P4-187).
--   created_ts/last_update_ts: epoch-millis, last_update_ts >= created_ts;
--     schema_version writer-set '2' (DDL Schema version in this header).
--     Prefer TIMESTAMP_LTZ(3)/INT DEFAULT only with a full recreate (P4-188).
--   bucket key (P4-189): position_id spread is point-lookup optimal (the hot
--     path); per-account views scatter-gather across 8 buckets — accepted
--     cost under single-account deployment. account_scope_id is materialized
--     NOT NULL and readers MUST predicate on it (a missing filter leaks
--     across accounts — app-side discipline, no RLS in Fluss). Secondary
--     indexes do not exist in Fluss; re-bucketing reshuffles a live table.
--
-- v2 (2026-08-03, review R-232): removed the derived `current_quantity`
-- column. It equals open_quantity - closed_quantity; persisting it as a
-- separate NOT NULL column lets any write path that updates only one of the
-- three quantity fields silently corrupt position state. Consumers derive it.

CREATE TABLE Positions (
    position_id             STRING      NOT NULL,
    trade_context_id        STRING      NOT NULL,
    account_scope_id        STRING      NOT NULL,
    instrument_token        BIGINT      NOT NULL,
    exchange                STRING      NOT NULL,
    symbol                  STRING      NOT NULL,
    side                    STRING      NOT NULL,
    state                   STRING      NOT NULL,
    open_quantity           BIGINT      NOT NULL,
    closed_quantity         BIGINT      NOT NULL,
    average_entry_paise     BIGINT,
    average_exit_paise      BIGINT,
    source_event_id         STRING      NOT NULL,
    source_version          BIGINT      NOT NULL,
    created_ts              BIGINT      NOT NULL,
    last_update_ts          BIGINT      NOT NULL,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (position_id) NOT ENFORCED
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'position_id',
    'table.log.ttl' = '90d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
