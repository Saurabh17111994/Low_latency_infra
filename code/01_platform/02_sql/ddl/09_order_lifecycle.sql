-- Order_Lifecycle: KV projection — current order state keyed by account+broker order
-- Owner: Action Capture
-- Type: KV (primary key on account_scope_id, broker_order_id)
-- Bucket key: account_scope_id (raw-client compatible subset of the composite PK)
-- Retention: current state + short rebuild buffer (2 calendar days);
--   rebuildable from Fills audit (Fills retains 7d; see 08_fills.sql)
-- Scope: account_scope_id (R-013: column materialized + composite PK)
-- Schema version: 2 (routing/encoding compatibility amendment; columns unchanged)
--
-- v2 (2026-08-03, review R-013): the header declared "Scope: account_scope_id"
-- but the table had no such column and was keyed only on broker_order_id.
-- Broker-assigned order IDs are typically unique only within one brokerage
-- account; two accounts can produce the same broker_order_id and the KV
-- projection would silently overwrite one account's order state. The composite
-- key (account_scope_id, broker_order_id) makes the projection account-safe.
--
-- Domain contract (P4-032/P4-033/P4-034/P4-185/P4-186 — NOT ENFORCED is
-- engine reality; Fluss has no CHECK; enforced in running code, not here):
--   ordering (P4-032): enforced in OrderLifecycleProjector (wired by
--     PostbackProjectionDriver): stale version rejected (audited, no write),
--     same-version-different-content -> CONFLICT quarantine, terminal
--     regression -> quarantine + halt, exact duplicate -> no-op. Writer rule:
--     apply only if (source_version, source_event_time, source_event_id) >
--     stored tuple; never regress cumulative_qty / normalized_state.
--   key discipline (P4-033): sole upsert/read key is the FULL composite
--     (account_scope_id, broker_order_id) — NEVER broker_order_id alone.
--     Offline audit: GROUP BY broker_order_id HAVING COUNT(DISTINCT
--     account_scope_id) > 1 must return zero rows.
--   TTL limits (P4-034, stated honestly): 2d bounds the changelog only;
--     quiescent multi-day open orders (GTT, no events >2d) lose changelog
--     cover, and Fills-rebuild restores FILL states only — non-fill states
--     (OPEN/REJECTED/CANCELLED/EXPIRED with no fills) and average-price
--     semantics are unrecoverable past the window. Retain-until-terminal is
--     a retention-policy + projector change, not a DDL value edit — no TTL
--     bump (moves the cliff without removing it).
--   enums/units (P4-185): normalized_state/correlation_state closed sets
--     live in OrderLifecycleState + projector; quantities >= 0 enforced at
--     the NormalizedPostback boundary; average_fill_price NULL iff
--     cumulative_qty = 0.
--   bucket key (P4-186): account-only bucketing is single-account-deployment
--     optimal today; per-account hotspot is theoretical until multi-account
--     scale. Re-bucketing reshuffles live distribution — load-test before
--     any change; perf validation is follow-up, not this batch.

CREATE TABLE Order_Lifecycle (
    account_scope_id        STRING      NOT NULL,
    broker_order_id         STRING      NOT NULL,
    instruction_id          STRING,
    execution_attempt_id    STRING,
    trade_context_id        STRING,
    normalized_state        STRING      NOT NULL,
    cumulative_qty          BIGINT      NOT NULL,
    pending_qty             BIGINT      NOT NULL,
    average_fill_price_paise BIGINT,
    source_event_id         STRING      NOT NULL,
    source_version          BIGINT      NOT NULL,
    source_event_time       BIGINT,
    last_receive_time       BIGINT      NOT NULL,
    correlation_state       STRING      NOT NULL,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (account_scope_id, broker_order_id) NOT ENFORCED
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'account_scope_id',
    'table.kv.format-version' = '2',
    'table.log.ttl' = '2d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
