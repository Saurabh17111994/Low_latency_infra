-- Fills: Immutable LOG — postback/fill audit from Action Capture
-- Owner: Action Capture
-- Type: LOG (no primary key)
-- Bucket key: postback_event_id
-- Retention: 7 calendar days via table.log.ttl — R-144: this covers the
-- Order_Lifecycle rebuild window (7d) it is the source for; R-184: 7 calendar
-- days always contains ≥3 trading days, satisfying the compliance floor even
-- across a weekend + holiday.
-- Lake: encrypted immutable audit under approved policy (one-year minimum target)
-- Scope: account_scope_id (R-085: column materialized — the header declared
-- account scoping but the schema had no such column)
-- Schema version: 2
--
-- Domain contract (P4-028/P4-029/P4-030/P4-031/P4-181/P4-182/P4-183/P4-184 —
-- LOG takes no PK; Fluss has no CHECK/GRANT/PARTITION; enforced in code):
--   dedup (P4-028): LOG has no PK BY DESIGN. Identity key is
--     (postback_fingerprint, fingerprint_version) — PostbackFingerprint
--     SHA-256 hex over canonical fields; rebuilds dedup by postback_event_id
--     keeping max ingest_ts. Downstream lifecycle is KV so last-write wins,
--     but replays that SUM fill_qty MUST dedup first or double-count.
--   fill invariants (P4-029): enforced in NormalizedPostback compact ctor
--     (fail-closed throw): quantities >= 0, non-fill must not carry a price,
--     fill-present-requires-price (fill_qty>0 ⇒ price>0; FIXED in FOLLOW-3),
--     side BUY/SELL-closed. Fill id at this layer is sourceEventId (always
--     required). Heartbeat-vs-partial (FOLLOW-3): fill_qty=0 is a
--     status/heartbeat update (no price); fill_qty>0 is a fill event
--     (partial while pending_qty>0, complete at pending_qty=0).
--   payload (P4-030): original_payload retained for forensics; hot-path
--     joins use payload_hash. Fluss has no GRANT/RLS/masking — readers
--     predicate on account_scope_id; lake encryption admin-owned.
--   rebuild source (P4-031): authoritative rebuild is the Iceberg lake
--     (>=1y), NOT the 7d hot log (lag buffer only). No TTL bump — moving
--     7d->10d moves the cliff without removing it.
--   trading-day claim (P4-181): 7d covers >=3 trading days as a TARGET
--     under the exchange holiday calendar, not a guarantee (long festival
--     shutdowns can break it); compliance floor is the lake, not calendar
--     TTL. (Note: the audit table's 5d/3-day claim stands — 5 consecutive
--     days always contain >=3 weekdays.)
--   ts/units (P4-182): all timestamps epoch-millis UTC. Rebuild order:
--     broker_event_time, then receive_time, then ingest_ts, tiebreak by
--     postback_event_id. No TIMESTAMP_LTZ migration (connector churn, zero
--     semantic gain).
--   bucket key (P4-183): postback_event_id spread is ingest-parallelism
--     optimal; rebuild joins on broker_order_id fan out — accepted tradeoff,
--     stated. Re-bucketing reshuffles a live table; perf evaluation is
--     follow-up, not this batch.
--   lake config (P4-184): 5min freshness + auto-compaction only; snapshot
--     retention/encryption/WORM are lake-admin-owned (no such properties in
--     Fluss DDL). Compaction is file-level and preserves every logical row.

CREATE TABLE Fills (
    postback_event_id       STRING      NOT NULL,
    postback_fingerprint    STRING      NOT NULL,
    fingerprint_version     STRING      NOT NULL,
    account_scope_id        STRING      NOT NULL,
    broker_order_id         STRING,
    instruction_id          STRING,
    execution_attempt_id    STRING,
    trade_context_id        STRING,
    order_status            STRING      NOT NULL,
    cumulative_qty          BIGINT      NOT NULL,
    pending_qty             BIGINT      NOT NULL,
    fill_qty                BIGINT,
    fill_price_paise        BIGINT,
    fill_id                 STRING,
    broker_event_time       BIGINT,
    receive_time            BIGINT      NOT NULL,
    ingest_ts               BIGINT      NOT NULL,
    original_payload        BYTES       NOT NULL,
    payload_hash            STRING      NOT NULL,
    correlation_state       STRING      NOT NULL,
    correlation_reason      STRING,
    decoder_version         STRING      NOT NULL,
    schema_version          STRING      NOT NULL
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'postback_event_id',
    'table.log.ttl' = '7d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
