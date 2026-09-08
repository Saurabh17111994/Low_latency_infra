-- Trade_Decisions: Immutable instruction feed — immutable Signal-owned LOG
-- Owner: Signal job (sole writer). Executor never mutates this table.
-- Type: LOG (no primary key)
-- Bucket key: instruction_id
-- Retention: 7 calendar days via table.log.ttl (T8 G1/G4 7d hardening
-- 2026-08-22 — was 2d; now 7d + block-delete-unverified guard: Fluss delete
-- blocked until iceberg manifest VERIFIED, else EOD controller extends;
-- critical alert)
-- Lake: execution audit links retained under approved policy (one-year minimum target)
-- Scope: portfolio_id, account_scope_id
-- Schema version: 2
--
-- Domain contract (P4-023/P4-024/P4-025/P4-026/P4-027/P4-179/P4-180/P4-328 —
-- LOG takes no PK; Fluss has no CHECK/DEFAULT/GRANT; enforced in code):
--   sole writer (P4-023): Signal job sole writer via TradeDecisionsSinks
--     (pinned UIDs). Fluss has no GRANTs — column ownership matrix is the
--     authority, not a phantom REVOKE/GRANT block.
--   dedup (P4-024): LOG has no PK BY DESIGN (a PK would destroy the feed).
--     instruction_id = ins-v1- + SHA-256 of executable identity
--     (TradeDecisionBuilder); downstream authority is the KV index +
--     TradeInstructionFeedProtocol.verify (ACCEPTED/DUPLICATE/VIOLATION).
--   domain checks (P4-025): enforced in TradeDecisionBuilder.requireValid
--     (fail-closed throw): quantity/instrument/created positive, finite
--     composite_score, positive-when-present price, all non-blank. GAPS
--     (future builder hardening, not DDL): side/order_type not closed-enum
--     checked; LIMIT/MARKET-price coupling absent here (cf. intent builder).
--   superseded_by (P4-026): write-once-at-emit, resolved read-side; nothing
--     UPDATEs it through the append-only sink (serialization (true,true)).
--     DROP would be a recreate + columns/mapper cascade for zero runtime
--     effect — rejected.
--   retention split (P4-027): 7d hot changelog + block-delete-unverified
--     guard + EOD extend + alert (see above); 1-year history in the lake
--     (admin-owned lifecycle; no lake-retention property in Fluss DDL).
--   ts/units (P4-179/P4-180): created_ts/expiry_ts epoch-millis UTC;
--     builder pins created>0, expiry>0-when-present; NULL expiry = GTC/
--     session default; readers MUST drop expired rows.
--   version (P4-328): schema_version writer-pinned '2'; no DEFAULT/CHECK in
--     Fluss — drift breaks downstream branching, writer owns it.

CREATE TABLE Trade_Decisions (
    instruction_id          STRING      NOT NULL,
    candidate_id            STRING      NOT NULL,
    trade_context_id        STRING      NOT NULL,
    instrument_token        BIGINT      NOT NULL,
    exchange                STRING      NOT NULL,
    symbol                  STRING      NOT NULL,
    side                    STRING      NOT NULL,
    quantity                BIGINT      NOT NULL,
    order_type              STRING      NOT NULL,
    product_type            STRING      NOT NULL,
    limit_price_paise       BIGINT,
    portfolio_id            STRING      NOT NULL,
    account_scope_id        STRING      NOT NULL,
    strategy_id             STRING      NOT NULL,
    strategy_version        STRING      NOT NULL,
    configuration_version   STRING      NOT NULL,
    evaluation_id           STRING      NOT NULL,
    composite_score         DOUBLE,
    reservation_id          STRING      NOT NULL,
    reservation_version     STRING      NOT NULL,
    created_ts              BIGINT      NOT NULL,
    expiry_ts               BIGINT,
    supersedes_instruction_id STRING,
    superseded_by_instruction_id STRING,
    schema_version          STRING      NOT NULL
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'instruction_id',
    'table.log.ttl' = '7d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
