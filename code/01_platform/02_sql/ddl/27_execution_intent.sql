-- Execution_Intent: immutable execution request produced from an approved signal
-- Owner: Signal job (sole writer); Nautilus/Executor never mutates this table
-- Type: LOG (append-only, no primary key)
-- Bucket key: instruction_id
-- Retention: table.log.ttl = 7d for operational replay (T8 G1/G4 7d hardening
--   2026-08-22 — was 2d; now 7d + block-delete-unverified guard: Fluss delete
--   blocked until iceberg manifest VERIFIED, else EOD controller extends;
--   critical alert); durable audit evidence is offloaded and retained under
--   the approved policy-controlled minimum.
-- Lake: enabled for durable replay/offload; this table is intent evidence, not
--   an order-lifecycle or position state machine.
-- Scope: account_scope_id, execution_partition_id
-- Schema version: 1
--
-- Domain contract (P4-081/P4-082/P4-083/P4-084/P4-235/P4-236 — LOG takes no
-- PK; Fluss has no CHECK/DEFAULT/GRANT; enforced in running code, not here):
--   idempotency (P4-081): instruction_id globally unique per intent; LOG has
--     no PK BY DESIGN (a PK would flip to KV and destroy the audit trail).
--     Readers MUST deduplicate on (instruction_id, request_hash): same id +
--     same hash = replay (skip), same id + different hash = contract
--     violation (quarantine + halt). Enforcers: IntentReader (documented
--     single-writer) + DurableIntentDispatcher.classify + putIfAbsent commit
--     (P4-004, pinned by IntentDeduplicatorTest).
--   order fields (P4-082): enforced in ExecutionIntentBuilder.validate
--     (fail-closed throw): quantity > 0; LIMIT <=> limit_price_paise present
--     and > 0; MARKET must not carry a price; side/order_type/product_type/
--     time_in_force non-blank. Free-form STRING in DDL is storage, not truth.
--   expiry (P4-083, enforced): created_ts/expiry_ts epoch-millis UTC;
--     builder rejects expiry_ts <= created_ts; readers MUST drop expired rows.
--     Supersede chain (UNWITNESSED): supersedes_instruction_id dangling/self/
--     cyclic guards exist nowhere — future writer work, not DDL.
--   TTL guard (P4-084): 7d TTL must not expire any offset whose Iceberg
--     manifest is not VERIFIED — block-delete-unverified guard + EOD extend
--     + critical alert (see Retention above). Offload lag beyond 7d pages
--     before data ages out; TTL value intentionally unchanged.
--   sole writer (P4-235): Signal job sole writer (IntentReader single-writer
--     loop downstream; Nautilus/Executor read-only). Fluss has no GRANTs —
--     column ownership matrix is the authority, not a phantom ACL link.
--   request_hash (P4-236): SHA-256 lowercase hex over the builder's '|'
--     -joined length-prefixed canonical field list (ExecutionIntentBuilder
--     join/sha256); writers MUST NOT truncate; readers compare exact string
--     equality.

CREATE TABLE Execution_Intent (
    instruction_id             STRING      NOT NULL,
    candidate_id               STRING      NOT NULL,
    trade_context_id           STRING      NOT NULL,
    account_scope_id           STRING      NOT NULL,
    execution_partition_id     STRING      NOT NULL,
    instrument_token           BIGINT      NOT NULL,
    exchange                   STRING      NOT NULL,
    symbol                     STRING      NOT NULL,
    side                       STRING      NOT NULL,
    quantity                   BIGINT      NOT NULL,
    order_type                 STRING      NOT NULL,
    limit_price_paise          BIGINT,
    product_type               STRING      NOT NULL,
    time_in_force              STRING      NOT NULL,
    strategy_id                STRING      NOT NULL,
    strategy_version           STRING      NOT NULL,
    configuration_version      STRING      NOT NULL,
    created_ts                 BIGINT      NOT NULL,
    expiry_ts                  BIGINT,
    request_hash               STRING      NOT NULL,
    supersedes_instruction_id  STRING,
    schema_version             STRING      NOT NULL
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'instruction_id',
    'table.log.ttl' = '7d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
