-- Execution_Intent_Processed: gateway-owned durable source-event reprocessing
--   index — one row per Execution_Intent (instruction_id) the gateway has
--   durably handed off. This replaces the process-local duplicate guard (which
--   started empty on every restart) with a durable attempt/index lookup, so a
--   replayed intent is idempotent across a gateway restart: same instruction_id
--   + same request_hash = already handed off (skip, no duplicate side effect);
--   same instruction_id + different request_hash = contract violation
--   (quarantine + halt). This is the IntentReader dedup authority referenced by
--   docs/08_implementation/19-nautilus-execution-service-implementation-plan.md
--   T2 ("durable attempt/index lookup must replace the current process-local
--   duplicate guard").
-- Owner: Action Capture
-- Type: KV (primary key on instruction_id)
-- Bucket key: instruction_id (single-field PK -> raw-client writable per the
--   COMPAT-FLUSS-005 matrix; same single-field-PK shape as Execution_Attempts /
--   trade_instruction_state)
-- Hash-violation safety (P4-004 — no blind-upsert clobber BY CONSTRUCTION):
--   DurableIntentDispatcher.classify runs BEFORE any write; HASH_VIOLATION ->
--   violationHandler + skip (IntentReader.poll), committed() fires only after
--   FORWARDED, and IntentDeduplicator.commit is putIfAbsent — the first hash is
--   never overwritten by this path. first/last-seen-hash columns would need a
--   recreate for zero new safety; the ordering guarantee is pinned by
--   IntentDeduplicatorTest + DurableIntentDispatcherTest.
-- Retention: table.log.ttl = 7d bounds the changelog (T8 G1/G4 7d hardening
--   2026-08-22 — was 2d; now 7d + block-delete-unverified guard: Fluss delete
--   blocked until iceberg manifest VERIFIED, else EOD controller extends;
--   critical alert); the index is rebuildable from the Execution_Intent LOG
--   replay (same replay model as the source)
-- Lake: none — transient reprocessing index, rebuildable from the source LOG;
--   the LOG twin (Execution_Intent) is the audit record
-- Scope: account_scope_id, execution_partition_id
-- Schema version: 1
--
-- Follow-up contract (P4-085/P4-086/P4-087/P4-237 — no column/value changes):
--   scope columns (P4-085, REJECTED): single-field PK is deliberate
--     (COMPAT-FLUSS-005 raw-client writability, same shape as attempts/
--     instruction-state). instruction_id is globally unique BY CONSTRUCTION
--     (nothing mints per-account sequences); adding NOT NULL scope columns
--     breaks positional mapping (FlussIntentDedupStore cols 0/1 + hydrate),
--     the columns-class pin, and raw-client writability — a recreate for an
--     unwitnessed collision class. Reader cross-checks hash via classify.
--   single writer (P4-086): IntentReader owns the single-writer loop;
--     classify-before-write + putIfAbsent is the fencing — no concurrent
--     handoff for one instruction_id is possible without violating the
--     single-writer premise. No conditional-write surgery.
--   TTL horizon (P4-087): index and source share the 7d +
--     block-delete-unverified model; a one-sided 30d bump fixes nothing.
--     Rebuild horizon is the source guard, stated explicitly.
--   source_log_offset (P4-237): informational progress-tracking only, NOT
--     dedup authority (hash compare is); nullable + unqualified BY DESIGN.

CREATE TABLE Execution_Intent_Processed (
    instruction_id     STRING      NOT NULL,
    request_hash       STRING      NOT NULL,
    handed_off_ts      BIGINT      NOT NULL,
    source_log_offset  BIGINT,
    schema_version     STRING      NOT NULL,
    PRIMARY KEY (instruction_id) NOT ENFORCED
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'instruction_id',
    'table.log.ttl' = '7d'
);
