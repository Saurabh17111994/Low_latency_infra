-- trade_instruction_state: authoritative instruction-hash index — one row per
--   published immutable instruction (SCH-19, REQ-FLS-008/015)
-- Owner: Signal job (sole writer)
-- Type: KV state table (PK instruction_id) — the LOG twin (Trade_Decisions) is the
--   immutable instruction feed; this index holds the canonical content hash the
--   instruction-feed protocol checks before every append (same id + same hash =
--   duplicate evidence; same id + different hash = contract violation)
-- Bucket key: instruction_id (routing identity — every instruction is routable;
--   single-field PK, so raw-client writable per the COMPAT-FLUSS-005 matrix)
-- Retention: table.log.ttl = 2d bounds the changelog; the index is rebuildable from
--   the Trade_Decisions LOG replay (REQ-FLS-015 pinned Fluss source replay)
-- Lake: none — transient enforcement index, rebuildable; no EOD/audit value
--   (the LOG twin is the audit record)
-- Scope: account_scope_id
-- Schema version: 1
--
-- Domain contract (P4-075/P4-076/P4-228/P4-229/P4-230 — single-field PK is
-- deliberate per COMPAT-FLUSS-005 raw-client writability; no column changes):
--   scope (P4-075, REJECTED composite PK): instruction_id is content-derived
--     per emission (ins-v1- + SHA-256 of executable identity), NOT a
--     per-account sequence. Two accounts emitting byte-identical executable
--     fields SHOULD share an id — and canonical_hash (which INCLUDES
--     accountScopeId) then correctly raises VIOLATION instead of silently
--     sharing. The design working, not a bug. Composite PK breaks the
--     columns-class pin, index-mapper positional mapping, and raw-client
--     writability — rejected (same ruling as P4-085).
--   first-write-wins (P4-076, FIXED 2026-09-09): keyed-state first-write-wins
--     filter before the index sink (TradeDecisionsSinks
--     InstructionStateFirstWriteWinsFunction, keyed by instruction_id,
--     ValueState marker with 24 h native StateTtlConfig — mirror of the
--     candle_closed filter in MultiTimeframeSinks; drops + counts replay
--     arrivals via compute.trade_decisions.duplicate_instruction). The sink
--     path (fan-out -> FWW filter -> TradeDecisionIndexMapper -> KV upsert)
--     now has the read-before-write the protocol assumes: only the first
--     emission of an id reaches the upsert, so a divergent-hash replay can
--     never overwrite hash + first_written_ts. Marker TTL 24 h bounds state
--     to one trading day; replays older than that are outside the premise
--     (single-writer, same-day rollback) and the LOG twin preserves forensics.
--   hash spec (P4-228): SHA-256 lowercase hex (64 chars) of
--     TradeDecisionBuilder canonicalContent (CONTENT_SERIALIZATION_VERSION
--     tdc-v1) via ImmutabilityProtocol.canonicalHash; writer rejects
--     blank/malformed (requireValid); any encoding change needs a
--     schema_version bump.
--   first_written_ts (P4-229): source createdTs of the ACCEPTED first write
--     (mapper line 36: decision.createdTs — event time, NOT arrival time),
--     immutable thereafter (enforced by the P4-076 FWW filter — see above).
--   KV lifetime (P4-230): 2d TTL bounds the changelog only; the KV snapshot
--     grows one row per instruction with no expiry column. Rebuild beyond
--     the 7d LOG window is unprovable. Expiry column + cleaner, or lifetime
--     alignment, remains open — stated, not solved here.

CREATE TABLE trade_instruction_state (
    instruction_id     STRING      NOT NULL,
    canonical_hash     STRING      NOT NULL,
    first_written_ts   BIGINT      NOT NULL,
    schema_version     STRING      NOT NULL,
    PRIMARY KEY (instruction_id) NOT ENFORCED
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'instruction_id',
    'table.log.ttl' = '2d'
);
