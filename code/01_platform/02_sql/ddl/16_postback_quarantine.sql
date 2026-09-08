-- Postback_Quarantine: Immutable LOG — unknown/malformed/ambiguous postback events
-- Owner: Action Capture
-- Type: LOG (no primary key)
-- Bucket key: quarantine_id
-- Retention: until disposition + buffer (2 calendar days via table.log.ttl —
--   R-088: table.retention.days is not a Fluss option; table.log.ttl is)
-- Lake: encrypted evidence per policy (R-146: datalake options restored — they
--   were dropped in a rewrite while the header still claimed lake storage)
-- Scope: deployment-scoped (P4-055: single ACCOUNT_SCOPE_ID per Action
--   Capture instance — no per-row account_scope_id column. The Action Capture
--   quarantine writer is pure logic with no live Fluss append path (offline
--   stub only); multi-tenant isolation is by deployment, not by row filter.
--   Adding a column + backfill needs a recreate + a live writer — deferred.)
-- Schema version: 2
--
-- Lifecycle (P4-056): disposition columns are write-once at insert on this
-- immutable LOG — there is NO disposition update path. A disposition change
-- is a second row with the same quarantine_id; readers resolve current
-- disposition as latest-quarantined_ts per quarantine_id. No KV conversion
-- (would change table kind + break the offline/append contract).
-- Retention (P4-057): Fluss 2d log is a buffer only; authoritative evidence
-- is the Iceberg offload (datalake enabled) retained until disposition +
-- buffer. Never query Fluss for investigations older than TTL; offload must
-- be VERIFIED before the log window expires (EOD guard, same as siblings).
-- Vocabulary (P4-203): reason/disposition are writer-contract enums (DDL has
-- no CHECK) — reason per R-219 (header list above), disposition per header
-- list above. Typos fail in writer validation, not in storage.
-- Integrity fields (P4-204): payload_hash is SHA-256 hex of original_payload;
-- no separate algo column (frozen layout — rotation = new fingerprint_version
-- convention, not a column). DUP_FINGERPRINT joins via payload_hash to the
-- dedup/fills record (opaque by design — the fingerprint IS the hash).
-- correlation_attempt is a free-form attempt marker (count-or-blob,
-- writer-defined); disposition_ts NULL-means-OPEN invariant: NULL when
-- disposition=OPEN, NOT NULL once RESOLVED/DISMISSED/ESCALATED/INVESTIGATING.
-- Lake encryption (P4-205): encryption at rest is platform/R2-side (SSE),
-- not a Fluss WITH option — no DDL knob exists for it. original_payload
-- access is restricted to triage roles per platform policy.
--
-- reason vocabulary (R-219 — restored from the pre-rewrite DDL):
--   MISSING_BROKER_ID | AMBIGUOUS_CORRELATION | NO_MATCHING_INSTRUCTION
--   | UNPARSEABLE_PAYLOAD | UNKNOWN_POSTBACK_TYPE | DUP_FINGERPRINT
-- disposition vocabulary:
--   OPEN | INVESTIGATING | RESOLVED | DISMISSED | ESCALATED

CREATE TABLE Postback_Quarantine (
    quarantine_id           STRING      NOT NULL,
    postback_event_id       STRING,
    reason                  STRING      NOT NULL,
    original_payload        BYTES       NOT NULL,
    payload_hash            STRING      NOT NULL,
    broker_order_id         STRING,
    instruction_id          STRING,
    correlation_attempt     STRING,
    disposition             STRING      NOT NULL,
    disposition_reason      STRING,
    quarantined_ts          BIGINT      NOT NULL,
    disposition_ts          BIGINT,
    schema_version          STRING      NOT NULL
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'quarantine_id',
    'table.log.ttl' = '2d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
