-- Portfolio_Reservations: KV state — portfolio capacity reservations
-- Owner: Signal job
-- Type: KV (primary key on reservation_id)
-- Retention: active + rebuild/reconciliation window
-- Scope: portfolio_id
-- Schema version: 2
--
-- No live writer (P4-051/053): this table has no producing service — the
-- DdlBootstrap entry is existence-check-only (logTable fallback), and the
-- ranking feed that would write it was REMOVED by CHG-005. These bind the
-- future writer; no composite-PK/CAS surgery until a writer exists:
--   identity (P4-051): reservation_id MUST be globally unique/namespaced
--     (portfolio_id-prefixed UUID); writer MUST scope-check
--     (portfolio_id, account_scope_id) on every write. Composite
--     (portfolio_id, reservation_id) PK is recreate-only — deferred.
--   concurrency (P4-053): future writer MUST do read-check-write on
--     (reservation_id, transition_version) with version+1 + retry; DDL
--     alone never gives CAS (NOT ENFORCED is required syntax).
-- Vocabulary (P4-052): state HELD|RELEASED|EXPIRED|CONSUMED;
--   capacity_class LONG|SHORT|GROSS|NET (writer-enforced, DDL has no CHECK).
--   expiry_ts > created_ts invariant + an expiry sweeper are the future
--   writer's contract — without them stale holds leak capacity.
-- Retention (P4-054): 2d changelog TTL bounds replay only; KV snapshot is
--   the durable contract. Rebuild window = active + reconciliation window
--   (bounded by expiry_ts sweep, not by TTL). No lake twin by design
--   (transient holds, not audit) — holds needing audit ride the decision
--   tables, not this one; TTL bump + datalake needs a recreate + a reader.
-- Lineage (P4-201): source_evidence NULL-legal only for system-generated
--   releases (writer documents the cause); instruction_id NULL until a hold
--   binds to an instruction. Lookup paths: by reservation_id (PK point
--   lookup), sweeps by portfolio_id/expiry_ts (full-scan, see bucketing).
-- Bucketing (P4-202): bucket.key=reservation_id = point-lookup path;
--   per-portfolio sweeps full-scan by design at this volume — no secondary
--   index in Fluss, no rebucket without measured fan-out cost.

CREATE TABLE Portfolio_Reservations (
    reservation_id          STRING      NOT NULL,
    portfolio_id            STRING      NOT NULL,
    account_scope_id        STRING      NOT NULL,
    instruction_id          STRING,
    candidate_id            STRING      NOT NULL,
    capacity_class          STRING      NOT NULL,
    state                   STRING      NOT NULL,
    transition_version      BIGINT      NOT NULL,
    source_evidence         STRING,
    expiry_ts               BIGINT      NOT NULL,
    created_ts              BIGINT      NOT NULL,
    updated_ts              BIGINT      NOT NULL,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (reservation_id) NOT ENFORCED
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'reservation_id',
    'table.log.ttl' = '2d'
);
