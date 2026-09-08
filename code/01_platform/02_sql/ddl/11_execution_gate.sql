-- Execution_Gate: KV state — gate management owned by Executor
-- Owner: Executor
-- Type: KV (primary key on execution_partition_id)
-- Retention: current + history in audit
-- Lake: encrypted immutable audit under approved policy (one-year minimum target)
-- Scope: execution_partition_id, account_scope_id
-- Schema version: 3
--
-- v2 history: state/epoch/approvals plus evidence hash.
--
-- v3 (2026-08-20, CHG-044, T5): added the fencing representation the epoch-only
-- DDL previously lacked — explicit `owner_instance_id` (the fenced executor
-- instance that holds the partition lease), `fence_token` (monotonically
-- increasing per partition, never reused), `fence_acquired_ts` /
-- `fence_lost_ts` (acquisition and loss evidence), `lease_expires_ts`, and
-- `approved_evidence_hash` (the exact evidence hash the single-operator approval
-- covered, so an epoch change or a new evidence package invalidates the
-- approval; DEC-044 keeps `approval_1` (authenticated authorized principal
-- `saurabh`) and retains `approval_2` as optional — a second approval is not
-- required and not checked).
-- `epoch` remains the gate-generation value and is NOT a substitute for the
-- fence token: the fence token is the per-partition owner sequence that must
-- still be valid immediately before every authorized bridge command.
-- Writers: gate-transition (state/epoch/reason/detection_time/evidence_hash/
-- transition_ts), gate-fence (owner/fence/lease columns), gate-approvals
-- (approval_1/approval_2/approved_evidence_hash). See
-- ExecutionGateColumnOwnership (SCH-15).
--
-- Domain contract (P4-037/P4-038/P4-039/P4-040 — Fluss has no CHECK/DEFAULT/
-- FK; enforced in running code, not here):
--   state/epoch/fence (P4-037): state is the GateState enum, transitions gated
--     by GateTransitionValidator (legal matrix + stale-epoch reject, epoch+1 per
--     transition); fence_token monotonic per partition via the store's fence
--     sequence (acquire/renew conflict on stale token); schema_version
--     writer-set '3' (ExecutionGateColumns.SCHEMA_VERSION_V3).
--   approvals (P4-038): GateRow.approvalsComplete requires approval_1 AND
--     approved_evidence_hash; the store rejects epoch-mismatch + unauthorized
--     principals and GatewayHttpServer halts the gate on either. Principal is
--     the DEC-044 single-operator placeholder — rotate via the store's
--     authorizedApprovers set, not via schema.
--   fence nullability (P4-039): NULL owner/token/lease = unfenced BY DESIGN
--     (a HALTED/never-fenced row holds no lease); every bridge command
--     re-validates immediately before execution — NautilusIntentClient.forward
--     DEFERS on missing row, non-ENABLED state, null/zero fence, or expired
--     lease (same rule in GateRow.fenceValidFor).
--   PK (P4-040): NOT ENFORCED is mandatory Flink syntax; uniqueness is the KV
--     upsert key + single-writer + store init never-clobber. (execution_
--     partition_id, account_scope_id) composite waits on the multi-account
--     milestone (store lookup, client lookup, scratch tables, columns pin).
--   retention split (P4-041): 'table.log.ttl' 30d is the HOT changelog only;
--     1-year history lives in the Iceberg lake (datalake enabled, audit carrier)
--     + GateStateStore immutable audit log (append-order, crash-window
--     reconstruction). KV snapshot is current state, not the audit carrier;
--     no 365d TTL bump (storage cost, zero proof lake path broken).
--   ts ordering (P4-190 — Fluss has no CHECK): lease-freshness enforced live
--     by GateRow.fenceValidFor (expired lease fails closed) + forward DEFERS
--     on null/zero/expired fence; detection_time/transition_ts/fence_acquired_ts/
--     fence_lost_ts ordering unwitnessed — future gate-transition validator,
--     not DDL.
--   bucket key (P4-191): 'bucket.key' = execution_partition_id alone is
--     deliberate; partition→account functionally 1:1 today (see PK above),
--     so adding account_scope_id reshuffles distribution for zero isolation
--     gain. bucket.num 4 fixed; changing it later requires rewrite.
--   lake reads (P4-192): bridge auth MUST read the Execution_Gate KV
--     strongly-consistent snapshot (gate-store lookup), NEVER the Iceberg
--     replica (5min stale + auto-compaction); lake is audit/forensics only.

CREATE TABLE Execution_Gate (
    execution_partition_id  STRING      NOT NULL,
    account_scope_id        STRING      NOT NULL,
    state                   STRING      NOT NULL,
    epoch                   BIGINT      NOT NULL,
    reason                  STRING,
    detection_time          BIGINT,
    evidence_hash           STRING,
    approval_1              STRING,
    approval_2              STRING,
    transition_ts           BIGINT      NOT NULL,
    owner_instance_id       STRING,
    fence_token             BIGINT,
    fence_acquired_ts       BIGINT,
    lease_expires_ts        BIGINT,
    fence_lost_ts           BIGINT,
    approved_evidence_hash  STRING,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (execution_partition_id) NOT ENFORCED
) WITH (
    'bucket.num' = '4',
    'bucket.key' = 'execution_partition_id',
    'table.log.ttl' = '30d',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
