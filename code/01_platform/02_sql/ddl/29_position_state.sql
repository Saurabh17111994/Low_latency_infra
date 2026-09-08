-- Position_State: KV current-state — open/closed lifecycle per instrument
-- Owner: Execution Gateway (Nautilus) — steady-state writer; ops ADMIN_CLEAR
--   break-glass (runbook) is the only authorized second writer (P4-238)
-- Type: KV (primary key on (account_scope_id, instrument_token) — P4-005 v2:
--   was instrument_token-only; a second account reusing the same token
--   overwrote the lifecycle row)
-- Bucket key: instrument_token (PK suffix — per-instrument colocation with
--   Signal tables; Fluss connector requires bucket.key ⊆ primary key)
-- Retention: unbounded changelog for the ACTIVE block — no log.ttl (P4-090 v2:
--   was 7d, contradicting the indefinite-block design; TTL expired the
--   changelog a fresh deploy/scale replays after >7d idle. Position TTL
--   policy: block lives until CLOSED/ADMIN_CLEAR; the stuck-ACTIVE alert
--   (position-state-alerts.json) is the expiry signal, not TTL)
-- Lake: EOD Iceberg offload (like Signal_Candidates_current)
-- Scope: account_scope_id (P4-238 v2: portfolio_id REMOVED CHG-005)
-- Schema version: 2 (v2: composite PK + source_version + status contract)
--
-- Why KV (2026-08-18, Option B): Signal_Candidates is immutable LOG,
-- Signal_Candidates_current is Flink's current-signal view, but the
-- lifecycle (OPEN vs CLOSED) is owned by the execution layer (Nautilus
-- confirms broker fill/exit). Fluss Position_State is the handshake:
-- Flink writes signal → Nautilus reads → broker → Nautilus UPSERTS
-- CLOSED. Flink's ActiveSignalFilter watches this table's LOG changelog
-- (via a second Fluss source) and clears its per-instrument ACTIVE
-- block only on CLOSED. No TTL — block is indefinite until CLOSED,
-- survives restarts (checkpointed + durable KV). TTL would risk duplicate
-- while broker position still open.
--
-- Status contract (P4-088 v2 — Fluss has no CHECK; enforced in gateway +
-- ActiveSignalFeedbackFunction + startup check): status OPEN | CLOSED |
-- ADMIN_CLEAR (exact, uppercase). CLOSED/ADMIN_CLEAR MUST set
-- closed_ts+closed_reason; OPEN MUST leave them NULL. Unknown states are
-- quarantined + halted, never defaulted to OPEN (a typo must not block an
-- instrument forever). ADMIN_CLEAR is the ops break-glass value (runbook +
-- position-state-alerts.json), not a gateway value.
-- Ordering (P4-089 v2): blind LWW upsert is stale-unsafe — the writer MUST
-- ignore-if-older on source_version (monotonic per (account, token)).

CREATE TABLE Position_State (
    account_scope_id        STRING      NOT NULL,
    instrument_token        BIGINT      NOT NULL,
    status                  STRING      NOT NULL, -- OPEN|CLOSED|ADMIN_CLEAR, see contract above
    position_id             STRING,
    updated_ts              BIGINT      NOT NULL, -- epoch-millis of Nautilus lastUpdateTs; NOT an ordering guard
    closed_ts               BIGINT, -- epoch-millis; REQUIRED when status IN ('CLOSED','ADMIN_CLEAR')
    closed_reason           STRING, -- REQUIRED when status IN ('CLOSED','ADMIN_CLEAR')
    source_version          BIGINT      NOT NULL, -- monotonic per (account_scope_id, instrument_token); writer MUST ignore stale versions
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (account_scope_id, instrument_token) NOT ENFORCED
) WITH (
    'bucket.num' = '16',
    'bucket.key' = 'instrument_token', -- P4-005: ⊆ composite PK; add 'table.kv.format-version' = '2' for composite keys
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
