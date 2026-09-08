-- Postback_Projection_Ledger: KV state — projection recovery workflow
-- Owner: Action Capture
-- Type: KV (primary key on postback_event_id)
-- Retention: incomplete + recovery/disposition window
-- Note: MVP (2026-07-23) skipped — re-process recent postbacks on restart
-- R-235 (2026-08-03): column renamed completeted_ts -> completed_ts (typo baked
--   into the schema would have propagated into downstream recovery code).
-- Scope: deployment-scoped (P4-059: postback_event_id is the globally-unique
--   deterministic fingerprint — PostbackFingerprint SHA-256 over the canonical
--   broker tuple (id/remarks/status/report_type/fill_shares/average_price/
--   exchange_update_time) — NOT a broker-assigned per-account id, so no
--   cross-account overwrite is possible. No composite-PK surgery: the live
--   gateway writer (FlussProjectionLedgerStore) upserts the single-field PK
--   today, and widening the PK needs kv.format-version=2 + raw-client proof
--   (COMPAT-FLUSS-005) + a recreate — deferred, no caller needs it.)
-- Schema version: 2
--
-- Writer contract (P4-058): single-writer-per-event via ProjectionApplier —
-- apply() is synchronized, reads current state, advances one step, and puts.
-- expected_prior_state is the CAS witness (mismatch = concurrent write, retry
-- from lookup). The KV PK alone gives last-writer-wins, NOT atomicity — the
-- atomicity lives in the applier, and Flink keyed-state CAS is the fallback
-- if multi-writer retries ever appear. This table is the durable snapshot.
-- Vocabulary (P4-206, real sets — not the finding's guesses):
--   projection_state: RECEIVED|AUDIT_WRITTEN|LIFECYCLE_APPLIED|
--     POSITION_APPLIED_OR_NOT_REQUIRED|COMPLETE (ProjectionLedger.State;
--     transition-guarded by advance(), terminal = COMPLETE).
--   disposition: OPEN|COMPLETE (ProjectionApplier.entry: COMPLETE iff state
--     is COMPLETE, else OPEN).
-- Invariants (P4-207): retry_count >= 0 (writer always writes 0 today —
--   retry accounting is future work, not a live counter); last_error REQUIRED
--   iff the apply threw (failure put carries the message, success puts null);
--   disposition COMPLETE iff projection_state COMPLETE; step_ts epoch-ms
--   writer clock; completed_ts NULL until COMPLETE (clock.millis() at the
--   COMPLETE put), never before.
-- Retention (P4-208/209): KV snapshot is the durable contract; the 2d log
-- TTL bounds changelog replay only. Recovery window = re-process recent
-- postbacks on restart (MVP note above) — recent means within the 2d log
-- window; older incomplete rows are found via incomplete() scan of the KV
-- snapshot, NOT log replay. Rebuild source for a lost snapshot: broker
-- replay (re-process recent postbacks), same as the MVP path — no lake twin
-- by design (transient recovery state, not audit evidence).

CREATE TABLE Postback_Projection_Ledger (
    postback_event_id       STRING      NOT NULL,
    projection_state        STRING      NOT NULL,
    expected_prior_state    STRING,
    retry_count             INT         NOT NULL,
    last_error              STRING,
    disposition             STRING,
    step_ts                 BIGINT      NOT NULL,
    completed_ts            BIGINT,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (postback_event_id) NOT ENFORCED
) WITH (
    'bucket.num' = '8',
    'bucket.key' = 'postback_event_id',
    'table.log.ttl' = '2d'
);
