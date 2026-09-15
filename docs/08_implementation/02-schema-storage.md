# Schema and Storage

Use this file to build the data tables and the tests that prove they are safe.

## What to build

<!-- markdownlint-disable MD013 -->

### Status

| Field               | Value                                                                                                 |
| ------------------- | ----------------------------------------------------------------------------------------------------- |
| Status              | Design-ready; Phase A (26 DDLs) complete — manifest grew from 21 to 26 on 2026-08-15 (`24_fingerprint_dedup.sql` DEC-038 dedup state + `25_trade_instruction_state.sql` SCH-19 instruction index + `26_eod_offload_state.sql` SCH-23 offload state); Phase B (static validation) complete; Phase C complete (2026-08-15: SCH-12/13 implemented as the env-gated `CompatFlussDdlParityIntegrationTest` — all 27 DDLs apply + inspect historical 2026-08-24 with manifest parity on Fluss 0.9.1-incubating; SCH-14 implemented — KV changelog records verified FULL row images; SCH-16 implemented — COMPAT-FLINK-002 cross-table-visibility probe + evidence; SCH-17 checkpoint/restore verified earlier); Phase D partially complete (SCH-19 machinery implemented 2026-08-15 — pure-JVM writer core + dual-sink helper + instruction index DDL; the ranking feed was REMOVED from scope by decision — CHG-005, 2026-08-15, not deferred; SCH-20 operator wiring still open); Phase E partially complete (SCH-23 controller service + durable offload state implemented 2026-08-15 — runner, state table, extension drill; SCH-24 pure-JVM encrypted-export core implemented 2026-08-15 — `EncryptedExportEodOffloadExecutor` + bundle source + envelope crypto/key management, 8 tests — the live tiered-storage/lake offload half REMOVED from scope by decision — CHG-005)                                         |
| Owner               | Storage/Platform Team                                                                                 |
| Source requirements | `REQ-FLS-*`, `docs/02_requirements/04-data.md`, `DEC-001`, `DEC-005`, `DEC-018`, `DEC-020`, `DEC-021` |
| Acceptance criteria | `AC-FLS-001`–`AC-FLS-017` (proving families: `SCHEMA-*`, `COMPAT-FLUSS-*`, `COMPAT-FLINK-001`, `DDL-*`) |
| Migration posture   | Pre-production clean break until live-money release                                                   |

### Purpose

This dossier defines how logical schemas become validated physical Fluss DDLs and how schema changes, retention, replay, lake offload, and rollback are controlled.

### Schema states

```text
PROPOSED
  → APPROVED   (the only state carrying executable authority)
  → APPLYING
  → OBSERVED
  REJECTED     (failure exit; no authority)
```

A DDL under `code/01_platform/02_sql/ddl/` is not executable authority until it reaches `APPROVED` for the pinned Fluss/Flink matrix. The code enum (`code/common/.../schema/SchemaState.java`) is deliberately five states — an earlier seven-state design with separate reconciliation and dialect/integration-validation states was simplified; the load-bearing rule is unchanged (`isExecutableAuthority()` == APPROVED only).

### Schema manifest

Each release must generate a machine-readable manifest containing:

| Field                     | Meaning                                           |
| ------------------------- | ------------------------------------------------- |
| `schema_manifest_version` | Version of the manifest format                    |
| `table_name`              | Exact case-sensitive logical/physical table name  |
| `schema_version`          | Table contract version                            |
| `ddl_path`                | Repository-relative DDL file                      |
| `ddl_sha256`              | Checksum of normalized DDL                        |
| `table_kind`              | LOG, KV, manifest, or immutable feed              |
| `writer_owner`            | Sole writer or column-group owners                |
| `primary_key`             | Physical key or `none`                            |
| `bucket_key`              | Guaranteed non-null routing key                   |
| `retention_policy`        | Live retention and extension rule                 |
| `lake_policy`             | Offload/audit behavior                            |
| `compatibility_class`     | Backward, forward, full, breaking, or clean-break |
| `validated_matrix`        | Version compatibility record ID                   |

> **Note (2026-08-15):** the on-disk manifest emits all 12 per-entry fields — `table_name`, `ddl_path`, `ddl_sha256`, `table_kind`, `primary_key`, `bucket_key`, `compatibility_class`, `validated_matrix` (plus top-level `schema_manifest_version`) AND `schema_version`, `writer_owner`, `retention_policy`, `lake_policy`, all parsed from the DDL header comments and WITH options by `ddl_apply.py`. The 2026-08-13 note that only 8 fields were emitted is superseded; `ddl_apply.py` now flags a manifest missing/stale on the emitted fields as drift (regenerate with `--force`). `ingestion_quarantine` gained its missing Type/Retention/Schema-version DDL header the same day (checksum changed, manifest regenerated).

### DDL application contract

1. Validate exact Fluss/Flink versions.
2. Verify the schema manifest and DDL checksums.
3. Parse every DDL against the pinned dialect.
4. Apply to an empty acceptance catalog in deterministic order.
5. Inspect the effective schema/options from the runtime.
6. Run schema parity tests against logical requirements.
7. Run write/read/changelog/restore tests.
8. Record the applied manifest ID.
9. Refuse service readiness if required table/version differs.

**Implemented 2026-08-15** — `make ddl APPLY=1 EVIDENCE=<capability-evidence>` executes the contract end-to-end through the Java engine `DdlApplyTool` (`code/common/.../schema/ddl/`, invoked by `ddl_apply.py`):

- Steps 1–2: version gate + manifest/checksum verification (ddl_apply.py; the tool re-verifies each DDL's sha256 against the manifest).
- Step 3: `DdlText` parses each DDL into an admin-API descriptor (Fluss 0.9.1 has no SQL client).
- Step 4: refuses with exit 3 unless the acceptance catalog is empty; BEFORE creating anything, the COMPAT-FLUSS-005 raw-client composite-PK matrix is re-verified **in-band** against this live cluster (scratch tables, dropped after — `CompositeKeyMatrixVerifier`, shared with the env-gated JUnit test); a deviation refuses the apply (exit 1) so the matrix is never just referenced as capability evidence; applies in DDL-file order.
- Steps 5–6: `Admin.getTableInfo` inspection with per-table parity — bucket count, columns, PK, bucket key, and the **full WITH-option set**: every option the DDL declares (the manifest pins the DDL bytes by checksum) must be honored by the effective table, with two carve-outs — `bucket.num`/`bucket.key` are distribution (expressed via `distributedBy`, not properties) and `table.datalake.enabled` is the documented dev deviation (forced false; lake-enable is create-only in 0.9.1). The coordinator may stamp extras (`table.replication.factor`, cluster-inherited `table.datalake.format`, `table.kv.format-version`) — those are not asserted. Verified 2026-08-15 against all 21 live tables; same checks as COMPAT-FLUSS-001.
- Step 7: write/read smoke per table **against a scratch twin** (`<prefix>smoke_twin_<table>`, same descriptor — LOG append + scan, KV upsert + lookup) that is dropped immediately after the round trip; real tables never receive fixture rows (LOG fixture rows are undeletable and would permanently halt fail-closed readers — see CHG-100). `--allow-live-smoke` (old behaviour, fixtures then land in the real table) is a debug/evidence escape hatch only; `DDL_APPLY_SWEEP=1` + `DDL_APPLY_SWEEP_FIX_KV=1` sweep live tables for leftover fixture signatures (KV fixtures deletable, LOG fixtures reported with exit 3). The full changelog/restore battery is the capability evidence supplied as `EVIDENCE=` (COMPAT-FLUSS-*/COMPAT-FLINK-* records), whose path + sha256 are recorded in the apply record. Composite-PK KV tables whose bucket key equals the PK cannot be upserted by the raw 0.9.1 client (see the finding below) — recorded per table as `LIMITATION`. **A limitation is never absorbed into PASS:** the apply status is `PASS` only when EVERY table smoke passes; otherwise it is `PASS_WITH_LIMITATION`, which exits with the dedicated code 6 when `--ack-limitations` names exactly the limited tables (the documented Flink-connector-only design; `DDL_APPLY_ACK_LIMITATIONS` via `ddl_apply.py`) and exits 1 otherwise (fail-closed, `DdlApplyToolStatusTest`). `--ack-limitations auto` predicts the limited tables from the manifest (composite PK with bucket key = PK) and prefills the acknowledgment, so operators confirm rather than guess — the resolved names and `ack_mode=auto` are recorded in the evidence. The evidence record carries `limitations` and `acknowledged_limitations` arrays.
- Step 8: the tool writes `logs/ddl-apply/<ts>/apply.json` carrying `applied_manifest_id` = sha256 of the manifest bytes, versions, per-table ids/parity/smoke, the matrix-evidence reference, and the in-band `matrix` record (status + the 4 cells with expected vs observed outcomes + matched flags).
- Step 9: the applied-manifest-id is the token each service's existing preflight compares against its required table/version to refuse readiness on mismatch (e.g. `SignalJob.preflightTableContracts`, `InstrumentManifestLoader`).

Dev verification (never production): `DDL_APPLY_TABLE_PREFIX=<p>` applies to scratch tables (dropped after the run) so the full flow can run against a non-empty dev cluster; `DDL_APPLY_SKIP_SMOKE=1` skips the smoke.

**Containerized run (2026-08-15):** the engine is packaged as the `ddl-apply` image (`code/01_platform/01_docker/ddl-apply/`, `make ddl-image`) so the contract runs INSIDE the compose network — `FLUSS_BOOTSTRAP` resolves via compose DNS (`fluss-coordinator:9123`), no host `/etc/hosts` aliases. One-shot: `docker compose -f code/01_platform/01_docker/docker-compose.yml run --rm ddl-apply {validate|apply|smoke|evidence-check|self-test}` (the entrypoint passes through `DDL_APPLY_TABLE_PREFIX`, `DDL_APPLY_ACK_LIMITATIONS` incl. `auto`, `DDL_APPLY_SKIP_SMOKE`); evidence is bind-mounted to host `logs/ddl-apply`. The `evidence-check` subcommand runs the non-root ownership gate standalone, and `apply` auto-runs it on its own output corpus before exiting — a violation is a hard failure (exit 1) even when the apply itself passed. The image mirrors the repo layout (`REPO_ROOT=/app`), so `ddl_apply.py`/`ddl_apply_smoke.py` run unchanged (jars are baked at `/opt/ddl-apply/m2/repository`, selected via `DDL_APPLY_M2_REPO` — no root home needed). The ENGINE runs as the non-root `ddlapply` user (uid/gid 10001, `DDL_APPLY_UID/GID` overridable): the entrypoint wrapper repairs the evidence ROOT dir (top-level chown + setgid 2775, umask 002 → records land group-writable 664) and then `setpriv`-drops permanently — engine code never executes as root. On every start the wrapper prints the applied contract — evidence root owner/mode read back via stat, umask → 664 — into the container output (and the enforced uid/gid on strict non-root runs), so operators see exactly what the ownership gate will enforce. The host-side orchestrator (`ddl_apply.py`) echoes the same contract on every host run (host uid, no repair; host-owned records are out of the gate's scope), suppressed in-container (`DDL_APPLY_IN_CONTAINER`, set by the image runner) so the wrapper's applied line is never doubled — operators see one ownership expectation either way. The ownership repair is **non-recursive by contract**: only the evidence root dir itself is claimed, and descendants are created by the engine (owned by it, group inherited via the setgid bit) — so pre-existing content under a redirected path is never flipped to 10001. `DDL_APPLY_EVIDENCE_DIR` redirects where evidence is written (both the engine and the wrapper honor it); point it at a **dedicated subdirectory** on any mount — never the mount root itself, since only that directory is claimed. The contract is regression-guarded by `evidence_ownership_check.py` (`make evidence-ownership-check`, `docs-audit` C15, Monday gate DDL step): the evidence root dir must carry setgid + group-write (expected 2775), and every container-written record (owner == the engine uid) must be group-writable AND carry the engine GID (10001) — no record may be root-owned. The gate fails on drift; host-side `make ddl` records are out of scope. Host delete/rotate of container-written evidence without sudo needs a one-time shared group: `sudo groupadd -g 10001 ddlapply && sudo usermod -aG ddlapply $USER` (then re-login). Verified 2026-08-15: in-container `validate` exit 0, full `apply` (scratch prefix + `auto` ack) exit 6 with in-band matrix PASS and group-writable evidence on the host, and the 4-scenario `smoke` battery (incl. the containerized bad-ownership drill) PASS — all against `fluss-coordinator:9123`.

> **Evidence finding (2026-08-15, verified):** Fluss 0.9.1-incubating's raw client cannot upsert to a KV table whose primary key has more than one field — "Key fields must have exactly one field for iceberg format" (`IcebergKeyEncoder`). Root cause: the dev cluster configures cluster-level `datalake.format=iceberg` (docker-compose `DATALAKE_FORMAT`), so **every** table inherits the iceberg datalake key encoder, and `IcebergKeyEncoder` hard-requires exactly one key field (`Preconditions.checkArgument` in its constructor). Two table options decide the outcome: `table.kv.format-version` (absent → v1) and whether the bucket key is a single-field subset of the PK. Probe matrix against the live cluster (all four upsert+lookup round-trips):
>
> | PK | bucket key | kv.format-version | raw-client upsert |
> | --- | --- | --- | --- |
> | composite | = PK (default) | 1 (default) | ✗ |
> | composite | = PK (default) | 2 | ✗ (default-bucket path still uses the datalake encoder) |
> | composite | single-field subset | **2** | ✓ **PASS** |
> | composite | single-field subset | 1 | ✗ |
>
> **Superseded for the execution-gateway boundary (2026-08-19, CHG-042):** the prior
> connector-only decision for `Order_Lifecycle` and `Order_Correlation` is retained as historical
> evidence, but the approved current DDL now uses `table.kv.format-version='2'` with the
> single-field subset bucket keys `account_scope_id` and `instruction_id`, respectively. This
> preserves the composite primary keys and account/attempt isolation while making the tables
> writable by the gateway's raw Fluss client. **Live-verified 2026-08-19:** the COMPAT-FLUSS-005
> matrix passed against the local cluster, and a full scratch apply+publish/lookup ran to
> `RESULT=PASS EXIT=0` for all 26 tables (incl. both composite-PK tables and the gateway's new
> durable `Execution_Intent_Processed` reprocessing index) with the matrix PASS
> gated in-band (see `ddl_apply_smoke.py` and the T2 evidence in `logs/nautilus-execution/phase1/t2-fluss-live`).
>
> Applied: `feature_candles_15s` (already had bucket.key=`instrument_token`) and `instruments` (bucket.key narrowed to `instrument_token`) now carry `table.kv.format-version='2'` and pass the raw-client smoke; both are the documented-current format with proper prefix-lookup semantics. Accepted design: `Order_Lifecycle`/`Order_Correlation` keep their deliberate composite bucket keys (per-account / per-instruction colocation, headers document them) — their writers are the Flink connector (Executor-era), which bypasses the limitation entirely (composite-PK candle writes proven by CandleGraphReplayIntegrationTest). A raw-client writer targeting them would need the v2 + single-field-subset-bucket-key workaround above; no such writer exists or is planned. The DDL apply contract now requires an explicit acknowledgment for exactly these tables (`--ack-limitations Order_Lifecycle,Order_Correlation` / `DDL_APPLY_ACK_LIMITATIONS`) before an apply can complete — an acknowledged partial apply exits with the dedicated code 6 and status `PASS_WITH_LIMITATION` (distinct from full-PASS 0, so automation can branch), and exits 1 if unacknowledged. The composite-PK matrix result is wired into the contract, so an apply is never silently `PASS` while limited tables exist. Every terminal outcome prints a machine-readable `ddl-apply: RESULT=<STATUS> EXIT=<code> TABLES=<n> MANIFEST=<id>` sentinel (via `ddl_apply.py`: `DDL-APPLY-RESULT: PASS exit=0` / `PASS_WITH_LIMITATION exit=6`). Evidence: `logs/schema-compat/composite-pk-raw-client-20260815.md`. The matrix is now a permanent env-gated test — `CompatFlussCompositeKeyIntegrationTest` (COMPAT-FLUSS-005) re-verifies every cell on each live run — and the SAME verifier (`CompositeKeyMatrixVerifier`, main code) gates every DDL apply **in-band**: `DdlApplyTool` re-derives the 4 cells against the live cluster before creating anything, records them in the evidence `matrix` object, and refuses the apply (exit 1) on any deviation. The matrix is verified, not merely referenced as capability evidence. The terminal contract itself is regression-guarded by `code/01_platform/04_scripts/ddl_apply_smoke.py` (`make ddl-apply-smoke`, wired into the Monday gate `run-monday-gates.sh`) — an env-gated live smoke that runs the orchestrator three times against scratch-prefixed catalogs and asserts the exit-code contract 0 (full PASS) / 6 (acknowledged `PASS_WITH_LIMITATION`) / 1 (refused limitation) plus the `RESULT=`/`DDL-APPLY-RESULT:` sentinels and the evidence record (`status`, `ack_mode`, `acknowledged_limitations`). A fourth containerized drill (hosts with docker + the built image) mounts a pre-seeded engine-uid-owned 644 evidence record and asserts the apply exits 1 with `EVIDENCE OWNERSHIP CHECK FAILED` naming the seeded record — while the engine's own sentinel still documents the apply — so the non-root ownership gate's fail-closed behavior is itself regression-guarded end-to-end.

> **Current interpretation (CHG-042):** the long paragraph above records the 2026-08-15
> connector-only decision as historical evidence. It does not describe the current gateway
> deployment contract. The current contract is the DDL amendment and live-evidence requirement
> stated in the supersession banner above.

### Table categories and invariants

| Category                   | Invariant                                                                               |
| -------------------------- | --------------------------------------------------------------------------------------- |
| Immutable LOG              | Append one platform event per delivery; no silent update/delete; duplicates explicit    |
| Immutable instruction feed | Existing identity may not change content; mutation is a contract violation              |
| KV projection              | Keyed current state; source-version and transition rules reject stale/regressive writes |
| Gate/attempt state         | Compare current epoch/phase before transition; every transition audited                 |
| Manifest                   | One approved manifest version defines active subscription state                         |

### Routing-key rule

Every LOG write must have a non-null routing identity. Nullable business fields must not be the only bucket key.

Proposed routing review:

| Table                        | Required routing identity                                 | Rationale                                |
| ---------------------------- | --------------------------------------------------------- | ---------------------------------------- |
| `raw_table_1`                | `instrument_token` after validation                       | Per-instrument processing order          |
| `feature_candles_15s`        | `instrument_token`                                        | Per-instrument window history            |
| `Signal_Candidates`          | `instrument_token` (LOG append; R-084 KV conversion reversed 2026-08-13) | Per-instrument signal locality          |
| `Signal_Candidates_current`  | `instrument_token` (KV primary key)                            | Colocated current-state per instrument  |
| `Execution_Intent`           | `instruction_id`                                               | Durable immutable execution request routing |
| `Ranking_Results`            | ~~`evaluation_id`~~ (R-136 — was `candidate_id`) — **REMOVED from scope 2026-08-15 (CHG-005)**; DDL retained as reserved schema (in `schema_manifest.json`, applied, never written)                         | ~~Avoid cross-instrument/null ambiguity~~    |
| `Fills`                      | `postback_event_id` when broker ID may be absent          | Every delivery is routable               |
| `Execution_Audit`            | `audit_event_id`                                          | Gate-only events may lack instruction ID |
| `Portfolio_Reservations`     | ~~`reservation_id`~~ — **REMOVED from scope 2026-08-15 (CHG-005)**; DDL retained as reserved schema (in `schema_manifest.json`, applied, never written)                                          | ~~Authoritative reservation state~~          |
| `Postback_Projection_Ledger` | `postback_event_id`                                       | Recovery workflow state                  |
| `Safety_Halt_Requests`       | `halt_request_id`                                         | Durable control event identity           |
| `Postback_Quarantine`        | `quarantine_id`                                           | Missing broker ID is expected            |

Final physical keys remain evidence-gated by pinned Fluss distribution semantics.

### Immutability protocol

For each immutable entity, calculate a canonical versioned content hash:

```text
same identity + same hash    → idempotent duplicate evidence
same identity + different hash → contract violation
new executable content       → new identity with supersedes relation
```

Writers must persist or query enough state to detect mutation. LOG-table comments and `NOT ENFORCED` keys do not enforce this protocol.

### KV state update protocol

A projection update includes:

- Aggregate key
- Source event ID
- Source version/timestamp
- Previous expected state/version when supported
- New state
- Transition reason
- Schema/projection version

The projector must check:

1. Duplicate source event.
2. Older source version.
3. Invalid state transition.
4. Terminal-state regression.
5. Conflicting evidence at equal version.

Conflict yields `UNKNOWN`, quarantine/audit, alert, and affected order-path halt. `partial_update` merges columns but does not replace these checks.

### Schema evolution classes

| Class                 | Rule                                                                                     |
| --------------------- | ---------------------------------------------------------------------------------------- |
| Additive compatible   | New optional field with default/null semantics; readers tolerate unknown field           |
| Behavioral compatible | Same physical schema but changed algorithm/config version; replay comparison required    |
| State incompatible    | Flink serializer/state shape changes; savepoint migration or clean restart required      |
| Wire incompatible     | Protocol/event schema cannot be read by old consumer; ordered deployment required        |
| Breaking clean-break  | Pre-production only; destructive approval, reset, replay, and rollback evidence required |

Every change records producer-first/consumer-first order, dual-read/write needs, savepoint impact, lake synchronization, and rollback readability.

### EOD controller and offload gate

The EOD controller is a named service or scheduled job owning manifest creation, verification, retry/backoff, retention extension, expiry protection, and manual reconciliation. Source data for a trading day SHALL not expire while the manifest is unverified, retryable, or under reconciliation.

Each table/day offload record contains:

- Trading date
- Table and schema version
- Source offset/range
- Row and byte counts
- Source and target hashes/checksums
- Iceberg snapshot/commit ID
- Verification state
- Retry count and next retry
- Earliest allowed source expiry

State machine:

```text
PENDING → WRITING → COMMITTED → VERIFYING → VERIFIED
                    ↘ FAILED_RETRYABLE
                    ↘ FAILED_MANUAL
```

Source data cannot expire unless state is `VERIFIED`, and at least three complete trading days remain live. Unverified or retryable state extends retention through a tested control mechanism; a fixed DDL TTL comment is insufficient.

### Test requirements

- DDL parse/apply for pinned matrix.
- Effective schema/options inspection.
- Schema parity for every required field.
- Non-null routing and bucket-skew tests. _(Implemented 2026-08-15: SCH-07 `RoutingKeyRule` + full-manifest routing identity (every LOG table carries `bucket.key`, every KV table its PK — `SchemaComplianceFullSuiteTest`) + COMPAT-FLUSS-006 live skew probe (`CompatFlussIntegrationTest.compatFluss006BucketDistributionSkew`: 400 distinct keys over 8 buckets spread across every bucket within mean+3σ; constant-key control collapses to one bucket).)_
- Immutable duplicate/mutation tests.
- KV stale/regressive/conflict tests. _(Implemented: `KvStateUpdateProtocol` unit tests + COMPAT-FLUSS-004 rejection half (`KvStaleWriteRejectionTest` — stale/regressive/conflict/unknown writes rejected without mutation, halt + quarantine raised, duplicates no-op, mixed sequences never regress and audit every attempt) + the live last-write-wins observation.)_
- Changelog and partial-update tests.
- Checkpoint/replay compatibility tests.
- Clean-break reset and replay tests.
- EOD failure/retry/expiry-protection tests.

### Implementation tasks

Each task is atomic — verifiable by a single test or code inspection.
Tasks are ordered by dependency; no downstream task may start before its
upstream is complete.

#### Phase A: DDL authoring (blocks all downstream tests)

| ID | Task | Status | Location / Evidence |
| --- | --- | --- | --- |
| SCH-01 | Write DDL SQL for all 21 tables (plan-time count — 2026-08-10, before CHG-003/CHG-004; now 26): `raw_table_1`, `feature_candles_15s`, `forming_bar`, `Signal_Candidates`, ~~`Ranking_Results`, `Trade_Decisions`~~ (**REMOVED 2026-08-15, CHG-005**), `Fills`, `Order_Lifecycle`, `Positions`, `Execution_Gate`, `Execution_Attempts`, `Order_Correlation`, `Execution_Audit`, ~~`Portfolio_Reservations`~~ (**REMOVED 2026-08-15, CHG-005**), `Postback_Quarantine`, `Postback_Projection_Ledger`, `Safety_Halt_Requests`, `suspected_discontinuities`, `instruments`, `ingestion_quarantine`, `Signal_Candidates_current` | [x] | `code/01_platform/02_sql/ddl/` — 21 SQL files (02_raw_table_1 through 23_signal_candidates_current; 22_feature_candles_15s_current deleted) with column definitions, bucket keys, retention policies from contracts. **2026-08-15: the manifest grew to 26 files** — `24_fingerprint_dedup.sql` (DEC-038 dedup state) + `25_trade_instruction_state.sql` (SCH-19 instruction index) + `26_eod_offload_state.sql` (SCH-23 EOD offload state), added through the same offline DDL gate |
| SCH-02 | Compute SHA-256 checksum for every DDL file; populate `schema_manifest.json` entries | [x] | `schema_manifest.json` (26 entries as of 2026-08-15 — 21 at authoring, +`fingerprint_dedup`, +`trade_instruction_state`, +`eod_offload_state` — with table_name, ddl_sha256, table_kind LOG/KV, primary_key, bucket_key, compatibility_class, validated_matrix); `ddl_apply.py` computes + validates checksums (no schema_state field — see manifest note below) |
| SCH-03 | Stale DDL paths removed from `make ddl` application workflow; manifest drift check runs clean | [x] | `ddl_apply.py` — manifest is current, no DDL drift detected (21 tables as of 2026-08-10 — now 24; checksum + `table_kind` + `primary_key`/`bucket_key` field-level validation since 2026-08-10); stale duplicates removed |
| SCH-04 | Version-gate: `make ddl` refuses to apply when any Fluss/Flink version is unpinned | [x] | `version_matrix_verify.py` + `VersionGate.requirePinned()` + `versions.pin` |

> **Note (2026-08-09):** `forming_bar` has a DDL and a manifest entry but no requirement, contract, or consumer defines its role — the compute contract (`04_contracts/03-compute.md`) explicitly keeps forming-bar state in-process with no Fluss round trip. It stays `PROPOSED`/unowned until a consumer requirement (e.g. per-instrument freshness, DEC-028) is written; do not treat it as owned.
> **Note (2026-08-14, DEC-038):** when forming-bar state is implemented (Slice 2.2), the durable current forming bar is **Fluss-owned** — this `forming_bar` KV table is its authoritative home (in-process events still flow to Business Logic without a Fluss round trip, REQ-FC-007 preserved; the ranking rationale is REMOVED 2026-08-15, CHG-005). The dedup set likewise gains a Fluss KV state table (proposed `fingerprint_dedup`, key `(instrument_token, fingerprint_version, event_fingerprint)`); the 21-table manifest grows only with the DDL-implementation stage of DEC-038, gated by the same offline DDL path.
>
> **2026-08-15 — the dedup table has landed, and the DEC-038 live wiring is IMPLEMENTED:** `24_fingerprint_dedup.sql` (v1, 6 columns, PK `(instrument_token, fingerprint_version, event_fingerprint)`, `bucket.key=instrument_token`, 16 buckets, `kv.format-version=2`) is manifest-listed (26 tables) with the layout pinned by `FingerprintDedupTableColumns` + `FingerprintDedupTableColumnsAgreementTest`; `DedupExpiry` (writer-enforced logical TTL + bounded re-entrant cleanup selection) and the `DEDUP_*` config keys are unit-tested. **Live writer wiring (2026-08-15 final session):** `FingerprintDedupStateStore` (SPI) + `InMemoryFingerprintDedupStateStore` + `FlussFingerprintDedupStateStore` (raw client — the v2 + single-field-subset-bucket-key combo is the documented-working COMPAT-FLUSS-005 shape, same as `feature_candles_15s`); the `FingerprintDedupFunction` rework (bounded cache capped by `DEDUP_CACHE_MAX_ENTRIES`/`DEDUP_CACHE_MAX_BYTES`, authoritative store lookup on cache miss — lazy rehydration, never an empty dedup set (SIG-STATE-003), first-seen rows to the `fingerprint-dedup-write` side output, grid-aligned processing-time cleanup timers driving `scanExpired`+`delete` at `DEDUP_CLEANUP_INTERVAL_MS` with ZERO added keyed state); `FingerprintDedupWriterFunction` (batched durable upsert at `DEDUP_WRITE_BATCH_MS`/`DEDUP_WRITE_BATCH_SIZE`); `validateFingerprintDedupTable` now ALWAYS-ON in `preflightTableContracts` (fail-closed — the table must exist and match v1 before the job starts); 9 new tests green (externalization + writer cadence; 268 compute total). **Prerequisite:** the dev cluster must apply the table (`make ddl APPLY=1`, manifest 26) before the next SignalJob start — the preflight now requires it. The second new table is the SCH-19 instruction index `trade_instruction_state` (`25_trade_instruction_state.sql`, PK `instruction_id`, 4 columns, 8 buckets).
>
> **Note (2026-08-10, SUPERSEDED 2026-08-13 — see note below):** `Signal_Candidates` is now **owned and written** by the Signal job's Slice 2.1 (DEC-034) — closed-candle detection appends immutable candidate records via the KV upsert writer (`DdlBootstrap` carries the full 22-column KV descriptor; dev table created DDL-faithful). ~~`Ranking_Results` / `Trade_Decisions` / `Portfolio_Reservations` remain unwritten (ranking postponed)~~ — **REMOVED 2026-08-15 (CHG-005, not deferred).**
>
> **Note (2026-08-13):** REQUIREMENT CHANGE — `feature_candles_15s` is **KV-only** (PK `(instrument_token, window_start)`, upsert last-write-wins — converted in code/DDL/tests 2026-08-13; live dev table recreated as KV the same day, drop+recreate verified); `Signal_Candidates` is an append-only LOG (one row per fired signal, routed by `instrument_token`) and `Signal_Candidates_current` KV (PK `(instrument_token)`) holds the latest/active candidate per instrument; the R-084 KV conversion is reversed on the signal LOG and the candle KV projection (`feature_candles_15s_current`) is deleted (plan section: `08_implementation/04-signal-job.md` → "Current build plan — Signal LOG/KV dual-sink", Stages 2–4; the candle-KV part superseded by the 2026-08-13 KV-only conversion); the 2026-08-10 note above describes the pre-change state.

#### Phase B: Static schema validation (unit level, no cluster)

| ID | Task | Status | Location / Evidence |
| --- | --- | --- | --- |
| SCH-05 | Schema manifest format: generate, serialize, deserialize round-trip | [x] | `SchemaManifest.java` + `SchemaManifestEntry.java` + `SchemaManifestSerializationTest` |
| SCH-06 | Schema state machine: `isExecutableAuthority()` is false until APPROVED | [x] | `SchemaState.java` + `SchemaStateTest` |
| SCH-07 | Routing-key rule: every LOG table must have a non-null routing identity | [x] | `RoutingKeyRule.java` + `RoutingKeyRuleTest` + `SchemaComplianceFullSuiteTest` (incl. full-manifest routing identity — every LOG table carries `bucket.key`, every KV table its PK) + `CompatFlussIntegrationTest.compatFluss006BucketDistributionSkew` (COMPAT-FLUSS-006 live skew probe) |
| SCH-08 | Immutability protocol: same-id+same-hash=duplicate, same-id+diff-hash=violation | [x] | `ImmutabilityProtocol.java` + `ImmutabilityProtocolTest` + `SchemaComplianceFullSuiteTest` |
| SCH-09 | KV state update protocol: stale, regressive, conflict → UNKNOWN + halt signal | [x] | `KvStateUpdateProtocol.java` + `KvStateUpdateProtocolTest` + `SchemaComplianceFullSuiteTest` |
| SCH-10 | Schema evolution classes: all 5 classes (additive/behavioral/state-incompatible/wire-incompatible/breaking) defined and distinguishable | [x] | `SchemaEvolutionClass.java` |
| SCH-11 | EOD controller state machine: PENDING→WRITING→COMMITTED→VERIFYING→VERIFIED + FAILED_RETRYABLE/FAILED_MANUAL; `permitsSourceExpiry()` only true for VERIFIED; `requiresRetentionExtension()` for all non-VERIFIED; `isRetryable()` for FAILED_RETRYABLE | [x] | `EodControllerState.java` (7 states matching spec) + `EodControllerStateTest` (3 tests: expiry gating, retention extension, retryable discrimination) |

#### Phase C: Pinned-dialect validation (live dev cluster active since 2026-07)

| ID | Task | Status | Location / Evidence |
| --- | --- | --- | --- |
| SCH-12 | DDL parse + apply against pinned Fluss 0.9.1-incubating dialect | [x] | **Implemented 2026-08-15 (COMPAT-FLUSS-001)** — `CompatFlussDdlParityIntegrationTest` (common, env-gated on `FLUSS_BOOTSTRAP`): parses each of the 21 approved DDLs (columns/PK/bucket key/bucket num/TTL/datalake options) into an admin-API `TableDescriptor`, applies it to a unique scratch table on the live cluster, and inspects via `getTableInfo`. Fluss 0.9.1 has no SQL client, so "parse" is descriptor apply (documented). Live run 2026-08-15: all 21 tables apply + inspect green. Earlier ad-hoc evidence (ApplySignalDdl → ids 607/608; Recreate2d → 696–698) stands. |
| SCH-13 | Inspect effective schema/options from runtime → parity against logical requirements | [x] | **Implemented 2026-08-15 (COMPAT-FLUSS-001)** — the parity test asserts, per table: bucket count == `bucket.num`; column names/types (order) == DDL; KV → effective PK == DDL PK, LOG → no PK; bucket key == DDL `bucket.key`; `table.log.ttl` == DDL; datalake dev deviation recorded. `TableContractValidator` preflight + the 2026-08-13 ad-hoc inspections stand as corroboration. |
| SCH-14 | Changelog image behaviour: FULL changelog for KV tables with `partial_update` | [x] | **Implemented 2026-08-15 (COMPAT-FLUSS-003)** — `CompatFlussIntegrationTest.compatFluss003ChangelogFullImage` (live, env-gated): reads the KV changelog via LogScanner from offset 0 and asserts every record is a **FULL row image** (all columns populated) — full upserts AND `partial_update` writes merge to a FULL image at the storage layer (untouched columns preserved in the changelog record, not a column delta). 2 upserts + 1 partial update observed green. |
| SCH-15 | Partial-update merge semantics: column ownership, merge engine behaviour | [ ] | **Partial (2026-08-15)** — the storage-layer merge behaviour is observed (COMPAT-FLUSS-003 changelog FULL-image test: a `partial_update` merges at the storage layer and the changelog record carries the merged FULL image; untouched columns preserved). **Column-ownership core IMPLEMENTED (pure JVM)** — `ColumnOwnership` (DEC-005 matrix: identity columns creation-only, one declared writer per mutable column group, no unowned column, fail-closed `checkWrite`) + per-table matrices `OrderLifecycleColumnOwnership`/`PositionsColumnOwnership`/`ExecutionAttemptsColumnOwnership` over the new DDL pins `OrderLifecycleColumns`/`ExecutionAttemptsColumns` (Positions reuses `PositionsColumns`) + agreement tests (DDL parity, PK/schema_version identity, full coverage) — 49 ownership tests. **First consumer wired:** `InMemoryAttemptStore` (common, `com.trading.common.schema.execution`, EXE-UNIT-006 core) — `prepare()` replay never rewrites the PREPARED attempt's identity (duplicate returns existing; modified decision = contract violation + halt with no mutation), and the Task 5 transition rules are in: legal moves (PREPARED→SUBMITTING; SUBMITTING→ACCEPTED/REJECTED/CANCELLED/UNKNOWN), monotonic `phase_epoch` (+1 per applied move), stale-epoch rejection, terminal protection, UNKNOWN exits only via explicit `resolveUnknown` reconciliation — with the `checkWrite` guard running on every mutation (fails closed on matrix drift, proven) — +20 tests (323 common total). **Still open:** the production `partial_update` writer half — no live writer exists (depends on Executor-era writers; maps COMPAT-FLUSS-004). |
| SCH-16 | Cross-table visibility/atomicity limits documented with evidence | [x] | **Implemented 2026-08-15 (COMPAT-FLINK-002)** — `CompatFlussIntegrationTest.compatFlink002CrossTableVisibility` probe (live, env-gated) demonstrates per-table visibility: a write to table A is visible immediately while table B is still empty — there is no cross-table atomic commit; each append commits independently. Evidence file: `logs/schema-compat/compat-flink-002-20260815.md`. Consequence already engineered: multi-table consumers (Signal job LOG+KV dual sinks) reconcile partial visibility by ID (SIG-INT-002). |
| SCH-17 | Checkpoint + replay compatibility: connector checkpoint/restore + state savepoint/rescale | [x] | 2026-08-13 — `SignalJobSavepointRestoreIntegrationTest` (savepoint + 2× rescale, embedded MiniCluster, file:// savepoints) PASSES 1/0/0; `CandleGraphReplayIntegrationTest` + `CandleRocksDbRestoreIntegrationTest` green in the container battery; maps COMPAT-FLINK-001. |

> **Live dev cluster lake-state note (2026-08-13):** all ten recreated 2d-TTL
> tables were created with `table.datalake.enabled=false` while their DDLs
> declare `enabled=true`: `raw_table_1`(696), `feature_candles_15s`(697),
> `ingestion_quarantine`(698), `Order_Lifecycle`(699),
> `suspected_discontinuities`(700), `Postback_Quarantine`(701),
> ~~`Trade_Decisions`(702), `Ranking_Results`(703), `Portfolio_Reservations`(704)~~ — **REMOVED from scope 2026-08-15 (CHG-005)** (DDLs retained as reserved schema, still in `schema_manifest.json`),
> `Postback_Projection_Ledger`(705). Why: Fluss 0.9.1 `table.datalake.enabled`
> is create-only — enabling after create collides with orphaned R2 lake
> objects (`LakeTableAlreadyExistException` precedent: `candle_scale_log` drop
> 2026-08-12). Consequences for future work: (1) lake-tier reads of these
> tables return nothing — batch/validation probes must use log-scan reads
> (e.g. `CountRows`, `CandleKvInfo`), NOT the lake tier; (2) re-enabling the
> lake is a documented recovery (ZK registration patch + coordinator restart),
> not a routine alter — follow the toggle-recovery procedure if a phase needs
> the lake; (3) DDLs stay `enabled=true` (production blueprint); dev
> deliberately deviates. Evidence:
> `logs/tracker-14/ttl-live-recreate-2d-20260813.md` (batch 1) +
> `logs/tracker-14/ttl-live-recreate-2d-batch2-20260813.md` (batch 2).

> **raw_table_1 lake migration (2026-08-31, CHG-117):** the 2026-08-13 state
> above no longer holds for `raw_table_1` — it was dropped and recreated as
> **v3** (table id 76, 21 columns, `event_day` first, `PARTITIONED BY
> (event_day)`, auto-partition DAY, `table.datalake.enabled=true` at create),
> and R2 iceberg tiering is live with a day-partitioned lake layout
> (`.../data/event_day=<yyyyMMdd>/instrument_token_bucket=<N>/*.parquet`).
> The other nine tables keep the 2026-08-13 lake-disabled state. Partition
> keys are frozen at CREATE in Fluss 0.9.1 (no ALTER path) — any further
> partitioning change is a drop/recreate via `fluss-repair/RawTableAdmin.java`
> (archive the old lake prefix aside first or create collides with orphaned
> R2 objects — see the T-7 precedent). Fluss API notes verified in source:
> partition create is `ResolvedPartitionSpec.fromPartitionName(keys,name).toPartitionSpec()`
> (not `PartitionSpec.of`); iceberg partition key columns must be STRING;
> auto-partition DAY naming is `yyyyMMdd`; client dynamic partitioning
> (on by default) creates today's partition on first write. Ops surface and
> known failure modes: `06_operations/07-lake-archive-ops.md`.

#### Phase D: Runtime enforcement (straddles Ingestion, Signal, Executor phases)

| ID | Task | Status | Location / Evidence |
| --- | --- | --- | --- |
| SCH-18 | LOG append-only enforced: no silent update/delete; duplicates explicit | [x] | Fluss LOG table semantics + `RawTickWriter` per-tick append (Ingestion) |
| SCH-19 | Immutable instruction feed: existing identity may not change content | [x] | **IMPLEMENTED 2026-08-15** — pure-JVM core: `TradeDecisionsTableColumns` (25-col pin of `07_trade_decisions.sql` v2), `TradeDecisionBuilder` (deterministic `ins-v1-` instruction identity over the REQ-SS-004 executable set + canonical content hash over the complete REQ-FLS-008 request), `TradeInstructionFeedProtocol` (ACCEPTED / DUPLICATE / VIOLATION via `ImmutabilityProtocol`, VIOLATION → REQ-FLS-015 quarantine event + halt), `TradeDecisionIndexMapper` + `TradeDecisionsSinks` dual-sink helper (LOG append + instruction-state KV upsert, pinned UIDs), `TradeInstructionStateColumns` (4-col index pin), and the preflight checks `validateTradeDecisionsLogTable` / `validateTradeInstructionStateKvTable` wired into `SignalJob.preflightTableContracts` behind `TRADE_DECISIONS_ENABLED`; `25_trade_instruction_state.sql` manifest-listed. 21+ unit tests green (agreement pins, builder determinism/sensitivity, protocol outcomes, index mapper, config). **The ranking feed (winner selection, Slice 3) that would emit decisions was REMOVED from scope by decision (CHG-005, 2026-08-15 — not deferred)**; the dual-sink is complete and tested but stays gated off (`TRADE_DECISIONS_ENABLED=false` default) with no producer in scope |
| SCH-20 | KV projection version gating: stale/regressive writes rejected at projector | [ ] | **Pure core IMPLEMENTED 2026-08-15** — `com.trading.common.schema.position`: `PositionProjector` (version-gated fill projection over the Positions KV shape via `KvStateUpdateProtocol` — DUPLICATE no-op, STALE/REGRESSION/CONFLICT rejected, cycle-aware lifecycle FLAT→OPEN→REDUCING→CLOSED with legal-transition validation, oversell = violation), `PositionLifecycle`, `FillsColumns`/`PositionsColumns` DDL pins (08/10 v2) + agreement tests, 23 tests green (254 common total). **Operator-wiring core IMPLEMENTED 2026-08-15** — `FillEventMapper` (Fills row → `FillEvent` by pinned `FillsColumns` indexes; caller-resolved side/instrument via `FillContext`; `sourceVersion` pins to `receive_time` (non-authoritative monotone default); non-fill rows filtered) + `PositionProjectorDriver` (per-position version-gated projection, deterministic `position_id` minting per account/instrument/side, re-entry after CLOSED mints a NEW id, in-memory snapshot store, STALE/VIOLATION reported not swallowed) + `PositionsStateStore` SPI with `InMemoryPositionsStateStore` + `FlussPositionsStateStore` (raw-client upsert/point-lookup, single-field PK — COMPAT-FLUSS-005-safe) — 17 unit tests + the env-gated live drill (`FlussPositionsStateStoreIntegrationTest`) PASSED on the dev cluster (scratch Positions-shaped table: upsert+lookup round-trip + last-write-wins re-upsert, 2026-08-15). **Still open:** the live producer wiring — Action Capture must resolve `side` from the correlated instruction and tail the Fills changelog into the driver (depends on Action Capture / broker postback evidence) |
| SCH-21 | Gate/attempt epoch+phase transition validation: stale epoch rejected, illegal transition blocked, UNKNOWN→HALTED | [x] | `GateTransitionValidator.java` — legal transition matrices for GateState + AttemptPhase |
| SCH-22 | Manifest enforcement: one approved manifest version defines active subscription state (version + count + fingerprint validation) | [x] | `InstrumentManifestLoader.java` — `loadDefault()` returns `ManifestResult(approved, version, count, fingerprint)`; `isManifestApproved()` validates version/count/fingerprint against expected; synthetic fallback only when `ALLOW_SYNTHETIC_MANIFEST=true`; `IngestionService.java` logs manifest metadata + exits on empty load |

#### Phase E: Retention and offload (post-MVP / deferred)

| ID | Task | Status | Location / Evidence |
| --- | --- | --- | --- |
| SCH-23 | Retention extension executable: EOD controller extends live retention for unverified days | [ ] | **Controller service IMPLEMENTED 2026-08-15** — `com.trading.common.schema.eod`: the pure-JVM core (`EodOffloadRecord` 15-field record + validated state machine, `EodBackoff`, `EodRetentionPolicy` incl. `parseTtl`, `EodPlanner`) now driven by the **`EodControllerTool` CLI** (`status`/`run`/`extend`/`reconcile`/`reset`, ddl-apply-style RESULT sentinel, exit 0/1/2/3/4/5) over `EodController` (planning, due-day selection, offload drive with crash-resume re-drive, lease-gated single-writer fencing) + `EodStateStore`/`FlussEodStateStore` (raw client, upsert + current-state scan) + the `EodOffloadExecutor` SPI (shipped default `NotConfiguredEodOffloadExecutor` / `MockEodOffloadExecutor`). **Runner wiring done 2026-08-15** — `make eod-controller`, `eod_controller.py` launcher, `docker-compose.yml` `eod-controller` one-shot service + 47 tests. **Still open:** the live extend drill against the dev cluster and the real offload SPI wiring. |
| SCH-24 | Lake offload / encrypted export for policy-controlled audit reconstruction (one-year minimum target) | [x] | **Pure-JVM core IMPLEMENTED 2026-08-15 (CHG-005)** — `EncryptedExportEodOffloadExecutor` provides immutable, key-versioned export bundles and source-hash verification; the R2 push rides the existing `audit_r2.py` tooling. Live tiered-storage integration and policy reconstruction remain evidence-gated. |
| SCH-25 | Clean-break reset + replay: destructive approval, state reset, full replay from broker/log | [ ] | **Drill machinery IMPLEMENTED 2026-08-15** — `CleanBreakSimulation` (pure JVM, 11 tests: full replay reconverges; partial replay and a mutated source diverge and fail closed — the immutable-LOG guarantee is load-bearing; wave 32 added list-order independence via (offset, sourceVersion), separator-safe table keys, fail-closed null handling and an `immutabilityVerified` flag on the result, CHG-172) + `clean_break_drill.py` (approval-gated runner: plan file + `--approve` required, `--dry-run`, evidence record, exit 3 refusal without approval). **Still open:** the live pre-production execution against a running cluster (drop via `make ddl APPLY=1`, restart the Signal/Executor jobs, post-replay parity check — runbook procedure, needs all services) |
#### Test mapping

Each SCH task maps to a test ID from `11-testing-and-release.md`:

| SCH | Test IDs |
| --- | --- |
| SCH-05 | SCHEMA-UNIT-001 |
| SCH-07 | SCHEMA-UNIT-002, COMPAT-FLUSS-006 |
| SCH-08, SCH-09 | SCHEMA-UNIT-003 |
| SCH-12, SCH-13 | COMPAT-FLUSS-001 |
| SCH-14 | COMPAT-FLUSS-003 |
| SCH-15 | COMPAT-FLUSS-004 |
| SCH-17 | COMPAT-FLINK-001 |
| SCH-18, SCH-21, SCH-25 | SCHEMA-REC-001 |
| SCH-23 | SCHEMA-EOD-001 |

### Completion checklist

- [x] Schema manifest format is implemented. ✓ `SchemaManifest.java` + `SchemaManifestEntry.java` (SCH-05)
- [x] All DDLs have checksums and compatibility classes. ✓ `schema_manifest.json` (27 tables as of 2026-08-24 — 26 at 2026-08-15 + Position_State CHG-087 — 21 at authoring, +`fingerprint_dedup` +`trade_instruction_state` +`eod_offload_state`, SHA-256 per file), `ddl_apply.py` validates checksums (SCH-01, SCH-02)
- [x] Stale DDL paths are removed from application workflow. ✓ `ddl_apply.py` manifest drift check runs clean; stale duplicates removed (SCH-03)
- [x] Pinned dialect tests pass. ✓ COMPAT-FLUSS-001 (all 27 DDLs apply + inspect historical 2026-08-24 with manifest parity — the parity test parses the full manifest, 26 DDLs after the 2026-08-15 regen), COMPAT-FLUSS-003 (LOG/KV/changelog behavior + FULL changelog images), COMPAT-FLUSS-006 (bucket-distribution skew probe — distinct keys spread across every bucket within mean+3σ, constant-key control collapses to one bucket; the live half of the non-null-routing/bucket-skew requirement, SCH-07), COMPAT-FLINK-002 (cross-table visibility evidence) implemented 2026-08-15 as env-gated live tests against Fluss 0.9.1-incubating; COMPAT-FLINK-001 checkpoint/restore verified earlier via `SignalJobSavepointRestoreIntegrationTest` (SCH-17). Live run 2026-08-15: 177 common tests / 0 failures / 1 skip historical 2026-08-15 with `FLUSS_BOOTSTRAP=localhost:9123` — **superseded**: that run predates the SCH-15/SCH-20/SCH-24 additions, and the default-run totals as of 2026-08-24 are 464 common / 0 failures / 1 skip historical 2026-08-24 (CHG-003/005/006/007; docs-audit C6 line 464/247/387 (compute 387 — the 2026-08-17 Design-B merge `34af190` −19 DEC-038-era tests, then −10 CHG-023 item-1 emitter→native-reporter swap, then −2 CHG-023 item-2 native-TTL expiry swap, then −11 CHG-023 item-4 StallGuardedSink removal 2026-08-17, then +1 SIG-FAIL-001 checkpoint-failure test `SignalJobCheckpointFailureIntegrationTest` 2026-08-17; counts corrected 2026-08-16 — were 193/268))
- [x] Every table has a non-null routing strategy. ✓ `RoutingKeyRule.java` + all 27 DDLs have `bucket.key` historical 2026-08-24 or `PRIMARY KEY` (SCH-07)
- [x] Immutability and stale-update protocols are implemented and tested. ✓ `ImmutabilityProtocol.java` + `KvStateUpdateProtocol.java` + unit tests (SCH-08, SCH-09); SCH-19's `TradeInstructionFeedProtocol` reuses `ImmutabilityProtocol` (ACCEPTED/DUPLICATE/VIOLATION + quarantine/halt on violation)
- [x] Retention extension is executable, not just documented. _(controller service done 2026-08-15 — `EodControllerTool` status/run/extend/reconcile/reset + `26_eod_offload_state.sql` durable state + `extend --apply` rewrite drill + 59 Eod tests; runner wiring done 2026-08-15 — `make eod-controller`, `eod_controller.py` launcher, compose `eod-controller` service; the live extend drill against the dev cluster and the live offload SPI wiring remain — SCH-24's live lake half is out of scope (CHG-005); the `EncryptedExportEodOffloadExecutor` core is the real SPI, ready to wire)_→ live extend drills 2026-08-24: `Portfolio_Reservations__eod_ext_20260824` ttl 32d rows 1/1 parity 5s (CHG-098); `feature_candles_15s__eod_ext_20260824` ttl 37d rows=385,389 parity 385,389/385,389 in ~16s ≈24k rows/s — parallel bucket copy + drain-loop (CHG-099; 1,700× vs per-row `.get()` ~9.6 rows/s; pollBatch returns time-bounded batches — single pollBatch under 16-way contention yielded only ~6.8% of rows). Evidence `logs/eod-retention-extend-20260824T171204Z.md` + `logs/eod-retention-extend-20260824T181743Z.md` (CHG-099). **Remaining:** live offload SPI wiring (CHG-005 scope); Flink batch-copy job NOT viable on lake-disabled tables in fluss 0.9.1-incubating (enumerator throws `Batch only supports when table option 'table.datalake.enabled' is set to true`) — revisit if lake-enabled tables exist._ live extend drill ran 2026-08-24 (CHG-098): `Portfolio_Reservations__eod_ext_20260824` created ttl=32d rows 1/1 parity 5s (PASS — small PK table); `feature_candles_15s` shadow created ttl=37d but copy partial 1738/385389 rows — per-row UpsertWriter copy ~10 rows/s, 180s bound exceeded (large-table rewrite NOT viable yet — needs bulk scan/batch copy; finding recorded); shadow-name bug fixed (BASIC_ISO_DATE on ZonedDateTime emitted +0530 → invalid alias; `toLocalDate()` fix, 62 Eod tests green). Live offload SPI wiring + large-table bulk copy still open. Evidence `logs/eod-retention-extend-20260824T171204Z.md`_
- [ ] Clean-break reset and full replay evidence exist. _(drill machinery done 2026-08-15, hardened in wave 32 — `CleanBreakSimulation` pure JVM 11 tests + `clean_break_drill.py` approval-gated runner; live pre-production drop via `make ddl APPLY=1`, restart of Signal/Executor jobs, and post-replay parity check remain)_
## Verification mapping

The required behavior above is verified by the canonical [Schema and storage test design](./11-testing-and-release.md#schema-and-storage): `SCHEMA-UNIT-001` to `SCHEMA-UNIT-003`, `COMPAT-FLUSS-001` to `COMPAT-FLUSS-004`, `SCHEMA-REC-001`, and `SCHEMA-EOD-001`.
