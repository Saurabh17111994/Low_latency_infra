# 04 — Data Requirements

## 4.1 Authority and migration posture

Physical schemas SHALL be generated/reconciled from these logical contracts into `code/01_platform/02_sql/ddl/`. The platform is pre-production; stale schemas may be replaced as a clean break. Until DDLs and connector tests match this document, schema readiness is **blocked**.

Every table records a schema version. Event tables are immutable; corrections append superseding records. State tables use source event/version checks and explicit ownership.

## Constraints

- Every table SHALL record an explicit schema version. A table without a schema version is not ready for physical DDL generation or implementation.
- Event/audit tables SHALL be immutable. Corrections append new superseding records; in-place mutation of an existing event row is prohibited.
- State tables SHALL use source event/version checks and explicit writer ownership. Stale, out-of-order, or cross-writer updates SHALL be rejected or move state to `UNKNOWN`.
- An overloaded `order_id` SHALL NOT be used as a generic cross-domain identity. Each domain uses its own identity from the 14-entry identity model.
- All monetary price values SHALL be stored as integer paise (BIGINT). ₹1 = 100 paise. Floating-point (DOUBLE/FLOAT) SHALL NOT be used for financial values. Conversion to decimal rupees occurs only at display/API boundaries.
- No source data SHALL expire before verified EOD offload plus the minimum three-day live buffer. Retention SHALL extend automatically while the manifest is unverified, retryable, or under reconciliation.
- Money-moving audit records (`Execution_Audit`, `Fills`, postback/fill audit events) SHALL be immutable, encrypted at rest in the lake tier, integrity-verifiable, access-controlled, and retained for at least one year or longer under the approved retention policy.
- Deletion before the approved retention period SHALL require an approved policy change, legal-hold release where applicable, and single-operator (Saurabh, DEC-044) authorization recorded as immutable deletion-evidence events.
- All state tables SHALL be rebuildable from immutable events/audit or have a documented, tested backup/restore contract.
- Physical schemas SHALL be generated/reconciled from these logical contracts. A DDL that contradicts this document is blocked until the contract is reconciled.
- LOG tables SHOULD use a tested `bucket.key` aligned with their dominant identity. KV tables SHALL distribute by primary key. Bucket counts SHALL be workload-tested configuration, not copied assumptions.
- Bucket affinity SHALL NOT be described as establishing global order, exact event identity, or cross-table atomicity. State projections enforce source-version/precedence rules independently.

## Assumptions

| ID | Assumption | Source |
| --- | --- | --- |
| ASM-DATA-001 | The pre-production clean break permits replacing all stale physical DDLs without preserving compatibility with old consumers. | RISK-011 |
| ASM-DATA-002 | The selected Fluss version supports BYTES payload, KV state tables, changelog images, partial-update merge semantics, three-node replication, retention extension, and lake tiering properties as specified. | ASM-008 |
| ASM-DATA-003 | Fluss connector atomic visibility semantics are per-sink, not cross-sink. Consumers can tolerate partial visibility when reading multiple LOG and KV tables. | RISK-008 |
| ASM-DATA-004 | S3 `ap-south-1` can complete verified EOD offload of a full trading day's data within 30 minutes, and Iceberg table format is compatible with the audit retention and export requirements. | ASM-006 |
| ASM-DATA-005 | Fingerprint collisions and identical-legitimate-event collapses remain within the measured and accepted rate under production workload. Fingerprints are not exact identity. | RISK-001 |
| ASM-DATA-006 | Arrow postbacks expose `broker_order_id`, lifecycle status, and the submitted `remarks` value for correlation into `Order_Correlation`. | ASM-002 |
| ASM-DATA-007 | The approved audit-retention policy, currently at least one year, with encrypted lake storage is acceptable for the applicable live-money jurisdiction and account model. | ASM-010 |

Assumptions are validated by the owner and method recorded in the project risks and assumptions register (`docs/01_project/05-risks-and-assumptions.md`). An invalidated assumption blocks the affected requirement.

## Accepted Behaviors

These behaviors are conscious trade-offs accepted by the platform:

- **Pre-production clean break:** All physical DDLs may be replaced. Stale schemas, old table definitions, and incompatible old consumer compatibility are not preserved.
- **Immutable event tables with superseding corrections:** Event rows are never updated. Corrections append new records with explicit supersession relations. This preserves complete audit history at the cost of requiring consumers to resolve the latest authoritative record.
- **At-least-once LOG delivery:** LOG tables guarantee at-least-once append. Duplicate event rows may exist. Exact deduplication is owned by specific producers, not the storage layer.
- **KV projections are rebuildable, not permanently retained:** Operational projections (`Order_Lifecycle`, `Positions`) may have shorter live retention than their source audit logs, provided the source audit enables complete rebuild.
- **Partial cross-table visibility:** A single Flink checkpoint commits source offsets and sinks, but atomic visibility across multiple LOG and KV tables is not assumed. Consumers tolerate partial visibility and reconcile using stable IDs and version checks.
- **Best-effort fingerprint identity:** `event_fingerprint` and `postback_fingerprint` are versioned best-effort identifiers. They may collapse identical legitimate events or miss semantically duplicate packets. They are not broker-global identity.
- **`order_id` is prohibited:** Every domain uses its own identity (`instruction_id`, `broker_order_id`, `execution_attempt_id`, `position_id`, `postback_event_id`, `candidate_id`). A single overloaded identifier is not used across components.
- **Three legacy table names are deliberate exceptions:** `raw_table_1`, `suspected_discontinuities`, and `instruments` deviate from Pascal_Snake_Case. They may only be renamed through an approved schema migration with full consumer impact analysis.

## Out of Scope

The following capabilities are explicitly NOT owned by the Data Requirements layer:

- **Physical DDL generation, application, and schema lifecycle management:** Owned by the schema lifecycle process (`docs/08_implementation/01-foundation.md`). This document defines the logical contract; tooling applies it.
- **Actual broker protocol field definitions, packet schemas, and decoder versions:** Evidence-gated. Owned by the broker protocol evidence and the Ingestion implementation dossier.
- **Candle computation, signal detection, strategy evaluation:** Owned by the Signal Flink job. (**Ranking logic REMOVED 2026-08-15, CHG-005.**)
- **Broker order submission, execution, gate management, and reconciliation:** Owned by the Executor.
- **Postback capture, fill lifecycle, position projection logic, and quarantine disposition:** Owned by Action Capture.
- **EOD controller orchestration and manifest creation:** Owned by the EOD controller. This document defines retention and expiry requirements; the controller drives the process.
- **Observability, alerting, and dashboard configuration:** Owned by the observability layer.
- **Kubernetes deployment:** Deferred. Production is Docker Swarm.

## 4.2 Identity model

| Identity               | Scope                                       | Required relationship                                      |
| ---------------------- | ------------------------------------------- | ---------------------------------------------------------- |
| `candidate_id`         | One detected setup/audit record             | May produce zero or one `instruction_id`                   |
| `instruction_id`       | One immutable execution request             | May have multiple attempts; executable fields never mutate |
| `execution_attempt_id` | One submission attempt                      | Belongs to one instruction or position action              |
| `client_order_ref`     | Broker-facing attempt reference             | Maps durably to attempt/instruction                        |
| `broker_order_id`      | Broker-authoritative order                  | Maps to attempt/instruction when correlated                |
| `trade_context_id`     | Entry and related position-management chain | Groups one or more broker orders/positions                 |
| `position_id`          | One exposure aggregate                      | Minted on first correlated fill                            |
| `postback_event_id`    | One platform-captured delivery              | May be logically duplicate by fingerprint                  |
| `action_id`            | One immutable structured position action    | Future phase; maps to attempts like an instruction         |
| `account_scope_id`     | Broker/account isolation boundary            | Scopes gates, mappings, positions, attempts, and audit     |
| ~~`portfolio_id`~~         | ~~Ranking and capacity boundary~~                 | ~~Scopes reservations~~ — **REMOVED 2026-08-15 (CHG-005)** |
| `execution_partition_id` | Fenced Executor ownership boundary          | Scopes one active Executor owner and fencing token          |
| ~~`reservation_id`~~       | ~~One portfolio capacity reservation~~            | ~~Maps candidate/instruction to a versioned reservation~~ — **REMOVED 2026-08-15 (CHG-005)** |
| `halt_request_id`      | One durable safety-halt request               | Idempotently maps detected uncertainty to a gate action    |

An overloaded `order_id` is prohibited.

## 4.3 Table ownership and retention

| Table                       | Type           | Writer               | Live retention                                            | Lake/audit                             |
| --------------------------- | -------------- | -------------------- | --------------------------------------------------------- | -------------------------------------- |
| `raw_table_1`               | LOG            | Ingestion            | ≤7 complete trading days (ceiling); extend while offload unverified | EOD Iceberg                            |
| `feature_candles_15s`       | KV (PK `(instrument_token, window_start)` — 2026-08-13 conversion; sole candle output) | Signal job           | ≤7 complete trading days (ceiling); extend while offload unverified | EOD Iceberg                            |
| `forming_bar`               | KV (PK `instrument_token`) | Signal job           | Current state only (Slice 2.2 consumer — DEC-038 durable home) | Rebuilt from raw_table_1 replay        |
| `Signal_Candidates`         | LOG            | Signal job           | ≤7 complete trading days                                  | EOD Iceberg                            |
| `Signal_Candidates_current` | KV             | Signal job           | Current state plus rebuild window                         | Rebuilt from LOG audit                |
| `Ranking_Results`           | ~~LOG~~            | ~~Signal job~~           | ~~≤7 complete trading days~~                                  | ~~EOD Iceberg~~ — **REMOVED 2026-08-15 (CHG-005)** |
| `Trade_Decisions`           | ~~immutable feed~~ | ~~Signal job~~           | ~~Until consumed plus replay buffer~~          | ~~Execution audit links~~ — **REMOVED 2026-08-15 (CHG-005)** |
| `Fills`               | LOG            | Action Capture       | ≥3 complete trading days                                  | Encrypted audit under approved policy  |
| `Order_Lifecycle`           | KV             | Action Capture       | Current state plus rebuild window                         | Rebuilt from audit                     |
| `Positions`                 | KV             | Position projector   | Current state plus rebuild window                         | Rebuilt from audit                     |
| `Execution_Gate`            | KV             | Executor             | Current plus history in audit                             | Encrypted audit under approved policy  |
| `Execution_Attempts`        | KV             | Executor             | Active/reconciliation window                              | Encrypted audit under approved policy  |
| `Order_Correlation`         | KV             | Executor             | Active/reconciliation window                              | Encrypted audit under approved policy  |
| `Execution_Audit`           | LOG            | Executor             | ≥3 complete trading days                                  | Encrypted audit under approved policy  |
| `Position_Actions`          | LOG            | Babysitter after MVP | Replay/reconciliation buffer                              | Encrypted audit under approved policy  |
| `Postback_Quarantine`       | LOG            | Action Capture       | Until disposition plus buffer                             | Encrypted evidence per policy          |
| `Portfolio_Reservations`    | ~~KV/logical state~~ | ~~Signal job~~          | ~~Active plus rebuild window~~                 | ~~Reservation audit/rebuild evidence~~ — **REMOVED 2026-08-15 (CHG-005)** |
| `Postback_Projection_Ledger` | KV            | Action Capture       | Incomplete plus recovery/disposition window                | Rebuilt/reconciled from postback audit |
| `Safety_Halt_Requests`      | KV              | Authorized components | Safety/reconciliation window                             | Execution audit under approved policy  |
| `suspected_discontinuities` | LOG            | Ingestion            | Operational investigation window                          | Optional operational lake retention    |
| `ingestion_quarantine`      | LOG            | Ingestion            | Operational investigation window (2d TTL)                 | Optional operational lake retention    |
| `instruments`               | manifest       | Operators            | Current and prior manifest versions                       | Configuration audit                    |

### 4.3.1 Naming convention

Logical table names use Pascal_Snake_Case (e.g. ~~`Trade_Decisions`~~ — REMOVED 2026-08-15 CHG-005, `Order_Lifecycle`, `Postback_Projection_Ledger`). The following legacy names are excepted because they serve as stable identifiers for early-stage tooling and cross-document references: `raw_table_1`, `suspected_discontinuities`, `instruments`. These exceptions are deliberate and may only be renamed through an approved schema migration with full consumer impact analysis.

## 4.4 Core logical schemas

### `raw_table_1`

Required fields: event/ingest/ack timestamps, instrument/routing data, verified typed trade/depth data, `connection_id`, `connection_epoch`, `event_fingerprint`, `fingerprint_version`, original packet bytes, payload hash, decoder/protocol version, validity state/reason, and schema version. Broker sequence is not required.

### `feature_candles_15s`

Required fields: instrument, window start/end, OHLCV, tick count, algorithm/configuration version, output timestamp, and schema version. One final row per non-empty accepted window; no MVP correction rows.

### `Signal_Candidates`

Required fields: `candidate_id`, nullable `instruction_id`, nullable `trade_context_id`, instrument, strategy/rule/version, event/evaluation timestamps, action/request fields, score inputs, formation snapshot reference, validity/detection reason, supersession relation, and schema version.

### `Signal_Candidates_current`

Current-state KV projection of the signal stream, keyed by `(instrument_token)`: one row per instrument holding the latest/active candidate (same field set as the LOG row, plus supersession relation); supersession overwrites in place. Rebuildable from the immutable `Signal_Candidates` LOG.

### `Ranking_Results` — REMOVED (CHG-005, 2026-08-15)

**REMOVED from scope 2026-08-15 (CHG-005, not deferred).** No ranking evidence records in the current system.

### `Trade_Decisions` — REMOVED (CHG-005, 2026-08-15)

**REMOVED from scope 2026-08-15 (CHG-005, not deferred).** No immutable instruction feed in the current system.

### `Fills`

Required fields: `postback_event_id`, fingerprint/version, correlated IDs when available, broker status, verified cumulative/pending/fill quantities and prices, broker/receive/ingest timestamps, original payload and hash, correlation state/reason, decoder/schema version. Duplicate deliveries may exist as separate immutable events.

### `Order_Lifecycle`

Key: `broker_order_id`. Fields: correlated IDs, normalized state, cumulative/pending quantities, average fill price, source event/version/timestamps, correlation state, and schema version. Stale/regressive updates are rejected or move state to `UNKNOWN`.

### `Positions`

Key: `position_id`. Fields: `trade_context_id`, instrument, side, open/closed/current quantities, average entry/exit values, state (`FLAT`, `OPEN`, `REDUCING`, `CLOSED`, `UNKNOWN`), source event/version, timestamps, and schema version.

### Execution state

- `Execution_Gate`: scope key, state, epoch, reason, detection time, evidence hash, single-operator (Saurabh, DEC-044) approval (a second approval is optional, not required), transition timestamp, schema version.
- `Execution_Attempts`: attempt/instruction/action IDs, immutable request hash, client reference, gate epoch, phase/outcome, timestamps, retry eligibility, broker response summary, schema version.
- `Order_Correlation`: instruction/attempt/client/broker/trade/position IDs, verification state/evidence, timestamps, schema version.
- `Execution_Audit`: immutable event ID/type, all relevant IDs, gate epoch, actor/service identity, evidence hash/summary, timestamp, schema version.
- ~~`Portfolio_Reservations`: reservation ID, account/portfolio scope, candidate/instruction IDs, capacity class, state, transition version, source evidence, expiry, timestamps, schema version~~ — **REMOVED 2026-08-15 (CHG-005).**
- `Postback_Projection_Ledger`: postback event ID, projection state, expected prior state, retry/error/disposition, step timestamps, schema version.
- `Safety_Halt_Requests`: halt request ID, account/portfolio/execution scope, source, reason, detection time, source epoch/version, evidence hash, application result, schema version.

### Future `Position_Actions`

Required fields are the structured action contract in `02-functional/05-babysitter.md`. The table exists only when post-MVP behavior is approved; free-form command strings are prohibited.

## 4.5 Distribution and ordering

LOG tables SHOULD use a tested `bucket.key` aligned with their dominant identity (`instrument_token`, `broker_order_id`, `instruction_id`, or `position_id`). KV tables distribute by primary key. Bucket counts are workload-tested configuration, not copied assumptions.

Bucket affinity does not establish global order or exact event identity. State projections enforce source-version/precedence rules.

## 4.6 EOD manifest and expiry

Each EOD commit produces a manifest with trading date, table/schema version, source range, object/row/byte counts, hashes/checksums, commit identifier, verification state, and retries. Source data for that date cannot expire until verification succeeds. Minimum live retention remains three complete trading days even after success.

## 4.7 Evolution and recovery

## 4.8 Storage capacity budget (`STOR-7DAY-60000-001`)

Seven-day Fluss retention is the ceiling for the operational source buffer, subject to measured capacity. Actual data volume at the variable 50,000 ticks/s average baseline (3,000 instruments, no instrument above 20 ticks/s; the 90,000 ticks/s peak model is retired, DEC-036; 60,000 ticks/s gate, DEC-045) for seven complete trading days is evidence-gated. The storage capacity formula SHALL account for:

- Raw data volume (packet bytes + typed fields + overhead per row)
- Three-node replication factor (3× raw data)
- Fluss overhead (segments, indices, metadata, compaction workspace)
- Flink checkpoints/savepoints on S3 (separate from local SSD)
- Local durable buffer if selected (slow-Fluss policy, choice 1)
- Named free-space reserve (minimum 20% of SSD for operational headroom)

Warning, critical, and stop thresholds SHALL be defined as configuration placeholders to be filled by capacity evidence. S3 availability is not assumed: EOD offload failure retains data locally, retries offload, alerts operators, and prevents premature expiry. No data SHALL expire while EOD offload is unverified — this invariant overrides the seven-day ceiling.

| Evidence ID | Purpose | Status |
| --- | --- | --- |
| `STOR-7DAY-60000-001` | Seven-day capacity model from 50,000 ticks/s workload measuring projected data volume with replication, checkpoints, buffer, and free-space reserve | `EVIDENCE-BLOCKED`; live-money blocking |
| `STOR-7DAY-90000-001` | RETIRED with the peak campaign (DEC-036, 2026-08-13) — was: seven-day capacity model from the 90,000 ticks/s peak | `RETIRED`; storage model uses the 60,000 ticks/s gate (DEC-045) / 50,000 ticks/s baseline |

All state tables must be rebuildable from immutable events/audit or have a documented backup/restore contract. Schema evolution requires compatibility classification, replay test, lake synchronization, deployment order, and rollback. Audit deletion requires an approved retention-policy change, applicable legal-hold handling, and immutable deletion evidence.
