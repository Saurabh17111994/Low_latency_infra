# 02.2 — Storage

## Purpose and authority

Fluss is the live streaming bus and operational storage layer. This requirement owns the logical schema, table ownership, distribution, retention, replication, changelog behavior, and EOD lake policy. Physical DDLs under `code/01_platform/02_sql/ddl/` SHALL be reconciled to this document before implementation.

No table definition may use an overloaded `order_id`, assume broker sequence IDs, or claim an atomic cross-table transaction without a pinned connector test.

## Constraints

- Fluss production topology SHALL use three-node replication/quorum across three workload VMs. Replica placement SHALL prevent co-location of a table's replicas on a single VM.
- `order_id` SHALL NOT be used as a generic cross-domain identity. Each domain uses its own identity: `instruction_id`, `broker_order_id`, `execution_attempt_id`, `position_id`, `postback_event_id`, `candidate_id`.
- `seq_no` is not a required column in any table. If retained for future evidence, it is nullable observational data and SHALL NOT support ordering, deduplication, or completeness guarantees.
- `speculated` or exact gap-range columns SHALL NOT exist in quarantine or discontinuity tables unless broker protocol evidence proves the required sequence semantics.
- Quarantine tables SHALL NOT be silently discarded and SHALL NOT become executable state for any downstream component.
- Source retention SHALL NOT expire a trading day's data while its EOD manifest is unverified, retryable, or under reconciliation.
- Money-moving audit records (`Execution_Audit`, `Fills`, postback/fill audit events) SHALL be immutable, encrypted, integrity-verifiable, access-controlled, and retained for at least one year or longer under the approved policy.
- Every logical table SHALL carry an explicit schema version and one writer owner. A table with no declared owner is not ready for implementation.
- Partial-update merge semantics SHALL NOT be used across column groups where writers are not the declared and tested owner of every updated column.
- Atomic cross-table visibility SHALL NOT be claimed without a version-pinned connector test that proves the specific behavior.

## Assumptions

| ID | Assumption | Source |
| --- | --- | --- |
| ASM-STOR-001 | Fluss `partial_update` and FULL changelog behavior match the pinned server/client version, and stale/out-of-order updates are rejected. | ASM-004 |
| ASM-STOR-002 | The selected Fluss version supports BYTES payload, KV state tables, changelog images, three-node replication, retention extension, and lake tiering properties as specified in the DDLs. | ASM-008 |
| ASM-STOR-003 | The production Swarm workload VMs (v1: 3 Manager+Worker; v2: N≥3 Worker VMs) can sustain the normal production baseline of 50,000 ticks/s variable average baseline (3,000 instruments; ≈16.7 ticks/s/instrument average) with three-node Fluss replication/quorum while one HA VM is unavailable. | ASM-005, RISK-010 |
| ASM-STOR-004 | S3 `ap-south-1` can complete verified EOD offload of a full trading day's data within 30 minutes. | ASM-006 |
| ASM-STOR-005 | Docker Swarm encrypted overlay, TLS, and S3 checkpoint storage operate within the four-VM target. | ASM-009 |
| ASM-STOR-006 | Fluss connector atomic visibility semantics are per-sink, not cross-sink. Consumers can tolerate partial visibility when reading multiple LOG and KV tables from the same checkpoint boundary. | RISK-008 |
| ASM-STOR-007 | The pre-production clean break permits replacing all stale physical DDLs without preserving compatibility with old consumers. | RISK-011 |

Assumptions are validated by the owner and method recorded in the project risks and assumptions register (`docs/01_project/05-risks-and-assumptions.md`). An invalidated assumption blocks the affected requirement.

## Accepted Behaviors

These behaviors are conscious trade-offs accepted by the platform:

- **Pre-production clean break:** All physical DDLs may be replaced. Stale schemas, incompatible old consumer compatibility, and untested table definitions are not preserved.
- **At-least-once LOG delivery:** LOG tables guarantee at-least-once append. Duplicate event rows may exist. Stronger deduplication is owned by the specific producer, not the storage layer.
- **Late candles are discarded in MVP:** `feature_candles_15s` emits one final row per non-empty window. Records arriving after `window_end + allowed_lateness` are discarded and measured. Corrections to already-emitted candles are not written.
- ~~**Immutable instruction feed is Signal-owned:** `Trade_Decisions`…~~ — **REMOVED 2026-08-15 (CHG-005 — decision feed out of scope).** Executor-owned state (`Execution_Attempts`, `Order_Correlation`) is separate.
- **Operational projections are rebuildable, not permanently retained:** `Order_Lifecycle`, `Positions`, and other KV operational projections may have shorter live retention than their source audit logs, provided the source audit (`Fills`, `Execution_Audit`) enables complete rebuild.
- **Partial-update KV is writer-colocated:** KV tables using `partial_update` assign every column group to one declared writer. Cross-writer, untested updates are rejected by stale-version guards.
- **EOD offload blocks expiry:** Source retention for a trading day is extended automatically while the corresponding Iceberg manifest is unverified or retryable. The minimum live buffer is three complete trading days even after successful offload.

## Out of Scope

The following capabilities are explicitly NOT owned by Storage:

- **Market data ingestion and broker connection management:** Owned by Ingestion.
- **Candle computation, signal detection, strategy evaluation:** Owned by the Signal Flink job. (**Ranking REMOVED 2026-08-15, CHG-005.**)
- **Broker order submission, execution, and Arrow REST integration:** Owned by the Executor.
- **Postback capture, fill lifecycle, and position projection logic:** Owned by Action Capture.
- **Babysitter position monitoring and action emission:** Owned by the Babysitter Flink job.
- **EOD controller orchestration and manifest creation:** Owned by the EOD controller. Storage provides the data; the controller drives the offload process.
- **Schema migration tooling and DDL application:** Owned by the schema lifecycle process (`docs/08_implementation/01-foundation.md`). Storage defines compatibility requirements; tooling applies them.
- **Observability, alerting, and dashboard configuration:** Owned by the observability layer and operations.
- **Multi-broker support:** Deferred; not in MVP scope.
- **Kubernetes deployment:** Deferred. Production is Docker Swarm.
- **Automatic live gap backfill:** Deferred; not in MVP scope.

## REQ-FLS-001: Production storage posture

Production Fluss SHALL use three-node replication/quorum across the three workload VMs, with replica placement preventing co-location of a table's replicas on one VM. Loss of any one workload VM SHALL be tested at the normal workload.

Fluss metadata, tablet data, and replication configuration SHALL be version-pinned. A topology that cannot prove one-VM tolerance is not production-ready.

## REQ-FLS-002: Logical table classes

| Class               | Role                                   | Guarantee                                                                                            |
| ------------------- | -------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| LOG                 | Immutable event/audit append           | At-least-once unless a specific producer/dedup test proves stronger behavior                         |
| KV                  | Current materialized operational state | Idempotent projection under versioned source events; partial-update only with tested merge semantics |
| Execution audit LOG | Money-moving evidence                  | Immutable, encrypted lake retention for at least one year under approved policy                     |

## REQ-FLS-003: Required logical tables

| Table                       | Type                                  | Owner                       | Purpose                                      |
| --------------------------- | ------------------------------------- | --------------------------- | -------------------------------------------- |
| `raw_table_1`               | LOG                                   | Ingestion                   | Original bytes plus normalized market ticks  |
| `feature_candles_15s`       | KV                                    | Signal job                  | Final MVP candles (KV upsert PK `(instrument_token, window_start)`, sole candle output — 2026-08-13 conversion; authoritative durable candle state) |
| `feature_candles_15s_current` | KV (RETIRED 2026-08-13)            | Signal job                  | Was canonical candle projection PK `(instrument_token, window_start)`; `feature_candles_15s` is now the sole KV-only candle output (2026-08-13), projection dropped, live-table teardown pending |
| `Signal_Candidates`         | LOG (v3; KV v2 R-084 reversed)       | Business Logic              | Immutable append-only candidate audit; one row per fired signal |
| `Signal_Candidates_current` | KV                                    | Business Logic              | Current-state projection, PK `(instrument_token)`; latest/active candidate per instrument, supersession overwrites in place |
| `Ranking_Results`           | ~~LOG~~                                   | ~~Signal job ranking operator~~ | ~~Immutable score/selection audit~~ — **REMOVED 2026-08-15 (CHG-005)** |
| `Trade_Decisions`           | ~~LOG (DECIDED)~~                         | ~~Signal job~~                  | ~~Immutable instructions~~ — **REMOVED 2026-08-15 (CHG-005)** |
| `Order_Lifecycle`           | KV                                    | Action Capture              | Broker-order lifecycle projection            |
| `Positions`                 | KV                                    | Fill-derived projector      | Position lifecycle aggregate                 |
| `Fills`               | LOG                                   | Action Capture              | Immutable postback/fill audit                |
| `Order_Correlation`         | KV                                    | Executor                    | Three-ID and attempt mappings                |
| `Execution_Gate`            | KV                                    | Executor                    | Gate state and approvals                     |
| `Execution_Attempts`        | KV                                    | Executor                    | Attempt state and request hash               |
| `Execution_Audit`           | LOG                                   | Executor                    | Immutable execution and gate audit           |
| `Position_Actions`          | LOG                                   | Babysitter after MVP        | Structured future position actions; no DDL yet — not in the 24-table manifest |
| `Postback_Quarantine`       | LOG                                   | Action Capture              | Uncorrelated/invalid postbacks               |
| `Portfolio_Reservations`    | ~~KV (EVIDENCE-GATED)~~                   | ~~Signal job ranking operator~~ | ~~Reservation state per portfolio + instrument~~ — **REMOVED 2026-08-15 (CHG-005)** |
| `Postback_Projection_Ledger`| KV (EVIDENCE-GATED)                   | Action Capture              | Durable projection completion tracking        |
| `Safety_Halt_Requests`      | KV (v3, R-089)                        | Authorized components        | Durable safety-control events; PK `halt_request_id` dedups re-delivery |
| `suspected_discontinuities` | LOG                                   | Ingestion                   | Non-sequence discontinuity evidence          |
| `ingestion_quarantine`      | LOG                                   | Ingestion                   | Invalid/undecodable broker rows quarantined by ingestion |
| `forming_bar`               | KV                                    | Signal job (deferred consumer) | Per-ticker forming-bar projection, PK `instrument_token`; no consumer requirement yet — when implemented (Slice 2.2) this is the Fluss-authoritative durable forming-bar home (DEC-038), with in-process events to Business Logic (REQ-FC-007) |
| `instruments`               | KV                                    | Operators                   | Versioned instrument manifest                |

A pre-production clean break permits replacing stale table definitions. Every table SHALL have an explicit schema-version and owner matrix.

## REQ-FLS-004: Identity and event fields

Event/audit tables SHALL use the identities applicable to their domain:

- Market events: `event_fingerprint`, `fingerprint_version`, `connection_id`, `connection_epoch`
- Strategy: `candidate_id`, `instruction_id`, `trade_context_id`
- Broker/execution: `broker_order_id`, `client_order_ref`, `execution_attempt_id`
- Position: `position_id`, `trade_context_id`
- Postbacks: `postback_event_id`, `postback_fingerprint`

`order_id` SHALL NOT be used as a generic cross-domain identity.

## REQ-FLS-005: Raw market log

`raw_table_1` SHALL contain original packet bytes, payload hash, decoder/protocol version, fingerprint/version, UTC event/ingest/ack timestamps, instrument/routing fields, and verified normalized trade/depth fields. `seq_no` is not a required column. If retained for future evidence, it is nullable observational data and cannot support guarantees.

Distribution SHALL preserve per-instrument affinity using tested Fluss bucketing. Retention is at least three complete trading days and is extended automatically while the relevant EOD manifest is unverified or retryable.

## REQ-FLS-006: Candle state

`feature_candles_15s` is **KV** current-state candle output (PK `(instrument_token, window_start)`, upsert last-write-wins — 2026-08-13 user requirement; the older LOG-only wording is superseded). It contains instrument, UTC window boundaries, OHLCV, tick count, source/algorithm version, and output timestamp. Late corrections are not written in MVP. Upsert makes replay converge (re-upserting the same key replaces the row; no row growth). Retention is at least three complete trading days plus offload safety extension. **Candle-table exception (user decision 2026-08-16):** the 3-trading-day floor is the source/event-table invariant; `feature_candles_15s` itself is derived data kept **2 calendar days** (`table.log.ttl='2d'`) — it is rebuildable from `raw_table_1` replay (which keeps the floor), and the lake-off dev deviation means no offload extension applies to it.

As the durable candle state, `feature_candles_15s` is an **authoritative Fluss state table under DEC-038**: Flink computes each final row from its small in-flight window accumulator and writes the row to Fluss; the Flink checkpoint never carries a second full copy of candle history. The `feature_candles_15s_current` KV projection, its dual sink, and the offline `CandleMigrationTool` historical load are RETIRED (2026-08-13 re-scope); replay/duplicate handling is derived from the KV upsert itself. The current-state projection requirement for signals moves to `Signal_Candidates_current` under REQ-FLS-007. The candle-KV replay-safety line (CANDLE-KV-REPLAY-001) is decommissioned — the measured close-out is absorbed into the master dossier `04-signal-job.md` §Absorbed documents (the retired-candle-era record, 2026-08-17 consolidation; tracker `14-candle-log-kv-replay-safety_2.md` was deleted the same day).

## REQ-FLS-007: Strategy and ranking audit

`Signal_Candidates` is an immutable append-only LOG table (v3, re-scoped 2026-08-13 — the R-084 KV conversion is reversed, resolving the dead-supersede-chain problem): one new row per fired signal, routed by `instrument_token`, never updated; corrections are new rows with an explicit supersession relation. `Signal_Candidates_current` is the companion KV current-state projection keyed by `(instrument_token)`: one row per instrument holding the latest/active candidate, where supersession overwrites in place and the table rebuilds from the LOG. ~~`Ranking_Results` is an immutable LOG table.~~ **(REMOVED 2026-08-15, CHG-005 — ranking out of scope, not deferred.)** The tables are EOD-tiered and retained in encrypted lake storage for the approved analytics/audit period.

## REQ-FLS-008: Immutable instruction feed — REMOVED (CHG-005, 2026-08-15)

**REMOVED from scope 2026-08-15 (CHG-005, not deferred).** The `Trade_Decisions` immutable instruction feed (decisions half of Slice 3) is out of scope; the decision dual-sink machinery is gated off (`TRADE_DECISIONS_ENABLED=false`, no producer in scope).

## REQ-FLS-009: Lifecycle, positions, and execution state

`Order_Lifecycle`, `Positions`, `Order_Correlation`, `Execution_Gate`, and `Execution_Attempts` are separate state aggregates. Partial update may be used only where columns have explicit owners and stale-update/version tests pass.

`Execution_Audit` and money-moving postback/fill audit events SHALL be immutable, integrity-verifiable, access-controlled, and lake-tiered/encrypted for at least one year under approved policy. Operational projections may have shorter live retention only if their source audit allows complete rebuild.

## REQ-FLS-010: Quarantine and discontinuities

Quarantine tables preserve original bytes, reason codes, schema/version, source identity, timestamps, and disposition status. They are not silently discarded and do not become executable state.

`speculated` or exact gap-range columns are prohibited unless protocol evidence proves the required sequence semantics.

## REQ-FLS-011: EOD offload and retention gate

Eligible immutable tables SHALL be offloaded to encrypted Iceberg/S3 at EOD. The offload process SHALL produce a manifest containing table/schema version, trading date, source range, row/byte counts, content hashes/checksums, and commit status.

Source retention SHALL NOT expire a trading day's data while its manifest is unverified, retryable, or under reconciliation. The minimum live buffer is three complete trading days. A failed offload extends retention and emits a critical storage alert.

## REQ-FLS-012: Schema evolution

Every schema change requires:

1. Versioned DDL and compatibility classification.
2. Additive/removed/renamed field analysis.
3. Replay and lake schema tests.
4. Deployment order and rollback plan.
5. Consumer compatibility validation.
6. Migration evidence before live-money enablement.

Pre-production clean break allows replacing stale schemas without preserving incompatible old consumers.

## REQ-FLS-014: Scope, reservation, and projection state

The logical model SHALL include the following additional state contracts before physical DDL is finalized:

- ~~`Portfolio_Reservations`: authoritative reservation state keyed by `portfolio_id`~~ — **REMOVED 2026-08-15 (CHG-005 — reservations out of scope, not deferred).**
- `Postback_Projection_Ledger`: durable projection workflow keyed by `postback_event_id`, with audit/lifecycle/position completion states, retries, errors, and disposition.
- `Safety_Halt_Requests`: durable safety-control events keyed by `halt_request_id`, with scope, source, reason, detection time, evidence hash, and application status.

The physical table kind, merge engine, changelog image, bucket configuration, and partial-update behavior remain evidence-gated until the pinned Fluss capability suite passes.

Each state contract SHALL define one writer owner, readers, key, bounded growth, cleanup, retention, rebuild source, stale-update behavior, and acceptance tests.

## REQ-FLS-015: Instruction feed enforcement — REMOVED (CHG-005, 2026-08-15)

**REMOVED from scope 2026-08-15 (CHG-005, not deferred) with the `Trade_Decisions` feed (REQ-FLS-008).**

## REQ-FLS-016: Position and reservation scope

`Positions`, ~~`Portfolio_Reservations`~~, `Order_Lifecycle`, `Execution_Gate`, and `Execution_Attempts` SHALL carry `account_scope_id` and the applicable ~~`portfolio_id`~~ or `execution_partition_id`. Cross-scope reads/writes are prohibited unless an explicit reconciliation contract authorizes them. **(The `Portfolio_Reservations` / `portfolio_id` reservation-scope half is REMOVED 2026-08-15, CHG-005.)**

## REQ-FLS-017: Authoritative Signal hot state (DEC-038)

Fluss is the authoritative durable hot-state layer for the applicable current Signal-job business state. Large durable state that must survive a Flink restart SHALL be owned and persisted by Fluss; Flink SHALL retain only (a) the small working state needed for active processing and (b) the minimal recovery state needed to restart safely. This requirement applies to the current simple Signal path only — dedup state, candle state, forming-bar state (when implemented), and current signal state. Ranking and reservation state are explicitly OUT OF SCOPE and unchanged.

For each Fluss-owned Signal state table/domain, the design SHALL define, before DDL generation:

1. **Owner** — the single writing component (the Signal job).
2. **Keys** — logical key and routing (`bucket.key`) with per-instrument colocation.
3. **Update semantics** — upsert vs append; how replay converges.
4. **TTL / cleanup** — bounded growth and the exact cleanup mechanism (native per-key TTL exists in Fluss 1.0.0 — `table.kv.ttl` / `table.kv.ttl.time-column`; where it is not adopted, an expiry column plus a cleanup path must be designed and tested, not assumed).
5. **Rebuild source** — the immutable LOG/audit that can reconstruct the state (e.g. `raw_table_1` replay within the dedup TTL window).
6. **Versioning** — schema version and serialization contract.
7. **Restart behavior** — how Flink restores its compact checkpoint, verifies Fluss state availability/compatibility, and rehydrates only the working state it needs.
8. **Consistency requirements** — what a bounded Flink working cache may hold vs the Fluss authoritative copy, and the fail-closed rule when Fluss state is unavailable or incompatible.

Flink checkpoints SHALL NOT become a duplicate full copy of Fluss-owned Signal business state. This requirement does not create ranking/reservation tables or requirements. **(Ranking/reservation/decision state is REMOVED from scope 2026-08-15, CHG-005.)**

## REQ-FLS-013: Acceptance

Storage tests SHALL prove schema/DDL parity, three-node replication and one-VM loss, partial-update ownership, stale update rejection, immutable event behavior, checkpoint/sink recovery, quarantine rebuild, projection-ledger recovery, EOD manifest validation/retry, three-day retention safety, policy-controlled encrypted audit retrieval for at least one year, scope isolation, and schema migration/rollback.
