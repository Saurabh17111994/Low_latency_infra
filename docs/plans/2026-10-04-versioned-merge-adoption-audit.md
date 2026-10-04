# Versioned-merge adoption audit — which KV tables still race, and can they use the engine?

**Status:** read-only audit (2026-10-04). No code, DDL, config or cluster artifact changed.
Follow-up to CHG-547 (`docs/plans/2026-10-04-signal-candidates-current-versioned-merge.md`).

## 0. Question

CHG-547 put the ordering guarantee for `Signal_Candidates_current` in the tablet itself
(`table.merge-engine='versioned'`, `ver-column='evaluation_ts'`). Which other KV tables carry the
same "two writes, last one wins blindly" hazard, and which of them can adopt the same engine?

## 1. Method

Read-only: the DDLs (`29_position_state.sql`, `10_positions.sql`, `13_order_correlation.sql`,
`09_order_lifecycle.sql`), the gateway write path (`FlussProjectionWriter`, `ProjectionApplier`,
`OrderLifecycleWriteGate`), the executor projection (`code/02_services/04_executor/src/projection/mod.rs`),
the readers (`PositionsObservationOperator`), the ops runbook (`docs/06_operations/01-runbooks.md`),
and the production deck (`docker-stack.yml`). No cluster was touched.

## 2. Verdicts

| table | writers today | version column | guard today | readers | verdict |
|---|---|---|---|---|---|
| `Position_State` | gateway only (blind upsert) | `source_version BIGINT NOT NULL`, set on every write | **none** | **none in code** | **ADOPT** — cheapest, no reader risk |
| `Positions` | gateway (blind) + executor (version gate) | `source_version BIGINT NOT NULL` | executor in-process; gateway none | `PositionsObservationOperator` (changelog) | **CONDITIONAL** — prove one version space first |
| `Order_Lifecycle` | gateway, through the M1-6 gate | `source_version` | strongest: gate + quarantine + halt | gateway/quarantine | **DO NOT ADOPT** — would lose detection |
| `Order_Correlation` | gateway (blind upsert) | none (only `schema_version`) | ledger dedup only | correlation lookups | **NOT NOW** — needs a schema version bump |

## 3. `Position_State` — adopt

Evidence:

- DDL `code/01_platform/02_sql/ddl/29_position_state.sql:49` PK
  `(account_scope_id, instrument_token)`; `:47` `source_version BIGINT NOT NULL, -- monotonic per
  (account_scope_id, instrument_token); writer MUST ignore stale versions`; `:44` `updated_ts …
  -- NOT an ordering guard`; `:36-37` "blind LWW upsert is stale-unsafe — the writer MUST
  ignore-if-older on source_version".
- Writer `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/FlussProjectionWriter.java:131-136`
  `writePosition` → `upsert("Positions", …)` + `upsert("Position_State", positionStateRow(e))` —
  **blind**, no read-evaluate-write. Contrast the same class at `:127`, where the lifecycle write
  goes through `lifecycleGate().apply(e)`.
- `positionStateRow` (`:428-470`) sets `sourceVersion` from `p.sourceVersion()` and
  `schema_version='2'` on every write, so the version column is always populated — a precondition
  of the versioned engine.
- **No reader.** `rg Position_State` over the repo finds only the gateway writer, `DdlBootstrap`,
  tests, one OpenObserve dashboard and a stale-table scanner. The DDL's claim (`:23`) that "Flink's
  `ActiveSignalFilter` watches this table's LOG changelog" is **stale**: the runbook records that
  `ActiveSignalFeedbackFunction` and its metrics were deleted in `0f3e5952` and that
  RB-POS-001 "is not an operative procedure" (`docs/06_operations/01-runbooks.md:960-966`).

So the engine can be adopted with no reader-side risk, and it closes a hazard the table's own header
already flags. Preconditions all pass: full-row writes, no partial updates, no DELETE, version
non-null.

## 4. `Positions` — conditional, one precondition to prove

- Two writers, and they derive the version from **different upstream fields**: the executor sets
  `source_version = fill.source_sequence` (`code/02_services/04_executor/src/projection/mod.rs:397`,
  documented at `:754` as "the report's own [sequence]") and gates in-process
  (`VersionGate::evaluate(c.source_version, fill.source_sequence, content_matches)` `:276`); the
  gateway writes `positionRow(e)` with `p.sourceVersion()` from the normalized postback event.
- A tablet-side comparison is only meaningful inside **one** version space. If bridge-report
  sequences and postback source versions are not comparable, the engine would silently ignore a
  legitimate write (the merge is `compare(old,new) > 0 ? old : new`, no exception).
  **This is not yet proven either way** — it is the gate on adopting for `Positions`.
- Related finding: the guarded Java path exists but is **unwired**. `PositionProjectionWriter`
  ("applies the version gate", outcomes `APPLIED|DUPLICATE|STALE|VIOLATION`) and
  `PostbackProjectionDriver` are referenced only by tests and `FlussCorrelationIndex`; the gateway's
  main sources contain no reference to them (`rg` → 0 hits). So the DDL header's "enforced in
  `PositionProjector.apply`" is true for the executor's own path and **not** for the gateway's.

## 5. `Order_Lifecycle` — do not adopt

The M1-6 gate (`OrderLifecycleWriteGate.java:17`) is strictly stronger than the engine: it reads the
stored row, classifies the write, and on `CONFLICT`/`REGRESSION`/`UNKNOWN` writes
`Postback_Quarantine` **and halts** the scope. The versioned engine's rejection is silent (no
exception, no quarantine). Adopting it here would convert a detected, halted conflict into a
silently dropped write that the gate still reports as `APPLIED` — a detection loss on the
money path. The P3-326 concern (a second gateway replica lost-updating the same eventId,
`docker-stack.yml:1040-1044`) is a ledger/CAS problem, not a reason to weaken this gate.

## 6. `Order_Correlation` — not now

Blind upsert (`FlussProjectionWriter.java:~138`), PK `(instruction_id, execution_attempt_id)`, and
**no version column** (`13_order_correlation.sql` carries only `schema_version`). Adopting needs a
schema v3 plus writer and recreate. The projection ledger already absorbs redelivered postbacks.
Defer.

## 7. Cross-cutting findings (docs vs code)

1. `29_position_state.sql` prose is stale twice: the `ActiveSignalFilter`/`ActiveSignalFeedbackFunction`
   consumer (deleted in `0f3e5952`) and the "ops ADMIN_CLEAR … only authorized second writer" path
   (retired with it).
2. The RB-POS-001 break-glass SQL (`docs/06_operations/01-runbooks.md:996`) is **pre-v2**: it lists
   `(instrument_token, status, position_id, updated_ts, closed_ts, closed_reason, schema_version)`
   with `'v1'`, i.e. no `account_scope_id` (now PK prefix) and no `source_version` (now NOT NULL) —
   as written it would now fail. Worth fixing regardless of the engine: an operator reaching for it
   during an incident should not hit a schema error.
3. The unwired guarded position writer (§4) is either a fallback to wire up or test-only code to
   label as such.

## 8. Recommendation and effort

1. **Docs only, no risk (~15 min):** fix the runbook SQL and the two stale `29_position_state.sql`
   claims.
2. **Adopt the engine on `Position_State` (CHG-548, ~1-2 h):** the CHG-547 shape — DDL
   `table.merge-engine`/`ver-column='source_version'`, `DdlBootstrap` descriptor,
   `schema_manifest.json`, a `TableContractValidator` assertion that fails closed, one focused test,
   and a table-recreate window. Only writer is the gateway, no readers to coordinate.
3. **Measure the `Positions` version space (before any code):** record the observed
   `source_version` values from both writers for the same `(account_scope_id, instrument_token)` and
   check they are one monotone sequence. Only then decide.
4. **Leave `Order_Lifecycle` and `Order_Correlation`** (reasons above).

Non-goals: no partial-update rewrite (no production path calls `partialUpdate`), no executor/Rust
change, no second-replica CAS work (that is P3-326's own scope).

## 9. Still open

What does a merge-**rejected** stale write put into the changelog — the retained row or the rejected
one? The CHG-547 Q1 probe measured that a rejection appends exactly one record, not its content.
This matters only to changelog consumers; `Position_State` has none and `Signal_Candidates_current`
has none today, so it is a prerequisite for any future tailing consumer rather than a blocker here.
