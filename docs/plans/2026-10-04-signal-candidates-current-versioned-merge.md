# Plan — `Signal_Candidates_current`: versioned merge engine (native write-ordering fix)

Status: **implemented 2026-10-04 (CHG-547)** — DDL, boot descriptor, startup
contract and the live dev-table recreate all landed; see §0 Outcome for the evidence.
Target change record: `CHG-547` (filed).
Owner table: `code/01_platform/02_sql/ddl/23_signal_candidates_current.sql` (tableId 22 in dev).
Opened: 2026-10-04, after the operator asked for a native, not-deferred fix
(`ok i want some native fix or solution, now only … rather zero worry solution i want`).

---

## 0. Outcome (2026-10-04)

Landed, in the order the plan specified (test-first, RED observed before each edit):

| # | Artifact | Evidence |
| - | -------- | -------- |
| 1 | `23_signal_candidates_current.sql` — `table.merge-engine=versioned`, ver-column `evaluation_ts`, rewritten `Staleness (P4-070/224)` header | `SignalCurrentDdlContractTest::kvDdlPinsVersionedMergeEngine` RED (`expected <versioned> but was <null>`) → GREEN (`Tests run: 5, Failures: 0`) |
| 2 | `DdlBootstrap.java:573` — the same two options as `.property(...)` | `DdlBootstrapSchemaAgreementTest::signalKvDescriptorMatchesDdlMergeEngine` RED → GREEN (`Tests run: 10, Failures: 0`) |
| 3 | `TableContractValidator.validateSignalCurrentKvTable` — fail-closed `validateVersionedMergeEngine` | `TableContractValidatorTest` RED (`Tests run: 44, Failures: 3`, lines 185/196/206) → GREEN (`Tests run: 44, Failures: 0`) |
| 4 | Live dev table recreated and probed | `TableInfo` gains `table.merge-engine=versioned`, `table.merge-engine.versioned.ver-column=evaluation_ts`, `table.delete.behavior=IGNORE`; §5.3 answered — the catalog **does** surface the options |
| 5 | Behavioural proof | `SignalCandidatesCurrentMergeEngineIntegrationTest` (`@Tag("integration")`, scratch table, dropped after): `Tests run: 1, Failures: 0` — a newer `evaluation_ts=2000` row survived a later write of `1000`, and an equal version still kept last-writer-wins |
| 6 | `docs/05_deployment/change-records/CHG-547.md` | filed, machine block complete |

Deviations from the plan, each deliberate:

- **`GateTableAdmin.java` was generalized** (`assertMergeEnginePresent` no longer hard-pins
  `fence_token`; it requires any non-blank version column with a `versioned` engine, and the
  summary line prints the actual column). Without this the gate tool refused table 23. The
  `fence_token` pin stays covered by `11_execution_gate.sql` and
  `GateMergeEngineDrillIntegrationTest`, and the *tool* is still gate-named — a rename is
  cosmetic and was not done.
- **`docs/09_data_gaps.md` does not exist** (the plan's §4.6 named it). The stale-write gap
  is recorded where it actually lives: `docs/01_project/02-system-context.md:76` now names
  the two versioned tables and the reason the engine refuses partial updates (corrected
  2026-10-04: no production writer calls `partialUpdate(...)`, so adoption is gated by a
  per-table writer audit, not by a change of write mode).
- **A recreate runbook section was added** (`docs/06_operations/01-runbooks.md`,
  "Signal_Candidates_current recreate (versioned merge engine, CHG-547)") beside the gate's
  CHG-122 one, including the DDL-path-is-argument-3 trap.

Both §10 open questions are now answered (Q1 measured on the dev cluster 2026-10-04, Q2 by probe).

---

## 1. Problem (measured, not assumed)

`Signal_Candidates_current` is a KV table (`PRIMARY KEY (instrument_token) NOT ENFORCED`,
`bucket.num=16`) that receives one full-row upsert per fired signal from the SignalJob sink
`strategy-host-candidates-current-sink` (`code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java:386-400`).
The tablet applies **unconditional last-writer-wins**: whichever write arrives last wins, regardless of
the event time it carries.

Consequences today:

1. **Clobber window (documented, unguarded).** A delayed or retried signal older than the row already
   stored overwrites it. The DDL header states it outright: *"KV upserts are last-writer-wins on
   instrument_token with NO ordering guard in the sink — a delayed/retried out-of-order signal CAN
   clobber a newer row. Ordering is the producer's contract … no Flink deduplicate/row-number stage
   exists today."* (`code/01_platform/02_sql/ddl/23_signal_candidates_current.sql:14-22`, P4-070/224).
2. **Blast radius today = read-only.** Verified: the decision path does not read this table back;
   execution intents are built in-process from the same event-time-ordered stream. Readers are tools
   and viewers (`JobGraphDump`, `SignalCandidatesViewer`, `FlussSignalLatency`, DDL/lint scripts,
   tests). So a clobbered row misleads an operator view, not an order. This is why the gap has been
   acceptable so far — and why the fix must be cheap.
3. **Storage amplification.** The table holds ~2 400 rows (one per instrument, ~2–3 MB of state), yet
   its `log-N` changelog retains 7 days of *every* overwrite: 1 870 MB measured on 2026-10-04
   (`du` of `/tmp/fluss/data/default/Signal_Candidates_current-22`).

## 2. Decision

Adopt Fluss 1.0's native **versioned merge engine** on this one table:

```
'table.merge-engine' = 'versioned',
'table.merge-engine.versioned.ver-column' = 'evaluation_ts',
```

The guarantee then lives in the tablet, for every writer and every future consumer: nothing to prune,
no Flink state, no reader discipline, no ordering-by-convention.

### 2.1 Semantics (proven from the shipped jar, not from prose)

Source: `/opt/fluss/lib/fluss-server-1.0.0.jar` inside `01_docker-fluss-tablet-1`, extracted to
`/tmp/fx`; `javap -p -c -l` on
`org/apache/fluss/server/kv/rowmerger/VersionedRowMerger.merge(BinaryValue oldValue, BinaryValue newValue)`:

```java
if (oldValue == null) return newValue;
return versionComparator.compare(oldValue.row, newValue.row) > 0 ? oldValue : newValue;
```

| incoming vs stored | result |
|---|---|
| newer version | incoming row wins |
| **older version** | **incoming row silently discarded — no exception, no client-visible error** |
| equal version | incoming row wins (today's LWW preserved) |
| key absent | incoming row stored |

Engine restrictions (from the class's own message strings):

- version column must be `INT`, `BIGINT`, `TIMESTAMP` or `TIMESTAMP_LTZ` — ours is
  `evaluation_ts BIGINT NOT NULL` (`23_signal_candidates_current.sql:65`). ✔
- "Partial update is not supported for the versioned merge engine." — the sink writes full rows
  (`RowDataSerializationSchema`, no partial-update API). ✔
- "DELETE is not supported for the versioned merge engine."; `delete.behavior` defaults to `ignore`
  for versioned — the producer never deletes. ✔
- `org/apache/fluss/exception/InvalidUpdateVersionException` exists but is referenced nowhere in the
  client/record packages and is not thrown on this path ⇒ adopting the engine adds **no new
  writer-visible failure mode**.

### 2.2 Precedent — this is the second application of a pattern already in the repo

`Execution_Gate` has used exactly this since CHG-122:
`code/01_platform/02_sql/ddl/11_execution_gate.sql:120-121` sets
`'table.merge-engine' = 'versioned'` with `'table.merge-engine.versioned.ver-column' = 'fence_token'`,
adopted by drop+create because the option is create-only. Doctrine: *"the only local merge-engine use
is versioned on `11_execution_gate.sql:120-121` (`fence_token`). No drift."*
(`docs/plans/2026-09-22-fluss-1.0-native-adoption.md:998`). Recreate runbook already written:
`docs/06_operations/01-runbooks.md:238-271`. Tests to model on:
`code/common/src/test/java/com/trading/common/schema/execution/{GateMergeEngineDrillIntegrationTest,FlussGateStateStoreWriteOrderTest,FencingLeaseRenewRevokeTest}.java`.

Corollary: the "is the engine safe in our stack" question is already answered in production-adjacent
code (the money gate), by a change that shipped and passed its drills.

## 3. Non-goals (explicitly out of scope)

- **`Position_State` and the execution-gateway projections** (the other documented LWW-clobber sites:
  `docker-stack.yml:1040-1044`, P3-326, `FlussProjectionWriter`, `PostbackQuarantineStore`). Same
  pattern, but gated: versioned merge is **incompatible with partial updates**, and these are the
  tables whose design is multi-writer column-group ownership (DEC-005). *Corrected 2026-10-04:*
  the blocker is a per-table writer audit, not a mechanical "move off partial updates" — the gateway
  projector already upserts full rows (`FlussProjectionWriter.java:226,231`) and `partial_update`
  appears only in the ownership contract and its tests. The audit asks: is there one effective writer
  per row, is the version column non-null and genuinely monotonic, and does any reader depend on
  columns that a partial write would leave untouched? Separate change.
- Re-keying `Signal_Candidates` (LOG) on `candidate_id` — tried and reverted 2026-08-13 (DEC-035).
- Changing retention/TTL or `bucket.num` (P4-332/DEC-035 pin them; the LOG twin must stay colocated).
- Any Flink-side dedup/row-number stage (unnecessary once the tablet orders by version).
- Changelog compaction/trimming — Q1 (below) measured the per-write cost; trimming the
  changelog itself is still a separate, unbuilt idea.

## 4. Changes to make

### 4.1 DDL — `code/01_platform/02_sql/ddl/23_signal_candidates_current.sql`

1. Add the two options to the `WITH` block (after the existing `'bucket.key'` line, next to the other
   table-semantics options).
2. Rewrite the `Staleness (P4-070/224)` header block: the clobber is now **prevented by the tablet**
   for strictly-older versions, with the residual cases named (equal version ⇒ last writer wins;
   `evaluation_ts` must be non-null and monotonic per instrument). Keep P4-070/224 reachable — the
   producer contract is still the source of truth for the values; cite P4-332/DEC-035 as unchanged.
3. No column, key, or bucket change ⇒ **no schema-version note needed** (same reasoning as the gate
   v4 entry in `docs/plans/2026-09-11-wave0-closure-durable-fencing.md:61`: options, not schema).

### 4.2 Descriptor — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java:573`

Add to the `Signal_Candidates_current` builder, in the same style as the sibling entry
(`:565-571` uses `.property("table.kv.format-version","2")`):

```java
.property("table.merge-engine", "versioned")
.property("table.merge-engine.versioned.ver-column", "evaluation_ts")
```

This is the dev/prod create path (`tableRegistry()` :521, `ALL_TABLES` :530); the DDL file above is
the apply contract. Both must agree — that is what `DdlBootstrapSchemaAgreementTest` and
`SignalCurrentDdlContractTest` exist to enforce.

### 4.3 Contract validation — `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/TableContractValidator.java:135`

Extend `validateSignalCurrentKvTable(TableInfo info)` with a **fail-closed property assertion**: read
`info.getProperties()` (returns `org.apache.fluss.config.Configuration`, verified by `javap`) and
require `table.merge-engine=versioned` with
`table.merge-engine.versioned.ver-column=evaluation_ts`, in the existing error style
(`requireExactPrimaryKey` :234, `validateSchema` :207, `validateRouting` :252). Rationale: a job that
runs against an LWW table must refuse to start rather than silently lose the guarantee — the same
reason `GateTableAdmin.assertMergeEnginePresent` refuses to rebuild an LWW gate
(`code/01_platform/04_scripts/fluss-repair/GateTableAdmin.java:105-120`).

### 4.4 Tests (written before the code, per repo rule)

| # | Test | Asserts |
|---|---|---|
| T1 | `TableContractValidator` unit test (new case in the existing validator test class) | properties absent / engine `first_row` / wrong ver-column ⇒ validation fails; correct pair ⇒ passes. RED first against today's validator. |
| T2 | `code/common/src/test/java/com/trading/common/schema/SignalCurrentDdlContractTest.java` | the DDL text declares both options with the exact values (text-level pin, mirrors the gate's DDL pin). |
| T3 | `code/02_services/01_ingestion/src/test/java/com/trading/ingestion/DdlBootstrapSchemaAgreementTest.java` | the descriptor carries the same options as the DDL (add the leg if the existing comparison is schema-only). |
| T4 | live, env-gated integration test (pattern: `code/common/src/test/java/com/trading/common/schema/fluss/CompatFlussIntegrationTest.java`, next free id **COMPAT-FLUSS-007**) | against a real cluster: write `evaluation_ts=2000` then `evaluation_ts=1000` ⇒ stored row is still the 2000 one; write `2000` then `2000` ⇒ second wins; null version ⇒ rejected at write/validate. |
| T5 | (guard) a test that fails if the DDL gains a `versioned` engine while the descriptor does not, and vice versa | parity cannot drift silently. |

### 4.5 Change record

`docs/05_deployment/change-records/CHG-547.md`: trigger, the bytecode evidence for the semantics,
restrictions check, recreate procedure, rollback, evidence pointers. `python3 code/01_platform/04_scripts/change_control_check.py`
must stay at *all records complete, 0 FAIL* (currently 540).

### 4.6 Docs touched

- `docs/08_implementation/02-schema-storage.md` — the table's row in the KV inventory + the clobber note.
- `docs/08_implementation/04-signal-job.md` — any "ordering is the producer's contract" claim.
- `docs/09_data_gaps.md` — if the clobber gap is listed there.
- `docs/06_operations/01-runbooks.md` — add the table-23 recreate entry beside the gate's (CHG-122) one.

## 5. Preconditions to verify while implementing (each is a command, not a belief)

1. `evaluation_ts` is non-null on every write path (`N7RangeBreakoutStrategy`, `ContextProbeStrategy`,
   `StrategyHostFunction`) — a null version defeats the comparator.
2. The sink's existing options do not silently disable writer idempotence:
   `client.writer.enable-idempotence` is on by default *only if no conflicting config is set*, and
   `SignalJob.java:392-398` sets `client.request-timeout` and `client.writer.retries`. Confirm the
   effective value on the live sink; if it is off, either drop the conflicting option or set
   idempotence explicitly (an explicit enable + a real conflict throws `ConfigException` — a
   start-up failure, which is the intended fail-loud behaviour).
3. `TableInfo.getProperties()` on the live table returns the option after recreate (probe read).
4. `table.merge-engine` is create-only ⇒ the dev table must be recreated (see §6).

## 6. Apply procedure (dev cluster)

Prerequisite: the arm currently in flight (`bdd54bc03`, `C0c`) has finished — never touch the cluster
mid-capture.

1. `java -cp "code/common/target/classes:$(cat code/02_services/01_ingestion/target/cp.txt)" GateTableAdmin show code/01_platform/02_sql/ddl/23_signal_candidates_current.sql`
   — prints the options `DdlText` derives from the DDL; must show `table.merge-engine = versioned`.
   (Classpath convention is the repo's standard one — `code/01_platform/04_scripts/tiering-smoke.sh:148`.)
2. Stop writers for the table: cancel/finish the SignalJob so the sink is not writing during the drop
   (a recreate makes the next sink write fail until the table exists again).
3. Recreate — **reuse the existing tool**, taking the DDL path as arg 3 (it is generic:
   `DdlText.parse` + `toDescriptor`, `GateTableAdmin.java:52-64`), which also fails closed if the DDL
   lacks the merge engine:
   `java -cp "code/common/target/classes:$(cat code/02_services/01_ingestion/target/cp.txt)" GateTableAdmin recreate localhost:9123 code/01_platform/02_sql/ddl/23_signal_candidates_current.sql`
   No lake archive step is needed for this table (`table.datalake.enabled=false` in the dev DDL).
   Cosmetic note: the tool's messages are gate-worded ("VERSIONED on fence_token", gate KV warning);
   a label generalisation is optional follow-up, not part of this change.
4. `make ddl` (validate) and, where the contract is executable in this context,
   `make ddl APPLY=1 EVIDENCE=<file>`; regenerate `schema_manifest.json` if the parity check asks for it.
5. Restart the writers (job + ingestion), confirm the sink starts, then **live-verify ordering** with a
   probe (the pattern already exists: `code/01_platform/04_scripts/fluss-probes/FlussKvScanStrategy.java`
   / `FlussKvProbe`): write a newer then an older `evaluation_ts` for one instrument and read the row back.
6. Record evidence under `logs/tracker-14/` (dated, immutable) and update this plan's status.

Production rollout (Swarm/VM) is **not** executed by this plan: the DDL change ships in the repo, the
recreate needs a maintenance window per `docs/06_operations/01-runbooks.md` pattern, and the
production apply path for this table is unproven (same honesty as the existing prod-deploy note).

## 7. Risks and caveats (stated, not hidden)

- **Q1 (answered 2026-10-04, measured): yes — a *rejected* stale write still appends one changelog
  record.** Ordering is fixed; the bytes are not. The record is appended and then discarded at apply
  time. For the write class this change targets (an out-of-order or retried upsert of a key that
  already has a row) the cost per write *halves*: an accepted update writes two records
  (`UPDATE_BEFORE` + `UPDATE_AFTER`), a rejected one writes one. New keys are unaffected (one record
  either way), so the 1 870 MB changelog keeps growing — at roughly half the rate for those writes.
  Numbers in §10 Q1.
- **Version column must be non-null and sane.** A bogus future `evaluation_ts` (clock skew, bad
  producer) now becomes *sticky*: the newer row can no longer be overwritten by correct rows. Mitigation:
  §4.3 makes the job validate the table; the producer contract for `evaluation_ts`
  (`>= detection_ts`, epoch millis UTC) is unchanged and pinned by the twin contract.
- **Equal versions still race** (last writer wins). Unchanged behaviour, acceptable: retries reuse the
  same `candidate_id` and the same `evaluation_ts`.
- **Recreate destroys the dev KV state** (2 400 rows, re-derivable from the LOG twin within one tick —
  viewers only, no order path). Production equivalent needs the documented window.
- **Writers must be stopped during the recreate** (§6.2), otherwise the sink fails on the missing table.
- **First-ever merge engine on this table** ⇒ the recreate is the risky step, not the option: same
  class of operation already performed for the gate (CHG-122) on a money-path table.

## 8. Rollback

Recreate again from the previous DDL revision (`git show HEAD~1:code/01_platform/02_sql/ddl/23_signal_candidates_current.sql`,
via `GateTableAdmin recreate` with that file) — but note `assertMergeEnginePresent` refuses a DDL
without the engine, so rollback uses `RawTableAdmin`-style drop+create or a temporary DDL path with
the assertion bypassed deliberately. Practically: rollback is only needed if the job cannot start
against a versioned table, which §4.4 T4/T5 rule out before apply.

## 9. Acceptance criteria

1. DONE — DDL, descriptor, validator and change record landed; T1–T5 green (RED observed first for T1–T3).
2. `make test` (common + ingestion) + `change_control_check.py` — see §0 Outcome / the task report.
3. DONE — the recreated dev table reports `table.merge-engine=versioned` +
   `table.merge-engine.versioned.ver-column=evaluation_ts` in `TableInfo.getProperties()`.
4. DONE — ordering proof is executable and green (`SignalCandidatesCurrentMergeEngineIntegrationTest`);
   the live evidence is recorded in `CHG-547.md` ("Verified on the dev cluster").
5. DONE — Q1 answered by measurement (§10): a rejected stale write still appends one changelog
   record; an accepted update appends two. The guard halves the changelog cost of the
   out-of-order/retry writes it rejects, and does not remove it.
6. DONE — no change to `bucket.num`/`bucket.key`/schema; `SignalCurrentDdlContractTest` stays green.

## 10. Open questions

- Q1 — **answered by measurement (2026-10-04).** Method: a scratch table `chg547_q1_probe` created
  from this same DDL (so the versioned engine is on) on the running dev cluster, 8 writes to one key
  in bucket 5, log end offset read as `sum(Admin.listOffsets(path, buckets 0..15, new
  OffsetSpec.LatestSpec()))` — the primitive
  `code/01_platform/04_scripts/fluss-probes/FlussReadLagProbe.java` already uses. Results (re-read
  3 s later, unchanged, so nothing was lagging):

  | write | state | log end delta |
  |---|---|---|
  | `eval_ts=1000 SELL` | first row (insert) | +1 |
  | `eval_ts=2000 BUY` | accepted update | +2 |
  | `eval_ts=1500 / 1400 / 1300 SELL` | **stale, rejected** | **+1 each** |
  | `eval_ts=2000 SELL` | accepted (equal version, newer write wins) | +2 |
  | `eval_ts=2500 BUY`, `eval_ts=2600 SELL` | accepted updates | +2 each |

  The KV row stayed at the newer version throughout (`eval_ts=2000 side=BUY` across the stale
  burst). Mechanism from the server bytecode: `KvWriteProcessor.applyInsert` appends
  `ChangeType.UPDATE_BEFORE` + `UPDATE_AFTER` and returns 2 when the key already has a row, and
  appends once for an insert. **Consequence:** rejected writes are not free — they cost half of the
  accepted update they replace, so the ~7 d changelog growth from out-of-order/retried upserts is
  roughly halved, not stopped. Unverified detail: which `ChangeType` the rejected record carries (a
  log-scan probe on the scratch table failed on an Arrow/JDK-17 `MemoryUtil` reflection error,
  unrelated to Fluss).
- Q2 — **answered by probe (2026-10-04).** `ConfigOptions.CLIENT_WRITER_ENABLE_IDEMPOTENCE` default
  is `true`, `client.writer.max-inflight-requests-per-bucket` default is `5`, and
  `WriterClient.buildIdempotenceManager()` checks exactly one conflict: idempotence enabled **and**
  inflight > 5 → `IllegalConfigurationException` ("…should be less than or equal to 5 when
  idempotence is enabled to ensure message ordering"). The SignalJob sink overrides only
  `client.request-timeout` and `client.writer.retries` (`SignalJob.java:365-398`), and nothing in
  `code/` or `docs/` sets the idempotence or inflight keys, so **idempotence is enabled** for this
  writer as configured. No change needed; the trap to know about is inflight > 5.
- Should the same validator assertion be extended to `Position_State` when its writers move to
  full-row upserts? (Deferred; record as the follow-up trigger, not as work here.)
