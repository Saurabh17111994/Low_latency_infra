# Wave 0 closure — durable fencing + the remaining 27 P3 items

**Goal:** tick the last 27 unticked Wave 0 items in
`~/.opencodereview/sessions/home-saurabh-Jupyter_notebook-Flink_Fluss_Infrastructure-streaming_project_New/phases/p3-execution-gateway-bridge-executor.md`
(239/266 → 266/266), starting with the 3 CRITICALs.

**Decisions taken (confirmed by the user):**
- Adopt the **VERSIONED merge engine** on `fence_token` — the durable conditional write the ledger wrongly claims does not exist.
- **Keep replay-on-restart** for `P3-086`/`P3-091`; close both as an intentional, documented design.

---

## Part 0 — What the audit overturned

The ledger's blocker for the 3 CRITICALs — *"a true durable CAS is impossible — Fluss 0.9.1 has no conditional write"* — is **false**. It names VERSIONED and then dismisses it.

- `fluss/fluss-server/.../kv/rowmerger/VersionedRowMerger.java:73` — `versionComparator.compare(old, new) <= 0 ? newValue : oldValue`. A **lower-version write is dropped by the tablet**, server-side and durable.
- `MergeEngineType.java:55` + `ConfigOptions.java:1473/1484` — `'table.merge-engine'` / `'table.merge-engine.versioned.ver-column'`.
- `VersionedRowMerger.java:115` — `BIGINT` is a valid version column. The gate DDL already has `fence_token BIGINT`, documented as *"monotonically increasing per partition, never reused"* — it is already a version column.
- Feasibility confirmed: a VERSIONED table is a normal PK table, so `newUpsert().createWriter()` works (`FlussTable.java:82-92`), lookups work (`newLookup()` has no merge check), and both VERSIONED constraints hold — the gate store has **one** write site (`FlussGateStateStore:281`), writes a **full** row, and **never deletes**.

### Three hazards this creates — all must be handled

1. **A dropped write returns SUCCESS.** No `ignored`/`notApplied` signal exists in 0.9.1 (`UpsertResult` carries only `bucket` + `logEndOffset`; `KvTablet.java:575-583` returns the old value and skips silently). So a version-rejected write is **invisible** to the caller.
2. **`halt`/`revoke`/`approve` do NOT verify read-after-write.** Only `acquire`/`renew` call `verifyFence` (`FlussGateStateStore:185`); `halt`/`revoke`/`approve` go through `persistOrRollback` (`:262`) which has no read-back. **A rejected `halt` would be silently lost — a safety regression.** This is the single most important thing in this plan.
3. **`withFenceCleared` resets `fence_token` to `0L`** (`GateRow.java:148`). Under VERSIONED that release/halt write has version `0 < held token` and is **dropped**. `P3-374` ("monotonicity held by the store's fenceSequence") is therefore not a cosmetic deviation — it is a hard prerequisite.

Also: the coordinator auto-injects `table.delete.behavior=IGNORE` for VERSIONED (`CoordinatorService.java:555-570`), and `DdlApplyTool` compares **every declared** WITH option against the live table (`DdlApplyTool.java:718-756`), so the DDL must declare it explicitly.

**Scope honestly:** VERSIONED closes the **zombie-executor clobber** (the CRITICAL's real mechanism — a stale token overwriting the live owner). It does **not** close simultaneous double-acquire: `compare <= 0` accepts **equal** versions, so two hosts minting the same next token both win. That residual stays deployment-owned (ASM-EXE-005) and must be recorded as a real residual, not a footnote.

---

## Batch A — Token monotonicity (unblocks Batch B)

`P3-374` [MED] — prerequisite for the CRITICALs, not a cosmetic note.

- `GateRow.withFenceCleared(long clearedTs)` (`GateRow.java:142-149`): **stop resetting `fenceToken` to `0L`.** Keep the current token; clear `ownerInstanceId`/`fenceAcquiredTs`/`leaseExpiresTs`; set `fenceLostTs`. "No fence" is now expressed by `ownerInstanceId == null` (which `fenceValidFor:54` already requires), not by `token == 0`.
- Update the idempotence guard (`:143`) off `fenceToken == 0L` onto `ownerInstanceId == null && fenceLostTs` equality.
- Update the DDL header contract note (`11_execution_gate.sql`, P4-039 text says *"NULL owner/token/lease = unfenced BY DESIGN"*) to state that the token is retained as the write-ordering version and `owner = NULL` is the unfenced marker.
- Update `FencingLeaseRenewRevokeTest:111` (`assertEquals(0L, revoked.fenceToken(), "token must be 0 after revoke")`) and grep for any other `0L`-token assertion. The test's real requirement (`:122` — *"fenceToken must increment monotonically after revoke"*) is unaffected.

**Verify:** the existing fencing suite stays green with the updated assertion; add a case asserting `token` is **strictly non-decreasing** across acquire → renew → revoke → acquire.

---

## Batch B — Durable fencing authority (the 3 CRITICALs + P3-326)

### B1. DDL change — `code/01_platform/02_sql/ddl/11_execution_gate.sql`

Add to the WITH block (`:87-96`):

```sql
    'table.merge-engine' = 'versioned',
    'table.merge-engine.versioned.ver-column' = 'fence_token',
    'table.delete.behavior' = 'ignore',
```

`delete.behavior` is declared explicitly so apply-parity asserts it rather than relying on coordinator injection.

**Keep `-- Schema version: 3`.** The column set, order, and indices are unchanged, so the row contract is unchanged and `SCHEMA_VERSION_V3` / `ExecutionGateColumnsAgreementTest.schemaVersionHeaderIsV3` / `ColumnOwnershipAgreementTest` stay valid. Only *table options* change. Record the merge-engine adoption in the DDL header as a v4 note (options, not schema). This is deliberate — a version bump would churn three agreement tests for no contract change.

### B2. Make every write path fail loud (the safety-critical half)

Add a read-after-write verification to the writes that currently have none, so a VERSIONED-rejected write can never be silent:

- Introduce a private `verifyPersisted(partitionId, token, owner)` next to `verifyFence` (`FlussGateStateStore:201`), reading the durable row and asserting the write landed or a **newer** token superseded it.
- Wire it into `revoke` (`:214-221`), `approve` (`:223-228`), `approveAndEnableIfComplete` (`:235-242`) and `halt` (`:244-251`) after their `persistOrRollback` calls.
- **`halt` must mint a strictly greater token** so the server can never drop it: in `InMemoryGateStateStore.halt` (`:244`-ish, the two `withFenceCleared` branches) mint `fenceSequence.incrementAndGet()` and write it with `owner = null`. This makes halt always accepted (token > durable at read time) **and** invalidates any stale holder's token — a real safety improvement, and it is what makes the halt path immune to hazard 2.
- `revoke` keeps its token; an equal-version write is accepted (`compare <= 0`). If a concurrent acquire already minted higher, the revoke is dropped — which is *correct* (the newer owner is authoritative) — so `verifyPersisted` must accept "superseded by a newer token" for revoke and fail loud otherwise.

**Verify (falsifiable, offline):** a `FaultWriter`-style stub is not enough here — this needs the real merge. Add a test that writes a stale token against a newer row and asserts the store **throws** rather than returning success. Falsify by removing `verifyPersisted` → the call must silently succeed, proving the guard is what catches it.

### B3. Table recreate (destructive; no ALTER exists)

`DdlApplyTool` only CREATEs and refuses a non-empty catalog (exit 3); there is no ALTER anywhere in the repo. `P3-010/012/013` therefore need a **drop + recreate**, modelled on the CHG-117 `raw_table_1` v3 recreate:

1. Archive the lake prefix first (a recreate against a stale iceberg dir throws `LakeTableAlreadyExistException`):
   `bash code/01_platform/04_scripts/r2-move-prefix.sh lake/default/Execution_Gate/ lake/_stale-<date>/Execution_Gate/`
2. Drop + recreate. CHG-117 used `code/01_platform/04_scripts/fluss-repair/RawTableAdmin.java` (`drop` / `create` from a descriptor mirroring the DDL). There is **no gate equivalent** — add `GateTableAdmin` alongside it, with `descriptor()` mirroring the new WITH block and the schema built from `ExecutionGateColumns.NAMES`/`TYPE_ROOTS` so it cannot drift.
3. Re-apply through the offline gate to prove manifest + parity agree: `make ddl APPLY=1 EVIDENCE=<file>`.
4. Gate KV fence state is **lost**; audit survives (Iceberg lake + `GateStateStore` audit log). Live money is `HALTED` by default (DEC-044), so a clean start is acceptable — same call as CHG-044.

### B4. Manifest + change control (easy to miss)

- **Regenerate the manifest — it is a byte hash, not an option list:**
  `python3 code/01_platform/04_scripts/ddl_apply.py --force` (edit → `schema_manifest.json:105` `ddl_sha256`). Skipping this fails `CompatFlussDdlParityIntegrationTest:151-152`, `DdlApplyTool.java:202-214`, `make ddl` (exit 5), and docs-audit C1.
- **File `CHG-122`** in `docs/05_deployment/change-records/` with all six fields (`affected_artifacts` naming `code/01_platform/02_sql/ddl/11_execution_gate.sql` + `schema_manifest.json`, `compatibility_class: INCOMPATIBLE`, `savepoint_impact`, `test_updates`, `rollback_behavior`, `plan_tasks`). Validated by `change_control_check.py`; enforced as docs-audit **C14**.

### B5. P3-326 — CAS arm

With server-side version filtering in place, the *"CAS is not expressible"* rationale no longer holds for the monotonic case. Re-evaluate with B1–B2 landed; the LWW-put decision should be restated as "write-ordering is now server-enforced on `fence_token`" rather than "impossible".

### B6. Live drill (the empirical unknown)

**VERSIONED + `table.datalake.enabled` is untested upstream** — no test or doc covers the combination, and the gate has datalake on. This must be proven on a live cluster, not assumed: `@Tag("integration")` + `FLUSS_BOOTSTRAP` gating, `compat_test_*` names, drop in `@AfterAll`.

Assert, in order:
1. the table creates with the new options (`CompatFlussDdlParityIntegrationTest` covers option parity);
2. `delete.behavior` is effectively `ignore`;
3. a **stale-token write is dropped** and our `verifyPersisted` throws (falsify by removing the guard);
4. **`halt` still lands** under a concurrent-stale-token scenario;
5. no DB/table leak; reactor clean.

---

## Batch C — Resource reuse (7): `P3-065`, `P3-068`, `P3-072`, `P3-365`, `P3-367`, `P3-371`, `P3-502`

The prescribed *"cache and reuse a single Lookuper"* is **unsafe as written**: `Lookuper` is `@NotThreadSafe` (`Lookuper.java:43`) and gateway dispatch is pooled. And *"close per call"* is **impossible** — `TableWriter` declares only `flush()` (`TableWriter.java:39`); only `BatchScanner` extends `Closeable`.

Correct, split by confinement:
- **`FlussGateStateStore`** (`P3-365/367/371`): every method is `synchronized`, so a single cached `Lookuper` + `UpsertWriter` field **is** thread-safe by monitor confinement. Cache both, document *why* it is safe here.
- **`FlussProjectionLedgerStore` (`P3-068`), `FlussControlStateStore` (`P3-065`), `FlussProjectionWriter` (`P3-072`), `FlussPostbackQuarantineStore` (`P3-502`)**: not synchronized → use a **bounded pool** (`ArrayBlockingQueue`, fixed small size, created lazily, drained on `close()`), not one shared instance and not a `ThreadLocal` (the executor is bounded but ThreadLocal retains handles after threads die).
- If measurement shows churn is immaterial at these rates, closing with a measured rationale is acceptable — but the rationale must cite a number, not the CHG-121 reconcile figure (a different store at ~68 lookups/s).

**Verify:** a reuse test (N calls → pool size stays 1 per concurrent borrower; the same instance is returned) and a leak test (after `close()`, every pooled handle is released). Falsify by reverting to per-call → the reuse assertion fails.

---

## Batch D — Functional gaps (5)

- **`P3-102`** `ProjectionLedgerStore.incomplete()` (`:61`) — add a paged variant (`incomplete(limit, afterEventId)`); keep the unbounded form for compatibility or migrate callers.
- **`P3-169`** `FlussPostbackQuarantineStore.all()` is process-local — add the **scan-backed** read path so quarantine history survives restart (this is the substantive half; documenting it was not enough).
- **`P3-410`** `PostbackQuarantineStore.all()` (`:27`) is unbounded on an append-only LOG — add a bounded/paged read and stop mirroring every row into an in-memory `ArrayList`.
- **`P3-079`** `GatewayProtocol` — implement the length-prefixed canonical encoding (v2) the newline backstop only mitigates.
- **`P3-150`** `GateStateStore.revoke()` (`:121`) returns a bare `GateRow` — introduce an outcome type (reuse the `FenceResult`/`ApprovalResult` pattern already in this package).

---

## Batch E — Mechanical / close-out (10)

- `P3-142`, `P3-143` — add the hard throw on an empty `authorizedApprovers` set; keep a explicitly-named test overload for the drill seam (`FlussGateStateStore:90` currently delegates with `Set.of()`).
- `P3-364`, `P3-369`, `P3-370` — thread real timestamps through `GateRow`: stop leaving `DETECTION_TIME` null (`FlussGateStateStore:302`) and stop faking `TRANSITION_TS` from `fenceAcquiredTs` / `0L` on a NOT NULL column (`:312`). `withState` currently drops its ts param (`GateRow:86`).
- `P3-101` — `ProjectionLedgerStore.lookup()` nullable: either return `Optional` or restate the decision explicitly (the code comment says intentional).
- `P3-383` — `InMemoryGateStateStore` retained `SAME_PRINCIPAL`/second-`ALREADY_APPLIED` arms: justify in-code or collapse.
- `P3-515` — narrow `throws Exception` on `PostbackQuarantineStore.append`/`all`.
- `P3-086`, `P3-091` — **close as an intentional design decision** (user-confirmed): recovery is replay-on-restart, made safe by the durable dedup ledger (`Execution_Intent_Processed`). Replace the "Partial — deviated" notes with a rationale, and delete the stale `TODO: enqueue for bounded retry` suggestion in the finding's Fix block so it stops reading as unfinished work.

---

## Verification ladder (every batch)

1. `mvn -B test -o` (full reactor, offline) — **581 / 469 / 95** baseline, zero failures. Guards must run here, not in the gated suite.
2. Live drills: `FLUSS_BOOTSTRAP=… mvn test` gated subsets (`common` + `06_execution_gateway`, ingestion if touched) — 597 / 95 baseline.
3. **No leaks:** `ClusterCleanCheck` / `ListTables` → 31 tables, zero `compat_test_*` residue, no new databases.
4. `bash code/01_platform/04_scripts/flush_guard.sh .` + `make docs-audit` (C1/C9/C14) + `make full-audit`.
5. **Falsify every new guard**: restore the violation, prove the guard fails, revert.
6. Tick the ledger with dated evidence and run `python3 p3_map_refresh.py` (the Implementation map is derived — do not hand-edit it).

---

## Ordering and risk

```
A (token monotonicity, offline)
  └→ B1 DDL → B2 fail-loud writes → B3 recreate → B4 manifest+CHG → B6 live drill
                                                        └→ B5 P3-326 re-evaluation
C (reuse, independent) — can run parallel to B
D (functional gaps)    — independent
E (mechanical)         — last, cheapest
```

**Highest risk, in order:** B2 (a silently-dropped `halt` is a safety regression if the verification is incomplete) → B6 (datalake+VERSIONED is unproven upstream) → B3 (destructive recreate loses gate KV fence state) → C (a wrong reuse mechanism turns a perf finding into a correctness bug).

**Residual that stays open by design:** simultaneous double-acquire (equal-version acceptance) — deployment-owned, ASM-EXE-005, to be restated as an enforced artifact rather than a comment.

## Notes

- None of the current work is committed: **78 modified + 22 partially-staged + 17 untracked**, last commit `2026-09-10 16:05`. Commit after each batch so the 239 existing "Verified 2026-09-11" ticks have a commit behind them.
- `B4HaltedIntentConsumeDeferE2ETest:216-237` builds a **hardcoded 15-column** gate descriptor, already drifted from the 17-column DDL (missing `approval_1/2`). Not forced by this change, but it is an unpinned second descriptor — fold it onto `ExecutionGateColumns` while in the area.
- Carried over from the prior session, still open and unaddressed here: `stop_grace_period` is set on ingestion only; `FlussGateStateStore` holding its monitor across Fluss I/O is *the* mechanism Batch B hardens further (B2 adds three more read-backs inside the same monitor) — if acquire/renew/halt latency matters, that belongs in a follow-up.
