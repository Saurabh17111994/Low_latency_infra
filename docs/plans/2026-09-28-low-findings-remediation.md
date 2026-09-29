# Low findings remediation — audit 2026-09-28 (scope doc)

**Created:** 2026-09-28 · **Status:** proposed — awaiting operator approval; no code changed by
this doc.
**Source:** `logs/full-project-audit-20260928/REPORT.md` — every Low row (the 17 `- L:` bullets;
the report header's "~15 Low" was approximate).
**Related:** Critical `…-critical-findings-remediation.md` (C1–C3), High
`…-high-findings-remediation.md` (H1–H6), Medium `…-medium-findings-remediation.md` (M1–M7) land
first and separately; this doc composes with them.
**Execution protocol:** `docs/plans/2026-09-24-plan-execution-protocol.md` (verify-first, marker
discipline, failing-first tests, one CHG per commit, smoke-before-run, tracker hygiene).
**Operator goal:** each defect fixed once at its root boundary, with a permanent machine guard and
a test that fails if the defect returns. No mode the operator must remember, no firefighting.

## 0. Live tracker

This section is the single source of truth for this wave; the body sections below are the evidence.
Where a body section and this list disagree, this list wins.

**Marker legend**

| Marker | Meaning |
|---|---|
| `[x]` | landed and verified (evidence named) |
| `[~]` | in progress right now |
| `[ ]` | action owed — doable offline / on the local stack |
| `[L]` | action owed that needs the live VM / a real rehearsal |
| `[?]` | needs an operator decision before it can start |
| `[-]` | no action — trigger-gated or superseded (trigger recorded) |

**Roll-up**

| Stage | Tasks | done | wip | todo | live | decide | skip |
|---|---|---|---|---|---|---|---|
| L1 — execution: mass-status honesty | 1 | 1 | 0 | 0 | 0 | 0 | 0 |
| L2 — ops tooling: lint scope and tracker wiring | 2 | 1 | 0 | 1 | 0 | 0 | 0 |
| L3 — compute: doc currency, gate pins, watchdog, tick features | 4 | 0 | 0 | 4 | 0 | 0 | 0 |
| L4 — ingestion: config doc parity and final-report atomicity | 2 | 0 | 0 | 2 | 0 | 0 | 0 |
| L5 — common/mock: semantics and naming truth | 4 | 0 | 0 | 4 | 0 | 0 | 0 |
| L6 — infra/DDL: guards, parity, docs | 4 | 0 | 0 | 4 | 0 | 0 | 0 |
| **Total** | **17** | **2** | **0** | **15** | **0** | **0** | **0** |

#### L1 — execution: mass-status honesty

- [x] **L1-1** — Mass-status becomes honest instead of empty: override `generate_mass_status` →
  one `record_tick()` + WARN `mass_status=unavailable` + `Ok(None)` (Nautilus's native "no mass
  status available" — the OMS skips reconciliation instead of reconciling against fabricated
  emptiness, and startup does not abort); the two granular generators return a typed
  `MASS_STATUS_UNSUPPORTED` error, never `Ok(vec![])`; fix the dossier claim and the soak comment
  (1 tick/round, not 3). (Known P3-200; real serving stays B7/Workstream-D work.) — CHG-418

#### L2 — ops tooling: lint scope and tracker wiring

- [ ] **L2-1** — One lint enumeration for both sites: `git ls-files '*.sh'` from the repo root
  (exclude `target`/`third_party`), replacing the `code/`-rooted `find` and its code-rooted
  fallback; an empty enumeration fails closed; the Prometheus float-trap scan widens to the repo
  root; root entry scripts (`start-all.sh`, `run-ingestion.sh`, `show-ticks.sh`) are now checked.
  (A one-time lint repair of those scripts ships in the same commit if needed.)
- [x] **L2-2** — `plan_tracker.py` gains a consumer and nested-marker support: shared marker regex
  `^\s*- \[(.)\]` in both the count and the unknown-marker validation; gate step 3 test discovers
  tracker plans by anchored `^## 0\. Live tracker\s*$` + the Overview section heading, asserts
  discovery ≥ 2 and
  includes the AGENTS-named trackers, then runs `--check` per plan (no gate-script edit, no new
  numbered step). Unit tests cover nested markers, stale tables, prose traps, round-trip. — CHG-372

#### L3 — compute: doc currency, gate pins, watchdog, tick features

- [ ] **L3-1** — Dossier dedup currency: replace the `DEDUP_TTL_MS=60000` / `SIG-UNIT-003` rows
  and recipe export with `DEDUP_WINDOW_ENTRIES=200` (DEC-054), keep `SIG-UNIT-003` only as the
  requirement id, and extend the banner so all later TTL mentions are historical; guarded by a
  dossier-currency test (joins M3-4's parity test if that lands first).
- [ ] **L3-2** — The restore-graph UID pins actually run in the gate: step 16 gains a targeted
  second invocation with `COMPUTE_INT_TEST_P6=true` + `FLUSS_BOOTSTRAP(_SERVERS)` running
  `SignalJobOperatorUidTest,TradeDecisionsSinksUidTest`, plus a `require_class_clean` guard
  asserting `N>0 / Failures: 0 / Errors: 0 / Skipped: 0` per class — a dead cluster or missing
  flag fails instead of skipping. Record the conflict with the 2026-09-18 decision not to touch
  step 16 in `01-foundation.md`.
- [ ] **L3-3** — Idle-watchdog state is per subtask, not per construction: introduce
  `SourceIdleWatchdogState` (episode latch + job last-event clock) held by a serializable
  supplier; the first generator created in a subtask stamps it and every later split generator on
  that subtask shares it; the constructor resets are deleted, so a runtime split can no longer
  re-arm the latch and mask idleness.
- [ ] **L3-4** — Snapshot-fallback tick features stop being inflated 6×: feed `onTick` only for
  the canonical tick timeframe (FIFTEEN_S) when resolving `CandleLiveColumns.TF`, while all six
  TF rows still fan out to strategies; `compute.features.updates.tick` increments only on an
  actual update (no metric change needed); a malformed TF row is skipped and counted like the
  close path. Dossier sentence updated.

#### L4 — ingestion: config doc parity and final-report atomicity

- [ ] **L4-1** — Ingestion dossier/config parity: drain deadline documented as **2 s** default
  (range 1–300, B130 shared close budget; was 30 s); the `DEPLOY_ENV` row corrected to canonical
  `DEPLOYMENT_ENV` (fail-closed on blank, legacy alias, compose/entrypoint default `dev`); fix the
  ING-FAIL-010 default/range and the ING-TCP-003 citation (deleted NDJSON tests → surviving
  `tickcounts*_test.go`); append "superseded by B130" to the dated configuration audit. Guard: a
  dossier↔config parity test with a fail-closed key registry.
- [ ] **L4-2** — The tick-count final report can no longer be clobbered or truncated:
  `reportTickCounts` takes the report lock **first**, then snapshots under the counter lock, so
  the file always holds the newest complete snapshot; writes go through a temp-file + fsync +
  atomic rename helper (a crash mid-write can no longer leave a 0-byte report); the final `Once`
  stays; a test hook sits after the snapshot under the lock.

#### L5 — common/mock: semantics and naming truth

- [ ] **L5-1** — `requiresHalt(STALE)` tells the truth: `REGRESSION || CONFLICT || UNKNOWN`
  (STALE is a non-halting soft reject, matching every production switch); javadoc and the stale
  `PositionProjector` prose corrected. Lands with/before M1-6 so one semantic vocabulary holds.
- [ ] **L5-2** — Safety-halt parser never saturates: reject via the true exclusive bound
  `2^63` (`0x1p63`) — non-finite, non-integral, below `-2^63`, or ≥ `2^63` all throw; the Float
  branch widens to `double`; the largest representable value below the bound is accepted exactly.
- [ ] **L5-3** — Positions average-price encoding one truth: the DDL sentence becomes
  "`average_*_paise = 0` iff the matching quantity is 0; legacy SQL NULL reads as 0"; the
  `PositionSnapshot` constructor enforces the invariant; the gateway writer canonicalizes
  null→0 when the matching quantity is 0 and rejects null-with-quantity>0 (fail-closed). No
  reader distinguishes 0 from NULL today, so no consumer changes.
- [ ] **L5-4** — The common `FlussProjectionLedgerStore` is deleted (it opens a Connection/Table
  then delegates to an in-memory map — the name promises durability it does not provide). The
  in-memory oracle (`InMemoryProjectionLedgerStore`) stays; durability belongs to the gateway
  store + `Postback_Projection_Ledger` single writer (H1-1/M1-6 keep that invariant). Guard: a
  gate-discovered check that no Fluss-named ledger exists outside the gateway and the common
  interface is only implemented in memory.

#### L6 — infra/DDL: guards, parity, docs

- [ ] **L6-1** — Dev compose refuses an empty O2 root password: `ZO_ROOT_USER_PASSWORD:
  ${O2_PASSWORD:?set O2_PASSWORD in .env/secrets.env (make env + secrets-bootstrap.sh)}`; the
  example keeps `O2_PASSWORD=` empty with a REQUIRED comment (no placeholder that satisfies the
  guard); guard asserts the `:?` form + empty example + the stack's existing guard.
- [ ] **L6-2** — `29_position_state.sql` gets `'table.kv.format-version' = '2'` (composite PK +
  subset bucket key is writable only with v2; six other DDLs already carry it), and
  `DdlApplyTool.isPredictedLimited` is tightened to require the option for that cell; regenerate
  the manifest offline **after M6-1** (one shared regeneration); guard: a DDL invariant test over
  every composite KV + the updated prediction test.
- [ ] **L6-3** — The `05_instruments/README.md` stops describing a tree that isn't there: rewrite
  to the real loader contract (`INSTRUMENT_MANIFEST_PATH`; compose host bind; stack Swarm config),
  delete the stub/`instruments.csv` claims, the removed `ARROW_INSTRUMENT_MANIFESTS` key, and the
  dead `manifests/README.md` / `docs/09_data_gaps.md` links. Guards: extend
  `test_p5_env_contract.py` + a gate-discovered README link/key test.
- [ ] **L6-4** — Swarm ZooKeeper keeps its transaction log: add `zk-datalog-1/2/3` volumes and
  `/datalog` mounts beside `/data` for all three members (compose already does this — the 2026-09-05
  catalog-loss class); guard: the existing stack volumes test gains data+datalog target parity
  against the compose deck (reuse H3-2's parity helper if it lands first).

## Overview

This is a six-region wave of truth-and-guard fixes: no new features, no runtime risk on the money
path, every item ending in a machine check.

| Group | Boundary that failed | Fix boundary | Permanent guard |
|---|---|---|---|
| L1 | unserved reconciliation looked like an empty broker snapshot | declare mass status unavailable in Nautilus's native channel; typed error on direct calls | client test + hosted-loop/soak no-abort legs |
| L2 | lint scope missed the entry scripts; the tracker had no consumer | one root-scoped enumeration; nested-aware tracker wired into gate step 3 | scope test + tracker unit/wiring tests |
| L3 | the dossier outlived DEC-054; the UID pin never ran; watchdog state was static; fallback inflated tick features | doc truth, gate-run pin, per-subtask state, single-TF tick feed | dossier parity + class-clean gate guard + watchdog/tick tests |
| L4 | docs drifted from code; the final report could be overwritten | code-truth docs + parity test; lock-held snapshot + atomic rename | config parity test + concurrency/atomicity tests |
| L5 | helper semantics, a saturating bound, an encoding sentence, and a misleading class name | align to call-site truth; reject instead of saturate; code-as-truth; delete the false store | truth-table + boundary + agreement + durability-boundary tests |
| L6 | a bare secret was allowed; a KV option was missing; a README described a dead tree; ZK lost its txn log | fail-closed guard; v2 option + predicate; README rewrite; datalog volumes | compose/stack guards + DDL invariant + README test |

## 1. Verify-first (reproduced on `5b4725d2`)

All 17 items were re-verified against the working tree by six read-only design agents (the
load-bearing rows re-checked by hand). Every verdict is REPRODUCES.

| Report row | Plan | Verdict | Evidence (abbreviated) |
|---|---|---|---|
| Exec L | L1-1 | REPRODUCES | Both granular generators `record_tick()` then return `Ok(Vec::new())` (`client.rs:1103-1124`); no `generate_mass_status` override. |
| Ops L | L2-1 | REPRODUCES | `Makefile:521` `find code …`; gate step 1 `cd "$CODE_DIR" && find . …` + code-rooted fallback; root scripts are tracked outside `code/`. |
| Ops L | L2-2 | REPRODUCES | No consumer of `plan_tracker.py` anywhere; the count/validation predicate is `startswith("- [")` (column 0), so indented markers vanish. |
| Compute L | L3-1 | REPRODUCES | Dossier `:373,:640,:795,:812,:1041` carry `DEDUP_TTL_MS=60000`/`SIG-UNIT-003`; truth is `DEDUP_WINDOW_ENTRIES=200` (foundation/runbook already mark TTL deprecated). |
| Compute L | L3-2 | REPRODUCES | `SignalJobOperatorUidTest:42` `@EnabledIfEnvironmentVariable(COMPUTE_INT_TEST_P6)`; the gate script never sets it (zero hits). |
| Compute L | L3-3 | REPRODUCES | `EPISODE_REPORTED`/`JOB_LAST_EVENT_WALL_CLOCK_MS` are static and reset in the constructor (`:117,:127,:173`). |
| Compute L | L3-4 | REPRODUCES | Fallback wires `hostLive = multiTfLive` (six TF rows) into `updateFeaturesOnTick` (`SignalJob:327-329`; `StrategyHostFunction:199`), so the tick counter reads 6 per snapshot. |
| Ingestion L | L4-1 | REPRODUCES | Code default `DRAIN_DEADLINE_SECONDS` = 2 (range 1–300); dossier says 30 s; dossier `:89` calls `DEPLOY_ENV` optional while `:223` documents blank=fail-closed. |
| Ingestion L | L4-2 | REPRODUCES | `reportTickCounts` snapshots under `tickCountsMu` then takes `tickReportMu` (write order ≠ data age); `os.WriteFile` truncates in place. |
| Common L | L5-1 | REPRODUCES | `requiresHalt` returns true for STALE while every production switch (and its own javadoc) treats STALE as non-halting. |
| Common L | L5-2 | REPRODUCES | Bound check `d > Long.MAX_VALUE` (i.e. `≥ 2^63` as doubles passes); `longValue()` saturates to `Long.MAX_VALUE`. |
| Common L | L5-3 | REPRODUCES | `10_positions.sql:23-25` says "NULL iff matching quantity 0"; writers pass primitive `long` (always 0), no reader distinguishes NULL. |
| Common L | L5-4 | REPRODUCES | Common `FlussProjectionLedgerStore` opens Connection/Table and delegates `lookup` to `InMemoryProjectionLedgerStore`; `open()` has zero callers. |
| Infra L | L6-1 | REPRODUCES | Compose `:1318` `${O2_PASSWORD}` unguarded while the stack uses `:?`; `.env.example:162` empty. |
| Infra L | L6-2 | REPRODUCES | `29_position_state.sql` WITH block lacks `table.kv.format-version` (comment only); six other DDLs carry it; the prediction predicate ignores options. |
| Infra L | L6-3 | REPRODUCES | `05_instruments/` holds only `README.md` + `split_manifest.py`; README claims `instruments.csv`, `manifests/README.md`, `docs/09_data_gaps.md`, and the removed `ARROW_INSTRUMENT_MANIFESTS`. |
| Infra L | L6-4 | REPRODUCES | Stack ZK members mount only `zk-data-N:/data`; compose mounts `zookeeper-datalog:/datalog`. |

## 2. Root cause (one sentence each, grouped)

- **L1** — the adapter overrode Nautilus's granular generators with harmless empties instead of
  using the framework's "mass status unavailable" verdict, so an unserved reconciliation was
  indistinguishable from a genuinely empty broker.
- **L2** — both lint enumerations were written before the root entry scripts existed, and the
  tracker parser anchors markers at column 0 with no consumer to notice either.
- **L3** — DEC-054 changed code/banners but not the dossier's config tables; the UID pin's only
  runner stayed the manual P6 recipe; episode state was made static for the embedded run while
  the supplier constructs a generator per split; and the fallback fed all six TF rows to an input
  contract that expects one tick feed.
- **L4** — B130 and the canonical-key change updated code/tests but not their prose copies, and
  the tick-report serialization guarded the write while leaving the snapshot and the truncating
  write unsafe.
- **L5** — a helper outlived the call-site semantics it was replaced by; the parser compared
  against the rounded double of `Long.MAX_VALUE`; the DDL described an encoding the model never
  produced; and a class was kept under a name whose durability lives elsewhere.
- **L6** — the dev deck interpolated a required secret with the silent-empty form; one KV option
  was not carried while the prediction predicate read only half the matrix rule; a README kept
  pre-retirement references; and the post-incident ZK persistence fix reached compose only.

## 3. Recommended design (native; no new dependency)

### L1 — execution

**Mass-status honesty.** Override `generate_mass_status`: `record_tick()` once, log
`mass_status=unavailable`, return `Ok(None)` — Nautilus's native "no mass status available", which
skips reconciliation for this client instead of reconciling against fabricated emptiness and does
not abort startup (unlike `Err`). The granular `generate_fill_reports` /
`generate_position_status_reports` return a typed error with
`MASS_STATUS_UNSUPPORTED` so no direct caller can mistake emptiness for "no fills". Serving real
mass status (token→instrument mapping, Arrow normalizers) stays B7/Workstream-D. Update
`05-execution-core.md:490-491` and the soak comment (one tick per round).

### L2 — ops tooling

1. **Lint scope.** Replace both enumerations with `git ls-files '*.sh'` rooted at the repo,
   excluding `target`/`third_party`; a failing/empty enumeration must FAIL (the gate already
   does; the Makefile currently passes vacuously). Extend the Prometheus float-trap scan to the
   repo root. If `shellcheck -S warning` surfaces pre-existing warnings in the root scripts,
   repair them in the same region commit (precedent: T1.10).
2. **Tracker wiring.** One marker regex `^\s*- \[(.)\]` used by both `parse()` and the
   unknown-marker scan; a gate step-3 test discovers plans by anchored
   `^## 0\. Live tracker\s*$` **and** the presence of the Overview section heading (guarding
   against the
   substring trap in the protocol doc), asserts discovery ≥ 2 and includes the two AGENTS-named
   live trackers, then runs `plan_tracker.py --plan P --check` per plan. Unit tests cover nested
   markers, indented unknown markers, stale tables, and `--write`/`--check` round-trips.

### L3 — compute

1. **Dossier dedup currency.** Rewrite `:373` (config row), `:640`/`:812` (validation wording,
   DEC-054 cited, `SIG-UNIT-003` as requirement id only), `:795` (`DEDUP_WINDOW_ENTRIES=200`),
   delete the dead env from `:1041`, and extend the `:78-80` banner to cover every later
   `DEDUP_TTL_MS`/`60 000`/`StateTtlConfig` mention. Guard with a section-aware dossier-currency
   test (historical-marked sections and inline markers exempt): the live config table must hold
   exactly one `DEDUP_WINDOW_ENTRIES` row equal to the Java constant, and any `SIG-UNIT-003` line
   must name it. If M3-4 lands first, these legs join `test_compute_config_doc_parity.py`.
2. **UID pin in the gate.** Fold into step 16 (no new numbered step): after the existing suite,
   run `mvn -o test -Dtest='SignalJobOperatorUidTest,TradeDecisionsSinksUidTest'` with
   `COMPUTE_INT_TEST_P6=true` and the Fluss bootstrap env, logging to
   `$OUT_DIR/compute-uid-pin.log`; add `require_class_clean` asserting `Tests run: N>0, Failures:
   0, Errors: 0, Skipped: 0` for both classes. Offline `mvn test` stays green (classes remain
   skipped by default). Record the conflict with `2026-09-18-certification-hygiene.md:246-251`
   in `01-foundation.md` before implementing.
3. **Watchdog state.** Package-private `SourceIdleWatchdogState` (latch + job clock) held by a
   serializable supplier installed by `CandleWatermarkStrategy.of`: the first generator in a
   subtask creates and stamps the state, later split generators on that subtask share it; delete
   the static resets; the per-split `lastEventWallClockMs` remains only for the mark-idle edge.
   Document "one WARN per subtask" in the class javadoc.
4. **Tick features.** In `updateFeaturesOnTick`, resolve the timeframe from
   `CandleLiveColumns.TF` and update `PerInstrumentFeatures` only for `FIFTEEN_S` (matching the
   fast tag and the ascending-order contract); other TF rows still fan out to strategies;
   increment `compute.features.updates.tick` only on an actual feature update; a malformed TF row
   is counted as a feature failure and skipped.

### L4 — ingestion

1. **Config doc parity.** Docs-only: drain default 2 s (range 1–300, B130; prior 30 s noted as
   historical), `DEPLOYMENT_ENV` canonical + fail-closed on blank + legacy `DEPLOY_ENV` alias +
   entrypoint `dev` default; ING-FAIL-010 default/range fixed; ING-TCP-003 cites the surviving
   `tickcounts*_test.go`; the dated configuration audit gains a "superseded by B130" note.
   Guard: `tests/test_ingestion_config_doc_parity.py` parses the dossier table and a fail-closed
   registry maps every documented key to a code-source regex + required doc substrings.
2. **Final-report atomicity.** In `reportTickCounts`, take `tickReportMu` first, then copy the
   map under `tickCountsMu`, format and write — the file always holds the newest complete
   snapshot. New `writeTickReportAtomic(path, data)`: temp file in the same directory → write →
   `Sync` → `Close` → `Chmod(0644)` → `os.Rename` (previous complete file survives on error).
   Keep `finalTickCountReport sync.Once`; optionally delete the interval goroutine's early
   `ctx.Done` final call so only main's post-`runHFT` snapshot is authoritative. Test hook sits
   after the snapshot, holding the lock.

### L5 — common/mock

1. **requiresHalt truth.** `STALE` never halts: return `REGRESSION || CONFLICT || UNKNOWN`;
   rewrite the javadoc and the stale `PositionProjector` prose; flip the unit assertion and add a
   STALE-apply case asserting `halted == false` while still quarantined. Land with/before M1-6.
2. **Saturation.** `LONG_EXCLUSIVE_BOUND = 0x1p63`; reject `!isFinite || d % 1 != 0 ||
   d < -bound || d >= bound`; widen Float to double; keep the exact BigInteger/BigDecimal paths.
   The largest representable value below the bound (`Math.nextDown(0x1p63)`) maps exactly.
3. **Nullability.** DDL comment-only fix to the 0-encoding sentence (DDL stays unapplied);
   `PositionSnapshot` constructor invariant (`qty == 0 → average == 0`); the gateway writer
   canonicalizes null→0 when the matching quantity is 0 and rejects null-with-qty>0 (IAE →
   existing quarantine/halt path). No reader distinguishes the encodings today.
4. **Ledger store.** Delete the common `FlussProjectionLedgerStore`; keep
   `InMemoryProjectionLedgerStore` as the differential oracle; document the durability boundary
   (gateway store + `Postback_Projection_Ledger`, single writer, TTL-bounded). Guard: no
   Fluss-named ledger outside the gateway, and the common interface is implemented in memory
   only.

### L6 — infra/DDL

1. **O2 password guard.** One line in compose; keep `.env.example` empty with a REQUIRED comment
   (a placeholder that satisfies the guard would defeat the finding and conflicts with the
   at-rest scan's empty-placeholder classification). Guard: compose `:?` present, example empty,
   stack keeps its guard.
2. **Position_State v2.** Add `'table.kv.format-version' = '2'` to the WITH block; tighten
   `isPredictedLimited` to return "writable" only when the bucket key is a subset **and** the
   option is `2`; regenerate the manifest offline after M6-1 (one shared regeneration, never
   `--apply-verified`). Guard: a DDL invariant over every composite KV (subset ⇒ v2 present) and
   the updated prediction test.
3. **Instruments README.** Rewrite Files/Status/parameterization to the real loader contract
   (`INSTRUMENT_MANIFEST_PATH`; compose host bind; stack Swarm config), remove the stub/CSV
   claims, the removed key, and the dead links; keep `split_manifest.py` documented. Guards:
   extend `test_p5_env_contract.py` (dead key + live keys) and a gate-discovered README test
   (links resolve, no removed key, no "CSV loaded at startup" claim).
4. **ZK datalog.** Add `zk-datalog-1/2/3` volume declarations and `/datalog` mounts for all three
   stack members (distinct per member, mirroring compose). Guard: extend the stack volumes test
   to require `{data, datalog}` targets per member, unique sources, equal to compose's ZK target
   set (reuse H3-2's parity helper if present).

## 4. Permanent guards (the "will not come back" list)

| Item | Guard (test file + rule) |
|---|---|
| L1-1 | `client.rs` test: `generate_mass_status == Ok(None)`, granular `is_err()` with marker, progress tick advanced; hosted-loop/soak legs fail on `Err` or no tick |
| L2-1 | `tests/test_static_check_scope.py`: checked set == tracked set − exclusions for both sites; no bare `find code`/`$CODE_DIR`; empty enumeration fails |
| L2-2 | `tests/test_plan_tracker.py`: nested marker counted; indented unknown → rc 2; prose mention not discovered; stale → rc 1; per-plan `--check` in gate step 3 |
| L3-1 | `test_compute_dossier_currency.py` (or joined into M3-4's parity test): one live `DEDUP_WINDOW_ENTRIES` row == Java constant; historical mentions marked |
| L3-2 | step 16 `require_class_clean` (both FQCNs, `N>0`, skipped 0) + static wiring assertion |
| L3-3 | `SourceIdleWatchdogGeneratorTest`: two generators on one shared state → one episode, no reset; record arrival clears once |
| L3-4 | `StrategyHostFeaturesTest`: six-row feed → `featureTickUpdatesForTest()==1` and FIFTEEN_S value wins |
| L4-1 | `test_ingestion_config_doc_parity.py`: registry maps every documented key to code + required substrings; unknown key fails |
| L4-2 | `tickcounts_concurrent_test.go` (variable map) + `tickcounts_final_test.go`: deterministic clobber, inode change, complete-file reader stress |
| L5-1 | `KvStateUpdateProtocolTest` truth table + `KvStaleWriteRejectionTest` STALE-apply no-halt |
| L5-2 | `SafetyHaltRequestParserTest`: `2^63` double/float, `(double) Long.MAX_VALUE`, `Double.MAX_VALUE` → ParseException; `Math.nextDown(0x1p63)` → exact |
| L5-3 | `PositionsColumnsAgreementTest` sentence pin + `PositionSnapshot` invariant tests + writer null cases |
| L5-4 | `tests/test_projection_ledger_durability.py`: no Fluss-named ledger outside the gateway; common interface in-memory only |
| L6-1 | `test_08_local_compose_prod.py`: compose `:?`, example empty, stack guard present |
| L6-2 | `CompositeKvDdlInvariantTest` (subset ⇒ v2) + updated `DdlApplyToolStatusTest` cell |
| L6-3 | extended `test_p5_env_contract.py` + `tests/test_instruments_readme.py` (links/keys/claims) |
| L6-4 | `test_09_stack.py` ZK volumes: `{data, datalog}` per member, unique sources, equals compose |

## 5. Code map (current lines, for the commit slices)

| Item | Files : lines |
|---|---|
| L1-1 | `execution/client.rs:24-45,281-288,1084-1124`; `engine.rs:260-262,318-338,727-754`; `tests/live_node_soak.rs:85-171`; `05-execution-core.md:488-491`; `07-executor.md:125-130` |
| L2-1 | `Makefile:280,519-544`; `run-monday-gates.sh:75,511-556`; `start-all.sh`, `run-ingestion.sh`, `show-ticks.sh` |
| L2-2 | `plan_tracker.py:24,29-58,61-110`; tracker plans under `docs/plans/`; `2026-09-24-plan-execution-protocol.md:5` |
| L3-1 | `04-signal-job.md:78-80,219,232,259,273,373,640,795,812,1041`; `PlatformConfig` `DEDUP_WINDOW_ENTRIES=200`; `SignalJobConfig.java:1019-1034`; `SignalJobConfigTest.java:19,195-209` |
| L3-2 | `run-monday-gates.sh:152,291-322,1036-1052`; `SignalJobOperatorUidTest.java:27-44,87-146`; `TradeDecisionsSinksUidTest.java:26-95`; `2026-09-18-certification-hygiene.md:246-251` |
| L3-3 | `SourceIdleWatchdogGenerator.java:117-128,143-177,250-275`; `CandleWatermarkStrategy.java:31-52`; `SourceIdleWatchdogGeneratorTest.java:36-135,260-330` |
| L3-4 | `SignalJob.java:288-296,326-331`; `StrategyHostFunction.java:163-164,185-211,435-452`; `PerInstrumentFeatures.java:74-82`; `MultiTimeframeAggregateFunction.java:613-662`; `SignalJobConfig.java:119,180` |
| L4-1 | `IngestionConfig.java:276-281,317-331`; `03-ingestion.md:63,86,89,223`; `11-testing-and-release.md:170,174`; `24-configuration-audit.md:31`; `docker-entrypoint.sh:21-23` |
| L4-2 | `tickcounts.go:23-47,49-56,63-118`; `main.go:218-252,262,281-286`; `tickcounts_concurrent_test.go:14,53`; `p1_review_fixes_test.go:26,47` |
| L5-1 | `KvStateUpdateProtocol.java:39-51`; `PositionProjector.java:26,98-100`; `PositionProjectionWriter.java:99-101`; `OrderLifecycleProjector.java:128-133`; `KvStateUpdateProtocolTest.java:44-56` |
| L5-2 | `SafetyHaltRequestParser.java:139-150`; `SlotSafetyRequest.java:56-57`; `SafetyStateTracker.java:150-153,171-180`; `SafetyHaltRequestParserTest.java` |
| L5-3 | `10_positions.sql:23-25`; `PositionsColumns.java:46-49`; `FlussPositionsStateStore.java:100-101,128-135`; `FlussProjectionWriter.java:215-219`; `PositionsColumnOwnership.java:38-45` |
| L5-4 | common `FlussProjectionLedgerStore.java:1-34`; `ProjectionLedgerStore.java`; `InMemoryProjectionLedgerStore`; gateway store + `GatewayStartup.java:64`; P4-010 |
| L6-1 | `docker-compose.yml:1305-1333`; `.env.example:160-165`; `docker-stack.yml:1267-1272`; `secrets-bootstrap.sh:271-279` |
| L6-2 | `29_position_state.sql:39-57`; `CompositeKeyMatrixVerifier.java:34-46,77-81`; `DdlApplyTool.java:683-700`; `DdlApplyToolStatusTest.java:130-163`; manifest entry |
| L6-3 | `05_instruments/README.md:15-47`; `test_p5_env_contract.py:18-45`; `docker-compose.yml:19-32`; `docker-stack.yml:24-29,59-60` |
| L6-4 | `docker-stack.yml:130-139,181-270`; `docker-compose.yml:335-346`; `test_09_stack.py:285-311` |

## 6. Test plan (failing-first + mutation)

Each item lands test-first (the test must fail on the unmodified tree), the fix turns it green,
one mutation proves it catches the defect's return; mutations run locally and are reverted.

| Item | Failing-first (today) | Mutation (must go red) |
|---|---|---|
| L1-1 | aggregate override absent → default empty composition | delete the aggregate override / restore `Ok(vec![])` |
| L2-1 | scope test red (root scripts unchecked) | re-add `find code`; break `show-ticks.sh` syntax |
| L2-2 | nested marker invisible; no consumer | re-anchor the regex to column 0; substring discovery |
| L3-1 | parity red on the live TTL rows | re-add a live `DEDUP_TTL_MS` row; flip the Java constant |
| L3-2 | class guard red (never runs) | drop the P6 env var → guard red; rename a UID → JUnit red |
| L3-3 | two-generator leg fails (reset) | restore `EPISODE_REPORTED.set(false)` in the constructor |
| L3-4 | six-row leg fails (counter 6, wrong TF wins) | remove the TF guard; count before the guard |
| L4-1 | parity red on 30-vs-2 and "optional" deploy env | restore code default 30; drop the canonical-key assertion |
| L4-2 | order/inode/reader legs red | snapshot outside the lock; `os.WriteFile` instead of rename |
| L5-1 | flipped truth-table red | restore `o != APPLIED && o != DUPLICATE` |
| L5-2 | `2^63` accepted today | restore `d > Long.MAX_VALUE` |
| L5-3 | sentence/ctor/writer pins red | restore "NULL iff"; pass null through with qty>0 |
| L5-4 | durability-boundary test red while the class exists | re-add a delegating common class |
| L6-1 | `:?` absent | revert to `${O2_PASSWORD}`; put a placeholder in the example |
| L6-2 | invariant red on 29; prediction red on v1+subset | delete v2 from a working DDL; drop the option check |
| L6-3 | README test red (dead key/paths/claims) | re-insert the plural key; break one link |
| L6-4 | stack volumes red (datalog absent) | remove a datalog mount; share one datalog volume |

## 7. Rollout and evidence plan

- **Order:** fully offline and independent — any order works. Natural sequence: L2 (tooling can
  then guard the rest), L5-1 before M1-6, L6-2 after M6-1 (shared manifest regeneration), L3-1
  joined into M3-4 if it landed first, L6-4 reusing H3-2's parity helper if present, L1-1 with a
  note that real mass-status serving stays B7/Workstream-D.
- **Commits:** one region commit per item with one CHG each, citing the failing-first test and
  the mutation. L2-1 may include a one-time lint repair of the root scripts in the same commit.
- **Gate absorption:** all new Python tests live in
  `code/01_platform/04_scripts/tests/test_*.py` (gate step 3 auto-discovery); L3-2 folds into
  step 16's existing block; L6-4 rides the existing stack test; no numbered step is added
  (`GATE_TOTAL=19`).
- **Per-area verification:** executor `cargo test --offline`; ingestion `make test` + Go tests;
  compute/mock module suites; scripts `python3 -m pytest code/01_platform/04_scripts/tests -q`
  from the repo root; finish with `make gate` + `make static-check`.
- **Smoke-before-run:** no item needs a run > 5 minutes; L3-2's targeted gate invocation is
  bounded by the two test classes; the soak leg in L1-1 is the existing 30 s default.
- **Evidence:** CHG files carry command + result; L3-2 records the class-clean line from the gate
  log; L6-2 records the manifest regeneration diff (one entry).
- **Never edit code or touch the cluster while a gate run is in flight.**

## 8. Risks and rollback

- **L1-1:** boot log changes from a false "reconciliation succeeded" to the upstream "No mass
  status available" WARN — honest and boot-time only. If the operator prefers hard refusal, that
  requires `reconciliation=false` and breaks paper/E2E; the recommended default is warn-and-skip.
- **L2:** the tracker gate now fails when a live marker is flipped without `--write` (intended;
  AGENTS/protocol already mandate it). Root-script lint may surface pre-existing warnings —
  repaired in the same commit.
- **L3:** the watchdog semantic moves from per-TM to per-subtask (strictly more local, documented);
  the tick-feature fix changes feature state and the counter only — strategies still receive all
  six TF rows; step 16 now needs the stack's `9123` (already a `make gate` prereq) and a dead
  cluster fails loudly on the pin instead of skipping.
- **L4:** docs + test only for parity; the atomic writer serializes snapshot+write (I/O-bounded)
  and a crash may leave a cosmetic temp file (removed on error path).
- **L5:** the helper change is test-oracle-only today (no hidden consumer); the saturation fix
  rejects an exact-top BIGINT delivered as double (safe direction); the nullability fix newly
  rejects only the latent null-with-qty>0 write; deleting the common ledger has no in-repo
  callers.
- **L6:** the compose guard breaks a bare `docker compose up` without `--env-file secrets.env` —
  the sanctioned `make up`/`make day` paths pass both env files; the DDL edits remain
  proposals (never applied) with one offline manifest regeneration; ZK datalog is a stack-only
  addition (no migration; a future deploy creates empty volumes).

## 9. Operator notes (recommended defaults; none blocks the wave)

All noted defaults follow the operator standard (2026-09-28): **low latency · high throughput ·
correctness · low memory · native · no future maintenance or firefighting**. Where the criteria
conflict, correctness wins and the measured steady-state cost decides.

| # | Note | Recommended default |
|---|---|---|
| N1 | L1-1 mass-status behavior | warn-and-skip (`Ok(None)`); hard boot refusal rejected (breaks paper/E2E) |
| N2 | L5-3 average-price encoding | code truth: `0` iff matching qty 0; legacy NULL reads as 0 |
| N3 | L5-4 common ledger store | delete it; durability is the gateway store + `Postback_Projection_Ledger` |
| N4 | L2-1 root-script lint | if `shellcheck -S warning` flags the root scripts, repair them in the same commit |

## 10. Out of scope

- The Critical, High, and Medium waves (separate plans, landing first).
- The dead position-state dashboard and any tracked open item in the audit report §5.
- Applying or migrating DDL; production deploy proof; live-money enablement.
- Real mass-status serving (fills/positions reconciliation) — explicitly B7/Workstream-D.
- Any redesign beyond the listed boundaries (no new services, no new dependencies, no new
  numbered gate steps).
