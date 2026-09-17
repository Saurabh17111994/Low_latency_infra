# Compute-identifier parity — stop 04_scripts referencing names the job graph does not have

## Overview

`code/01_platform/04_scripts/` refers to Flink operator names, uid names and metric
leaf names that exist only because `02_compute` declares them. **Nothing in the
repository joins the two sets.** A topology cutover therefore orphans the script
side silently, and the failure is always soft:

| Orphaned thing | Soft failure |
|---|---|
| metric name in a gate | Flink REST returns `[]` → gate compares `0` vs `0` (passes) or `0` vs `20` (fails for the wrong reason) |
| metric name in a fallback operator list | the list never matches a vertex → falls through to a weaker check |
| metric name in an O2 dashboard/alert | the stream never exists → tile renders empty, **alert never fires** |
| table/name in an analyzer | empty result set → reported as a system failure, not a harness failure |

This has already cost three waves of run-by-run repair, and in every case the
breakage came from a commit that **touched no file under `04_scripts/`**:

* **CHG-191** — `0f3e5952` ("retire 15s candle path") moved
  `compute.candles.late.dropped` behind `MULTITF_ENABLED`; the harness kept
  asserting a counter its own job graph no longer created, so `late_delta` was
  structurally `0` and the inject gate could never pass (run 8).
* **CHG-193** — `pipeline_metric_input_progress`'s fallback matched a
  hand-written operator set two-thirds of which `0f3e5952` had deleted
  (`fingerprint-dedup | candle-15s | forming-bar-*`); run 10 called a healthy
  pipeline dead while `multi-tf-aggregator` held 458 132 records.
* **CHG-194** — G7c's zero-loss parity proof read a table the cutover had
  emptied (`final_rows = []`), so all 22 528 closed windows were reported as
  "NO final candle".

**Desired outcome:** a rename, retirement or flag-gating of any identifier in the
compute job graph fails a fast, cluster-free test that names the offending script
and identifier, before a live run is spent.

**Acceptance criteria:**

1. A new `unittest` suite in `code/01_platform/04_scripts/tests/`, cluster-free,
   runs under `make test-audit-r2` (already inside `gate-fast`) and under the
   Monday gate's Python step with **no Makefile change**.
2. It is **red against today's tree**, naming all 22 dead identifiers in
   `stage-capture.sh` (see Technical Details) and the dead O2 streams in
   `o2-provision.py`.
3. It is green after the cleanup, and a mutation of any of the five legs makes it
   red again (mutation list below).
4. Every identifier referenced by a script resolves to a producer in the compute
   job graph, or is classified in a checked-in allowlist row carrying **source +
   recheck condition**.
5. For every identifier that is gated behind a topology flag, the referencing
   script opts the flag in (or refuses an explicit opt-out) — the contract
   `holistic-measure.sh` L105–129 already writes by hand.
6. A new metric that no script measures fails the suite unless it is explicitly
   recorded as `not-captured` with a reason.
7. One change record filed (`CHG-195`, `scope: gate-behavior, measurement-gate`).

## Context

### Files and symbols

| Path | Role |
|---|---|
| `code/01_platform/04_scripts/stage-capture.sh` | **The worst offender.** `sample_tick()` starts L283; the `custom_names=` list is **L422** — 39 dotted names, **22 dead**. (Both shifted by +30 on 2026-09-17 when the leg-4 availability record landed at L104/L161/L712.) The REST metric-id contract it satisfies is documented just above the list. |
| `code/01_platform/04_scripts/o2-provision.py` | Dashboard tiles + alerts on dead streams: L351–358 (`_compute_candles_late_updates`), L363–364 (`_compute_signals_detected`), L375 + L509 (`_compute_dedup_state_count`, `_compute_dedup_state_bytes_estimate`), L68–73 (the `schema_version` / `schema-version` divergence), L1085 (alert on one of the divergent names). Already has a fail-closed precedent: `validate_command_spec()` **L123**, called at L1559 and L1889, whose whole purpose is "fail fast instead of silently pushing a broken spec to O2" — it just does not cover dashboard/alerts, only `KNOWN_COMMAND_*` (L92/L111). |
| `code/01_platform/04_scripts/holistic-measure.sh` | L105–119 (CHG-191 flag contract, refuse-on-off) and L121–129 (CHG-194 leg, report UNAVAILABLE) — the behaviour the new test formalizes. |
| `code/01_platform/04_scripts/pipeline-lib.sh` | L1163–1166 submits the three topology flags as `-e FLAG="${FLAG:-false}"`. L1469 `pipeline_metric_input_progress()`, L1488–1495 the CHG-193 comment ("Naming no operator cannot rot when operators are added or renamed") — the house style for this class. |
| `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java` | The graph. Names at L223, L228, L240, L259, L278, L314, L327, L333, L344, L382, L393; uids alongside. Gating `if`s at **L271** (`config.multiTfEnabled()`), **L313** (`config.strategyHostEnabled()`), **L380** (`config.executionIntentEnabled() && strategySignals != null`). |
| `.../signaljob/MultiTimeframeSinks.java`, `TradeDecisionsSinks.java`, `CanonicalSignalFilterFunction.java`, `StrategyHostFunction.java`, `ExecutionIntentProducerFunction.java`, `RawValidationFunction.java`, `FingerprintDedupFunction.java`, `IngestLatencyMonitorFunction.java`, `MultiTimeframeAggregateFunction.java` | Every metric literal: `MultiTimeframeAggregateFunction` L159–165, `StrategyHostFunction` L113–129 + L223–224, `FingerprintDedupFunction` L107–108, `RawValidationFunction` L63/L65/L73/L84, `IngestLatencyMonitorFunction` L32, `MultiTimeframeSinks` L277–278, `TradeDecisionsSinks` L183–184, `CanonicalSignalFilterFunction` L75, `ExecutionIntentProducerFunction` L54–59. |
| `.../babysitter/BabysitterJob.java`, `PositionsObservationOperator.java`, `PositionsRowDeserializer.java` | The **Babysitter job** (separate graph): names L143–154, `SOURCE_UID` L54, metrics L108–123 and L37–40. |
| `.../safetyhalt/SafetyHaltJob.java` | `safety-halt-tracker` L91; the `addGroup("safety")` counters L194–197 (`transitions.applied`, `rows.malformed`, `rows.skipped`). |
| `code/01_platform/04_scripts/tests/` | `unittest` `test_*.py`. The gate discovers it at `run-monday-gates.sh` **L584** (`python3 -m unittest discover -s "$SCRIPT_DIR/tests" -p "test_*.py"`) and refuses `Ran 0 tests` at **L589**. |
| `code/01_platform/04_scripts/tests/test_gate_harness_guards_wave36.py` | The most recent test in this shape — the precedent for "a test that pins script ↔ repo truth". |
| `docs/plans/2026-09-16-wave36-holistic-measure-repair.md` | The wave whose Post-Completion section filed CHG-191…194; this plan is the structural answer to its own post-mortem. |

### Verified anchors (do not trust stale line numbers — re-check before editing)

The audit file this plan descends from warns that **709 of 864 findings sit in
files modified after 2026-09-07**, and the wave-36 plan's doc references had
drifted ~3 lines. Every line reference above was re-verified on 2026-09-17. If a
task's anchor has moved, re-locate by symbol name, not by line.

### Existing patterns to reuse

* **Derived manifest with a staleness check** — `02_sql/ddl/schema_manifest.json`
  (per-entry `ddl_sha256`, checker `ddl_apply.py` in `04_scripts`, `gate-fast`
  running `make ddl` first so a corpus byte change with no manifest refresh fails
  in seconds). This plan deliberately does **not** copy it for compute names
  (decision 1) but cites it as the pattern if a manifest is ever wanted.
* **Small pinned policy file beside its enforcer** — `versions.pin`,
  `corpus.sha256`, `version_matrix.yaml` in `04_scripts/`, enforced by
  `pin-check.sh`. This is the pattern the allowlist follows.
* **Ledger rows with provenance + recheck** — `docs/ENVIRONMENT.md` (FACT-nnn:
  claim / proof / `Recheck when`). The allowlist rows use this shape, per that
  file's own rule "expiry or it becomes a lie".
* **Fail-closed stream validation** — `o2-provision.py`'s `validate_command_spec()`
  L123. The cleanup extends its coverage rather than inventing a second mechanism.
* **Explicit-UID pinning, but integration-gated** — `SignalJobOperatorUidTest`
  (`@Tag("integration")` + `@EnabledIfEnvironmentVariable(COMPUTE_INT_TEST_P6)`);
  it pins `.uid()` only and is **skipped in the plain suite and in `gate-fast`**.
  That gating is why this class reached production three times.

### Constraints

* **No Makefile change is needed.** `make test-audit-r2` is
  `python3 -m unittest discover -s code/01_platform/04_scripts/tests -v`, and
  `gate-fast` already calls it. A new `test_*.py` in that directory is picked up
  by both the fast gate and the Monday gate. The test must therefore stay
  **static and fast** (no docker, no cluster, no network) — it runs inside a
  ~20 s gate.
* **`unittest`, not bare pytest functions.** The gate runs
  `python3 -m unittest discover`; a module-level `def test_*()` would not be
  discovered and the suite would pass vacuously while the gate's `Ran 0 tests`
  refusal only catches a wholly empty discovery.
* `set -uo pipefail` in the harness scripts; the new test is Python and does not
  touch them.
* The test must read the Java **source**, not a build artifact — `target/` is
  gitignored and C6-style gates already parse source text.
* `code/02_services/02_compute` is outside the `code/pom.xml` reactor (R-272), so
  no new Java module dependency may be assumed.

## Review Handoff

### Selected approach

**Extract the authoritative set from Java source at test time; check in only what
cannot be derived.**

| Store | Kind | Where | Maintained by |
|---|---|---|---|
| **A — authoritative set** | derived, never checked in | extracted from `02_compute/src/main/java` on every run | the test |
| **B — operator registry** | hand-written, small (~7 rows) | the test file | asserted live by leg 2 |
| **C — foreign-metric allowlist** | hand-written, ~12 rows, name + source + recheck | `code/01_platform/04_scripts/compute-identifiers.allowlist` | legs 3 + completeness |
| **D — condition table** | derived from `SignalJob.java` `if` blocks, completeness-asserted | the test | leg 2/4 |

No derived manifest is checked in. A second copy of A would be one more thing to
forget going stale, and the whole failure being fixed is "a copy of the truth
rotted". C is checked in because a Flink built-in or an agent metric is not
derivable from this repo at all.

### Decisions (resolved 2026-09-17, operator-accepted — do not re-litigate)

1. **Manifest home: neither `04_scripts/` nor `docs/04_contracts/`.** Derive from
   source (no derived manifest); the allowlist lives in
   `code/01_platform/04_scripts/`, beside `versions.pin`/`corpus.sha256`, where
   every other machine-checked pin already lives and where `gate-fast` looks.
   `docs/04_contracts/` is prose for humans — nothing parses a data block out of
   it, and only `full_audit.sh` / `stale_table_kind_scan.py` / `o2-provision.py`
   *scan* it. If a manifest is ever wanted for diff review, it follows
   `schema_manifest.json`: generated, beside the code it describes
   (`code/02_services/02_compute/`), staleness-checked by the cheap gate.
2. **Dead names: delete, do not annotate.** `pipeline-lib.sh`'s CHG-193 fix is the
   house precedent — it *deleted* the hand-written operator set, with the reason
   in the comment. The audit trail already exists in the `SignalJob.java` cutover
   comments, the CHG records and `docs/08_implementation/`. The repo does have a
   managed "retired artifact" idiom (`_template.md`: annotate with the removing
   commit, verified by C14) — but it applies to `affected_artifacts` **paths**;
   these are string literals in a live file, so there is no path to annotate and
   no verifier would run. An unverified `# dead since 0f3e5952` comment is exactly
   the lie `ENVIRONMENT.md` forbids. Exception: a name kept because a **live O2
   org** already has a dashboard on it gets an allowlist row with provenance and
   expiry — a ledger row, not an inline comment.
3. **Flag-awareness: narrow — the three compute topology flags only**
   (`MULTITF_ENABLED`, `STRATEGY_HOST_ENABLED`, `EXECUTION_INTENT_ENABLED`).
   Those are the only env inputs whose value decides whether an *identifier
   exists*. `ALLOW_FULL_REPLAY` / `STATE_RECOVERY_PATH` / `DEPLOYMENT_ENV`
   (CHG-185/187/189) are **value** drift with a different failure mode — the job
   refuses loudly at startup, already fail-closed inside `SignalJobConfig` — and
   are out of scope. A wide net here buys false positives and dilutes the signal.
4. **Foreign metrics: checked-in allowlist, not derived from vendor trees.**
   Deriving would mean inspecting unpacked Flink jars at test time, which no test
   in this repo does, for a set of ~12 stable names. Rows carry name + source +
   recheck condition, per the `ENVIRONMENT.md` rule.
5. **One change record; scope is not `observability`.** `scope` is **not** in
   `change_control_check.py`'s `REQUIRED_FIELDS` and only `compatibility_class`
   is enum-validated, so any string passes — the value is a human convention, and
   `observability` appears in no filed record (precedents: CHG-139
   `observability-and-ownership-common`, CHG-190/191 `deployment, measurement-gate`,
   CHG-194 `gate-behavior, measurement-gate`). The new suite is a **fail-closed
   gate**, so: `scope: gate-behavior, measurement-gate`,
   `compatibility_class: COMPATIBLE_WITH_LIMITATION` (a previously-accepted script
   reference is now refused, and captured-metric coverage changes — name that
   limitation in the body), `savepoint_impact: none`.

### Non-goals

* No Java refactor to a constants class for names. It would touch ~10 production
  files for the same guarantee the source text already provides.
* No change to `pipeline-lib.sh`'s CHG-193 fallback — it is already correct
  (names no operator on purpose).
* No change to `SignalJobOperatorUidTest`'s gating or scope; the `.name()` axis is
  covered by leg 5 in a cluster-free test instead.
* No production/Swarm work. Everything here is host-side tooling and evidence
  lists.

### Assumptions

* The compute graph is built entirely from `02_compute/src/main/java` +
  `common`; no identifier is composed at runtime from config. (Verified for every
  name in store A — they are all compile-time literals. `addGroup("strategy", id)`
  has a dynamic second argument but a literal group, so the path is resolvable.)
* `target/` is absent in a clean checkout, so source parsing is the only option.
* The dev stack (10 `01_docker*` containers) may be up or down; the new suite must
  not care.

### Authorization

Adding a test file, deleting dead names, and filing one change record are ordinary
in-tree work on `main` (verify with `git branch --show-current` first). The O2
divergence check in Task 4 is read-only against the local O2. No `docker push`,
no `stack deploy`, no Swarm secret changes.

## Implementation Steps

> **STAGE 1 LANDED 2026-09-17** — the suite exists and is red, as intended:
> `code/01_platform/04_scripts/tests/test_compute_identifier_parity.py`, **10 tests,
> 0.18 s**, static, cluster-free, `pyflakes` clean. `unittest discover` collects it, so
> `make test-audit-r2` and therefore **`make gate-fast` stay RED until Task 3 lands**
> (one failing test: leg 1).
>
> **Leg 4 FIXED 2026-09-17** (separate from Task 3 — asked for on its own). The leg was
> wrong in shape: it demanded a flag opt-in from every script, but `stage-capture.sh`
> attaches to a job it did not start and cannot change, so "opt the flag in" is a lie for
> it. Leg 4 is now split by what a script is allowed to do — a **driver** (sources
> `pipeline-lib.sh`, so it chooses the topology) must opt in or refuse; a **capture**
> must RECORD. `stage-capture.sh` now derives the branch state from the running job's own
> vertex map (the flags reach the `flink run` **client**, so they are persisted on no
> container and cannot be read) and writes `metric-availability.tsv` — one
> `present=yes|no` row per requested name per sample — plus a one-time WARN naming every
> absent identifier and the observed topology. `run-meta.txt` gains
> `topology_branches` / `topology_operators`. Smoked against a stub REST
> (`/tmp/avail_smoke/`); `make static-check` 0 failures.
>
> **Corollary the record exposes:** `stage-capture.sh` captures a *SignalJob*, but its
> list requests 6 `babysitter.positions.*` counters and 3 safety counters
> (`rows.malformed`, `rows.skipped`, `transitions.applied`) that live in the
> **BabysitterJob / SafetyHaltJob graphs** — those 9 names can never appear in this
> capture. They are now recorded as `present=no` with the reason instead of being
> silently missing; whether to delete or repoint them belongs to Task 3.
>
> Stage 1 replaced the planned extractor with a **hardcoded snapshot + provenance
> anchors** (`EXPECTED`: 60 rows, each naming its declaring file and the literal that
> must still be in it). That keeps this decision-1-compatible — no derived manifest is
> checked in — while failing immediately instead of waiting for a parser. The
> `(kind, name, condition, anchor_file, anchor_literal)` row shape is exactly what the
> extractor in Task 1 (now stage 2) will produce, so the swap is mechanical.
>
> **Two scanner findings that are part of the design, not incidental:**
> * Dotted namespaces are `compute|babysitter|preview|rows|transitions` — deliberately
>   **without `safety` and `strategy`**, because both are common config words as well as
>   metric groups (`strategy.type`/`strategy.fixed` are Fluss restart-strategy keys,
>   `safety.state` is a command-spec stream). The stream form disambiguates them.
> * The bare form is limited to `compute_`, and only quoted literals are scanned — that
>   is what excludes `compute_env`, `compute_manifest_entries` (unquoted code) and
>   `rows_s`, `rows_min`, `safety_2`, `strategy_type` (variable names) with **no
>   allowlist entries**.
> * Documented limitation: bare non-`compute_` stream spellings (e.g.
>   `strategy_host_candidates_current_sink`) are not scanned in stage 1.
>
> **Stage-1 result: 43 true findings, 0 false positives, across 2 failing tests.**
> * Leg 1 — 39 dead references: `stage-capture.sh:422` (the 22 predicted names);
>   `o2-provision.py` (4 dead streams — `compute_candles_late_updates`,
>   `compute_signals_detected`, `compute_dedup_state_count`,
>   `compute_dedup_state_bytes_estimate`); and **one new finding** —
>   `compute_kv_filtered_noncanonical` at L442/443/901/902 is missing `signal_`
>   (live: `compute.signal.kv.filtered.noncanonical`).
> * Leg 4 — 4 gated references with no flag opt-in: `stage-capture.sh` reads
>   `compute.candles.emitted`, `compute.candles.late.dropped`,
>   `compute.candles.restored_timer_noop` (MULTITF) and
>   `compute.execution_intent.rejected` (EXECUTION_INTENT) but never mentions the flags.
>   `pipeline-lib.sh` L1163 defaults `MULTITF_ENABLED=false`, so a lib-submitted
>   capture silently records nothing for those counters — **CHG-191's shape, live.**
>
> Also confirmed by construction: `UNWIRED_NAMES` (a fifth instance — `TradeDecisionsSinks`
> declares 3 operators + 1 metric that no `buildTopology()` reaches) is asserted disjoint
> from `EXPECTED`, and leg 2 passes on all 60 anchors, so the snapshot is live today.

### Task 1: (stage 2, deferred) Replace the snapshot with a source extractor

**Why:** nothing else can be built until the truth is readable. This task produces
only the extraction, no assertions.

**Files:** new `code/01_platform/04_scripts/tests/compute_identifiers.py` (a helper
module, **not** `test_*` so `unittest discover` does not collect it),
`code/01_platform/04_scripts/tests/test_compute_identifier_parity.py`

**Depends on:** none

- [ ] Extract from `code/02_services/02_compute/src/main/java/**/*.java` (**not**
      `src/test/java`), recording `(kind, name, declaring_class, condition)`:
      * `\.name\("([^"]+)"\)` → `operator`
      * `\.uid\("([^"]+)"\)` → `uid`
      * `\.(counter|histogram|gauge|meter)\(\s*\n?\s*"([^"]+)"` → `metric` —
        **the next-line form is mandatory**: `PositionsObservationOperator` L120–122
        writes `gauge(\n "babysitter.positions.latest_observed_version", …)`, and a
        same-line-only regex produces false "dead" verdicts (it did, on the first
        pass of this analysis).
      * `\.addGroup\("([^"]+)"` → `group`; a leaf registered on that group's
        variable is `<group>.<leaf>` (this is how `safety` → `transitions.applied`
        and `strategy` → `emitted` resolve; the second `addGroup` argument is
        dynamic and is ignored).
- [ ] Resolve each metric's **condition** from `SignalJob.java` control flow, not
      from a hand map: find each `if (config\....Enabled())` (L271/L313/L380) by
      balanced-brace scan, and attribute every `new <Class>(` inside it. A metric
      whose declaring class is instantiated inside that block inherits its
      condition; `babysitter.*`/`babysitter-*` resolve to `job=BabysitterJob` and
      the `safety`-group counters to `job=SafetyHaltJob`.
- [ ] **Completeness assertion (this is what makes derivation safe):** every
      identifier in A resolves to exactly one condition, or `unconditional`.
      An unresolvable class fails the test with the class name, so a new metric or
      a new operator forces a deliberate answer instead of silently inheriting
      "unconditional".
- [ ] Unit-test the extractor itself against a small inline Java fixture covering
      each form: same-line, next-line, `addGroup`-qualified, dynamic-second-arg,
      and a test-only `.name()` that must be ignored.
- [ ] **Verify:** dumping A prints the expected counts and shows
      `multi-tf-aggregator` under `MULTITF_ENABLED=true`, `raw-validation` under
      `unconditional`, and `babysitter.positions.observed` under
      `job=BabysitterJob`.

### Task 2: The registry, the allowlist, and the legs — store B and C landed in stage 1

> Stage 1 already ships B (`OPERATOR_REFERENCES`, 6 rows, both-ways asserted) and C
> (the foreign-stream allowlist, now **inline in the test file** as
> `ALLOWLIST_STREAM_EXACT` / `ALLOWLIST_STREAM_PREFIXES` with a source + recheck comment
> per family, rather than as a separate `compute-identifiers.allowlist` file). Decide in
> this task whether C should move to its own file: the in-file form is simpler and there
> is exactly one consumer, but a separate file makes the pin visible to `pin-check.sh`.
> Legs 1-5 are implemented; **leg 6 (both-ways) is not** — it needs A to be complete
> enough to assert that nothing live is unmeasured.

**Why:** the assertions are the deliverable; the extractor is plumbing.

**Files:** `test_compute_identifier_parity.py`;
new `code/01_platform/04_scripts/compute-identifiers.allowlist`

**Depends on:** Task 1

- [ ] **Store B — operator registry (in the test file, ~7 rows).** Each row is
      `name → the scripts that reference it`. Explicit rather than regex, because
      operator names are hyphenated and share shape with infra names
      (`trading-net`, `go-bridge`, `execution-bridge`, `dev-webhook`) — a broad
      regex here costs a 100-row ignore list. Leg 5 keeps the two halves honest.
- [ ] **Store C — allowlist file**, one row per foreign metric:
      `name | source | recheck-when`. Seed with the ~12 verified rows
      (`flink_taskmanager_job_task_operator_currentinputwatermark`,
      `…numlaterecordsdropped`, `…sourceidletime`, `…currentfetcheventtimelag`,
      `…flink_jobmanager_*`, `fluss_client_*`, `bridge_*`, `bridge.*`, `process_*`,
      `go_*`, `jvm_*`, `otelcol_*`, `container.memory.*`, `append_latency_ms`,
      `decode_errors*`). A row whose "source" is this repo means it does not
      belong in C.
- [ ] **Leg 1 — resolve.** Every compute-namespace candidate in a script resolves
      in A. Extraction is **quoted-string scope only** (kills the identifier
      false positives `compute_env`, `compute_manifest_entries`) and
      **lowercase-segments only** (kills the FQCN `compute.signaljob.SignalJob`,
      `docs_audit.py`'s `compute.X.xml`, `compute.md`). Two candidate forms:
      * dotted: `^(compute|babysitter|preview|safety|strategy|rows|transitions)(\.[a-z][a-z0-9_]*)+$`
      * O2-renamed stream: strip the literal `flink_taskmanager_job_task_operator_`
        prefix, then match against A **after normalising `[^a-z0-9]` → `''` on both
        sides** (the registered spelling `compute.invalid.byReason.schema-version`
        carries a hyphen that O2 preserves while `.`→`_`).
      A trailing `.` is a **family** reference and matches any leaf with that
      prefix (`compute.candles.invalid.` → family), and
      `compute.invalid.byReason` matches the group.
- [ ] **Leg 2 — registry live.** Every name in B resolves in A, and appears
      literally in every script that claims to reference it.
- [ ] **Leg 3 — foreign classified.** Every foreign-prefix candidate is in C or in
      a built-in category. Unknown prefix → fail with the token and the file.
- [ ] **Leg 4 — condition satisfiable (the CHG-191 leg).** For each script
      reference whose A-condition is `FLAG=true`, that script must opt the flag in
      (`FLAG:-true` / `export FLAG=true`) or **refuse an explicit false** — the
      exact contract at `holistic-measure.sh` L117–119. Read the resolved value
      from `pipeline-lib.sh` L1163–1166 for scripts that delegate submission. A
      gated reference with no opt-in fails.
- [ ] **Leg 5 — the `.name()` axis.** Every operator name in B is a `.name()`
      value in A, **not** a `.uid()` value, because Flink's metric id embeds the
      operator display name while restore keys on the uid — a `.uid()`-only pin
      (`SignalJobOperatorUidTest`) cannot see a `.name()` rename that breaks
      `rollout-savepoint.sh` / `pipeline-lib.sh` / `holistic-measure.sh`.
- [ ] **Leg 6 — both ways (the CHG-194 leg).** Every metric in A that the
      script's own topology runs is referenced by ≥1 script **or** is recorded
      `not-captured` with a reason. Fail on a new unmeasured metric — this is the
      leg that would have caught G7c.
- [ ] **Tests:** each leg gets a failing case built from a synthetic script + a
      synthetic store, so the legs are pinned independently of the real tree.
- [ ] **Verify:** the suite is **red against today's tree** and names the 22 dead
      `stage-capture.sh` identifiers. Capture that output as the failing-first
      evidence.

### Task 3: Clean the dead references

**Why:** a guard that ships red is not shipped. Delete, per decision 2.

**Files:** `code/01_platform/04_scripts/stage-capture.sh` (L422);
`code/01_platform/04_scripts/o2-provision.py`

**Depends on:** Task 2 (the red suite is the worklist)

- [x] **Re-verify each candidate against a live scrape or a live O2 query before
      deleting.** A static "dead" verdict that is wrong deletes a working tile —
      the same failure this plan exists to prevent, pointed the other way. Record
      the check per name in the commit body.

      **Done 2026-09-17, with the method adjusted.** No live scrape was possible:
      the stack was up (11 containers) but `GET /jobs/overview` returned
      `{"jobs":[]}` — nothing running to dump metrics from. Verification was
      therefore **source-authoritative**: each deletion checked against the
      registration site that would have to produce it (for the dedup gauges,
      `FingerprintDedupFunction` declares exactly `compute.dedup.first` /
      `compute.dedup.duplicates` and zero gauges — `totalEntries` is an internal
      field), and each added name against leg 2's anchor literal. Stronger than a
      live scrape for "can this ever be exported", weaker for "is it exported
      right now"; stated in CHG-195's limitation section.
- [x] `stage-capture.sh` L422: remove the 22 dead names. The verified list — old
      15 s candle path, forming-bar stack, 1 s preview path and old signal
      LOG/KV sinks, all retired by the 2026-09-05 cutover (`0f3e5952` and
      successors):

      `compute.candles.late.updates`, `compute.candles.invalid.total`,
      `compute.candles.invalid.`, `compute.candles.duplicate_window` (live spelling
      is `compute.candles.multitf.duplicate_window`),
      `compute.candles.previews.emitted`, `compute.candles.previews.late.dropped`,
      `compute.candles.previews.restored_timer_noop`, `compute.forming.bar.updates`,
      `compute.signal.passed`, `compute.signal.dropped.active_exists`,
      `compute.signal.cleared.closed`, `compute.signal.cleared.admin`,
      `compute.signal.active.count`, `compute.signals.detected`,
      `compute.signals.detected.forming`, `compute.signals.early.tentative`,
      `compute.signals.early.confirmed`, `compute.signals.early.confirmed_early`,
      `compute.signals.early.cancelled`, `compute.signals.early.marker_dropped`,
      `preview.trigger.previewFires`, `preview.trigger.eventTimeFires`

      **Done:** all 22 removed. The list is now grouped by condition
      (unconditional / `MULTITF_ENABLED` / `EXECUTION_INTENT_ENABLED` / cross-job)
      with the grouping written down where the list lives, so the next person can
      see *why* a name is there. Leg 1's stage-capture findings: 22 → 0.
- [x] `stage-capture.sh`: **add** the live metrics the capture currently misses —
      `compute.latency.ingest_to_monitor`, `compute.latency.signal_to_intent`,
      `compute.latency.tick_to_intent` (`stage_capture_parse.py` L184 already
      comments about `compute.latency.*` but nothing captures them),
      `compute.candles.gap.detected`, `compute.candles.live.emitted`,
      `compute.session.filtered.pre_open`, `compute.session.filtered.post_close`,
      `compute.startup.mode`, `compute.signal.kv.filtered.noncanonical`,
      `compute.trade_decisions.duplicate_instruction`.

      **Done — with one name deliberately withheld.** 9 of the 10 were added (the
      three `compute.latency.*` histograms, `compute.candles.gap.detected`,
      `compute.candles.live.emitted`, both session filters, `compute.startup.mode`,
      `compute.signal.kv.filtered.noncanonical`).
      `compute.trade_decisions.duplicate_instruction` was **not** added: it is
      declared in `TradeDecisionsSinks`, which no `buildTopology()` reaches (zero
      references outside its own file), so requesting it would guarantee a
      permanent `present=no` row — the same "measures nothing" defect this cleanup
      removes, introduced on purpose. That class is part of the open question in
      Task 3b.
- [x] `o2-provision.py`: delete or repoint the dead dashboard tiles and the dead
      alert (`_compute_candles_late_updates` L351–358, `_compute_signals_detected`
      L363–364, `_compute_dedup_state_count` L375, `_compute_dedup_state_bytes_estimate`
      L509). `FingerprintDedupFunction` declares exactly two counters and **zero
      gauges**, so both dedup-state tiles are dead.

      **Done — 7 panels resolved, favouring repoint over delete where a live 1:1
      successor exists.** Repointed: `Candles late updates` → `Candles late
      dropped (out-of-order)` (`flink_taskmanager_job_task_operator_compute_candles_late_dropped`,
      on both dashboards — the live late-drop counter CHG-191/192 are about had
      **no** tile while its dead predecessor had two) and `KV filter noncanonical
      (emitter)` → `KV filter noncanonical`
      (`compute_signal_kv_filtered_noncanonical`; the old spelling was one
      `signal_` short of the live stream, so the panel was silently empty).
      Deleted with no successor: `Signals detected`, `Dedup state count` (×2),
      `Dedup state bytes estimate`. The dead alert this item predicted no longer
      exists — CHG-023 already rebound `SIGNAL-warn-dedup-state` to the total
      dedup+compute checkpoint size, and `SIGNAL-warn-dedup-firsts-rate` binds the
      live `compute_dedup_first`.
- [ ] Extend `validate_command_spec()`'s coverage to the dashboard/alert stream
      set — reuse the existing mechanism (L123), do not add a second one. This is
      what makes the cleanup stay clean.

      **Open — not done in this pass.** The o2-provision side of the cleanup is
      by hand, so nothing yet *prevents* the next cutover from re-orphaning a
      tile. This remains the item that closes the class rather than the instance.
- [x] Update the compute dossier's metric inventory and any count in
      `01-foundation.md`'s C6 truth line that cites the captured-metric set.

      **Checked — nothing to update.** No dossier or C6 line cites the
      captured-metric set (`grep` for `stage-capture` / `stage_capture` /
      `metric inventory` across `01-foundation.md` and `04-signal-job.md` returns
      nothing), and no Java test was added or removed, so the C6 triple is
      unchanged by this work.
- [x] **Verify:** the Task-2 suite goes green; `python3 -m pyflakes` clean on
      `o2-provision.py`; `make test-audit-r2` green.

      **Results:** `tests.test_compute_identifier_parity` 10 tests **OK** (was
      `FAILED (failures=1)` with 39 leg-1 findings); `py_compile` + `pyflakes`
      clean on `o2-provision.py`; `make static-check` **0 failures**;
      `change_control_check.py` and `docs_audit.py` (C14, and all of C1–C17)
      green. `make test-audit-r2` as one command exceeds 60 s in this
      environment, so the corpus was swept **per module from the repo root**:
      104/104 modules ran, 100 PASS. The 4 non-pass are environmental, not
      behavioural — `test_bench_throughput_wave33` FAILs because a stray
      `python3` (pid 1400273) holds port 8899 (`!! port 8899 busy — preflight
      FAILED`), and `test_fluss_probes` / `test_raw_table_admin_args` exceed a
      20 s per-module cap on JVM work; `test_holistic_measure_wave36` passes in
      24 s when given 55 s. Note: running a module from `04_scripts/` instead of
      the repo root makes `test_change_control_check` fail spuriously (its path
      resolver is cwd-sensitive).

### Task 3b: The fourth site — the seeded dashboard corpus (found by the sweep)

**Why:** the 39 leg-1 findings were an **undercount of the class**, not the whole of
it. The sweep that Task 3 forced turned up a location the suite never looks at.

**Files:** `code/01_platform/01_docker/openobserve/dashboards/dedup-state.json`,
`compute-decision.json`, `manifest.json`

- [x] **`dedup-state.json` — 2 dead panels removed.** Both gauges named in the
      dashboard `description` had no producer, and the file's own live panels
      (`compute.dedup.first` / `.duplicates`, added later) sat alongside them. The
      two survivors moved up into the freed row and the `description` was
      corrected — it still advertised both retired gauges as live. Rejected
      alternative: repointing the "Live dedup entries" gauge at
      `compute.dedup.first`. It is a **cumulative counter**, not a live-set count,
      so that would have been a false claim; the live-entry count survives only as
      the internal `totalEntries` (G-DEDUP-2) guard.
- [x] **`compute-decision.json` — 1 dead panel removed** (`Dedup live state`; its
      live counterpart C06 already existed), layout closed up, `description`
      corrected to drop the DEC-038 Fluss dedup table it claimed to measure.
- [x] **`manifest.json` — the dashboard-as-evidence records updated:**
      `dedup-state` `dashboard_version` 9 → 10, `compute-decision` 8 → 9, and both
      `measurement_boundary` / `workload` rewritten to the metric set that
      actually exists. A record naming a retired gauge as its measurement
      boundary is not evidence (`10-observability.md`).
- [x] **Verify:** `json.load` on all three; `tests.test_seed_dashboards` +
      `test_seed_dashboards_wave25` (21 tests, OK — corpus still v8, 192-grid,
      unique panel ids); the dead-name sweep over the corpus matched against
      `EXPECTED_NAMES` 3 → 0.
- [x] **Panel C04 (`Decision p99`, stream `compute.decision.latency.p99`) left
      alone deliberately.** It has no producer in the repo, but that is design,
      not drift: `SIG-PERF-001`'s p99 decision-latency half is `NOT_IMPLEMENTED`
      (AC-NFR-002). It is a placeholder for a pending emitter, so it stays — and
      now says so in its own description and in the manifest
      `measurement_boundary`, so the next sweep does not re-open it as a finding.

**Open (found, verified as
*flagged*, **not** resolved):**

- [ ] `position-state.json` — 4 panels read 5 retired active-signal streams
      (`compute_signal_passed`, `…dropped_active_exists`, `…active_count`,
      `…cleared_closed`, `…cleared_admin`) via `LIKE '%…%'`, which the suite's
      scanner does not match (its quoted-literal pattern misses the `%` prefix —
      a second scanner gap). Whether the active-signal lifecycle is retired or
      merely renamed is **not** established, and unlike the dedup gauges these
      may be the only observability for a live subsystem. Needs a producer check
      before anything is deleted.
- [ ] `docs/06_operations/01-runbooks.md:523` — the `SIGNAL-warn-dedup-state`
      row still describes the alert as watching dedup-state entries > 6.5M, while
      the live alert watches total dedup+compute checkpoint size > 1.5 GB. A
      runbook that names the wrong stream and threshold for an alert is the same
      defect class, in the place an operator reads under pressure.
- [ ] **Extend the suite's scan root** beyond `04_scripts` to the seeded corpus
      (and fix the `LIKE '%…%'` blind spot). This is what would have found the
      above three automatically; the plan's leg set is otherwise sound.

### Task 4: Resolve the `schema_version` / `schema-version` divergence

**Why:** two O2 streams exist for one counter (`o2-provision.py` L73 vs L1085) and
only one can be receiving samples. A static test cannot tell them apart — the
normalisation in leg 1 makes both "resolve".

**Files:** `code/01_platform/04_scripts/o2-provision.py`

**Depends on:** none (read-only investigation)

- [ ] Query the local O2 for both streams and record which carries samples and
      which is empty. (`O2_PASSWORD` must be in the environment; never print it.)
- [ ] Delete the empty one and keep the live spelling; if **both** are live,
      record why in the allowlist and file the finding instead of guessing.
- [ ] **Verify:** the surviving spelling matches the counter's registered name
      after O2's `.`→`_` rename (`stream()`, L79) — i.e. hyphen preserved.

### Task 5: The change record

**Files:** new `docs/05_deployment/change-records/CHG-195.md`

**Depends on:** Tasks 2 and 3 (so `affected_artifacts` can resolve)

- [x] Write `CHG-195.md` from `_template.md` with the decision-5 fields:
      `scope: gate-behavior, measurement-gate`,
      `compatibility_class: COMPATIBLE_WITH_LIMITATION`,
      `savepoint_impact: none — host-side tooling and evidence lists; no Flink
      state, checkpoint, DDL, connector or container definition is touched.`
- [x] `affected_artifacts` must resolve: the new test file, the helper module, the
      allowlist, `stage-capture.sh`, `o2-provision.py`,
      `docs/plans/2026-09-17-compute-identifier-parity.md`.
      **As landed:** no helper module and no allowlist *file* exist (stage 1 is a
      single test file with an inlined allowlist), so those two are correctly
      absent; the five dashboard/manifest files were added to the list.
      **Trap:** do **not** list the retired metric names — they are string
      literals in a live file, not deleted paths, and the `_template.md`
      dead-path annotation requires a real path that the named commit removed
      (C14 verifies it; a fabricated annotation fails the audit).
- [x] `plan_tasks`: cite this plan.
- [x] State the limitation honestly in the body: a script reference to an
      identifier the job does not produce is now **refused**, and the
      captured-metric list changed (22 removed, ~10 added).
      **As landed:** the body states that the suite still scans only `04_scripts`
      (so the corpus findings came from a hand sweep), that the capture list
      changed 22 removed / 10 added with 1 withheld (above), and that the live
      re-seed of O2 has not been run.
      **Update (same day):** the re-seed **has now been run** — see Task 7 below.
- [x] **Verify:** `python3 code/01_platform/04_scripts/change_control_check.py`
      → exit 0, and the same check via `make docs-audit` (C14).
      **Results:** `change-control: all 188 change record(s) complete` (CHG-195
      PASS); `docs-audit: all checks pass — docs agree with code`, C14 included.

### Task 6: Confirm the suite needs no Makefile change

- [x] Confirm the new suite runs with **no Makefile change**:
      `python3 -m unittest discover -s code/01_platform/04_scripts/tests -v`
      collects it, and `make gate-fast` is green (it calls `test-audit-r2`, which
      is that same discovery).
      **Confirmed:** the file is collected and `make gate-fast` needs no edit; its
      red cause is gone — that module is green and the corpus sweep found no other
      behavioural failure. `gate-fast` as a single command still needs several
      minutes here (the suite has grown past a 60 s budget), so the sweep, not one
      run, is the evidence.
### Task 7: Re-seed O2 from the cleaned sources (done 2026-09-17, same day)

- [x] `make seed-dashboards` with `O2_PASSWORD` from `secrets.env` and `O2_USER`
      from `.env` — **RESULT: created=8 updated=0 untouched=0**. The O2 org turned
      out to have never been seeded, so nothing needed `--force`: all 8 seed
      dashboards went in fresh from the cleaned corpus.
- [x] `o2-provision.py` (first run) — created its 11 dashboards, the
      `dev-webhook` destination, 43 alert rules and first-time retention policies
      (845 streams) from the fixed spec; reached `done`.
- [x] Idempotency proof (second run) — all 11 dashboards `converged`, every
      alert `exists`, `destination exists: dev-webhook`, nothing recreated.
- [x] Post-state verified over the REST API: **19 dashboards** deployed,
      **zero** dead stream names in any of them; both repointed panels carry
      their live successors in the deployed copies:
      `compute_candles_late_dropped` on *COMPUTE - Candle Health* and
      *COMPUTE - SignalJob Overview*; `compute_signal_kv_filtered_noncanonical`
      on *COMPUTE - Candle Health* and *COMPUTE - Quality*.
- [x] `CHG-195` updated in place: Status line and Verification section now
      record the applied re-seed; `affected_artifacts` gains `status.md`.
      Side effect recorded: since the org was empty, this run also stood up the
      alert rules and retention for the first time.
- Note: `o2-provision.py` runs the COMMAND converge check before any write
      (`validate_command_spec()`), so the Task-3 edits are proven against the
      live instance every provision run.
- Note: no repo code changed in this task — O2 state only — so the only commit
      here is this bookkeeping.

### Task 8: Verify the guard, not just the change (open)

- [ ] **Mutation list** — apply each to a copy and confirm the suite catches it:
      (a) rename a metric literal in `MultiTimeframeAggregateFunction`;
      (b) add a `.name()` to `SignalJob.java` and remove its script reference;
      (c) change a `.uid()` without its `.name()` (leg 5 must **not** fire — it is
      the name axis only);
      (d) drop `export MULTITF_ENABLED` from a script that reads
      `compute.candles.late.dropped` (leg 4);
      (e) delete every script reference to a live metric (leg 6).
      A mutation that is **not** caught is a documented limitation in the test
      docstring, per the wave-35 precedent.
- [ ] Confirm the suite is static and fast (no docker, no cluster, no network); if
      it adds more than a second or two, keep it that way deliberately and say so.
- [ ] `make static-check` green (unaffected, but it is the gate that owns
      `04_scripts`).
- [ ] Re-run `make docs-audit` for C6's test-count line and C14.

## Technical Details

**Why 22 of the 39 names in `stage-capture.sh` are dead.** The declared metric set
in `02_compute/src/main/java` is exactly 33 names: `babysitter.positions.{applied,
conflict,duplicate,latest_observed_version,observed,rows.malformed,rows.observed,
stale,stale_arrival}`, `compute.candles.{emitted,gap.detected,live.emitted,
late.dropped,multitf.duplicate_window,restored_timer_noop}`, `compute.dedup.{first,
duplicates}`, `compute.execution_intent.rejected`, `compute.invalid.{byReason,rows}`,
`compute.latency.{ingest_to_monitor,signal_to_intent,tick_to_intent}`,
`compute.session.filtered.{post_close,pre_open}`,
`compute.signal.kv.filtered.noncanonical`, `compute.startup.mode`,
`compute.strategy.{dropped.oversize,dropped.unkeyed,failed,suppressed}`,
`compute.trade_decisions.duplicate_instruction`, `container.memory.{limit,usage}.bytes`,
`emitted`, `rows.malformed`, `rows.skipped`, `strategy`, `transitions.applied`.
Counts: **22 dead / 17 live** out of the 39 in the list.

**Why the 22 all trace to one cutover.** The 2026-09-05 topology cutover (batch 2
and batch 3, following `0f3e5952`) retired the 15 s candle window path, the
forming-bar stack, the 1 s preview window, the position-state gate and the old
signal LOG/KV sinks, replacing them with `raw ticks → dedup → multi-TF candles →
strategy host`. `SignalJob.java`'s own comment block says so in place ("Preview
path retired (2026-09-05 cutover, batch 3)"), and no class named `EarlySignal*`,
`FormingBar*`, `CandlePreview*`, `SignalDetection*`, `ActiveSignal*` or
`CandleSinks*` exists under `src/main/java`. Nothing in `04_scripts/` was touched
by that cutover — which is the finding, not the incident.

**Why the metric extraction must read quoted strings and lowercase segments.**
The candidate profile in `04_scripts` includes real false positives that a naive
regex turns into failures: `compute.jar` (26 occurrences — the shaded build
artifact, `pipeline-lib.sh` L56 and friends), `compute.md` / `babysitter.md`
(dossier filenames, `docs_audit.py` L366/L371), `compute.X.xml` (a surefire
glob), `compute.signaljob.SignalJob` (a Java FQCN in `rollout-savepoint.sh` L30),
`compute_env` and `compute_manifest_entries` (Python identifiers in
`t8_sandbox_contract_check.py` L125 and `ddl_apply.py` L290), and `compute.get`.
Quoted-string scope kills the identifiers; the lowercase rule kills the FQCN and
`.xml`; an explicit suffix exclusion for `.jar` / `.md` / `.java` / `.sh` / `.py`
/ `.yaml` / `.json` kills the rest — and because that exclusion list is explicit,
a **new** false positive fails closed instead of being silently dropped.

**Why leg 1 normalises rather than comparing text.** A registered name may contain
a character O2 preserves (`compute.invalid.byReason.schema-version` → the hyphen
survives the `.`→`_` rename). Comparing text would fail every hyphenated name;
normalising `[^a-z0-9]`→`''` on both sides matches them, at the cost of not being
able to distinguish two streams that normalise identically — which is precisely
why Task 4 is a live check and not a test.

**Why the condition is derived, not tabulated.** A hand-written name→flag map is
the exact artifact CHG-193 deleted for rotting ("Naming no operator cannot rot when
operators are added or renamed"). Deriving from the `if` blocks in `SignalJob.java`
and asserting **completeness** means the map cannot rot silently: a new metric with
no resolvable condition fails the suite and forces a deliberate answer. If the
balanced-brace scan proves unreliable in practice, the documented fallback is an
explicit table **plus** the completeness assertion — never a table alone.

**Why the both-ways leg matters as much as the dead-name leg.** The three filed
incidents split evenly: two were a script reading a name that had gone (CHG-191,
CHG-193) and one was an analyzer reading a *table* that had gone (CHG-194). A
one-direction guard would have caught two of three. Leg 6 is what covers the
third — and it is also what makes the Task-3 additions (the `compute.latency.*`
histograms nothing measures today) a deliberate decision rather than an omission.

## Required External or Manual Verification

**The live O2 read in Task 4.** Needs `O2_PASSWORD` in the environment and the
local O2 up. Read-only: two queries against `_search`. Capture which stream
returned rows and paste the result into the commit body or the allowlist row's
provenance. **Authorization:** ordinary read against the local dev O2; never echo
the password.

**The pre-deletion re-verification in Task 3.** For each of the 26 names (22 in
`stage-capture.sh`, 4 O2 streams), confirm against a live scrape or the `custom_names`
REST path that the name yields nothing *today*. A static verdict alone is not
sufficient evidence to delete a working dashboard tile. If the stack is down,
either bring it up (`make up` — needs the stack lock) or defer the deletion of the
names you could not check and say which. **Why it cannot be automated:** it needs
the real Flink REST endpoint and the real O2 stream table.

**Not required:** `make gate`. Nothing here touches production code, DDL, a
container definition or Flink state, so the Monday gate's live steps add no
evidence for this change. `gate-fast` + `docs-audit` are the right scope.

## Post-Completion

* Record the residual limitation honestly: the suite proves a *reference*
  resolves, it cannot prove the *value* is meaningful. A metric that exists and is
  always `0` still passes — that is what the live gates (G7/G8, the smoke inject
  gate) are for, and the division of labour is worth stating in the test docstring.
* `stage-capture.sh`'s `custom_names` will still be a single hand-maintained line.
  A follow-up worth considering (not this plan) is deriving it from the leg-1
  resolution instead of a literal, so the list cannot disagree with the guard that
  checks it.
* The audit file's own structure notes apply to the next wave: 709/864 findings
  predate 2026-09-07, so re-verify a quoted `Was` line before acting on it. Keep
  running the Fix Audit on waves 37+ — in waves 35 and 36 it caught 3 and 7 broken
  proposed fixes respectively.
* This class will recur one level up too: nothing yet ties O2 **alert thresholds**
  to the config constants they assume (the `AlertThresholds` ↔ `seed_alerts.py`
  pairing is hand-copied). Out of scope here; noted so it is not re-discovered as
  a surprise. **Update (2026-09-17, same day): done** —
  `tests/test_alert_threshold_parity.py` (15 tests) now ties all 47 ALERTS
  setpoints to their authorities (`AlertThresholds`/`PlatformConfig` constants,
  the compose pin, 02-ingestion-alerting.md, the runbook table, the
  position-state JSON corpus) with a fail-closed completeness leg, and the
  predicted drift was real: `SIGNAL-error-checkpoint-slow` fired at 240 000 ms
  against a 30 000 ms pin (runbook said 24 000) — fixed as CHG-197, including
  the live rule re-provisioned in O2.
