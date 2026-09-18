# Certification hygiene — plan (2026-09-18)

**Status:** closed (2026-09-18) — Step 1 and Step 2 shipped; the probe work shipped under
B0+ instead of B1; Phase 2 and the B1 fixture deferred. **No certifying gate ran for this
tree** — see §9 for the evidence, the reason, and the consequences.
**Baseline:** `a77c4927`, tree clean, certificate run 6 green (`GATE RESULT: PASS — 19/19
verified, 0 skipped`, 51 m 0 s, dir `logs/soak/monday-gates-20260918-163105`).
**Next free change-record id:** CHG-218.

## 1. Why this plan exists

Three reds found *after* the certificate were not defects. They were structure, and each
one will re-bite:

1. a **manual step that must be remembered** — after any rebuild of the four local-build
   images, `runtime.lock` goes stale and both `pin-check.sh`-adjacent tests and the next
   gate's step 3 go red until a human re-pins by hand (2026-09-18: 2 of 4 images, ~10 min
   plus one extra `gate-fast` cycle);
2. a **test that depends on live mutable state** — the two `intents` probe legs flipped red
   because the drill grew `Execution_Intent` past what the probe will census (mitigated
   today by CHG-217, which accepts the documented refusal);
3. **test files the certificate never runs** — 18 pytest-only files, so wave-37/38 evidence
   inside them never executes under the gate.

This plan removes those three structures. It explicitly does not try to make the gate
faster; that is Phase 2 (§7) and needs its own decision.

## 2. Verified facts (the ground this plan stands on)

### 2.1 The four local image pins

| Claim | Evidence |
|---|---|
| Local-build lines: `INGESTION_IMAGE` (42), `NAUTILUS_IMAGE` (79), `EXECUTION_BRIDGE_IMAGE` (80), `EXECUTION_GATEWAY_IMAGE` (81). Released/registry pins: `FLUSS_IMAGE` (21), `FLINK_IMAGE` (22), `OPENOBSERVE_IMAGE` (28), `ZOOKEEPER_IMAGE` (77). 81-line tracked file. | `code/01_platform/01_docker/runtime.lock` |
| Compose never reads them: `COMPOSE := docker compose --env-file …/.env --env-file …/secrets.env -f …`; the four vars are absent from `code/01_platform/01_docker/.env` (6626 B); the local services in `docker-compose.yml` declare `build:` (8 services). | `Makefile:6`; `docker-compose.yml` |
| `docker-stack.yml` requires them from a *deployment* env. A local config digest cannot exist in a registry, so the recorded value is unusable for that purpose. | `docker-stack.yml:727,792,820,855`; lock comment "image ID (config digest), not a registry manifest digest" |
| `pin-check.sh` [5/6] is a shape rule (`@sha256:[0-9a-f]{64}`, ≥1 ref). With the four local lines removed, four released refs remain → still `PASS`. | `pin-check.sh:57-90` (P6-139/140/141 notes) |
| Exactly one test depends on the values; `test_real_runtime_lock_check_5_passes` survives the change. | `tests/test_pin_check.py:22,29,113,120` |
| This is a **decided** design, not drift: "The live leg is docker-gated, and deliberately fails on drift… goes red until the lock is re-pinned". | `docs/05_deployment/change-records/CHG-162.md` judgement call 6 |
| CHG-162 already declares freshness a separate concern the stamp owns: "`image_staleness_check.py` compares a content stamp, not a digest; it does not read `runtime.lock`". | `CHG-162.md` judgement call 3 |
| The stamp check is real and wired post-build: sha256 over the Dockerfile's COPY inputs, baked as a label by `make images`, FRESH iff label == stamp recomputed from the tree; it exists because a stale gateway jar once served `readyz 200` with old semantics. | `image_staleness_check.py` docstring (CHG-101); `Makefile:266-270`; gate step 18 runs it *after* the build (`run-monday-gates.sh:960-974`) |
| The template already ships the local entries as commented placeholders, as does the live lock for `COMPUTE_IMAGE`/`EXECUTOR_IMAGE`. | `runtime.lock.example:20-22`; `runtime.lock:43-44` |

**Conclusion.** The ID pins cannot detect what matters (source changed, image not rebuilt —
the lock's own comment concedes this) while firing on the benign case (a rebuild). The
safety property is owned, better, by the content stamp. The step-17-before-18 ordering
concern applies only to these ID pins; post-build freshness verification is already ordered
correctly.

### 2.2 The probe legs

| Claim | Evidence |
|---|---|
| The probe only reads: modes `signals`/`intents`/`orphans`; `intents` defaults to `Execution_Intent`; the census is cross-checked against `Admin.getTableStats` and a disagreement exits 1 with no census; success prints `table=` and `rows=`. | `code/01_platform/04_scripts/fluss-probes/FlussSignalLatency.java:33,59,87,118,306,329,407` |
| The header records the Fluss 0.9.1 undecount: 25 rows by batch, 41 by offset-paged read, 48 by the server's count. | same file, header |
| The test harness compiles `PROBES` itself with `javac` and runs any main from that directory on a runtime classpath; a compile failure is an AssertionError, never a skip. | `tests/test_fluss_probes.py:85-124` |
| No python Fluss client exists (`ModuleNotFoundError: fluss`), and no python row-writer exists. | probed directly |
| `GateTableAdmin` derives the table path from the DDL text (`TablePath.of("default", parsed.tableName())`) but enforces the gate's merge-engine rule, and `Execution_Intent` is a LOG table. Direct reuse is therefore **out**. | `fluss-repair/GateTableAdmin.java:30-56,62-64,76` |
| The drill creates and drops its own scratch tables. | `code/common/src/test/java/com/trading/common/schema/fluss/CompatFlussIntegrationTest.java:112,126,134` |

**Conclusion.** A deterministic non-empty fixture is feasible only with a new Java seeder in
the probes directory (the one place a writer can live), building its own `TableDescriptor`
from a scratch copy of `27_execution_intent.sql`. It is bounded work, not a shortcut.

### 2.3 The unrun test files

| Claim | Evidence |
|---|---|
| 18 `test_*.py` files contain pytest-style tests and no `unittest.TestCase`, so `unittest discover` collects none of them; step 3 runs only `unittest discover`. | measured; `run-monday-gates.sh:603-620` |
| They are green: `278 passed in 203.82s`, rc=0 (17 passed for the same selection the explicit list gives). | measured 2026-09-18 18:08 |
| `pytest … -p no:unittest` collects **exactly those 278**, so no file list is needed and the selection cannot go stale. Full collection is 1754 (= 1477 unittest + 278 pytest-only, with a 1-test accounting difference not claimed as equivalence); unittest reports 1476 once CHG-218's retirement lands. | measured |
| Step 3's budget is `PY_TIMEOUT_SEC` default 600, for a suite that needs ~295 s quiet and exceeded 300 s under gate load; failure detection is `grep -q "^OK"` plus no `^FAILED (`; the PASS line carries `Ran N tests`. | `run-monday-gates.sh:603-620` (CHG-199, CHG-201) |
| pytest 9.0.2 is installed and already used by other Makefile targets. | `python3 -c "import pytest"`; `Makefile:274-296` |

## 3. Decisions required

| # | Decision | Options | Recommendation |
|---|---|---|---|
| D1 | Local-ID pins | **A1** retire the leg (comment the four lines to template shape, retire the test, note that the stamp owns freshness) · **A2** keep byte provenance by having `make images` machine-write the four IDs (no hand editing; the host-comparison check becomes self-fulfilling and goes away either way) · **A3** keep paying the cost | **A1** |
| D2 | Probe legs | **B1** build the deterministic fixture (strict assertions on a seeded scratch table; the `orphans` leg keeps covering the refusal path) · **B0** stop at CHG-217 and defer | **B1** if census agreement is part of what the certificate asserts, else B0 |
| D3 | Step 2 content | **no decision needed** — it only adds coverage; the scope is fixed in §5 | proceed |

## 4. Step 1 — retire the local-ID pin leg (D1 = A1)

**Removes:** one hand edit plus one red test after every rebuild of the four modules.

Edits:

1. `runtime.lock`: comment the four local lines to the template's placeholder shape; add one
   header line stating that local-build freshness is owned by `make check-image-stale`, and
   that a *pushed* image's registry manifest digest is what gets pinned here.
2. `tests/test_pin_check.py`: retire `test_local_build_pins_match_the_local_images` (line 120)
   and `LOCAL_BUILD_IMAGES` (line 29); nine tests remain and nothing else in the file reads
   the four values.
3. `CHG-218`, citing CHG-162 judgements 3 and 6 as the redundancy proof (required before a
   test is retired).

Verification: `pin-check.sh` (expect `OK: 4 image refs all digest-pinned`) →
`test_pin_check.py` → `make check-image-stale` → `make gate-fast`.

Effort ~30 min · machine ~6 min · risk low · rollback: revert the commit; the four lines
return and the retired leg must be restored from the parent commit.

*(D1 = A2 instead: same edits minus retirement, plus a `--write` path in `make images` and a
recorded decision that the host-comparison check is deliberately dropped.)*

## 5. Step 2 — run the pytest-only files in step 3

**Removes:** a coverage hole where 18 files' evidence never executes under the certificate.

Edits:

1. `run-monday-gates.sh`: declare `PYTEST_LOG` and `PYTEST_TIMEOUT_SEC` in the **shared
   region** (scoping guard), and add a second invoker inside step 3 (no renumbering):
   `timeout -k 60 "$PYTEST_TIMEOUT_SEC" python3 -m pytest code/01_platform/04_scripts/tests
   -q -p no:cacheprovider -p no:unittest`, its own failure check (pytest exit code, no
   zero-collection pass) and its own `PASS:` line, so the unittest evidence string
   `PASS: python unit suites (Ran N tests)` is untouched.
2. `Makefile`: add the same invoker to `test-audit-r2` so the 4-minute T0 tier exercises it.
3. `CHG-219`: budget justification (measured 204 s; budget 420 s = ~2× healthy).

Verification: `make -C … gate --steps 3` (subset — never certifies) → `make gate-fast`.

Effort ~30 min · machine ~10 min · certificate cost +3.4 min (51 → ~54.5 min), worst case
bounded by the 420 s budget · risk low (additive) · rollback: revert; the unittest invoker is
untouched.

## 6. Step 3 — deterministic fixture for the two probe legs (D2 = B1)

**Removes:** opportunistic coverage that silently degrades as the live table grows.

Edits:

1. New Java seeder in `code/01_platform/04_scripts/fluss-probes/`: create a scratch table
   from a scratch copy of `02_sql/ddl/27_execution_intent.sql`, append N rows, run the probe,
   drop the table; builds its own `TableDescriptor` (no `code/common` dependency).
2. `tests/test_fluss_probes.py`: run it via the existing compile+`run_probe` harness; both
   `intents` legs assert **strictly** against the seeded count; keep the refusal branch only
   where the fixture cannot guarantee a clean read.
3. `CHG-220`, recording that the refusal path stays covered by the pre-existing `orphans`
   leg, so no coverage is lost by tightening the other two.

Verification: the probe test file standalone → `make gate-fast`.

Effort ~2-4 h · machine ~10 min · risk medium (descriptor/schema match; scratch-table
lifecycle on this cluster) · rollback: revert; CHG-217's tolerant legs return.

## 7. Phase 2 — scheduled live certification (outline only, not scoped)

The two live steps are 79% of the 51-minute clock and the origin of every fire this session.
Native shape: a hermetic gate per batch, plus a live certification that provisions its own
scratch namespace, runs on a schedule, and is **recorded**; the gate reads the last certified
live run inside a freshness window instead of re-running it inline. This redefines what
"certificate" means, so it needs its own decision and record before any code. Estimate: a
scoping pass plus 1-2 days; not this week, and not bundled with Steps 1-3.

## 8. Non-goals

- Not fixing the probe's census (upstream Fluss 0.9.1 behaviour; the refusal is the contract).
- Not parallelizing or overlapping gate steps to shave minutes.
- Not auto-regenerating `runtime.lock` inside the build.
- Not deleting or skipping any test, or any live step, to improve numbers.

## 9. Batch acceptance criteria

- Tree clean; each step's T0 (`make gate-fast`) green before its commit.
- One certifying `make gate` at the end of the batch (T3), because Step 2 changes the gate
  itself and Step 3 changes a live test; `--steps` runs never certify.
- Change records validate: `change_control_check.py --dir docs/05_deployment/change-records`
  exit 0, `docs_audit.py` C14 pass.
- No test retired without its redundancy proof recorded in a change record.
- Each step's verification evidence comes from that step's own run (evidence is never reused
  across runs).

### How the batch actually closed (2026-09-18, option B — no certifying run)

Step 1 (D1 = A1) shipped as `beca9f28` + CHG-218; Step 2 (D3) as `d8d29ffe` + CHG-219; the
probe work shipped as `2edc8a8b` under **B0+** — an accepted refusal now reports as a skip
instead of a silent pass — rather than B1's Java fixture, which stays deferred. Phase 2 (§7)
is untouched.

> **Later the same day — CHG-221 (todo #21).** B1 was built after all, and the fixture needs no
> new harness: `ProbeFixtureSeeder.java` creates two scratch KV tables (single-field PK,
> `kv.format-version=2`, tiering off), seeds 5 + 3 rows, and waits until the server's own row
> count agrees before returning. Two **new** legs in `SignalLatencyContractTests` assert the
> agreement the live legs can only refuse for; the live refusal legs keep their assertions. §6's
> design is what shipped, with one difference worth naming: the fixture backs new legs rather
> than replacing the live refusal coverage. Measured — probe suite `Ran 35 tests in 94.990s —
> OK (skipped=5)` (the same 5 skips), the fixture census printing `rows=5 distinct_candidates=5
> duplicate_rows=0` and `orphan_intents=3` with `orphan=fx-c`, `orphan=fx-d`, `orphan=fx-e`, and
> the catalog verified back at 33 tables afterwards.

Evidence, per step, from its own run. **Step 1** — `pin-check` PASS (`OK: 4 image refs all
digest-pinned`), `test_pin_check.py` 9 tests OK, `make check-image-stale` PASS (8 images
current), `make gate-fast` green (233 s). **Step 2** — probe suite `Ran 33 tests in 72.595s —
OK (skipped=5)`, all three skips naming `census read 23 rows but Fluss reports 27`. **Step 3** —
`bash -n` and `shellcheck -S warning -x` clean; `--steps 3` → `SUBSET RESULT: PASS — 1/1
verified`, with `PASS: python unit suites (Ran 1476 tests)` and `PASS: python pytest-only
suites (278 passed)`; `make gate-fast` green — `Ran 1476 tests in 246.490s OK (skipped=11)`,
`278 passed, 1 skipped, 25 warnings in 205.23s`, `docs-audit: all checks pass`. Docs: the
stale-claim scanner exits 0 in both `--upstream` (97 LINE-ANNOTATED) and default mode (88).

**The second criterion is deliberately not met.** No certifying `make gate` was run for
`d8d29ffe`. The standing certificate is run 6, whose preflight recorded `HEAD 24d8c59f` — 8
commits behind, two of which change what the gate checks (`d8d29ffe` step 3, `beca9f28` step
17). The gap is accepted because all six components of `gate-fast` were proven green in the
same hour, and because 40 of a certifying run's 51 minutes are two live steps — the drill
(23 m 5 s) and the DDL apply smoke (17 m 16 s) — which would re-verify code these commits do
not touch.

Consequences, stated so they cannot be discovered later: nobody may quote a certificate for
`d8d29ffe`; the step-3 invoker and the retired pin leg carry subset-level evidence only; the
next scheduled certification (§7) or any release run must certify this tree or a descendant.

## 10. Open unknowns

- Whether a small scratch table drops quickly on this cluster (deletion queue + remote-log
  cleanup) — measured during Step 3, not assumed.
- Whether the probe's `intents` read needs the full `Execution_Intent` column set, which
  decides the seeder's descriptor size — read from `27_execution_intent.sql` at implementation.
- Whether any of the 18 pytest files become order-dependent when run inside the gate (they
  passed standalone).
