# Daily single-command runner — plan (final product + build plan)

**Created:** 2026-09-26 · **Status:** decisions locked 2026-09-26 (D1–D6,
all recommended); implementation started with P1-1.

**User decisions locked (2026-09-26):**
1. **D1** — execution mode **offline/halted** (orders off; paper/sandbox stays a
   separate release-gated step).
2. **D2** — daily universe **full 2 433 @ 3 sockets in dev** (production
   single-socket policy untouched).
3. **D3** — lifecycle: **one manual morning command; stack left running 24×7**;
   no timer in v1.
4. **D4** — daily actions: **`start`/`status`/`stop` only**; savepoint stays an
   explicit command; EOD stays with the `eod-controller` service.
5. **D5** — success bar: **nine checks I1–I9; pre-session data checks report
   PENDING, exit 0**.
6. **D6** — interface: **`make day ARGS="..."` + `day-run.sh`**.

**Operator goal (2026-09-26):** one command that makes the full local platform —
tick ingestion → Fluss → Flink SignalJob (candles + signals in one job) —
execution chain, companions, EOD, observability — come up **in coordination**
and prove it, with no manual assembly and no patchwork that needs future care.

**Related:**
- `docs/plans/2026-09-26-real-broker-datastream-profiling.md` — real-feed
  profiling mode (CHG-319/320/321) + the Monday in-session runbook.
- `docs/plans/2026-09-24-plan-execution-protocol.md` — how §0 items execute
  (verify-first, marker rules, evidence).
- `docs/06_operations/08-live-readiness-gaps.md` — why live-order enablement
  is not automatable in a daily script.
- `docs/06_operations/01-runbooks.md` — existing start/stop/restart practices
  the runner must reuse, not duplicate.

## 0. Live tracker

This section is the single source of truth for build status; the body sections
below are the evidence. Where a body section and this list disagree, this list
wins. **Execution protocol:** `2026-09-24-plan-execution-protocol.md` — the
operator names an item ID; verify-first, marker discipline, smoke-before-run,
test + CHG + doc, `plan_tracker.py --write/--check`.

**Marker legend**

| Marker | Meaning |
|---|---|
| `[x]` | landed and verified (evidence named) |
| `[~]` | in progress right now |
| `[ ]` | action owed — doable offline / on the local stack |
| `[L]` | action owed that needs a live market window |
| `[?]` | needs an operator decision before it can start |
| `[-]` | no action — trigger-gated or superseded (trigger recorded) |

#### D — Operator decisions (decided 2026-09-26, all recommended)

- [x] **D1** — Execution posture: **offline/halted** (orders off; paper/sandbox
  stays a separate release-gated step).
- [x] **D2** — Daily universe: **full 2 433 @ 3 sockets in dev** (production
  single-socket policy untouched).
- [x] **D3** — Lifecycle: **one manual morning command; stack left running
  24×7**; no timer in v1.
- [x] **D4** — Daily actions: **`start`/`status`/`stop` only**; savepoint stays
  an explicit command; EOD stays with the `eod-controller` service.
- [x] **D5** — Success bar: **nine checks I1–I9; pre-session data checks report
  PENDING, exit 0**.
- [x] **D6** — Interface: **`make day ARGS="..."` + `day-run.sh`**.

#### P1 — Platform start-safety (no decision needed)

- [x] **P1-1** — ingestion: bounded, leader-aware Fluss readiness wait at
  startup (retryable-only, backoff, budget, fail-closed; auth/config fast-fail).
  Landed CHG-322 (2026-09-26): 9 new + 4 classifier tests, `make test` green
  (ingestion 508), static-check + docs-audit green; the compose-recreate drill
  runs with P4-3.
- [ ] **P1-2** — ingestion: capacity/policy check surfaced at start with a
  precise operator message (tokens vs sockets, approval flag, prod policy).

#### P2 — Config truth

- [ ] **P2-1** — universe/socket daily contract mapping `full|approved` onto the
  existing env knobs, effective-manifest verification, production policy
  untouched (CHG-320).
- [ ] **P2-2** — execution-posture guard: runner never sets live flags; refuses
  the execution profile if any enablement flag is present on a dev host.

#### P3 — Orchestrator interface

- [ ] **P3-1** — `code/01_platform/04_scripts/day-run.sh start|status|stop`:
  sequencing, idempotency, singleton guard, fail-closed exit codes.
- [ ] **P3-2** — verification predicates + status board (pure functions +
  read-only probes; session-aware data predicate).
- [ ] **P3-3** — tests in `code/01_platform/04_scripts/tests/` (auto-discovered
  by the gate; docker/flink CLIs stubbed).
- [ ] **P3-4** — `make day` thin target (interface only, no logic).

#### P4 — Ops integration and validation

- [ ] **P4-1** — runbook: daily path + failure→native-recovery playbook
  (each failing invariant maps to an existing recovery action).
- [ ] **P4-2** — dossier/doc updates (local-compose profile, operational
  strategy) with `make docs-audit` green.
- [ ] **P4-3** — off-hours dry run on the local stack: full chain via the single
  command; rerun = no-op; recovery demo; evidence under `logs/`.
- [L] **P4-4** — Monday in-session validation: real feed, decided universe,
  the command as the sole entry point; evidence + comparison to the profiling
  ladder.
- [-] **P4-5** — optional systemd timer for automatic morning start —
  trigger-gated: revisit after ≥5 consecutive green daily runs.

**Roll-up**

| Stage | Tasks | done | wip | todo | live | decide | skip |
|---|---|---|---|---|---|---|---|
| D — Operator decisions (decided 2026-09-26, all recommended) | 6 | 6 | 0 | 0 | 0 | 0 | 0 |
| P1 — Platform start-safety (no decision needed) | 2 | 1 | 0 | 1 | 0 | 0 | 0 |
| P2 — Config truth | 2 | 0 | 0 | 2 | 0 | 0 | 0 |
| P3 — Orchestrator interface | 4 | 0 | 0 | 4 | 0 | 0 | 0 |
| P4 — Ops integration and validation | 5 | 0 | 0 | 3 | 1 | 0 | 1 |
| **Total** | **19** | **7** | **0** | **10** | **1** | **0** | **1** |

## Overview — the final product

### The operator contract

One command, three verbs, no manual assembly:

```
make day ARGS="start"    # morning: whole chain up, coordinated, verified, board printed
make day ARGS="status"   # one-glance read-only board (safe to run any time)
make day ARGS="stop"     # graceful stop; state (checkpoints/savepoints) preserved
```

`start` composes what the platform already owns — it does not reimplement it:

1. **Preflight** — resolve the config contract (universe, sockets, execution
   posture); verify all live-enablement flags are OFF; check images/build
   stamps are current (rebuild only if asked).
2. **Stack** — bring up Fluss, Flink, ingestion, companions, EOD,
   observability through the native compose path. Ingestion now survives a
   not-yet-ready Fluss (P1-1), which removes the known start race.
3. **SignalJob** — ensure exactly one, ever: keep a healthy running job;
   restore through the native savepoint path if state exists; refuse a fresh
   submit unless explicitly allowed (no silent state loss, no duplicates).
4. **Execution chain** — bring up the `execution-t3` profile in its designed
   offline posture (gateway gated, bridge disabled, executor HALTED). The
   runner verifies the flags; it never flips them.
5. **Verify** — evaluate invariants I1–I9. If anything is red: exit non-zero,
   name the first failing invariant and its native recovery action. If data
   predicates are PENDING because the market is not open yet, report PENDING
   without failing (D5).
6. **Board** — print the same status board `status` prints; exit 0 only on
   GREEN/PENDING.

### The nine coordination invariants (the success bar, D5)

| # | Invariant | Check | Pre-session |
|---|---|---|---|
| I1 | Core services up | fluss×2, flink×2, ingestion, eod, companions | required |
| I2 | Fluss leadership healthy | coordinator + tablet metadata probe | required |
| I3 | Raw appends advancing | ingestion append rate ≥ floor, quarantine ≈ 0 | PENDING |
| I4 | Downstream advancing | dedup/candles/signals row movement | PENDING |
| I5 | SignalJob exactly one, healthy | RUNNING + last checkpoint age < 2× interval | required |
| I6 | Execution posture correct | gateway healthy, `EXECUTION_ENABLED=false`, bridge disabled, executor HALTED | required |
| I7 | Live flags audit | no enablement flag set anywhere in the effective config | required |
| I8 | Error budget | no fatal lines / no quarantine or zero-ack spike | required |
| I9 | Restartability discoverable | latest checkpoint/savepoint listed for the next start | required |

### Board (mock)

```
$ make day start
[day] preflight   OK   host=dev compose=true exec-posture=offline(verified) images=current
[day] universe    OK   mode=full tokens=2433 sockets=3 slots=3/3 approval=dev-only
[day] stack       OK   fluss=2/2 flink=2/2 ingestion=1/1 eod=1/1 obs=5/5 companions=2/2
[day] ingestion   OK   effective-manifest=2433 connected=3/3 appends=842/s quarantine=0
[day] signaljob   OK   RUNNING id=<..> checkpoints=COMPLETED age=9s savepoint=<latest>
[day] execution   OK   profile=execution-t3 gateway=healthy gate=CLOSED executor=HALTED(design)
[day] data-flow   OK   raw=842/s candles=../s signals=../s
[day] session     INFO market=OPEN (09:21 IST)
[day] verdict     GREEN 9/9 invariants · next: make day status
```

Failure shape (fail-closed, actionable):

```
[day] ingestion   FAIL I3 no appends for 30s · manifest=2433 tokens · sockets=3/3
                      recovery: make logs SVC=ingestion  (playbook: docs/06_operations/01-runbooks.md §daily)
[day] verdict     RED exit=1 · first failing invariant: I3
```

### What the product is not

- **Not a live-order enabler.** Live money is blocked by the release gates
  (`docs/06_operations/08-live-readiness-gaps.md`); auto-enable/auto-resume is
  prohibited (DEC-044). The runner proves the chain, it does not open the gate.
- **Not a replacement for the profiler.** Measurement runs purge tables; the
  daily tool must never do that.
- **Not a production deploy path.** This targets the local compose stack; the
  4-VM production deploy stays out of scope.
- **Not new platform architecture.** One small ingestion resilience change +
  one thin, tested orchestrator around existing native entry points.

## 2. Audit findings (facts this plan is built on)

### 2.1 Native pieces the runner composes (nothing duplicated)

| Piece | Native entry point today | Note |
|---|---|---|
| Stack bring-up | `make up` | documented `up` hazard: a recreated Fluss can leave ingestion crash-looping on no-leader; recovery today is recreating ingestion alone |
| SignalJob lifecycle | `make rollout-savepoint` (`rollout-savepoint.sh`) | savepoint→stop→redeploy with pinned checkpoint env vars; `COMPUTE_SUBMIT_SIGNAL=0` by default (no accidental double submit) |
| Companion jobs | compute launcher during stack start | babysitter + safety-halt |
| EOD | `eod-controller` service (default stack) | after-close offload |
| Execution chain | compose profile `execution-t3` | gateway `EXECUTION_ENABLED` fail-closed default false; bridge `EXECUTION_BRIDGE_MODE=disabled`; executor is gateway/bridge-only |
| Observability | otel, openobserve, minio, exporters | default stack |
| Fluss probes | `pipeline-lib.sh` readiness + existing profiler sampling | read-only; reuse |
| Tests | `code/01_platform/04_scripts/tests/test_*.py` | auto-discovered by the gate |

### 2.2 Gaps this plan closes

- **G1 — start race.** Ingestion fails fast into the Fluss no-leader window;
  nothing makes it wait (the documented `make up` hazard).
- **G2 — no orchestrator.** "Whole chain up" is three manual steps with
  ordering and singleton rules that live in operators' heads.
- **G3 — universe/socket config is manual.** Compose defaults to the 1 024
  token CSV with 1 connection; the full 2 433 needs the full CSV, 3 sockets,
  and the dev approval flag (CHG-320). Java ingestion overrides inherited
  `ARROW_INSTRUMENT_TOKENS` from the manifest — the effective universe must be
  verified, not assumed.
- **G4 — SignalJob lifecycle is manual.** Fresh vs restore is an operator
  decision; a wrong choice duplicates or loses state.
- **G5 — execution profile is manual and unguarded.** Nothing verifies posture
  before/after start.
- **G6 — no verification.** "Containers up" is not "data flowing"; no
  fail-closed success bar exists.
- **G7 — no test/doc tie-in.** Ad-hoc scripts rot; the runner must join the
  gate via auto-discovered tests and land with CHG + docs.

### 2.3 Hard constraints

- Swarm ignores compose `depends_on` conditions → ordering resilience must live
  in the ingestion client (P1-1), not in compose-only health checks.
- Production stays single-socket; multi-socket is dev-only behind the approval
  flag and a non-production `DEPLOYMENT_ENV` (CHG-320).
- Savepoint rollout requires pinned env
  (`DEDUP_TTL_MS`, `CANDLE_WINDOW_MS`, `CHECKPOINT_INTERVAL_MS`,
  `CHECKPOINT_TIMEOUT_MS`, `MAX_CONCURRENT_CHECKPOINTS`); the runner reuses the
  native script, never re-derives the pins.
- The monthly/measurement profiler purges tables → never call it from the
  daily path.

## 3. Design

### 3.1 P1 — start-safety in ingestion (the only production-code change)

- **Readiness wait:** at startup, before the first append, retry only
  **retryable availability** conditions (leader election, not-serving,
  connection refused/reset) using the existing `RetryClassifier` taxonomy;
  bounded exponential backoff; budget via a new env (default ≈90 s);
  **fail closed** after budget with a precise message. Auth/config errors still
  fail fast. When Fluss is healthy the path is unchanged (zero extra latency).
- **Interplay guard:** the zero-ack watchdog already distinguishes "never
  appended" — the wait must end before watchdog semantics begin, and tests must
  cover that boundary.
- **Capacity fail-closed (P1-2):** if tokens exceed slots × 1 024, or the
  approval flag is set outside dev, fail at start naming the values, the host
  manifest path, and the policy source.

### 3.2 P2 — config truth

- One selection (`UNIVERSE=approved|full`) expands to the existing knobs
  (`INSTRUMENT_MANIFEST_HOST_PATH`, `ARROW_HFT_CONNECTIONS`, approval env).
  Preflight verifies the **effective** manifest the JVM loaded (log/config
  probe), not the file we think we passed.
- Execution posture guard: the runner reads effective config and refuses to
  start `execution-t3` if any live-enablement flag is set on a dev host;
  the board always prints the posture it verified.

### 3.3 P3 — orchestrator

- `day-run.sh` owns sequencing, idempotency, and the board; `make day` is a
  3-line wrapper. Subcommands are safe to re-run any time.
- `start` ordering: preflight → up → wait healthy → ensure SignalJob (keep /
  restore / refuse-fresh) → execution profile (offline) → verify → board.
- `stop`: graceful stop preserving checkpoints and volumes; never deletes
  state; `--savepoint` remains an explicit separate action (D4).
- Singleton rules: at most one SignalJob is ever observed; two → fail with the
  native recovery action; none + restorable state → native restore; none + no
  state → refuse unless explicitly allowed.

### 3.4 Verification predicates

Pure decision functions (unit-testable) + thin read-only probes
(compose/Flink REST/pipeline-lib). The data predicate is session-aware: inside
09:15–15:30 IST it must see movement within a bounded window; outside it
reports PENDING (D5). Every failure names the invariant and the native recovery
action from the runbook.

### 3.5 Rejected approaches

| Not this | Why |
|---|---|
| a large bespoke `daily.sh` reimplementing compose/job management | duplicates native logic → drifts, needs maintenance |
| `depends_on: service_healthy` as the ordering fix | swarm ignores compose conditions; crash returns in production |
| auto-enable/auto-resume of execution | prohibited (DEC-044); safety-critical |
| a second SignalJob supervisor | split-brain risk; the native savepoint path owns lifecycle |
| profiler reuse for daily status | profiler purges tables by design |
| scheduler/timer in v1 | adds a moving part before the interface is proven |

### 3.6 Footprint

- Changed: ingestion startup path + tests (`P1`), runbook/docs, Makefile
  (one thin target).
- Added: `day-run.sh`, `test_day_run.py`, plan docs; CHG records (next free
  numbers at land time: P1 → CHG-322 class, runner → CHG-323 class).
- Untouched: profiler, gates, release semantics, production policy.

## 4. Evidence plan per item

| Item | Evidence on land |
|---|---|
| P1-1 | failing-first test (waits on retryable, fast-fails on fatal, budget bound) → green scoped suite; static-check |
| P1-2 | unit test on the capacity boundary + message content |
| P2-1/2 | decision-function tests; effective-config probe demo |
| P3-1/2/3/4 | auto-discovered tests green; off-hours dry run log (`logs/`); rerun no-op demo; recovery demo |
| P4-1/2 | runbook + docs; `make docs-audit` green |
| P4-4 | Monday in-session log under `logs/`; board snapshot; comparison to the profiling ladder |

## 5. Risks and open issues

- Off-hours DataStream staleness: I3/I4 must not be tuned to off-hours values;
  Monday in-session is authoritative (P4-4).
- Restart from checkpoint assumes checkpoint storage survives `stop`; the
  `stop` design must preserve it (D4).
- The Java manifest override can silently change the universe; preflight reads
  the effective value (P2-1).
- Fresh SignalJob submit without state can duplicate/count-again; the runner's
  default is refuse-and-report (P3-1).
- Pre-session start must not look broken; PENDING semantics are explicit (D5).
- Next free CHG numbers are assigned at land time; the plan names classes, not
  fixed numbers.

## 6. Out of scope

- Live-order enablement or execution-flag changes (release-gated; DEC-044).
- Production 4-VM deployment/validation.
- Scheduling/timer automation (P4-5, trigger-gated).
- Profiler/gate behavior changes; measurement runs.
- Feature-layer warm-up / trade-rate semantics (separate open decisions).
- Anything on the broker side (entitlements, session hours).

## 7. Rollback

- P1: env-gated wait; budget `0` restores fail-fast behaviour; revert commit is
  sufficient.
- P2/P3: new script + target + tests only; removing the target/script reverts
  the interface; no stack state depends on the runner.
- Docs/CHG: revert commit.

## 8. Order of work and effort

1. Close D1–D6 (operator).
2. P1 (-1/-2) + CHG — smallest, highest value: removes the only start race.
3. P2 + P3 (one change, CHG) — config contract + orchestrator + tests.
4. P4 docs, then off-hours dry run, then the Monday in-session validation.

Effort estimate: P1 ≈ half a session; P2+P3 ≈ 1–2 sessions; P4 ≈ one off-hours
hour + the Monday window. No item touches release-gated semantics.
