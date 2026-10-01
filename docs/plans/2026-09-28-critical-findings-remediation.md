# Critical findings remediation — audit 2026-09-28 (C1–C3 scope doc)

**Created:** 2026-09-28 · **Status:** implemented — operator go-ahead recorded; roll-up 18 done /
1 live (C3-8 on-VM rehearsal) / 1 skipped by decision (C2-5) of 20; see the tracker below.
**(Updated 2026-10-01: this line previously said "awaiting the go-ahead"; the work has landed.)**
**Source:** `logs/full-project-audit-20260928/REPORT.md` — the three Critical rows only.
**Execution protocol:** `docs/plans/2026-09-24-plan-execution-protocol.md` (verify-first, marker
discipline, failing-first tests, one CHG per commit, smoke-before-run, tracker hygiene).
**Scope:** exactly C1 (live postback mapping), C2 (secrets in day evidence), C3 (daily-VM
toolchain). High/Medium/Low findings stay in the backlog; they are not touched here.
**Operator goal:** fix each defect once, at its root boundary, with a permanent machine guard and
a test that fails if the defect returns. No host-specific firefighting, no mode the operator must
remember.

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
| C1 — live postback mapping (fills/cancels/rejects must never be dropped) | 7 | 7 | 0 | 0 | 0 | 0 | 0 |
| C2 — broker secrets must never reach day evidence | 5 | 4 | 0 | 0 | 0 | 0 | 1 |
| C3 — golden image must run the daily paths without a host JDK | 8 | 7 | 0 | 0 | 1 | 0 | 0 |
| **Total** | **20** | **18** | **0** | **0** | **1** | **0** | **1** |

#### C1 — live postback mapping (fills/cancels/rejects must never be dropped)

- [x] **C1-1** — Canonical postback event vocabulary + one shared, machine-readable fixture
  (`code/testdata/postback-report-types.json`): every Arrow `reportType`/`orderStatus` combination
  from `docs/04_contracts/arrow_broker.md` maps to exactly one canonical `event_type`
  (`order_filled` / `order_canceled` / `order_rejected` / `order_accepted` / `order_unknown`).
- [x] **C1-2** — Bridge normalization (`postback.go`): publish `event_type` (canonical) alongside
  the untouched raw `report_type` (fingerprint stays pinned to the raw value, contract 06) and a
  new optional `reject_reason`; unknown or absent Arrow vocabulary → `event_type="order_unknown"`
  (fail-closed, never a silent success).
- [x] **C1-3** — Fill payload mapping on the real postback shape: when `fillQuantity`/`fillPrice`
  are absent (Arrow postbacks carry `fillShares`/`averagePrice` — contract `arrow_broker.md`
  §4), the bridge fills `fill_quantity`/`fill_price` from `fillShares`/`averagePrice` so the
  executor never fabricates and never halts on a valid live fill.
- [x] **C1-4** — Executor dispatch on `event_type` (`client.rs::handle_report`): explicit arms for
  filled/canceled/rejected/accepted; **any missing or unrecognized value halts the gate**
  (protocol-drift sentinel) with a metric and an audit reason — replaces the current `_ => {}`.
  (DECIDED 2026-09-28 — fail-closed halt confirmed, §9.)
- [x] **C1-5** — `FakeBridge` emits the canonical `event_type` too, so fake and live paths share
  one vocabulary; existing behavior tests keep passing unchanged.
- [x] **C1-6** — Cross-boundary guard tests: Go test iterates the shared fixture; Rust test
  iterates the same fixture and asserts the dispatch action per canonical value plus halt on
  unknown/missing; a gate-discovered Python test asserts the fixture covers the contract's Arrow
  vocabulary exactly.
- [x] **C1-7** — Contract/dossier update: `06-action-capture.md` (raw `report_type` stays the
  fingerprint key; canonical `event_type` is the dispatch key) and `07-executor.md` (mapping
  table + fail-closed rule).

#### C2 — broker secrets must never reach day evidence

- [x] **C2-1** — `day_run.container_env()` reads an explicit posture allowlist only
  (`EXECUTION_ENABLED`, `EXECUTION_BRIDGE_MODE`) instead of the service's full `Config.Env`.
- [x] **C2-2** — Defense in depth: a redaction pass (key-name pattern) applied to the evidence
  payload before it is written, and evidence permissions `0700` directory / `0600` files.
- [x] **C2-3** — Tests: allowlist output, scrubber, "written file contains no secret value",
  and permissions.
- [x] **C2-4** — Containment: redact/purge the already-written `logs/day/*/facts.json` files that
  contain the three broker secret keys (measured 2026-09-28: 10 files under `logs/day/`).
- [-] **C2-5** — Credential rotation: **operator decision 2026-09-28 — later, not now**
  (trigger = operator schedule). The password / TOTP seed / gateway-bridge tokens were written to
  disk in cleartext (local only, but routinely copied for audits); rotate when convenient. Purge
  (C2-4) stands regardless.

#### C3 — golden image must run the daily paths without a host JDK

- [x] **C3-1** — Bake `FlussReadLagProbe.class` into the ingestion image (compile in the
  `java-builder` stage against the shaded `ingestion.jar`, COPY to `/app/probe/`).
- [x] **C3-2** — `day_run` runs the probe through the running ingestion container
  (`docker compose exec -T ingestion java … FlussReadLagProbe … fluss-coordinator:9123`); the
  host `javac` + `target/cp.txt` dependency is deleted.
  (DECIDED 2026-09-28 — container path on every host, PC and VM; §9.)
- [x] **C3-3** — `eod_schedule.py` gains `--runner host|compose` (env `EOD_RUNNER`, default
  `host`): compose mode runs the existing one-shot service —
  `docker compose -f <file> run --rm -T eod-controller run` — while timing, heartbeat and the
  `EOD_LAST_RUN_FILE` stamp stay on the host.
- [x] **C3-4** — Compose `eod-controller` service passes the `EOD_*` variables through
  explicitly (`EOD_DATABASE`, `EOD_STATE_TABLE`, `EOD_TABLES`, `EOD_TTL`, `EOD_SAFETY_FLOOR`,
  `EOD_EXTENSION`, `EOD_OFFLOAD`, `EOD_ZONE`); today the comment claims it, the block does not.
- [x] **C3-5** — `vm-golden-build.sh`: install `python3` + `tzdata` when missing; extend
  `--check` to assert the in-image toolchain the daily flow uses (JRE, probe class, EOD tool +
  java + m2 layout, scheduler compose mode) and fail **before** the snapshot, not at 09:00.
- [x] **C3-6** — Docs: `CLOUDPE_DAILY_VM.md` (§2/§3 + the failure-mode row that says to run the
  host EOD command), `.env.vm.example` (`EOD_RUNNER=compose`, `EOD_COMPOSE_FILE`), EOD runbook
  sections.
- [x] **C3-7** — Tests: scheduler compose runner (stubbed docker), golden-recipe pins extended,
  day_run probe command builder.
- [L] **C3-8** — On-VM rehearsal: one throwaway CloudPe VM built by the recipe; `--check` PASS;
  `make day ARGS="start"` / `status` / `stop` in a non-market window prove the probe path and
  the 15:45 EOD compose path end to end.

## Overview

This is a three-region wave, not a feature. Each region changes one boundary and one guard:

| Finding | Boundary that failed | Fix boundary | Permanent guard |
|---|---|---|---|
| C1 | bridge↔executor postback vocabulary drifted (bridge sends Arrow words, executor matches fake-only words) | bridge normalizes once; executor dispatches on canonical `event_type` and halts on anything else | shared fixture consumed by Go + Rust tests; Python covers the Arrow vocabulary; runtime halt + metric on drift |
| C2 | day evidence copied raw container env | allowlist + redaction at the single writer | unit tests on the writer + on-disk assertions; leaked files purged |
| C3 | golden image promised Docker-only but the daily flow used host Java | both Java paths move into images that already ship the toolchain | `--check` proves the in-image assets before snapshot; scheduler/probe unit tests |

## 1. Verify-first (reproduced on `5b4725d2`)

| # | Verdict | Evidence |
|---|---|---|
| C1 | **REPRODUCES** | Bridge publishes Arrow's raw `reportType` (`postback.go:234,252,271`; its own test pins `reportType:"Fill"` at `postback_test.go:15`). Executor matches only `order_filled`/`order_canceled` (`client.rs:604-611`), and those values exist only in the fake bridge/tests (`fake.rs:344,477`). Real fills/cancels/rejects hit the empty `_ => {}` arm. Added on the same path: `fillQuantity`/`fillPrice` are read from the postback (`postback.go:255-256`) but the documented Arrow postback shape carries `fillShares`/`averagePrice` (`arrow_broker.md:100-103`), and `handle_fill` requires the former (`client.rs` `bridge_positive_decimal("fill_quantity", …)`), so a type-correct fill would still halt. `rejectReason` is never carried into the envelope. |
| C2 | **REPRODUCES** | `container_env()` takes `docker inspect .Config.Env` of `execution-bridge` (`day_run.py:777-791,893`), whose compose env holds `ARROW_APP_SECRET`/`ARROW_PASSWORD`/`ARROW_TOTP_KEY`/tokens (`docker-compose.yml:988-1005,1040,1084`); `write_evidence` writes the whole facts payload (`day_run.py:1190-1193`). Measured today: 10 `logs/day/*/facts.json` files each contain all three broker secret keys. |
| C3 | **REPRODUCES** | `vm-golden-build.sh` installs Docker only (`:100-119`) and `--check` tests docker/env/images only (`:88-95`). `day start` needs host `javac` + `code/02_services/01_ingestion/target/cp.txt` (`day_run.py:54-55,794-817`); the 15:45 unit runs host `eod_schedule.py` (`vm-golden-build.sh:182`), which runs host python (`eod_schedule.py:141-154`) → host `java` + `~/.m2` + `common/target/classes` (`eod_controller.py:152`). None of these exist on a recipe-built fresh VM. The `ddl-apply` image already ships all three (`Dockerfile:77-124`), so the toolchain exists where it is needed — only the wiring is wrong. |

## 2. Root cause (one sentence each)

- **C1** — the broker's vocabulary and the executor's vocabulary were never pinned to a shared
  contract; the only value-producing path exercised in tests was the fake bridge, so a
  two-vocabulary system passed CI. The fill-payload half is the same class: the bridge forwards
  Arrow's documented fields but the executor reads a different field pair.
- **C2** — evidence collection and posture auditing shared one function; posture needs 2 keys,
  evidence kept everything.
- **C3** — the VM recipe was written for the local-dev execution model (host toolchain) while the
  platform's containers already carry the toolchain; nothing asserts the morning path at image
  build time.

## 3. Recommended design (native; no new dependency)

### C1 — one vocabulary, one fixture, fail-closed drift detector

1. **Canonical vocabulary** (documented in contract 06 and 07):

   | Canonical `event_type` | Source (Arrow) | Executor action |
   |---|---|---|
   | `order_filled` | `reportType=Fill` (any `orderStatus`) or `COMPLETE` with fill fields | book fill (uses normalized qty/price) |
   | `order_canceled` | `reportType=Canceled` (or `Cancelled`) | emit `order_canceled` |
   | `order_rejected` | `reportType=Rejected` or `orderStatus=REJECTED` | emit `order_rejected` with `reject_reason` |
   | `order_accepted` | `reportType=NewAck` / `PendingNew` | counted, no state change |
   | `order_unknown` | anything else | **safety halt** + metric + warning |

2. **Bridge** (`postback.go::NormalizeOrderUpdate`): compute `EventType` from the pair
   (`reportType`, `orderStatus`) using the table above; keep `ReportType` raw (fingerprint input
   unchanged — contract 06 vectors still hold); add `RejectReason` (from `rejectReason`); when the
   event is a fill and `fillQuantity`/`fillPrice` are empty, copy `fillShares`/`averagePrice` into
   them (raw fields stay for the fingerprint/audit).
3. **Executor** (`client.rs::handle_report`): match on `event_type`:
   filled → `handle_fill`; canceled → `emit_order_canceled`; rejected → `emit_order_rejected`;
   accepted → counter; `None` or unknown → `gate.safety_halt()` + warning + metric (the
   uncorrelated-report path already sets this precedent, `client.rs:570-597`).
4. **Shared fixture** `code/testdata/postback-report-types.json` — one record per Arrow input
   shape with `{arrow: {...}, event_type, normalized: {fill_quantity, fill_price, reject_reason}}`.
   Go reads it with a relative path; Rust embeds it with `include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../testdata/…"))`. A gate-discovered Python test
   validates the fixture against `arrow_broker.md` §4 (`NewAck/PendingNew/Fill/Canceled/Rejected`).
5. **Fake bridge**: emits `event_type` with the same canonical values; `contract_version` stays
   additive (new fields are optional on the wire).

**Rollout order (documented, because the two sides deploy together but not atomically):**
bridge image first (adds the field; old executor ignores it, unchanged behavior), then executor
image (activates dispatch). Rolling back only the executor leaves new-bridge→old-executor =
current bug; rolling back only the bridge leaves old-bridge→new-executor = every postback halts
(loud, safe). The executor is deployed with orders halted, so either order is safe to land here.

### C2 — two keys for posture, redaction at the writer, private files

1. `POSTURE_ENV_KEYS = ("EXECUTION_ENABLED", "EXECUTION_BRIDGE_MODE")` — `container_env()`
   returns only these per service; `posture_violations()` behavior is unchanged.
2. `redact_evidence(payload)` replaces values whose key matches
   `(?i)(SECRET|TOKEN|PASSWORD|TOTP|CREDENTIAL|API_KEY|ACCESS_KEY|PRIVATE_KEY|AUTH)` with
   `"[REDACTED]"`; applied inside `write_evidence` before `json.dumps`.
3. Evidence dir `0o700`, `facts.json` and `board.txt` `0o600` (today: default umask).
4. Containment: redact or delete the 10 existing leaked `facts.json` (operator command recorded
   in this plan); rotation is a separate operator decision (C2-5).

### C3 — Docker is the only host toolchain; the image proves it

**Recommended probe path (decided 2026-09-28): Path A — container path on every host.** The
probe runs inside the running ingestion container via `docker compose exec` on the dev PC and on
the VM; the host `javac` + `target/cp.txt` route is deleted. Options considered:

| Path | Description | Verdict |
|---|---|---|
| **A — container everywhere (recommended, DECIDED)** | Probe class shipped in the ingestion image, executed with `docker compose exec` on every host; no host Java | **Chosen**: one command, one behaviour; the image already carries the JRE + Fluss client; `vm-golden-build.sh --check` proves the assets before the snapshot |
| B — host `javac` on dev, container on the VM | Two code paths for one check | Rejected: a VM-only behaviour can hide from the dev PC — the exact failure class this wave closes |
| C — install a JDK + Maven artifacts on the VM | Keep host Java and add the toolchain to the golden image | Rejected: host toolchain drift and image-sync overhead; more to remember every morning |

**Recommended EOD path (decided 2026-09-28): compose runner.** `eod_schedule.py` keeps
timing/heartbeat/stamp on the host and executes the controller in the existing `eod-controller`
container (`docker compose … run --rm -T eod-controller run`). Alternatives — host Java on the VM
(same rejection as Path C above) and a long-running in-container scheduler (loses the host
stop-gate stamp) — are rejected.

1. **Probe**: add to `02_services/01_ingestion/Dockerfile` — in `java-builder`:
   `COPY 01_platform/04_scripts/fluss-probes/FlussReadLagProbe.java /src/probe/` then
   `javac -cp 02_services/01_ingestion/target/ingestion.jar -d /probe-classes /src/probe/…`
   (shaded fat jar already contains `org.apache.fluss.*`); runtime stage:
   `COPY --from=java-builder /probe-classes/FlussReadLagProbe.class /app/probe/`.
2. **day_run**: replace `_compile_fluss_probe`/host-java with one command builder:
   `docker compose exec -T ingestion java --add-opens=… -cp /app/ingestion.jar:/app/probe
   FlussReadLagProbe default <table> fluss-coordinator:9123`; parse the same output line
   (epoch, table, partitions, buckets, logEndSum). No `javac`, no `cp.txt`, on any host.
3. **EOD**: `eod_schedule.py` `--runner compose` executes
   `docker compose -f "${EOD_COMPOSE_FILE:-code/01_platform/01_docker/docker-compose.yml}"
   run --rm -T eod-controller run` (controller args appended); timing/heartbeat/last-run stay on
   the host, so `DAY_STOP_REQUIRE_EOD` semantics do not change. Compose mode fails loud if docker
   or the file is missing.
4. **Compose**: add the `EOD_*` pass-through entries to the `eod-controller` environment block
   (comment already promises it; the block omits them).
5. **Golden build**: install `python3`+`tzdata`; `--check` runs, in order:
   docker/compose → `.env`/`.env.vm` → 7 images → `java -version` inside ingestion →
   `/app/probe/FlussReadLagProbe.class` exists → EOD image has `java`, the controller script and
   `/opt/ddl-apply/m2/repository` → `eod_controller.py --help` exits 0 inside the EOD image →
   `.env.vm` carries `EOD_RUNNER=compose` + the fresh-start/stop-gate keys. Any miss = non-zero
   exit with the exact missing item (snapshot must not proceed).
6. **Docs**: morning guide, EOD runbook and `.env.vm.example` updated to the container commands.

## 4. Permanent guards (the "will not come back" list)

| Guard | Kills recurrence of | Where it runs |
|---|---|---|
| Shared fixture + Go/Rust/Python tests | C1 vocabulary drift (any side changes alone → test fails) | Go suite, Rust suite, gate step 3 auto-discovery |
| Executor halt on unknown/missing `event_type` + metric | C1 silent drops if a new Arrow value appears live | live runtime |
| Fill payload normalization test with the real Arrow shape (no `fillQuantity`/`fillPrice`) | C1 fill-field drift | Go suite + Rust suite |
| Allowlist unit test + "file contains no secret" test + perms test | C2 re-leak through the same writer | `make test` (day_run tests) + gate step 3 |
| Redaction pass (defense in depth) | C2 re-leak through a future field | runtime writer |
| Golden `--check` image-asset assertions + extended `test_vm_golden_recipe.py` | C3 an image without the toolchain reaching a VM | image build + gate step 3 |
| Scheduler compose-runner unit test (stubbed docker) | C3 unit/EOD wiring drift | gate step 3 |
| day_run probe command test | C3 probe path drifting back to host java | gate step 3 |

## 5. Code map (current lines, for the commit slices)

| Region | File | Current lines |
|---|---|---|
| C1 bridge | `code/02_services/06_execution_bridge/go-bridge/postback.go` | 234 (`reportType`), 252-256 (envelope fields), 271 (fingerprint map), 295-301 (`knownReportType`) |
| C1 bridge test | `.../postback_test.go` | 15 (pins raw `"Fill"`) |
| C1 executor | `code/02_services/04_executor/src/execution/client.rs` | 598-612 (`handle_report` dispatch), 560-597 (uncorrelated halt precedent), 645-665 (fill validation) |
| C1 fake | `.../src/bridge/fake.rs` | 344, 477; `transport.rs` test helpers 939, 954 |
| C1 protocol | `.../src/bridge/protocol.rs` | 547-596 (`ReportEnvelope`) |
| C1 fixture (new) | `code/testdata/postback-report-types.json` | new |
| C2 writer | `code/01_platform/04_scripts/day_run.py` | 777-791 (`container_env`), 267-278 (`posture_violations`), 1183-1193 (`write_evidence`) |
| C2 tests | `code/01_platform/04_scripts/tests/test_day_run.py` | add cases |
| C3 probe image | `code/02_services/01_ingestion/Dockerfile` | 25-34 (builder), 38-44 (runtime) |
| C3 probe call | `code/01_platform/04_scripts/day_run.py` | 54-55 (`CP_FILE`), 794-817 (`_compile_fluss_probe`, `fluss_log_end`) |
| C3 scheduler | `code/01_platform/04_scripts/eod_schedule.py` | 141-154 (`run_controller`) |
| C3 controller | `code/01_platform/04_scripts/eod_controller.py` | 152 (`java -cp`) |
| C3 compose | `code/01_platform/01_docker/docker-compose.yml` | 1145-1157 (`eod-controller`) |
| C3 golden | `code/01_platform/04_scripts/vm-golden-build.sh` | 88-95 (`--check`), 100-119 (install), 161-191 (env + unit) |
| C3 docs | `docs/05_deployment/CLOUDPE_DAILY_VM.md` | 58, 105-107, 133 |

## 6. Test plan (failing-first + mutation)

Order per region: write the failing test against current code, watch it fail, land the fix,
watch it pass, then mutate the fix and watch the test catch it.

- **C1 (Go):** `postback_normalization_test.go` — fixture iteration; real-shape fill maps
  `fill_quantity`/`fill_price`; unknown type → `order_unknown`; fingerprint vectors from
  contract 06 still match (regression pin).
- **C1 (Rust):** dispatch test per canonical value (filled books, canceled emits, rejected emits,
  accepted counts, unknown/missing halts); fill from `fill_shares`/`average_price` books; fill
  with neither halts; existing fake-based tests updated to `event_type`.
- **C1 (Python, gate-discovered):** `test_postback_contract_fixture.py` — fixture covers the
  contract vocabulary exactly; canonical set matches the contract table in 06/07.
- **C2 (Python):** allowlist output; scrubber; written `facts.json` has no secret value and mode
  `0600`; dir `0700`.
- **C3 (Python):** scheduler compose runner builds the exact command with a stubbed docker on
  `PATH` (host mode still default and tested); day_run probe command builder; golden-recipe test
  pins the new `--check` assertions; ingestion Dockerfile probe-copy assertions.
- **Mutation checks (minimum):** C1 — change one mapping row (caught by fixture), remove the
  halt arm (caught by dispatch test). C2 — re-add a secret key to the allowlist (caught), remove
  the scrubber (caught by on-disk test). C3 — point the scheduler back at host python (caught),
  drop the probe COPY (caught by recipe test).

## 7. Rollout and evidence plan

1. Commit per region (C1 bridge+fixture+tests; C1 Rust+protocol; C2; C3 image+probe; C3
   scheduler+compose+golden+docs), one CHG record each under `docs/05_deployment/change-records/`.
2. Per-region smoke: C1 — Go suite + Rust suite (`cargo test`) offline; full contract test.
   C2 — `make test` (day_run slice); inspect a freshly written facts file. C3 — local
   `docker compose run --rm eod-controller --help`, local probe `exec` against the running
   stack, then `make day ARGS="status"`.
3. Certification: `make gate` (full 19 steps) + `make static-check` + docs audit;
   `plan_tracker.py --check` green.
4. C3-8 on-VM rehearsal evidence into `logs/` (dated): `--check` output, `help` from the EOD
   image, probe sample, EOD run stamp, `stop` gate accepting the stamp.
5. No production deploy and no live-money action is part of this wave; orders remain halted.

## 8. Risks and rollback

| Risk | Mitigation |
|---|---|
| New executor sees a postback without `event_type` (rollback window) | It halts (loud, safe); documented rollout order bridge→executor; both ship in one image set |
| Rust `Include_str!` fixture path breaks on a moved file | Fixture validated by a gate-discovered Python test that fails on a missing/misplaced file |
| Probe in-image classpath differs from the shaded jar's contents | `--check` runs the class inside the image (imports must load) before snapshot; day_run returns PENDING if exec fails (boards never lie) |
| Compose-mode EOD needs the stack up | Same precondition as today (controller already requires Fluss); `depends_on` starts it; unit test stubs docker |
| Redaction hides a real posture problem | Posture checks run on the allowlisted raw values before evidence is written; only the written copy is filtered |
| Secret rotation is skipped | Tracked as C2-5, an explicit operator decision, not silently assumed done |

## 9. Operator decisions — recorded 2026-09-28

| # | Decision | Ruling | Effect on this plan |
|---|---|---|---|
| 1 | C2-5 credential rotation | **Later** (operator will do it, not now) | C2-5 → `[-]`, trigger recorded; C2-4 purge proceeds now; treat the on-disk secrets as burned until rotated |
| 2 | C1-4 halt policy | **Fail-closed halt confirmed** (the recommended answer) | unchanged: any missing/unrecognized postback `event_type` halts the gate + metric + audit reason |
| 3 | C3 probe path | **One path everywhere — Path A in §3 (container path on every host)** | the probe runs inside the running ingestion container (`docker compose exec`) on the **dev PC and the VM**; the host `javac` + `target/cp.txt` route is deleted. Rejected: host-on-dev + container-on-VM, because a two-path split is exactly how the VM-only C3 defect hid from the dev PC |
| 4 | C3-8 rehearsal | **Later** (operator will schedule) | C3-8 stays `[L]`; the wave lands and certifies offline, the on-VM proof is booked by the operator |

## 10. Out of scope

- The 19 High, 32 Medium and 20 Low findings (their own waves; e.g. the Safety_Halt consumer is
  High and touches the same component but is a separate region).
- Any live-money enablement, production `docker stack deploy`, hosted CI, or retention changes.
- `03_action_capture` (retired) and the currently green execution-gate machinery.
