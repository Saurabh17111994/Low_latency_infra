# Wave 36 — make `holistic-measure.sh` produce trustworthy measurements

## Overview

The wave-36 audit filed 18 findings against
`code/01_platform/04_scripts/holistic-measure.sh` (428 lines). A
pre-implementation **Fix Audit** re-checked every finding against the real file
and falsified each proposed fix. That changes the wave's shape:

* **15 findings need code changes.** Two (`P6-104`, `P6-105`) are **not bugs** —
  the `EXIT` trap already runs cleanup. One (`P6-103`) is **already fixed** and its
  cited line no longer exists; re-adding the call would purge a retired table.
* **Seven proposed fixes are wrong or absent** — six are bare `# TODO` comments
  (`P6-007`, `P6-405`, `P6-406`, `P6-408`, `P6-409`, `P6-410`) and `P6-106`'s
  separator produces *byte-identical* corrupt output to what it claims to fix.
  Two more need correction: `P6-102`'s fix changes nothing on its own, and
  `P6-007`'s stated mechanism is wrong in both halves.
* The harness **has never produced a run directory**. `logs/tracker-14/` holds no
  `holistic-measure-*` directory, and `git log -S 'JVM_PID='` shows the variable
  was never assigned in the file's entire history. It cannot run today.

**Desired outcome:** the harness completes both phases on the live dev cluster,
its evidence files carry correct values, and every gate fails closed.

**Acceptance criteria:**

1. No host-PID read remains (`JVM_PID`, `FAKETOOL_PID`, `BPID` gone); liveness is
   proved against the containers that carry the data path.
2. A live two-phase run completes and writes `smoke/` + `main/` evidence.
3. `main/throughput.tsv` columns are `epoch / operator / read / write / state`
   with read and write values in the correct columns.
4. `main/proc-io.tsv` carries one integer row per container per poll — no embedded
   newlines from multi-device `io.stat`, no empty `jvm`/`bridge` rows.
5. A failed table purge stops the run before measuring, unless the operator opts
   in explicitly.
6. Invalid knobs (`POLL_S=0`, `SMOKE_S=abc`) fail fast naming the knob, before
   Phase A starts.
7. The G8 fingerprint gate fails when `j1/java.out` is missing or empty.
8. A dead loadgen or ingestion container ends the phase with a message naming the
   container, and the phase is torn down.
9. The poll body stays bounded: one REST call per vertex per sample, and pacing is
   deadline-based so a slow poll does not silently stretch the cadence.
10. All 18 findings carry a recorded verdict; the two not-bug and one already-fixed
    findings are recorded as such, not silently dropped.

## Context

### Files and symbols

| Path | Role |
|---|---|
| `code/01_platform/04_scripts/holistic-measure.sh` | The target, 428 lines. `run_phase()` L71-264 owns the measurement loop; `collect_vertex_metrics()` L270-295; `smoke_inject_gate()` L318-351; `fingerprint_gate()` L366-377; phase driver L297-428. |
| `code/01_platform/04_scripts/pipeline-lib.sh` | Sourced; owns all bring-up/teardown. `pipeline_purge_table` L743-818, `pipeline_purge_raw_table` L820-823, `pipeline_cleanup` L992, `pipeline_install_cleanup_trap` L1010-1023, `pipeline_validate_rate` L167-173, `pipeline_preflight` L362-523. |
| `code/01_platform/04_scripts/holistic-analyze.py` | Consumer of the evidence. Reads `main/proc-io.tsv` at L1127-1170 (A2b ranking) and L1281-1303 (G6c tablet read-storm). Does **not** read `throughput.tsv`. |
| `code/01_platform/04_scripts/tests/test_holistic_measure_wave36.py` | **New.** The wave-36 suite. |
| `code/01_platform/04_scripts/test-pipeline-lib.sh` L174-177 | Existing gate **G9** greps `holistic-measure.sh` for the literal `fingerprint_gate`. The function name must survive. |

### Corrected line references

The doc's references drifted ~3 lines. Verified targets:

| Finding | Doc says | Real line | What is there |
|---|---|---|---|
| P6-007 | L196 | **L199** | `echo "$now jvm $(grep … /proc/$JVM_PID/io …)"` |
| P6-008 | L246 | **L249** | `kill -0 "$FAKETOOL_PID" …` |
| P6-009 | L247 | **L250** | `kill -0 "$JVM_PID" …` |
| P6-102 | L91 | **L95** | `pipeline_purge_raw_table \|\| true` |
| P6-103 | L94 | **L94** | a comment about purge ordering — the cited line is unrelated to the finding |
| P6-106 | L135 | **L138** | `awk -F'\| ' …` |
| P6-107 | L138 | **L141** | `DISK_DEV="$(df /var/lib/docker …)"` |
| P6-108 | L401 | **L404** | the analyzer invocation |
| P6-405 | L82 | **L86** | `export INJECT_AFTER_MS=120000 …` |
| P6-406 | L126 | **L129** | `for ((i = 0; i < duration_s; i += POLL_S))` |
| P6-407 | L191 | **L194-195** | the two `awk` io.stat reads |
| P6-411 | L365 | **L368** | the `grep -ac` inside `fingerprint_gate` |
| P6-743 | L404 | **L407** | `set +o pipefail` |

### Existing patterns to reuse

* **Container liveness** — `pipeline-lib.sh` L620 and L698 both do
  `docker inspect --format '{{.State.Running}}' "<container>" 2>/dev/null || echo false`.
  There is **no** shared `container_running` helper; the doc's P6-008 fix invents
  one. Inline the existing pattern instead.
* **Container names** are lib variables, not literals: `LIB_FAKETOOL_CONTAINER`
  (`pipeline-faketool`) and `LIB_INGESTION_CONTAINER` (`pipeline-ingestion`),
  defined at `pipeline-lib.sh` L99-100. Liveness probes must use them, never the
  `01_docker-*` stack names.
* **Batched metric fetch** — `loadtest-collect.sh` L136 already requests several
  metrics in one call: `…/vertices/$VID/metrics?get=0.a,0.b,0.c`. Precedent for
  P6-409.
* **Test-hook precedent** — `G7_REUSE_RAW=1` (`holistic-analyze.py` L1355),
  `W34_PRE_FIX_SRC` (wave 34), `DIGEST_PIN_LIVE` (wave 35).
* **Reset-aware counter walk** — `counter_deltas()` in `holistic-analyze.py`
  L164-192 already implements exactly what P6-410 needs. Reuse it rather than
  writing a second implementation.

### Constraints

* `set -uo pipefail` (L38) — an unbound variable is fatal. That is the mechanism
  behind three findings and the reason the script cannot run.
* `shellcheck -S warning` (the CI gate, `Makefile` L476-477) reports **nothing**
  on this file — verified `rc=0`, empty output, even at `-S style`. `JVM_PID` is
  invisible to shellcheck because the script sources `pipeline-lib.sh`. **The CI
  gate cannot protect this change; the new tests are the only guard.**
* `docs/plans/` convention: `### Task N:` headings with `- [ ]` checkboxes are the
  `plan-exec` interface.
* Findings live in `~/.opencodereview/sessions/…/phases/p6-ops-scripts-tests-chaos-audit.md`
  — **outside the repo, in a git repo with no commits**, so tick-marking is not
  version-controlled (same as wave 35).
* CHG records: check the 9 triggers in `docs/08_implementation/01-foundation.md`
  L323-336 before filing. Task 2 removes a caller opt-in that `CHG-154.md`
  documents as deliberate — decide there, with evidence.

## Review Handoff

**Selected approach.** Fix in dependency order, because only the first block
unblocks a live run. Every fix is the smallest change that makes the behaviour
correct; where the doc's fix is wrong, the verified replacement is stated inline.

**Findings disposition — 18 findings, 18 verdicts.**

| ID | Verdict | Doc's fix | Plan's fix |
|---|---|---|---|
| P6-007 | REAL (mechanism wrong) | `# TODO` only | Delete the `jvm` row; sample the ingestion container's cgroup `io.stat` instead |
| P6-008 | REAL | inline `docker inspect` on `LIB_FAKETOOL_CONTAINER` | Same, plus a message naming the container |
| P6-009 | REAL | inline `docker inspect` on `LIB_INGESTION_CONTAINER` | Same |
| P6-102 | REAL, fix inert | `\|\| return 1` | Remove `export ALLOW_STALE_TABLE=true` **and** change `\|\| true` → `\|\| return 1` — both, or neither takes effect |
| P6-103 | **ALREADY FIXED** | `\|\| return 1` | **Nothing.** `pipeline_purge_preview_table` was deleted in `72bd64ae`; re-adding it would purge a retired table |
| P6-104 | **NOT A BUG** | add `pipeline_cleanup` | Nothing — the `EXIT` trap already runs it (proved) |
| P6-105 | **NOT A BUG** | add `pipeline_cleanup` | Nothing — same |
| P6-106 | REAL, fix wrong | `-F' \| '` | `-F' *[|] *'` — the proposed form yields identical corrupt output |
| P6-107 | REAL | hoist `DISK_DEV` | Hoist `DISK_DEV` **and** the 6 `docker inspect` calls (the comment already claims they are hoisted) |
| P6-108 | REAL-latent | explicit `CP` guard + drop dead code | Same, plus delete `analyze_latency()` |
| P6-405 | REAL, fix absent | `# TODO` only | Scale the inject offset to the phase length; fail fast below the floor |
| P6-406 | REAL, fix absent | `# TODO` only | Validate the 4 duration knobs up front (`RATE_HZ` is already validated by the lib) |
| P6-407 | REAL | sum across lines | Sum across lines; drop both `eval`s; clear stale `DISK_*_CG` on inspect failure |
| P6-408 | REAL, fix absent | `# TODO` only | Deadline-based pacing |
| P6-409 | REAL, fix absent | `# TODO` only | Batch all 6 metrics into one call per vertex |
| P6-410 | REAL, fix absent; rationale partly wrong | `# TODO` only | Tolerance + reset-aware delta. The "config-echo double-count" claim is **false** |
| P6-411 | REAL | require non-empty `java.out` | Same |
| P6-743 | REAL, no impact | `set -o pipefail` | Same |

**Non-goals.**

* Do not re-add a preview-table purge (P6-103) or any retired-table purge.
* Do not change `pipeline-lib.sh` except where a task names it. The purge opt-in is
  a *caller* decision (L53), so the fix belongs in the caller.
* Do not remove the exact-match inject gate — P6-410 asks for sampling tolerance,
  not gate removal.
* No production/Swarm work. This is a dev-cluster measurement harness.

**Assumptions.**

* The dev cluster (10 `01_docker*` containers) stays up for the live run.
* The cgroup v2 path `/sys/fs/cgroup/system.slice/docker-<id>.scope/io.stat` holds
  for `docker run` containers as well as stack ones (the existing loop already
  relies on this).
* `throughput.tsv` has no automated consumer (verified), so its column fix is an
  evidence-quality fix, not a data-contract fix.

**Decisions — both resolved (2026-09-16), with the evidence.**

**(1) CHG record: YES — file one.** This reverses the initial lean and the wave-35
precedent, because the trigger analysis turns on what Task 2 *removes*, not on
where it lives.

* The 9th trigger is *Execution gate or approval behavior* — decisions about
  whether a system may proceed when a precondition fails. A table purge is a
  pre-measurement precondition, and `ALLOW_STALE_TABLE` is precisely an
  approval-to-continue-when-it-failed switch. Removing that switch changes gate
  behaviour: the run stops instead of proceeding on stale data.
* `CHG-154.md` **L164** already recorded this as gate behaviour in a filed
  record: "*a failed purge refuses without ALLOW_STALE_TABLE (holistic-measure.sh
  opts in explicitly...)*". Its `compatibility_class` is
  **`COMPATIBLE_WITH_LIMITATION`**, and this task removes the limitation. A filed
  record's stated limitation being removed is a reconciliation event.
* Precedent agrees once the scope is read as behaviour, not file path: `CHG-174`
  (wave 33, `bench-throughput.sh` — the same harness family) carries
  `scope: gate-behavior`; `CHG-176` uses `measurement-gate`. Six of the last
  fourteen records (`CHG-170`…`CHG-183`) sit on this axis. The counter-example is
  narrower than it first looks: `CHG-173`'s "host-side tool, not an execution
  gate" reasoning covers a **stream-routing** fix (`WARN`→stderr) with no change
  to exit behaviour, and says so — "*Neither line's exit behaviour changes*".
  This task changes exactly that.
* The opt-in is not in `docs/01_project/04-decisions.md` (the active-decision
  registry, `grep ALLOW_STALE` is empty), so the *Active decision* trigger does
  not fire on its own — the gate-behaviour trigger is what carries it.
* Filing cost is one file plus `change_control_check.py`, which is wired into
  `docs-audit` as C14. Suggested fields: `scope: gate-behavior`,
  `compatibility_class: COMPATIBLE_WITH_LIMITATION` (the operator opt-in survives;
  only the default flips), `savepoint_impact: none`.

**(2) Inject offset: original behaviour, clamped — decided, do not ask again.**
The offset stays `120000` for every phase that can host it and is clamped down
only when it cannot. This is not a preference call: adding `WARMUP_S` on top of an
already-absolute offset would have silently moved the default smoke's injection
from 120s to 165s — an unrequested change to a live-verified path, for no benefit.
A uniform fraction (`duration_s/3`) was rejected: it moves the default off the
value the L75-84 commentary was verified against and buys nothing the clamp does
not already provide.

**Authorization.** Committing is pre-authorized under the standing rule (verified,
lint-clean, gated). A live two-phase run on the dev cluster is part of the work.
No `docker push`, no `stack deploy`, no Swarm secret changes.

## Implementation Steps

### Task 1: Make the harness able to finish a phase

**Why:** `FAKETOOL_PID` at L249 is fatal on the first poll iteration, so nothing
below it has ever run. Until this lands, no other fix can be observed. The task
also removes the two unreadable host-PID rows and the per-poll inspection cost,
because they are the same loop body.

**Files:** `code/01_platform/04_scripts/holistic-measure.sh` (`run_phase`
L129-252); `code/01_platform/04_scripts/tests/test_holistic_measure_wave36.py`

**Depends on:** none

- [x] **P6-008 / P6-009 — liveness.** Replace L249-250 with container probes using
      the lib's own variables and message shape:
      ```bash
      # CHG-122: the data path is containers, not host PIDs. Library container
      # names, not the 01_docker-* stack names.
      _alive() {
        [ "$(docker inspect --format '{{.State.Running}}' "$1" 2>/dev/null || echo false)" = "true" ]
      }
      _alive "$LIB_FAKETOOL_CONTAINER" \
        || { echo "!! loadgen container $LIB_FAKETOOL_CONTAINER dead at t+${i}s" >&2; return 1; }
      _alive "$LIB_INGESTION_CONTAINER" \
        || { echo "!! ingestion container $LIB_INGESTION_CONTAINER dead at t+${i}s" >&2; return 1; }
      ```
      `_alive` is a local helper, not a lib export — do not add it to
      `pipeline-lib.sh`.
- [x] **P6-007 — delete the `jvm` row** (L199). `/proc/<pid>/io` is mode 0400 and the
      ingestion process runs as root inside a container, so the row can only ever
      be empty or raise an unbound-variable error. Replace it with a cgroup
      `io.stat` sample for `$LIB_INGESTION_CONTAINER`, label `ingestion`.
- [x] **Also delete the `bridge` row** (L200-202, `pgrep -f arrow-bridge`). Same
      reason — and a host-side `arrow-bridge` makes `pipeline_preflight` L418 fail
      the run as a CHG-122 policy violation, so the row is unreachable in a valid
      run either way.
- [x] **Extend the cgroup list** (L149) beyond the 6 stack containers to include
      `$LIB_FAKETOOL_CONTAINER` and `$LIB_INGESTION_CONTAINER`, so the removed
      `/proc` rows are replaced by samples that carry data.
- [x] **P6-107 — hoist.** Move `DISK_DEV` (L141), the container-ID resolution
      (L148-156) and the `io.stat` path build **above** the `for ((i…))` loop so
      they run once per phase instead of ~180×. The existing comment already says
      "once per phase"; make the code match it. Place the block after the warm-up
      guard (L121) — the pipeline containers do not exist before L96-97.
- [x] **P6-406 — validate the knobs.** `fail()` is defined at **L59**, after the
      `source` at L57, so the validation must sit immediately after L59/L60 —
      **not** before `source`, where `fail` does not exist yet. Add:
      ```bash
      # RATE_HZ is validated by pipeline_validate_rate (pipeline-lib.sh L167,
      # called from pipeline_preflight L380): non-integers and rates that do not
      # divide 1000 are already rejected. Only the duration knobs are unchecked.
      for _knob in SMOKE_S MAIN_S POLL_S WARMUP_S; do
        _v="${!_knob}"
        case "$_v" in ''|*[!0-9]*) fail "$_knob='$_v' must be a positive integer";; esac
        [ "$_v" -gt 0 ] || fail "$_knob must be greater than 0 (got '$_v')"
      done
      ```
- [x] **Add the cgroup-root test seam.** One line beside the other locals:
      `_CG_ROOT="${HOLISTIC_CG_ROOT:-/sys/fs/cgroup/system.slice}"`, used to build the
      `io.stat` path. Comment it as a test hook (precedent: `G7_REUSE_RAW`), because
      a test cannot write to `/sys`.
- [x] **Tests** (failing first):
      * a dead loadgen container ends the phase, names `$LIB_FAKETOOL_CONTAINER` on
        stderr, and the run does **not** die with "unbound variable" — assert that
        string is absent from stderr;
      * no `JVM_PID`, `FAKETOOL_PID` or `BPID` appears in any **non-comment** line of
        `run_phase` (use a `code_lines()` helper that skips `#` lines — the strings
        do appear in comments);
      * `POLL_S=0` and `SMOKE_S=abc` each exit non-zero, name the knob, and never
        reach Phase A — assert the evidence directory was not created;
      * the hoist is observable: with a `docker` shim counting invocations, a 3-poll
        phase calls `docker inspect` the same number of times as a 1-poll phase.
- [x] **Verify:** `shellcheck -S warning` clean; `bash -n` clean; new tests pass;
      and the liveness test **fails against a copy of the pre-fix file** (capture
      that output as evidence).

### Task 2: Fail closed on a failed purge

**Why:** a stale raw table is replayed from EARLIEST by the fresh job and the
numbers become garbage. The current code cannot fail closed: L53 exports
`ALLOW_STALE_TABLE=true`, so `pipeline_purge_table` returns 0 even when the purge
failed. L95's `|| true` and the proposed `|| return 1` therefore behave
identically. Both edits are required or nothing changes.

**Files:** `code/01_platform/04_scripts/holistic-measure.sh` (L50-53, L95);
`code/01_platform/04_scripts/tests/test_holistic_measure_wave36.py`

**Depends on:** Task 1

- [x] Replace `export ALLOW_STALE_TABLE=true` (L53) with a `false` default, or
      delete the line and let the library's own default apply. Keep the comment
      (L50-52) but correct it — it currently asserts the opt-in is deliberate,
      which this task reverses.
- [x] File **CHG-184** (`scope: gate-behavior`) in the same commit train, because
      this task removes a limitation `CHG-154.md` L164 filed as gate behaviour.
      Fields and validation in *Required External or Manual Verification*.
- [x] Change L95 to `pipeline_purge_raw_table || return 1`.
- [x] **Do not touch P6-103.** `pipeline_purge_preview_table` was removed in
      `72bd64ae` together with this file's call site. `pipeline_purge_table`'s
      comment at lib L735-742 still describes the retired preview purge and is
      **stale**; fixing that comment is optional and belongs in a docs commit.
- [x] **Tests:** with the stub lib reporting a failed purge, `run_phase` returns
      non-zero and never reaches `pipeline_start_ingestion`; with
      `ALLOW_STALE_TABLE=true` set by the **operator**, the phase continues (the
      opt-in stays available, it is just not the default). Cite
      `test_pipeline_lib_hardening.py` L252-267 as precedent for both halves.
- [x] **Verify:** both tests pass; `make docs-audit` stays green.

### Task 3: Make the evidence columns correct

**Why:** two evidence files are wrong in ways the analyzer cannot see.
`throughput.tsv` mis-splits every row; `proc-io.tsv` embeds newlines that split one
sample across rows.

**Files:** `code/01_platform/04_scripts/holistic-measure.sh` (L138-139, L194-195);
`code/01_platform/04_scripts/tests/test_holistic_measure_wave36.py`

**Depends on:** Task 1

- [x] **P6-106 — the separator.** The emitter is `pipeline-lib.sh` L1253:
      `print(vertex_name, "|", read, "|", write)`, so a real row is
      `src_raw | 12345 | 67890`. `-F'| '` is two characters and awk reads a
      multi-character FS as an ERE: `|` is alternation, so the separator is
      *empty-or-space* and a leading space matches. Result:
      `$1=src_raw $2=| $3=12345` — the `read` column receives `|` and the `write`
      column receives the read count. The finding's proposed `-F' \| '` is **also**
      two characters once gawk resolves `\|` to a literal `|` (it warns:
      `escape sequence '\|' treated as plain '|'`), so its FS becomes
      *space-or-space* — byte-identical wrong output. Use:
      ```bash
      # pipeline-lib.sh L1253 emits "<vertex> | <read> | <write>". A multi-char
      # FS is an ERE, so the pipe needs a bracket class.
      echo "$m" | grep -v '^STATE' | awk -F' *[|] *' -v e="$now" -v s="$state" \
        '{printf "%s\t%s\t%s\t%s\t%s\n", e, $1, $2, $3, s}' >> "$tsv"
      ```
      Verified against the real row: gives `$1=src_raw $2=12345 $3=67890`.
- [x] **P6-407 — multi-device `io.stat`.** A cgroup with more than one device emits
      one line per device, so `print $i` returns several lines and `_r`/`_w` carry
      embedded newlines. Sum within awk:
      `awk '{for(i=2;i<=NF;i++) if($i ~ /^rbytes=/) {sub("rbytes=","",$i); s+=$i}} END{print s+0}' "$_cg"`
      (same shape for `wbytes`), keeping the `${_r:-0}` fallbacks.
- [x] **P6-407 — stale paths.** When `docker inspect` fails (container recreated ⇒
      new id), unset the corresponding `DISK_<name>_CG` instead of leaving the old
      path, which silently samples the previous container.
- [x] **P6-407 — drop both `eval`s.** L155 (`eval "DISK_${_c}_CG=…"`) and L192
      (`eval echo \$DISK_${_n}_CG`) build variable names by string. Replace with one
      associative array (`declare -A DISK_CG`) — already used in this repo
      (`run-full-suite.sh` L59). This also removes the `set -u` stderr noise the
      audit measured when a monitored container is absent.
- [x] **Keep the analyzer's label contract.** `holistic-analyze.py` parses
      `main/proc-io.tsv` with `r"(\d+) (\w+) read_bytes: (\d+) write_bytes: (\d+)"`
      (L1131, L1284) and special-cases the label `tablet` for the G6c read-storm
      guard (L1288). Keep `tablet` spelled exactly as today and use only `\w`-safe
      labels for new rows (`faketool`, `ingestion`) — a hyphen or space would
      silently drop every row for that container.
- [x] **Tests:**
      * feed the real emitter line `src_raw | 12345 | 67890` through the script's
        own awk invocation and assert the written row carries `12345` in the `read`
        column and `67890` in `write` — key by column **name** from the header, never
        by index;
      * with `HOLISTIC_CG_ROOT` pointing at a fixture `io.stat` holding **two**
        device lines, assert `proc-io.tsv` gains exactly one row for that container
        whose read/write equal the summed values;
      * with a fixture where one device has no `wbytes`, assert the row still
        appears with a correct read sum and `write_bytes: 0`.
- [x] **Verify:** tests pass; byte-compare the new awk against the current one on the
      real emitter format to show the split changed.

### Task 4: Bound the poll body and the metric fan-out

**Why:** the poll body currently issues up to 20 vertices × 6 metrics = 120
sequential REST calls at 4s timeout each, plus ~20 more scrapes and inspections —
worst case 480s inside a loop whose `sleep "$POLL_S"` assumes 5s. The measurement
distorts the system it measures.

**Files:** `code/01_platform/04_scripts/holistic-measure.sh`
(`collect_vertex_metrics` L270-295, pacing L251);
`code/01_platform/04_scripts/tests/test_holistic_measure_wave36.py`

**Depends on:** Task 1

- [x] **P6-409 — batch the fetch.** One call per vertex instead of six, using the
      comma-joined form proven in `loadtest-collect.sh` L136:
      `…/vertices/{vid}/metrics?get=busyTimeMsPerSecond,backPressuredTimeMsPerSecond,idleTimeMsPerSecond,latencyP50,latencyP95,latencyP99`.
      Emit one TSV row per returned metric, preserving the existing
      `epoch / vertex / metric / value` shape, and keep the per-item `try/except`
      so a missing metric stays a missing row, never a fatal error.
- [x] **P6-408 — deadline-based pacing.** Replace `sleep "$POLL_S"` (L251) with:
      ```bash
      next=$((next + POLL_S)); now=$(date +%s)
      [ "$now" -lt "$next" ] && sleep $((next - now))
      ```
      Initialise `next` from the phase start so drift cannot accumulate, and never
      sleep a negative value. Log once per phase when the body overruns its budget,
      so an under-sampled run is visible rather than silent.
- [x] **Tests:** with a `curl` shim counting requests, a phase with 3 vertices makes
      exactly 3 vertex-metric calls, not 18; with a `sleep` shim recording its
      argument, a body that exceeds `POLL_S` results in no sleep call for that
      iteration.
- [x] **Verify:** tests pass; the suite stays under ~60s (use shims and 1-2 poll
      iterations, never real multi-minute waits).

### Task 5: Make the gates prove what they claim

**Why:** the smoke phase is the gate that stops the main phase from running on a
broken pipeline, and two of its three checks can pass without evidence.

**Files:** `code/01_platform/04_scripts/holistic-measure.sh` (L85-90, L318-351,
L366-377); `code/01_platform/04_scripts/tests/test_holistic_measure_wave36.py`

**Depends on:** Task 1

- [x] **P6-405 — injection must fire inside the sampled window.** The offset is
      measured from faketool start (`main.go` L328 `time.Now().Add(*injectAfter)`),
      but counters are only sampled from the end of warm-up onward — the poll loop
      begins at L129, after `sleep "$WARMUP_S"` at L103. The gate compares
      `last_sample - first_sample`, so an injection **before** the first sample is
      already inside it (delta 0) and an injection **after** the last sample never
      lands (delta 0). Both ends fail the gate.
      **The offset is absolute from faketool start — clamp it, do not add to it.**
      Today's `INJECT_AFTER_MS=120000` is already measured from process start, so
      it must merely be clamped into the sampled window, never shifted by
      `WARMUP_S` on top:
      ```
      viable offset = [ WARMUP_S + MARGIN , WARMUP_S + duration_s - MARGIN ]
      chosen offset = MIN(120s, WARMUP_S + duration_s - MARGIN)
      MARGIN        = 20s  (15s candle window + one POLL_S sample)
      ```
      ```bash
      # Injection must land inside the SAMPLED window (warm-up end .. phase end),
      # not merely inside the phase: the gate compares counter deltas, and the
      # first sample is taken after warm-up. INJECT_AFTER_MS is already absolute
      # from faketool start, so CLAMP it — do not add WARMUP_S.
      #   duration 180 -> 120000 (unchanged)   duration 60 -> 85000
      _margin=20
      [ "$duration_s" -gt $(( _margin * 2 )) ] \
        || fail "$name duration ${duration_s}s is too short to host an injection (need > $(( _margin * 2 ))s)"
      _limit=$(( (WARMUP_S + duration_s - _margin) * 1000 ))
      export INJECT_AFTER_MS=$(( _limit < 120000 ? _limit : 120000 ))
      ```
      Verified arithmetic (durations 180 / 60 / 300 / 900 → `120000 / 85000 /
      120000 / 120000`): the default 180s smoke keeps **exactly** today's 120000,
      so the live-verified commentary at L75-84 stays true and the change carries
      zero risk to a normal run; only the documented quick variant moves, from an
      offset it could never satisfy to one inside its 65..85s window.
- [x] **P6-411 — fail closed on missing evidence.** `fingerprint_gate` L368 runs
      `grep -ac … "$dir/j1/java.out" 2>/dev/null || true`; a missing file leaves `n`
      empty and `${n:-0} -eq 0` **passes**. Add before the grep:
      ```bash
      [ -s "$dir/j1/java.out" ] || {
        echo "!! G8 FINGERPRINT GATE FAIL: missing or empty $dir/j1/java.out — ingestion produced no log" >&2
        return 1
      }
      ```
      This cannot false-fail a successful phase: the library creates `$OUT/j1`
      (L364, L520), mirrors the container log into `java.out` (L692), and requires
      `HFT subscribed` to appear there (L710-713).
- [x] **P6-410 — tolerant, reset-aware counter delta.** Exact equality at L341/L345
      treats a missed sample, a series reset (a job restart resets counters to 0),
      subtask churn, or a `%.0f` rounding difference as a pipeline bug. Reuse
      `counter_deltas()` (`holistic-analyze.py` L164-192) rather than writing a
      second implementation. Accept `|delta - want| <= max(1, 5% * want)` and print
      both numbers plus the tolerance on every run so a tuned tolerance stays
      auditable.
      **Do not** act on the finding's "config echoes double-count" sentence: only
      `main.go` L381 prints `dups=`, and only on an INJECT line (verified), so the
      `want_*` sums are already correct. Record that correction in the finding's
      evidence note.
- [x] **Tests:** missing/empty `j1/java.out` ⇒ gate non-zero (failing-first for
      P6-411 — it passes today); delta off by 1 ⇒ gate passes; delta off by 50% ⇒
      gate fails; a series dropping to 0 mid-run (restart) ⇒ gate passes;
      `SMOKE_S=60` ⇒ `INJECT_AFTER_MS=85000`; `SMOKE_S=180` ⇒ still `120000`.
- [x] **Verify:** all new tests pass; the P6-411 test fails against the current file
      before the edit.

### Task 6: Remove dead weight and restore shell discipline

**Files:** `code/01_platform/04_scripts/holistic-measure.sh` (L62-68, L403-407);
`code/01_platform/04_scripts/tests/test_holistic_measure_wave36.py`

**Depends on:** Task 1

- [x] **P6-743 — restore, do not clear, `pipefail`.** L403 enables it for the
      analyzer pipeline and L407 does `set +o pipefail`, permanently clearing the
      script-global mode set at L38. Change L407 to `set -o pipefail`.
- [x] **P6-108 — capture `CP` explicitly.** `CP` is assigned only as a side effect of
      `pipeline_preflight` (lib L425, no `local`), so L404 depends on a library
      internal. Add the guard before the invocation: `CP="${CP:-}"` then
      `[ -n "$CP" ] || { echo "FATAL: empty CP after main phase" >&2; exit 1; }`.
      Do **not** add `local CP` in the lib — that would break every caller reading
      it this way.
- [x] **P6-108 — delete `analyze_latency()`** (L65-68). It has exactly one
      occurrence in the repo (its own definition), takes a 2-argument shape that no
      longer matches the analyzer's 4-argument call, and wraps the call in
      `|| true`, which would hide a failing guard.
- [ ] **P6-104 / P6-105 — no code change.** Record in the findings' evidence notes:
      all three mid-phase `return 1` sites are reached only from `run_phase`, whose
      callers (L304-308, L384-386) `exit 1` immediately, which fires the `EXIT` trap
      installed at L60 and runs `pipeline_cleanup`. An explicit call is redundant,
      not harmful; omitting it keeps the diff honest.
- [x] **Do not break `test-pipeline-lib.sh` G9:** it greps `holistic-measure.sh` for
      the literal `fingerprint_gate` (L174-177), so that function keeps its name.
- [x] **Tests:** a non-zero analyzer exit still fails the run after the `pipefail`
      change (the guard at L408-411 must still fire); with `CP` unset the script
      exits non-zero with the message instead of an "unbound variable" abort;
      `analyze_latency` is gone (source-text check skipping comment lines).
- [x] **Verify:** `bash -n`; `shellcheck -S warning`; `bash
      code/01_platform/04_scripts/test-pipeline-lib.sh` still green (it covers G9).

### Task 7: Verify acceptance criteria

- [x] `shellcheck -S warning code/01_platform/04_scripts/holistic-measure.sh` clean;
      `bash -n` clean; `python3 -m pyflakes` clean on the new test file.
- [x] New suite passes:
      `python3 -m pytest code/01_platform/04_scripts/tests/test_holistic_measure_wave36.py -q`;
      wave-35 suites still pass.
- [x] **Failing-first evidence for the three headline bugs**, captured against a
      copy of the pre-fix file (not asserted from memory): the liveness test, the
      `throughput.tsv` column test, and the `fingerprint_gate` test each fail before
      the fix and pass after. Record commands and output in the commit body or the
      findings' evidence notes.
- [x] **Mutation check.** Apply these to a copy and confirm the suite catches each:
      (a) restore `kill -0 "$FAKETOOL_PID"`; (b) restore `-F'| '`; (c) remove the
      `-s` guard in `fingerprint_gate`; (d) restore `export ALLOW_STALE_TABLE=true`;
      (e) re-add `set +o pipefail`. A mutation that is **not** caught is a documented
      limitation, not a silence — the wave-35 precedent records honest limits in the
      test docstring.
- [x] Confirm the test sandbox **mirrors the repo layout**. Tests resolve
      `REPO = Path(__file__).resolve().parents[4]`; the copy under test must sit at
      `<sandbox>/code/01_platform/04_scripts/` or the tests exercise the real repo
      and every mutation becomes a no-op.
- [x] Full gate: `make docs-audit` and the full pytest suite. Report the totals.
- [x] **Live run** — see Required External or Manual Verification.
- [ ] **All 18 findings ticked** in
      `~/.opencodereview/sessions/home-saurabh-Jupyter_notebook-Flink_Fluss_Infrastructure-streaming_project_New/phases/p6-ops-scripts-tests-chaos-audit.md`,
      each with a `> **Fixed:**` / `> **Not a bug:**` / `> **Already fixed:**`
      evidence note naming the commit and mechanism. Then run
      `python3 phases/p6_map_refresh.py --write` and confirm the wave-36 progress
      count moved by exactly 18. **This file is outside the repo and is not
      version-controlled** — nothing in git will show this step.

## Technical Details

**Why L199 does not abort but L249 does** — the most counter-intuitive fact in the
wave, and the doc gets it backwards. In `echo "… $(grep … /proc/$JVM_PID/io …)"`
the expansion happens inside the command-substitution subshell. `set -u` kills
*that subshell*, not the script: the substitution yields an empty string, `echo`
succeeds, and execution continues writing a truncated `jvm` row. `kill -0
"$FAKETOOL_PID"` at L249 expands in the main shell, so it terminates the process
with exit 1 and **no stderr** (the `2>/dev/null` on the enclosing group swallows
the message). Verified both ways. Consequence for ordering: the first fatal line is
L249, **not** L199 — but L199 must still be deleted, because once `$JVM_PID` is
removed from the file it becomes a genuine unbound-variable error.

**Why `|| true` → `|| return 1` alone fixes nothing (P6-102).** `ALLOW_STALE_TABLE=true`
is exported at L53. `pipeline_purge_table` (lib L798-817) checks `PURGE_STRICT`
first, then `ALLOW_STALE_TABLE`; with the latter `true` it falls off the end of the
function and returns 0 even when the purge failed. The caller therefore sees
success and `|| true` never matters.

**The real emitter format.** Do not guess the separator for P6-106 — read
`pipeline-lib.sh` L1253. `print(a, "|", b, "|", c)` joins arguments with single
spaces, so the row is `src_raw | 12345 | 67890`. `-F' *[|] *'` was verified against
that exact string; `-F'[|]'` also splits correctly but leaves the padding spaces in
the values.

**Why the P6-405 fix is anchored to the sampled window.** The gate's arithmetic is
`last_sample - first_sample` over the Prometheus counters. The counters are
cumulative since job start, and the first sample is taken after warm-up. An
injection that fires before the first sample is already in it; one that fires after
the last sample never lands. So the offset must satisfy
`WARMUP_S + ε < offset < WARMUP_S + duration_s`. Anchoring to `duration_s` alone
(the obvious reading of the finding) puts a 60s phase's injection at 20s — before
the first sample — and the gate still fails. The chosen rule preserves 120000 for
every default-length phase.

## Required External or Manual Verification

**Live two-phase run — required for acceptance criteria 2 and 8.**

* **Environment:** the dev cluster, 10 `01_docker*` containers up. Confirm with
  `docker ps --format '{{.Names}}'` first.
* **Procedure:** from `streaming_project_New/`:
  `SMOKE_S=60 MAIN_S=300 POLL_S=5 bash code/01_platform/04_scripts/holistic-measure.sh`.
  The short variant is reachable only after Task 5 — previously impossible to pass.
* **Evidence to capture:** the exit code; the new
  `logs/tracker-14/holistic-measure-<ts>/` listing; `head -3 main/throughput.tsv`
  (columns correct); the `proc-io.tsv` line count plus a check that no sample spans
  two lines; the `G8 fingerprint gate PASS` lines for both phases; the tail of
  `latency-analysis.txt`; and the `INJECT` line's offset from `faketool.log`.
* **Authorization:** pre-authorized under the standing rule. This starts and stops
  containers via the library and restarts the TM; it does **not** touch Swarm, the
  production stack, or any registry.
* **Why it cannot be automated:** it needs the real Fluss cluster, the real Flink
  REST endpoint and the real cgroup paths. The suite stubs all three.
* **This will be the first successful run in the file's history.** If it fails, the
  honest result is "the harness now fails later than before, and here is where" —
  record that rather than declaring the wave done.

**File the change record (decided — see Review Handoff, decision 1).** Task 2's
removal of the `ALLOW_STALE_TABLE` opt-in is a *gate-behavior* change and
`CHG-154.md` L164 already recorded that opt-in as a limitation of a filed record,
so a reconciliation record is required. Highest existing id is **CHG-183**, so the
new record is **CHG-184**.

* Write `docs/05_deployment/change-records/CHG-184.md` from `_template.md` with
  `scope: gate-behavior`, `compatibility_class: COMPATIBLE_WITH_LIMITATION`
  (the operator opt-in survives; only the default flips), and
  `savepoint_impact: none — host-side measurement tooling; no Flink state,
  checkpoint, DDL, connector or container definition is touched.`
* `affected_artifacts` and `plan_tasks` must resolve — cite
  `code/01_platform/04_scripts/holistic-measure.sh`, the new test file, and this
  plan. Both fields reject phantom references.
* Validate: `python3 code/01_platform/04_scripts/change_control_check.py` → exit 0,
  and the same check via `make docs-audit` (C14).
* `docs/01_project/04-decisions.md` needs no entry: the opt-in was never an
  active decision there (`grep ALLOW_STALE` is empty).

## Post-Completion

### Run-log findings (live runs, 2026-09-17)

The acceptance runs did fail later each time, as this plan predicted, and each
layer is filed rather than fixed quietly. Runs 1-8 (see
`logs/tracker-14/holistic-measure-2026*`), newest layer last:

* **CHG-185** `DEPLOYMENT_ENV` — the harness used the compose default and the
  job refused a non-dev deployment.
* **CHG-186** manifest path, **CHG-187** bare-null `STATE_RECOVERY_PATH`
  (compose cannot unset an env var), **CHG-188** partition-aware purge.
* **CHG-189** `user: "root"` on the jobmanager — without it every checkpoint
  failed on permissions. Run 8 produced the **first non-empty
  `checkpoints.jsonl` in the repository's history** (2× COMPLETED).
* **CHG-190** the purge now waits for the write path before returning
  (P6-485), and the G8 cross-check compares two digests of the same scheme
  (P6-486).
* **CHG-191** — the layer this plan could not see, because it was written from
  a code audit rather than a run: **the harness gated on a metric its own job
  graph no longer created.** `compute.candles.late.dropped` belongs to
  `MultiTimeframeAggregateFunction`, wired only when `MULTITF_ENABLED=true`,
  and the harness never set it — so the smoke inject gate's late-drop half
  compared 0 against 20 and could never pass, while the dedup half passed
  exactly (200/200). The counter used to be unconditional; `0f3e5952` ("retire
  15s candle path") moved it behind the flag and touched **no file under
  `04_scripts/`**. **This plan's blind spot:** Review Handoff L148 says "Do not
  remove the exact-match inject gate" and P6-405/P6-410 tune how the gate
  *detects* a drop, but nothing in the plan asks whether the counter the gate
  reads still exists in the submitted graph. The fix opts the candle path in
  (and refuses an explicit opt-out rather than failing later with a misleading
  count), which also makes the header's own methodology — `window_start` →
  preview cadence, `window_end` → settlement — measure the pipeline it
  describes instead of a three-operator ingest stub.

* **CHG-192** — run 9: the inject gate compared a **superset** as if it were an
  exact set. `compute.candles.late.dropped` counts every late/out-of-order drop
  the aggregator makes, not only the injected frames: subtask 2 carried exactly
  the injected 20, subtask 1 carried 226 of the live feed's own re-feeds, and the
  phase-wide sum (134) was reported as 114 lost ticks against an injection of 20
  that had been counted perfectly. The late half is now one-sided (a shortfall
  still fires; a surplus is printed as `natural-late(not injected)=N`), while the
  dedup half stays two-sided because dedup drops *are* exclusively the injection.
  Zero-loss authority remains G7c — see CHG-194, which is what made that
  statement true again.

* **CHG-193** — run 10: the warm-up guard failed a phase whose pipeline was at
  full rate (`multi-tf-aggregator` 458 132 records, candle sinks 534 528, against
  an expected ~460 800 for 45s). `pipeline_metric_input_progress` documents a
  `0|0` snapshot race and had a downstream fallback for it, but the fallback
  matched a hand-written operator list two-thirds of which `0f3e5952` had
  deleted. The fallback now names no operator: raw counters keep priority, else
  the largest counter on **any** other vertex proves the feed; all-zero still
  fails closed.

* **CHG-194** — run 11 was the **first run in the wave to pass every pipeline
  gate and complete both phases**, and it still exited 1. The smoke inject gate
  was exact (200/200 duplicates, 20/20 late), G8 passed, the main phase ran its
  full 300s, and the analyzer's own data-quality audit was exact (G7a 400/400
  duplicate extras, G7b 40/40 late rows, F6 zero rejections, F8 ordering
  consistent, F4 zero orphans). The failure was in the measurement layer: G7c's
  zero-loss parity proof had `final_rows = []` hardcoded, so the candle side of
  the comparison was **structurally empty** and all 22 528 closed windows (1024
  tokens × 22 windows) were reported as "NO final candle". Separately, three
  legs that this topology genuinely cannot measure (retired preview table, no
  write timestamp on the candle pair, no writer for `Signal_Candidates` while the
  strategy host is off) were appended to `failures`, whose non-emptiness *is* the
  exit code — so the harness could not pass **any** run however clean. Parity now
  reads the live `candle_closed` (DDL 33) through the existing
  `fluss-probes/FlussPrefixReader`, selecting the 15s family by window width and
  keying by column name; the unmeasurable legs moved to a separate, printed,
  rc-neutral channel with an explicit allowlist so a new unmeasurable leg still
  fails. **This plan's blind spot, second instance:** L148 forbids removing the
  inject gate and P6-405/410 tune how it detects a drop, but nothing asks
  whether the *analyzer's* proof still reads a table that exists — the same
  `0f3e5952` cutover that hid CHG-191 also emptied G7c.

* The stale comment at `pipeline-lib.sh` L735-742 still describes the preview purge
  that `72bd64ae` deleted. Worth a one-line fix on the next lib-touching change; it
  is not part of this wave's acceptance.
* `holistic-measure.sh` is undocumented in `docs/` (only `CHG-154.md` mentions it).
  Wave 35 added `COMMANDS.md` rows for two comparable operator tools; the same row
  here — usage, knob meanings, and the "read the exit code, not just the directory"
  caveat — is the natural follow-up.
* Keep running the Fix Audit on waves 37+. In waves 35 and 36 it caught 3 and 7
  broken proposed fixes respectively, plus two findings the audit itself got wrong
  in this wave.
