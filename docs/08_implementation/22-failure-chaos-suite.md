# 22 — Failure chaos suite (T13, G5 Ops)

**Status:** Partially implemented — 5 runner scripts created; offline legs green. Live stack results 2026-08-24 (CHG-096, evidence `logs/phase-d-20260824T151952Z/`): 01 slot PASS, 02 TM kill **PASS** (both legs; leg B after a runner fix — this docker daemon does not auto-restart a `docker kill -s KILL`ed container despite `restart: unless-stopped`, the runner now falls back to explicit `docker start`; job survived via checkpoint failover, checkpoint strictly newer), 03 tablet kill **FAIL (reproducible 3/3 → FIXED 2026-08-25, CHG-104)** — coordinator re-registers the tablet in ~1s but data-plane reads did not return within the drill's 180s bound on this RF1 single-tablet dev box; root cause was a **test-harness bug, not data loss**: the recovery loop conflated "is the table serving reads" (needs a small probe) with "full-log rescan of 11.5M rows" (takes ~4 min on this box), and the 60s per-attempt bound aborted in-progress scans. Fixed by separating a fast `probeReadable()` (detected recovery in **5s**) from the full invariant scan (bounded by `CHAOS_POST_KILL_SCAN_BOUND_SEC`, default 600s). Re-run 2026-08-25: **PASS** — `total-log-rows-after=11,535,601` = exact pre-kill count (LOG never shrank); RF1 tail-loss of the just-acked rows observed as expected (motivates D23 LOG x3 for prod). VM-loss `multi-node` + `3k×20` still `TO_BE_VERIFIED`.


The four planned failure tests (plan `streaming-3000-flink-fluss-hardening.md`
§5 T13 "Add slot kill, TM kill, tablet kill, VM loss as L11 gates") live as
env-gated integration tests + gate scripts, driven by ONE runner. They are
deliberately layered the same way the repo's L0–L11 suite is: the
deterministic core runs anywhere (Go unit tests, embedded MiniCluster), the
cluster-shaped assertions run against the live stack / Swarm and SKIP
cleanly when the prerequisite is absent — because the deployment tests run
later, serially, by the main agent.

> **2026-09-15 supplement — wave 30: the drill and its runner stopped
> overstating coverage (CHG-170).** The tablet drill reported PASS on a
> *skipped* integration test (its `-DfailIfNoTests=false` build is green
> whether or not the gated IT ran), picked its victim with `head -n 1` over an
> unstable `docker ps` order, accepted any running container as the victim
> through a substring grep before handing it to `docker kill`, and let
> `TABLET_KILL_ROWS=0` pass vacuously. The runner, in turn, called four SKIPs
> `RESULT=PASS EXIT=0` and counted chaos-02's named leg-B skip as a plain
> PASS. Both now derive their verdict from evidence: chaos-03 requires the
> IT's own sentinel and verifies the victim with `docker inspect` (`2` for a
> bad pin, `3` for an absent stack), and chaos-run.sh counts
> PASS/SKIP/FAIL/PARTIAL, reports a drill that names a skipped part of itself
> as PARTIAL, and exits `3` with `RESULT=SKIP` when nothing passed. Coverage
> claims in the tables below that predate this date describe the superseded
> counting; the line counts and the `RESULTS`/`SKIP 3` tokens they name
> (`chaos-run.sh 69L`, `chaos-03-tablet-kill.sh 43L`) are the 2026-08-24
> revision of those two files.

> **2026-09-02 supplement — C2 TM-kill-at-full-load drill (tracker-14 Block 1):**
> beyond this suite's chaos-02 (kill on an idle-ish job), the C2 drill
> (`code/01_platform/04_scripts/tm-kill-full-load.sh`, CHG-120/CHG-121) kills
> the TaskManager under FULL load (10,240 ticks/s) with recovery, checkpoint
> continuity, single-restart, drain, G7c zero-loss parity, and F4
> settlement gates. Main-mode PASS 2026-09-02
> (`tm-kill-full-load-20260902-121511`: G7c 33,792/0, F4 0 orphans,
> recovery 52s). See CHG-121 for the five-fix hardening history.

## Files

| Path | Role |
|---|---|
| `code/01_platform/04_scripts/chaos/chaos-run.sh` | Runner: static self-check, then tests 1→4 in order, PASS/FAIL/SKIP + exit codes |
| `code/01_platform/04_scripts/chaos/chaos-01-slot-kill.sh` | Test 1 — slot kill (Go bridge) |
| `code/01_platform/04_scripts/chaos/chaos-02-tm-kill.sh` | Test 2 — TM kill (Flink checkpoint) |
| `code/01_platform/04_scripts/chaos/chaos-03-tablet-kill.sh` | Test 3 — tablet kill (Fluss LOG) |
| `code/01_platform/04_scripts/chaos/chaos-04-vm-loss.sh` | Test 4 — VM loss (Swarm overlay) |
| `code/02_services/02_compute/src/test/.../SignalJobTaskManagerKillIntegrationTest.java` | SIG-TMKILL-001 MiniCluster IT (test 2, offline leg) |
| `code/02_services/02_compute/src/test/.../TabletKillChaosIntegrationTest.java` | DUR-TABLETKILL-001 live IT (test 3) |
| `Makefile` target `chaos-suite` | `make chaos-suite` == runner |

No production code (Java/Go) and no compose/stack/otel/dashboard files were
touched — tests reference them read-only. `docs/08_implementation/08-local-compose.md`
L8 `FAIL-*`/`DUR-*` lanes stay as-is; this suite is the T13 "L11 gate" row on
top of them.

## Invocation

```bash
# everything, in order (01→04)
make chaos-suite                        # or: code/01_platform/04_scripts/chaos/chaos-run.sh

# individual tests
code/01_platform/04_scripts/chaos/chaos-01-slot-kill.sh     # offline, always runs
code/01_platform/04_scripts/chaos/chaos-02-tm-kill.sh       # offline IT + optional live leg
code/01_platform/04_scripts/chaos/chaos-03-tablet-kill.sh   # live stack required (SKIP 3)
code/01_platform/04_scripts/chaos/chaos-04-vm-loss.sh       # multi-node swarm required (SKIP 3)
```

Exit codes: **0 PASS, 1 FAIL, 3 SKIP** (prerequisite absent), **2 FAIL —
usage/config error** (a bad knob, or a pinned container that is not a running
`fluss-tablet`). Children are unchanged in kind; chaos-03 gained the explicit
2. The runner exits 0 when at least one drill passed and none failed, 1 with a
`CHAOS-SUITE: RESULT=FAIL EXIT=1` sentinel when any test failed (or an evidence
log could not be written), and **3 with `RESULT=SKIP` when every drill
skipped** — an all-SKIP run proves nothing about resilience and no longer
reads as PASS. A drill that exits 0 while naming a skipped part of itself is
reported `PARTIAL` (chaos-02 leg B is the named case); a drill exiting 2 is a
FAIL, because a misconfigured drill is not an absent prerequisite. Evidence
lands under `logs/chaos/chaos-<ts>-<pid>/` (per-test logs + `SUMMARY.txt`); the
PID suffix keeps simultaneous runs from sharing one directory.

## Expected output shape

```text
=== [1/4] 01 slot kill — Go bridge, 1-of-3 slots ===
SLOT-KILL-CHAOS-01: PASS — 1-of-3 slot kill: terminal isolated, peers healthy, reconnect clean
RESULT [1]: PASS
=== [2/4] 02 TM kill ... ===
TM-KILL-CHAOS-02: [leg A] PASS — restore from checkpoint, no duplicate fingerprint
TM-KILL-CHAOS-02: [leg B] SKIP — no flink-taskmanager container (stack not up)
RESULT [2]: PARTIAL (exit 0)
=== [3/4] 03 tablet kill ===
TABLET-KILL-CHAOS-03: SKIP — no fluss-tablet container (start the stack; ...)
RESULT [3]: SKIP (exit 3)
=== [4/4] 04 VM loss ===
VM-LOSS-CHAOS-04: SKIP — this docker daemon is not in an active swarm (...)
RESULT [4]: SKIP (exit 3)
CHAOS-SUITE: passed=1 skipped=2 failed=0 partial=1 of 4
CHAOS-SUITE: RESULT=PASS EXIT=0
```

On a machine with the full stack + M3 swarm, tests 03/04 run their live
assertions instead of skipping. On a machine with neither stack nor Swarm the
same four lines end with `CHAOS-SUITE: RESULT=SKIP EXIT=3` instead: only the
offline drill ran, and the suite says so.

## Test 1 — slot kill (Go bridge)

Script: `chaos-01-slot-kill.sh`. Offline, deterministic, no cluster/broker.
Runs the named Go tests that implement the chaos:

| Go test | Assertion the gate relies on |
|---|---|
| `TestSubscriptionPlanShards3000Tokens` | 3000 tokens → deterministic 1024+1024+952 (T1 baseline) |
| `TestSupervisorAuthTerminalIsolatedPerSlot` | slot-0 driven TERMINAL → exactly 1 terminal outcome, shared context NOT cancelled, peer sockets open, peer ticks flow (no cross-slot kill) |
| `TestINGRES001HealthySlotNotInterruptedByPeerReconnect` | slot-0 drop-cycles via the real SDK while slot-1 stays ACTIVE with zero disconnect events (no peer drop regression) |
| `TestReconnectLoopRecoversAfterFailures`, `TestReconnectLoopEpochAndBackoffAfterForcedDisconnect` | killed slot reconnects with a fresh epoch |
| `TestINGRES001OneHundredForcedDisconnectReconnectCycles` | 100 forced disconnect/reconnect cycles: every cycle recovers to ACTIVE, no goroutine/FD/socket leak (drop-missed resource regression) |

Invariant (plan §6 row 1): kill 1 of 3 slots → reconnect, no drop-missed
regression, peers stay healthy.

## Test 2 — TM kill (Flink)

Script: `chaos-02-tm-kill.sh`. Two legs:

- **Leg A (always, offline):** `SignalJobTaskManagerKillIntegrationTest`
  (SIG-TMKILL-001) — MiniCluster with 2 TMs, the real
  `FingerprintDedupFunction` (the gate in front of candle emission and
  signal detection), one TM hard-terminated (`terminateTaskManager`, the
  same failover path as `docker kill` on the TM). Asserts: job leaves
  RUNNING and comes back RUNNING restored from the last checkpoint; a fresh
  checkpoint completes after restore; the legacy source re-emits its whole
  feed after restore and the restored first-write-wins markers suppress
  every replayed row — **no duplicate fingerprint** ever leaves the dedup
  boundary (the plan's "no dup candle / no dup signal" invariant; the
  KV-side `CandleKvFirstWriteWinsFunction` guard adds a second layer
  verified live in leg B).
- **Leg B (optional, live stack):** SIGKILL the taskmanager container,
  assert container restart (compose/swarm restart policy), job RUNNING
  again, a completed checkpoint strictly newer than the pre-kill one
  (restore from last checkpoint, then live), and the job's own
  `compute.candles.duplicate_window` counter does not advance across the
  failover replay.

Env: `FLINK_REST_URL` (default `http://localhost:8081`),
`TM_METRICS_URL` (default `http://localhost:9250/metrics`).

## Test 3 — tablet kill (Fluss)

Script: `chaos-03-tablet-kill.sh` → `TabletKillChaosIntegrationTest`
(DUR-TABLETKILL-001), env-gated `COMPUTE_INT_TEST_TABLET_KILL=true`, live
stack required (SKIP 3 otherwise). Flow: write N acked rows to `raw_table_1`
with the same fluss-client append path the bridge uses (append futures
complete BEFORE the kill); `docker kill -s KILL` the tablet container;
wait for the restart policy + table readability; assert every acked
fingerprint is still readable and the immutable LOG count never shrank.
Reports the coordinator-stamped `table.replication.factor`; the prod x3
contract is asserted when `CHAOS_REPLICATION_REQUIRED=true`
(`CHAOS_REPLICATION_MIN`, default 3) — set on prod-like Swarm, keep false on
the single-tablet dev compose. A truncated tail (repair-tablet.sh symptom)
surfaces as a missing acked row / failed scan → FAIL.

**Which container dies, and on whose evidence.** The drill resolves the victim
before it kills anything: candidates are listed sorted, and with more than one
running `fluss-tablet` container it refuses to pick and asks for
`TABLET_CONTAINER` (a runner that silently kills "the first replica" is not
reproducible and does not say which tablet died). The chosen container is
verified with `docker inspect` and must be a *running* `*fluss-tablet*`, so a
typo or a name that merely looks like a container cannot reach the `docker
kill` inside the IT; a pinned override that does not resolve is a config error
(2), while an absent stack is a SKIP (3). The knobs are validated the same way
(`TABLET_KILL_ROWS`, `CHAOS_REPLICATION_MIN`, `CHAOS_SCAN_LIMIT_ROWS` — never
zero or smaller than the acked batch, which would make the acked-row assertion
hold vacuously), and the online compile retry is announced and bounded by
`CHAOS_COMPILE_TIMEOUT_S` (default 300) instead of hanging an unattended run.

**What a PASS means here.** Maven exiting 0 is not a PASS: the IT is gated and
assumes its stack, so a skipped test leaves a green build. The drill greps the
IT's own `TABLET-KILL-CHAOS-03: RESULT=PASS` sentinel and treats anything else
as either a named SKIP (an unmet assumption, exit 3) or a FAIL. On an RF1 dev
box the sentinel still means "the LOG did not shrink and the table is readable
again" — the just-acked tail loss is reported by the IT, not asserted away.

Env: `FLUSS_BOOTSTRAP`, `TABLET_CONTAINER`, `TABLET_KILL_ROWS` (default 25),
`CHAOS_REPLICATION_REQUIRED`, `CHAOS_REPLICATION_MIN`,
`CHAOS_POST_KILL_SCAN_BOUND_SEC` (default 600 — the full-log invariant rescan
on this dev box takes ~4 min for 11.5M rows; the old 60s attempt bound aborted
in-progress scans, see CHG-104), `CHAOS_SCAN_TIMEOUT_SEC`,
`CHAOS_COMPILE_TIMEOUT_S` (default 300 — bound on the one online compile retry).

## Test 4 — VM loss (Swarm)

Script: `chaos-04-vm-loss.sh`. Multi-node Swarm required (SKIP 3: single-node
swarms cannot lose their only survivor; local compose has no swarm at all).
Selects a workload node (`CHAOS_WORKLOAD_NODE` overrides; default: worker ≠
manager) and a replicated workload service running on it (`CHAOS_SERVICE`
overrides). Two modes:

- `CHAOS_VM_OFF_MODE=drain` (default) — `docker node update --availability
  drain`, automatic `active` restore on exit (trap).
- `CHAOS_VM_OFF_MODE=poweroff` — runs `CHAOS_VM_OFF_CMD` (e.g. an SSH
  poweroff of the real VM); restore is manual via `CHAOS_VM_ON_CMD`.

Asserts (T0 = loss injection): **order halt < 5 s** — `CHAOS_ORDER_PROBE_TCP`
(host:port of the order-facing endpoint that depends on the killed node)
stops answering within 5000 ms; skipped with a note when unset. **Data
recovery < 30 s** — the watched service re-schedules a Running task on a
surviving node within 30000 ms (overlay re-route). Evidence: `docker service
ps` dumps before/after + node list under `logs/chaos/.../chaos-04-vm-loss/`.

## Mapping to the plan and existing lanes

| Task | Docs | Test ID | Plan §6 invariant |
|---|---|---|---|
| T13 test 1 slot kill | this doc | L11 gate (Go) | No cross-slot kill |
| T13 test 2 TM kill | this doc | SIG-TMKILL-001 (extends L8 FAIL-002) | TM death → restore → no dup window/signal |
| T13 test 3 tablet kill | this doc, 02-schema-storage.md | DUR-TABLETKILL-001 (extends L8 FAIL-004, DUR-002) | Tablet death → LOG x3 → no acked-row loss |
| T13 test 4 VM loss | this doc, 09-production-swarm.md | SWARM-FAIL/DUR-003 style (M3) | VM loss → <30 s recovery, <5 s halt, overlay re-route |
| T13 acceptance | plan §5 row | 4 failures: no dup candle/signal, halt <5 s | Swarm 3k ×20 (main agent, serial, later) |

## Validation performed

- `bash -n` clean on all five scripts.
- `shellcheck -S warning` clean where shellcheck is available on the host.
- `mvn -o test-compile` on `02_compute` (both new ITs compile; no main
  sources touched).
- Go sources untouched — the Go leg only *selects* existing named tests.

### Implementation status — 2026-08-24 (offline laptop, no 4VM; single-VM `docker-compose.yml` + single-node Swarm only)

| # | Doc claim | Code verification | Status 2026-08-24 | Evidence offline (laptop) | Needs live |
| --- | --- | --- | --- | --- | --- |
| 1 | Runner `chaos-run.sh` `set -euo pipefail` static self-check `bash -n`+`shellcheck -S warning` clean 5 files `logs/chaos/chaos-<ts>/SUMMARY.txt` sentinel `EXIT 0/1/3` | `code/01_platform/04_scripts/chaos/chaos-run.sh 69L` `SCRIPT_DIR` `REPO_ROOT` `TS date -u` `LOGDIR` `SELF_FAIL` loop `bash -n`+`shellcheck` `RESULTS` `FAIL_COUNT` `run_one` `PASS` `SKIP 3` `FAIL` | PARTIALLY (offline DONE) | `bash -n 5/5 0` `shellcheck -S warning 5/5 0` `make chaos-suite` runner invoked 4 tests `SUMMARY.txt` `CHAOS-SUITE: RESULT=PASS EXIT=0` `logs/chaos/chaos-20260824-1130*/` | Live `3/4 SKIP` stays until `docker compose up` Swarm |
| 2 | Test 1 slot kill offline deterministic `go test -run` 6 Go tests T1 3000→1024+1024+952 no cross-slot kill peer healthy epoch 100 cycles | `chaos-01-slot-kill.sh 49L` `BRIDGE_DIR go-bridge` `go -C` `TestSubscriptionPlanShards3000Tokens` `TestSupervisorAuthTerminalIsolatedPerSlot` `TestINGRES001HealthySlotNotInterrupted` `TestReconnectLoopRecoversAfterFailures`/`EpochAndBackoff` `TestINGRES001OneHundred` `go vet ./...` | PARTIALLY (offline DONE) | `go -C bridge test -run … 1.43s PASS` `SLOT-KILL-CHAOS-01: PASS — 1-of-3 terminal isolated` `go vet 0` `RESULT [1]: PASS` | None — fully offline |
| 3 | Test 2 TM kill Leg A MiniCluster 2 TMs `terminateTaskManager` `RUNNING→RUNNING` fresh checkpoint no duplicate fingerprint extends `08 L8 FAIL-002` | `chaos-02-tm-kill.sh 85L` `COMPUTE_INT_TEST_TM_KILL=true mvn -Dtest=SignalJobTaskManagerKillIntegrationTest` `MiniCluster` `FingerprintDedupFunction` `CandleKvFirstWriteWinsFunction` | PARTIALLY (offline DONE) | `COMPUTE_INT_TEST_TM_KILL=true mvn test 1/1 12.73s` `Tests run:1 Failures:0 Skipped:0` `TM-KILL-CHAOS-02: [leg A] PASS` `RESULT [2]: PASS` | Leg B live `docker kill flink-taskmanager` `RUNNING` `checkpoint newer` `duplicate_window` needs `docker ps --filter name=flink-taskmanager` |
| 4 | Test 3 tablet kill `COMPUTE_INT_TEST_TABLET_KILL=true` `docker kill -s KILL tablet` `TABLET_KILL_ROWS 25` `replication x3` | `chaos-03-tablet-kill.sh 43L` `TABLET_CONTAINER` auto-discover `repair-tablet.sh` pattern `FLUSS_BOOTSTRAP` `CHAOS_REPLICATION_REQUIRED=false MIN 3` `mvn -o test-compile` | PARTIALLY (offline compile + stub DONE) | `mvn -o test-compile 0` `TABLET-KILL-CHAOS-03: SKIP — no fluss-tablet container (start the stack)` `RESULT [3]: SKIP EXIT 3` | Live `docker kill` `ack rows readable` `LOG never shrank` `x3` needs `fluss-tablet` + multi-node |
| 5 | Test 4 VM loss `drain|poweroff` `docker node update --availability drain` trap `active` `halt <5s` `recovery <30s` overlay `service ps` | `chaos-04-vm-loss.sh 102L` `docker info Swarm:active` `NODE_COUNT wc -l` `CHAOS_WORKLOAD_NODE` `CHAOS_SERVICE` `CHAOS_VM_OFF_MODE` `CHAOS_ORDER_PROBE_TCP` `timeout /dev/tcp` `service ps` `LOGDIR` | PARTIALLY (offline stub DONE) | `VM-LOSS-CHAOS-04: SKIP — single-node swarm cannot lose its only survivor` `RESULT [4]: SKIP EXIT 3` | Live `drain`/`poweroff` `order halt <5s` `recovery <30s` needs `09` M3 `multi-node` `worker≠manager` `CHAOS_VM_OFF_CMD` |
| 6 | Makefile `chaos-suite` `@bash code/01_platform/04_scripts/chaos/chaos-run.sh $(ARGS)` pass-through env | `Makefile:241-242` `.PHONY … chaos-suite` `chaos-suite: @bash …/chaos-run.sh $(ARGS)` `09` fluss `--filter` | PARTIALLY (offline DONE) | `make chaos-suite` invokes runner correctly `EXIT 0` `PASS 01 PASS 02 SKIP 03 SKIP 04` `08 L8` unchanged `T13 L11 gate` | None |
| 7 | No prod code touched `08 L8` `FAIL-* DUR-*` as-is | `git status` `01_platform/01_docker` untouched `code/02_services/02_compute` only ITs (no main) | PARTIALLY (offline DONE) | `docker-compose/stack/otel` not touched `08` `L8` `FAIL-002/004` `DUR-002/003` as-is `T13` on top | None |
