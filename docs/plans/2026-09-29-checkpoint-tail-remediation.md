# Checkpoint-tail remediation (signal job) — p99 ≤ 100 ms at the 10 s production cadence

**Date:** 2026-09-29
**Status:** operator-approved 2026-09-29; CT-1 in progress
**Baseline evidence:** `logs/stage-profile-20260929-174239/` (900 s, 60 s cadence, tree `eb3026ad`)
and gate certificate `logs/soak/monday-gates-20260929-180026` (19/19, 0 skipped)

**Problem (measured, not inferred):** every checkpoint produces one ~170–210 ms pause
concentrated on strategy-host subtask 0 (other subtasks p99 66 ms). At the profiler's 60 s
cadence that is <1 % of ticks (p99 67 ms ✅); at the 10 s production cadence it is ~4–6 % of
ticks, so p99 lands inside the pause (~200 ms ❌). The per-subtask checkpoint-phase probe
currently records nulls, so the pause's half — the host's own checkpoint phases vs the Fluss
sink-flush window — is undetermined. CT-1/CT-2/CT-3 close that gap; CT-4A/CT-4B then remove
the cause natively.

## 0. Live tracker

#### P1 - phase capture tooling

- [x] **CT-1** Per-checkpoint, per-subtask phase capture: `cp_phase_capture.py` + stage-profile wiring + parser test (CHG-440; smoke `logs/stage-profile-20260929-191631`: 24 cps / 1,728 subtasks / 0 null `sy`/`as`/`sd`; gate `--steps 1-3` PASS `logs/soak/monday-gates-20260929-192308`)

#### P2 - 10 s baseline and branch decision

- [~] **CT-2** 10 s baseline: 200 s smoke + 900 s record with phases (env `CHECKPOINT_INTERVAL_MS=10000`; smoke done as CT-1 verification, 900 s record in flight)
- [ ] **CT-3** Branch decision recorded from CT-2 data (rule in the item block)

#### P3 - root-cause fix (one branch only)

- [ ] **CT-4A** Branch A: changelog state backend trial + restore drill
- [ ] **CT-4B** Branch B: Fluss signal-sink linger 1 ms (`client.writer.batch-timeout`)

#### P4 - insurance

- [ ] **CT-5** Buffer debloat config trial (optional; only if S1 is not cleanly met)

#### P5 - cadence decision

- [ ] **CT-6** Cadence 10 s -> 30/60 s (only if S1 still fails after CT-4/CT-5; operator decision + REQ-FC-006)

#### P6 - close-out

- [ ] **CT-7** Final per-goal report + dossier/plan updates

**Roll-up**
| Stage | Tasks | done | wip | todo | live | decide | skip |
|---|---|---|---|---|---|---|---|
| P1 - phase capture tooling | 1 | 1 | 0 | 0 | 0 | 0 | 0 |
| P2 - 10 s baseline and branch decision | 2 | 0 | 1 | 1 | 0 | 0 | 0 |
| P3 - root-cause fix (one branch only) | 2 | 0 | 0 | 2 | 0 | 0 | 0 |
| P4 - insurance | 1 | 0 | 0 | 1 | 0 | 0 | 0 |
| P5 - cadence decision | 1 | 0 | 0 | 1 | 0 | 0 | 0 |
| P6 - close-out | 1 | 0 | 0 | 1 | 0 | 0 | 0 |
| **Total** | **8** | **1** | **1** | **6** | **0** | **0** | **0** |

## Overview

**Success criteria**

| # | Criterion | Proof |
|---|---|---|
| S1 | At 10 s checkpoint interval, p99 of `compute.latency.tick_to_strategy` <= 100 ms over 900 s | stage-profiler run, same protocol (2433 stocks @ 2 Hz, faketool, strategy host on) |
| S2 | Subtask-0 pause removed or shown reduced; phases name where it was | `stages/cp-phases-detail.jsonl` from the same run |
| S3 | No regression: all checkpoints COMPLETED, zero data loss, throughput unchanged, TM memory stable | presence gate + checkpoint list + metrics from the same run |
| S4 | Each change has its own test, CHG record, and a green gate | `make gate` certificate + `docs/05_deployment/change-records/CHG-*.md` |

**Working rules**

1. One variable per measurement round. Never two levers in one run.
2. Smoke first: 200 s must show data flow and non-null phases before any 900 s run.
3. No blocking waits over 30 s; use readiness signals and process completion.
4. Code changes: failing-first test -> change -> full `make gate` -> only then measure.
   No code edits while a gate is running; no profiler run during a gate.
5. Config changes go through existing repo paths and guards; committed with a CHG.
6. Evidence lands in `logs/` and is never edited afterwards.

**Non-goals:** p99 < 50 ms (separate normal-path workstream), 100 % of ticks < 100 ms
(not achievable), Flink 2.3 (no value for these goals), Fluss `acks`/`enable-idempotence`
(correctness guarantee, must not change).

## Items

### CT-1 — per-checkpoint, per-subtask phase capture

**GIVES YOU** — for every completed checkpoint and every subtask: align duration, start
delay, sync, async, alignment buffered, unaligned flag. Note (measured 2026-09-29): the
checkpoint-details endpoint itself is task-level only in Flink 2.2.1; the phases are served
per vertex at `/jobs/:jobId/checkpoints/details/:checkpointId/subtasks/:vertexId`.
**FIT** — new tool `code/01_platform/04_scripts/cp_phase_capture.py`; wired into
`stage-profile.sh` (background sampler for the capture window, stopped before teardown);
new parser test in `code/01_platform/04_scripts/tests/` (gate step 3 auto-discovers).
No edits to `stage-capture.sh`, `pipeline-lib.sh`, or any Java/Go/Flink code (the profiler's
reuse-by-invocation contract).
**COST** — tooling only; one extra REST call per vertex per checkpoint (plus one for the
checkpoint itself).
**ACTION** — done (CHG-440): implement + unit test + a 200 s smoke proving `sy`/`as`/`sd`
non-null on every subtask of every completed checkpoint (24 cps / 1,728 rows / 0 nulls).
`al`/`ab` are recorded only when the subtask ran alignment; unaligned checkpoints have no
alignment step, so null there is correct, not missing.
**WRONG IF** — the phases cannot be fetched at all or the smoke shows null `sy`/`as`/`sd`;
then fall back to per-subtask metrics + logs and record the limitation.

### CT-2 — 10 s baseline run with phases

**GIVES YOU** — the honest production-cadence number and the raw material for CT-3.
**FIT** — `CHECKPOINT_INTERVAL_MS=10000` exported before `stage-profile.sh` (dev range
1000..600000; job receives it through `pipeline-lib.sh` submit). Env only, no code change.
**COST** — one smoke (200 s) + one record run (900 s); no product changes.
**ACTION** — smoke -> record -> extract per-minute and per-subtask KPI + phase table.
**WRONG IF** — the run does not confirm `checkpoint_interval_ms=10000` at runtime or the
presence gate fails; fix before using the data.

### CT-3 — branch decision

**GIVES YOU** — the choice of CT-4A or CT-4B, written into this plan.
**FIT** — read-only analysis of CT-2 evidence (`stages/cp-phases-detail.jsonl`,
`flink-checkpoints.jsonl`, prom snapshots, KPI timeline).
**COST** — none (analysis).
**ACTION** — apply the rule:
Branch A if strategy-host subtask 0's own checkpoint phases carry the pause and the
candidates sink shows no coincident batch spike;
Branch B if the pause coincides with the output-flush window (large candidates-sink
batches at the same timestamps) and host phases are small;
both present -> A first, re-measure, then B.
**WRONG IF** — the phases cannot separate the two; then the fallback evidence (metrics +
logs) decides and the limitation is recorded.

### CT-4A — changelog state backend (Branch A only)

**GIVES YOU** — continuous state materialization so the checkpoint's own sync/async phases
shrink; less failover replay.
**FIT** — `state.changelog.enabled: true`, `state.changelog.storage: filesystem`, base-path
= existing checkpoint dir; keep `max-concurrent-checkpoints: 1`. Restore drill both
directions (enable and disable) before measuring.
**COST** — more TM memory for change buffering, more files/IO; recovery time can vary.
Exactly-once unchanged.
**ACTION** — restore drill -> smoke -> 900 s at 10 s; compare against CT-2.
**WRONG IF** — the run shows memory growth beyond the TM budget, checkpoint failures, or no
p99 improvement; revert (one config line) and move to CT-5.

### CT-4B — Fluss sink linger 1 ms (Branch B only)

**GIVES YOU** — signal sinks send sooner, in smaller pieces, removing the flush pile-up at
checkpoint windows.
**FIT** — `client.writer.batch-timeout=1ms` on the signal sinks (same `setOption` mechanism
already used for `client.request-timeout`); durability settings untouched.
**COST** — more, smaller requests; small CPU/network overhead.
**ACTION** — failing-first test -> full gate -> smoke -> 900 s at 10 s; compare against CT-2.
**WRONG IF** — sink throughput or error counters regress, or p99 does not improve; revert the
commit + gate, move to CT-5.

### CT-5 — buffer debloat insurance (optional)

**GIVES YOU** — smaller in-flight data, so unaligned-cp payload and barrier travel shrink.
**FIT** — `taskmanager.network.memory.buffer-debloat.enabled: true` (default false), target
1 s; one config line.
**COST** — small; watch debloat metrics under spiky load. Does NOT reduce memory usage.
**ACTION** — enable -> smoke -> 900 s; include only if S1 is not cleanly met or headroom is
wanted.
**WRONG IF** — throughput drops or checkpoint times regress; revert the config line.

### CT-6 — cadence decision (only if S1 still fails)

**GIVES YOU** — a guaranteed way to meet p99 <= 100 ms: fewer pauses means fewer slow ticks
(60 s -> <1 % hit rate).
**FIT** — contract REQ-FC-006 + production pin + docs; operator decision, never silent.
**COST** — after a crash, more data to replay (up to the interval). Recovery size, not
result correctness.
**ACTION** — present CT-2/CT-4 data; operator decides; implement + gate + confirming run.
**WRONG IF** — the operator prefers exceeding the SLO at 10 s; then this stays skipped and
S1 is reported unmet with evidence.

### CT-7 — close-out

**GIVES YOU** — per-goal verdicts (latency, throughput, memory, correctness) with evidence
links; updated dossier `docs/08_implementation/04-signal-job.md` and this plan's markers.
**FIT** — documentation only, in the same change as the final measured state.
**COST** — none.
**ACTION** — report + doc updates + marker flips + `plan_tracker.py --check` green.

## Evidence map

| Phase | Evidence |
|---|---|
| CT-1 | unit test + smoke `stages/cp-phases-detail.jsonl` (non-null) + CHG + scoped gate 1-3 |
| CT-2 | `logs/stage-profile-<ts>/` (smoke + main), KPI extraction, phase table |
| CT-3 | decision note in this plan (item CT-3 `ACTION` result) |
| CT-4A/B | CHG + gate certificate + before/after 900 s runs |
| CT-5 | CHG + gate certificate + before/after run |
| CT-6 | contract diff + CHG + confirming run |
| CT-7 | dossier diff + tracker `--check` output |

## Rollback

- CT-1: revert the commit; the tool is additive, no runtime state.
- CT-4A: revert the changelog config line; both enable/disable directions are documented as
  restore-safe.
- CT-4B: revert the commit + `make gate`.
- CT-5: revert the config line.
- CT-6: contract change only on operator approval; revert = restore the 10 s pin + docs.
