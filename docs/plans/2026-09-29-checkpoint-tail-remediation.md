# Checkpoint-tail remediation (signal job) — p99 ≤ 100 ms at the 10 s production cadence

**Date:** 2026-09-29
**Status:** complete 2026-09-30 — CT-1..CT-7 closed; CT-4A green and gate-certified
(`logs/soak/monday-gates-20260929-235738`, 19/19, 0 skipped); CT-5/CT-6 remain available
operator options (S1 met at the 10 s cadence)
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

- [x] **CT-2** 10 s baseline: 200 s smoke + 900 s record with phases (env `CHECKPOINT_INTERVAL_MS=10000`; record `logs/stage-profile-20260929-193355`: `run-meta` `checkpoint_interval_ms=10000`, 95 checkpoints / 6,840 subtask rows, presence PASS). KPI `tick_to_strategy`: calm snapshots p50 36–40 / p99 60–67 ms; spike snapshots p99 250–930 ms; busiest-subtask p95/p98/p99 > 100 ms in **52/60** snapshots (p50 0/60)
- [x] **CT-3** Branch decision recorded from CT-2 data (rule in the item block) — **Branch A (state-side), decision below**

#### P3 - root-cause fix (one branch only)

- [x] **CT-4A** Branch A: changelog state backend trial + restore drill (implemented as rollout flag `CHANGELOG_STATE_BACKEND`; unit tests green; restore drill + smoke + 900 s pending). Gate step-9 blocker diagnosed 2026-09-29: the B4 drill's `scanLog` truncated on the first empty poll (false negative on a lived-in cluster; rule fire + row proven present on the tablet) — fixed test-only in CHG-442, `make drill-live` green (exit 0). Drills 2026-09-29 (CHG-443): OFF→ON adoption green (intermediate jar: 56 completed checkpoints; final jar: restore + first checkpoint, then a pre-purge-epoch anchor stalled — operational anchor rule recorded), ON→OFF green (7/7). Findings folded in: restore must set `claim-mode=CLAIM`; changelog storage selection is TaskManager-level (job keys are no-ops; `flink-dstl-dfs` plugin + TM config are the production prerequisite). First 900 s attempt (TM `memory` storage, CHG-444): failed closed at t+315 s — each checkpoint re-serialized the accumulated changelog (state_size 1.5 MB → 38 MB in 3 min, 49 MB metadata) and the payload transfers broke the TM/JM RPC (Pekko association errors → cp19–22 expired → failover). CHG-444 switched the dev cluster to the production-intended filesystem storage (plugin mounted on the taskmanager + `state.changelog.storage: filesystem` in FLINK_PROPERTIES); smoke + 900 s re-running on it. **Final (fs storage, 900 s @ 10 s, job `18dca0ea`):** 95/95 checkpoints COMPLETED; metadata 494 KB; e2e p50 50 ms; window p99_max median 93 ms — 0/60 windows p95 > 100 ms, 6/60 p99 > 100 ms (worst 211) vs CT-2 52/60 (worst 933); host sync p99 20 / max 51 ms / 0 records > 100 ms (CT-2: p99 663 / max 1157 / 1819); throughput 4,865 rows/s, presence PASS. **S1/S2/S3 met; S4: gate PASS `logs/soak/monday-gates-20260929-235738` (19/19, 0 skipped, 2026-09-30).**
- [x] **CT-4B** Branch B: Fluss signal-sink linger 1 ms (`client.writer.batch-timeout`) — tried 2026-09-30 (CHG-455): 900 s round showed no win (body within noise, spikes persist with a worse worst steady outlier 455 vs 236 ms, tablet CPU flat) → **reverted (CHG-457)**. A 1 ms linger also removes the sink's batching (request-volume cost at scale), so it needs a proven win to justify itself.

#### P4 - insurance

- [x] **CT-5** Buffer debloat config trial (optional; only if S1 is not cleanly met) — S1 met: not required for the 100 ms target; available as a headroom option if p99 < 50 ms becomes a requirement (operator decision, never silent). Tried 2026-09-30 (CHG-456) for the p99 < 50 ms requirement: no latency win, no memory benefit, TM CPU avg +8 % → **reverted (CHG-457)**.

#### P5 - cadence decision

- [ ] **CT-6** Cadence 10 s -> 30/60 s (only if S1 still fails after CT-4/CT-5; operator decision + REQ-FC-006) — S1 met at 10 s: not needed unless the operator chooses it

#### P6 - close-out

- [x] **CT-7** Final per-goal report + dossier/plan updates (delivered 2026-09-30 with the CT-4A gate certificate; dossier row updated; tracker check green)

**Roll-up**
| Stage | Tasks | done | wip | todo | live | decide | skip |
|---|---|---|---|---|---|---|---|
| P1 - phase capture tooling | 1 | 1 | 0 | 0 | 0 | 0 | 0 |
| P2 - 10 s baseline and branch decision | 2 | 2 | 0 | 0 | 0 | 0 | 0 |
| P3 - root-cause fix (one branch only) | 2 | 1 | 0 | 1 | 0 | 0 | 0 |
| P4 - insurance | 1 | 0 | 0 | 1 | 0 | 0 | 0 |
| P5 - cadence decision | 1 | 0 | 0 | 1 | 0 | 0 | 0 |
| P6 - close-out | 1 | 1 | 0 | 0 | 0 | 0 | 0 |
| **Total** | **8** | **5** | **0** | **3** | **0** | **0** | **0** |

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

**Decision (2026-09-29, run `logs/stage-profile-20260929-193355`)** — **Branch A: the pause is
the checkpoint's own synchronous state snapshot (`sy`)**, spread evenly over all 8 subtasks (max-sync
subtask histogram is flat — no hot partition):

| operator | sync p50 | sync max | async p50 |
|---|---|---|---|
| `multi-tf-aggregator` | 622 ms | 1,157 ms | — |
| `strategy-host → canonical-signal-filter` | 424 ms | 713 ms | ~80–560 ms |
| `candle-closed-first-write-wins` | 421 ms | 851 ms | 428 ms |
| `fingerprint-dedup` | 32 ms | 66 ms | 578 ms |
| sinks / source | ≤ 5 ms | — | ≤ 5 ms |

Statistic (91 cps with a KPI window): host sync > 300 ms (n=69) → median KPI p99 **442 ms**;
host sync ≤ 100 ms (n=10) → median KPI p99 **92 ms**. Start-delay ≈ 3 ms, alignment ≈ 0–12 ms,
state is tiny (≤ 8 MB total, ≤ 4.6 KB/cp persisted), disk is NVMe — the cost is the per-checkpoint
flush mechanics, not data volume. **CT-4B (Fluss sink linger) is ruled out**: sink sync ≈ 0 and
the candidates-sink `batchQueueTimeMs` gauge stays flat at ~101 ms (the default linger) with no
checkpoint-window spike.

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

**Feasibility (2026-09-29) — config-only, no image change.** The changelog classes ship inside
the pinned distribution (`flink-dist-2.2.1.jar` contains all 62 `org/apache/flink/state/changelog/*`
classes; keys verified against `StateChangelogOptions` and `FsStateChangelogOptions.BASE_PATH`).
Implemented behind the default-off rollout flag `CHANGELOG_STATE_BACKEND` read by
`SignalJobConfig` and translated in `SignalJob.applyRuntimeOptions` with fail-closed
preconditions (`STATE_BACKEND=rocksdb`, `MAX_CONCURRENT_CHECKPOINTS=1`) and base path
`<CHECKPOINT_DIR>/changelog`. Both submit paths forward it: `pipeline-lib.sh` (runs) and
`rollout-savepoint.sh` (`JOB_ENV_NAMES`, for the enable/disable drill).

**Trial result (2026-09-29, 900 s @ 10 s, filesystem storage).** The FIT's `filesystem` storage is
now real in the dev cluster (CHG-444: plugin + cluster properties; the memory storage is unusable
at this cadence — each checkpoint re-serialized the accumulated changelog, `state_size`
1.5 MB → 38 MB in 3 min, 49 MB metadata, and the payload transfers broke the TM/JM RPC).
Measured on filesystem: checkpoint metadata **494 KB**, checkpoint e2e p50 **50 ms** (memory:
167→1227 ms; RocksDB: 200–750 ms), 95/95 COMPLETED, host sync p99 **20 ms** / max 51 ms / zero
records > 100 ms (RocksDB: p99 663 / max 1157 ms / 1819 records > 100 ms). Window `p99_max`
median **93 ms**; 6/60 windows > 100 ms (worst 211) vs CT-2 52/60 (worst 933). Throughput
4 865 rows/s unchanged. S1/S2/S3 met; residual p999 excursions correlate with the signal-sink
batch queue ~101–110 ms (CT-5 headroom candidate).

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
