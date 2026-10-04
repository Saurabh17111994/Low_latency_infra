# W3/W4 design note — residual stalls: materialization, GC, and the unexplained outliers

**Status:** proposed — for operator approval (A1–A3 below). No code or config touched.
**Date:** 2026-09-30
**Sources:** plan `docs/plans/2026-09-30-p99-50ms-normal-path.md` (W3/W4); CHG-449/CHG-450;
runs `logs/stage-profile-20260929-231505` (baseline), `logs/w1-control-main-20260930-104228`
(control 20/10), `logs/w1-fetchwait-main-20260930-101707` (W1 2/10),
`logs/w1w2-combined-main-20260930-110606` (combined 2/2); TM `gc.log*` and `tm-diag.jfr`
(covers 05:36:16–05:51:16 UTC = 11:06–11:21 IST, i.e. the combined run).

## 1. Problem statement (measured)

After W1+W2 (combined, 900 s): ingest p50 23 / p95 49 / p99 med 58; tick p50 31 / p95 62 /
p99 med 71. The SLO (`p99 ≤ 50 ms`, every window) is still failed by the **tail**: 6 spike
windows in 900 s, in two measured classes:

| Class | Size in the combined run | Correlation | Evidence |
|---|---|---|---|
| Mid spikes | tick p99 125–180 ms | **5/6 coincide (±1 snapshot) with changelog materialization events** (aggregator 115–217 ms durations) | staircase analysis across 4 runs |
| Large outliers | 200–430 ms (control 243; W1 206, 416; combined **354/424**) | **none** with materialization | same |

## 2. Verified facts from this design pass

- **F1 — materialization shape:** per-subtask and staggered (each subtask materializes on
  its own trigger), ~1–2 rounds/subtask per 900 s; durations aggregator 91–216 ms, others
  40–96 ms; state 0.2–6.3 MB/subtask (`ChangelogMaterialization_*`,
  `ChangelogStateBackend_*` metrics).
- **F2 — TM GC evidence (gc.log + JFR):** young pauses 6–36 ms are common (295 events
  ≥ 15 ms over 10:04–11:25); the **largest pause in the whole combined run was 107 ms**
  (G1PauseRemark, 11:09:02) and it produced **no spike window**. At the 354/424 outlier
  (11:12:23) the longest TM stop was ~6 ms at the instant and 25.6 ms within ±30 s. ⇒ **The
  TM JVM does not explain the large outliers.** (JFR: longest VM ops in the run 107 ms
  remark, 68.7 ms young, 66.8 ms `ClassLoaderStatsOperation` at 11:08:56 — the latter is a
  capture-induced candidate worth watching.)
- **F3 — W4's premise is a unit misread:** `flink_taskmanager_job_task_busyTimeMsPerSecond`
  is **ms/s** (1 000 = 100 %). In every run, the candle-live sink's hottest subtask reads
  59–92 ms/s (**6–9 %**), all operators ≤ 75 ms/s (7.5 %), back-pressure 0. The plan's
  "72–87 % busy" is that metric read as %. Slot isolation has **no measured basis**.
- **F4 — large-outlier suspects (unattributed):** ingestion writer JVM (JDK 17, no GC/JFR
  instrumentation detected), Fluss tablet (log-silent in the window), host scheduling
  (around 11:12:23: cpu_user 30–33 %, PSI-cpu ≈ 8 % — elevated vs 0–4 % elsewhere —
  mem ≈ 3.4 GB free). The profiling capture itself can trigger JVM-wide VM ops
  (`ClassLoaderStatsOperation` 66.8 ms) — capture hygiene to keep on the list.

## 3. Scope decisions

- **W4 closes as not-reproducible** (F3); its productive content (JVM/background
  attribution) is re-homed into W3's instrumentation. Re-open only if W3-i attributes the
  outliers to host/container scheduling (then the lever is W4' below).
- **W3 proceeds in two workstreams:**
  - **W3-i (instrumentation, no behavior change):** (a) enable GC logging + a JFR recording
    on the **ingestion writer** (JVM flags; container recreate; CHG); (b) capture
    per-container CPU at 1 s during rounds (cadvisor data or a profiler-side sampler); (c)
    make TM gc.log/JFR parsing part of every round's extraction (available today, done by
    hand this pass).
  - **W3-l (levers), only on W3-i evidence:** the menu below.

### 3.1 W3-i implementation notes (2026-09-30) — two premise corrections

- **The ingestion writer already had GC+safepoint logging.** `stage-profile.sh` launches it
  with `-Xlog:gc*,safepoint:file=/logs/gc.log` on the `java` command line; every run dir
  already carried `capture/j1-*/gc.log`. W3-i adds the missing **JFR**, keeps the
  JAVA_TOOL_OPTIONS copy as branch hardening, materialises the JFR via a **graceful stop**
  before teardown, and wires the **summariser** in. Running the summariser over the combined
  run's writer logs **exonerates the ingestion-writer JVM**: max stop 27.1 ms, zero ≥ 28 ms
  events, nothing near the 11:12:23 IST outlier. Suspects remaining: **Fluss tablet, host
  scheduling**.
- **Host pressure rides in the stats sampler.** `stages/docker-stats.log` now also carries
  `/proc/pressure/{cpu,io,memory}` per sample (8 % psi-cpu at the 354 ms outlier vs 0–4 %
  elsewhere) — the host-vs-container discriminator.
- **Tablet JFR remains uncaptured** (digest-pinned image strips `jcmd`/`jdk.jcmd`; its
  JVM-level recording is a long-expired 900 s window). Per-phase tablet restart was rejected
  for the certification round (cold-tablet bias); a dedicated diagnostics round can restart
  it if PSI/CPU evidence points there. See CHG-451 "Limitations".

## 4. Lever menu (for operator selection)

| id | Lever | Expected gain | Cost / risk | Guardrails |
|---|---|---|---|---|
| **W3-a** | JVM/GC tuning (TM, ingestion): G1 pause target, young-gen sizing, heap ceiling | mid class −10–20 ms on spike windows; outliers only if ingestion-side | env-only per service; container recreate | 4 866 rows/s flat; pause/alloc counters; no heap growth |
| **W3-b** | changelog storage → filesystem (dstl-dfs plugin on TM; CHG-443 path) — **ALREADY LIVE since 2026-09-29** (CHG-444: plugin mounted, `state.changelog.storage: filesystem`; 95/95 checkpoints, sync p99 663 → 20 ms). This row is retained for history only; nothing to implement. | — | — | — |
| **W3-c** | materialization cadence/stagger tuning (periodic-materialize default 10 min; size trigger not yet pinned) | fewer/shorter rounds | config; longer restores | restore drills |
| **W3-c (closed 2026-09-30)** | tried 30 min (CHG-454) in two 900 s rounds: no KPI gain; only the startup materialization batch occurred either way (the scheduler staggers the rest outside the window) | none measured | cost seen: changelog base path 1.9 GB, longer restore replay | **reverted (CHG-457)** |
| **W3-d** | aggregator state-size reduction | cheaper materialization | code/design; correctness review | full gate; DEC-056 features untouched |
| **W3-d (design done 2026-09-30)** | re-scoped after verify-first: the candle/dedup structures are heap-only (never checkpointed); the real churn is the **per-key live-mirror timers** (~4 900 state mutations/s → ~20 MB/min changelog, 94 MB checkpoint state). Fix = operator-scope live mirrors (scan) — expected ≥85 % churn cut, same observable behavior | changelog/disk/checkpoint/CPU all drop | code change in `MultiTimeframeAggregateFunction` + failing-first tests | see `2026-09-30-w3d-state-churn-design.md`; gate when un-deferred |
| **W4'** | container CPU isolation (cpuset/pinning) | only if W3-i attributes outliers to host scheduling | compose change; dev-only finding | production sizing note |

## 5. Recommended sequence

1. Approve **W3-i** (instrumentation only) and the **W4 closure record**.
2. Implement W3-i; then run the **combined W1–W4 round** (smoke + 900 s) with instrumentation
   live — it doubles as the certification attempt and as the attribution run.
3. If outliers persist, the captured data (ingestion GC/JFR, per-container CPU, Fluss
   tablet metrics) attributes them; implement the relevant lever (**W3-a first**; W3-b only
   if the storage path shows up; W4' only on evidence) and re-run.
4. **W5** (hop removal, −5–7 ms) remains the fallback if the round still misses `p99 ≤ 50`.

## 6. Operator approval asks

- **A1:** approve **W3-i instrumentation** (ingestion GC/JFR flags; per-container CPU
  capture) — no behavior change.
- **A2:** approve **W4 closure** ("not reproducible" — premise corrected in the plan) or
  request a different W4 scope.
- **A3:** confirm the lever order for step 3 (recommended: W3-a → W3-b if needed; W4' only
  on evidence).

## 7. Update 2026-10-03 — post-W3-d certification: the residual is the per-checkpoint async persist

**Status:** proposed — for operator approval (§7.4). No code or config touched.
**Sources:** 2026-10-03 900 s certification `logs/stage-profile-20261003-l4-cert` (job
`e79be4c94ae8379c4ef1398bb4b59b3f`, 94 checkpoints @ 10 s, changelog filesystem, W3-d in HEAD);
CHG-526 arms (same tree, 60 s, `logs/stage-profile-20261003-chg526-on/off`); TM log + TM
changelog metrics from the run.

### 7.1 What changed since §1–§4

- W3-d (CHG-460) landed: the per-key live-mirror timers are gone (changelog +0.45 MB/min in the
  certification; state 0.5 → 6.5 MB over the run). The single-subtask materialization stall of
  §1 (126–216 ms on the aggregator) is **no longer the tail driver**: the certification's worst
  sync across all subtasks is 66 ms (aggregator p99 24).
- The residual moved to the **async phase** and it is **cluster-wide, not single-subtask**:
  all-subtask async p50 6 / p90 55 / p99 83 / max 121 ms; 25 of 94 checkpoints carry a wave
  (many subtasks > 50 ms at once). Sync stays small.
- Impact at the 10 s cadence: `tick_to_strategy` worst-subtask p99 > 100 ms in **45/50**
  snapshots (2026-09-29 certified tree: 6/60; same tree at 60 s: 8/50). The stall is
  per-checkpoint, so the 10 s cadence multiplies it — W3 is now the S1 (p99 ≤ 50 ms) blocker.

### 7.2 Mechanism — measured

- **Materialization is ruled out as the wave driver.** 36 materializations completed across 24
  keyed subtasks, durations ≤ 86 ms, and the per-checkpoint correlation with the async waves is
  ~none (most wave checkpoints have 0 materialization starts in the preceding window). The old
  §1 single-subtask pattern does not reproduce.
- **The async cost is the per-checkpoint changelog persist, and the bytes are tiny:**
  `ChangelogStateBackend_lastIncSizeOfNonMaterialization` sums to ~29–184 KB per snapshot across
  24 subtasks. The cost is therefore **coordination/queueing, not bytes**.
- **Leading hypothesis — the shared upload pool:** all subtasks on the TM share one filesystem
  changelog storage with `state.changelog.dstl.dfs.upload.num-threads: 5` (default) and
  `state.changelog.dstl.dfs.batch.persist-delay: 10 ms` (default). At a 10 s checkpoint, ~24
  keyed subtasks request persist within a few ms; the batch delay coalesces them, then 5 threads
  drain the queue — the last subtask's async ≈ queue depth / threads × per-upload time
  (≈ 5 rounds × ~15 ms ≈ 75 ms), matching the measured p99 83 / max 121 ms. Fast checkpoints are
  the ones whose persist requests arrive staggered.
- **Open secondary — registry churn:** `TaskChangelogRegistryImpl "state is not in tracking"`
  WARNs fired **954 times during the run** (smoke 11:26 + main 11:57–11:58 UTC) even though the
  preflight wiped the base path (CHG-458 assumed leftovers; this is intra-run). The discard path
  (`discard.num-threads: 1`) is the visible suspect; its contribution is unmeasured.

### 7.3 Native levers (config-only, TM-level; Flink 2.2.1 options)

| id | Lever (default → candidate) | Expected effect | Cost / risk |
|---|---|---|---|
| **W3-e1** | `state.changelog.dstl.dfs.upload.num-threads` 5 → 16 | drains the per-checkpoint queue; cuts the async tail if the hypothesis holds | TM recreate; more concurrent FS writes |
| **W3-e2** | `state.changelog.dstl.dfs.batch.persist-delay` 10 ms → 0–2 ms | removes the batching wait; more/smaller files | TM recreate; more registry entries |
| **W3-e3** | `UNALIGNED_CHECKPOINTS=false` (job env) | diagnostic: isolates the in-flight-data flush from the changelog persist inside the async phase | env-only; CHG-318 measured unaligned as a win for RocksDB — re-measure with the changelog |
| **W3-e4** | `state.changelog.dstl.dfs.compression.enabled` false → true | fewer bytes (CPU trade) | only if bytes matter (they do not today) |
| — | `state.changelog.periodic-materialize.interval` | **stays closed** (W3-c: 30 min no gain, reverted CHG-457) | — |

### 7.4 Proposed measurement sequence (one lever per round, smoke first)

1. **D0 (diagnostics, no config change):** extract the changelog metric series
   (`ChangelogMaterialization_*`, `ChangelogStateBackend_*`) per snapshot alongside the
   cp-phases timeline for one 200 s smoke; confirm the persist-queue shape (async vs upload
   completions). No TM recreate.
2. **W3-e3 first** (env-only): one 200 s smoke — it decides whether the tail is changelog-side
   at all; extend to 900 s only if the smoke shows the wave collapsing.
3. **W3-e1 next** (TM recreate): same protocol; if the async p99 drops, the queue hypothesis is
   confirmed and the option becomes the production candidate.
4. **W3-e2** only if e1 is insufficient. Restore drills both directions before any production
   adoption of a TM config change.

**Operator approval asks:** approve **D0 + W3-e3** (no TM recreate) and **W3-e1** as the
follow-up lever.

### 7.5 Verdict — W3-e3 900 s verified (2026-10-03)

**Run:** `logs/stage-profile-20261003-w3e3-main` (job `989d1822f74c83e9e534833ce7ebaed8`,
120 s smoke + 900 s main, unaligned OFF, changelog ON, 10 s cadence; 94/94 COMPLETED,
presence counts for every stage, 0 errors, 4,865 rows/s unchanged).

| Metric (full 900 s) | Cert (unaligned ON) | W3-e3 (unaligned OFF) |
|---|---|---|
| Async p50 / p90 / p99 / max | 6 / 55 / 83 / 121 ms | **0 / 19 / 32 / 44 ms** |
| Subtasks > 50 ms (wave load) | 618 in 35/94 cps | **0 in 0/94 cps** |
| Sync max | 66 ms | 31 ms |
| e2e max per checkpoint | 181 ms | 94 ms |
| `tick_to_strategy` max (worst p99/snapshot) | 359 ms | **115 ms** |
| `tick_to_strategy` > 100 ms windows | 45/50 | 29/49 |
| `ingest_to_monitor` max / min | 113 / 81 ms | 89 / 47 ms |

- **Verdict:** the per-checkpoint in-flight dump is confirmed as the spike cause; aligned-only
  removes it for the full run (zero wave checkpoints). The residual >100 ms windows are the
  constant ~101–104 ms baseline (p50 unchanged), i.e. the separate W1/W2/W5 item.
- **Production packaging decision:** hybrid (unaligned ON +
  `execution.checkpointing.aligned-checkpoint-timeout` > 0) keeps the normal-case speed and the
  backpressure fallback; requires one config line + TM/JM recreate and one verification round.
  Plain OFF is the simpler alternative.
- **Note (separate):** S11 readability tail in this run (worst-TF p99 ≈ 9.7 s) is the known
  pre-existing readability anomaly (M4.1 measurement-coverage register), not a checkpoint effect.

### 7.6 Verdict — hybrid packaging verified (2026-10-03)

**Run:** `logs/stage-profile-20261003-w3hybrid-main` (job `3f9bc67428ffe9e68f58202d1b8be8a4`,
120 s smoke + 900 s main, unaligned ON + `execution.checkpointing.aligned-checkpoint-timeout: 10 s`,
changelog ON, 10 s cadence; 94/94 COMPLETED, presence PASS, 4,866 rows/s, 0 errors).

| Metric (full 900 s) | CERT (unaligned) | Aligned-only | **Hybrid** |
|---|---|---|---|
| Checkpoint types | UNALIGNED | CHECKPOINT | **CHECKPOINT (0 conversions)** |
| Async p90 / p99 / max | 55 / 83 / 121 ms | 19 / 32 / 44 ms | **18 / 30 / 65 ms** |
| Subtasks > 50 ms | 618 in 35 cps | 0 | **9 in 2 cps** |
| Sync max | 66 ms | 31 ms | 36 ms |
| e2e max per checkpoint | 181 ms | 94 ms | 113 ms |
| `tick_to_strategy` max | 359 ms | 115 ms | 120 ms |
| `tick_to_strategy` > 100 ms | 45/50 | 29/49 | 35/50 |
| `ingest_to_monitor` max | 113 ms | 89 ms | 91 ms |

- **Verdict:** hybrid ≡ aligned-only within run noise (the hybrid run's ingestion S5 was
  p50 33 vs 26 ms — the host was ~20% slower; the 9 subtasks > 50 ms track that, not hybrid
  overhead). The safety switch never fired: all 94 checkpoints completed aligned.
- **Decision (operator, 2026-10-03):** adopt **aligned-only** (`UNALIGNED_CHECKPOINTS=false`) —
  the hybrid was verified equivalent (above) but the operator chose the simpler mode. Production
  config: CHG-543 (VM template + local env flag); the hybrid line was removed from both decks
  before deployment (CHG-542 kept as the documented re-upgrade path).
- **Accepted trade-off:** aligned saves wait for the marker; under real backpressure the wait can
  grow (the hybrid's auto-fallback would have covered it). Production has shown no backpressure;
  re-upgrade to the hybrid = one line (CHG-542).
- **Validation:** 92 + 79 tests green. Live verification at the next production start window
  (same window as CHG-541); rollback = `UNALIGNED_CHECKPOINTS=true`.
