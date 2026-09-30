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
