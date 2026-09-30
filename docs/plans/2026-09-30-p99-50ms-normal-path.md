# p99 < 50 ms normal-path latency (signal job) — SLO on `compute.latency.ingest_to_monitor`

**Date:** 2026-09-30
**Status:** operator-approved 2026-09-30. Rounds start only when their window opens;
go/no-go after each round's report.
**Baseline evidence:** `logs/stage-profile-20260929-231505/main` (900 s, 2 433 stocks @ 2 Hz,
job `18dca0eaba3dfc266ba0a28d040f4ea5`, 10 s checkpoints, changelog filesystem storage,
gate-certified `logs/soak/monday-gates-20260929-235738`) + read-only analysis
`logs/50ms-analysis-20260930/analysis.md`.

**SLO (operator decision, 2026-09-30):**

- **Primary — the promise:** p99 of `compute.latency.ingest_to_monitor` ≤ **50 ms**:
  from ingestion accept (`raw.ingest_ts`, our clock) to the post-dedup read in the signal
  job. This is the number the repo tooling already defines as the real-feed SLO
  (`code/01_platform/04_scripts/latency_probe.py:6-8`).
- **Companion — acceptance on the ms test feed:** p99 of
  `compute.latency.tick_to_strategy` ≤ **50 ms** (tick event-time → strategy host read)
  on the fake feed (millisecond timestamps), same run.
- **Explicitly not the promise:** `tick_to_strategy` on the production feed — its event
  time is epoch-seconds × 1000 (0–1000 ms quantization outside the pipeline;
  `latency_probe.py:9-11`). Fixing feed timestamp fidelity is a separate item, not here.

## Problem (measured, not inferred)

The checkpoint freeze is fixed (CT-4A: 95/95 COMPLETED, host sync ≈ 0, save pause
20–51 ms, gate-certified). The normal path is what remains:

| KPI | p50 | p95 | p99 | windows ≤ 50 ms |
|---|---|---|---|---|
| `ingest_to_monitor` | 29–35 (med 32) | 52–68 | 66–82 (med 75) | **0/60** |
| `tick_to_strategy` | 43–59 (med 47) | 72–93 (med 80) | 86–211 (med 93) | **0/60** |

Measured decomposition of the ~47 ms p50 (analysis file, §2–§3):

- ~10–20 ms: Fluss source log-fetch window — cap `FLUSS_SCANNER_FETCH_WAIT_MAX_TIME_MS=20`;
  `fetchLatencyMs` reads 20 ms on every raw subtask, every snapshot.
- ~18 ms: in-DAG transit over 3 shuffle hops (dedup 6 → aggregator 11 → host 18 ms
  cumulative p50); `BUFFER_TIMEOUT_MS=10` per hop.
- ~6–9 ms: ingestion accept → Fluss visible → read start (linger already 1 ms — not a lever).
- Spikes: 6/60 windows at 103–211 ms, always a single host subtask (105–211 ms while
  siblings stay 84–92); coincident with changelog materializations of 126–216 ms per
  checkpoint (aggregator) and a 72–87 % busy candle-live sink writer on the same TM.

The earlier plan note that attributed the residual to the signal-sink batch queue is
superseded: the sink queue is flat (~100 ms, unchanged) and correlates −0.10 with
`tick_to_strategy` p99; the ingest leg's own p99 correlates +0.61. The sink path is
downstream of the KPI point and is out of scope.

## 0. Live tracker

#### P1 - measurement lock

- [x] **W0** Baseline re-derivation with the exact pass method (worst subtask per
  snapshot, both KPIs) — done 2026-09-30, `logs/50ms-analysis-20260930/analysis.md`;
  pass state 0/60 at 50 ms.

#### P2 - lever rounds (combined-first for ready env levers, operator-approved 2026-09-30; one-variable-per-round as fallback)

- [~] **W1** Fluss source fetch window 20 → 2 ms (env-only trial; control round in flight)
- [~] **W2** Output-buffer flush timer 10 → 2 ms (env-only trial; measured combined with W1)
- [ ] **W3** Checkpoint materialization churn (design first, then config/code)
- [ ] **W4** TM CPU contention / slot isolation (design)
- [ ] **W5** Structural hop reduction (only if W1–W4 miss 50 ms; design first)

#### P3 - certification

- [ ] **W6** Combined confirmation + certification

**Roll-up**

| Stage | Tasks | done | wip | todo | live | decide | skip |
|---|---|---|---|---|---|---|---|
| P1 - measurement lock | 1 | 1 | 0 | 0 | 0 | 0 | 0 |
| P2 - lever rounds (combined-first for ready env levers, operator-approved 2026-09-30; one-variable-per-round as fallback) | 5 | 0 | 2 | 3 | 0 | 0 | 0 |
| P3 - certification | 1 | 0 | 0 | 1 | 0 | 0 | 0 |
| **Total** | **7** | **1** | **2** | **4** | **0** | **0** | **0** |

## Overview

**Success criteria**

| # | Criterion | Proof |
|---|---|---|
| S1 | In a 900 s run at 10 s checkpoints (same protocol as the baseline), **every** 60 s snapshot's worst-subtask p99 is ≤ 50 ms for `ingest_to_monitor`, and the same for `tick_to_strategy` (ms fake feed) | stage-profiler run + the frozen extraction method |
| S2 | No regression: all checkpoints COMPLETED, presence PASS, throughput within ±2 %, TM memory stable, no read/write errors | same run |
| S3 | Every adopted change has its own test (code), CHG record, and a green gate; env-only trial rounds record the exact env in the run evidence | `make gate` certificate + CHG files |
| S4 | Dossiers updated (`docs/08_implementation/04-signal-job.md`, KPI docs) and this tracker closed with evidence | doc diff + `plan_tracker.py --check` |

**Working rules** (same as CT-4A):

1. One lever per measurement round. Never two levers in one run.
2. Smoke first: 200 s must show data flow (table growth, source read counters,
   presence) before any 900 s run.
3. No blocking waits > 30 s; readiness/process signals only.
4. Code changes: failing-first test → change → full `make gate` → only then measure.
   No code edits while a gate is running; no profiler run during a gate.
5. Env-only trial rounds touch no tracked file. If a winning value becomes a default,
   it lands as a code change with CHG + full gate.
6. Evidence lands under `logs/` and is never edited afterwards.
7. After each round: short report + operator go/no-go before the next lever.

**Non-goals:** feed timestamp-fidelity fix (epoch-seconds × 1000 — separate item);
100 % of ticks < 50 ms; sink/downstream (executor) latency; Flink 2.3; any change to
dedup, candle, or strategy semantics.

## Items

### W1 — Fluss source fetch window (env-only trial)

**GIVES YOU** — removes most of the 10–20 ms the Fluss server waits to accumulate each
fetch chunk before the source can emit ticks.
**FIT** — `FLUSS_SCANNER_FETCH_WAIT_MAX_TIME_MS` 20 → 2 (existing env key;
default `SignalJobConfig.java:1267-1268`; applied as
`client.scanner.log.fetch.wait-max-time` at `SignalJob.java:477-478`). No code change.
**COST** — fetch request rate rises (~50/s → ~600/s per subtask at 4.9 k ticks/s;
~3 k/s at the 60 k/s production rate). Watch `requestsPerSecond`, `fetchLatencyMs`,
source throughput, Fluss server CPU during the run.
**ACTION** — smoke 200 s → 900 s → extract both KPIs + fetch metrics; report.
If it wins and holds, propose the default change (code + CHG + full gate) in W6.
**WRONG IF** — read errors, throughput drop, p99 unchanged or worse; revert the env
value, record the failed round, fall back to an intermediate value (5 ms) as one more round.

**ROUND RESULT (2026-09-30, CHG-449):** smoke + 900 s at 2 ms ran clean (fetchLatencyMs
20 → 2 ms, errors 0, throughput 4 866 rows/s, 95/95 checkpoints). Medians improved on both
KPIs (ingest p50 33 → 29, p99 med 75 → 71; tick p50 47 → 45, p99 med 93 → 89). The 20 ms
control run (same day, same protocol, `logs/w1-control-main-20260930-104228`) resolved the
tail question: the control carries the same class **more heavily** (1/60 ingest > 100 ms,
max 243; **19/60** tick > 100, max 273) than W1 (2/60 ingest, max 416; 7/60 tick, max 434)
⇒ **W1 passes**; the spikes are environmental, not lever-caused. Not an SLO pass
(0/60 ≤ 50 ms). Marker stays `[~]` until the operator's deferred gate; next: the
operator-approved combined W1+W2 round (CHG-450).

### W2 — Output-buffer flush timer / debloat (env/config trial)

**GIVES YOU** — cuts up to ~9 ms of worst-case wait per network hop (3 hops on the tick path).
**FIT** — `BUFFER_TIMEOUT_MS` 10 → 2–5 (existing env key; default
`SignalJobConfig.java:1334-1335` — verified 2026-09-30; applied `SignalJob.java:635-636`).
Buffer-debloat (`taskmanager.network.memory.buffer-debloat.enabled`, the former CT-5 lever)
is a **separate** run only if the timeout alone is not enough.
**COST** — more flush events (CPU/network overhead); watch busy time, throughput,
checkpoint durations.
**ACTION** — same protocol as W1.
**WRONG IF** — throughput drops or checkpoint times regress; revert and record.

**COMBINED ROUND (operator-approved 2026-09-30, `CHG-450`):** W2 is measured together with
W1 in one round — `FLUSS_SCANNER_FETCH_WAIT_MAX_TIME_MS=2 BUFFER_TIMEOUT_MS=2`, smoke 200 s
then main 900 s. Attribution without extra runs: W1 effect = W1-alone (2/10) − control
(20/10); W2 marginal = combined (2/2) − W1-alone. A win ⇒ keep both, no W2-alone run; a
regression ⇒ bisect with one W2-alone run (20/2). Both keys are already plumbed
(`pipeline-lib.sh:1146,1170`) — env-only, no code change. The one-variable-per-round rule
remains the fallback for W3–W5 (not implemented yet, so they cannot join a combined round).

**ROUND RESULT (2026-09-30, CHG-450):** smoke + 900 s at 2/2 ran clean (errors 0,
throughput 4 866 rows/s, 95/95 checkpoints; lever values live). W2 marginal vs W1-alone:
ingest p50 −6 / p95 −11 / p99 med −13 ms; tick p50 −14 / p95 −17 / p99 med −18 ms —
larger than projected. W1+W2 total vs control: ingest p50 37 → 23, p99 med 81 → 58; tick
50.5 → 31, p99 med 99 → 71. Cost recorded: checkpoint e2e med 75 ms (W1 53 / control 40),
sync medians 8–10 ms — sub-0.75 % duty, all COMPLETED, re-check in the W1–W4 round;
fallback `BUFFER_TIMEOUT_MS=5` if it grows. Tail split: 5/6 spike windows align with
materialization events; the largest (354/424) does not — the unexplained class goes to the
W3 GC/background-thread instrumentation. W2 passes; marker stays `[~]` until the deferred
gate.

### W3 — Checkpoint materialization churn (design first)

**GIVES YOU** — removes the 100–211 ms single-subtask spikes that set the worst windows.
**FIT** — investigation only at first: changelog materializations run 126–216 ms per
checkpoint on the multi-tf aggregator; host sync is already ≈ 0. Compare native
`state.changelog.*` / filesystem-storage materialization options (CHG-443/CHG-444 are
the current state) and locate why one host subtask eats a stall while siblings do not.
**COST** — design note first; any storage/config change = CHG + full gate; may affect
checkpoint duration/recovery — restore drills both directions required.
**ACTION** — design note → operator approval → implement if approved → smoke → 900 s.
**WRONG IF** — checkpoint failures/durations regress or spikes persist; revert, keep the
evidence, and let W4 carry the stall investigation.

**W3-i IMPLEMENTED (2026-09-30, CHG-451; design note §3.1).** Instrumentation only, no
behavior change: per-run ingestion **JFR** (GC+safepoint logging already existed on the
`java` command line — premise corrected in the design note and CHG-451), graceful-stop
materialisation, TM + ingestion `gc.log`/JFR pulled into every phase dir, per-phase
`docker stats` **+ host PSI** sampler, and `stage_gc_summary.py` wired after each phase.
Read-only findings while implementing: the **ingestion-writer JVM is exonerated** for the
known outliers (max stop 27.1 ms on the combined run, zero ≥ 28 ms events, nothing near
11:12:23 IST). Remaining suspects: the **Fluss tablet** (digest-pinned image strips `jcmd`;
per-phase restart rejected for the certification round — CHG-451 limitations) and **host
scheduling** (psi-cpu 8 % at the outlier vs 0–4 % elsewhere). Marker stays `[~]` until the
instrumented combined round lands and a gate certifies.

### W4 — TM CPU contention / slot isolation (design)

**GIVES YOU** — addresses the suspected second half of the single-subtask stalls: the
candle-live sink writer at 72–87 % busy sharing the TaskManager with the host.
**FIT** — evidence first: map per-subtask busy time against the stalled subtask per
window (the analysis already shows they are single-subtask events). If confirmed,
evaluate slot-sharing isolation (`slotSharingGroup`) and production sizing. The dev
environment is a single TM — label findings as dev-only until a production decision.
**COST** — topology/resource change: design + gate + production deploy review.
**ACTION** — map → design → approval → implement → measure.
**WRONG IF** — no mapping reproduces; close as not reproducible, record it, rely on W3.

**VERIFY-FIRST (2026-09-30, read-only):** the premise does **not reproduce**. Flink's
per-task busy metric (`flink_taskmanager_job_task_busyTimeMsPerSecond`; unit is **ms/s**,
so 1 000 = 100 %) shows the candle-live sink's **hottest subtask at 59–92 ms/s (≈ 6–9 %)**
across the night baseline, control, W1 and the combined smoke; every other operator
≤ 51 ms/s; back-pressure 0 everywhere. The "72–87 %" in the analysis/plan reads as a
**unit misread** (ms/s taken as %) unless it came from a measurement not captured here —
recompute before relying on it. The busy metric does not cover background threads
(materialization/compaction) or GC — the likely residence of the stalls. W4 stays
design-only until W3's GC instrumentation either proves a contention mechanism (then the
fix is likely JVM/tuning, not slot isolation) or the mapping stays unreproduced (then close
per the WRONG IF and rely on W3).

**CLOSED AS NOT REPRODUCIBLE (2026-09-30, W3/W4 design note):** the premise is a unit
misread (see above); no CPU saturation, back-pressure 0, and the TM JVM is exonerated for
the large outliers (`gc.log` + JFR `tm-diag.jfr`: max 25.6 ms stop near the 354/424 ms
spike; the largest pause of the whole run — 107 ms — produced no spike window). JVM and
background-thread attribution moves to W3-i instrumentation; W4 reopens only if that
attributes outliers to host/container scheduling (lever **W4'** in
`docs/plans/2026-09-30-w3-w4-design-note.md`).

### W5 — Structural hop reduction (only if W1–W4 miss 50 ms)

**GIVES YOU** — removes or merges one shuffle on the tick path (e.g. the host reading the
deduped stream directly instead of the aggregator side output).
**FIT** — design document first; correctness review required (host live/closed inputs and
DEC-056 features depend on the aggregator output; dedup semantics must not move).
**COST** — the largest change: full gate + restore drill if state layout changes.
**WRONG IF** — any effect on semantics, dedup, or state restore — abandon and re-scope.

### W6 — Combined confirmation + certification

**GIVES YOU** — the SLO certificate (or the honest unmet number).
**FIT** — best approved combination, one 900 s run under the locked protocol; all adopted
changes already merged with CHGs; full `make gate` after the last code change.
**ACTION** — run → extract → if all 60 snapshots pass both KPIs ≤ 50 ms: update the
dossier, this tracker, evidence map; operator sign-off. If not: record the achieved
numbers, the remaining driver, and mark S1 unmet with evidence (never silently).
**WRONG IF** — mixed results are reported as pass; forbidden.

## Risks

| Risk | Mitigation |
|---|---|
| W1 fetch-rate blow-up on Fluss servers | A/B with server metrics; intermediate values (5 ms); revert path |
| W2 flush overhead under load | watch CPU/throughput; revert; one variable per round |
| W3 materialization tuning changes checkpoint behavior | restore drills both directions + full gate |
| W4 slot isolation affects the production deck layout | design + deploy review; this host cannot prove a real-VM deploy (FACT clauses in ENVIRONMENT.md) |
| KPI histograms use sliding windows (4096 samples/subtask) | same protocol and extraction method for every round; compare like-for-like (baseline is windowed too) |
| Dev environment is a single TM; contention differs in production | label findings dev-only; production verification is a separate deploy step |

## Operator approval points

1. Approve this scope (now). — required before any round starts.
2. After each round's report: go/no-go for the next lever.
3. Before any code change (W1 default adoption, W3, W4, W5): explicit approval + CHG.
4. Production adoption: separate deploy approval; no silent default flips.
5. **2026-09-30 (operator):** sequence approved — after the W1+W2 combined report, proceed
   to **W3 and W4** (design notes first, per point 3), then run a combined **W1–W4** 900 s
   round as the certification attempt. **W5** stays the fallback if that round misses
   `p99 ≤ 50 ms`. W4's stated premise (sink 72–87 % busy) did not reproduce in the
   verify-first pass; its real content is the GC/background-thread investigation unless a
   contention mechanism is proven.

## Evidence map

| Step | Evidence |
|---|---|
| W0 | `logs/50ms-analysis-20260930/analysis.md` (baseline + decomposition) |
| W1/W2 | `logs/stage-profile-<ts>/` smoke + main, KPI extraction, env record |
| W3/W4/W5 | design note (in this plan) + CHG + gate certificate + before/after runs |
| W6 | 900 s run + `make gate` certificate + dossier diff |

## Rollback

- W1/W2: revert the env value; no repo state.
- W3: revert the config/commit; restore drills already required before measuring.
- W4/W5: revert the commit + `make gate`; topology returns to the certified shape.
- If any round fails: record it, keep the previous best measured state, continue or stop
  by operator decision.
