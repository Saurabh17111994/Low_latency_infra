# Plan: Stage-by-Stage Latency & Throughput Detection Harness — v2 (evolved)

- **Date (original)**: 2026-09-01
- **Date (v2 rewrite)**: 2026-09-02
- **Status**: v2 ACTIVE — proposed, awaiting approval to execute the ramp
- **Goal parent**: throughput >15k ticks/s sustained, end-to-end latency <4 s
  (see docs/Investigations_and_Reports/2026-09-01-throughput-degradation.md — RESOLVED for the 10k tier)
- **Repo**: streaming_project_New

## Progress / evolution log (why this file v2)

| When | What | Outcome |
|---|---|---|
| 2026-09-01 | Original plan proposed (below, condensed): Stage-by-stage capture harness to find where throughput collapsed | Plan accepted; harness built |
| 2026-09-01 | Stage A executed — `stage-capture.sh` + parser + tests landed | Harness works, unit-tested |
| 2026-09-01 | Stage B+C executed **pre-fix** (10Hz load, 15min capture) | Captured the DEGRADED regime (~1.6–3k/s) — first evidence set |
| 2026-09-01→02 | Root causes isolated **before** further Stages: (1) watermark idleness starved window closing under backpressure; (2) RocksDB managed-memory fraction 0.4→0.6 | Both fixed, validated at **9,950/s sustained** |
| 2026-09-02 | C2 TM-kill drill full-chain PASS (G7c 33,792/0, F4 0 orphans) | Pipeline correct + full-load stable at the 10.24k feed |
| 2026-09-02 | **v2 rewrite**: the original Stage C question ("which stage degrades?") is ANSWERED — the hunt shifts from *degradation* to *next bottleneck at higher rates* | This file |
| 2026-09-02 | Stage A2 fresh baseline (probes OFF, 10.24k, 12 min) | 10,346/s source in=out; e2e p50 611ms; no diverging stage |
| 2026-09-02 | Stage B2 probes built + A/B-validated (probes ON vs OFF) | CP3→CP4 net lag ~0 (source keeps up); CP9→CP10 staleness p50 1.9s; probe overhead +5-8% busy within noise (commit `f0ebbdd`) |
| 2026-09-02 | p95/p99 latency coverage added (user request) | Ingestion sampler flattens p50/p90/p99; B2 reports carry p99 columns (74/74 guards) |

**Key shift:** the original plan hunted a mystery collapse. We found it and fixed it
(no per-stage divergence needed — the collapse was systemic: watermark idleness +
RocksDB sizing, provable from the old captures + the fixes' validation). What remains
is the ORIGINAL 15k goal: find the *next* limiter as the feed scales up, and
decompose the latency budget. That is this v2.

## Aim (v2)

1. **Throughput**: name the exact stage that limits sustained rate as the feed
   ramps 10.24k → 12.5k → 15k ticks/s. The stage whose in-rate diverges from its
   out-rate — or that shows busy while upstream shows backpressure — is the
   bottleneck and the single target of the next fix.
2. **Latency**: decompose the measured tick→preview p50 (611ms at 10.24k) into a
   per-stage budget — feed→ack, read-lag, in-Flink, commit→read — so optimization
   targets the largest segment, not guesses.

## Design principles (unchanged from v1 — proven correct)

1. **Measure, don't instrument**: native Flink/Fluss metrics only. No per-record
   timestamps, no data-path code changes — instrumentation would add overhead to
   the path under test and distort the numbers.
2. **One aligned timeline**: every source sampled at the same cadence (5s), one run
   directory, offline analysis. Diagnostic artifacts are offline TSV/JSONL only —
   never in the data path.
3. **Fail-closed**: harness aborts if the job is not RUNNING or the feed is not at
   the expected rate; captures are never silently invalid.
4. **A/B validation for anything active**: any probe that touches Fluss while the
   pipeline runs gets a with/without run at one tier first, to prove the delta is
   noise (target: ≤1 read/s = 0.01% of write load).
5. **One lever at a time** after the verdict; re-measure the same tier after each
   change.

## Reused assets (already built, zero new cost)

- `stage-capture.sh` + `stage_capture_parse.py` + tests — per-5s per-operator
  numRecordsIn/Out, busy/backpressured/idle, latency histograms, watermark lag,
  ingestion counters, fluss-side rates, checkpoint jsonl (Stage A deliverable).
- Flink latency histograms already on (`metrics.latency.interval=2000`) in every drill.
- `holistic-analyze.py` — drill-time e2e measurements (preview e2e, window-close→commit).
- `tm-kill-full-load.sh` — proven load-gen + lifecycle harness (lock-serialized).

## Stages (v2)

### Stage A2 — fresh baseline at current code (10.24k)

Re-capture with the FIXED pipeline (the v1 Stage B capture predates the watermark
+ RocksDB fixes → invalid as baseline).

- Run: RATE_HZ=10, 12 min load, stage-capture throughout.
- Deliverable: per-stage in-rate / out-rate / busy% / latency / watermark-lag table
  for the current healthy steady state; the 611ms-p50 baseline budget (known total,
  unbudgeted).

### Stage B2 — the two missing passive checkpoints (build, small)

1. **CP3→CP4 read lag** (Fluss append → Flink source consume): source offsets vs
   appended count, sampled — both numbers already exist in metrics; this is a
   subtraction, purely passive, zero hot-path cost.
2. **CP9→CP10 consumer-read probe** (row committed → readable): one throttled
   reader sampling latest preview row with timestamps, 1 read/s max. Active but
   negligible; A/B-validated at one tier before trusting.

### Stage C2 — rate ramp to find the NEXT bottleneck

| Tier | Load | Capture | Question answered |
|---|---|---|---|
| 1 | 10.24k/s (RATE_HZ=10) | full stage capture | fresh baseline (Stage A2 already produced; reuse) |
| 2 | 12.5k/s (RATE_HZ≈12.2) | full stage capture | first divergence appears? which stage? |
| 3 | 15k/s (RATE_HZ≈14.6) | full stage capture | does the feed sustain 15k? where does it break? |

Per-stage verdict rule at each tier (unchanged from v1, now the core method):

```
stage in-rate ≈ out-rate, all busy% high   → whole pipeline CPU-bound (scale problem)
one stage: in > out, it is busy             → THAT operator is the bottleneck (code target)
one stage: in > out, it is backpressured    → bottleneck is DOWNSTREAM of it (follow chain)
source stalls, everything idle              → feed or Fluss read side (CP3→CP4)
```

Deliverable: per-tier divergence table + the named bottleneck stage with numbers.

**Feed caveat:** faketool is currently the known bound at 10,240/s — tier 2/3 need
a feed-rate bump first (faketool `-real-rate-hz` parameter; verify real-rate
confirmation line, fail-closed if the feed can't deliver).

### Stage D2 — latency budget decomposition

Stack the 611ms total from passive timestamps:
feed→ack (ingestion counters) + CP3→CP4 read lag (A2/B2) + in-Flink source→operator
(latency markers) + window-close→commit (output_ts − window_end, known p50 1.6s for
finals; preview path uses its own stamp) + commit→consumer-read (B2 probe).

Deliverable: stacked budget table where segments sum ≈ the measured total; the
largest segment is the latency fix target. Final candles stay floored at 15s by
design (reported separately; optimization target = preview/early-signal path only).

### Stage E2 — verdict + fix target

Update the throughput investigation doc with: named bottleneck (throughput) +
largest latency segment, evidence, chosen fix (config / single-operator code /
infrastructure), re-measurement criteria. Fix implementation is a SEPARATE
follow-up plan (not this file).

## What this plan deliberately does NOT do (unchanged)

- No code changes in the SignalJob data path.
- No custom per-record instrumentation; no serialization/format changes.
- No capacity tuning experiments before the verdict (RocksDB buffers, checkpoint
  contract) — those follow, one lever at a time.
- No production-sizing claims — single-box findings only.
- No second "degradation hunt" — that question is answered (see evolution log).

## Risks / open questions

- Flink 2.2 busy/backpressure REST shapes were inconsistent for some vertices in
  v1 — Stage A2 re-verifies per-vertex availability first; TM Prometheus fallback.
- Latency histograms are source-to-operator, NOT tick-to-table — Stage D2 uses the
  passive table-row stamps for the true e2e; the histograms only bound the in-Flink
  segment.
- Feed rate at 12.5k/15k unproven — fail-closed check first (feed is the cheapest
  potential answer: if faketool can't sustain, the "bottleneck" is the harness).
- Power-cut incident 2026-09-02 (host reboot mid-analysis) — runbooks now assume
  captures may need standalone re-analysis; evidence files are the durable truth.

## Sequencing

1. Write v2 file (this). Wait for approval.
2. Stage A2 (fresh baseline) → Stage B2 (two passive checkpoints, A/B-validated).
3. Stage C2 tiers → Stage D2 budget → Stage E2 verdict.
4. One fix lever → re-measure same tier → update investigation doc.
